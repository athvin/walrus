#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! DDL capture against compose (`#[ignore]` — needs source PG + MinIO + control PG). An `ALTER TABLE …
//! ADD COLUMN` on the source writes a `ddl_manifest` row (stamped with the DDL's `c_lsn`), bumps the
//! table's structural `schema_version`, and cuts a fresh Parquet file — so the prior file carries the
//! old version and the next the new (the homogeneous-file rule). A `COMMENT ON` is recorded but is
//! metadata-only (no bump, no cut). `walrus.ddl_audit`/`walrus.heartbeat` are never materialised. The
//! parsing/version logic is unit-tested in `src/ddl.rs`.
//!
//!   cargo test -p pg-sink --test ddl_capture -- --ignored

use common::Lsn;
use pg_sink::batch::{BatchTriggers, SystemClock};
use pg_sink::consume::{flush_batch, on_frame, on_relation, BatchRouter};
use pg_sink::ddl::{DdlConsumer, DdlEvent};
use pg_sink::heartbeat::InternalTables;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::relcache::RelationCache;
use pg_sink::replication::{ReplicationMessage, ReplicationStream};
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0002: &str = include_str!("../../../migrations/source/0002_ddl_triggers.sql");

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn source() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

fn minio() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

async fn drop_slot(admin: &tokio_postgres::Client, slot: &str) {
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + MinIO + control PG)"]
async fn alter_add_column_bumps_version_and_cuts_file() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_ddl";
    let epoch = 2_330_001;
    let admin = source().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0002).await.unwrap();
    admin
        .execute(
            "ALTER TABLE public.orders DROP COLUMN IF EXISTS ddl_extra",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 850000 AND 850999",
            &[],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();
    let sink = ParquetSink::new(minio(), "walrus".to_string(), epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    for tbl in ["file_manifest", "ddl_manifest", "schema_registry"] {
        sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(&pool)
            .await
            .unwrap();
    }
    // High row cap → the pre-DDL row stays buffered until the DDL CUTS it (the interesting path).
    let mut router = BatchRouter::new(
        BatchTriggers {
            max_rows: u64::MAX,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
    );
    let mut ddl = DdlConsumer::new(epoch);
    let mut internal = InternalTables::default();
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    // v1 row · ALTER ADD COLUMN · v2 rows · COMMENT (metadata) · a final v2 row to give a stop marker.
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (850001, 'v1')",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute("ALTER TABLE public.orders ADD COLUMN ddl_extra text", &[])
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO public.orders (id, status, ddl_extra) VALUES (850002, 'v2', 'x')",
            &[],
        )
        .await
        .unwrap();
    admin
        .execute("COMMENT ON TABLE public.orders IS 'walrus ddl test'", &[])
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO public.orders (id, status, ddl_extra) VALUES (850003, 'end', 'y')",
            &[],
        )
        .await
        .unwrap();

    let mut saw_end = false;
    tokio::time::timeout(Duration::from_secs(30), async {
        while !saw_end {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Relation { relation, .. } => {
                    internal.note_relation(relation);
                    let v = ddl.version_of(&relation.schema, &relation.name);
                    on_relation(&mut cache, &pool, epoch, relation.clone(), v)
                        .await
                        .unwrap();
                }
                Message::Insert {
                    relation_oid, new, ..
                } if internal.is_ddl_audit(*relation_oid) => {
                    let rel = internal.ddl_audit_rel().unwrap();
                    let ev = DdlEvent::from_tuple(rel, new).unwrap();
                    if ddl.consume(&pool, &ev).await.unwrap().is_some() {
                        for sealed in router
                            .cut_table(&cache, &ev.source_schema, &ev.source_table)
                            .unwrap()
                        {
                            flush_batch(&sink, &pool, epoch, sealed).await.unwrap();
                        }
                    }
                }
                Message::Insert { new, .. } => {
                    router.route(&cache, &msg, frame_lsn, 1).unwrap();
                    // The last row (850003) is our stop marker.
                    if matches!(new.first(), Some(common::TupleValue::Text(s)) if s == "850003") {
                        saw_end = true;
                    }
                }
                Message::Commit { .. } => {
                    for sealed in router.route(&cache, &msg, frame_lsn, 1).unwrap() {
                        flush_batch(&sink, &pool, epoch, sealed).await.unwrap();
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the DDL + rows stream within 30s");

    // Final drain: force-seal the buffered v2 rows into a v2 file.
    for sealed in router.drain_committed().unwrap() {
        flush_batch(&sink, &pool, epoch, sealed).await.unwrap();
    }

    // --- Assertions. Files: a v1 file (the cut pre-DDL row) AND v2 file(s) (post-DDL rows).
    let files: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT schema_version, row_count FROM walrus.file_manifest WHERE epoch = $1 AND source_table = 'orders'",
    )
    .bind(epoch)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        files.iter().any(|(v, _)| *v == 1),
        "a schema_version=1 file exists (pre-DDL, cut)"
    );
    assert!(
        files.iter().any(|(v, _)| *v == 2),
        "a schema_version=2 file exists (post-DDL)"
    );

    // ddl_manifest: the ALTER bumped to version 2 with a real c_lsn; the COMMENT is recorded but did
    // NOT bump (still version 2).
    let ddls: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT c_tag, schema_version, c_lsn::text FROM walrus.ddl_manifest WHERE epoch = $1 ORDER BY id",
    )
    .bind(epoch)
    .fetch_all(&pool)
    .await
    .unwrap();
    let alter = ddls
        .iter()
        .find(|(t, ..)| t == "ALTER TABLE")
        .expect("ALTER recorded");
    assert_eq!(alter.1, 2, "ALTER produced schema_version 2");
    assert!(
        alter.2.parse::<Lsn>().unwrap() > Lsn::ZERO,
        "the ALTER carries the DDL's c_lsn"
    );
    let comment = ddls
        .iter()
        .find(|(t, ..)| t == "COMMENT")
        .expect("COMMENT recorded");
    assert_eq!(
        comment.1, 2,
        "COMMENT is metadata-only — no version bump beyond the structural 2"
    );

    // Internal tables are NEVER materialised.
    let internal_files: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1 AND source_table IN ('ddl_audit', 'heartbeat')",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        internal_files, 0,
        "walrus.ddl_audit / walrus.heartbeat are never files"
    );

    // --- Cleanup.
    let uris: Vec<String> =
        sqlx::query_scalar("SELECT s3_uri FROM walrus.file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(&pool)
            .await
            .unwrap();
    let store = minio();
    for uri in uris {
        if let Some(key) = uri.strip_prefix("s3://walrus/") {
            let _ = store.delete(&object_store::path::Path::from(key)).await;
        }
    }
    for tbl in ["file_manifest", "ddl_manifest", "schema_registry"] {
        let _ = sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(&pool)
            .await;
    }
    let _ = admin
        .execute(
            "ALTER TABLE public.orders DROP COLUMN IF EXISTS ddl_extra",
            &[],
        )
        .await;
    let _ = admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 850000 AND 850999",
            &[],
        )
        .await;
    drop(stream);
    drop_slot(&admin, slot).await;
}
