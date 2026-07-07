//! The flagship correctness test (§1.6, proto §9b) against compose (`#[ignore]` — needs source PG with
//! `logical_decoding_work_mem=64kB` + MinIO + control PG). A rolled-back **savepoint** inside an
//! otherwise-committing streamed transaction is *still streamed*; only `Stream Abort {sub != top}` tells
//! us those rows died. 3000 kept-A + a rolled-back savepoint + 3000 kept-B must yield a `ready` file
//! with **exactly 6000** rows — the rolled-back rows are never present. Off-by-one here is precisely the
//! silent mirror corruption this test exists to catch. The demux logic is unit-tested in
//! `src/stream_txn.rs`.
//!
//!   cargo test -p pg-sink --test subtransaction_exclusion -- --ignored

use common::Lsn;
use pg_sink::batch::{BatchTriggers, SystemClock};
use pg_sink::consume::on_frame;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::relcache::RelationCache;
use pg_sink::replication::{ReplicationMessage, ReplicationStream};
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use pg_sink::stream_txn::StreamDemux;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_MIGRATION: &str = include_str!("../../../migrations/source/0001_publication.sql");

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
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn savepoint_rollback_ready_file_has_exactly_6000_rows() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_subtxn";
    let epoch = 2_310_001;
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 810000 AND 839999",
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
    // Start clean: a prior failed run may have left ready rows for this epoch.
    sqlx::query("DELETE FROM walrus.file_manifest WHERE epoch = $1")
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
    let mut demux = StreamDemux::new(
        BatchTriggers {
            max_rows: 100_000,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        1,
        "test".to_string(),
    );
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    // proto §9b: kept-A (3000, top branch) · rolled-back savepoint (3000, streamed then discarded) ·
    // kept-B (3000, new savepoint after the rollback). The whole thing exceeds work_mem → streams.
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO public.orders (id, status) SELECT g, 'A' FROM generate_series(810000, 812999) g;
             SAVEPOINT sp;
             INSERT INTO public.orders (id, status) SELECT g, 'X' FROM generate_series(820000, 822999) g;
             ROLLBACK TO SAVEPOINT sp;
             INSERT INTO public.orders (id, status) SELECT g, 'B' FROM generate_series(830000, 832999) g;
             COMMIT;",
        )
        .await
        .unwrap();

    let mut saw_subabort = false;
    let mut committed = false;
    tokio::time::timeout(Duration::from_secs(60), async {
        while !committed {
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
                    cache.upsert_from_relation(relation.clone(), 1).unwrap();
                }
                Message::StreamStart { xid, first_segment } => {
                    demux.on_stream_start(*xid, *first_segment, frame_lsn);
                }
                Message::StreamStop => demux.on_stream_stop(),
                m @ (Message::Insert { xid: Some(_), .. }
                | Message::Update { xid: Some(_), .. }
                | Message::Delete { xid: Some(_), .. }) => {
                    demux.on_change(m, frame_lsn).unwrap();
                }
                Message::StreamAbort { top_xid, sub_xid } => {
                    if top_xid != sub_xid {
                        saw_subabort = true;
                    }
                    demux.on_stream_abort(*top_xid, *sub_xid);
                }
                Message::StreamCommit {
                    xid, commit_lsn, ..
                } => {
                    let objs = demux
                        .on_stream_commit(*xid, *commit_lsn, &cache, &sink)
                        .await
                        .unwrap();
                    for obj in &objs {
                        pg_sink::manifest::record_ready(&pool, epoch, obj)
                            .await
                            .unwrap();
                    }
                    committed = true;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the streamed txn commits within 60s");

    assert!(
        saw_subabort,
        "a Stream Abort {{sub != top}} (rolled-back savepoint) was decoded"
    );

    // THE flagship assertion: exactly 6000 rows in the ready file(s) — never the rolled-back savepoint.
    let total_rows: i64 = sqlx::query_scalar(
        "SELECT COALESCE(sum(row_count), 0)::bigint FROM walrus.file_manifest WHERE epoch = $1 AND status = 'ready'",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        total_rows, 6000,
        "the ready file has EXACTLY 6000 rows (3000 kept-A + 3000 kept-B); the rolled-back savepoint's rows are excluded"
    );
    // Every file is kind='stream' (the top-level txn still committed).
    let non_stream: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1 AND kind <> 'stream'",
    )
    .bind(epoch)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(non_stream, 0, "the committed survivors are kind='stream'");

    // Cleanup.
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
    let _ = sqlx::query("DELETE FROM walrus.file_manifest WHERE epoch = $1")
        .bind(epoch)
        .execute(&pool)
        .await;
    let _ = admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 810000 AND 839999",
            &[],
        )
        .await;
    drop(stream);
    drop_slot(&admin, slot).await;
}
