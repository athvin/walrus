#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Snapshot / backfill bootstrap against compose (`#[ignore]` — needs source PG + MinIO + control PG).
//! Rows that exist before the slot's `consistent_point` are backfilled as `kind='snapshot'` Parquet +
//! manifest rows (all sharing `lsn_end = consistent_point`); a row written *after* the export is not in
//! the snapshot and instead arrives once as a post-`consistent_point` **stream** change on handoff.
//! The SQL/meta shape is unit-tested in `src/snapshot.rs`.
//!
//!   cargo test -p pg-sink --test snapshot_backfill -- --ignored

use common::{Lsn, TupleValue};
use object_store::path::Path;
use object_store::ObjectStore;
use pg_sink::batch::BatchTriggers;
use pg_sink::consume::on_frame;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::replication::ReplicationMessage;
use pg_sink::snapshot::{describe_source_relation, published_user_tables, Backfill, SnapshotConn};
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

fn minio() -> Arc<dyn ObjectStore> {
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

fn orders_id(new: &[TupleValue]) -> Option<i32> {
    match new.first()? {
        TupleValue::Text(s) => s.parse().ok(),
        _ => None,
    }
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + MinIO + control PG)"]
async fn backfill_preloaded_rows_then_streams_post_consistent_point() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_snapshot";
    let epoch = 2_290_001;
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id IN (990001, 990002)",
            &[],
        )
        .await
        .unwrap();
    // Preload a row that MUST land in the snapshot (it exists before the export).
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (990001, 'preloaded')",
            &[],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;

    // Create the slot with an exported snapshot — this fixes consistent_point.
    let mut snap_conn = SnapshotConn::connect(&source_url()).await.unwrap();
    let snapshot = snap_conn.create_slot_with_snapshot(slot).await.unwrap();

    // A row written AFTER the export: absent from the snapshot, must stream on handoff.
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (990002, 'post-consistent')",
            &[],
        )
        .await
        .unwrap();

    // Backfill every published user table under the exported snapshot.
    let sink = pg_sink::sink::ParquetSink::new(minio(), "walrus".to_string(), epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let mut backfill = Backfill::connect(
        &source_url(),
        epoch,
        "walrus-snapshot-test".to_string(),
        BatchTriggers {
            max_rows: u64::MAX,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Duration::ZERO,
    )
    .await
    .unwrap();
    let tables = published_user_tables(&admin, "walrus_pub").await.unwrap();
    assert!(
        tables.iter().any(|(s, t)| s == "public" && t == "orders"),
        "orders is a published user table"
    );
    assert!(
        !tables.iter().any(|(s, _)| s == "walrus"),
        "walrus-internal tables are never backfilled"
    );
    let mut orders_oid = None;
    for (schema, table) in &tables {
        let rel = describe_source_relation(&admin, schema, table)
            .await
            .unwrap();
        if table == "orders" {
            orders_oid = Some(rel.oid);
        }
        backfill
            .copy_table(&rel, &snapshot, &sink, &pool, 1)
            .await
            .unwrap();
    }

    // Every snapshot file for this epoch is kind='snapshot' and shares one lsn_end = consistent_point.
    let files: Vec<(String, String)> =
        sqlx::query_as("SELECT kind, lsn_end::text FROM walrus.file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        !files.is_empty(),
        "backfill produced snapshot manifest rows"
    );
    assert!(
        files.iter().all(|(kind, _)| kind == "snapshot"),
        "all files are kind='snapshot'"
    );
    let distinct_ends: std::collections::HashSet<Lsn> =
        files.iter().map(|(_, e)| e.parse().unwrap()).collect();
    assert_eq!(
        distinct_ends.len(),
        1,
        "all snapshot files share one lsn_end"
    );
    assert_eq!(
        *distinct_ends.iter().next().unwrap(),
        snapshot.consistent_point,
        "the shared lsn_end is the exported consistent_point"
    );

    // Hand off to streaming from consistent_point → the post-export row arrives as a STREAM change
    // (it was never in the snapshot: no double count).
    let mut stream = snap_conn.into_stream(slot, "walrus_pub").await.unwrap();
    let mut ctx = StreamCtx::default();
    let saw_streamed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            let _lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            if let Some(Message::Insert {
                relation_oid, new, ..
            }) = on_frame(&mut ctx, frame).unwrap()
            {
                if orders_oid == Some(relation_oid) && orders_id(&new) == Some(990002) {
                    return true;
                }
            }
        }
    })
    .await
    .expect("the post-consistent-point row streams within 15s");
    assert!(
        saw_streamed,
        "a row written during backfill streams once, not double-counted"
    );

    // Cleanup: S3 objects, manifest rows, slot, test data.
    let uris: Vec<String> =
        sqlx::query_scalar("SELECT s3_uri FROM walrus.file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(&pool)
            .await
            .unwrap();
    let store = minio();
    for uri in uris {
        if let Some(key) = uri.strip_prefix("s3://walrus/") {
            let _ = store.delete(&Path::from(key)).await;
        }
    }
    sqlx::query("DELETE FROM walrus.file_manifest WHERE epoch = $1")
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id IN (990001, 990002)",
            &[],
        )
        .await
        .unwrap();
    drop(stream);
    drop_slot(&admin, slot).await;
}
