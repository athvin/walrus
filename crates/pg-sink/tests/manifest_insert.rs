//! Durability step (b) against compose (`#[ignore]` — needs MinIO + control PG). After a durable PUT,
//! a `file_manifest` `ready` row is committed with `lsn_end` = the **commit** LSN. Each test runs in a
//! rolled-back transaction (control DB) under a unique epoch, and cleans up its S3 object.
//!
//!   cargo test -p pg-sink --test manifest_insert -- --ignored

use common::{
    Kind, Lsn, Op, PgColumn, PgRelation, ReplicaIdentity, SinkMeta, TupleValue, UtcTimestamp,
};
use control::{connect, run_migrations};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use pg_sink::batch::SealedBatch;
use pg_sink::consume::flush_batch;
use pg_sink::sink::ParquetSink;
use pg_to_arrow::{oids, BatchBuilder};
use sqlx::postgres::PgPool;
use std::sync::Arc;

fn minio_store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .expect("build MinIO store"),
    )
}

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn control_pool() -> PgPool {
    let pool = connect(&control_url())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    pool
}

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, typmod: i32| PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key: false,
    };
    PgRelation {
        oid: 16397,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", oids::INT4, -1), col("note", oids::TEXT, -1)],
    }
}

/// Row LSN is deliberately `0/10` — far below the commit LSN passed to `sealed()`.
fn meta() -> SinkMeta {
    SinkMeta {
        op: Op::Insert,
        lsn: "0/10".parse().unwrap(),
        commit_lsn: "0/10".parse().unwrap(),
        commit_ts: UtcTimestamp::parse_rfc3339("2026-07-07T12:00:00Z").unwrap(),
        xid: 1,
        epoch: 1,
        batch_id: "b".to_string(),
        schema_version: 1,
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        kind: Kind::Stream,
        unchanged_toast: vec![],
        sink_instance: "walrus-pg-sink-0".to_string(),
        sink_processed_at: UtcTimestamp::now(),
    }
}

/// A one-row batch whose **commit LSN** (`lsn_end`) is `lsn_end`, distinct from the row LSN (`0/10`).
fn sealed(lsn_end: &str) -> SealedBatch {
    let mut bb = BatchBuilder::new(&orders()).unwrap();
    bb.append_row(
        &[TupleValue::Text("1".into()), TupleValue::Text("hi".into())],
        &meta(),
    )
    .unwrap();
    SealedBatch {
        record_batch: bb.finish().unwrap(),
        schema: "public".to_string(),
        table: "orders".to_string(),
        schema_version: 5,
        lsn_start: "0/A000".parse().unwrap(),
        lsn_end: lsn_end.parse().unwrap(),
        row_count: 1,
    }
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO + control PG)"]
async fn object_and_manifest_row_both_exist_after_flush() {
    let store = minio_store();
    let pool = control_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 2_250_001;
    let sink = ParquetSink::new(store.clone(), "walrus".to_string(), epoch);

    let obj = flush_batch(&sink, &mut *tx, epoch, sealed("0/A100"))
        .await
        .unwrap();

    // The object is durably present in S3.
    assert!(store.head(&obj.key).await.unwrap().size > 0);
    // Exactly one ready row was committed (in this tx).
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1 AND source_table = 'orders'",
    )
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(count, 1);

    tx.rollback().await.unwrap();
    let _ = store.delete(&obj.key).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO + control PG)"]
async fn manifest_lsn_end_equals_commit_lsn_not_row_lsn() {
    let store = minio_store();
    let pool = control_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 2_250_002;
    let sink = ParquetSink::new(store.clone(), "walrus".to_string(), epoch);

    // Commit LSN 0/A100; the batch's rows carry row LSN 0/10.
    let obj = flush_batch(&sink, &mut *tx, epoch, sealed("0/A100"))
        .await
        .unwrap();

    let lsn_text: String =
        sqlx::query_scalar("SELECT lsn_end::text FROM walrus.file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let stored: Lsn = lsn_text.parse().unwrap();
    assert_eq!(stored, obj.lsn_end, "lsn_end is the batch's commit LSN");
    assert_eq!(stored, "0/A100".parse().unwrap());
    assert_ne!(stored, "0/10".parse().unwrap(), "NOT the max row LSN");

    tx.rollback().await.unwrap();
    let _ = store.delete(&obj.key).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (MinIO + control PG)"]
async fn row_is_ready_kind_stream_and_epoch_stamped() {
    let store = minio_store();
    let pool = control_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 2_250_003;
    let sink = ParquetSink::new(store.clone(), "walrus".to_string(), epoch);

    let obj = flush_batch(&sink, &mut *tx, epoch, sealed("0/B200"))
        .await
        .unwrap();

    let (kind, status, schema_version): (String, String, i64) = sqlx::query_as(
        "SELECT kind, status, schema_version FROM walrus.file_manifest WHERE epoch = $1",
    )
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(kind, "stream");
    assert_eq!(status, "ready");
    assert_eq!(schema_version, 5, "schema_version stamped from the batch");

    tx.rollback().await.unwrap();
    let _ = store.delete(&obj.key).await;
}
