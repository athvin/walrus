#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Compose-gated integration tests for the `file_manifest` queue models.
//!
//! Each test runs inside a rolled-back transaction and namespaces its rows by a unique `epoch`, so
//! the tests are isolated from each other and idempotent across runs. Gated behind the
//! `integration` feature (needs the PR 0.6 control Postgres).
#![cfg(feature = "integration")]

use common::Lsn;
use control::NewManifestFile;
use control::{claim_ready, connect, delete_claimed, insert_ready, mark_failed, run_migrations};
use sqlx::postgres::PgPool;

fn control_dsn() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn pool() -> PgPool {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    pool
}

fn file(epoch: i64, table: &str, lsn_end: &str) -> NewManifestFile {
    let lsn: Lsn = lsn_end.parse().unwrap();
    NewManifestFile {
        epoch,
        source_schema: "public".to_string(),
        source_table: table.to_string(),
        s3_uri: format!("s3://walrus/{epoch}/public/{table}/{lsn_end}.parquet"),
        kind: "stream".to_string(),
        row_count: 1,
        lsn_start: lsn,
        lsn_end: lsn,
        schema_version: 1,
        reload_id: None,
    }
}

#[tokio::test]
async fn claim_orders_by_lsn_end_then_id() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 900_001;

    // Insert with lsn_ends out of order, plus two files that SHARE one lsn_end (0/20).
    let a = insert_ready(&mut *tx, &file(epoch, "t", "0/30"))
        .await
        .unwrap();
    let b = insert_ready(&mut *tx, &file(epoch, "t", "0/10"))
        .await
        .unwrap();
    let c = insert_ready(&mut *tx, &file(epoch, "t", "0/20"))
        .await
        .unwrap();
    let d = insert_ready(&mut *tx, &file(epoch, "t", "0/20"))
        .await
        .unwrap();
    assert!(c < d, "c was inserted before d, so its serial id is lower");

    let claimed = claim_ready(&mut *tx, epoch, "public", "t", 100)
        .await
        .unwrap();
    let order: Vec<i64> = claimed.iter().map(|r| r.id).collect();
    // (lsn_end ASC, id ASC): 0/10 (b), then 0/20 (c before d), then 0/30 (a).
    assert_eq!(order, vec![b, c, d, a]);

    // The commit-LSN values round-trip through pg_lsn keeping their ordering.
    assert_eq!(claimed[0].lsn_end, "0/10".parse::<Lsn>().unwrap());
    assert_eq!(claimed[3].lsn_end, "0/30".parse::<Lsn>().unwrap());

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn claim_does_not_skip_equal_lsn_end_snapshot_files() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 900_002;

    // Many snapshot files sharing one lsn_end (the exported snapshot's consistent_point).
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(
            insert_ready(&mut *tx, &file(epoch, "snap", "0/AAAA"))
                .await
                .unwrap(),
        );
    }

    let claimed = claim_ready(&mut *tx, epoch, "public", "snap", 100)
        .await
        .unwrap();
    // ALL five are claimed (none skipped by an `lsn_end >` filter), in ascending id order.
    assert_eq!(claimed.len(), 5);
    assert_eq!(claimed.iter().map(|r| r.id).collect::<Vec<_>>(), ids);

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn delete_claimed_retires_exactly_the_given_ids() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 900_003;

    let id1 = insert_ready(&mut *tx, &file(epoch, "d", "0/10"))
        .await
        .unwrap();
    let id2 = insert_ready(&mut *tx, &file(epoch, "d", "0/20"))
        .await
        .unwrap();
    let id3 = insert_ready(&mut *tx, &file(epoch, "d", "0/30"))
        .await
        .unwrap();

    let n = delete_claimed(&mut *tx, &[id1, id3]).await.unwrap();
    assert_eq!(n, 2);

    let remaining = claim_ready(&mut *tx, epoch, "public", "d", 100)
        .await
        .unwrap();
    assert_eq!(
        remaining.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![id2]
    );

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn mark_failed_removes_row_from_ready_claims() {
    let pool = pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 900_004;

    let good = insert_ready(&mut *tx, &file(epoch, "f", "0/10"))
        .await
        .unwrap();
    let poison = insert_ready(&mut *tx, &file(epoch, "f", "0/20"))
        .await
        .unwrap();

    mark_failed(&mut *tx, poison).await.unwrap();

    // A `status='failed'` row is no longer `ready`, so the claim skips it (the partial index only
    // covers ready rows).
    let claims = claim_ready(&mut *tx, epoch, "public", "f", 100)
        .await
        .unwrap();
    assert_eq!(claims.iter().map(|r| r.id).collect::<Vec<_>>(), vec![good]);

    tx.rollback().await.unwrap();
}
