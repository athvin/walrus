//! Compose-gated integration tests for the control-plane migrations.
//!
//! Requires the control Postgres from the PR 0.6 dev harness (`just up`). Gated behind the
//! `integration` feature so the DB-free baseline `cargo test --workspace` skips it; the CI
//! integration job runs `cargo test -p control --features integration --test migrations`.
#![cfg(feature = "integration")]

use control::{connect, run_migrations};
use sqlx::postgres::PgPool;

/// The control DSN — the compose `control-pg` (host port 5433) unless overridden.
fn control_dsn() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn migrated_pool() -> PgPool {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG");
    run_migrations(&pool).await.expect("migrations apply");
    pool
}

#[tokio::test]
async fn migrations_create_all_tables() {
    let pool = migrated_pool().await;
    // Idempotent: a second run is a no-op (sqlx skips already-applied versions).
    run_migrations(&pool)
        .await
        .expect("migrations are idempotent");

    for table in [
        "replication_state",
        "file_manifest",
        "loader_checkpoint",
        "schema_registry",
        "ddl_manifest",
    ] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'walrus' AND table_name = $1)",
        )
        .bind(table)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exists, "walrus.{table} must exist after migration");
    }
}

#[tokio::test]
async fn file_manifest_partial_index_is_ready_only() {
    let pool = migrated_pool().await;
    let indexdef: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes \
         WHERE schemaname = 'walrus' AND indexname = 'file_manifest_claim_idx'",
    )
    .fetch_one(&pool)
    .await
    .expect("claim index exists");

    assert!(
        indexdef.contains("status = 'ready'"),
        "claim index must be partial on status='ready': {indexdef}"
    );
    assert!(
        indexdef.contains("lsn_end"),
        "claim index must be keyed by lsn_end: {indexdef}"
    );
}

#[tokio::test]
async fn checkpoint_check_rejects_transformed_ahead_of_raw() {
    let pool = migrated_pool().await;

    // A valid checkpoint (transformed <= raw) is accepted — proven inside a rolled-back txn so the
    // shared control DB stays clean.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "INSERT INTO walrus.loader_checkpoint \
         (epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn) \
         VALUES (9999, 'public', 'chk_ok', '0/20'::pg_lsn, '0/10'::pg_lsn)",
    )
    .execute(&mut *tx)
    .await
    .expect("transformed <= raw is accepted");
    tx.rollback().await.unwrap();

    // transformed AHEAD of raw violates the CHECK and is rejected.
    let res = sqlx::query(
        "INSERT INTO walrus.loader_checkpoint \
         (epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn) \
         VALUES (9999, 'public', 'chk_bad', '0/10'::pg_lsn, '0/20'::pg_lsn)",
    )
    .execute(&pool)
    .await;
    assert!(
        res.is_err(),
        "CHECK (transformed_lsn <= raw_appended_lsn) must reject transformed ahead of raw"
    );
}
