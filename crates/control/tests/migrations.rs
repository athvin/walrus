#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compose-gated integration tests for the control-plane migrations.
//!
//! Requires the control Postgres from the dev harness (`just up`). Gated behind the
//! `integration` feature so the DB-free baseline `cargo test --workspace` skips it; the CI
//! integration job runs `cargo test -p control --features integration --test migrations`.
#![cfg(feature = "integration")]

use control::{connect, run_migrations};
use sqlx::Connection;
use sqlx::postgres::PgPool;
use uuid::Uuid;

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
        "table_reload",
        "table_reload_marker",
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
async fn unified_reconcile_migration_installs_request_fences_and_marker_constraints() {
    let pool = migrated_pool().await;

    for column in [
        "source_request_id",
        "parent_request_id",
        "request_scope",
        "start_lsn",
    ] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'walrus' AND table_name = 'table_reload' AND column_name = $1)",
        )
        .bind(column)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exists, "walrus.table_reload.{column} must exist");
    }

    let request_index: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes \
         WHERE schemaname = 'walrus' AND indexname = 'table_reload_source_request_target'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    for key in [
        "epoch",
        "source_request_id",
        "source_schema",
        "source_table",
    ] {
        assert!(
            request_index.contains(key),
            "source request idempotency index must contain {key}: {request_index}"
        );
    }
    assert!(request_index.contains("source_request_id IS NOT NULL"));

    let live_index: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes \
         WHERE schemaname = 'walrus' AND indexname = 'table_reload_one_live'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    for predicate_part in [
        "exporting",
        "export_complete",
        "requested",
        "source_request_id IS NULL",
    ] {
        assert!(
            live_index.contains(predicate_part),
            "live-attempt index must serialize active attempts while leaving source requests queued; \
             missing {predicate_part}: {live_index}"
        );
    }

    let marker_pk: Vec<String> = sqlx::query_scalar(
        "SELECT a.attname
         FROM pg_index i
         JOIN pg_class t ON t.oid = i.indrelid
         JOIN pg_namespace n ON n.oid = t.relnamespace
         JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
         JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
         WHERE n.nspname = 'walrus' AND t.relname = 'table_reload_marker' AND i.indisprimary
         ORDER BY k.ord",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(marker_pk, ["reload_id", "marker_kind"]);
}

#[tokio::test]
async fn unified_reconcile_migration_backfills_and_guards_direct_fence_identities() {
    let pool = connect(&control_dsn())
        .await
        .expect("connect to control PG for isolated 0006 upgrade fixture");
    let mut tx = pool.begin().await.unwrap();
    let schema = format!("walrus_migration_{}", Uuid::new_v4().simple());

    // Build the exact table/index shape that 0006 inherited from 0004 in an isolated schema, then
    // run the production migration text against it. This tests the upgrade UPDATE itself instead
    // of merely observing a database whose sqlx ledger applied 0006 before this test began.
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.table_reload (
           reload_id bigserial PRIMARY KEY,
           epoch bigint NOT NULL,
           source_schema text NOT NULL,
           source_table text NOT NULL,
           flavor text NOT NULL,
           status text NOT NULL DEFAULT 'requested'
         );
         CREATE UNIQUE INDEX table_reload_one_live
           ON {schema}.table_reload (epoch, source_schema, source_table)
           WHERE status NOT IN ('complete', 'failed');
         INSERT INTO {schema}.table_reload
           (epoch, source_schema, source_table, flavor, status)
         VALUES
           (1, 'public', 'historical_complete', 'reload', 'complete'),
           (1, 'public', 'historical_failed', 'reload', 'failed');"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();

    let migration = include_str!("../../../migrations/control/0006_unified_reconcile.sql")
        .replace("walrus.", &format!("{schema}."));
    sqlx::raw_sql(&migration)
        .execute(&mut *tx)
        .await
        .expect("0006 applies to the isolated v5-shaped fixture");

    let backfilled: Vec<(Option<Uuid>, Option<Uuid>)> = sqlx::query_as(&format!(
        "SELECT source_request_id, parent_request_id
         FROM {schema}.table_reload
         WHERE source_table LIKE 'historical_%'
         ORDER BY source_table"
    ))
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(backfilled.len(), 2);
    assert!(
        backfilled
            .iter()
            .all(|(source, parent)| { source.is_none() && parent.is_some() })
    );
    assert_ne!(
        backfilled[0].1, backfilled[1].1,
        "each historical direct attempt needs its own reset-safe fence namespace"
    );

    let column_default: Option<String> = sqlx::query_scalar(
        "SELECT column_default
         FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = 'table_reload'
           AND column_name = 'parent_request_id'",
    )
    .bind(&schema)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert!(
        column_default
            .as_deref()
            .is_some_and(|default| default.contains("gen_random_uuid")),
        "legacy direct INSERTs that omit UUID columns must receive a durable namespace"
    );

    let post_migration: (Option<Uuid>, Option<Uuid>) = sqlx::query_as(&format!(
        "INSERT INTO {schema}.table_reload
           (epoch, source_schema, source_table, flavor)
         VALUES (1, 'public', 'legacy_insert_after_0006', 'reload')
         RETURNING source_request_id, parent_request_id"
    ))
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(post_migration.0, None);
    assert!(post_migration.1.is_some());

    let mut savepoint = Connection::begin(&mut *tx).await.unwrap();
    let error = sqlx::query(&format!(
        "INSERT INTO {schema}.table_reload
           (epoch, source_schema, source_table, flavor, source_request_id, parent_request_id)
         VALUES (1, 'public', 'identityless', 'reload', NULL, NULL)"
    ))
    .execute(&mut *savepoint)
    .await
    .unwrap_err();
    let database = error.as_database_error().expect("Postgres CHECK violation");
    assert_eq!(database.code().as_deref(), Some("23514"));
    assert_eq!(
        database.constraint(),
        Some("table_reload_fence_request_identity")
    );
    savepoint.rollback().await.unwrap();

    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn transactional_ddl_migration_installs_replay_and_sql_audit_columns() {
    let pool = migrated_pool().await;
    for column in ["source_audit_id", "c_ddl_text"] {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'walrus' AND table_name = 'ddl_manifest' AND column_name = $1)",
        )
        .bind(column)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(exists, "walrus.ddl_manifest.{column} must exist");
    }

    let source_id_nullable: String = sqlx::query_scalar(
        "SELECT is_nullable FROM information_schema.columns \
         WHERE table_schema = 'walrus' AND table_name = 'ddl_manifest' \
           AND column_name = 'source_audit_id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(source_id_nullable, "NO");

    let unique_index: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_indexes \
         WHERE schemaname = 'walrus' AND tablename = 'ddl_manifest' \
           AND indexname = 'ddl_manifest_source_audit_idx' \
           AND indexdef LIKE 'CREATE UNIQUE INDEX%')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(unique_index);
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

#[tokio::test]
async fn markerless_upgrade_attempts_fail_and_purge_only_their_files() {
    let pool = migrated_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let epoch = 9_008_001_i64;

    let markerless: i64 = sqlx::query_scalar(
        "INSERT INTO walrus.table_reload
           (epoch, source_schema, source_table, flavor, status, chunk_no,
            first_lsn, final_lsn, schema_version)
         VALUES ($1, 'public', 'markerless', 'reload', 'export_complete', 1,
                 '0/10'::pg_lsn, '0/20'::pg_lsn, 1)
         RETURNING reload_id",
    )
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let fenced: i64 = sqlx::query_scalar(
        "INSERT INTO walrus.table_reload
           (epoch, source_schema, source_table, flavor, status, chunk_no,
            start_lsn, first_lsn, final_lsn, schema_version)
         VALUES ($1, 'public', 'fenced', 'reload', 'export_complete', 1,
                 '0/08'::pg_lsn, '0/10'::pg_lsn, '0/20'::pg_lsn, 1)
         RETURNING reload_id",
    )
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await
    .unwrap();

    for (reload_id, table) in [(markerless, "markerless"), (fenced, "fenced")] {
        sqlx::query(
            "INSERT INTO walrus.file_manifest
               (epoch, source_schema, source_table, s3_uri, kind, row_count,
                lsn_start, lsn_end, schema_version, reload_id)
             VALUES ($1, 'public', $2, $3, 'reload', 1,
                     '0/10'::pg_lsn, '0/10'::pg_lsn, 1, $4)",
        )
        .bind(epoch)
        .bind(table)
        .bind(format!("s3://walrus/upgrade/{table}.parquet"))
        .bind(reload_id)
        .execute(&mut *tx)
        .await
        .unwrap();
    }

    sqlx::raw_sql(include_str!(
        "../../../migrations/control/0008_retire_markerless_reload.sql"
    ))
    .execute(&mut *tx)
    .await
    .expect("0008 upgrade cleanup applies to an existing v7-shaped fixture");

    let markerless_state: (String, Option<String>) =
        sqlx::query_as("SELECT status, error FROM walrus.table_reload WHERE reload_id = $1")
            .bind(markerless)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(markerless_state.0, "failed");
    assert!(
        markerless_state
            .1
            .as_deref()
            .unwrap_or_default()
            .contains("markerless export")
    );
    let markerless_files: i64 =
        sqlx::query_scalar("SELECT count(*) FROM walrus.file_manifest WHERE reload_id = $1")
            .bind(markerless)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(markerless_files, 0, "unsafe baseline files are purged");

    let fenced_state: String =
        sqlx::query_scalar("SELECT status FROM walrus.table_reload WHERE reload_id = $1")
            .bind(fenced)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(fenced_state, "export_complete");
    let fenced_files: i64 =
        sqlx::query_scalar("SELECT count(*) FROM walrus.file_manifest WHERE reload_id = $1")
            .bind(fenced)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(
        fenced_files, 1,
        "already-fenced attempts remain publishable"
    );

    tx.rollback().await.unwrap();
}
