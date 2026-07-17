#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Snapshot/stream boundary through the transform (loader §7, architecture §1.7) — compose (`#[ignore]`).
//! The loader has **no special snapshot mode**: `kind='snapshot'` files append into `<table>_raw` like
//! any `ready` file, and the transform collapses the overlap by `(commit_lsn, lsn)`. Two edges proven
//! here: (1) an overlapping stream change out-ranks the snapshot row, and (2) equal-`lsn_end` snapshot
//! files **split across loader batches** are all applied — none skipped by the watermark. A `lsn_end`-only
//! watermark filter (`lsn_end > raw_appended_lsn`) would drop the equal-`lsn_end` snapshot files; the
//! claim is `ORDER BY lsn_end, id` + queue-delete, and Phase B's `>=` bound + the PR 3.7 guard close the
//! boundary key.
//!
//!   cargo test -p loader --test snapshot_boundary -- --ignored

use common::{PgColumn, PgRelation, ReplicaIdentity};
use loader::duck::{S3Access, TableDb};
use loader::health::LoaderState;
use loader::phase_a::{run_phase_a, TableCtx};
use loader::phase_b::run_phase_b;
use std::time::Duration;

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

fn s3() -> S3Access {
    S3Access {
        endpoint: "localhost:9000".into(),
        region: "us-east-1".into(),
        access_key_id: "minioadmin".into(),
        secret_access_key: "minioadmin".into(),
        use_ssl: false,
    }
}

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, is_key: bool| PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("walrus-loader-snap-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn meta(op: &str, commit_hex: &str, l: u64) -> String {
    format!(
        "{{\"op\":\"{op}\",\"commit_lsn\":\"{commit_hex}\",\"lsn\":\"{:016X}\",\"sink_processed_at\":\"2026-07-07T12:00:{:02}Z\"}}",
        l, l % 60
    )
}

/// Write a single-row (id, status, walrus_pg_sink_meta) Parquet fixture to S3.
fn write_row(
    epoch: i64,
    tag: &str,
    id: i64,
    status: &str,
    op: &str,
    commit_hex: &str,
    l: u64,
) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    let a = s3();
    w.execute_batch(&format!(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='{}'; SET s3_endpoint='{}'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='{}'; SET s3_secret_access_key='{}';",
        a.region, a.endpoint, a.access_key_id, a.secret_access_key
    ))
    .unwrap();
    w.execute_batch(
        "CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_pg_sink_meta VARCHAR);",
    )
    .unwrap();
    w.execute(
        "INSERT INTO fixture VALUES (?, ?, ?)",
        duckdb::params![id, status, meta(op, commit_hex, l)],
    )
    .unwrap();
    let uri = format!("s3://walrus/{epoch}/public/orders/{tag}-{epoch}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

async fn insert_file(pool: &sqlx::PgPool, epoch: i64, uri: String, kind: &str, lsn_end: &str) {
    control::insert_ready(
        pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri,
            kind: kind.into(),
            row_count: 1,
            lsn_start: lsn_end.parse().unwrap(),
            lsn_end: lsn_end.parse().unwrap(),
            schema_version: 1,
            reload_id: None,
        },
    )
    .await
    .unwrap();
}

async fn setup(epoch: i64, max_files: i64) -> (TableCtx, std::path::PathBuf) {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    for tbl in ["file_manifest", "loader_checkpoint", "replication_state"] {
        let _ = sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(&pool)
            .await;
    }
    control::insert_epoch(
        &pool,
        &control::ReplicationState {
            epoch,
            slot_name: "walrus_slot".into(),
            created_lsn: "0/64".parse().unwrap(), // consistent_point
            status: "streaming".into(),
        },
    )
    .await
    .unwrap();
    control::ensure_checkpoint(&pool, epoch, "public", "orders")
        .await
        .unwrap();

    let dir = tmpdir(&epoch.to_string());
    let db = TableDb::open(&dir.join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), 1).unwrap();
    db.configure_s3(&s3()).unwrap();
    let ctx = TableCtx {
        pool,
        epoch,
        schema: "public".into(),
        table: "orders".into(),
        rel: orders(),
        db,
        state: LoaderState::new(),
        max_files,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
        resync_ids: Default::default(),
    };
    (ctx, dir)
}

fn mirror(ctx: &TableCtx) -> Vec<(i64, String)> {
    let conn = ctx.db.conn();
    let mut stmt = conn
        .prepare("SELECT id, status FROM orders ORDER BY id")
        .unwrap();
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    rows.map(Result::unwrap).collect()
}

/// A snapshot load then an OVERLAPPING stream change on the same PK → the mirror ends at the stream
/// value (`commit_lsn > consistent_point` out-ranks the snapshot row). One code path, zero dupes.
#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn snapshot_then_overlapping_stream_yields_stream_value() {
    let _g = LOCK.lock().await;
    let epoch = 3_105_001;
    let (ctx, dir) = setup(epoch, 100).await;

    // Snapshot file (commit_lsn = consistent_point 0x64) then an overlapping stream update (0xC8).
    let snap = write_row(epoch, "snap", 1, "snap", "i", "0000000000000064", 1);
    insert_file(&ctx.pool, epoch, snap, "snapshot", "0/64").await;
    let stream = write_row(epoch, "stream", 1, "streamed", "u", "00000000000000C8", 5);
    insert_file(&ctx.pool, epoch, stream, "stream", "0/C8").await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(
        mirror(&ctx),
        vec![(1, "streamed".to_string())],
        "the overlapping stream change wins; zero loss, zero dupes"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Two equal-`lsn_end` snapshot files, one per loader batch (`max_files=1`), must BOTH land — none
/// skipped by the watermark. The second file's `commit_lsn == transformed_lsn` after the first cycle;
/// only Phase B's `>=` bound + the PR 3.7 guard apply it.
#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn equal_lsn_end_snapshot_files_split_across_batches_all_applied() {
    let _g = LOCK.lock().await;
    let epoch = 3_105_002;
    let (ctx, dir) = setup(epoch, 1).await; // max_files=1 forces the split across batches

    // Two snapshot files at the SAME lsn_end (= consistent_point 0/64), distinct keys.
    let f1 = write_row(epoch, "snapA", 1, "A", "i", "0000000000000064", 1);
    insert_file(&ctx.pool, epoch, f1, "snapshot", "0/64").await;
    let f2 = write_row(epoch, "snapB", 2, "B", "i", "0000000000000064", 2);
    insert_file(&ctx.pool, epoch, f2, "snapshot", "0/64").await;

    // Cycle 1: claims + applies file 1 (key A), transformed_lsn reaches the consistent_point.
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    // Cycle 2: claims + applies file 2 (key B) — commit_lsn == transformed_lsn; the `>=` bound keeps it.
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(
        mirror(&ctx),
        vec![(1, "A".to_string()), (2, "B".to_string())],
        "BOTH equal-lsn_end snapshot files applied — none skipped by the watermark"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
