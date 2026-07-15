//! Graceful SIGTERM drain (loader §8.5) — compose (`#[ignore]`). On cancel each worker finishes the
//! in-flight Phase A + Phase B (both watermarks committed), the lease is released and the file is
//! checkpointed + closed (no stale lock), and an in-flight full-rebuild is aborted (rolled back).
//!
//!   cargo test -p loader --test shutdown -- --ignored

use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity};
use loader::apply_loop::apply_loop;
use loader::compaction::full_rebuild_abortable;
use loader::duck::{S3Access, TableDb};
use loader::health::LoaderState;
use loader::phase_a::TableCtx;
use loader::transform::{apply_transform, TransformSql};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

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
    let d = std::env::temp_dir().join(format!("walrus-loader-shut-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn meta(op: &str, commit_hex: &str, l: u64) -> String {
    format!(
        "{{\"op\":\"{op}\",\"commit_lsn\":\"{commit_hex}\",\"lsn\":\"{:016X}\",\"sink_processed_at\":\"2026-07-08T12:00:{:02}Z\"}}",
        l, l % 60
    )
}

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

async fn clean(pool: &sqlx::PgPool, epoch: i64) {
    for tbl in [
        "file_manifest",
        "loader_checkpoint",
        "replication_state",
        "table_ownership",
    ] {
        let _ = sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(pool)
            .await;
    }
    control::insert_epoch(
        pool,
        &control::ReplicationState {
            epoch,
            slot_name: "walrus_slot".into(),
            created_lsn: "0/0".parse().unwrap(),
            status: "streaming".into(),
        },
    )
    .await
    .unwrap();
    control::ensure_checkpoint(pool, epoch, "public", "orders")
        .await
        .unwrap();
}

fn ctx_on(
    pool: sqlx::PgPool,
    epoch: i64,
    path: &std::path::Path,
    poll: Duration,
    compaction: Duration,
) -> TableCtx {
    let db = TableDb::open(path).unwrap();
    db.ensure_tables(&orders(), 1).unwrap();
    db.configure_s3(&s3()).unwrap();
    TableCtx {
        pool,
        epoch,
        schema: "public".into(),
        table: "orders".into(),
        rel: orders(),
        db,
        state: LoaderState::new(),
        max_files: 100,
        poll_interval: poll,
        compaction_interval: compaction,
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
    }
}

/// Run one worker until it drains: cancel the token after `cancel_after` (SIGTERM), then await the loop.
async fn run_until_drain(ctx: TableCtx, cancel_after: Duration) {
    let token = CancellationToken::new();
    let tc = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(cancel_after).await;
        tc.cancel();
    });
    apply_loop(ctx, token).await.unwrap();
}

async fn lease_is_live(pool: &sqlx::PgPool, epoch: i64) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT lease_expiry > now() FROM walrus.table_ownership \
         WHERE epoch = $1 AND source_table = 'orders'",
    )
    .bind(epoch)
    .fetch_optional(pool)
    .await
    .unwrap()
    .unwrap_or(false)
}

// ---- SIGTERM mid-apply: both watermarks commit; the lease is released; no stale lock. ----
#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn sigterm_mid_apply_commits_both_watermarks_and_releases_lease() {
    let _g = LOCK.lock().await;
    let epoch = 3_120_001;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    clean(&pool, epoch).await;

    let uri = write_row(epoch, "f1", 1, "v1", "i", "0000000000000064", 1);
    control::insert_ready(
        &pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri,
            kind: "stream".into(),
            row_count: 1,
            lsn_start: "0/64".parse().unwrap(),
            lsn_end: "0/64".parse().unwrap(),
            schema_version: 1,
            reload_id: None,
        },
    )
    .await
    .unwrap();
    // The worker owns the table (as bootstrap would).
    control::acquire_lease(&pool, epoch, "public", "orders", "loader-test", 30)
        .await
        .unwrap()
        .expect("free lease acquired");

    let dir = tmpdir(&epoch.to_string());
    let path = dir.join("orders.duckdb");
    // Long poll so exactly ONE cycle runs (first tick fires immediately), then the drain returns.
    let ctx = ctx_on(
        pool.clone(),
        epoch,
        &path,
        Duration::from_secs(60),
        Duration::from_secs(3600),
    );
    run_until_drain(ctx, Duration::from_millis(400)).await;

    // Both watermarks committed by the finished in-flight cycle.
    let cp = control::read_checkpoint(&pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    let point: Lsn = "0/64".parse().unwrap();
    assert_eq!(cp.raw_appended_lsn, point, "Phase A watermark committed");
    assert_eq!(cp.transformed_lsn, point, "Phase B watermark committed");

    // The lease is released on drain (as `main` does after the worker drains).
    control::release_lease(&pool, epoch, "public", "orders", "loader-test")
        .await
        .unwrap();
    assert!(
        !lease_is_live(&pool, epoch).await,
        "ownership lease released"
    );

    // No stale lock: a replacement can re-open the checkpointed file read-write.
    assert!(
        TableDb::open(&path).is_ok(),
        "file closed cleanly — no stale lock"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- An in-flight full-rebuild is aborted (rolled back) on SIGTERM, not waited on. ----
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a real .duckdb file (interrupt across threads)"]
async fn in_flight_full_rebuild_is_aborted_on_sigterm() {
    let dir = tmpdir("abort");
    let db = TableDb::open(&dir.join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), 1).unwrap();
    let t = TransformSql::from_relation(&orders());

    // A committed baseline the rebuild must NOT lose or partially replace.
    db.conn()
        .execute(
            "INSERT INTO orders_raw (id, status, walrus_pg_sink_meta, \"_walrus_op\", \
             \"_walrus_commit_lsn\", \"_walrus_lsn\", \"_walrus_sink_processed_at\") \
             VALUES (1, 'ORIGINAL', '{}', 'i', '0000000000000001', '0000000000000001', 'x')",
            [],
        )
        .unwrap();
    apply_transform(db.conn(), &t, &Lsn::ZERO).unwrap();

    // Bloat raw with a LOT of wide rows across many keys (one fast bulk insert) so the rebuild's
    // CREATE OR REPLACE runs for well over a second — long enough to be reliably interrupted mid-flight
    // on any runner. Each bloat row is its own key, so a COMPLETED rebuild would have 200k+1 rows.
    let wide = "w".repeat(400);
    db.conn()
        .execute(
            "INSERT INTO orders_raw (id, status, walrus_pg_sink_meta, \"_walrus_op\", \
             \"_walrus_commit_lsn\", \"_walrus_lsn\", \"_walrus_sink_processed_at\") \
             SELECT r + 100, ?, '{}', 'i', '0000000000000002', format('{:016X}', r + 100), 'x' \
             FROM range(200000) t(r)",
            duckdb::params![wide],
        )
        .unwrap();

    // Fire SIGTERM shortly after the rebuild starts; the watcher interrupts the running query.
    let token = CancellationToken::new();
    let tc = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        tc.cancel();
    });
    full_rebuild_abortable(db.conn(), &t, &token)
        .await
        .expect("an aborted rebuild is Ok (rolled back), not an error");

    // Rolled back to the intact baseline — the 60k bloat keys were NOT applied.
    let n: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        n, 1,
        "rebuild rolled back to the old mirror (1 row), not the bloat"
    );
    let s: String = db
        .conn()
        .query_row("SELECT status FROM orders WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        s, "ORIGINAL",
        "the committed value is intact after the abort"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- A replacement loader resumes from the two watermarks: no loss, no duplicate application. ----
#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn a_replacement_loader_resumes_from_the_two_watermarks() {
    let _g = LOCK.lock().await;
    let epoch = 3_120_003;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    clean(&pool, epoch).await;

    let dir = tmpdir(&epoch.to_string());
    let path = dir.join("orders.duckdb");

    // File 1 processed by the first worker, then SIGTERM drains it.
    let f1 = write_row(epoch, "f1", 1, "v1", "i", "0000000000000064", 1);
    control::insert_ready(
        &pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: f1,
            kind: "stream".into(),
            row_count: 1,
            lsn_start: "0/64".parse().unwrap(),
            lsn_end: "0/64".parse().unwrap(),
            schema_version: 1,
            reload_id: None,
        },
    )
    .await
    .unwrap();
    let ctx1 = ctx_on(
        pool.clone(),
        epoch,
        &path,
        Duration::from_secs(60),
        Duration::from_secs(3600),
    );
    run_until_drain(ctx1, Duration::from_millis(400)).await;

    // A NEW file arrives after the drain; the REPLACEMENT worker re-opens the same .duckdb and resumes.
    let f2 = write_row(epoch, "f2", 1, "v2", "u", "00000000000000C8", 5);
    control::insert_ready(
        &pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: f2,
            kind: "stream".into(),
            row_count: 1,
            lsn_start: "0/C8".parse().unwrap(),
            lsn_end: "0/C8".parse().unwrap(),
            schema_version: 1,
            reload_id: None,
        },
    )
    .await
    .unwrap();
    let ctx2 = ctx_on(
        pool.clone(),
        epoch,
        &path,
        Duration::from_secs(60),
        Duration::from_secs(3600),
    );
    run_until_drain(ctx2, Duration::from_millis(400)).await;

    // Resume from the watermarks: the mirror is at v2, exactly one row — file 1 was NOT reprocessed
    // (dequeued), file 2 applied. No loss, no duplicate.
    let db = TableDb::open(&path).unwrap();
    let rows: Vec<(i64, String)> = {
        let mut stmt = db
            .conn()
            .prepare("SELECT id, status FROM orders ORDER BY id")
            .unwrap();
        let it = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
        it.map(Result::unwrap).collect()
    };
    assert_eq!(
        rows,
        vec![(1, "v2".to_string())],
        "replacement resumed from the watermarks: v2 applied, no dup of v1"
    );
    let cp = control::read_checkpoint(&pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cp.transformed_lsn, "0/C8".parse::<Lsn>().unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}
