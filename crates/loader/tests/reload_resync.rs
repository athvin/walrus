//! The `resync` flavor against compose (`#[ignore]` — needs control PG + MinIO). Unlike `reload`
//! (clear + rebuild, PR 6.7), `resync` merges chunks over the LIVE mirror: no pause, no
//! `CREATE OR REPLACE`, no purge, no meta latch, raw history preserved. It repairs stale and
//! missing rows through the ordinary Phase A/B path — but **never removes phantoms** (a row that
//! drifted into the mirror and no longer exists upstream is in no chunk and gets no delete). That
//! caveat is the flavor's defining property, asserted here so a future regression that killed the
//! phantom would fail loudly (reload H3).
//!
//!   cargo test -p loader --test reload_resync -- --ignored --test-threads=1

use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity};
use control::reload::{self, ReloadFlavor};
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
    let d = std::env::temp_dir().join(format!("walrus-loader-rs-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn write_rows(epoch: i64, name: &str, rows: &[(i32, &str, &str, &str, &str)]) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    let a = s3();
    w.execute_batch(&format!(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='{}'; SET s3_endpoint='{}'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='{}'; SET s3_secret_access_key='{}'; \
         CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_pg_sink_meta VARCHAR);",
        a.region, a.endpoint, a.access_key_id, a.secret_access_key
    ))
    .unwrap();
    for (id, status, op, commit, lsn) in rows {
        w.execute_batch(&format!(
            "INSERT INTO fixture VALUES ({id}, '{status}', \
             '{{\"op\":\"{op}\",\"commit_lsn\":\"{commit}\",\"lsn\":\"{lsn}\",\
               \"sink_processed_at\":\"2026-07-15T12:00:00.{lsn}Z\"}}');"
        ))
        .unwrap();
    }
    let uri = format!("s3://walrus/{epoch}/public/orders/{name}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

async fn seed_file(
    pool: &sqlx::PgPool,
    epoch: i64,
    uri: &str,
    kind: &str,
    lsn_end: &str,
    reload_id: Option<i64>,
) -> i64 {
    control::insert_ready(
        pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri.into(),
            kind: kind.into(),
            row_count: 1,
            lsn_start: lsn_end.parse().unwrap(),
            lsn_end: lsn_end.parse().unwrap(),
            schema_version: 1,
            reload_id,
        },
    )
    .await
    .unwrap()
}

async fn setup(epoch: i64) -> (TableCtx, std::path::PathBuf) {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    for tbl in [
        "file_manifest",
        "loader_checkpoint",
        "replication_state",
        "table_reload",
    ] {
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
            created_lsn: "0/0".parse().unwrap(),
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
        max_files: 100,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
        resync_ids: Default::default(),
    };
    (ctx, dir)
}

/// Walk a `resync` reload through the real transitions to `export_complete` at `first_lsn = l1`.
async fn drained_resync(pool: &sqlx::PgPool, epoch: i64, l1: &str, h: &str) -> i64 {
    let id = reload::request(pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    reload::claim_requested(pool, epoch, "sink-t", 60, 10)
        .await
        .unwrap();
    reload::advance_cursor(
        pool,
        id,
        1,
        &serde_json::json!(["999"]),
        l1.parse::<Lsn>().unwrap(),
        1,
    )
    .await
    .unwrap();
    reload::complete_export(pool, id, h.parse::<Lsn>().unwrap())
        .await
        .unwrap();
    id
}

fn mirror_status(ctx: &TableCtx, id: i32) -> Option<String> {
    ctx.db
        .conn()
        .query_row("SELECT status FROM orders WHERE id = ?", [id], |r| r.get(0))
        .ok()
}

fn mirror_count(ctx: &TableCtx) -> i64 {
    ctx.db
        .conn()
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap()
}

fn raw_has(ctx: &TableCtx, id: i32) -> bool {
    let n: i64 = ctx
        .db
        .conn()
        .query_row("SELECT count(*) FROM orders_raw WHERE id = ?", [id], |r| {
            r.get(0)
        })
        .unwrap();
    n > 0
}

/// Establish a live mirror {1,2,3} via a stream file at 0/50, then update id 2 nowhere yet.
async fn seed_live_mirror(ctx: &TableCtx, epoch: i64) {
    let live = write_rows(
        epoch,
        "live",
        &[
            (1, "v1", "i", "0000000000000050", "0000000000000050"),
            (2, "v2", "i", "0000000000000050", "0000000000000051"),
            (3, "v3", "i", "0000000000000050", "0000000000000052"),
        ],
    );
    seed_file(&ctx.pool, epoch, &live, "stream", "0/50", None).await;
    run_phase_a(ctx).await.unwrap();
    run_phase_b(ctx).await.unwrap();
    assert_eq!(mirror_count(ctx), 3);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn resync_repairs_drift_but_phantoms_survive() {
    let _g = LOCK.lock().await;
    let epoch = 660_001;
    let (ctx, dir) = setup(epoch).await;
    seed_live_mirror(&ctx, epoch).await;

    // Drift the mirror both ways, directly in DuckDB: a MISSING row (delete id 1) and a PHANTOM
    // (insert id 9999 that exists nowhere upstream). Deleting via the source would emit a real
    // tombstone and heal through the stream — that is CDC working, not drift.
    ctx.db
        .conn()
        .execute_batch("DELETE FROM orders WHERE id = 1; INSERT INTO orders (id, status) VALUES (9999, 'phantom');")
        .unwrap();
    assert_eq!(mirror_status(&ctx, 1), None, "id 1 is now missing");
    assert_eq!(mirror_status(&ctx, 9999).as_deref(), Some("phantom"));

    // A resync (L_1 = 0/100) carrying the source truth {1,2,3}; and — concurrently — a stream event
    // at 0/200 updates id 2 to 'newest'. One claim batch: the chunk (0/100) sorts before the stream
    // (0/200), so both transform together and the applied-(commit_lsn,lsn) guard lets the newer
    // stream event beat the chunk's stale L_1 copy (the reason resync is safe over a live table).
    let resync_id = drained_resync(&ctx.pool, epoch, "0/100", "0/100").await;
    let chunk = write_rows(
        epoch,
        "chunk1",
        &[
            (1, "v1", "i", "0000000000000100", "0000000000000100"),
            (2, "v2", "i", "0000000000000100", "0000000000000100"),
            (3, "v3", "i", "0000000000000100", "0000000000000100"),
        ],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(resync_id)).await;
    let post = write_rows(
        epoch,
        "post",
        &[(2, "newest", "u", "0000000000000200", "0000000000000200")],
    );
    seed_file(&ctx.pool, epoch, &post, "stream", "0/200", None).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(
        mirror_status(&ctx, 1).as_deref(),
        Some("v1"),
        "missing row repaired"
    );
    assert_eq!(
        mirror_status(&ctx, 2).as_deref(),
        Some("newest"),
        "the mid-resync stream event beats the chunk's stale stamp (no regression)"
    );
    assert_eq!(mirror_status(&ctx, 3).as_deref(), Some("v3"));
    assert_eq!(
        mirror_status(&ctx, 9999).as_deref(),
        Some("phantom"),
        "THE CAVEAT: resync never removes a phantom — use flavor='reload' for that"
    );
    // No rebuild happened: no meta latch, raw history preserved.
    assert_eq!(
        ctx.db.recorded_reload_id().unwrap(),
        0,
        "resync never writes the reload_id latch"
    );
    assert!(
        raw_has(&ctx, 1) && raw_has(&ctx, 3),
        "chunk rows flowed through raw (preserved)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn resync_never_pauses_the_table() {
    let _g = LOCK.lock().await;
    let epoch = 660_002;
    let (ctx, dir) = setup(epoch).await;
    seed_live_mirror(&ctx, epoch).await;

    // A LIVE (non-terminal) resync — requested → exporting, never driven to completion.
    reload::request(&ctx.pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    reload::claim_requested(&ctx.pool, epoch, "sink-t", 60, 10)
        .await
        .unwrap();

    // A post-`W` stream file arrives while the resync is live. A `reload` would PAUSE the table
    // here (PR 6.6); a `resync` must not — `active_rebuilds` scopes the pause to `flavor='reload'`.
    let post = write_rows(
        epoch,
        "post",
        &[(5, "streamed", "i", "0000000000000200", "0000000000000200")],
    );
    seed_file(&ctx.pool, epoch, &post, "stream", "0/200", None).await;

    let lsn = run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(lsn, Some("0/200".parse().unwrap()), "claimed, not paused");
    assert_eq!(
        mirror_status(&ctx, 5).as_deref(),
        Some("streamed"),
        "the stream kept flowing over the live table during the resync"
    );
    assert_eq!(*ctx.pause_logged.lock(), None, "no pause was ever latched");
    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cp.raw_appended_lsn,
        "0/200".parse().unwrap(),
        "the frontier advanced (no freeze at W)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn resync_chunks_flow_through_raw() {
    let _g = LOCK.lock().await;
    let epoch = 660_003;
    let (ctx, dir) = setup(epoch).await;
    seed_live_mirror(&ctx, epoch).await;

    // The open-question decision, pinned: a resync chunk row lands in `<table>_raw` like any file
    // (uniform Phase A path) — raw history is preserved, unlike a rebuild which discards it.
    let resync_id = drained_resync(&ctx.pool, epoch, "0/100", "0/100").await;
    let chunk = write_rows(
        epoch,
        "chunk1",
        &[(7, "from-chunk", "i", "0000000000000100", "0000000000000100")],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(resync_id)).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert!(
        raw_has(&ctx, 7),
        "the resync chunk row is in orders_raw (uniform path)"
    );
    assert!(
        raw_has(&ctx, 1) && raw_has(&ctx, 2) && raw_has(&ctx, 3),
        "the pre-resync stream rows are still in raw — no rebuild discarded them"
    );
    assert_eq!(mirror_status(&ctx, 7).as_deref(), Some("from-chunk"));
    assert_eq!(ctx.db.recorded_reload_id().unwrap(), 0, "no latch");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG)"]
async fn resync_ddl_restart_preserves_the_resync_flavor() {
    let _g = LOCK.lock().await;
    let epoch = 660_004;
    let (ctx, dir) = setup(epoch).await;

    // A mid-resync DDL restart (PR 6.8) reissues the attempt — the successor must stay `resync`
    // (restart_for_ddl copies the flavor via INSERT…SELECT), else a refresh would silently become a
    // rebuild. Driven at the control layer: request → claim → chunk 1 → restart.
    let old = reload::request(&ctx.pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    reload::claim_requested(&ctx.pool, epoch, "sink-t", 60, 10)
        .await
        .unwrap();
    reload::advance_cursor(
        &ctx.pool,
        old,
        1,
        &serde_json::json!(["10"]),
        "0/100".parse().unwrap(),
        1,
    )
    .await
    .unwrap();
    let old_row = reload::get(&ctx.pool, old).await.unwrap().unwrap();

    let mut conn = ctx.pool.acquire().await.unwrap();
    let new_id = reload::restart_for_ddl(&mut conn, &old_row, 2, 3)
        .await
        .unwrap()
        .expect("cap 3 leaves room for the first restart");
    drop(conn);

    let successor = reload::get(&ctx.pool, new_id).await.unwrap().unwrap();
    assert_eq!(
        successor.flavor,
        ReloadFlavor::Resync,
        "the restart kept the resync flavor"
    );
    assert_eq!(successor.restart_count, 1);
    assert_eq!(
        reload::get(&ctx.pool, old).await.unwrap().unwrap().status,
        reload::ReloadStatus::Failed,
        "the predecessor turned terminal"
    );

    sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
        .bind(epoch)
        .execute(&ctx.pool)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
