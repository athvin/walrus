#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Compatibility coverage for the persisted `resync` flavor against compose (`#[ignore]` — needs
//! control PG + MinIO). `resync` now aliases `reload`: it pauses claims, builds a hidden full-table
//! generation, removes phantoms, publishes at H, and then resumes post-H WAL. The enum/database
//! value remains accepted so existing callers and rows do not need a flag migration.
//!
//!   cargo test -p loader --test reload_resync -- --ignored --test-threads=1

use common::{EpochNo, Lsn, PgColumn, PgRelation, ReplicaIdentity};
use control::reload::{self, ReloadFenceIdentity, ReloadFlavor};
use loader::duck::{S3Access, TableDb};
use loader::health::LoaderState;
use loader::phase_a::{TableCtx, run_phase_a};
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

/// A scratch directory for one test's `.duckdb` file. The returned guard deletes it on drop — even
/// when an assertion panics, which a trailing `remove_dir_all` would skip.
fn tmpdir(name: &str) -> tempfile::TempDir {
    let prefix = format!("walrus-loader-rs-{name}-");
    tempfile::Builder::new().prefix(&prefix).tempdir().unwrap()
}

fn write_rows(epoch: EpochNo, name: &str, rows: &[(i32, &str, &str, &str, &str)]) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    let a = s3();
    w.execute_batch(&format!(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='{}'; SET s3_endpoint='{}'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='{}'; SET s3_secret_access_key='{}'; \
         CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_pg_sink_meta VARCHAR);",
        a.region,
        a.endpoint,
        a.access_key_id,
        a.secret_access_key.expose()
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
    epoch: EpochNo,
    uri: &str,
    kind: &str,
    lsn_end: &str,
    reload_id: Option<common::ReloadId>,
) -> i64 {
    control::insert_ready(
        pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri.into(),
            kind: kind.parse::<control::ManifestKind>().unwrap(),
            row_count: 1,
            lsn_start: lsn_end.parse().unwrap(),
            lsn_end: lsn_end.parse().unwrap(),
            schema_version: common::SchemaVersionNo(1),
            reload_id,
        },
    )
    .await
    .unwrap()
    .0
}

async fn setup(epoch: EpochNo) -> (TableCtx, tempfile::TempDir) {
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
            status: control::ReplicationStatus::Streaming,
        },
    )
    .await
    .unwrap();
    control::ensure_checkpoint(&pool, epoch, "public", "orders")
        .await
        .unwrap();
    let dir = tmpdir(&epoch.to_string());
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    db.configure_s3(&s3()).unwrap();
    let ctx = TableCtx {
        pool,
        epoch,
        epoch_rx: loader::epoch::fixed_epoch_watch(epoch),
        schema: "public".into(),
        table: "orders".into(),
        series: "public.orders".into(),
        rel: orders(),
        db,
        state: LoaderState::new(),
        max_files: std::num::NonZeroI64::new(100).unwrap(),
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
    };
    (ctx, dir)
}

/// Walk a `resync`-spelled rebuild through the real transitions to `export_complete`.
async fn drained_resync(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    l1: &str,
    h: &str,
) -> common::ReloadId {
    let id = reload::request(pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    reload::claim_requested(pool, epoch, "sink-t", 60, 10)
        .await
        .unwrap();
    let request_id = reload::get(pool, id)
        .await
        .unwrap()
        .unwrap()
        .parent_request_id
        .expect("direct resync has a durable fence namespace");
    let start_lsn = l1.parse::<Lsn>().unwrap();
    let final_lsn = h.parse::<Lsn>().unwrap();
    let fence = ReloadFenceIdentity {
        request_id: Some(request_id),
        source_schema: "public",
        source_table: "orders",
        schema_version: common::SchemaVersionNo(1),
    };
    reload::record_start_fence(pool, id, start_lsn, fence)
        .await
        .unwrap();
    reload::advance_cursor(
        pool,
        id,
        1,
        &serde_json::json!(["999"]),
        start_lsn,
        common::SchemaVersionNo(1),
    )
    .await
    .unwrap();
    reload::record_end_marker(pool, id, final_lsn, fence)
        .await
        .unwrap();
    reload::complete_export(pool, id, final_lsn).await.unwrap();
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
#[allow(
    clippy::future_not_send,
    reason = "this helper borrows a TableCtx containing a Send + !Sync TableDb across awaits"
)]
async fn seed_live_mirror(ctx: &TableCtx, epoch: EpochNo) {
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
async fn resync_alias_rebuilds_removes_phantoms_and_then_replays_post_h() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(660_001);
    let (ctx, _dir) = setup(epoch).await;
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

    // The full dump at H=0/100 carries source truth {1,2,3}; a later stream event at 0/200 updates
    // id 2. The first pass must stop at H and publish the replacement before a second pass applies
    // the post-H event.
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
        Some("v2"),
        "the first cutover stops exactly at H"
    );
    assert_eq!(mirror_status(&ctx, 3).as_deref(), Some("v3"));
    assert_eq!(
        mirror_status(&ctx, 9999).as_deref(),
        None,
        "the resync alias is a full rebuild, so phantoms are removed"
    );
    assert_eq!(
        ctx.db.recorded_reload_id().unwrap(),
        Some(resync_id),
        "the compatibility spelling records the published generation"
    );

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    assert_eq!(
        mirror_status(&ctx, 2).as_deref(),
        Some("newest"),
        "post-H WAL resumes after publication"
    );
    assert!(
        raw_has(&ctx, 1) && raw_has(&ctx, 3),
        "the published generation contains the full dump raw rows"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn resync_alias_pauses_the_table() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(660_002);
    let (ctx, _dir) = setup(epoch).await;
    seed_live_mirror(&ctx, epoch).await;

    // A live (non-terminal) compatibility-spelled rebuild: requested → exporting.
    let reload_id = reload::request(&ctx.pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    reload::claim_requested(&ctx.pool, epoch, "sink-t", 60, 10)
        .await
        .unwrap();

    // A stream file arrives while the export is live. It must remain ready until export_complete.
    let post = write_rows(
        epoch,
        "post",
        &[(5, "streamed", "i", "0000000000000200", "0000000000000200")],
    );
    seed_file(&ctx.pool, epoch, &post, "stream", "0/200", None).await;

    let lsn = run_phase_a(&ctx).await.unwrap();
    assert_eq!(lsn, None, "the compatibility spelling pauses claims");
    assert_eq!(mirror_status(&ctx, 5), None, "the stream remains unapplied");
    assert_eq!(ctx.pause_logged.get(), Some(reload_id));
    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cp.raw_appended_lsn,
        "0/50".parse().unwrap(),
        "the frontier remains frozen until export_complete"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn resync_chunks_build_a_hidden_generation_before_cutover() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(660_003);
    let (ctx, _dir) = setup(epoch).await;
    seed_live_mirror(&ctx, epoch).await;

    // The resync-spelled dump goes to a hidden generation while the old mirror remains public.
    let resync_id = drained_resync(&ctx.pool, epoch, "0/100", "0/100").await;
    let chunk = write_rows(
        epoch,
        "chunk1",
        &[(7, "from-chunk", "i", "0000000000000100", "0000000000000100")],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(resync_id)).await;

    run_phase_a(&ctx).await.unwrap();
    assert_eq!(
        mirror_status(&ctx, 7),
        None,
        "live is untouched before H cutover"
    );
    assert_eq!(mirror_count(&ctx), 3);
    run_phase_b(&ctx).await.unwrap();

    assert!(
        raw_has(&ctx, 7),
        "the dump row is in the published generation's raw table"
    );
    assert!(
        !raw_has(&ctx, 1) && !raw_has(&ctx, 2) && !raw_has(&ctx, 3),
        "publishing the rebuilt generation replaces old raw history"
    );
    assert_eq!(mirror_status(&ctx, 7).as_deref(), Some("from-chunk"));
    assert_eq!(mirror_count(&ctx), 1);
    assert_eq!(ctx.db.recorded_reload_id().unwrap(), Some(resync_id));
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG)"]
async fn resync_ddl_restart_preserves_the_resync_flavor() {
    let _g = LOCK.lock().await;
    let epoch = EpochNo(660_004);
    let (ctx, _dir) = setup(epoch).await;

    // A DDL restart preserves the stored compatibility spelling even though both values now select
    // the same rebuild behavior. Driven at the control layer: request → claim → chunk 1 → restart.
    let old = reload::request(&ctx.pool, epoch, "public", "orders", ReloadFlavor::Resync)
        .await
        .unwrap();
    reload::claim_requested(&ctx.pool, epoch, "sink-t", 60, 10)
        .await
        .unwrap();
    let request_id = reload::get(&ctx.pool, old)
        .await
        .unwrap()
        .unwrap()
        .parent_request_id
        .expect("direct resync has a durable fence namespace");
    let start_lsn = "0/100".parse().unwrap();
    reload::record_start_fence(
        &ctx.pool,
        old,
        start_lsn,
        ReloadFenceIdentity {
            request_id: Some(request_id),
            source_schema: "public",
            source_table: "orders",
            schema_version: common::SchemaVersionNo(1),
        },
    )
    .await
    .unwrap();
    reload::advance_cursor(
        &ctx.pool,
        old,
        1,
        &serde_json::json!(["10"]),
        start_lsn,
        common::SchemaVersionNo(1),
    )
    .await
    .unwrap();
    let old_row = reload::get(&ctx.pool, old).await.unwrap().unwrap();

    let mut conn = ctx.pool.acquire().await.unwrap();
    let new_id = reload::restart_for_ddl(&mut conn, &old_row, common::SchemaVersionNo(2), 3)
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
}
