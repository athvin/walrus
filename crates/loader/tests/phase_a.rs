//! Phase A against compose (`#[ignore]` — needs control PG + MinIO). A seeded `ready` Parquet is
//! claimed and appended **verbatim** to `<table>_raw` (meta intact + op/commit_lsn/lsn/sink_processed_at
//! promoted), the watermark advances and the queue row is deleted in one control txn, and a replay of
//! the same file appends **zero** rows. The fixture Parquet is written by DuckDB itself.
//!
//!   cargo test -p loader --test phase_a -- --ignored

use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity};
use loader::duck::{S3Access, TableDb};
use loader::health::LoaderState;
use loader::phase_a::{run_phase_a, TableCtx};
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
    let d = std::env::temp_dir().join(format!("walrus-loader-pa-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn write_fixture(epoch: i64) -> String {
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
        "CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_pg_sink_meta VARCHAR);
         INSERT INTO fixture VALUES
           (1, 'a', '{\"op\":\"Insert\",\"commit_lsn\":\"0000000000000064\",\"lsn\":\"0000000000000064\",\"sink_processed_at\":\"2026-07-07T12:00:00Z\"}'),
           (2, 'b', '{\"op\":\"Insert\",\"commit_lsn\":\"0000000000000064\",\"lsn\":\"0000000000000065\",\"sink_processed_at\":\"2026-07-07T12:00:01Z\"}');",
    )
    .unwrap();
    let uri = format!("s3://walrus/{epoch}/public/orders/fixture-{epoch}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

async fn seed_manifest(pool: &sqlx::PgPool, epoch: i64, uri: &str) {
    control::insert_ready(
        pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: uri.into(),
            kind: "stream".into(),
            row_count: 2,
            lsn_start: "0/64".parse().unwrap(),
            lsn_end: "0/64".parse().unwrap(),
            schema_version: 1,
            reload_id: None,
        },
    )
    .await
    .unwrap();
}

/// Fresh control state + an owned `TableCtx` (DuckDB in a temp dir).
async fn setup(epoch: i64) -> (TableCtx, std::path::PathBuf) {
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
    };
    (ctx, dir)
}

fn raw_count(ctx: &TableCtx) -> i64 {
    ctx.db
        .conn()
        .query_row("SELECT count(*) FROM orders_raw", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn appends_rows_verbatim_with_promoted_columns_and_meta_intact() {
    let _g = LOCK.lock().await;
    let epoch = 3_200_001;
    let uri = write_fixture(epoch);
    let (ctx, dir) = setup(epoch).await;
    seed_manifest(&ctx.pool, epoch, &uri).await;

    let lsn = run_phase_a(&ctx).await.unwrap();
    assert_eq!(lsn, Some("0/64".parse().unwrap()));
    assert_eq!(raw_count(&ctx), 2, "both rows appended verbatim");

    let (op, meta, promoted_lsn): (String, String, String) = ctx
        .db
        .conn()
        .query_row(
            "SELECT _walrus_op, walrus_pg_sink_meta, _walrus_lsn FROM orders_raw WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(op, "Insert", "op promoted from the meta");
    assert!(
        meta.contains("\"op\":\"Insert\""),
        "walrus_pg_sink_meta kept intact"
    );
    assert_eq!(
        promoted_lsn, "0000000000000064",
        "lsn promoted (sortable 16-hex)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn advances_raw_watermark_and_deletes_the_claimed_manifest_rows() {
    let _g = LOCK.lock().await;
    let epoch = 3_200_002;
    let uri = write_fixture(epoch);
    let (ctx, dir) = setup(epoch).await;
    seed_manifest(&ctx.pool, epoch, &uri).await;

    run_phase_a(&ctx).await.unwrap();

    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cp.raw_appended_lsn,
        "0/64".parse::<Lsn>().unwrap(),
        "watermark = max(lsn_end)"
    );
    let remaining = control::claim_ready(&ctx.pool, epoch, "public", "orders", 100)
        .await
        .unwrap();
    assert!(
        remaining.is_empty(),
        "claimed manifest rows deleted in the same control txn"
    );
    let mirror: i64 = ctx
        .db
        .conn()
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mirror, 0, "Phase A never writes the mirror");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn re_running_the_same_file_appends_zero_rows() {
    let _g = LOCK.lock().await;
    let epoch = 3_200_003;
    let uri = write_fixture(epoch);
    let (ctx, dir) = setup(epoch).await;

    seed_manifest(&ctx.pool, epoch, &uri).await;
    run_phase_a(&ctx).await.unwrap();
    assert_eq!(raw_count(&ctx), 2);

    seed_manifest(&ctx.pool, epoch, &uri).await;
    run_phase_a(&ctx).await.unwrap();
    assert_eq!(
        raw_count(&ctx),
        2,
        "ON CONFLICT DO NOTHING on the composite PK → zero new rows"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn pause_withholds_claims_and_lifts_on_failed() {
    let _g = LOCK.lock().await;
    let epoch = 3_200_004;
    let uri = write_fixture(epoch);
    let (ctx, dir) = setup(epoch).await;
    seed_manifest(&ctx.pool, epoch, &uri).await;
    sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
        .bind(epoch)
        .execute(&ctx.pool)
        .await
        .unwrap();

    // A live rebuild-flavor reload: Phase A must treat the table as PAUSED, not idle (PR 6.6).
    let reload_id = control::reload::request(
        &ctx.pool,
        epoch,
        "public",
        "orders",
        control::reload::ReloadFlavor::Reload,
    )
    .await
    .unwrap();

    // Paused: nothing claimed or appended, the backlog stays ready, the frontier is frozen (no
    // rewind, no CHECK violation — it simply does not move), and the once-per-pause latch holds
    // exactly this reload_id.
    assert_eq!(run_phase_a(&ctx).await.unwrap(), None);
    assert_eq!(raw_count(&ctx), 0, "nothing appended while paused");
    assert!(
        control::max_ready_lsn_end(&ctx.pool, epoch, "public", "orders")
            .await
            .unwrap()
            .is_some(),
        "the ready backlog accumulates (the lag gauge SHOULD grow — PR 6.11)"
    );
    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cp.raw_appended_lsn, Lsn::ZERO, "frontier frozen at W");
    assert_eq!(
        *ctx.pause_logged.lock().unwrap(),
        Some(reload_id),
        "the pause is latched (logged once)"
    );

    // A second poll changes nothing — same latch value means no re-log.
    assert_eq!(run_phase_a(&ctx).await.unwrap(), None);
    assert_eq!(*ctx.pause_logged.lock().unwrap(), Some(reload_id));

    // `failed` lifts the pause: the backlog drains and the latch clears.
    control::reload::claim_requested(&ctx.pool, epoch, "sink-t", 60, 10)
        .await
        .unwrap();
    let mut conn = ctx.pool.acquire().await.unwrap();
    control::reload::fail(&mut conn, reload_id, "demo")
        .await
        .unwrap();
    drop(conn);
    let lsn = run_phase_a(&ctx).await.unwrap();
    assert_eq!(lsn, Some("0/64".parse().unwrap()));
    assert_eq!(
        raw_count(&ctx),
        2,
        "the paused backlog drained after the lift"
    );
    assert_eq!(
        *ctx.pause_logged.lock().unwrap(),
        None,
        "the latch clears when claiming resumes"
    );

    sqlx::query("DELETE FROM walrus.table_reload WHERE epoch = $1")
        .bind(epoch)
        .execute(&ctx.pool)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
