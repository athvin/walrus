#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! The rebuild trigger against compose (`#[ignore]` — needs control PG + MinIO). The first
//! claimed `kind='reload'` file with `reload_id >` the `_walrus_meta` latch replaces both tables
//! at the file's schema_version, clears the quarantine, purges superseded pending rows, sets the
//! latch, and then ordinary Phase A/B replays chunks + post-`W` stream files in `(lsn_end, id)`
//! order — converging the mirror to the source (phantoms dead, mid-export updates win, deletes
//! no-op through the MERGE's guard). Stale-id files retire unapplied (latest wins, H9).
//!
//!   cargo test -p loader --test reload_rebuild -- --ignored --test-threads=1

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
    let d = std::env::temp_dir().join(format!("walrus-loader-rr-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Write a `(id, status, op, commit_lsn, lsn)` Parquet to MinIO. Ops use the wire values
/// (`i`/`u`/`d`); LSNs are the sortable 16-hex text the sink emits.
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
    seed_file_v(pool, epoch, uri, kind, lsn_end, reload_id, 1).await
}

/// Like [`seed_file`] but at an explicit `schema_version` — a file at a version NEWER than the
/// loader's would trigger a reconcile (PR 6.12's skip path).
async fn seed_file_v(
    pool: &sqlx::PgPool,
    epoch: i64,
    uri: &str,
    kind: &str,
    lsn_end: &str,
    reload_id: Option<i64>,
    schema_version: i64,
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
            schema_version,
            reload_id,
        },
    )
    .await
    .unwrap()
}

/// Fresh control state + an owned `TableCtx` (DuckDB in a temp dir).
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

/// Walk a reload of `flavor` through the REAL transitions to `export_complete` at `first_lsn = l1`
/// — the state the loader sees once the sink's export drains (for a rebuild-flavor reload the pause
/// is lifted and chunks are claimable; a resync never paused anything).
async fn drained_reload(
    pool: &sqlx::PgPool,
    epoch: i64,
    l1: &str,
    h: &str,
    flavor: ReloadFlavor,
) -> i64 {
    let id = reload::request(pool, epoch, "public", "orders", flavor)
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

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn rebuild_converges_mirror_to_source_and_kills_phantoms() {
    let _g = LOCK.lock().await;
    let epoch = 670_001;
    let (ctx, dir) = setup(epoch).await;

    // The OLD world: ids 1,2 streamed normally; then a phantom drifts into the mirror directly.
    let old = write_rows(
        epoch,
        "old",
        &[
            (1, "old", "i", "0000000000000050", "0000000000000050"),
            (2, "b", "i", "0000000000000050", "0000000000000051"),
        ],
    );
    seed_file(&ctx.pool, epoch, &old, "stream", "0/50", None).await;
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    assert_eq!(mirror_count(&ctx), 2);
    ctx.db
        .conn()
        .execute_batch("INSERT INTO orders (id, status) VALUES (9999, 'ghost')")
        .unwrap();

    // The reload: chunks stamped L1 = 0/100 carry the source truth {1:'snap', 2:'b', 3:'c'};
    // a mid-export stream file at 0/200 updates 1 → 'newest' and deletes 2.
    let reload_id = drained_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Reload).await;
    let chunk = write_rows(
        epoch,
        "chunk1",
        &[
            (1, "snap", "i", "0000000000000100", "0000000000000100"),
            (2, "b", "i", "0000000000000100", "0000000000000100"),
            (3, "c", "i", "0000000000000100", "0000000000000100"),
        ],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;
    let post_w = write_rows(
        epoch,
        "postw",
        &[
            (1, "newest", "u", "0000000000000200", "0000000000000200"),
            (2, "", "d", "0000000000000200", "0000000000000201"),
        ],
    );
    seed_file(&ctx.pool, epoch, &post_w, "stream", "0/200", None).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    // Convergence: the phantom is dead (the clear), the mid-export update wins (chunk stamp L_i
    // loses dedup to 0/200), the mid-export delete holds (its winner is 'd' — and a delete for a
    // row the rebuilt mirror never saw would no-op through NOT MATCHED AND op='d').
    assert_eq!(
        mirror_status(&ctx, 9999),
        None,
        "phantom killed by the clear"
    );
    assert_eq!(mirror_status(&ctx, 1).as_deref(), Some("newest"));
    assert_eq!(mirror_status(&ctx, 2), None, "mid-export delete holds");
    assert_eq!(mirror_status(&ctx, 3).as_deref(), Some("c"));
    assert_eq!(mirror_count(&ctx), 2);
    assert_eq!(ctx.db.recorded_reload_id().unwrap(), reload_id);
    let cp = control::read_checkpoint(&ctx.pool, epoch, "public", "orders")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        cp.transformed_lsn,
        "0/200".parse().unwrap(),
        "monotonic, no rewind"
    );

    // Crash-redo: the SAME chunk file re-seeded takes the equal ⇒ append arm — no re-clear (the
    // mirror keeps the newer 0/200 state; the guard makes the chunk's stale copies no-ops).
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    assert_eq!(
        mirror_status(&ctx, 1).as_deref(),
        Some("newest"),
        "no re-clear, no regression"
    );
    assert_eq!(mirror_status(&ctx, 2), None);
    assert_eq!(mirror_count(&ctx), 2);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn superseded_rows_are_purged_and_their_content_discarded() {
    let _g = LOCK.lock().await;
    let epoch = 670_002;
    let (ctx, dir) = setup(epoch).await;

    // One claim batch holds the whole story: a pre-`W` stream file (sorts first), the chunk, a
    // post-`W` stream file. The pre-`W` file is applied into the OLD raw before the chunk fires
    // the trigger — the wasted-but-harmless path — and the trigger then (a) PURGES its manifest
    // row (claimed rows are only deleted at end-of-batch, so it is still visible to the purge)
    // and (b) discards its content with the old raw. The chunk re-covers its commit (C ≤ L₁).
    let pre_w = write_rows(
        epoch,
        "prew",
        &[(
            8,
            "discarded-by-the-clear",
            "i",
            "0000000000000060",
            "0000000000000060",
        )],
    );
    let pre_w_id = seed_file(&ctx.pool, epoch, &pre_w, "stream", "0/60", None).await;

    let reload_id = drained_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Reload).await;
    let chunk = write_rows(
        epoch,
        "chunk1",
        &[(1, "snap", "i", "0000000000000100", "0000000000000100")],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;
    let post_w = write_rows(
        epoch,
        "postw",
        &[(9, "survives", "i", "0000000000000200", "0000000000000200")],
    );
    seed_file(&ctx.pool, epoch, &post_w, "stream", "0/200", None).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    let pre_w_gone: i64 =
        sqlx::query_scalar("SELECT count(*) FROM walrus.file_manifest WHERE id = $1")
            .bind(pre_w_id)
            .fetch_one(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(
        pre_w_gone, 0,
        "the superseded row left the queue at trigger time"
    );
    assert_eq!(mirror_status(&ctx, 1).as_deref(), Some("snap"));
    assert_eq!(
        mirror_status(&ctx, 9).as_deref(),
        Some("survives"),
        "post-`W` applies after the chunks"
    );
    assert_eq!(
        mirror_status(&ctx, 8),
        None,
        "pre-`W` content is discarded with the old raw — the chunks re-cover its commit"
    );
    assert_eq!(mirror_count(&ctx), 2);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn delete_superseded_prunes_by_kind_and_lsn() {
    let _g = LOCK.lock().await;
    let epoch = 670_005;
    let (ctx, dir) = setup(epoch).await;

    // The contract itself (the loop's safety net for LATE-arriving superseded rows too): every
    // non-reload row at lsn_end <= first_lsn goes; the chunk at lsn_end == first_lsn survives its
    // own purge (the kind filter); later stream rows survive.
    let f = write_rows(
        epoch,
        "any",
        &[(1, "x", "i", "0000000000000060", "0000000000000060")],
    );
    let old_stream = seed_file(&ctx.pool, epoch, &f, "stream", "0/60", None).await;
    let boundary_stream = seed_file(&ctx.pool, epoch, &f, "stream", "0/100", None).await;
    let chunk_at_boundary = seed_file(&ctx.pool, epoch, &f, "reload", "0/100", Some(42)).await;
    let newer_stream = seed_file(&ctx.pool, epoch, &f, "stream", "0/200", None).await;

    let purged = control::delete_superseded(
        &ctx.pool,
        epoch,
        "public",
        "orders",
        "0/100".parse().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(purged, 2, "old + boundary stream rows purged");

    let survivors: Vec<i64> =
        sqlx::query_scalar("SELECT id FROM walrus.file_manifest WHERE epoch = $1 ORDER BY id")
            .bind(epoch)
            .fetch_all(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(
        survivors,
        vec![chunk_at_boundary, newer_stream],
        "chunk 1 survives its own purge; post-`W` rows survive"
    );
    assert!(!survivors.contains(&old_stream) && !survivors.contains(&boundary_stream));

    sqlx::query("DELETE FROM walrus.file_manifest WHERE epoch = $1")
        .bind(epoch)
        .execute(&ctx.pool)
        .await
        .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn stale_reload_file_is_skipped_and_retired() {
    let _g = LOCK.lock().await;
    let epoch = 670_003;
    let (ctx, dir) = setup(epoch).await;

    // The .duckdb already rebuilt for a NEWER attempt (PR 6.8's restart hygiene, simulated).
    ctx.db.set_recorded_reload_id(999_999).unwrap();
    let stale = write_rows(
        epoch,
        "stale",
        &[(1, "stale", "i", "0000000000000100", "0000000000000100")],
    );
    let stale_id = seed_file(&ctx.pool, epoch, &stale, "reload", "0/100", Some(5)).await;

    run_phase_a(&ctx).await.unwrap();

    let gone: i64 = sqlx::query_scalar("SELECT count(*) FROM walrus.file_manifest WHERE id = $1")
        .bind(stale_id)
        .fetch_one(&ctx.pool)
        .await
        .unwrap();
    assert_eq!(gone, 0, "the stale file is retired from the queue");
    let raw: i64 = ctx
        .db
        .conn()
        .query_row("SELECT count(*) FROM orders_raw", [], |r| r.get(0))
        .unwrap();
    assert_eq!(raw, 0, "retired UNAPPLIED — DuckDB untouched");
    assert_eq!(
        ctx.db.recorded_reload_id().unwrap(),
        999_999,
        "the latch never regresses (latest wins)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn rebuild_clears_the_lossy_cast_quarantine() {
    let _g = LOCK.lock().await;
    let epoch = 670_004;
    let (ctx, dir) = setup(epoch).await;

    // The PR 3.9 terminal state: a lossy ALTER COLUMN TYPE cast failed; /ready is degraded. (The
    // entry path is ddl_destructive.rs's covered ground; the EXIT is under test here.)
    ctx.state.quarantine();
    assert!(!ctx.state.is_ready() || !ctx.state.is_started());

    let reload_id = drained_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Reload).await;
    let chunk = write_rows(
        epoch,
        "chunk1",
        &[(1, "recovered", "i", "0000000000000100", "0000000000000100")],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    assert!(
        !ctx.state.is_quarantined(),
        "the rebuild is the quarantine's one exit"
    );
    assert_eq!(mirror_status(&ctx, 1).as_deref(), Some("recovered"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn resync_flavor_never_rebuilds_and_merges_over_the_live_mirror() {
    let _g = LOCK.lock().await;
    let epoch = 670_006;
    let (ctx, dir) = setup(epoch).await;

    // A LIVE mirror the resync must NOT clear: ids 1,2 already streamed in. And — to prove the
    // resync arm skips `clear_quarantine` (only a rebuild is that exit) — latch the quarantine.
    let live = write_rows(
        epoch,
        "live",
        &[
            (1, "live", "i", "0000000000000050", "0000000000000050"),
            (2, "keep", "i", "0000000000000050", "0000000000000051"),
        ],
    );
    seed_file(&ctx.pool, epoch, &live, "stream", "0/50", None).await;
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    assert_eq!(mirror_count(&ctx), 2);
    ctx.state.quarantine();

    // A RESYNC reload (H3), drained to export_complete at first_lsn = 0/100. `active_rebuilds`
    // excludes resync, so the loader never paused — the chunk is claimable straight away.
    let reload_id = drained_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Resync).await;

    // A pre-`W` stream file (lsn_end <= first_lsn) that the REBUILD path would `delete_superseded`
    // and discard with the old raw. Under resync it must survive and apply — proof the arm runs no
    // purge. Claimed in the same batch, it sorts before the chunk in (lsn_end, id) order.
    let pre_w = write_rows(
        epoch,
        "prew",
        &[(
            4,
            "kept-no-purge",
            "i",
            "0000000000000060",
            "0000000000000060",
        )],
    );
    seed_file(&ctx.pool, epoch, &pre_w, "stream", "0/60", None).await;

    // The resync chunk stamped L1 = 0/100: merges over the live mirror — updates id 1, adds id 3.
    let chunk = write_rows(
        epoch,
        "chunk1",
        &[
            (1, "resynced", "i", "0000000000000100", "0000000000000100"),
            (3, "new", "i", "0000000000000100", "0000000000000100"),
        ],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    // Never rebuilt: the pre-existing live row survives (a rebuild would have dropped the whole
    // mirror), the chunk merged over it, and the pre-`W` file's content was applied — not purged.
    assert_eq!(
        mirror_status(&ctx, 2).as_deref(),
        Some("keep"),
        "no rebuild — the untouched live row survives"
    );
    assert_eq!(
        mirror_status(&ctx, 1).as_deref(),
        Some("resynced"),
        "the chunk merges over the live mirror"
    );
    assert_eq!(mirror_status(&ctx, 3).as_deref(), Some("new"));
    assert_eq!(
        mirror_status(&ctx, 4).as_deref(),
        Some("kept-no-purge"),
        "resync runs no delete_superseded — the pre-`W` content is NOT discarded"
    );
    assert_eq!(mirror_count(&ctx), 4);

    // No latch (a stale-vs-latest comparison must never fire off a resync chunk — H9) and no
    // quarantine exit (only a rebuild clears it).
    assert_eq!(
        ctx.db.recorded_reload_id().unwrap(),
        0,
        "resync never sets the reload_id latch"
    );
    assert!(
        ctx.state.is_quarantined(),
        "resync is not a quarantine exit — the latch still holds"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn superseded_version_crossing_file_is_skipped_not_reconciled() {
    let _g = LOCK.lock().await;
    let epoch = 670_007;
    let (ctx, dir) = setup(epoch).await;

    // A live mirror {1} at the loader's current schema_version (1).
    let live = write_rows(
        epoch,
        "live",
        &[(1, "live", "i", "0000000000000050", "0000000000000050")],
    );
    seed_file(&ctx.pool, epoch, &live, "stream", "0/50", None).await;
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    // A BLOCKER stream file at a NEWER schema_version (2) — reconciling it would run the DDL path
    // (and on a lossy cast, quarantine). It sits BELOW the reload's first_lsn, so a pending rebuild
    // supersedes it. This is the quarantine-recovery blocker (PR 6.12), simulated without the ddl
    // machinery: the skip happens BEFORE reconcile, so no v2 registry row is needed.
    let blocker = write_rows(
        epoch,
        "blocker",
        &[(8, "blocker", "i", "0000000000000060", "0000000000000060")],
    );
    let blocker_id = seed_file_v(&ctx.pool, epoch, &blocker, "stream", "0/60", None, 2).await;

    // A drained rebuild reload at first_lsn = 0/100 (>= the blocker's 0/60 ⇒ supersedes it).
    let reload_id = drained_reload(&ctx.pool, epoch, "0/100", "0/100", ReloadFlavor::Reload).await;
    let chunk = write_rows(
        epoch,
        "chunk1",
        &[(1, "rebuilt", "i", "0000000000000100", "0000000000000100")],
    );
    seed_file(&ctx.pool, epoch, &chunk, "reload", "0/100", Some(reload_id)).await;

    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();

    // The blocker was SKIPPED: never reconciled (NO quarantine), never appended to raw, and purged
    // by the rebuild's delete_superseded. The rebuild replaced the mirror from the chunk. Without
    // the skip, the blocker (lower lsn_end) would reconcile-then-quarantine before the chunk fired.
    assert!(
        !ctx.state.is_quarantined(),
        "the superseded blocker did not quarantine the loader"
    );
    let raw_blocker: i64 = ctx
        .db
        .conn()
        .query_row("SELECT count(*) FROM orders_raw WHERE id = 8", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        raw_blocker, 0,
        "the blocker's rows were never appended to raw"
    );
    assert_eq!(mirror_status(&ctx, 1).as_deref(), Some("rebuilt"));
    let blocker_gone: i64 =
        sqlx::query_scalar("SELECT count(*) FROM walrus.file_manifest WHERE id = $1")
            .bind(blocker_id)
            .fetch_one(&ctx.pool)
            .await
            .unwrap();
    assert_eq!(blocker_gone, 0, "the rebuild purged the skipped blocker");

    let _ = std::fs::remove_dir_all(&dir);
}
