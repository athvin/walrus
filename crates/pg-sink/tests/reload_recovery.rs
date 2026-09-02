#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Completion & crash recovery against compose (`#[ignore]` — needs source PG + control PG +
//! MinIO). Four proofs cover reload H7/H10:
//!
//! - A "crashed" export (exporter dropped mid-flight) is ADOPTED from control-pg — not WAL
//!   redelivery — then its lost-snapshot predecessor is failed/purged and a fresh fenced successor
//!   re-exports from chunk zero. The loader (simulated by advancing the checkpoint) flips
//!   `complete` once `transformed_lsn ≥ H`.
//! - A crash after durable H but before `complete_export(H)` adopts the exact F/H boundary and
//!   never scans a source row committed after H into an F-stamped baseline chunk.
//! - `complete` waits for `transformed_lsn ≥ H` both ways: it holds at `export_complete` while the
//!   mirror is behind H, and flips once it catches up. The LOADER owns the flip.
//! - Adoption is lease-aware: a live FOREIGN lease is never stolen; an EXPIRED one is taken.
//!
//!   cargo test -p pg-sink --test reload_recovery -- --ignored --test-threads=1

use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use pg_sink::reload::{RestartDecision, handle_lost_snapshot_restart};
use pg_sink::reload_event::FenceWaiters;
use pg_sink::reload_export::{ChunkExportConfig, ChunkExporter, RunOutcome};
use pg_sink::sink::ParquetSink;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[path = "support/reload_fence.rs"]
mod reload_fence_support;

use control::reload::{ReloadFlavor, ReloadStatus};

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const SOURCE_0004: &str = include_str!("../../../migrations/source/0004_reload_event.sql");
const TABLE: &str = "_walrus_rec_orders";
const HOLDER: &str = "walrus-sink-test";

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn admin() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

fn store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
}

async fn seed(admin: &tokio_postgres::Client, pool: &sqlx::PgPool, epoch: EpochNo, n: i64) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id int PRIMARY KEY, val text NOT NULL);
             INSERT INTO public.{TABLE} SELECT g, 'v' || g FROM generate_series(1, {n}) g;"
        ))
        .await
        .unwrap();
    let rel = pg_sink::source_catalog::describe_source_relation(admin, "public", TABLE)
        .await
        .unwrap();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: TABLE.to_string(),
            schema_version: SchemaVersionNo(1),
            descriptors: pg_to_arrow::describe_relation(&rel).unwrap(),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

async fn scrub(pool: &sqlx::PgPool, epoch: EpochNo) {
    for tbl in [
        "file_manifest",
        "table_reload",
        "schema_registry",
        "loader_checkpoint",
    ] {
        sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(pool)
            .await
            .unwrap();
    }
}

fn export_cfg(epoch: EpochNo, chunk_rows: u64) -> ChunkExportConfig {
    ChunkExportConfig {
        chunk_rows: std::num::NonZeroU64::new(chunk_rows).unwrap(),
        echo_timeout: Duration::from_secs(20),
        instance: HOLDER.to_string(),
        epoch,
        publication_name: "walrus_pub".to_string(),
    }
}

async fn request_and_claim(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    holder: &str,
) -> control::ReloadRow {
    control::reload::request(pool, epoch, "public", TABLE, ReloadFlavor::Reload)
        .await
        .unwrap();
    control::reload::claim_requested(pool, epoch, holder, 3600, 1)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn reload_file_count(pool: &sqlx::PgPool, epoch: EpochNo, reload_id: ReloadId) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1 AND reload_id = $2",
    )
    .bind(epoch)
    .bind(reload_id.0)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Simulate the loader reaching a watermark: seed the checkpoint and advance both frontiers (the
/// `raw >= transformed` CHECK needs raw first). Never applies a file — this exercises the
/// completion PREDICATE, not the mirror content (that is the loader's own suites).
async fn set_transformed(pool: &sqlx::PgPool, epoch: EpochNo, lsn: Lsn) {
    control::ensure_checkpoint(pool, epoch, "public", TABLE)
        .await
        .unwrap();
    control::advance_raw_appended(pool, epoch, "public", TABLE, lsn)
        .await
        .unwrap();
    control::advance_transformed(pool, epoch, "public", TABLE, lsn)
        .await
        .unwrap();
}

async fn status(pool: &sqlx::PgPool, reload_id: ReloadId) -> ReloadStatus {
    control::reload::get(pool, reload_id)
        .await
        .unwrap()
        .unwrap()
        .status
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn kill_mid_export_starts_fresh_successor_and_completes() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(690_001);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 5).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_rec_resume",
        pool.clone(),
        Arc::clone(&waiters),
        None,
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let resolver = resolver.handle;

    // requested → exporting, chunk 1 (2 of 5 rows), then "crash": drop the exporter mid-flight.
    let req = request_and_claim(&pool, epoch, HOLDER).await;
    let reload_id = req.reload_id;
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::Exporting);
    let mut crashed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetSink::new(store(), "walrus", epoch),
        export_cfg(epoch, 2),
        &req,
    )
    .await
    .unwrap();
    crashed.export_next_chunk().await.unwrap();
    drop(crashed);
    let mid = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mid.chunk_no, 1, "one chunk done before the crash");

    // "Restart": adoption cannot resurrect the connection-local snapshot, so the cursor is
    // evidence to purge rather than a safe resume point.
    let mut adopted = control::reload::adopt_resumable(&pool, epoch, HOLDER, 60, 5, true)
        .await
        .unwrap();
    assert_eq!(adopted.len(), 1, "our orphaned export is adopted");
    let row = adopted.pop().unwrap();
    assert_eq!(row.reload_id, reload_id);
    assert_eq!(row.chunk_no, 1, "adopted at the cursor, not chunk zero");

    let mut adopted_exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetSink::new(store(), "walrus", epoch),
        export_cfg(epoch, 2),
        &row,
    )
    .await
    .unwrap();
    assert_eq!(
        adopted_exporter.run(true).await.unwrap(),
        RunOutcome::SnapshotLost,
        "an adopted attempt without H never issues another source-table SELECT"
    );
    let new_id = match handle_lost_snapshot_restart(&pool, &row, 5).await.unwrap() {
        RestartDecision::Restarted(id) => id,
        RestartDecision::Capped => panic!("restart cap must have room for the first recovery"),
    };
    let failed = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, ReloadStatus::Failed);
    assert_eq!(
        reload_file_count(&pool, epoch, reload_id).await,
        0,
        "the lost-snapshot predecessor's chunk is purged"
    );

    let successor = control::reload::get(&pool, new_id).await.unwrap().unwrap();
    assert_eq!(successor.chunk_no, 0);
    assert_eq!(successor.cursor_pk, None);
    assert_eq!(successor.start_lsn, None);
    assert_eq!(successor.schema_version, None);
    let mut restarted = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetSink::new(store(), "walrus", epoch),
        export_cfg(epoch, 2),
        &successor,
    )
    .await
    .unwrap();
    let h = match restarted.run(false).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("expected drain, got SchemaChanged(new_version = {new_version})")
        }
        RunOutcome::SnapshotLost => panic!("a fresh successor owns its source snapshot"),
    };

    // The sink's last act applies to the fresh successor, never the snapshot-lost predecessor.
    control::reload::complete_export(&pool, new_id, h)
        .await
        .unwrap();
    let done = control::reload::get(&pool, new_id).await.unwrap().unwrap();
    assert_eq!(done.status, ReloadStatus::ExportComplete);
    assert_eq!(done.final_lsn, Some(h), "H recorded");
    assert_eq!(done.chunk_no, 3, "5 rows at chunk_rows=2 ⇒ 3 chunks");
    assert_eq!(
        reload_file_count(&pool, epoch, new_id).await,
        3,
        "the successor re-exports one internally consistent snapshot from chunk zero"
    );
    assert!(
        matches!((done.start_lsn, row.start_lsn), (Some(new_f), Some(old_f)) if new_f > old_f),
        "the successor establishes a fresh F instead of reusing the lost snapshot's fence"
    );

    // The LOADER flips complete only once transformed_lsn ≥ H. Behind H: it holds.
    set_transformed(&pool, epoch, "0/1".parse().unwrap()).await;
    assert!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap()
            .is_empty(),
        "behind H, the reload stays export_complete"
    );
    assert_eq!(status(&pool, new_id).await, ReloadStatus::ExportComplete);

    // Caught up to H: the loader flips complete. The full walk is requested→exporting→
    // export_complete→complete, in order, none skipped.
    set_transformed(&pool, epoch, h).await;
    let completed = control::reload::complete_reached(&pool, epoch, "public", TABLE)
        .await
        .unwrap();
    assert_eq!(completed, vec![new_id]);
    assert_eq!(status(&pool, new_id).await, ReloadStatus::Complete);

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn crash_after_h_before_complete_does_not_export_post_h_rows() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(690_004);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_rec_after_h",
        pool.clone(),
        Arc::clone(&waiters),
        Some(TABLE),
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let mut watched_commit = resolver.watched_commit;
    let resolver = resolver.handle;

    let req = request_and_claim(&pool, epoch, HOLDER).await;
    let reload_id = req.reload_id;
    let mut crashed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetSink::new(store(), "walrus", epoch),
        export_cfg(epoch, 10),
        &req,
    )
    .await
    .unwrap();
    let h = match crashed.run(false).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("expected drain, got SchemaChanged(new_version = {new_version})")
        }
        RunOutcome::SnapshotLost => panic!("a freshly claimed exporter owns its snapshot"),
    };
    let before = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.status, ReloadStatus::Exporting);
    assert_eq!(before.chunk_no, 1);
    assert_eq!(reload_file_count(&pool, epoch, reload_id).await, 1);
    assert_eq!(
        control::reload::read_markers(&pool, reload_id)
            .await
            .unwrap()
            .last()
            .map(|marker| marker.lsn),
        Some(h),
        "H is durable before the simulated crash"
    );
    drop(crashed); // crash after durable H, before complete_export(H)

    // This commit is strictly after H and sorts past the saved cursor. A buggy adopter resumes its
    // SELECT, exports id=4 as another F-stamped baseline chunk, and contaminates the H rebuild.
    admin
        .execute(
            &format!("INSERT INTO public.{TABLE} (id, val) VALUES (4, 'after-h')"),
            &[],
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), watched_commit.changed())
        .await
        .expect("post-H table commit reaches the resolver")
        .expect("resolver remains live");
    let post_h_commit =
        (*watched_commit.borrow_and_update()).expect("the watched insert has a commit LSN");
    assert!(
        post_h_commit > h,
        "test mutation must commit strictly after H"
    );

    let mut adopted = control::reload::adopt_resumable(&pool, epoch, HOLDER, 60, 5, true)
        .await
        .unwrap();
    assert_eq!(adopted.len(), 1);
    let row = adopted.pop().unwrap();
    assert_eq!(row.reload_id, reload_id);
    let mut recovered = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetSink::new(store(), "walrus", epoch),
        export_cfg(epoch, 10),
        &row,
    )
    .await
    .unwrap();
    let recovered_h = match recovered.run(true).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("expected H recovery, got SchemaChanged(new_version = {new_version})")
        }
        RunOutcome::SnapshotLost => panic!("durable H must recover before snapshot ownership"),
    };
    assert_eq!(recovered_h, h, "the adopter finishes from the original H");

    let after = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.chunk_no, before.chunk_no, "no post-H chunk SELECT");
    assert_eq!(after.cursor_pk, before.cursor_pk, "the cursor did not move");
    assert_eq!(
        reload_file_count(&pool, epoch, reload_id).await,
        1,
        "the post-H row was not copied into an F-stamped reload file"
    );
    control::reload::complete_export(&pool, reload_id, recovered_h)
        .await
        .unwrap();
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::ExportComplete);

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn complete_waits_for_transformed_lsn_to_reach_h() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(690_002);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_rec_wait",
        pool.clone(),
        Arc::clone(&waiters),
        None,
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let resolver = resolver.handle;

    // A clean full export to export_complete(H).
    let req = request_and_claim(&pool, epoch, HOLDER).await;
    let reload_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetSink::new(store(), "walrus", epoch),
        export_cfg(epoch, 10),
        &req,
    )
    .await
    .unwrap();
    let h = match exporter.run(false).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("expected drain, got SchemaChanged(new_version = {new_version})")
        }
        RunOutcome::SnapshotLost => panic!("a freshly claimed exporter owns its snapshot"),
    };
    control::reload::complete_export(&pool, reload_id, h)
        .await
        .unwrap();

    // Frozen loader (transformed_lsn = 0): complete does NOT fire.
    control::ensure_checkpoint(&pool, epoch, "public", TABLE)
        .await
        .unwrap();
    assert!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::ExportComplete);

    // Loader catches up to H: complete fires, exactly once (a second call is a no-op).
    set_transformed(&pool, epoch, h).await;
    assert_eq!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap(),
        vec![reload_id]
    );
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::Complete);
    assert!(
        control::reload::complete_reached(&pool, epoch, "public", TABLE)
            .await
            .unwrap()
            .is_empty(),
        "already complete — idempotent, flips nothing twice"
    );

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG)"]
async fn adoption_respects_live_leases_but_takes_expired_ones() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(690_003);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;

    // A reload held by ANOTHER live instance (fresh 1h lease).
    let req = request_and_claim(&pool, epoch, "walrus-sink-other").await;
    let reload_id = req.reload_id;

    // We must not steal a live foreign lease.
    assert!(
        control::reload::adopt_resumable(&pool, epoch, "walrus-sink-me", 60, 5, true)
            .await
            .unwrap()
            .is_empty(),
        "a live foreign lease is left alone"
    );
    assert_eq!(
        control::reload::get(&pool, reload_id)
            .await
            .unwrap()
            .unwrap()
            .lease_holder
            .as_deref(),
        Some("walrus-sink-other"),
        "foreign holder untouched"
    );

    // Expire it (a dead instance): now it is adoptable, and the guarded UPDATE re-acquires it.
    sqlx::query("UPDATE walrus.table_reload SET lease_expiry = now() - interval '1 hour' WHERE reload_id = $1")
        .bind(reload_id.0)
        .execute(&pool)
        .await
        .unwrap();
    let adopted = control::reload::adopt_resumable(&pool, epoch, "walrus-sink-me", 60, 5, true)
        .await
        .unwrap();
    assert_eq!(adopted.len(), 1);
    assert_eq!(adopted[0].reload_id, reload_id);
    assert_eq!(
        control::reload::get(&pool, reload_id)
            .await
            .unwrap()
            .unwrap()
            .lease_holder
            .as_deref(),
        Some("walrus-sink-me"),
        "the expired lease was re-acquired by the adopter"
    );

    scrub(&pool, epoch).await;
}
