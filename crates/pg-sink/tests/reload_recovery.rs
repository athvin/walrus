#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Completion & crash recovery against compose (`#[ignore]` — needs source PG + control PG +
//! MinIO). Three proofs (reload H7/H10, PR 6.9):
//!
//! - A "crashed" export (exporter dropped mid-flight) is ADOPTED from control-pg — not WAL
//!   redelivery — and resumes from the chunk cursor, re-exporting nothing at or before it, then
//!   drains and flips `export_complete(H)`. The loader (simulated by advancing the checkpoint)
//!   flips `complete` once `transformed_lsn ≥ H`. Full walk: requested → exporting →
//!   export_complete → complete.
//! - `complete` waits for `transformed_lsn ≥ H` both ways: it holds at `export_complete` while the
//!   mirror is behind H, and flips once it catches up. The LOADER owns the flip.
//! - Adoption is lease-aware: a live FOREIGN lease is never stolen; an EXPIRED one is taken.
//!
//!   cargo test -p pg-sink --test reload_recovery -- --ignored --test-threads=1

use common::Lsn;
use pg_sink::consume::on_frame;
use pg_sink::heartbeat::InternalTables;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::reload_export::{ChunkExportConfig, ChunkExporter, RunOutcome};
use pg_sink::reload_signal::{PendingSignal, PendingSignals, WatermarkWaiters};
use pg_sink::replication::ReplicationStream;
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use control::reload::{ReloadFlavor, ReloadStatus};

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
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

async fn drop_slot(admin: &tokio_postgres::Client, slot: &str) {
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

async fn seed(admin: &tokio_postgres::Client, pool: &sqlx::PgPool, epoch: i64, n: i64) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id int PRIMARY KEY, val text NOT NULL);
             INSERT INTO public.{TABLE} SELECT g, 'v' || g FROM generate_series(1, {n}) g;"
        ))
        .await
        .unwrap();
    let rel = pg_sink::snapshot::describe_source_relation(admin, "public", TABLE)
        .await
        .unwrap();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: TABLE.to_string(),
            schema_version: 1,
            descriptors: pg_to_arrow::descriptor::describe_relation(&rel),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

async fn scrub(pool: &sqlx::PgPool, epoch: i64) {
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

fn spawn_echo_resolver(
    slot: &'static str,
    waiters: Arc<WatermarkWaiters>,
    token: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let admin = admin().await;
        drop_slot(&admin, slot).await;
        let resume = verify_or_create_slot(&admin, slot).await.unwrap();
        let mut stream =
            ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
                .await
                .unwrap();
        let mut ctx = StreamCtx::default();
        let mut internal = InternalTables::default();
        let mut pending = PendingSignals::default();
        loop {
            let frame = tokio::select! {
                _ = token.cancelled() => break,
                f = stream.next() => f.unwrap().unwrap(),
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Relation { relation, .. } => internal.note_relation(relation),
                Message::Insert {
                    relation_oid,
                    new,
                    xid,
                } if internal.is_reload_signal(*relation_oid) => {
                    let rel = internal.reload_signal_rel().unwrap();
                    if let Some(sig) = PendingSignal::from_tuple(rel, new, *xid) {
                        pending.push(sig);
                    }
                }
                Message::Commit { commit_lsn, .. } => pending.on_commit(*commit_lsn, &waiters),
                _ => {}
            }
        }
        drop(stream);
        drop_slot(&admin, slot).await;
    })
}

async fn await_resolver_ready(
    admin: &tokio_postgres::Client,
    waiters: &Arc<WatermarkWaiters>,
    epoch: i64,
) {
    let sentinel = -epoch;
    let mut ready = false;
    for _ in 0..20 {
        let rx = waiters.subscribe(sentinel, 1);
        admin
            .batch_execute(&format!(
                "DELETE FROM walrus.reload_signal WHERE reload_id = {sentinel}; \
                 INSERT INTO walrus.reload_signal (reload_id, chunk_no) VALUES ({sentinel}, 1);"
            ))
            .await
            .unwrap();
        if tokio::time::timeout(Duration::from_millis(500), rx)
            .await
            .is_ok()
        {
            ready = true;
            break;
        }
    }
    assert!(ready, "the echo resolver never answered the sentinel");
    admin
        .execute(
            "DELETE FROM walrus.reload_signal WHERE reload_id = $1",
            &[&sentinel],
        )
        .await
        .unwrap();
}

fn export_cfg(epoch: i64, chunk_rows: u64) -> ChunkExportConfig {
    ChunkExportConfig {
        chunk_rows,
        echo_timeout: Duration::from_secs(20),
        instance: HOLDER.to_string(),
        epoch,
    }
}

async fn request_and_claim(pool: &sqlx::PgPool, epoch: i64, holder: &str) -> control::ReloadRow {
    control::reload::request(pool, epoch, "public", TABLE, ReloadFlavor::Reload)
        .await
        .unwrap();
    control::reload::claim_requested(pool, epoch, holder, 3600, 1)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn reload_file_count(pool: &sqlx::PgPool, epoch: i64, reload_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1 AND reload_id = $2",
    )
    .bind(epoch)
    .bind(reload_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Simulate the loader reaching a watermark: seed the checkpoint and advance both frontiers (the
/// `raw >= transformed` CHECK needs raw first). Never applies a file — this exercises the
/// completion PREDICATE, not the mirror content (that is the loader's own suites).
async fn set_transformed(pool: &sqlx::PgPool, epoch: i64, lsn: Lsn) {
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

async fn status(pool: &sqlx::PgPool, reload_id: i64) -> ReloadStatus {
    control::reload::get(pool, reload_id)
        .await
        .unwrap()
        .unwrap()
        .status
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn kill_mid_export_resumes_from_cursor_and_completes() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = 690_001;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 5).await;

    let waiters = Arc::new(WatermarkWaiters::default());
    let token = CancellationToken::new();
    let resolver = spawn_echo_resolver("walrus_rec_resume", waiters.clone(), token.clone());
    await_resolver_ready(&admin, &waiters, epoch).await;

    // requested → exporting, chunk 1 (2 of 5 rows), then "crash": drop the exporter mid-flight.
    let req = request_and_claim(&pool, epoch, HOLDER).await;
    let reload_id = req.reload_id;
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::Exporting);
    let mut crashed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        ParquetSink::new(store(), "walrus".to_string(), epoch),
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

    // "Restart": adopt from control-pg (our own lease), resume from the cursor, drain.
    let mut adopted = control::reload::adopt_resumable(&pool, epoch, HOLDER, 60, 5)
        .await
        .unwrap();
    assert_eq!(adopted.len(), 1, "our orphaned export is adopted");
    let row = adopted.pop().unwrap();
    assert_eq!(row.reload_id, reload_id);
    assert_eq!(row.chunk_no, 1, "adopted at the cursor, not chunk zero");

    let mut resumed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        ParquetSink::new(store(), "walrus".to_string(), epoch),
        export_cfg(epoch, 2),
        &row,
    )
    .await
    .unwrap();
    let h = match resumed.run().await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        other => panic!("expected drain, got {other:?}"),
    };

    // The sink's last act: export_complete(H). No chunk at/before the cursor was re-exported.
    control::reload::complete_export(&pool, reload_id, h)
        .await
        .unwrap();
    let done = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.status, ReloadStatus::ExportComplete);
    assert_eq!(done.final_lsn, Some(h), "H recorded");
    assert_eq!(done.chunk_no, 3, "5 rows at chunk_rows=2 ⇒ 3 chunks");
    assert_eq!(
        reload_file_count(&pool, epoch, reload_id).await,
        3,
        "chunk 1 (pre-crash) + chunks 2,3 (post-resume) — nothing re-exported"
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
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::ExportComplete);

    // Caught up to H: the loader flips complete. The full walk is requested→exporting→
    // export_complete→complete, in order, none skipped.
    set_transformed(&pool, epoch, h).await;
    let completed = control::reload::complete_reached(&pool, epoch, "public", TABLE)
        .await
        .unwrap();
    assert_eq!(completed, vec![reload_id]);
    assert_eq!(status(&pool, reload_id).await, ReloadStatus::Complete);

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn complete_waits_for_transformed_lsn_to_reach_h() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = 690_002;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    let waiters = Arc::new(WatermarkWaiters::default());
    let token = CancellationToken::new();
    let resolver = spawn_echo_resolver("walrus_rec_wait", waiters.clone(), token.clone());
    await_resolver_ready(&admin, &waiters, epoch).await;

    // A clean full export to export_complete(H).
    let req = request_and_claim(&pool, epoch, HOLDER).await;
    let reload_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        ParquetSink::new(store(), "walrus".to_string(), epoch),
        export_cfg(epoch, 10),
        &req,
    )
    .await
    .unwrap();
    let h = match exporter.run().await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        other => panic!("expected drain, got {other:?}"),
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
    let epoch = 690_003;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;

    // A reload held by ANOTHER live instance (fresh 1h lease).
    let req = request_and_claim(&pool, epoch, "walrus-sink-other").await;
    let reload_id = req.reload_id;

    // We must not steal a live foreign lease.
    assert!(
        control::reload::adopt_resumable(&pool, epoch, "walrus-sink-me", 60, 5)
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
        .bind(reload_id)
        .execute(&pool)
        .await
        .unwrap();
    let adopted = control::reload::adopt_resumable(&pool, epoch, "walrus-sink-me", 60, 5)
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
