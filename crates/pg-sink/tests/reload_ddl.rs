//! Restart-on-DDL against compose (`#[ignore]` — needs source PG + control PG + MinIO). A schema
//! change landing BETWEEN chunks invalidates the attempt: the exporter's per-chunk staleness check
//! returns `SchemaChanged`, and the controller fails-and-reissues in one transaction — the old row
//! turns `failed`, its chunk files are purged, and a successor `exporting` at `restart_count+1`
//! starts with a fresh cursor. The successor then re-exports from chunk zero at the NEW schema.
//! Past `reload_max_restarts` the reload fails outright with the cap named and no successor. Both
//! paths bump their metric.
//!
//! Every attempt is single-schema by construction (H9), so the loader never reconciles a version
//! change inside a rebuild — only in the stream, where that logic already runs. The pure staleness
//! and cap predicates are unit-tested in `src/reload_export.rs` / `src/reload.rs`; this is the
//! end-to-end transaction and metric proof.
//!
//!   cargo test -p pg-sink --test reload_ddl -- --ignored --test-threads=1

use bytes::Bytes;
use common::Lsn;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pg_sink::consume::on_frame;
use pg_sink::heartbeat::InternalTables;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::reload::{handle_ddl_restart, RestartDecision};
use pg_sink::reload_export::{ChunkExportConfig, ChunkExporter, RunOutcome};
use pg_sink::reload_signal::{PendingSignal, PendingSignals, WatermarkWaiters};
use pg_sink::replication::ReplicationStream;
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const TABLE: &str = "_walrus_ddl_orders";

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

fn store() -> Arc<dyn ObjectStore> {
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

/// Seed the target table (a plain 2-column table) with `n` rows and register its shape at v1.
async fn seed(admin: &tokio_postgres::Client, pool: &sqlx::PgPool, epoch: i64, n: i64) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id int PRIMARY KEY, val text NOT NULL);
             INSERT INTO public.{TABLE} SELECT g, 'v' || g FROM generate_series(1, {n}) g;"
        ))
        .await
        .unwrap();
    register(admin, pool, epoch, 1).await;
}

/// (Re)register the table's CURRENT source shape at `version` — the sink's decode loop does this on
/// a Relation message; here the test does it directly to simulate DDL bumping the structural version.
async fn register(admin: &tokio_postgres::Client, pool: &sqlx::PgPool, epoch: i64, version: i64) {
    let rel = pg_sink::snapshot::describe_source_relation(admin, "public", TABLE)
        .await
        .unwrap();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: TABLE.to_string(),
            schema_version: version,
            descriptors: pg_to_arrow::descriptor::describe_relation(&rel),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

async fn scrub(pool: &sqlx::PgPool, epoch: i64) {
    for tbl in ["file_manifest", "table_reload", "schema_registry"] {
        sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(pool)
            .await
            .unwrap();
    }
}

/// The decode-loop half of echo-wait, minimally: resolve signal echoes against `waiters`. Runs
/// until cancelled. (No overlap probe here — the DDL tests don't need the watch channel.)
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

/// Prove the resolver is live before the exporter signals through it (a handshake, not a sleep).
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
        instance: "walrus-sink-test".to_string(),
        epoch,
    }
}

async fn request_and_claim(pool: &sqlx::PgPool, epoch: i64) -> control::ReloadRow {
    control::reload::request(
        pool,
        epoch,
        "public",
        TABLE,
        control::reload::ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    control::reload::claim_requested(pool, epoch, "walrus-sink-test", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

async fn reload_rows(pool: &sqlx::PgPool, epoch: i64) -> Vec<control::ReloadRow> {
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT reload_id FROM walrus.table_reload
         WHERE epoch = $1 AND source_table = $2 ORDER BY reload_id",
    )
    .bind(epoch)
    .bind(TABLE)
    .fetch_all(pool)
    .await
    .unwrap();
    let mut out = Vec::new();
    for id in ids {
        out.push(control::reload::get(pool, id).await.unwrap().unwrap());
    }
    out
}

async fn manifest_count(pool: &sqlx::PgPool, epoch: i64, reload_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1 AND reload_id = $2",
    )
    .bind(epoch)
    .bind(reload_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// The (uri, schema_version) of a reload's chunk files, in claim order.
async fn reload_files(pool: &sqlx::PgPool, epoch: i64, reload_id: i64) -> Vec<(String, i64)> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT s3_uri, schema_version FROM walrus.file_manifest
         WHERE epoch = $1 AND reload_id = $2 ORDER BY lsn_end, id",
    )
    .bind(epoch)
    .bind(reload_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// The Arrow column names of a chunk Parquet file — proves which shape it was exported at.
async fn chunk_columns(uri: &str) -> Vec<String> {
    let key = uri.strip_prefix("s3://walrus/").unwrap();
    let bytes: Bytes = store()
        .get(&Path::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// A counter's total from the Prometheus exposition (0 if absent) — summed across all label sets,
/// so it works for both unlabelled (`walrus_reload_restart_cap_exhausted_total`) and per-table
/// (`walrus_reload_restarts_total{table="…"}`) counters (PR 6.11 relabelled restarts).
fn counter_value(name: &str) -> f64 {
    let mut total = 0.0;
    for line in common::metrics::render().lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(name) {
            // The metric name must end exactly here — the next char is a space (no labels) or `{`.
            if rest.starts_with(' ') || rest.starts_with('{') {
                if let Some(v) = rest.split_whitespace().last() {
                    total += v.parse::<f64>().unwrap_or(0.0);
                }
            }
        }
    }
    total
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn mid_export_ddl_restarts_fresh_attempt_at_new_schema() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = 680_001;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 5).await;

    let waiters = Arc::new(WatermarkWaiters::default());
    let token = CancellationToken::new();
    let resolver = spawn_echo_resolver("walrus_ddl_restart", waiters.clone(), token.clone());
    await_resolver_ready(&admin, &waiters, epoch).await;

    // Chunk 1 at v1 (freezes schema_version=1), then DDL lands: ALTER + re-register at v2.
    let req = request_and_claim(&pool, epoch).await;
    let old_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        ParquetSink::new(store(), "walrus".to_string(), epoch),
        export_cfg(epoch, 2),
        &req,
    )
    .await
    .unwrap();
    exporter.export_next_chunk().await.unwrap();
    admin
        .batch_execute(&format!(
            "ALTER TABLE public.{TABLE} ADD COLUMN priority int"
        ))
        .await
        .unwrap();
    register(&admin, &pool, epoch, 2).await;

    // The next chunk's staleness check trips: the run returns SchemaChanged (no chunk 2 at v1).
    let restarts_before = counter_value(common::metrics::names::RELOAD_RESTARTS_TOTAL);
    let outcome = exporter.run().await.unwrap();
    assert_eq!(outcome, RunOutcome::SchemaChanged { new_version: 2 });

    // The controller fails-and-reissues in one transaction.
    let old_after_chunk1 = control::reload::get(&pool, old_id).await.unwrap().unwrap();
    let decision = handle_ddl_restart(&pool, &old_after_chunk1, 2, 3)
        .await
        .unwrap();
    let new_id = match decision {
        RestartDecision::Restarted(id) => id,
        RestartDecision::Capped => panic!("cap of 3 must not be reached on the first DDL"),
    };
    assert_eq!(
        counter_value(common::metrics::names::RELOAD_RESTARTS_TOTAL) - restarts_before,
        1.0,
        "the restart counter incremented"
    );

    // Old: failed + superseded reason + zero chunk files. New: exporting, restart_count 1, fresh.
    let old = control::reload::get(&pool, old_id).await.unwrap().unwrap();
    assert_eq!(old.status, control::reload::ReloadStatus::Failed);
    assert!(
        old.error
            .as_deref()
            .unwrap_or_default()
            .contains("superseded"),
        "the old attempt names the supersession: {:?}",
        old.error
    );
    assert_eq!(
        manifest_count(&pool, epoch, old_id).await,
        0,
        "the old attempt's chunk files are purged (fail()'s coupling)"
    );
    let new = control::reload::get(&pool, new_id).await.unwrap().unwrap();
    assert_eq!(new.status, control::reload::ReloadStatus::Exporting);
    assert_eq!(new.restart_count, 1);
    assert_eq!(new.chunk_no, 0, "successor starts from chunk zero");
    assert_eq!(new.cursor_pk, None);
    assert_eq!(
        new.schema_version, None,
        "re-freezes at the new version on chunk 1"
    );
    assert_eq!(
        new.lease_holder.as_deref(),
        Some("walrus-sink-test"),
        "lease carried"
    );

    // The successor re-exports from zero at the NEW schema and drains.
    let mut resumed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        ParquetSink::new(store(), "walrus".to_string(), epoch),
        export_cfg(epoch, 2),
        &new,
    )
    .await
    .unwrap();
    assert!(matches!(
        resumed.run().await.unwrap(),
        RunOutcome::Drained { .. }
    ));

    let done = control::reload::get(&pool, new_id).await.unwrap().unwrap();
    assert_eq!(done.schema_version, Some(2), "the attempt froze at v2");
    let files = reload_files(&pool, epoch, new_id).await;
    assert_eq!(files.len(), 3, "5 rows at chunk_rows=2 ⇒ 3 files");
    assert!(
        files.iter().all(|f| f.1 == 2),
        "every successor file stamped v2"
    );
    let cols = chunk_columns(&files[0].0).await;
    assert!(
        cols.iter().any(|c| c == "priority"),
        "the new column is present in the successor's chunk files: {cols:?}"
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
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn restart_cap_exhaustion_fails_loudly() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = 680_002;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 5).await;

    let waiters = Arc::new(WatermarkWaiters::default());
    let token = CancellationToken::new();
    let resolver = spawn_echo_resolver("walrus_ddl_cap", waiters.clone(), token.clone());
    await_resolver_ready(&admin, &waiters, epoch).await;

    let req = request_and_claim(&pool, epoch).await;
    let old_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        ParquetSink::new(store(), "walrus".to_string(), epoch),
        export_cfg(epoch, 2),
        &req,
    )
    .await
    .unwrap();
    exporter.export_next_chunk().await.unwrap();
    admin
        .batch_execute(&format!(
            "ALTER TABLE public.{TABLE} ADD COLUMN priority int"
        ))
        .await
        .unwrap();
    register(&admin, &pool, epoch, 2).await;
    assert_eq!(
        exporter.run().await.unwrap(),
        RunOutcome::SchemaChanged { new_version: 2 }
    );

    // Cap 0: the first mid-export DDL fails the reload outright — no successor.
    let cap_before = counter_value(common::metrics::names::RELOAD_RESTART_CAP_EXHAUSTED_TOTAL);
    let old_after_chunk1 = control::reload::get(&pool, old_id).await.unwrap().unwrap();
    let decision = handle_ddl_restart(&pool, &old_after_chunk1, 2, 0)
        .await
        .unwrap();
    assert!(
        matches!(decision, RestartDecision::Capped),
        "cap 0 caps the first DDL"
    );
    assert_eq!(
        counter_value(common::metrics::names::RELOAD_RESTART_CAP_EXHAUSTED_TOTAL) - cap_before,
        1.0,
        "the cap-exhausted counter incremented"
    );

    let rows = reload_rows(&pool, epoch).await;
    assert_eq!(rows.len(), 1, "no successor row was written");
    assert_eq!(rows[0].reload_id, old_id);
    assert_eq!(rows[0].status, control::reload::ReloadStatus::Failed);
    let reason = rows[0].error.clone().unwrap_or_default();
    assert!(
        reason.contains("restart cap 0 exhausted"),
        "the failure names the cap: {reason}"
    );
    assert_eq!(
        manifest_count(&pool, epoch, old_id).await,
        0,
        "the failed reload's chunk files are purged"
    );

    // Sanity: the frozen L_1 that chunk 1 recorded is a real LSN (the attempt did run).
    let _l1: Lsn = rows[0].first_lsn.expect("chunk 1 froze L_1");

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}
