//! Reload observability against compose (`#[ignore]` — source + control PG + MinIO). Proves the
//! PR 6.11 metrics MOVE during a reload: chunk/row counters and the echo-wait histogram tick as an
//! export runs; the failed counter ticks on an echo timeout; the active gauge rises to 1 while an
//! exporter is in flight and returns to 0 when it ends; and the cross-check violation counter stays
//! 0 on a healthy run (its whole point is to be 0). The named-metric registration is covered by
//! `metrics_scrape.rs`; this is the movement proof.
//!
//!   cargo test -p pg-sink --test reload_metrics -- --ignored --test-threads=1

use pg_sink::consume::on_frame;
use pg_sink::heartbeat::InternalTables;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::reload::{ReloadController, ReloadControllerConfig};
use pg_sink::reload_export::{ChunkExportConfig, ChunkExporter};
use pg_sink::reload_signal::{PendingSignal, PendingSignals, WatermarkWaiters};
use pg_sink::replication::ReplicationStream;
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use control::reload::{self, ReloadFlavor, ReloadStatus};

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const TABLE: &str = "_walrus_met_orders";

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

fn minio(epoch: i64) -> ParquetSink {
    ParquetSink::new(
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
        ),
        "walrus".to_string(),
        epoch,
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
    for tbl in ["file_manifest", "table_reload", "schema_registry"] {
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

fn export_cfg(epoch: i64, chunk_rows: u64, echo_timeout: Duration) -> ChunkExportConfig {
    ChunkExportConfig {
        chunk_rows,
        echo_timeout,
        instance: "walrus-sink-test".to_string(),
        epoch,
    }
}

async fn request_and_claim(pool: &sqlx::PgPool, epoch: i64) -> control::ReloadRow {
    reload::request(pool, epoch, "public", TABLE, ReloadFlavor::Reload)
        .await
        .unwrap();
    reload::claim_requested(pool, epoch, "walrus-sink-test", 60, 1)
        .await
        .unwrap()
        .pop()
        .unwrap()
}

/// Sum a metric across all label sets (unlabelled or per-{table}/{flavor}); the current value for
/// a gauge, the total for a counter, the `_count` for a histogram (pass the `_count` name).
fn metric_sum(name: &str) -> f64 {
    let mut total = 0.0;
    for line in common::metrics::render().lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(name) {
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
async fn chunk_export_moves_chunk_row_and_echo_metrics() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = 700_001;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 5).await;

    let waiters = Arc::new(WatermarkWaiters::default());
    let token = CancellationToken::new();
    let resolver = spawn_echo_resolver("walrus_met_export", waiters.clone(), token.clone());
    await_resolver_ready(&admin, &waiters, epoch).await;

    let chunks_before = metric_sum(common::metrics::names::RELOAD_CHUNKS_TOTAL);
    let rows_before = metric_sum(common::metrics::names::RELOAD_ROWS_EXPORTED_TOTAL);
    let echo_before = metric_sum("walrus_reload_echo_wait_seconds_count");
    let crosscheck_before = metric_sum(common::metrics::names::RELOAD_CROSSCHECK_VIOLATIONS);

    // 5 rows at chunk_rows=2 ⇒ 3 chunks (2+2+1), each preceded by an echo round-trip.
    let req = request_and_claim(&pool, epoch).await;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        minio(epoch),
        export_cfg(epoch, 2, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();
    exporter.run().await.unwrap();

    assert_eq!(
        metric_sum(common::metrics::names::RELOAD_CHUNKS_TOTAL) - chunks_before,
        3.0,
        "three chunk files ⇒ chunks_total += 3"
    );
    assert_eq!(
        metric_sum(common::metrics::names::RELOAD_ROWS_EXPORTED_TOTAL) - rows_before,
        5.0,
        "all 5 rows counted"
    );
    assert!(
        metric_sum("walrus_reload_echo_wait_seconds_count") - echo_before >= 3.0,
        "the echo-wait histogram observed at least one round-trip per chunk"
    );
    assert_eq!(
        metric_sum(common::metrics::names::RELOAD_CROSSCHECK_VIOLATIONS) - crosscheck_before,
        0.0,
        "a healthy export raises zero cross-check violations"
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
async fn echo_timeout_moves_the_failed_metric() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = 700_002;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    let failed_before = metric_sum(common::metrics::names::RELOAD_FAILED_TOTAL);

    // NO resolver: the echo never arrives, so the reload fails loudly (short timeout to keep it snappy).
    let waiters = Arc::new(WatermarkWaiters::default());
    let req = request_and_claim(&pool, epoch).await;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters,
        minio(epoch),
        export_cfg(epoch, 1000, Duration::from_millis(300)),
        &req,
    )
    .await
    .unwrap();
    let err = exporter.run().await.unwrap_err();
    assert!(format!("{err:#}").contains("no echo after"));
    assert_eq!(
        reload::get(&pool, req.reload_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ReloadStatus::Failed
    );
    assert_eq!(
        metric_sum(common::metrics::names::RELOAD_FAILED_TOTAL) - failed_before,
        1.0,
        "the failed counter ticked for this table"
    );

    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn active_gauge_rises_and_returns_to_zero() {
    let _g = SOURCE_LOCK.lock().await;
    common::metrics::init();
    let epoch = 700_003;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 3).await;

    // The controller spawns an exporter that PARKS on the echo await (no resolver) — long enough to
    // observe active=1; cancelling the token ends it and drops the gauge back to 0.
    let token = CancellationToken::new();
    let waiters = Arc::new(WatermarkWaiters::default());
    let handle = ReloadController::spawn(
        pool.clone(),
        &source_url(),
        waiters,
        minio(epoch),
        ReloadControllerConfig {
            poll_interval: Duration::from_millis(200),
            max_concurrent_reloads: 2,
            lease_ttl: Duration::from_secs(6),
            instance: "walrus-sink-test".to_string(),
            publication_name: "walrus_pub".to_string(),
            epoch,
            chunk_rows: 1000,
            echo_timeout: Duration::from_secs(3600), // park forever, no resolver
            reload_max_restarts: 3,
        },
        token.clone(),
    );
    reload::request(&pool, epoch, "public", TABLE, ReloadFlavor::Reload)
        .await
        .unwrap();

    // active{flavor="reload"} rises to 1 within a few poll cadences.
    let rose = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if metric_sum(common::metrics::names::RELOAD_ACTIVE) >= 1.0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(rose.is_ok(), "reload_active never reached 1");

    // Cancel: the parked exporter ends (Cancelled) and decrements the gauge back to 0.
    token.cancel();
    handle.await.unwrap();
    let fell = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if metric_sum(common::metrics::names::RELOAD_ACTIVE) == 0.0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(fell.is_ok(), "reload_active never returned to 0");

    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}
