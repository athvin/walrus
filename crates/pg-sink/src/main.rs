//! The `walrus-pg-sink` binary — the pod lifecycle shell.
//!
//! `main` stays tiny: load+validate config, init tracing, build the runtime, and do the **only**
//! `anyhow::Error → ExitCode` mapping in the whole binary (the "context in the loop, exit code at
//! `main`" idiom — a broken deploy is greppable in `kubectl logs`). Everything below `main` returns
//! `anyhow::Result<_>`; a bootstrap failure carries a `common::Error` whose distinct exit code is
//! recovered here by downcast.

use anyhow::Context;
use pg_sink::config::SinkConfig;
use pg_sink::replication::ReplicationStream;
use pg_sink::{bootstrap, consume, health, shutdown};
use std::process::ExitCode;
use tokio::time::Instant;

fn main() -> ExitCode {
    // Step 1: config. Terminal on failure — before tracing exists, so report on stderr.
    let cfg = match SinkConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("walrus-pg-sink: invalid configuration: {e}");
            return common::ExitCode::Config.into();
        }
    };
    if let Err(e) = common::init_tracing(&cfg.telemetry) {
        eprintln!("walrus-pg-sink: tracing init failed: {e}");
        return common::ExitCode::Internal.into();
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!("failed to build tokio runtime: {e}");
            return common::ExitCode::Internal.into();
        }
    };

    match runtime.block_on(run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("walrus-pg-sink exiting: {e:#}");
            // Recover the distinct exit code from a classified bootstrap error; anything else is Internal.
            let code = e
                .downcast_ref::<common::Error>()
                .map(common::Error::exit_code)
                .unwrap_or(common::ExitCode::Internal);
            code.into()
        }
    }
}

/// The lifecycle: install signals, bind health (so probes see 503 during the slow bootstrap), run the
/// shared preflight, mark ready, then wait for SIGTERM. The replication loop lands in later PRs.
async fn run(cfg: SinkConfig) -> anyhow::Result<()> {
    let token = shutdown::install_signal_handlers();
    let state = health::HealthState::new();
    // Install the Prometheus recorder before anything can serve /metrics or emit a series (PR 4.10).
    common::metrics::init();

    // Bind health *after* config validated (no half-open port on a config crash) but *before* the
    // dependency checks, so `/startup` answers 503 while control PG / S3 come up, then flips to 200.
    let listener = tokio::net::TcpListener::bind(cfg.health_addr)
        .await
        .with_context(|| format!("bind health endpoints on {}", cfg.health_addr))?;
    let bound = listener.local_addr().context("read health bind address")?;
    tracing::info!(%bound, "health endpoints listening; bootstrapping");
    let server = tokio::spawn(health::serve_on(listener, state.clone(), token.clone()));

    // Shared bootstrap steps 2–4. On failure, tear the health server down before propagating the
    // classified error (whose exit code `main` surfaces).
    let deadline = Instant::now() + cfg.startup_deadline;
    let ctx = match bootstrap::run_shared(&cfg, deadline).await {
        Ok(ctx) => ctx,
        Err(e) => {
            token.cancel();
            let _ = server.await;
            return Err(e.into());
        }
    };

    state.mark_ready();
    tracing::info!("bootstrap complete; ready");

    const SCHEMA_VERSION: i64 = 1;
    let triggers = pg_sink::batch::BatchTriggers {
        max_fill: cfg.max_fill,
        max_rows: cfg.max_rows,
        max_bytes: cfg.max_bytes,
    };
    let mut cache = pg_sink::relcache::RelationCache::default();

    // Bootstrap decision (§1.7 / §1.8): a **pre-existing slot** means resume from `confirmed_flush_lsn`
    // (hydrate the cache from schema_registry). **No slot** means first bootstrap: create it with an
    // exported snapshot, backfill every published user table, then stream from `consistent_point`.
    let Bootstrapped {
        mut stream,
        epoch,
        start_lsn,
        sink,
    } = establish_stream(&cfg, &ctx, &mut cache, triggers, SCHEMA_VERSION).await?;
    tracing::info!(slot = %cfg.slot_name, start_lsn = %start_lsn, epoch, "streaming logical replication");

    let mut router = consume::BatchRouter::new(
        triggers,
        std::sync::Arc::new(pg_sink::batch::SystemClock),
        epoch,
        cfg.instance.clone(),
    );
    let mut checkpoint = pg_sink::checkpoint::DurabilityCheckpoint::new(start_lsn);
    // Large-transaction demux (§1.6): a txn over logical_decoding_work_mem streams before its commit.
    let mut demux = pg_sink::stream_txn::StreamDemux::new(
        triggers,
        std::sync::Arc::new(pg_sink::batch::SystemClock),
        epoch,
        cfg.instance.clone(),
        cfg.max_inflight_bytes,
    );

    // The idle heartbeat rides a SEPARATE ordinary SQL connection (distinct from replication); its
    // beat writes the published `walrus.heartbeat`, whose round-trip through the stream advances the
    // slot on an otherwise-idle publication (§1.9).
    let mut heartbeat = pg_sink::heartbeat::Heartbeat::connect(
        &cfg.source_db_url,
        cfg.instance.clone(),
        cfg.heartbeat_config(),
    )
    .await
    .context("connect heartbeat SQL connection")?;

    // DDL capture (§3): consume walrus.ddl_audit INSERTs → ddl_manifest + per-table structural version.
    let mut ddl = pg_sink::ddl::DdlConsumer::new(epoch);

    // Reload echo waiters (PR 6.3): Arc-shared so the reload controller's exporter tasks (PR 6.5)
    // can subscribe while the decode loop resolves.
    let waiters = std::sync::Arc::new(pg_sink::reload_signal::WatermarkWaiters::default());

    // The reload controller (PR 6.4): a side task off the decode path — own connections, polls
    // table_reload on the heartbeat cadence, schedules exporters under max_concurrent_reloads.
    let reload_controller = pg_sink::reload::ReloadController::spawn(
        ctx.control_pool.clone(),
        &cfg.source_db_url,
        waiters.clone(),
        pg_sink::reload::ReloadControllerConfig {
            poll_interval: cfg.heartbeat_idle_after,
            max_concurrent_reloads: cfg.max_concurrent_reloads as usize,
            lease_ttl: cfg.reload_lease_ttl,
            instance: cfg.instance.clone(),
            publication_name: cfg.publication_name.clone(),
            epoch,
        },
        token.clone(),
    )
    .await
    .context("spawn reload controller")?;

    let result = consume::run_decode_loop(
        &mut stream,
        token.clone(),
        &mut cache,
        &mut router,
        &sink,
        &mut checkpoint,
        &mut demux,
        &mut ddl,
        &mut heartbeat,
        &state,
        &ctx.control_pool,
        epoch,
        SCHEMA_VERSION,
        &waiters,
    )
    .await;

    // Whatever ended the loop (SIGTERM, stream end, or a decode error), drain the side tasks.
    state.mark_terminating();
    token.cancel();
    reload_controller
        .await
        .context("reload controller task join")?;
    tracing::info!("draining health server");
    server
        .await
        .context("health server task join")?
        .context("health server")?;
    result
}

/// The established streaming state after the bootstrap decision.
struct Bootstrapped {
    stream: ReplicationStream,
    epoch: i64,
    start_lsn: common::Lsn,
    sink: pg_sink::sink::ParquetSink,
}

/// Resume a pre-existing slot, or first-bootstrap with an exported snapshot + backfill (§1.7 / §1.8).
/// The sink is epoch-namespaced, so it is built here once the epoch is known.
async fn establish_stream(
    cfg: &SinkConfig,
    ctx: &bootstrap::BootstrapCtx,
    cache: &mut pg_sink::relcache::RelationCache,
    triggers: pg_sink::batch::BatchTriggers,
    schema_version: i64,
) -> anyhow::Result<Bootstrapped> {
    let make_sink = |epoch| {
        pg_sink::sink::ParquetSink::new(
            ctx.object_store.clone(),
            cfg.object_store.bucket.clone(),
            epoch,
        )
    };

    // Classify the slot on the (already-connected) source before deciding: resume a healthy slot, or —
    // only when the catalog authoritatively says the slot is gone — open a fresh one. A connection
    // hiccup (`Unreachable`) is NOT slot loss: exit so the orchestrator's backoff-restart reconnects,
    // never a total-restart (§1.8, the false-positive guard).
    let status = pg_sink::epoch::classify_slot(&ctx.source_client, &cfg.slot_name).await;
    match pg_sink::epoch::decide(&status) {
        pg_sink::epoch::SlotAction::Retry => {
            anyhow::bail!(
                "could not classify replication slot {} (source connection lost mid-bootstrap) — \
                 exiting to retry via backoff; this is NOT slot loss and does NOT bump the epoch",
                cfg.slot_name
            );
        }
        pg_sink::epoch::SlotAction::Resume { confirmed_flush } => {
            // Resume: stream from confirmed_flush_lsn; hydrate the relation cache from schema_registry.
            let epoch =
                current_or_new_epoch(&ctx.control_pool, &cfg.slot_name, confirmed_flush).await?;
            let rows = control::read_all_latest_registry(&ctx.control_pool, epoch)
                .await
                .context("read schema_registry for hydration")?;
            cache.hydrate(rows).context("hydrate relation cache")?;
            tracing::info!(
                epoch,
                cached_relations = cache.len(),
                "relation cache hydrated (resume)"
            );
            let stream = ReplicationStream::start(
                &cfg.source_db_url,
                &cfg.slot_name,
                confirmed_flush,
                &cfg.publication_name,
            )
            .await
            .context("START_REPLICATION (resume)")?;
            return Ok(Bootstrapped {
                stream,
                epoch,
                start_lsn: confirmed_flush,
                sink: make_sink(epoch),
            });
        }
        pg_sink::epoch::SlotAction::FreshSlot => { /* fall through to fresh-slot + backfill below */
        }
    }

    // Fresh slot: create it with an exported snapshot and backfill before streaming. This is the FIRST
    // bootstrap when no prior epoch exists, or a TOTAL-RESTART (§1.8) when the slot was lost/absent while
    // a generation was running — `bump_epoch` yields `1` on an empty table and `MAX+1` otherwise, so a
    // single path serves both; we distinguish them only to alert loudly on the disaster case.
    let mut snap = pg_sink::snapshot::SnapshotConn::connect(&cfg.source_db_url)
        .await
        .context("open snapshot replication connection")?;
    let snapshot = snap
        .create_slot_with_snapshot(&cfg.slot_name)
        .await
        .context("CREATE_REPLICATION_SLOT with exported snapshot")?;
    let prior = control::read_current_epoch(&ctx.control_pool)
        .await
        .context("read prior epoch")?;
    let epoch = control::bump_epoch(
        &ctx.control_pool,
        &cfg.slot_name,
        snapshot.consistent_point,
        "streaming",
    )
    .await
    .context("open new epoch")?;
    match &prior {
        Some(p) => tracing::error!(
            old_epoch = p.epoch,
            new_epoch = epoch,
            slot = %cfg.slot_name,
            slot_status = ?status,
            "TOTAL-RESTART: the replication slot was lost/absent — bumping the epoch and re-snapshotting \
             ALL tables under the new generation; old-epoch S3 is left to its lifecycle TTL"
        ),
        None => tracing::info!(epoch, "first bootstrap: created slot + established epoch"),
    }
    let sink = make_sink(epoch);

    // Backfill every published user table under the exported snapshot, registering each shape so the
    // subsequent streaming decode (and the loader) have it. Internal walrus tables are excluded.
    let tables =
        pg_sink::snapshot::published_user_tables(&ctx.source_client, &cfg.publication_name)
            .await
            .context("list published user tables")?;
    let mut backfill = pg_sink::snapshot::Backfill::connect(
        &cfg.source_db_url,
        epoch,
        cfg.instance.clone(),
        triggers,
        cfg.backfill_statement_timeout,
    )
    .await
    .context("open backfill connection")?;
    let mut total = 0u64;
    for (schema, table) in &tables {
        let rel = pg_sink::snapshot::describe_source_relation(&ctx.source_client, schema, table)
            .await
            .with_context(|| format!("describe {schema}.{table} for backfill"))?;
        consume::on_relation(cache, &ctx.control_pool, epoch, rel.clone(), schema_version)
            .await
            .context("register backfilled relation")?;
        total += backfill
            .copy_table(&rel, &snapshot, &sink, &ctx.control_pool, schema_version)
            .await
            .with_context(|| format!("backfill {schema}.{table}"))?;
    }
    tracing::info!(
        epoch,
        tables = tables.len(),
        rows = total,
        consistent_point = %snapshot.consistent_point,
        "backfill complete; handing off to streaming"
    );

    // Hand off: START_REPLICATION from consistent_point on the (now snapshot-done) connection.
    let stream = snap
        .into_stream(&cfg.slot_name, &cfg.publication_name)
        .await
        .context("hand off snapshot → streaming")?;
    Ok(Bootstrapped {
        stream,
        epoch,
        start_lsn: snapshot.consistent_point,
        sink,
    })
}

/// Resume the current epoch generation (§1.8), or establish the first one for this slot. Epoch bump /
/// total-restart is PR 4.6.
async fn current_or_new_epoch(
    pool: &sqlx::PgPool,
    slot_name: &str,
    created_lsn: common::Lsn,
) -> anyhow::Result<i64> {
    if let Some(state) = control::read_current_epoch(pool)
        .await
        .context("read current epoch")?
    {
        return Ok(state.epoch);
    }
    let state = control::ReplicationState {
        epoch: 1,
        slot_name: slot_name.to_string(),
        created_lsn,
        status: "streaming".to_string(),
    };
    control::insert_epoch(pool, &state)
        .await
        .context("insert first epoch")?;
    tracing::info!(epoch = 1, "established first epoch");
    Ok(1)
}
