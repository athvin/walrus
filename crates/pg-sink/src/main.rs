//! The `walrus-pg-sink` binary — the pod lifecycle shell.
//!
//! `main` stays tiny: load+validate config, init tracing, build the runtime, and do the **only**
//! `anyhow::Error → ExitCode` mapping in the whole binary (the "context in the loop, exit code at
//! `main`" idiom — a broken deploy is greppable in `kubectl logs`). Everything below `main` returns
//! `anyhow::Result<_>`; the application boundary recovers each typed failure's distinct exit code.

use anyhow::Context;
use common::EpochNo;
use pg_sink::config::SinkConfig;
use pg_sink::replication::ReplicationStream;
use pg_sink::{bootstrap, consume, health, shutdown};
use std::num::NonZeroUsize;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::time::Instant;

// The pre-subscriber window, and the only stderr in this binary: config validation and
// `init_tracing` both run before any `tracing` event has a subscriber to reach, so their failures
// would be silent as events. Everything from the runtime build down is a `tracing` event.
#[allow(
    clippy::print_stderr,
    reason = "config and tracing-init failures precede the subscriber they would otherwise log to"
)]
fn main() -> ExitCode {
    // Step 1: config. Terminal on failure — before tracing exists, so report on stderr.
    let Ok(cfg) =
        SinkConfig::load().inspect_err(|e| eprintln!("walrus-pg-sink: invalid configuration: {e}"))
    else {
        return common::ExitCode::Config.into();
    };
    if let Err(e) = common::init_tracing(&cfg.telemetry) {
        eprintln!("walrus-pg-sink: tracing init failed: {e}");
        return common::ExitCode::Internal.into();
    }

    let Ok(runtime) = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("walrus-sink")
        .worker_threads(common::runtime::resolve_worker_threads(cfg.worker_threads))
        .max_blocking_threads(common::runtime::MAX_BLOCKING_THREADS)
        .build()
        .inspect_err(|e| tracing::error!(error = %e, "failed to build tokio runtime"))
    else {
        return common::ExitCode::Internal.into();
    };

    match runtime.block_on(run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %format_args!("{e:#}"), "walrus-pg-sink exiting");
            pg_sink::exit::code_for(&e).into()
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
    let server = tokio::spawn(health::serve_on(
        listener,
        Arc::clone(&state),
        token.clone(),
    ));

    let result = shutdown::cancel_on_exit(&token, pipeline(&cfg, &token, &state)).await;
    tracing::info!("draining health server");
    server
        .await
        .context("health server task join")?
        .context("health server")?;
    result
}

/// The fallible middle of the sink lifecycle. The caller holds a cancellation drop guard for this
/// future's entire lifetime, so every early return winds down token-driven side tasks.
async fn pipeline(
    cfg: &SinkConfig,
    token: &tokio_util::sync::CancellationToken,
    state: &health::HealthState,
) -> anyhow::Result<()> {
    // Shared bootstrap steps 2–4. The enclosing drop guard tears down token-driven tasks before a
    // classified error reaches `main`.
    let deadline = Instant::now() + cfg.startup_deadline;
    let ctx = bootstrap::run_shared(cfg, deadline).await?;

    state.mark_ready();
    tracing::info!("bootstrap complete; ready");

    const SCHEMA_VERSION: common::SchemaVersionNo = common::SchemaVersionNo(1);
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
    } = establish_stream(cfg, &ctx, &mut cache, triggers, SCHEMA_VERSION).await?;
    tracing::info!(slot = %cfg.slot_name, start_lsn = %start_lsn, epoch = %epoch, "streaming logical replication");

    // ONE wall clock for the whole decode path: the router's and the demux's batchers share it by
    // `Arc` (see `batch::Clock`), so the test seam has a single instant source, not two.
    let clock = Arc::new(pg_sink::batch::SystemClock);
    let mut router = consume::BatchRouter::new(triggers, Arc::clone(&clock), epoch, &cfg.instance);
    let mut checkpoint = pg_sink::checkpoint::DurabilityCheckpoint::new(start_lsn);
    // Large-transaction demux (§1.6): a txn over logical_decoding_work_mem streams before its commit.
    let mut demux = pg_sink::stream_txn::StreamDemux::new(
        triggers,
        clock,
        epoch,
        &cfg.instance,
        cfg.max_inflight_bytes,
    );

    // The idle heartbeat rides a SEPARATE ordinary SQL connection (distinct from replication); its
    // beat writes the published `walrus.heartbeat`, whose round-trip through the stream advances the
    // slot on an otherwise-idle publication (§1.9).
    let mut heartbeat = pg_sink::heartbeat::Heartbeat::connect(
        &cfg.source_db_url,
        &cfg.instance,
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
        Arc::clone(&waiters),
        sink.clone(),
        pg_sink::reload::ReloadControllerConfig {
            poll_interval: cfg.heartbeat_idle_after,
            // Narrowing stays inside the non-zero domain: only the 64-bit magnitude can fail to fit
            // a `usize`, never the "at least one exporter" invariant the config already proved.
            max_concurrent_reloads: NonZeroUsize::try_from(cfg.max_concurrent_reloads)
                .context("max_concurrent_reloads does not fit usize")?,
            lease_ttl: cfg.reload_lease_ttl,
            instance: cfg.instance.clone(),
            publication_name: cfg.publication_name.clone(),
            epoch,
            chunk_rows: cfg.reload_chunk_rows,
            echo_timeout: cfg.reload_echo_timeout,
            reload_max_restarts: cfg.reload_max_restarts,
        },
        token.clone(),
    );

    let result = consume::DecodeLoop::builder()
        .stream(&mut stream)
        .token(token.clone())
        .cache(&mut cache)
        .router(&mut router)
        .sink(&sink)
        .checkpoint(&mut checkpoint)
        .demux(&mut demux)
        .ddl(&mut ddl)
        .heartbeat(&mut heartbeat)
        .health(state)
        .pool(&ctx.control_pool)
        .epoch(epoch)
        .waiters(&waiters)
        .build()
        .context("wire the decode loop")?
        .run()
        .await;

    // Whatever ended the loop (SIGTERM, stream end, or a decode error), drain the side tasks.
    state.mark_terminating();
    token.cancel();
    reload_controller
        .await
        .context("reload controller task join")?;
    result
}

/// The established streaming state after the bootstrap decision.
struct Bootstrapped {
    stream: ReplicationStream,
    epoch: EpochNo,
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
    schema_version: common::SchemaVersionNo,
) -> anyhow::Result<Bootstrapped> {
    let make_sink = |epoch| {
        pg_sink::sink::ParquetSink::new(
            Arc::clone(&ctx.object_store),
            cfg.object_store.bucket.clone(),
            epoch,
        )
    };

    // One parse, at the top of the one function that names the slot: every use below reads this
    // proven value rather than the raw config text. `SinkConfig::validate` ran the same gate at load,
    // so a config that booted cannot fail here — this is the handoff, not a second check.
    let slot = cfg
        .slot()
        .context("parse slot_name for the replication commands")?;

    // Classify the slot on the (already-connected) source before deciding: resume a healthy slot, or —
    // only when the catalog authoritatively says the slot is gone — open a fresh one. A connection
    // hiccup (`Unreachable`) is NOT slot loss: exit so the orchestrator's backoff-restart reconnects,
    // never a total-restart (§1.8, the false-positive guard).
    let status = pg_sink::epoch::classify_slot(&ctx.source_client, slot.as_str()).await;
    match pg_sink::epoch::decide(status) {
        pg_sink::epoch::SlotAction::Retry => {
            anyhow::bail!(
                "could not classify replication slot {slot} (source connection lost mid-bootstrap) \
                 — exiting to retry via backoff; this is NOT slot loss and does NOT bump the epoch"
            );
        }
        pg_sink::epoch::SlotAction::Resume { confirmed_flush } => {
            // Resume: stream from confirmed_flush_lsn; hydrate the relation cache from schema_registry.
            let epoch =
                current_or_new_epoch(&ctx.control_pool, slot.as_str(), confirmed_flush).await?;
            // The hydration read (control PG) and START_REPLICATION (the source) touch different
            // servers and neither consumes the other's output, so a resume costs the slower of the
            // two instead of their sum — the same 503-window argument as `bootstrap::run_shared`.
            // Opening the stream before the cache is hydrated is safe: nothing polls it until the
            // decode loop, so no frame can arrive ahead of the shapes it needs.
            let (rows, stream) = tokio::try_join!(
                async {
                    control::read_all_latest_registry(&ctx.control_pool, epoch)
                        .await
                        .context("read schema_registry for hydration")
                },
                async {
                    ReplicationStream::start(
                        &cfg.source_db_url,
                        slot.as_str(),
                        confirmed_flush,
                        &cfg.publication_name,
                    )
                    .await
                    .context("START_REPLICATION (resume)")
                },
            )?;
            cache.hydrate(rows).context("hydrate relation cache")?;
            tracing::info!(
                epoch = %epoch,
                cached_relations = cache.len(),
                "relation cache hydrated (resume)"
            );
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
    let (snap, snapshot) = pg_sink::snapshot::SnapshotConn::connect(&cfg.source_db_url)
        .await
        .context("open snapshot replication connection")?
        .create_slot_with_snapshot(&slot)
        .await
        .context("CREATE_REPLICATION_SLOT with exported snapshot")?;
    let prior = control::read_current_epoch(&ctx.control_pool)
        .await
        .context("read prior epoch")?;
    let epoch = control::bump_epoch(
        &ctx.control_pool,
        slot.as_str(),
        snapshot.consistent_point,
        "streaming",
    )
    .await
    .context("open new epoch")?;
    match &prior {
        Some(p) => tracing::error!(
            old_epoch = %p.epoch,
            new_epoch = %epoch,
            slot = %slot,
            slot_status = ?status,
            "TOTAL-RESTART: the replication slot was lost/absent — bumping the epoch and re-snapshotting \
             ALL tables under the new generation; old-epoch S3 is left to its lifecycle TTL"
        ),
        None => tracing::info!(epoch = %epoch, "first bootstrap: created slot + established epoch"),
    }
    let sink = make_sink(epoch);

    // Backfill every published user table under the exported snapshot, registering each shape so the
    // subsequent streaming decode (and the loader) have it. Internal walrus tables are excluded.
    // The listing rides the already-preflighted source connection while the backfill session dials
    // its own, so the dial + `SET statement_timeout` overlaps the catalog read instead of following
    // it. Independent by construction: two connections, and neither input feeds the other.
    let (tables, mut backfill) = tokio::try_join!(
        async {
            pg_sink::snapshot::published_user_tables(&ctx.source_client, &cfg.publication_name)
                .await
                .context("list published user tables")
        },
        async {
            pg_sink::snapshot::Backfill::connect(
                &cfg.source_db_url,
                epoch,
                &cfg.instance,
                triggers,
                cfg.backfill_statement_timeout,
            )
            .await
            .context("open backfill connection")
        },
    )?;
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
        epoch = %epoch,
        tables = tables.len(),
        rows = total,
        consistent_point = %snapshot.consistent_point,
        "backfill complete; handing off to streaming"
    );

    // Hand off: START_REPLICATION from consistent_point on the (now snapshot-done) connection.
    let stream = snap
        .into_stream(slot.as_str(), &snapshot, &cfg.publication_name)
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
) -> anyhow::Result<EpochNo> {
    if let Some(state) = control::read_current_epoch(pool)
        .await
        .context("read current epoch")?
    {
        return Ok(state.epoch);
    }
    let state = control::ReplicationState {
        epoch: 1_i64.into(),
        slot_name: slot_name.to_string(),
        created_lsn,
        status: "streaming".to_string(),
    };
    control::insert_epoch(pool, &state)
        .await
        .context("insert first epoch")?;
    tracing::info!(epoch = 1, "established first epoch");
    Ok(EpochNo(1))
}
