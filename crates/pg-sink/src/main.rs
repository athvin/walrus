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
use pg_sink::{bootstrap, consume, health, shutdown, slot};
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

    // Establish the replication stream and run the decode loop until SIGTERM (PR 2.21). Slot
    // management is over the preflight connection; the streaming is the hand-rolled connection.
    let resume = slot::verify_or_create_slot(&ctx.source_client, &cfg.slot_name)
        .await
        .context("verify/create replication slot")?;

    // Epoch: the generation that namespaces control-plane state (§1.8). Resume the current one, or
    // establish the first. Schema-version bumps arrive with DDL capture (PR 2.33); until then, 1.
    let epoch = current_or_new_epoch(&ctx.control_pool, &cfg.slot_name, resume.start_lsn()).await?;
    const SCHEMA_VERSION: i64 = 1;

    // Shared bootstrap step 7: hydrate the relation cache from schema_registry so a restart resumes.
    let mut cache = pg_sink::relcache::RelationCache::default();
    let rows = control::read_all_latest_registry(&ctx.control_pool, epoch)
        .await
        .context("read schema_registry for hydration")?;
    cache.hydrate(rows).context("hydrate relation cache")?;
    tracing::info!(
        epoch,
        cached_relations = cache.len(),
        "relation cache hydrated"
    );

    let mut stream = ReplicationStream::start(
        &cfg.source_db_url,
        &cfg.slot_name,
        resume.start_lsn(),
        &cfg.publication_name,
    )
    .await
    .context("START_REPLICATION")?;
    tracing::info!(
        slot = %cfg.slot_name,
        start_lsn = %resume.start_lsn(),
        "streaming logical replication"
    );

    let triggers = pg_sink::batch::BatchTriggers {
        max_fill: cfg.max_fill,
        max_rows: cfg.max_rows,
        max_bytes: cfg.max_bytes,
    };
    let mut router = consume::BatchRouter::new(
        triggers,
        std::sync::Arc::new(pg_sink::batch::SystemClock),
        epoch,
        cfg.instance.clone(),
    );
    let sink = pg_sink::sink::ParquetSink::new(
        ctx.object_store.clone(),
        cfg.object_store.bucket.clone(),
        epoch,
    );
    let mut checkpoint = pg_sink::checkpoint::DurabilityCheckpoint::new(resume.start_lsn());

    let result = consume::run_decode_loop(
        &mut stream,
        token.clone(),
        &mut cache,
        &mut router,
        &sink,
        &mut checkpoint,
        &ctx.control_pool,
        epoch,
        SCHEMA_VERSION,
    )
    .await;

    // Whatever ended the loop (SIGTERM, stream end, or a decode error), drain the health server.
    state.mark_terminating();
    token.cancel();
    tracing::info!("draining health server");
    server
        .await
        .context("health server task join")?
        .context("health server")?;
    result
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
