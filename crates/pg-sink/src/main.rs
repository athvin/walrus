//! The `walrus-pg-sink` binary — the pod lifecycle shell.
//!
//! `main` stays tiny: load+validate config, init tracing, build the runtime, and do the **only**
//! `anyhow::Error → ExitCode` mapping in the whole binary (the "context in the loop, exit code at
//! `main`" idiom — a broken deploy is greppable in `kubectl logs`). Everything below `main` returns
//! `anyhow::Result<_>`; a bootstrap failure carries a `common::Error` whose distinct exit code is
//! recovered here by downcast.

use anyhow::Context;
use pg_sink::config::SinkConfig;
use pg_sink::{bootstrap, health, shutdown};
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
    if let Err(e) = bootstrap::run_shared(&cfg, deadline).await {
        token.cancel();
        let _ = server.await;
        return Err(e.into());
    }

    state.mark_ready();
    tracing::info!("bootstrap complete; ready — awaiting shutdown signal");

    // The replication loop will run here (PR 2.20+). For now, hold the pod open until SIGTERM.
    token.cancelled().await;
    state.mark_terminating();
    tracing::info!("shutdown signal received; draining health server");
    server
        .await
        .context("health server task join")?
        .context("health server")?;
    Ok(())
}
