//! The `walrus-loader` binary — the pod lifecycle shell. `main` stays tiny: load+validate config, init
//! tracing, build the runtime, and do the **only** error → `ExitCode` mapping (context in the loop,
//! exit code at `main`). Everything below returns [`LoaderError`], whose distinct exit code `main`
//! surfaces so a broken deploy is greppable in `kubectl logs`.

use anyhow::Context;
use loader::bootstrap;
use loader::config::LoaderConfig;
use loader::error::LoaderError;
use loader::health::{self, LoaderState};
use loader::lease;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::process::ExitCode;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

fn main() -> ExitCode {
    let cfg = match LoaderConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("walrus-loader: {e}");
            return common::ExitCode::Config.into();
        }
    };
    if let Err(e) = common::init_tracing(&cfg.telemetry) {
        eprintln!("walrus-loader: tracing init failed: {e}");
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
            tracing::error!("walrus-loader exiting: {e}");
            e.exit_code().into()
        }
    }
}

async fn run(cfg: LoaderConfig) -> Result<(), LoaderError> {
    let token = install_signal_handlers();
    let state = LoaderState::new();

    // Bind health *before* bootstrap so `/startup` answers 503 while the lease + DuckDB open proceed.
    let listener = tokio::net::TcpListener::bind(cfg.health_addr)
        .await
        .map_err(|e| LoaderError::Internal(format!("bind health {}: {e}", cfg.health_addr)))?;
    tracing::info!(addr = %cfg.health_addr, "health endpoints listening; bootstrapping");
    let server = tokio::spawn(health::serve_on(listener, state.clone(), token.clone()));

    let pool = control::connect(&cfg.control_db_url).await?;
    let store: Arc<dyn ObjectStore> = build_store(&cfg)?;

    let owned = match bootstrap::bootstrap(&cfg, &pool, store.as_ref(), &state).await {
        Ok(owned) => owned,
        Err(e) => {
            token.cancel();
            let _ = server.await;
            return Err(e);
        }
    };
    state.mark_ready();
    let keys: Vec<(String, String)> = owned.iter().map(|t| t.key()).collect();
    let epoch = control::read_current_epoch(&pool)
        .await?
        .map(|s| s.epoch)
        .unwrap_or(1);
    tracing::info!(
        tables = keys.len(),
        "bootstrap complete; starting apply loops"
    );

    // Keep the lease alive off the apply thread until SIGTERM.
    let renewer = lease::spawn_renewer(
        pool.clone(),
        epoch,
        keys.clone(),
        cfg.instance.clone(),
        cfg.lease_ttl,
        token.clone(),
    );

    // One apply loop per owned table. DuckDB's `Connection` is `!Send`, so the loops run on a
    // `LocalSet` (this thread), the whole parallelism model being one worker per `.duckdb` file.
    let local = tokio::task::LocalSet::new();
    let handles: Vec<_> = owned
        .into_iter()
        .map(|o| {
            let ctx = loader::phase_a::TableCtx {
                pool: pool.clone(),
                epoch,
                schema: o.schema.clone(),
                table: o.table.clone(),
                rel: o.relation,
                db: o.db,
                state: state.clone(),
                max_files: cfg.max_files_per_cycle,
                poll_interval: cfg.poll_interval,
            };
            local.spawn_local(loader::apply_loop::apply_loop(ctx, token.clone()))
        })
        .collect();
    // Drive the loops until they all exit (each returns on the shutdown token).
    local
        .run_until(async {
            for h in handles {
                if let Ok(Err(e)) = h.await {
                    tracing::error!(error = %e, "apply loop failed");
                    token.cancel();
                }
            }
        })
        .await;

    tracing::info!("SIGTERM: releasing leases and draining");
    renewer.abort();
    lease::release_all(&pool, epoch, &keys, &cfg.instance).await;
    server
        .await
        .map_err(|e| LoaderError::Internal(format!("health server join: {e}")))?
        .map_err(|e| LoaderError::Internal(format!("health server: {e}")))?;
    Ok(())
}

fn build_store(cfg: &LoaderConfig) -> Result<Arc<dyn ObjectStore>, LoaderError> {
    let mut b = AmazonS3Builder::new()
        .with_bucket_name(&cfg.object_store.bucket)
        .with_region(&cfg.object_store.region);
    if let Some(endpoint) = &cfg.object_store.endpoint {
        b = b.with_endpoint(endpoint).with_allow_http(true);
    }
    let store = b
        .build()
        .map_err(|e| LoaderError::ObjectStore(format!("build S3 client: {e}")))?;
    Ok(Arc::new(store))
}

/// SIGTERM/SIGINT → cancel one shared token.
fn install_signal_handlers() -> CancellationToken {
    use tokio::signal::unix::{signal, SignalKind};
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        let mut term = signal(SignalKind::terminate()).context("SIGTERM").unwrap();
        let mut int = signal(SignalKind::interrupt()).context("SIGINT").unwrap();
        tokio::select! {
            _ = term.recv() => tracing::info!("SIGTERM received"),
            _ = int.recv() => tracing::info!("SIGINT received"),
            _ = child.cancelled() => {}
        }
        child.cancel();
    });
    token
}
