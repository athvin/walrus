// The loader runs one worker per `.duckdb` file on a `LocalSet`. The pipeline owns that LocalSet
// and drives futures borrowing Send + !Sync `TableDb` values, so it is intentionally !Send.
#![allow(
    clippy::future_not_send,
    reason = "the loader pipeline owns a LocalSet for futures borrowing Send + !Sync TableDb values"
)]

//! The `walrus-loader` binary — the pod lifecycle shell. `main` stays tiny: load+validate config, init
//! tracing, build the runtime, and do the **only** error → `ExitCode` mapping (context in the loop,
//! exit code at `main`). Everything below returns [`LoaderError`], whose distinct exit code `main`
//! surfaces so a broken deploy is greppable in `kubectl logs`.

use common::FailureClass;
use loader::bootstrap;
use loader::config::LoaderConfig;
use loader::error::LoaderError;
use loader::health::{self, LoaderState};
use loader::lease;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
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
        .thread_name("walrus-loader")
        .worker_threads(common::runtime::resolve_worker_threads(cfg.worker_threads))
        .max_blocking_threads(common::runtime::MAX_BLOCKING_THREADS)
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
    let token = loader::shutdown::install_signal_handlers();
    let state = LoaderState::new();
    // Install the Prometheus recorder before anything can serve /metrics or emit a series (PR 4.10).
    common::metrics::init();

    // Bind health *before* bootstrap so `/startup` answers 503 while the lease + DuckDB open proceed.
    let listener = tokio::net::TcpListener::bind(cfg.health_addr)
        .await
        .map_err(|source| LoaderError::Health {
            op: "bind",
            source: Box::new(source),
        })?;
    tracing::info!(addr = %cfg.health_addr, "health endpoints listening; bootstrapping");
    let server = tokio::spawn(health::serve_on(
        listener,
        Arc::clone(&state),
        token.clone(),
    ));

    let result = loader::shutdown::cancel_on_exit(&token, pipeline(&cfg, &token, &state)).await;
    server
        .await
        .map_err(|source| LoaderError::Health {
            op: "join",
            source: Box::new(source),
        })?
        .map_err(|source| LoaderError::Health {
            op: "serve",
            source: source.into(),
        })?;
    result
}

/// The fallible middle of the loader lifecycle. The caller holds a cancellation drop guard for
/// this future's entire lifetime, so every early return winds down token-driven side tasks.
async fn pipeline(
    cfg: &LoaderConfig,
    token: &CancellationToken,
    state: &Arc<LoaderState>,
) -> Result<(), LoaderError> {
    let pool = control::connect(&cfg.control_db_url).await?;
    let store: Arc<dyn ObjectStore> = build_store(cfg)?;

    let owned = bootstrap::bootstrap(cfg, &pool, store.as_ref(), state).await?;
    state.mark_ready();
    let keys: Vec<(String, String)> = owned.iter().map(|t| t.key()).collect();
    // Zero-init every per-table loader series so /metrics lists the owned tables from the first scrape,
    // before any apply cycle has moved a needle (PR 4.10).
    for (schema, table) in &keys {
        common::metrics::init_table_series(&format!("{schema}.{table}"));
    }
    let epoch = control::read_current_epoch(&pool)
        .await?
        .map(|s| s.epoch)
        .unwrap_or(common::EpochNo(1));
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
    let (epoch_rx, epoch_watch) =
        loader::epoch::spawn_epoch_watch(pool.clone(), epoch, cfg.poll_interval, token.clone());

    // Configure DuckDB's httpfs on every owned file so `read_parquet('s3://…')` (Phase A) has the
    // staging-bucket credentials — the binary's equivalent of what the compose tests set up by hand.
    let s3 = duck_s3_access(cfg);
    for o in &owned {
        o.db.configure_s3(&s3)?;
    }

    // One apply loop per owned table. DuckDB's `Connection` is `Send + !Sync` because it holds
    // an interior `RefCell`, so a future holding `&TableCtx` is not `Send` and cannot go to
    // `tokio::spawn`. The loops run on a `LocalSet` (this thread), the whole parallelism model being
    // one worker task per `.duckdb` file. Those tasks all share this one driver thread, so a long
    // compaction stalls its siblings; see
    // docs/implementation/notes/rust-skills/async-spawn-blocking.md.
    let local = tokio::task::LocalSet::new();
    let (failures_tx, failures_rx) = loader::supervisor::failure_channel(keys.len());
    let handles: Vec<_> = owned
        .into_iter()
        .map(|o| {
            let schema = o.schema.clone();
            let table = o.table.clone();
            let ctx = loader::phase_a::TableCtx {
                pool: pool.clone(),
                epoch,
                epoch_rx: epoch_rx.clone(),
                schema: o.schema.clone(),
                table: o.table.clone(),
                series: format!("{}.{}", o.schema, o.table),
                rel: o.relation,
                db: o.db,
                state: Arc::clone(state),
                max_files: cfg.max_files_per_cycle,
                poll_interval: cfg.poll_interval,
                compaction_interval: cfg.compaction_interval,
                retention_lsn_lag: cfg.retention_lsn_lag,
                pause_logged: Default::default(),
                resync_ids: Default::default(),
            };
            let worker_token = token.clone();
            let failures_tx = failures_tx.clone();
            local.spawn_local(async move {
                if let Err(error) = loader::apply_loop::apply_loop(ctx, worker_token).await {
                    loader::supervisor::report(
                        &failures_tx,
                        loader::supervisor::WorkerFailure {
                            schema,
                            table,
                            error,
                        },
                    );
                }
            })
        })
        .collect();
    // Only workers own receivers now; with zero workers, this also stops the poller immediately.
    drop(epoch_rx);
    // The receiver closes when the final worker exits; main must not keep an extra sender alive.
    drop(failures_tx);
    let first_failure = local
        .run_until(loader::supervisor::supervise(failures_rx, token, async {
            // Once the supervisor sees a failure it cancels `token`, so healthy workers leave
            // their loops and this deliberately sequential drain always makes progress.
            for h in handles {
                if let Err(error) = h.await {
                    tracing::error!(%error, "apply worker panicked");
                }
            }
        }))
        .await;

    tracing::info!("SIGTERM: releasing leases and draining");
    if let Err(error) = epoch_watch.await {
        tracing::error!(%error, "epoch watch task panicked");
    }
    renewer.abort();
    lease::release_all(&pool, epoch, &keys, &cfg.instance).await;
    if let Some(failure) = first_failure {
        return Err(failure.error);
    }
    Ok(())
}

fn build_store(cfg: &LoaderConfig) -> Result<Arc<dyn ObjectStore>, LoaderError> {
    // `from_env` so the AWS credential env (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) is honoured —
    // `new()` alone falls back to the EC2 IMDS credential chain, which hangs/fails off-EC2 (e.g. MinIO).
    let mut b = AmazonS3Builder::from_env()
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

/// DuckDB httpfs credentials for `read_parquet('s3://…')`, from the object-store config + the AWS env
/// (the same `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` the `object_store` client reads). DuckDB wants a
/// scheme-less `host:port` endpoint; the scheme selects TLS.
fn duck_s3_access(cfg: &LoaderConfig) -> loader::duck::S3Access {
    let raw = cfg.object_store.endpoint.as_deref().unwrap_or_default();
    let (use_ssl, endpoint) = match raw.strip_prefix("https://") {
        Some(host) => (true, host),
        None => (false, raw.strip_prefix("http://").unwrap_or(raw)),
    };
    loader::duck::S3Access {
        endpoint: endpoint.to_string(),
        region: cfg.object_store.region.clone(),
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default(),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default(),
        use_ssl,
    }
}
