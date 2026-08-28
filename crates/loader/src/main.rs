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
use loader::config::{LeaseTtl, LoaderConfig};
use loader::error::LoaderError;
use loader::health::{self, LoaderState};
use loader::lease;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use std::process::ExitCode;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

// The pre-subscriber window, and the only stderr in this binary: config validation and
// `init_tracing` both run before any `tracing` event has a subscriber to reach, so their failures
// would be silent as events. Everything from the runtime build down is a `tracing` event.
#[allow(
    clippy::print_stderr,
    reason = "config and tracing-init failures precede the subscriber they would otherwise log to"
)]
fn main() -> ExitCode {
    let cfg = match LoaderConfig::load() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("walrus-loader: invalid loader configuration: {e}");
            return common::ExitCode::Config.into();
        }
    };
    if let Err(e) = common::init_tracing(&cfg.telemetry) {
        eprintln!("walrus-loader: tracing init failed: {e}");
        return common::ExitCode::Internal.into();
    }
    // The multi-thread FLAVOR is load-bearing; the worker count is not. `block_on` drives the
    // pipeline — and with it the `LocalSet`'s apply loops — on THIS thread, which a full rebuild's
    // `CREATE OR REPLACE` blocks synchronously for seconds. Everything `tokio::spawn`ed (health
    // server, lease renewer, epoch watch, and `compaction::full_rebuild_abortable`'s interrupt
    // watcher) lives on the worker pool instead, so it keeps running across that block: the lease
    // stays renewed and SIGTERM still aborts the rewrite. `new_current_thread` would put all of
    // it on the blocked thread and lose both. One worker suffices — it is a thread of its own.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("walrus-loader")
        .worker_threads(common::runtime::resolve_worker_threads(cfg.worker_threads))
        .max_blocking_threads(common::runtime::MAX_BLOCKING_THREADS)
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(error = %e, "failed to build tokio runtime");
            return common::ExitCode::Internal.into();
        }
    };
    match runtime.block_on(run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            // `?e`, not `%e`: this is the one place a failure is HANDLED, and `LoaderError`'s
            // `Duck`/`ControlTxn`/`RegistryDecode`/`LsnParse`/`Health` variants deliberately name only
            // the operation in `Display`, keeping the engine/driver failure in `#[source]` (see
            // `error_test.rs`). `Debug` is what walks that chain into the log — `%e` would exit on
            // "DuckDB: append …" with the reason nowhere on disk.
            tracing::error!(error = ?e, "walrus-loader exiting");
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
    let store = build_store(cfg)?;

    // `&store` unsize-coerces to the `&dyn ObjectStore` bootstrap takes, so the concrete client is
    // erased at that one boundary and nowhere else.
    let owned = bootstrap::bootstrap(cfg, &pool, &store, state).await?;
    state.mark_ready();
    let keys: Vec<(String, String)> = owned.iter().map(bootstrap::OwnedTable::to_key).collect();
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

    // Keep the lease alive off the apply thread until SIGTERM. `load()` already admitted this TTL;
    // re-parsing it here is what hands the renewer the proof instead of a bare duration.
    let renewer = lease::spawn_renewer(
        pool.clone(),
        epoch,
        keys.clone(),
        cfg.instance.clone(),
        LeaseTtl::new(cfg.lease_ttl)?,
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
    // A `JoinSet` rather than a `Vec<JoinHandle>`: the worker count is whatever bootstrap owns, so
    // this is a dynamic collection, and the set owns the whole fleet as one value. The drain below
    // then reaps each worker in COMPLETION order instead of index order (a panic is logged when it
    // happens, not after every earlier worker has stopped), and an early exit out of this function
    // aborts the survivors instead of detaching them.
    let mut workers = tokio::task::JoinSet::new();
    for o in owned {
        // `apply_loop` consumes the ctx, so the failure report needs names of its own: these two
        // locals are the `async move` block's captures. Only ONE side clones — `series` is built
        // while both names are still borrowable, then the ctx takes `o`'s originals, because this
        // `OwnedTable` dies with the iteration and has nothing left to keep them for.
        let schema = o.schema.clone();
        let table = o.table.clone();
        let series = format!("{}.{}", o.schema, o.table);
        let ctx = loader::phase_a::TableCtx {
            pool: pool.clone(),
            epoch,
            epoch_rx: epoch_rx.clone(),
            schema: o.schema,
            table: o.table,
            series,
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
        // `spawn_local_on`, not `spawn_local`: this `LocalSet` is not running yet — these tasks
        // start when `run_until` below first drives it, exactly as they did before.
        workers.spawn_local_on(
            async move {
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
            },
            &local,
        );
    }
    // Only workers own receivers now; with zero workers, this also stops the poller immediately.
    drop(epoch_rx);
    // The receiver closes when the final worker exits; main must not keep an extra sender alive.
    drop(failures_tx);
    let first_failure = local
        .run_until(loader::supervisor::supervise(failures_rx, token, async move {
            // Once the supervisor sees a failure it cancels `token`, so healthy workers leave
            // their loops and this drain always makes progress. It must run to completion:
            // `supervise` only returns when this future does, and the lease release below depends
            // on every worker having been joined (see the drop-order note).
            while let Some(joined) = workers.join_next().await {
                if let Err(error) = joined {
                    tracing::error!(%error, "apply worker panicked");
                }
            }
        }))
        .await;

    // DROP ORDER, load-bearing (see `loader::shutdown` steps 4-5): each `join_next` above joined a
    // worker task, which dropped its `TableCtx` — and with it the `TableDb` whose drop closes the
    // `.duckdb` and releases DuckDB's writer lock. Only then is the lease released, so the two fences
    // come off in the reverse of their bootstrap order (lease → file lock, so file lock → lease).
    // Handing a replacement the lease first would only cost it a failed open and a retry, but this
    // order leaves no such window — so keep `release_all` after the joins, never beside them.
    tracing::info!("workers drained (files closed); releasing leases");
    if let Err(error) = epoch_watch.await {
        tracing::error!(%error, "epoch watch task panicked");
    }
    // Cancel-then-join, never `abort()`: the renewer already observes this token, and `abort()` only
    // *requests* a stop — it returns while an in-flight renewal may still be on the wire. That matters
    // because `release_lease` merely pushes `lease_expiry` into the past and leaves `owner_pod` set,
    // while `renew_lease` is guarded on the owner alone: a renewal landing AFTER the release would
    // push the expiry a whole TTL forward, and the successor's `acquire` reads that as a live owner —
    // a terminal `LeaseContended`, not a wait. Awaiting the cancelled renewer is what proves no
    // renewal can outlive this line. It is bounded by the same control-DB latency `release_all` is
    // about to pay anyway (the renewer's cancellation arm is `biased`, so at most the current round).
    token.cancel();
    if let Err(error) = renewer.await {
        tracing::error!(%error, "lease renewer task panicked");
    }
    lease::release_all(&pool, epoch, &keys, &cfg.instance).await;
    if let Some(failure) = first_failure {
        return Err(failure.error);
    }
    Ok(())
}

/// The loader's one object-store client.
///
/// Returns the concrete `AmazonS3`, not an `Arc<dyn ObjectStore>`: the loader builds exactly one
/// store, spends it on a single `head` probe in [`bootstrap::bootstrap`], and never clones, shares,
/// or stores it — so the `Arc` allocation and the vtable would both buy nothing here. `pg-sink`
/// keeps its `Arc<dyn ObjectStore>` for the opposite reason: `object_store::buffered::BufWriter::new`
/// takes one by value, so the erasure there is the upstream API's, not a choice.
fn build_store(cfg: &LoaderConfig) -> Result<AmazonS3, LoaderError> {
    // `from_env` so the AWS credential env (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`) is honoured —
    // `new()` alone falls back to the EC2 IMDS credential chain, which hangs/fails off-EC2 (e.g. MinIO).
    let mut b = AmazonS3Builder::from_env()
        .with_bucket_name(&cfg.object_store.bucket)
        .with_region(&cfg.object_store.region);
    if let Some(endpoint) = &cfg.object_store.endpoint {
        b = b.with_endpoint(endpoint).with_allow_http(true);
    }
    b.build()
        .map_err(|e| LoaderError::ObjectStore(format!("build S3 client: {e}")))
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
