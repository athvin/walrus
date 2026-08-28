//! The `walrus-loader` binary — the pod lifecycle shell, and nothing else. Everything a test could
//! reach lives in the `loader` library; this file keeps only the four steps that cannot. `main`
//! loads+validates config, inits tracing (the one install a process owns), builds the runtime, and
//! does the **only** error → `ExitCode` mapping (context in the loop, exit code at `main`). The
//! lifecycle itself is [`loader::app::run`], whose `LoaderError` carries the distinct exit code
//! `main` surfaces so a broken deploy is greppable in `kubectl logs`.

use common::FailureClass;
use loader::config::LoaderConfig;
use std::process::ExitCode;

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
    match runtime.block_on(loader::app::run(cfg)) {
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
