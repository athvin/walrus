//! The `walrus-pg-sink` binary — the pod lifecycle shell, and nothing else.
//!
//! Everything a test could reach lives in the `pg_sink` library; this file keeps only the four steps
//! that cannot. `main` loads+validates config, inits tracing (the one install a process owns),
//! builds the runtime, and does the **only** `anyhow::Error → ExitCode` mapping in the whole binary
//! (the "context in the loop, exit code at `main`" idiom — a broken deploy is greppable in
//! `kubectl logs`). The lifecycle itself is [`pg_sink::app::run`], which returns
//! `anyhow::Result<()>`; the application boundary recovers each typed failure's distinct exit code.

use pg_sink::config::SinkConfig;
use std::process::ExitCode;

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

    match runtime.block_on(pg_sink::app::run(cfg)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %format_args!("{e:#}"), "walrus-pg-sink exiting");
            pg_sink::exit::code_for(&e).into()
        }
    }
}
