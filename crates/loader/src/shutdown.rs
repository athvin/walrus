//! The loader's graceful SIGTERM drain (loader §8.5) — the mirror image of the sink's WAL drain.
//!
//! On `SIGTERM` each per-table worker drains **in order**, all keyed off one [`CancellationToken`]:
//! 1. **stop claiming** new files (the worker observes the token at the top of its poll loop);
//! 2. **finish the in-flight Phase A** append + its atomic control-DB txn (`raw_appended_lsn` advance +
//!    manifest `DELETE`) — `run_phase_a` is never interrupted mid-flight, so the two-DB step stays atomic
//!    and opens no new crash window;
//! 3. **finish the in-flight Phase B** transform + commit `transformed_lsn`; and **abort** any in-flight
//!    periodic **full-rebuild** (idempotent self-heal; re-runs next cycle) so it can't blow the grace
//!    budget — see [`crate::compaction::full_rebuild_abortable`];
//! 4. **release the ownership lease** (before closing the file — the file lock is the second fence — but
//!    only *after* the watermarks commit, so a fast replacement can't double-apply the tail);
//! 5. **`CHECKPOINT` and close** the `.duckdb` file so no stale lock is left for the next bootstrap;
//! 6. **never touch the replication slot** — the loader doesn't own it.
//!
//! Every restart is a resume from the two watermarks, so an ungraceful `SIGKILL` is still absorbed (the
//! `<table>_raw` PK + `ON CONFLICT` + the queue re-claim); graceful drain just minimises replay and
//! avoids a stale lock. There is **no `wal_sender_timeout` analogue** — the drain is bounded only by the
//! grace period and DuckDB commit latency (a genuine simplification vs the sink).
//!
//! **Grace-period sizing (PR 4.9):** the measured *incremental* worst case (append + transform + commit)
//! — the full-rebuild is **excluded** because it is aborted, not awaited. **Skip `preStop`**: a
//! non-serving consumer gains nothing from it, and any preStop time is subtracted from the same budget
//! the drain needs — let `SIGTERM` arrive at T=0. **PID-1 / exec-form** (so `SIGTERM` reaches the Rust
//! process, not a shell) is a Dockerfile concern wired in **PR 4.8**; note it here.

use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

/// Install `SIGTERM`/`SIGINT` → cancel **one** shared token. Idempotent: the token is cancelled once, and
/// the signal streams stay registered so a **double**-`SIGTERM` during the drain is swallowed (never the
/// default terminate) — the drain can't be cut short and skip a step.
pub fn install_signal_handlers() -> CancellationToken {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        // If registration fails the process can't drain; log, cancel the token so the drain path fires
        // (there is no graceful drain without the handlers), and stop the task rather than leak it.
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGTERM handler: {e}");
                child.cancel();
                return;
            }
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGINT handler: {e}");
                child.cancel();
                return;
            }
        };
        tokio::select! {
            _ = term.recv() => tracing::info!("SIGTERM received — draining"),
            _ = int.recv() => tracing::info!("SIGINT received — draining"),
            _ = child.cancelled() => {}
        }
        child.cancel();
        // Keep the streams alive and swallow any further signals so a second SIGTERM mid-drain cannot
        // restore the default action and kill the process before the ordered drain completes.
        loop {
            tokio::select! {
                _ = term.recv() => tracing::warn!("SIGTERM during drain — ignored, drain already in progress"),
                _ = int.recv() => tracing::warn!("SIGINT during drain — ignored, drain already in progress"),
            }
        }
    });
    token
}
