//! The loader's graceful SIGTERM drain (loader §8.5) — the mirror image of the sink's WAL drain.
//!
//! On `SIGTERM` each per-table worker drains **in order**, all keyed off one [`CancellationToken`]:
//! 1. **stop claiming** new files (the worker observes the token at the top of its poll loop);
//! 2. **finish the in-flight Phase A** append + its atomic control-DB txn (`raw_appended_lsn` advance +
//!    manifest `DELETE`) — [`crate::phase_a::run_phase_a`] is never interrupted mid-flight, so the
//!    two-DB step stays atomic and opens no new crash window;
//! 3. **finish the in-flight Phase B** transform + commit `transformed_lsn`; and **abort** any in-flight
//!    periodic **full-rebuild** (idempotent self-heal; re-runs next cycle) so it can't blow the grace
//!    budget — see [`crate::compaction::full_rebuild_abortable`];
//! 4. **`CHECKPOINT` and close** the `.duckdb` file so no stale lock is left for the next bootstrap.
//!    [`crate::apply_loop`]'s drain runs the `CHECKPOINT`; the *close* is a **drop** — the worker task
//!    owns its [`TableCtx`](crate::phase_a::TableCtx), so DuckDB's writer lock comes off when that
//!    task's future ends, which is why `main` joins every worker before step 5;
//! 5. **release the ownership lease** — last, and only *after* the watermarks commit, so a fast
//!    replacement can't double-apply the tail. The two fences therefore come off in the REVERSE of
//!    their bootstrap order (lease → file lock, so file lock → lease). Design §8.5 lists these two the
//!    other way round and is not wrong — the file lock is the second fence, so a successor handed the
//!    lease early merely fails its [`TableDb::open`](crate::duck::TableDb::open) and retries — but
//!    this order leaves no such window;
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

use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

/// Run `body` while holding a [`CancellationToken`] drop guard. Whether the body succeeds,
/// returns early with an error, or unwinds, the token is cancelled before this function returns.
/// This scope must end before callers join tasks whose shutdown waits on the token.
pub async fn cancel_on_exit<F>(token: &CancellationToken, body: F) -> F::Output
where
    F: std::future::Future,
{
    let _guard = token.clone().drop_guard();
    body.await
}

/// Install `SIGTERM`/`SIGINT` → cancel **one** shared token. Idempotent: the token is cancelled once, and
/// the signal streams stay registered so a **double**-`SIGTERM` during the drain is swallowed (never the
/// default terminate) — the drain can't be cut short and skip a step.
#[must_use]
pub fn install_signal_handlers() -> CancellationToken {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        // If registration fails the process can't drain; log, cancel the token so the drain path fires
        // (there is no graceful drain without the handlers), and stop the task rather than leak it.
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
                child.cancel();
                return;
            }
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGINT handler");
                child.cancel();
                return;
            }
        };
        tokio::select! {
            biased;
            // Cancelled elsewhere (for example, a bootstrap failure): stop without claiming a
            // signal arrived.
            _ = child.cancelled() => {}
            _ = term.recv() => tracing::info!("SIGTERM received — draining"),
            _ = int.recv() => tracing::info!("SIGINT received — draining"),
        }
        child.cancel();
        // Keep the streams alive and swallow any further signals so a second SIGTERM mid-drain cannot
        // restore the default action and kill the process before the ordered drain completes.
        loop {
            // Deliberately leave this select unbiased: both arms swallow an equivalent signal.
            tokio::select! {
                _ = term.recv() => tracing::warn!("SIGTERM during drain — ignored, drain already in progress"),
                _ = int.recv() => tracing::warn!("SIGINT during drain — ignored, drain already in progress"),
            }
        }
    });
    token
}

#[cfg(test)]
#[path = "shutdown_test.rs"]
mod tests;
