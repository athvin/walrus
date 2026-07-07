//! One cancellation source fanned out to every task (§4.5), plus the ordered SIGTERM **drain** (§4.5,
//! sink §4.5).
//!
//! SIGTERM (Kubernetes' graceful-stop signal) and SIGINT both trip a single [`CancellationToken`];
//! `clone()` it into each task, which selects on `token.cancelled()`. The process must be able to be
//! PID 1 with an exec-form entrypoint so the signal is delivered to *us*, not swallowed by a shell
//! (the Dockerfile is PR 4.8, but we design for it now: direct signal handling, no wrapper).
//!
//! **The K8s termination sequence** is preStop-*then*-SIGTERM, sharing one `terminationGracePeriod`
//! budget (wiring is PR 4.9). walrus skips preStop: the process handles SIGTERM directly as PID 1, so
//! there is nothing a preStop hook would add. On SIGTERM the decode loop's cancellation branch fires
//! and calls [`drain`] — which runs **after** the `select!` loop exits, so a slow S3 PUT is never
//! aborted mid-flight; the grace period bounds it externally, not cancellation.
//!
//! **Why the drain never drops the slot:** the slot persists across the connection, so a graceful
//! shutdown is just a *resume* — the drain only minimises the replay the loader would de-duplicate
//! anyway (at-least-once → effectively-once). An ungraceful `SIGKILL` is therefore still correct: the
//! replacement pod resumes from `confirmed_flush_lsn` and re-streams the uncommitted tail.
//!
//! The signal task also selects on the token so it exits if cancellation comes from elsewhere — it
//! never outlives the token as a leaked handle.

use crate::checkpoint::DurabilityCheckpoint;
use crate::consume::{flush_batch, BatchRouter};
use crate::replication::ReplicationStream;
use crate::sink::ParquetSink;
use anyhow::Context;
use common::Lsn;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

/// Outcome of a drain attempt (the caller maps this to an `ExitCode` — a completed drain is `Success`).
#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Committed batch(es) flushed + manifested, final feedback sent, connection closed — slot left
    /// in place. `confirmed_flush` is the durable LSN a replacement pod resumes from.
    Drained { confirmed_flush: Lsn },
    /// Nothing committed was in flight; final feedback + clean close only.
    Empty,
}

/// The ordered SIGTERM drain (§4.5). Runs to completion (**not** cancellable); the caller bounds it by
/// the K8s grace period. Steps: **(1)** stop consuming (the `select!` loop already exited) → **(2)**
/// flush + COMMIT the in-flight **committed** batch (PUT → manifest → advance checkpoint), dropping any
/// open uncommitted speculative buffers → **(3)** send a **final** standby update advancing
/// `confirmed_flush_lsn` (the checkpoint clamps it to the open-txn floor; `None` until PR 2.30) →
/// **(4)** `CopyDone` + clean close → **(5)** return, leaving the slot in place. **Never** issues
/// `DROP_REPLICATION_SLOT`.
pub async fn drain(
    stream: &mut ReplicationStream,
    router: &mut BatchRouter,
    sink: &ParquetSink,
    checkpoint: &mut DurabilityCheckpoint,
    pool: &sqlx::PgPool,
    epoch: i64,
) -> anyhow::Result<DrainOutcome> {
    // (2) Seal + flush the committed batches; open speculative buffers are dropped (they re-stream).
    let sealed = router
        .drain_committed()
        .context("seal committed batches on drain")?;
    let mut drained_any = false;
    for batch in sealed {
        let written = flush_batch(sink, pool, epoch, batch).await?; // (a) PUT then (b) manifest
        checkpoint.on_batch_durable(written.lsn_end); // (c) advance confirmed_flush
        drained_any = true;
        tracing::info!(
            uri = %written.s3_uri,
            lsn_end = %written.lsn_end,
            "drain: flushed in-flight committed batch"
        );
    }
    // (3) The final standby update carries the durable confirmed_flush (clamped to the open-txn floor).
    checkpoint
        .send(stream, false)
        .await
        .context("send final drain standby status")?;
    // (4) CopyDone + clean close. (5) The slot persists — no DROP_REPLICATION_SLOT, ever.
    stream.copy_done().await.context("CopyDone on drain")?;
    if drained_any {
        Ok(DrainOutcome::Drained {
            confirmed_flush: checkpoint.confirmed_flush(),
        })
    } else {
        Ok(DrainOutcome::Empty)
    }
}

/// Install SIGTERM/SIGINT handlers and return the token they cancel. Also returns early (without
/// cancelling) if the token is cancelled by another source, so the task can't leak.
pub fn install_signal_handlers() -> CancellationToken {
    let token = CancellationToken::new();
    let child = token.clone();
    tokio::spawn(async move {
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGTERM handler: {e}");
                child.cancel();
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGINT handler: {e}");
                child.cancel();
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received; cancelling");
                child.cancel();
            }
            _ = sigint.recv() => {
                tracing::info!("SIGINT received; cancelling");
                child.cancel();
            }
            // Cancelled elsewhere (e.g. a bootstrap failure) — stop listening; don't leak the task.
            _ = child.cancelled() => {}
        }
    });
    token
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn token_is_live_until_cancelled() {
        let token = install_signal_handlers();
        assert!(!token.is_cancelled());
        // A cancel from another source trips the same token and unwinds the signal task.
        token.cancel();
        token.cancelled().await;
        assert!(token.is_cancelled());
    }
}
