//! One cancellation source fanned out to every task (§4.5).
//!
//! SIGTERM (Kubernetes' graceful-stop signal) and SIGINT both trip a single [`CancellationToken`];
//! `clone()` it into each task, which selects on `token.cancelled()`. The process must be able to be
//! PID 1 with an exec-form entrypoint so the signal is delivered to *us*, not swallowed by a shell
//! (the Dockerfile is PR 4.8, but we design for it now: direct signal handling, no wrapper).
//!
//! The signal task also selects on the token so it exits if cancellation comes from elsewhere — it
//! never outlives the token as a leaked handle.

use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;

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
