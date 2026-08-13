//! Total-restart on the loader side (§1.8). When the control plane opens a new generation (the sink
//! bumped `replication_state.epoch` after the single lifelong slot was lost/invalidated), every
//! `.duckdb` built for the retired generation holds stale `<table>`/`<table>_raw` data. The fix is a
//! whole-file **rebuild**: wipe the mirror + CDC log so the fresh new-epoch snapshot re-appends from
//! scratch and the transform re-derives the mirror. **Both watermarks reset for free** — the new epoch's
//! `loader_checkpoint` row is a fresh `(0/0, 0/0)`, since checkpoints are epoch-keyed.
//!
//! Detection is at **bootstrap** (compare each file's `_walrus_meta['epoch']` to the control epoch) and,
//! for a *running* loader, through one shared epoch poller ([`apply_loop`](crate::apply_loop) exits loudly
//! on a bump so the orchestrator restarts it into a rebuild). A rebuild is **whole-system** by
//! construction — every table shares the epoch and is rebuilt together; there is no per-table reload
//! (a deferred goal, §1.8).

use crate::duck::TableDb;
use crate::error::LoaderError;
use common::EpochNo;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// If `db` was built for an older generation than `control_epoch`, wipe its mirror + raw so the caller's
/// subsequent `ensure_tables*` recreates them empty and the new-epoch snapshot rebuilds the file. Returns
/// `true` iff a rebuild happened. A no-op (returns `false`) when the file is brand-new (never stamped) or
/// already at `control_epoch` — so first-bootstrap and steady resume are untouched.
///
/// # Errors
///
/// Returns [`LoaderError::Duck`] if the stored epoch cannot be read or a retired generation cannot
/// be wiped.
pub fn rebuild_for_new_epoch(
    db: &TableDb,
    table: &str,
    control_epoch: EpochNo,
) -> Result<bool, LoaderError> {
    match db.built_epoch()? {
        Some(built) if built < control_epoch => {
            tracing::error!(
                table,
                old_epoch = %built,
                new_epoch = %control_epoch,
                "TOTAL-RESTART: .duckdb was built for a retired generation — wiping mirror + raw to \
                 rebuild under the new epoch (both watermarks reset from the fresh checkpoint)"
            );
            db.wipe_generation(table)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// Poll the control plane's current epoch once per `period` and publish its latest value to every
/// apply loop. A failed read is only a delayed guard observation, so it is logged and retried on the
/// next tick. The task stops on shutdown or when its final receiver is dropped.
#[must_use]
pub fn spawn_epoch_watch(
    pool: sqlx::PgPool,
    baseline: EpochNo,
    period: Duration,
    token: CancellationToken,
) -> (watch::Receiver<EpochNo>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = watch::channel(baseline);
    let handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await; // `baseline` was just read; skip the interval's immediate tick.

        loop {
            if tx.is_closed() {
                return;
            }
            tokio::select! {
                biased;
                () = token.cancelled() => return,
                () = tx.closed() => return,
                _ = tick.tick() => {}
            }
            if tx.is_closed() {
                return;
            }

            match control::read_current_epoch(&pool).await {
                Ok(observed) => {
                    advance(&tx, observed.map(|state| state.epoch));
                }
                Err(error) => {
                    tracing::warn!(%error, "epoch watch read failed; retrying next tick");
                }
            }
        }
    });
    (rx, handle)
}

/// Publish only a genuine forward epoch move, returning whether receivers were notified.
pub(crate) fn advance(tx: &watch::Sender<EpochNo>, observed: Option<EpochNo>) -> bool {
    let Some(observed) = observed else {
        return false;
    };
    tx.send_if_modified(|current| {
        if observed > *current {
            *current = observed;
            true
        } else {
            false
        }
    })
}

/// Return a receiver pinned to `epoch`; borrowing retains the final value after its sender is gone.
#[must_use]
pub fn fixed_epoch_watch(epoch: EpochNo) -> watch::Receiver<EpochNo> {
    watch::channel(epoch).1
}

#[cfg(test)]
#[path = "epoch_test.rs"]
mod tests;
