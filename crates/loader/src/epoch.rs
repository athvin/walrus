//! Total-restart on the loader side (§1.8). When the control plane opens a new generation (the sink
//! bumped `replication_state.epoch` after the single lifelong slot was lost/invalidated), every
//! `.duckdb` built for the retired generation holds stale `<table>`/`<table>_raw` data. The fix is a
//! whole-file **rebuild**: wipe the mirror + CDC log so the fresh new-epoch snapshot re-appends from
//! scratch and the transform re-derives the mirror. **Both watermarks reset for free** — the new epoch's
//! `loader_checkpoint` row is a fresh `(0/0, 0/0)`, since checkpoints are epoch-keyed.
//!
//! Detection is at **bootstrap** (compare each file's `_walrus_meta['epoch']` to the control epoch) and,
//! for a *running* loader, per poll ([`apply_loop`](crate::apply_loop) exits loudly on a bump so the
//! orchestrator restarts it into a rebuild). A rebuild is **whole-system** by construction — every table
//! shares the epoch and is rebuilt together; there is no per-table reload (a deferred goal, §1.8).

use crate::duck::TableDb;
use crate::error::LoaderError;

/// If `db` was built for an older generation than `control_epoch`, wipe its mirror + raw so the caller's
/// subsequent `ensure_tables*` recreates them empty and the new-epoch snapshot rebuilds the file. Returns
/// `true` iff a rebuild happened. A no-op (returns `false`) when the file is brand-new (never stamped) or
/// already at `control_epoch` — so first-bootstrap and steady resume are untouched.
pub fn rebuild_for_new_epoch(
    db: &TableDb,
    table: &str,
    control_epoch: i64,
) -> Result<bool, LoaderError> {
    match db.built_epoch()? {
        Some(built) if built < control_epoch => {
            tracing::error!(
                table,
                old_epoch = built,
                new_epoch = control_epoch,
                "TOTAL-RESTART: .duckdb was built for a retired generation — wiping mirror + raw to \
                 rebuild under the new epoch (both watermarks reset from the fresh checkpoint)"
            );
            db.wipe_generation(table)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[cfg(test)]
#[path = "epoch_test.rs"]
mod tests;
