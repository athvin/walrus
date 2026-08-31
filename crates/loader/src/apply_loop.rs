//! The per-table apply loop (loader §8.4) — **one worker per `.duckdb` file** (never share a DuckDB
//! connection across tables). Each poll interval: Phase A (append) then Phase B (transform), then stamp
//! `last_poll_completed_at` — **every** cycle, even a no-op poll, so an idle-but-healthy loader stays
//! `/healthz` green. On a **slower, distinct** cadence, and on THIS same worker thread (serialized right
//! after an apply cycle — no quiescing dance), it runs the full-rebuild + retention prune.
//! Exits cleanly on the shutdown token.

use crate::error::LoaderError;
use crate::phase_a::{TableCtx, run_phase_a};
use crate::phase_b::run_phase_b;
use common::{EpochNo, Lsn};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Return the terminal total-restart error when a running worker observes a newer generation than
/// its loop-start baseline.
pub(crate) fn epoch_guard(
    observed: EpochNo,
    baseline: EpochNo,
    started_at: EpochNo,
) -> Result<(), LoaderError> {
    if observed > baseline {
        Err(LoaderError::EpochBumped {
            from: started_at,
            to: observed,
        })
    } else {
        Ok(())
    }
}

/// Drive one owned table until `shutdown`. Phase A + Phase B share one poll cadence in v1 (two txns);
/// compaction runs on its own slower cadence on this thread.
///
/// # Errors
///
/// Returns [`LoaderError::EpochBumped`] when the control generation advances while this worker is
/// running. Control-plane, DuckDB, registry, quarantine, and watermark failures from the two phases
/// or compaction propagate through their corresponding [`LoaderError`] variants and are terminal.
///
/// # Panics
///
/// Panics if `ctx.poll_interval` is zero — [`tokio::time::interval`] rejects a zero period. The
/// loader's config admits no zero cadence, so only a hand-built [`TableCtx`] can reach this.
// One span per worker task: every owned table's loop runs concurrently on the SAME `LocalSet`
// driver thread, so their lines interleave. What the span adds is the context an event cannot
// spell for itself — `duck::in_txn`'s rollback warning, and every `log`-bridged sqlx/DuckDB record
// emitted while this worker is polled, now name the table they belong to. The events below keep
// their own `table` field regardless: a span ADDS context rather than relocating it, since the
// JSON formatter renders event fields under `fields` and span fields under `span`/`spans` — two
// query paths, pinned by `crates/common/src/telemetry_test.rs`.
// `#[instrument]` and not `span.enter()`, because this loop awaits and a guard held across
// `.await` follows the executor onto whichever task it resumed next. `ctx` is skipped (it owns the
// pool and the DuckDB connection); `series` is its precomputed `"<schema>.<table>"`.
#[tracing::instrument(skip_all, fields(table = %ctx.series))]
pub async fn apply_loop(ctx: TableCtx, shutdown: CancellationToken) -> Result<(), LoaderError> {
    let mut tick = tokio::time::interval(ctx.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Track the last compaction so the (slower) rebuild cadence is independent of the poll interval.
    let mut last_compaction = Instant::now();
    // The generation baseline for the total-restart guard: the highest epoch present as of loop start.
    // In production this equals `ctx.epoch` (bootstrap read the MAX), so the guard fires on ANY later
    // bump; using the observed MAX (not `ctx.epoch`) means a shared test control-plane carrying unrelated
    // higher epochs doesn't trip it — only a genuine bump *while this loader runs* does.
    let baseline_epoch = (*ctx.epoch_rx.borrow()).max(ctx.epoch);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return drain(&ctx),
            _ = tick.tick() => {}
        }
        // Total-restart guard (§1.8): if the control plane opened a newer generation than the one we
        // started on, this loader is now running a RETIRED epoch. Exit loudly (→ `app::pipeline`
        // cancels the token and the process restarts) so bootstrap wipes + rebuilds every `.duckdb`
        // under the new epoch — never rebuild in place mid-run.
        let observed = *ctx.epoch_rx.borrow();
        epoch_guard(observed, baseline_epoch, ctx.epoch)?;
        // Drain step 2+3: `run_phase_a`/`run_phase_b` are never interrupted mid-flight, so the in-flight
        // cycle FINISHES atomically (append + the control-DB `raw_appended_lsn`+DELETE txn, then the
        // transform + `transformed_lsn`) even if SIGTERM arrives now — no new crash window. A crash
        // BETWEEN the two phases is absorbed by the next cycle's plain re-run (both idempotent).
        run_phase_a(&ctx).await?;
        run_phase_b(&ctx).await?;
        // Reload completion (H10): AFTER Phase B advanced `transformed_lsn`, flip any
        // `export_complete` reload for this table to `complete` once the mirror has reached its `H`
        // (`transformed_lsn >= final_lsn`). One guarded UPDATE joining the checkpoint we just wrote;
        // a no-op on cycles with no such reload. The loader owns this flip, never the sink (H10).
        let completed =
            control::reload::complete_reached(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
                .await?;
        for reload_id in completed {
            tracing::info!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                reload_id = %reload_id,
                "reload complete: transformed_lsn reached H"
            );
        }
        ctx.state.stamp_poll();

        // Compaction on its own cadence, serialized AFTER the apply cycle on this same worker thread — it
        // needs the exclusive writer and ~2× transient space, so it can never contend with the writer. Do
        // NOT start a new rebuild once draining (an already-running one is aborted inside `compact`).
        if !shutdown.is_cancelled() && last_compaction.elapsed() >= ctx.compaction_interval {
            compact(&ctx, &shutdown).await?;
            last_compaction = Instant::now();
        }
    }
}

/// Drain one worker on SIGTERM: the in-flight cycle already finished (both watermarks committed), so just
/// `CHECKPOINT` the WAL into the main file and return — dropping `ctx` closes the file, releasing the
/// lock cleanly (no stale lock for the next bootstrap). The lease is released by `app::pipeline` after
/// all workers drain (after their watermarks commit).
fn drain(ctx: &TableCtx) -> Result<(), LoaderError> {
    if let Err(e) = ctx.db.conn().execute_batch("CHECKPOINT;") {
        tracing::warn!(table = %format_args!("{}.{}", ctx.schema, ctx.table), error = %e, "drain CHECKPOINT failed");
    }
    tracing::info!(table = %format_args!("{}.{}", ctx.schema, ctx.table), "apply loop drained (watermarks committed, file checkpointed)");
    Ok(())
}

/// One compaction pass: full-rebuild (self-heal + reclaim) then prune raw below the retention floor.
async fn compact(ctx: &TableCtx, shutdown: &CancellationToken) -> Result<(), LoaderError> {
    let cp = control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?;
    let transformed = cp.map(|c| c.transformed_lsn).unwrap_or(Lsn::ZERO);
    let t = crate::phase_b::current_transform(ctx).await?;

    // The rebuild is abortable: a SIGTERM mid-rewrite interrupts it, rolls back, and returns Ok.
    crate::compaction::full_rebuild_abortable(&ctx.db, &t, shutdown).await?;
    if shutdown.is_cancelled() {
        return Ok(()); // draining — skip the prune, the rebuild was aborted; both re-run next start
    }
    // Prune only below `transformed_lsn - retention_lsn_lag` (always behind transformed_lsn) — the rebuild
    // just captured every current value into the mirror baseline, so pruned raw can lose nothing.
    let floor = crate::compaction::retention_floor(transformed, ctx.retention_lsn_lag);
    let pruned = crate::compaction::prune_raw(ctx.db.conn(), &t, floor)?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        floor = %floor,
        pruned,
        "compaction: mirror rebuilt (reclaimed), raw pruned below the retention floor"
    );
    Ok(())
}

#[cfg(test)]
#[path = "apply_loop_test.rs"]
mod tests;
