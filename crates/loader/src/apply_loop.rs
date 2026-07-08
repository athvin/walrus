//! The per-table apply loop (loader §8.4) — **one worker per `.duckdb` file** (never share a DuckDB
//! connection across tables). Each poll interval: Phase A (append) then Phase B (transform), then stamp
//! `last_poll_completed_at` — **every** cycle, even a no-op poll, so an idle-but-healthy loader stays
//! `/healthz` green. On a **slower, distinct** cadence, and on THIS same worker thread (serialized right
//! after an apply cycle — no quiescing dance), it runs the full-rebuild + retention prune (PR 3.11).
//! Exits cleanly on the shutdown token.

use crate::error::LoaderError;
use crate::phase_a::{run_phase_a, TableCtx};
use crate::phase_b::run_phase_b;
use common::Lsn;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Drive one owned table until `shutdown`. Phase A + Phase B share one poll cadence in v1 (two txns);
/// compaction runs on its own slower cadence on this thread.
pub async fn apply_loop(ctx: TableCtx, shutdown: CancellationToken) -> Result<(), LoaderError> {
    let mut tick = tokio::time::interval(ctx.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Track the last compaction so the (slower) rebuild cadence is independent of the poll interval.
    let mut last_compaction = Instant::now();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return drain(&ctx),
            _ = tick.tick() => {}
        }
        // Drain step 2+3: `run_phase_a`/`run_phase_b` are never interrupted mid-flight, so the in-flight
        // cycle FINISHES atomically (append + the control-DB `raw_appended_lsn`+DELETE txn, then the
        // transform + `transformed_lsn`) even if SIGTERM arrives now — no new crash window. A crash
        // BETWEEN the two phases is absorbed by the next cycle's plain re-run (both idempotent).
        run_phase_a(&ctx).await?;
        run_phase_b(&ctx).await?;
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
/// lock cleanly (no stale lock for the next bootstrap). The lease is released by `main` after all workers
/// drain (after their watermarks commit). PR 3.12.
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

    // The rebuild is abortable: a SIGTERM mid-rewrite interrupts it, rolls back, and returns Ok (PR 3.12).
    crate::compaction::full_rebuild_abortable(ctx.db.conn(), &t, shutdown).await?;
    if shutdown.is_cancelled() {
        return Ok(()); // draining — skip the prune, the rebuild was aborted; both re-run next start
    }
    // Prune only below `transformed_lsn - retention_lsn_lag` (always behind transformed_lsn) — the rebuild
    // just captured every current value into the mirror baseline, so pruned raw can lose nothing.
    let floor = crate::compaction::retention_floor(transformed, ctx.retention_lsn_lag);
    let pruned = crate::compaction::prune_raw(ctx.db.conn(), &t, &floor)?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        floor = %floor,
        pruned,
        "compaction: mirror rebuilt (reclaimed), raw pruned below the retention floor"
    );
    Ok(())
}
