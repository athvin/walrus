//! The per-table apply loop (loader §8.4) — **one worker per `.duckdb` file** (never share a DuckDB
//! connection across tables). Each poll interval: Phase A (append) then Phase B (transform), then stamp
//! `last_poll_completed_at` — **every** cycle, even a no-op poll, so an idle-but-healthy loader stays
//! `/healthz` green. On a **slower, distinct** cadence, and on THIS same worker thread (serialized right
//! after an apply cycle — no quiescing dance), it runs the full-rebuild + retention prune (PR 3.11).
//! Exits cleanly on the shutdown token.

use crate::error::LoaderError;
use crate::phase_a::{run_phase_a, TableCtx};
use crate::phase_b::run_phase_b;
use crate::transform::TransformSql;
use common::{Lsn, PgRelation};
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
            _ = shutdown.cancelled() => {
                tracing::info!(table = %format_args!("{}.{}", ctx.schema, ctx.table), "apply loop stopping");
                return Ok(());
            }
            _ = tick.tick() => {}
        }
        // A crash between the two phases is absorbed by the next cycle's plain re-run (both idempotent).
        run_phase_a(&ctx).await?;
        run_phase_b(&ctx).await?;
        ctx.state.stamp_poll();

        // Compaction on its own cadence, serialized AFTER the apply cycle on this same worker thread —
        // it needs the exclusive writer and ~2× transient space, so it can never contend with the writer.
        if last_compaction.elapsed() >= ctx.compaction_interval {
            compact(&ctx, &shutdown).await?;
            last_compaction = Instant::now();
        }
    }
}

/// One compaction pass: full-rebuild (self-heal + reclaim) then prune raw below the retention floor.
async fn compact(ctx: &TableCtx, shutdown: &CancellationToken) -> Result<(), LoaderError> {
    let cp = control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?;
    let transformed = cp.map(|c| c.transformed_lsn).unwrap_or(Lsn::ZERO);
    let t = TransformSql::from_relation(&current_relation(ctx).await?);

    crate::compaction::full_rebuild(ctx.db.conn(), &t, shutdown)?;
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

/// The mirror's CURRENT reconciled shape — registry at the DuckDB `schema_version` (PR 3.8), falling back
/// to the bootstrap relation when there is no registry row. Keeps the rebuild's columns matching the
/// tables after any DDL, exactly as Phase B does.
async fn current_relation(ctx: &TableCtx) -> Result<PgRelation, LoaderError> {
    match control::read_registry(
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        ctx.db.schema_version()?,
    )
    .await?
    {
        Some(r) => serde_json::from_value(r.columns)
            .map_err(|e| LoaderError::Internal(format!("decode registry columns: {e}"))),
        None => Ok(ctx.rel.clone()),
    }
}
