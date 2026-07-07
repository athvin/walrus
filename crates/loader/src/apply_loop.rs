//! The per-table apply loop (loader §8.4) — **one worker per `.duckdb` file** (never share a DuckDB
//! connection across tables). Each poll interval: Phase A (append) then Phase B (transform), then stamp
//! `last_poll_completed_at` — **every** cycle, even a no-op poll, so an idle-but-healthy loader stays
//! `/healthz` green. Exits cleanly on the shutdown token.

use crate::error::LoaderError;
use crate::phase_a::{run_phase_a, TableCtx};
use crate::phase_b::run_phase_b;
use tokio_util::sync::CancellationToken;

/// Drive one owned table until `shutdown`. Phase A + Phase B share one poll cadence in v1 (two txns).
pub async fn apply_loop(ctx: TableCtx, shutdown: CancellationToken) -> Result<(), LoaderError> {
    let mut tick = tokio::time::interval(ctx.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
    }
}
