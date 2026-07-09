//! Phase B — transform the un-transformed tail of `<table>_raw` into the mirror `<table>` (loader §4).
//! Read only `commit_lsn > transformed_lsn`, run the PR 3.3 dedup + `MERGE` **inside one DuckDB
//! transaction**, commit, then advance `transformed_lsn = max(commit_lsn)` applied in a **separate**
//! control-DB transaction (the two databases can't share one).
//!
//! **Naturally idempotent:** the watermark bounds what is read and the LWW dedup picks the same winners,
//! so re-running over the same tail produces a byte-identical mirror. A crash between the DuckDB commit
//! and the control advance just re-runs Phase B — no bespoke recovery.

use crate::error::LoaderError;
use crate::phase_a::TableCtx;
use crate::plan::TablePlan;
use crate::transform::{apply_transform, TransformSql};
use common::{Lsn, PgRelation};

/// Build the transform for a table at its CURRENT reconciled `schema_version` (PR 3.8): read the registry
/// (columns + type descriptors) into a [`TablePlan`] (Tier-2 emit/recombine, PR 4.2); fall back to the
/// bootstrap relation's scalar shape when there is no registry row (single-version / hermetic setups).
pub async fn current_transform(ctx: &TableCtx) -> Result<TransformSql, LoaderError> {
    let ver = ctx.db.schema_version()?;
    match control::read_registry(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, ver).await? {
        Some(r) => {
            let rel: PgRelation = serde_json::from_value(r.columns)
                .map_err(|e| LoaderError::Internal(format!("decode registry columns: {e}")))?;
            Ok(TransformSql::from_plan(&TablePlan::from_registry(
                &rel,
                &r.descriptors,
            )))
        }
        None => Ok(TransformSql::from_relation(&ctx.rel)),
    }
}

/// One Phase-B pass. Returns the max `commit_lsn` applied, or `None` if the tail was empty.
pub async fn run_phase_b(ctx: &TableCtx) -> Result<Option<Lsn>, LoaderError> {
    let cp = control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
        .await?
        .ok_or_else(|| {
            LoaderError::Internal(format!("no checkpoint for {}.{}", ctx.schema, ctx.table))
        })?;
    let after = cp.transformed_lsn;
    // Phase-B transform lag = raw_appended_lsn − transformed_lsn (PR 4.10); pure math from the checkpoint
    // just read, no extra query. Labelled per table (bounded cardinality).
    common::metrics::set_transform_lag(
        &format!("{}.{}", ctx.schema, ctx.table),
        cp.raw_appended_lsn
            .as_u64()
            .saturating_sub(cp.transformed_lsn.as_u64()),
    );

    // The max commit LSN in the tail we (re)transform, bounded `>= transformed_lsn` (16-hex text sorts as
    // the LSN, so `max` = latest). The `>=` is load-bearing for the snapshot/stream boundary (PR 3.10,
    // closing PR 3.7 break-face A end-to-end): equal-`lsn_end` snapshot files carry `commit_lsn =
    // consistent_point`, and if a later loader batch appends one *after* `transformed_lsn` already reached
    // that point, a strict `>` scan would skip it forever. Re-including the boundary re-applies rows the
    // mirror already has — the per-PK `_applied_*` guard makes those a no-op, so the mirror stays exact
    // (`max()` is NULL only when `<table>_raw` is empty). A source that sits idle at the boundary re-scans
    // that one commit's rows each poll; normal streaming advances `transformed_lsn` past it immediately.
    let conn = ctx.db.conn();
    let max_hex: Option<String> = conn
        .query_row(
            &format!(
                "SELECT max(\"_walrus_commit_lsn\") FROM \"{}_raw\" WHERE \"_walrus_commit_lsn\" >= ?",
                ctx.table
            ),
            [after.to_string()],
            |r| r.get(0),
        )
        .map_err(|e| LoaderError::Duck(format!("scan un-transformed tail: {e}")))?;
    let Some(max_hex) = max_hex else {
        return Ok(None); // <table>_raw is empty — nothing to transform yet
    };
    let max_lsn: Lsn = max_hex
        .parse()
        .map_err(|e| LoaderError::Internal(format!("parse max commit_lsn {max_hex:?}: {e:?}")))?;

    // The transform must reference exactly the columns the reconciled tables now have — i.e. the shape at
    // the DuckDB tables' CURRENT reconciled `schema_version` (Phase A advanced it, PR 3.8), NOT the stale
    // bootstrap shape (and, PR 4.2, with the Tier-2 emit/recombine from the descriptors).
    let t = current_transform(ctx).await?;
    conn.execute_batch("BEGIN TRANSACTION;")
        .map_err(|e| LoaderError::Duck(format!("begin transform txn: {e}")))?;
    if let Err(e) = apply_transform(conn, &t, &after) {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(e);
    }
    conn.execute_batch("COMMIT;")
        .map_err(|e| LoaderError::Duck(format!("commit transform txn: {e}")))?;

    // Advance the watermark AFTER the DuckDB commit. The CHECK (transformed_lsn <= raw_appended_lsn)
    // holds because Phase A ran first this cycle. `max_lsn` can equal the prior `transformed_lsn` (a
    // boundary re-transform advances it to the same value — a no-op) — that is the snapshot/stream
    // boundary being held closed (PR 3.10). The full-rebuild (PR 3.11) is the safety net regardless.
    control::advance_transformed(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, max_lsn).await?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        transformed = %max_lsn,
        "Phase B: mirror updated, transformed_lsn advanced"
    );
    Ok(Some(max_lsn))
}
