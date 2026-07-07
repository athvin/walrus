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
use crate::transform::{apply_transform, TransformSql};
use common::Lsn;

/// One Phase-B pass. Returns the max `commit_lsn` applied, or `None` if the tail was empty.
pub async fn run_phase_b(ctx: &TableCtx) -> Result<Option<Lsn>, LoaderError> {
    let cp = control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
        .await?
        .ok_or_else(|| {
            LoaderError::Internal(format!("no checkpoint for {}.{}", ctx.schema, ctx.table))
        })?;
    let after = cp.transformed_lsn;

    // The max commit LSN in the un-transformed tail (16-hex text sorts as the LSN, so `max` = latest).
    // `max()` returns one row (NULL when the tail is empty).
    let conn = ctx.db.conn();
    let max_hex: Option<String> = conn
        .query_row(
            &format!(
                "SELECT max(\"_walrus_commit_lsn\") FROM \"{}_raw\" WHERE \"_walrus_commit_lsn\" > ?",
                ctx.table
            ),
            [after.to_string()],
            |r| r.get(0),
        )
        .map_err(|e| LoaderError::Duck(format!("scan un-transformed tail: {e}")))?;
    let Some(max_hex) = max_hex else {
        return Ok(None); // nothing new since transformed_lsn
    };
    let max_lsn: Lsn = max_hex
        .parse()
        .map_err(|e| LoaderError::Internal(format!("parse max commit_lsn {max_hex:?}: {e:?}")))?;

    // The transform runs atomically: dedup TEMP + three-branch MERGE in one DuckDB txn.
    let t = TransformSql::from_relation(&ctx.rel);
    conn.execute_batch("BEGIN TRANSACTION;")
        .map_err(|e| LoaderError::Duck(format!("begin transform txn: {e}")))?;
    if let Err(e) = apply_transform(conn, &t, &after) {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(e);
    }
    conn.execute_batch("COMMIT;")
        .map_err(|e| LoaderError::Duck(format!("commit transform txn: {e}")))?;

    // Advance the watermark AFTER the DuckDB commit. The CHECK (transformed_lsn <= raw_appended_lsn)
    // holds because Phase A ran first this cycle.
    // TODO(3.7): equal-commit_lsn snapshot straddle.
    control::advance_transformed(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, max_lsn).await?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        transformed = %max_lsn,
        "Phase B: mirror updated, transformed_lsn advanced"
    );
    Ok(Some(max_lsn))
}
