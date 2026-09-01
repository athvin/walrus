//! Phase B — transform the un-transformed tail of `<table>_raw` into the mirror `<table>` (loader §4).
//! Read only `commit_lsn > transformed_lsn`, run the dedup + `MERGE` **inside one DuckDB
//! transaction**, commit, then advance `transformed_lsn = max(commit_lsn)` applied in a **separate**
//! control-DB transaction (the two databases can't share one).
//!
//! **Naturally idempotent:** the watermark bounds what is read and the LWW dedup picks the same winners,
//! so re-running over the same tail produces a byte-identical mirror. A crash between the DuckDB commit
//! and the control advance just re-runs Phase B — no bespoke recovery.

use crate::duck_ext::DuckResultExt;
use crate::error::LoaderError;
use crate::phase_a::TableCtx;
use crate::plan::TablePlan;
use crate::table_name::{DuckTable, Mirror};
use crate::transform::{TransformSql, apply_transform, apply_transform_ducklake};
use common::{Lsn, PgRelation};

/// Build the transform for a table at its CURRENT reconciled `schema_version`: read the registry
/// (columns + type descriptors) into a [`TablePlan`] (Tier-2 emit/recombine); fall back to the
/// bootstrap relation's scalar shape when there is no registry row (single-version / hermetic setups).
///
/// # Errors
///
/// Returns [`LoaderError::Duck`] if the local schema watermark cannot be read,
/// [`LoaderError::Control`] if the registry lookup fails, or [`LoaderError::RegistryDecode`] if the
/// stored relation shape is invalid.
pub(crate) async fn current_transform(ctx: &TableCtx) -> Result<TransformSql, LoaderError> {
    let ver = ctx.db.schema_version()?;
    match control::read_registry(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, ver).await? {
        Some(r) => {
            // The `schema.table` label is built INSIDE the closure: `map_err` only runs it on a
            // decode failure, so the every-cycle success path allocates nothing.
            let rel: PgRelation = serde_json::from_value(r.columns).map_err(|source| {
                LoaderError::RegistryDecode {
                    table: format!("{}.{}", ctx.schema, ctx.table),
                    version: ver.0,
                    source,
                }
            })?;
            Ok(TransformSql::from_plan(&TablePlan::from_registry(
                &rel,
                &r.descriptors,
            )))
        }
        None => Ok(TransformSql::from_relation(&ctx.rel)),
    }
}

/// One Phase-B pass. Returns the max `commit_lsn` applied, or `None` if the tail was empty.
///
/// # Errors
///
/// Returns [`LoaderError::Control`] for checkpoint/registry/watermark operations,
/// [`LoaderError::Duck`] for local scan or transaction failures, [`LoaderError::LsnParse`] for an
/// invalid stored watermark, or [`LoaderError::RegistryDecode`] for an invalid relation shape.
pub async fn run_phase_b(ctx: &TableCtx) -> Result<Option<Lsn>, LoaderError> {
    let cp = control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
        .await?
        .ok_or_else(|| {
            LoaderError::Internal(format!("no checkpoint for {}.{}", ctx.schema, ctx.table))
        })?;
    let after = cp.transformed_lsn;
    // Phase-B transform lag = raw_appended_lsn − transformed_lsn; pure math from the checkpoint
    // just read, no extra query. Labelled per table (bounded cardinality).
    common::metrics::set_transform_lag(&ctx.series, cp.raw_appended_lsn - cp.transformed_lsn);

    // The max commit LSN in the tail we (re)transform, bounded `>= transformed_lsn` (16-hex text
    // sorts as the LSN, so `max` = latest). The `>=` is load-bearing for the snapshot/stream
    // boundary: equal-`lsn_end` snapshot files carry `commit_lsn =
    // consistent_point`, and if a later loader batch appends one *after* `transformed_lsn` already reached
    // that point, a strict `>` scan would skip it forever. Re-including the boundary re-applies rows the
    // mirror already has — the per-PK `_applied_*` guard makes those a no-op, so the mirror stays exact
    // (`max()` is NULL only when `<table>_raw` is empty). A source that sits idle at the boundary re-scans
    // that one commit's rows each poll; normal streaming advances `transformed_lsn` past it immediately.
    let conn = ctx.db.conn();
    let raw = DuckTable::<Mirror>::new(&ctx.table).to_raw();
    let max_hex: Option<String> = conn
        .query_row(
            &format!(
                "SELECT max(\"_walrus_commit_lsn\") FROM \"{}\" WHERE \"_walrus_commit_lsn\" >= ?",
                raw.as_str()
            ),
            [after.to_string()],
            |r| r.get(0),
        )
        .duck("scan un-transformed tail")?;
    let Some(max_hex) = max_hex else {
        return Ok(None); // <table>_raw is empty — nothing to transform yet
    };
    let max_lsn: Lsn = max_hex.parse().map_err(|source| LoaderError::LsnParse {
        field: "max commit_lsn",
        source,
    })?;

    // The transform must reference exactly the columns the reconciled tables now have — i.e. the shape at
    // the DuckDB tables' CURRENT reconciled `schema_version` (Phase A advanced it), NOT the stale
    // bootstrap shape, including Tier-2 recombination from the descriptors.
    let t = current_transform(ctx).await?;
    let ducklake = ctx.db.is_ducklake();
    ctx.db.in_txn("transform", |conn| {
        if ducklake {
            apply_transform_ducklake(conn, &t, after)
        } else {
            apply_transform(conn, &t, after)
        }
    })?;

    // Advance the watermark AFTER the DuckDB commit. The CHECK (transformed_lsn <= raw_appended_lsn)
    // holds because Phase A ran first this cycle. `max_lsn` can equal the prior `transformed_lsn` (a
    // boundary re-transform advances it to the same value — a no-op) — that is the snapshot/stream
    // boundary being held closed. The full-rebuild is the safety net regardless.
    control::advance_transformed(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, max_lsn).await?;
    // Only a watermark that MOVED is a lifecycle event. The boundary re-transform described above
    // re-applies the same commit on every poll while the source sits idle (`max_lsn == after`, a
    // no-op against the mirror), so keeping one `info!` here would emit a line per table per
    // `poll_interval` forever — claiming an advance that did not happen — and bury the cycles that
    // did move. The idle re-scan is diagnostic detail: `debug`.
    if max_lsn > after {
        tracing::info!(
            table = %format_args!("{}.{}", ctx.schema, ctx.table),
            transformed = %max_lsn,
            "Phase B: mirror updated, transformed_lsn advanced"
        );
    } else {
        tracing::debug!(
            table = %format_args!("{}.{}", ctx.schema, ctx.table),
            transformed = %max_lsn,
            "Phase B: boundary re-transform, transformed_lsn unchanged"
        );
    }
    Ok(Some(max_lsn))
}
