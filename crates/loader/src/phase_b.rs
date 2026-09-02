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
    transform_at_version(ctx, ver, &ctx.table).await
}

async fn transform_at_version(
    ctx: &TableCtx,
    ver: common::SchemaVersionNo,
    physical_table: &str,
) -> Result<TransformSql, LoaderError> {
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
            let plan = TablePlan::from_registry(&rel, &r.descriptors).for_table(physical_table);
            Ok(TransformSql::from_plan(&plan))
        }
        None => Ok(TransformSql::from_plan(
            &TablePlan::tier1(&ctx.rel).for_table(physical_table),
        )),
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
    let build = ctx.db.reload_build()?;
    let physical_table = build
        .as_ref()
        .map_or(ctx.table.as_str(), |build| build.shadow_table.as_str());
    let conn = ctx.db.conn();
    let raw = DuckTable::<Mirror>::new(physical_table).to_raw();
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
    let mut applied = None;
    if let Some(max_hex) = max_hex {
        let max_lsn: Lsn = max_hex.parse().map_err(|source| LoaderError::LsnParse {
            field: "max commit_lsn",
            source,
        })?;

        // A live transform uses the canonical schema watermark; a reload transform uses the frozen
        // version persisted with its shadow. In both cases the physical name is explicit.
        let version = build
            .as_ref()
            .map_or(ctx.db.schema_version()?, |build| build.schema_version);
        let t = transform_at_version(ctx, version, physical_table).await?;
        let ducklake = ctx.db.is_ducklake();
        ctx.db.in_txn("transform", |conn| {
            if ducklake {
                apply_transform_ducklake(conn, &t, after)
            } else {
                apply_transform(conn, &t, after)
            }
        })?;

        // Advance the watermark AFTER the DuckDB commit. The CHECK
        // (transformed_lsn <= raw_appended_lsn) holds because Phase A ran first this cycle.
        control::advance_transformed(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, max_lsn)
            .await?;
        if max_lsn > after {
            tracing::info!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                transformed = %max_lsn,
                generation = physical_table,
                "Phase B: mirror updated, transformed_lsn advanced"
            );
        } else {
            tracing::debug!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                transformed = %max_lsn,
                generation = physical_table,
                "Phase B: boundary re-transform, transformed_lsn unchanged"
            );
        }
        applied = Some(max_lsn);
    }

    let Some(build) = build else {
        return Ok(applied);
    };

    // `claim_ready` is an ordered read-only claim. Once its head is above H (or absent), every
    // durable manifest through H has been appended to the shadow. H itself is an explicit marker,
    // not a data row: advancing both checkpoint fields to it is what lets an empty dump complete.
    let next = control::claim_ready(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, 1).await?;
    if !queue_drained_through(
        next.first().map(|manifest| manifest.lsn_end),
        build.final_lsn,
    ) {
        return Ok(applied);
    }

    let mut tx = ctx
        .pool
        .begin()
        .await
        .map_err(|source| LoaderError::ControlTxn {
            op: "begin reload marker advance txn",
            source,
        })?;
    control::advance_raw_appended(
        &mut *tx,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        build.final_lsn,
    )
    .await?;
    control::advance_transformed(
        &mut *tx,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        build.final_lsn,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|source| LoaderError::ControlTxn {
            op: "commit reload marker advance txn",
            source,
        })?;

    if ctx.db.publish_reload_shadow(&ctx.table, build.reload_id)? {
        // Only publication replaces the quarantined live generation. Beginning an export must not
        // make readiness healthy while users still see the old incompatible table.
        ctx.state.clear_quarantine();
        tracing::info!(
            table = %format_args!("{}.{}", ctx.schema, ctx.table),
            reload_id = %build.reload_id,
            final_lsn = %build.final_lsn,
            "reload shadow atomically published at its explicit end marker"
        );
    }
    Ok(Some(
        applied.map_or(build.final_lsn, |lsn| lsn.max(build.final_lsn)),
    ))
}

/// Whether the ordered ready queue has no remaining file at or below the explicit end marker.
/// `None` is deliberately success: a zero-row export is represented by markers, not a fake row.
fn queue_drained_through(next_lsn: Option<Lsn>, final_lsn: Lsn) -> bool {
    next_lsn.is_none_or(|lsn| lsn > final_lsn)
}

#[cfg(test)]
#[path = "phase_b_test.rs"]
mod tests;
