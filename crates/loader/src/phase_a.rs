//! Phase A — the loader's ingest half (loader §4). **Claim** the next `ready` manifest files in
//! `(lsn_end, id)` order, **append every row verbatim** into `<table>_raw` with DuckDB, then — in **one
//! control-DB transaction** — advance `raw_appended_lsn = max(claimed lsn_end)` **and** delete the
//! claimed queue rows. No transform: this ends with a faithful, idempotent CDC log.
//!
//! **Two guards, both load-bearing (§4 crash-window):** (1) the queue *deletion* is what advances the
//! frontier (not the watermark alone); (2) the `<table>_raw` composite PK + `ON CONFLICT DO NOTHING`
//! absorbs a replay. DuckDB and control Postgres cannot share a transaction, so the ordering is strict:
//! the DuckDB append **commits first**, then the Postgres advance+delete txn. A crash between them
//! re-claims the still-`ready` file — which the row-level PK makes a no-op.

use crate::duck::TableDb;
use crate::error::LoaderError;
use crate::health::LoaderState;
use common::{Lsn, PgRelation};
use std::sync::Arc;
use std::time::Duration;

/// Everything one owned table's apply worker needs — **owned** (one `TableDb`/DuckDB connection per
/// table, never shared), so it can move into a `spawn_local`'d [`crate::apply_loop::apply_loop`].
pub struct TableCtx {
    pub pool: sqlx::PgPool,
    pub epoch: i64,
    pub schema: String,
    pub table: String,
    /// The table shape — the transform (Phase B) renders its SQL from this.
    pub rel: PgRelation,
    pub db: TableDb,
    pub state: Arc<LoaderState>,
    /// Files claimed per cycle.
    pub max_files: i64,
    /// The apply-loop poll cadence.
    pub poll_interval: Duration,
    /// The compaction cadence — full-rebuild + prune, on this worker thread after an apply cycle (PR 3.11).
    pub compaction_interval: Duration,
    /// Raw retention as an LSN-byte lag behind `transformed_lsn` (the prune floor).
    pub retention_lsn_lag: u64,
    /// The reload_id whose claim pause was already logged (PR 6.6) — a paused table says *why* it
    /// is idle once per pause, not once per poll. Per-table by construction (one `TableCtx` per
    /// worker); interior mutability so `run_phase_a(&ctx)` keeps its shared-ref signature.
    pub pause_logged: std::sync::Mutex<Option<i64>>,
    /// reload_ids already identified as `resync` (PR 6.10). A resync never sets the meta latch, so
    /// every one of its chunk files would otherwise re-enter `route_reload_file`'s "greater" arm and
    /// re-fetch the reload row; caching the flavor here makes chunks 2…n a plain append with no
    /// per-file lookup. Per-table interior mutability, like `pause_logged`.
    pub resync_ids: std::sync::Mutex<std::collections::HashSet<i64>>,
}

/// The once-per-pause transition: `Some(reload_id)` exactly when a NEW pause begins (a different
/// reload than last logged, or the first). A lifted pause (no live rebuild) clears the latch so
/// the next reload logs again.
pub fn pause_began(logged: &std::sync::Mutex<Option<i64>>, live: Option<i64>) -> Option<i64> {
    let mut slot = logged.lock().unwrap();
    match (*slot, live) {
        (prev, Some(id)) if prev != Some(id) => {
            *slot = Some(id);
            Some(id)
        }
        (_, None) => {
            *slot = None;
            None
        }
        _ => None,
    }
}

/// One Phase-A pass. Returns the max `lsn_end` appended, or `None` if the queue was empty.
pub async fn run_phase_a(ctx: &TableCtx) -> Result<Option<Lsn>, LoaderError> {
    // Observability (PR 5.6): set the Phase-A backlog gauge every poll (0 when caught up) —
    // `max(lsn_end over ready files) − raw_appended_lsn`. Both operands are cheap indexed control-DB
    // reads; doing this before the claim means idle polls report a truthful 0.
    let max_ready =
        control::max_ready_lsn_end(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?;
    let raw_appended = control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
        .await?
        .map(|cp| cp.raw_appended_lsn)
        .unwrap_or(Lsn::ZERO);
    common::metrics::set_raw_append_lag(
        &format!("{}.{}", ctx.schema, ctx.table),
        raw_append_lag_bytes(max_ready, raw_appended),
    );

    // 1. Claim in (lsn_end, id) order — NEVER `lsn_end > raw_appended_lsn` (that skips equal-lsn_end
    //    snapshot files forever).
    let claimed =
        control::claim_ready(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, ctx.max_files).await?;
    if claimed.is_empty() {
        // Distinguish IDLE from PAUSED (PR 6.6): a live rebuild-flavor reload withholds this
        // table's claims (reload §2 — claiming would retire post-`W` files the rebuild must
        // replay). Only probe when a backlog exists, and log the reason once per pause.
        if max_ready.is_some() {
            let live = control::reload::active_rebuilds(&ctx.pool, ctx.epoch)
                .await?
                .into_iter()
                .find(|r| r.source_schema == ctx.schema && r.source_table == ctx.table)
                .map(|r| r.reload_id);
            if let Some(reload_id) = pause_began(&ctx.pause_logged, live) {
                tracing::info!(
                    table = %format_args!("{}.{}", ctx.schema, ctx.table),
                    reload_id,
                    reason = "rebuild-in-flight",
                    "claims paused: ready rows accumulate (frontier frozen at W) until export_complete"
                );
            }
        } else {
            pause_began(&ctx.pause_logged, None); // caught up — clear the latch
        }
        return Ok(None);
    }
    pause_began(&ctx.pause_logged, None); // claiming again — any pause has lifted

    // 2. Append each file verbatim to <table>_raw (DuckDB auto-commits each statement). Idempotent.
    //    Files are claimed in (lsn_end, id) = commit order, and the sink cuts a fresh homogeneous file at
    //    every structural change, so schema_version is monotonic across `claimed`. Before appending a
    //    file at a NEWER version, reconcile both tables UP TO it (PR 3.8) — so `<table>_raw` always has
    //    exactly the file's columns and the verbatim `SELECT *` append lines up; already-appended older
    //    rows read NULL for the freshly-added column (additive superset).
    let mut max_lsn = Lsn::ZERO;
    let mut ids = Vec::with_capacity(claimed.len());
    let mut appended = 0u64;
    for f in &claimed {
        // kind='reload' routing (PR 6.7, H8/H9): greater ⇒ rebuild-then-append; equal ⇒ plain
        // append (chunks 2…n); less ⇒ a stale attempt's file — retire it unapplied (its id joins
        // the end-of-batch delete, its lsn_end never advances the frontier, DuckDB is untouched).
        if f.kind == "reload" && !route_reload_file(ctx, f).await? {
            tracing::debug!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                manifest_id = f.id,
                stale_reload_id = f.reload_id,
                "stale reload file retired unapplied (latest-id wins)"
            );
            ids.push(f.id);
            continue;
        }
        if f.schema_version > ctx.db.schema_version()? {
            if let Err(e) = crate::ddl::reconcile_to_version(
                &ctx.db,
                &ctx.pool,
                ctx.epoch,
                &ctx.schema,
                &ctx.table,
                f.schema_version,
            )
            .await
            {
                // A lossy DDL cast that fails is a QUARANTINE (PR 3.9): latch the state so `/ready`
                // degrades, fire a loud error-level alert, and stop — never a silent continue.
                if matches!(e, LoaderError::Quarantine { .. }) {
                    ctx.state.quarantine();
                    tracing::error!(
                        table = %format_args!("{}.{}", ctx.schema, ctx.table),
                        error = %e,
                        "QUARANTINE: lossy schema change could not be applied — /ready degraded, processing stopped"
                    );
                }
                return Err(e);
            }
        }
        // A `spill` file is one streamed txn written before its commit LSN was known, so its per-row
        // `commit_lsn` is a placeholder; `lsn_end` (corrected on `Stream Commit`) is the real commit LSN
        // for every row. Stamp it so the transform's commit-LSN window can't drop a neighbour txn that
        // committed inside the spill's placeholder range (architecture.md §1.6). Other kinds append verbatim.
        let commit_lsn_override = (f.kind == "spill").then(|| f.lsn_end.to_string());
        appended += ctx.db.append_parquet(
            &ctx.table,
            &f.s3_uri,
            f.schema_version,
            commit_lsn_override.as_deref(),
        )?;
        max_lsn = max_lsn.max(f.lsn_end);
        ids.push(f.id);
    }

    // 3. ONE control-DB txn: advance the watermark to the batch max AND delete the claimed queue rows.
    //    (The append is already durable in DuckDB — step 2 committed.)
    let mut tx = ctx
        .pool
        .begin()
        .await
        .map_err(|e| LoaderError::Internal(format!("begin advance+delete txn: {e}")))?;
    control::advance_raw_appended(&mut *tx, ctx.epoch, &ctx.schema, &ctx.table, max_lsn).await?;
    control::delete_claimed(&mut *tx, &ids).await?;
    tx.commit()
        .await
        .map_err(|e| LoaderError::Internal(format!("commit advance+delete txn: {e}")))?;

    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        files = ids.len(),
        rows = appended,
        raw_appended = %max_lsn,
        "Phase A: appended to <table>_raw, watermark advanced, queue drained"
    );
    Ok(Some(max_lsn))
}

/// Route one claimed `kind='reload'` file (PR 6.7). Returns `true` to append it, `false` when it
/// is a STALE attempt's file to retire unapplied.
///
/// The trigger's order of operations — rebuild DuckDB → clear quarantine → purge superseded
/// manifest rows → set the meta latch → (caller) append — is crash-walked as follows: a crash
/// anywhere BEFORE the latch re-runs the whole trigger on redo, and every step is idempotent
/// (`CREATE OR REPLACE`-style drop+recreate, the flag clear, the purge). A crash AFTER the latch
/// but before the append leaves the file still claimed/ready, and the redo takes the
/// `equal ⇒ append` arm. Nothing needs a rewind: chunk stamps `L_i > W`, so the frozen frontier
/// only ever moves forward (H8).
async fn route_reload_file(ctx: &TableCtx, f: &control::ManifestRow) -> Result<bool, LoaderError> {
    let file_reload_id = f.reload_id.ok_or_else(|| {
        LoaderError::Internal(format!(
            "manifest row {} is kind='reload' but carries no reload_id",
            f.id
        ))
    })?;
    // Fast path (PR 6.10): a resync we've already classified — plain append, no `recorded` read and
    // no per-file reload-row fetch. A resync never latches, so without this cache every chunk would
    // re-enter the "greater" arm below and re-fetch.
    if ctx.resync_ids.lock().unwrap().contains(&file_reload_id) {
        return Ok(true);
    }
    let recorded = ctx.db.recorded_reload_id()?;
    if file_reload_id < recorded {
        return Ok(false); // a superseded attempt whose purge raced the claim (H9): retire
    }
    if file_reload_id == recorded {
        return Ok(true); // chunks 2…n of the attempt already rebuilt for
    }

    // Greater: the first file of a NEW attempt. The reload row carries the flavor: a `resync` merges
    // over the LIVE mirror (H3) — no clear, no purge, no latch, and raw history preserved (chunks
    // flow through Phase A like any file, PR 6.10); only a `reload` triggers the rebuild.
    let row = control::reload::get(&ctx.pool, file_reload_id)
        .await?
        .ok_or_else(|| {
            LoaderError::Internal(format!(
                "reload {file_reload_id} has chunk files but no table_reload row"
            ))
        })?;
    if row.flavor == control::ReloadFlavor::Resync {
        ctx.resync_ids.lock().unwrap().insert(file_reload_id);
        return Ok(true);
    }
    let first_lsn = row.first_lsn.ok_or_else(|| {
        LoaderError::Internal(format!(
            "reload {file_reload_id} has chunk files but no first_lsn"
        ))
    })?;

    // 1. Rebuild both tables, empty, at the FILE's schema_version (all of an attempt's chunks
    //    share it by construction — PR 6.8 enforces that across DDL).
    let plan = plan_at_version(ctx, f.schema_version).await?;
    ctx.db.rebuild_for_reload(&plan, f.schema_version)?;
    // 2. The quarantine latch (PR 3.9) clears: the rebuild replaced the data the lossy cast
    //    could not be applied to — this is the per-table recovery path v1 never had.
    ctx.state.clear_quarantine();
    // 3. Purge superseded pending rows: every non-reload file at lsn_end <= first_lsn describes a
    //    commit the chunks re-cover; applying them after the clear would only churn. Post-`W`
    //    stream files (lsn_end > first_lsn) survive and apply AFTER the chunks in (lsn_end, id)
    //    order — the interleave H8 promises.
    let purged =
        control::delete_superseded(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, first_lsn)
            .await?;
    // 4. The latch: the rebuild happens exactly once per reload_id — a crash-redo of this same
    //    file now takes the equal ⇒ append arm and cannot re-clear the table.
    ctx.db.set_recorded_reload_id(file_reload_id)?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        reload_id = file_reload_id,
        schema_version = f.schema_version,
        first_lsn = %first_lsn,
        purged,
        "reload rebuild: tables replaced at the attempt's version; superseded rows purged; latch set"
    );
    Ok(true)
}

/// The registry shape at `version` as a [`crate::plan::TablePlan`] (the Tier-2 emit/recombine
/// path, PR 4.2), falling back to the bootstrap relation's scalar shape for hermetic
/// single-version setups — `phase_b::current_transform`'s exact precedent.
async fn plan_at_version(
    ctx: &TableCtx,
    version: i64,
) -> Result<crate::plan::TablePlan, LoaderError> {
    match control::read_registry(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, version).await? {
        Some(r) => {
            let rel: PgRelation = serde_json::from_value(r.columns)
                .map_err(|e| LoaderError::Internal(format!("decode registry columns: {e}")))?;
            Ok(crate::plan::TablePlan::from_registry(&rel, &r.descriptors))
        }
        None => Ok(crate::plan::TablePlan::tier1(&ctx.rel)),
    }
}

/// The raw-append backlog in LSN-bytes: how far the newest ready file's commit LSN leads the Phase-A
/// frontier. An empty queue (`None`) is 0; a frontier already at/after the head saturates to 0 and
/// never underflows. This is the value of `walrus_loader_raw_append_lag_bytes` (PR 5.6).
fn raw_append_lag_bytes(max_ready_lsn_end: Option<Lsn>, raw_appended: Lsn) -> u64 {
    max_ready_lsn_end.map_or(0, |head| {
        head.as_u64().saturating_sub(raw_appended.as_u64())
    })
}

#[cfg(test)]
mod tests {
    use super::{pause_began, raw_append_lag_bytes};
    use common::Lsn;

    #[test]
    fn empty_queue_is_zero_lag() {
        assert_eq!(raw_append_lag_bytes(None, Lsn::new(100)), 0);
    }

    #[test]
    fn lag_is_head_minus_frontier() {
        assert_eq!(
            raw_append_lag_bytes(Some(Lsn::new(500)), Lsn::new(200)),
            300
        );
        assert_eq!(raw_append_lag_bytes(Some(Lsn::new(200)), Lsn::new(200)), 0);
    }

    #[test]
    fn frontier_ahead_of_queue_saturates_to_zero() {
        // A just-advanced frontier can momentarily lead a stale MAX read — never underflow.
        assert_eq!(raw_append_lag_bytes(Some(Lsn::new(100)), Lsn::new(300)), 0);
    }

    #[test]
    fn pause_logs_once_per_pause_and_relatches_on_a_new_reload() {
        let latch = std::sync::Mutex::new(None);
        assert_eq!(pause_began(&latch, Some(7)), Some(7), "a new pause logs");
        assert_eq!(
            pause_began(&latch, Some(7)),
            None,
            "same pause: silent on later polls"
        );
        assert_eq!(
            pause_began(&latch, None),
            None,
            "lifted: silent, latch cleared"
        );
        assert_eq!(
            pause_began(&latch, Some(8)),
            Some(8),
            "the next reload logs again"
        );
        assert_eq!(
            pause_began(&latch, Some(9)),
            Some(9),
            "a superseding reload (a PR 6.8 restart) logs without an intervening lift"
        );
    }
}
