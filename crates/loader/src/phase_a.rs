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
use common::Lsn;

/// Everything one Phase-A pass needs for one owned table.
pub struct TableCtx<'a> {
    pub pool: &'a sqlx::PgPool,
    pub epoch: i64,
    pub schema: String,
    pub table: String,
    pub db: &'a TableDb,
    /// Files claimed per cycle (cadence lives in the PR 3.4 loop).
    pub max_files: i64,
}

/// One Phase-A pass. Returns the max `lsn_end` appended, or `None` if the queue was empty.
pub async fn run_phase_a(ctx: &TableCtx<'_>) -> Result<Option<Lsn>, LoaderError> {
    // 1. Claim in (lsn_end, id) order — NEVER `lsn_end > raw_appended_lsn` (that skips equal-lsn_end
    //    snapshot files forever).
    let claimed =
        control::claim_ready(ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, ctx.max_files).await?;
    if claimed.is_empty() {
        return Ok(None);
    }

    // 2. Append each file verbatim to <table>_raw (DuckDB auto-commits each statement). Idempotent.
    let mut max_lsn = Lsn::ZERO;
    let mut ids = Vec::with_capacity(claimed.len());
    let mut appended = 0u64;
    for f in &claimed {
        appended += ctx.db.append_parquet(&ctx.table, &f.s3_uri)?;
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
