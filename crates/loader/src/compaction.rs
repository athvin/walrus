//! The self-heal-and-reclaim job (loader §5.7, §9.4) — the **only** thing that actually reclaims disk.
//!
//! **DuckDB storage truth:** a `DELETE` merely tombstones rows (the file does not shrink), and
//! `VACUUM FULL` is unimplemented. Space is reclaimed only by rewriting the table — here, an atomic
//! `CREATE OR REPLACE TABLE <table> AS SELECT …` over **retained raw ∪ the current mirror injected as an
//! LSN-floor baseline**, dropping `op='d'` winners. It reuses the incremental transform's dedup/collapse
//! (via [`TransformSql::render_rebuild`]) so the two paths can't drift, and the mirror baseline
//! guarantees a value whose last real write was already pruned is **never lost**.
//!
//! It runs on the table's **own worker thread**, serialized after an apply cycle (no separate connection,
//! no quiescing dance), holds the exclusive writer, and needs ~2× transient space for the rewrite.

use crate::error::LoaderError;
use crate::transform::TransformSql;
use common::Lsn;
use tokio_util::sync::CancellationToken;

/// Knobs for one full-rebuild. `retention_floor` is the LSN below which `<table>_raw` is pruned — the
/// caller computes it as `transformed_lsn - retention_lsn_lag`, so it is always behind `transformed_lsn`.
pub struct RebuildOpts {
    pub retention_floor: Lsn,
}

/// Atomic full-rebuild: `CREATE OR REPLACE TABLE <table> AS SELECT <collapse over retained raw ∪
/// mirror-baseline, drop op='d'>`. Wrapped in one DuckDB transaction so a crash mid-rewrite rolls back
/// to the intact old mirror (readers on another connection see the old table until COMMIT). Reuses the
/// transform's dedup/collapse (TRUNCATE tuple boundary, TOAST resolution, `(commit_lsn, lsn)` ranking).
///
/// The `cancel` token is the PR 3.12 abort hook: checked before the (potentially long) rewrite starts.
pub fn full_rebuild(
    conn: &duckdb::Connection,
    t: &TransformSql,
    cancel: &CancellationToken,
) -> Result<(), LoaderError> {
    if cancel.is_cancelled() {
        return Ok(()); // shutting down — skip the heavy rewrite (PR 3.12 aborts in-flight)
    }
    // Rebuild over ALL retained raw (from LSN 0) plus the mirror baseline; the truncate boundary comes
    // from the retained tail exactly as the incremental path resolves it.
    let boundary = t.latest_truncate(conn, &Lsn::ZERO)?;
    conn.execute_batch("BEGIN TRANSACTION;")
        .map_err(|e| LoaderError::Duck(format!("begin rebuild txn: {e}")))?;
    if let Err(e) = conn.execute_batch(&t.render_rebuild(&boundary)) {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(LoaderError::Duck(format!(
            "full rebuild {}: {e}",
            t.table()
        )));
    }
    conn.execute_batch("COMMIT;")
        .map_err(|e| LoaderError::Duck(format!("commit rebuild txn: {e}")))?;
    Ok(())
}

/// Reclaim: `DELETE FROM <table>_raw WHERE commit_lsn < floor` then `CHECKPOINT`. The `floor` must be
/// **behind `transformed_lsn`** (the caller guarantees it) so the incremental transform never loses a row
/// it still needs — and the rebuild's mirror baseline preserves any current value whose raw was pruned.
/// Returns the rows deleted. (The `DELETE` only tombstones; the space itself is reclaimed by the rebuild
/// above — assert reclamation before/after `full_rebuild`, not after `prune_raw`.)
pub fn prune_raw(
    conn: &duckdb::Connection,
    t: &TransformSql,
    floor: &Lsn,
) -> Result<u64, LoaderError> {
    let n = conn
        .execute(
            &format!(
                "DELETE FROM \"{}_raw\" WHERE \"_walrus_commit_lsn\" < ?",
                t.table()
            ),
            duckdb::params![floor.to_string()],
        )
        .map_err(|e| LoaderError::Duck(format!("prune {}_raw: {e}", t.table())))?;
    conn.execute_batch("CHECKPOINT;")
        .map_err(|e| LoaderError::Duck(format!("checkpoint after prune {}: {e}", t.table())))?;
    Ok(n as u64)
}

/// The retention floor for a table: `transformed_lsn - retention_lsn_lag`, saturating at 0. Always `<=
/// transformed_lsn`, so pruning below it can never drop a row the incremental transform still reads.
pub fn retention_floor(transformed_lsn: Lsn, retention_lsn_lag: u64) -> Lsn {
    Lsn::new(transformed_lsn.as_u64().saturating_sub(retention_lsn_lag))
}
