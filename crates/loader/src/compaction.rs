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

/// Atomic full-rebuild: `CREATE OR REPLACE TABLE <table> AS SELECT <collapse over retained raw ∪
/// mirror-baseline, drop op='d'>`. Wrapped in one DuckDB transaction so a crash mid-rewrite rolls back
/// to the intact old mirror (readers on another connection see the old table until COMMIT). Reuses the
/// transform's dedup/collapse (TRUNCATE tuple boundary, TOAST resolution, `(commit_lsn, lsn)` ranking).
///
/// The `cancel` token is the PR 3.12 abort hook: checked before the rewrite starts, and — via
/// [`full_rebuild_abortable`], which interrupts the running DuckDB query — an in-flight rewrite that is
/// interrupted rolls back and returns `Ok(())` (an intentional drain abort; the idempotent rebuild
/// re-runs next cycle). Only a genuine (non-cancel) failure is an error.
pub fn full_rebuild(
    conn: &duckdb::Connection,
    t: &TransformSql,
    cancel: &CancellationToken,
) -> Result<(), LoaderError> {
    if cancel.is_cancelled() {
        return Ok(()); // shutting down — don't even start the heavy rewrite (PR 3.12)
    }
    // Rebuild over ALL retained raw (from LSN 0) plus the mirror baseline; the truncate boundary comes
    // from the retained tail exactly as the incremental path resolves it.
    let boundary = t.latest_truncate(conn, &Lsn::ZERO)?;
    conn.execute_batch("BEGIN TRANSACTION;")
        .map_err(|e| LoaderError::Duck(format!("begin rebuild txn: {e}")))?;
    if let Err(e) = conn.execute_batch(&t.render_rebuild(&boundary)) {
        let _ = conn.execute_batch("ROLLBACK;");
        if cancel.is_cancelled() {
            // Interrupted by the drain (the watcher called `interrupt()`): an intentional abort, the old
            // mirror is intact via ROLLBACK, and the rebuild re-runs next cycle. NOT an error.
            tracing::info!(
                table = t.table(),
                "full-rebuild aborted by drain (rolled back)"
            );
            return Ok(());
        }
        return Err(LoaderError::Duck(format!(
            "full rebuild {}: {e}",
            t.table()
        )));
    }
    conn.execute_batch("COMMIT;")
        .map_err(|e| LoaderError::Duck(format!("commit rebuild txn: {e}")))?;
    Ok(())
}

/// [`full_rebuild`] wrapped so an in-flight rewrite is **aborted** the instant `cancel` fires (PR 3.12).
/// The blocking `CREATE OR REPLACE` runs on this worker thread; a watcher task on the runtime pool holds
/// the connection's [`InterruptHandle`](duckdb) (Send + Sync) and calls `interrupt()` on cancellation,
/// which makes the running query error → `full_rebuild` rolls back and returns `Ok`. The watcher is
/// aborted once the rewrite returns (whether it completed or was interrupted).
pub async fn full_rebuild_abortable(
    conn: &duckdb::Connection,
    t: &TransformSql,
    cancel: &CancellationToken,
) -> Result<(), LoaderError> {
    let handle = conn.interrupt_handle();
    let watch = cancel.clone();
    let watcher = tokio::spawn(async move {
        watch.cancelled().await;
        handle.interrupt(); // cancel the running rewrite from another thread
    });
    let result = full_rebuild(conn, t, cancel);
    watcher.abort();
    result
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
