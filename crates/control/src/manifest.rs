//! `file_manifest` models: the sink's insert-ready, the loader's claim-in-commit-order, and the
//! queue's retire-by-delete.
//!
//! The manifest is a **work queue, not a history**. The single load-bearing line is the claim
//! ordering: `ORDER BY lsn_end, id` — commit LSN, then `id` as the tiebreaker. It is *not*
//! `lsn_end > raw_appended_lsn`: the many snapshot files that share `consistent_point` all have the
//! same `lsn_end`, and a `>` filter would skip them forever. And it keys on the **commit** LSN, not
//! a max-row LSN, or a late-committing large transaction would be silently dropped. Retiring a file
//! is a `DELETE`, not a status flip — the queue's frontier advances by removal.

use crate::ControlError;
use common::Lsn;
use sqlx::PgExecutor;

/// A `ready` file the loader can claim. The column set is exactly what the claim query reads.
///
/// `kind` is `'snapshot' | 'stream' | 'reload'` — reload chunk files (PR 6.1+) enter this same
/// queue and sort into the same `(lsn_end, id)` order, carrying the `reload_id` the loader's
/// rebuild trigger routes on (PR 6.7). Stream/snapshot rows never set `reload_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRow {
    pub id: i64,
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub s3_uri: String,
    pub kind: String,
    pub row_count: i64,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub schema_version: i64,
    pub status: String,
    /// `Some` only for `kind='reload'` chunk files; the purge/routing key.
    pub reload_id: Option<i64>,
}

/// What the sink inserts after its Parquet is durable in S3 (PR 2.25).
#[derive(Debug, Clone)]
pub struct NewManifestFile {
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub s3_uri: String,
    pub kind: String,
    pub row_count: i64,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub schema_version: i64,
    /// Set (with `kind='reload'`) only by the chunk export engine (PR 6.5); `None` otherwise.
    pub reload_id: Option<i64>,
}

/// Insert a `status='ready'` row with `lsn_end` set to the commit LSN; returns the new `id`.
pub async fn insert_ready(
    executor: impl PgExecutor<'_>,
    f: &NewManifestFile,
) -> Result<i64, ControlError> {
    let rec = sqlx::query!(
        r#"
        INSERT INTO walrus.file_manifest
            (epoch, source_schema, source_table, s3_uri, kind, row_count,
             lsn_start, lsn_end, schema_version, status, reload_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'ready', $10)
        RETURNING id
        "#,
        f.epoch,
        f.source_schema,
        f.source_table,
        f.s3_uri,
        f.kind,
        f.row_count,
        f.lsn_start as Lsn,
        f.lsn_end as Lsn,
        f.schema_version,
        f.reload_id,
    )
    .fetch_one(executor)
    .await
    .map_err(ControlError::Connect)?;
    Ok(rec.id)
}

/// Claim the next `ready` files for a table **in commit order**.
///
/// `ORDER BY lsn_end, id` — `id` breaks equal-`lsn_end` ties. There is deliberately **no**
/// `lsn_end > raw_appended_lsn` predicate: that would skip the equal-`lsn_end` snapshot files.
///
/// **The pause predicate (PR 6.6, reload §2/H8):** while a `flavor='reload'` reload is
/// `requested|exporting`, claiming would apply-and-RETIRE post-`W` stream files into the old
/// mirror — and the rebuild would then clear that mirror with those events gone from the queue
/// forever. Not claiming is a complete pause: rows accumulate `ready`, the frontier freezes at
/// `W`, and the rebuild later replays the world in `(lsn_end, id)` order. The pause lives in the
/// QUERY (one statement, no check-then-claim TOCTOU) and lifts at `export_complete` — the loader
/// must claim again to reach the chunk files and trigger the rebuild (PR 6.7); pausing through
/// `export_complete` would deadlock the reload. `resync` never pauses (H3). The `NOT EXISTS`
/// probe is served by the `table_reload_one_live` partial index (its predicate
/// `status NOT IN ('complete','failed')` covers `requested|exporting`).
pub async fn claim_ready(
    executor: impl PgExecutor<'_>,
    epoch: i64,
    source_schema: &str,
    source_table: &str,
    limit: i64,
) -> Result<Vec<ManifestRow>, ControlError> {
    sqlx::query_as!(
        ManifestRow,
        r#"
        SELECT m.id, m.epoch, m.source_schema, m.source_table, m.s3_uri, m.kind, m.row_count,
               m.lsn_start AS "lsn_start: Lsn", m.lsn_end AS "lsn_end: Lsn", m.schema_version,
               m.status, m.reload_id
        FROM walrus.file_manifest m
        WHERE m.epoch = $1 AND m.source_schema = $2 AND m.source_table = $3 AND m.status = 'ready'
          AND NOT EXISTS (
              SELECT 1 FROM walrus.table_reload r
              WHERE r.epoch = m.epoch
                AND r.source_schema = m.source_schema
                AND r.source_table = m.source_table
                AND r.flavor = 'reload'
                AND r.status IN ('requested', 'exporting')
          )
        ORDER BY m.lsn_end, m.id
        LIMIT $4
        "#,
        epoch,
        source_schema,
        source_table,
        limit,
    )
    .fetch_all(executor)
    .await
    .map_err(ControlError::Connect)
}

/// The newest `ready` file's commit LSN for a table — the head of the Phase-A backlog — or `None`
/// when the queue is empty. Powers the `walrus_loader_raw_append_lag_bytes` gauge (PR 5.6): the lag
/// is this minus `raw_appended_lsn`. `MAX` over an empty set is SQL `NULL` → `None`.
pub async fn max_ready_lsn_end(
    executor: impl PgExecutor<'_>,
    epoch: i64,
    source_schema: &str,
    source_table: &str,
) -> Result<Option<Lsn>, ControlError> {
    let row = sqlx::query!(
        r#"
        SELECT MAX(lsn_end) AS "max_lsn_end: Lsn"
        FROM walrus.file_manifest
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND status = 'ready'
        "#,
        epoch,
        source_schema,
        source_table,
    )
    .fetch_one(executor)
    .await
    .map_err(ControlError::Connect)?;
    Ok(row.max_lsn_end)
}

/// Retire claimed rows — the queue's "done" is a `DELETE`, not a status flip. Returns the count.
pub async fn delete_claimed(
    executor: impl PgExecutor<'_>,
    ids: &[i64],
) -> Result<u64, ControlError> {
    let result = sqlx::query!("DELETE FROM walrus.file_manifest WHERE id = ANY($1)", ids,)
        .execute(executor)
        .await
        .map_err(ControlError::Connect)?;
    Ok(result.rows_affected())
}

/// Purge a rebuilding table's SUPERSEDED pending rows at trigger time (PR 6.7 / reload H8): every
/// non-reload row with `lsn_end <= first_lsn` describes a commit the chunks re-cover (`C <= L_1`
/// ⇒ visible to chunk 1's SELECT), so applying it after the rebuild would only re-apply history
/// the clear just replaced. Chunk 1 itself has `lsn_end = first_lsn` — the `kind` filter is what
/// lets it survive its own purge. No status filter: a dead-lettered (`failed`) pre-`W` file is
/// equally superseded. Idempotent (a re-run deletes nothing). Returns rows purged.
pub async fn delete_superseded(
    executor: impl PgExecutor<'_>,
    epoch: i64,
    source_schema: &str,
    source_table: &str,
    first_lsn: Lsn,
) -> Result<u64, ControlError> {
    let done = sqlx::query!(
        r#"
        DELETE FROM walrus.file_manifest
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
          AND kind <> 'reload' AND lsn_end <= $4
        "#,
        epoch,
        source_schema,
        source_table,
        first_lsn as Lsn,
    )
    .execute(executor)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(done.rows_affected())
}

/// Dead-letter a repeatedly-failing file (`status='failed'`) so a poison file can't block the queue.
pub async fn mark_failed(executor: impl PgExecutor<'_>, id: i64) -> Result<(), ControlError> {
    sqlx::query!(
        "UPDATE walrus.file_manifest SET status = 'failed' WHERE id = $1",
        id,
    )
    .execute(executor)
    .await
    .map_err(ControlError::Connect)?;
    Ok(())
}
