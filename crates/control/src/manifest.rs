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

/// The kind of a `file_manifest` row — the canonical enum for the `kind` text column, shared by the
/// sink (which writes it; pg-sink re-exports this as `FileKind`) and the loader (which routes on it).
///
/// `Spill` is a *single* streamed transaction written before its commit LSN was known (PR 4.3); the
/// loader treats the file's `lsn_end` — not the per-row placeholder — as the authoritative
/// `commit_lsn` for its rows. `Reload` chunk files (PR 6.1+) enter the same `(lsn_end, id)` claim
/// order carrying a `reload_id`; `Snapshot`/`Stream` rows never set it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestKind {
    Snapshot,
    Stream,
    Spill,
    Reload,
}

impl ManifestKind {
    /// The exact `file_manifest.kind` string persisted in the control DB.
    pub fn as_str(self) -> &'static str {
        match self {
            ManifestKind::Snapshot => "snapshot",
            ManifestKind::Stream => "stream",
            ManifestKind::Spill => "spill",
            ManifestKind::Reload => "reload",
        }
    }
}

impl std::str::FromStr for ManifestKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "snapshot" => Ok(ManifestKind::Snapshot),
            "stream" => Ok(ManifestKind::Stream),
            "spill" => Ok(ManifestKind::Spill),
            "reload" => Ok(ManifestKind::Reload),
            other => Err(format!("unknown manifest kind: {other}")),
        }
    }
}

/// The lifecycle state of a `file_manifest` row: `Ready` to claim, or dead-lettered `Failed` (a
/// poison file that can't block the queue — see [`mark_failed`]). Applied rows are DELETED, never
/// kept (see [`delete_claimed`]), so those are the only two persisted states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestStatus {
    Ready,
    Failed,
}

impl ManifestStatus {
    /// The exact `file_manifest.status` string persisted in the control DB.
    pub fn as_str(self) -> &'static str {
        match self {
            ManifestStatus::Ready => "ready",
            ManifestStatus::Failed => "failed",
        }
    }
}

impl std::str::FromStr for ManifestStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ready" => Ok(ManifestStatus::Ready),
            "failed" => Ok(ManifestStatus::Failed),
            other => Err(format!("unknown manifest status: {other}")),
        }
    }
}

/// A `ready` file the loader can claim. The column set is exactly what the claim query reads.
///
/// `kind` is `Snapshot | Stream | Spill | Reload` — reload chunk files (PR 6.1+) enter this same
/// queue and sort into the same `(lsn_end, id)` order, carrying the `reload_id` the loader's
/// rebuild trigger routes on (PR 6.7). Stream/snapshot/spill rows never set `reload_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRow {
    pub id: i64,
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub s3_uri: String,
    pub kind: ManifestKind,
    pub row_count: i64,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub schema_version: i64,
    pub status: ManifestStatus,
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
    pub kind: ManifestKind,
    pub row_count: i64,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub schema_version: i64,
    /// Set (with `kind=Reload`) only by the chunk export engine (PR 6.5); `None` otherwise.
    pub reload_id: Option<i64>,
}

/// Insert a `status='ready'` row with `lsn_end` set to the commit LSN; returns the new `id`.
pub async fn insert_ready(
    executor: impl PgExecutor<'_>,
    f: &NewManifestFile,
) -> Result<i64, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/insert_ready.sql",
        f.epoch,
        f.source_schema,
        f.source_table,
        f.s3_uri,
        f.kind.as_str(),
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
    // The `kind`/`status` text columns decode to `String` here, then parse into the typed enums.
    // A value outside the known set is a data-integrity bug (the sink only ever writes `as_str()`),
    // so it maps to the terminal `Decode`. The SQL text is unchanged, so the committed `.sqlx`
    // offline cache stays valid without a regenerate.
    let rows = sqlx::query_file!(
        "sql/postgres/queries/claim_ready.sql",
        epoch,
        source_schema,
        source_table,
        limit,
    )
    .fetch_all(executor)
    .await
    .map_err(ControlError::Connect)?;
    rows.into_iter()
        .map(|r| {
            Ok(ManifestRow {
                id: r.id,
                epoch: r.epoch,
                source_schema: r.source_schema,
                source_table: r.source_table,
                s3_uri: r.s3_uri,
                kind: r.kind.parse().map_err(ControlError::Decode)?,
                row_count: r.row_count,
                lsn_start: r.lsn_start,
                lsn_end: r.lsn_end,
                schema_version: r.schema_version,
                status: r.status.parse().map_err(ControlError::Decode)?,
                reload_id: r.reload_id,
            })
        })
        .collect()
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
    let row = sqlx::query_file!(
        "sql/postgres/queries/max_ready_lsn_end.sql",
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
    let result = sqlx::query_file!("sql/postgres/queries/delete_claimed.sql", ids,)
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
    let done = sqlx::query_file!(
        "sql/postgres/queries/delete_superseded.sql",
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
    sqlx::query_file!("sql/postgres/queries/mark_failed.sql", id,)
        .execute(executor)
        .await
        .map_err(ControlError::Connect)?;
    Ok(())
}

#[cfg(test)]
#[path = "manifest_test.rs"]
mod tests;
