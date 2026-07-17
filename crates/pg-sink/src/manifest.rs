//! Durability step (b): after the Parquet PUT is durable in S3 (PR 2.24), **commit** a `file_manifest`
//! `ready` row — the loader's work-queue entry (§1.5).
//!
//! This is a thin adapter: map a [`WrittenObject`] → `control::NewManifestFile` and delegate to
//! `control::insert_ready` (PR 1.4), so the `WHERE status='ready'` partial index and the
//! `ORDER BY lsn_end, id` claim contract stay in one place. **`lsn_end` is the commit LSN** carried
//! from the `SealedBatch` — never `max(row.lsn)`, which would silently drop a late-committing large txn.
//!
//! **Ordering & at-least-once:** the row is committed *only after* the PUT returns durable. A crash
//! *between* the PUT and this commit leaves no `ready` row, so the batch re-streams and re-writes — no
//! loss. A duplicated INSERT after such a retry just produces a second `ready` row for the same object;
//! the loader's row-level `ON CONFLICT` (append idempotency) absorbs it.

use crate::sink::WrittenObject;

/// Record a durable object as a `ready` work-queue row. **Call ONLY after the PUT is durable.** Returns
/// the manifest `id`.
pub async fn record_ready(
    ex: impl sqlx::PgExecutor<'_>,
    epoch: i64,
    obj: &WrittenObject,
) -> Result<i64, ManifestError> {
    record_ready_with_reload(ex, epoch, obj, None).await
}

/// As [`record_ready`], carrying the `reload_id` a `kind='reload'` chunk file belongs to (PR 6.5)
/// — the loader's routing/purge key. Stream/snapshot/spill objects pass `None`.
pub async fn record_ready_with_reload(
    ex: impl sqlx::PgExecutor<'_>,
    epoch: i64,
    obj: &WrittenObject,
    reload_id: Option<i64>,
) -> Result<i64, ManifestError> {
    Ok(control::insert_ready(ex, &to_ready_row(epoch, obj, reload_id)).await?)
}

/// `WrittenObject` → the `ready` row (`kind` from the object, `lsn_end` = commit LSN).
fn to_ready_row(
    epoch: i64,
    obj: &WrittenObject,
    reload_id: Option<i64>,
) -> control::NewManifestFile {
    control::NewManifestFile {
        epoch,
        source_schema: obj.source_schema.clone(),
        source_table: obj.source_table.clone(),
        s3_uri: obj.s3_uri.clone(),
        kind: obj.kind,
        row_count: obj.row_count as i64,
        lsn_start: obj.lsn_start,
        lsn_end: obj.lsn_end,
        schema_version: obj.schema_version,
        reload_id,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error(transparent)]
    Control(#[from] control::ControlError),
}

#[cfg(test)]
#[path = "manifest_test.rs"]
mod tests;
