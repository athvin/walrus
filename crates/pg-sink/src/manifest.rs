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
    Ok(control::insert_ready(ex, &to_ready_row(epoch, obj)).await?)
}

/// `WrittenObject` → the `ready` row (`kind` from the object, `lsn_end` = commit LSN).
fn to_ready_row(epoch: i64, obj: &WrittenObject) -> control::NewManifestFile {
    control::NewManifestFile {
        epoch,
        source_schema: obj.source_schema.clone(),
        source_table: obj.source_table.clone(),
        s3_uri: obj.s3_uri.clone(),
        kind: obj.kind.as_str().to_string(),
        row_count: obj.row_count as i64,
        lsn_start: obj.lsn_start,
        lsn_end: obj.lsn_end,
        schema_version: obj.schema_version,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error(transparent)]
    Control(#[from] control::ControlError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::FileKind;
    use object_store::path::Path;

    #[test]
    fn maps_written_object_to_a_stream_ready_row() {
        let obj = WrittenObject {
            s3_uri: "s3://walrus/7/public/orders/000000000000A100-uuid.parquet".to_string(),
            key: Path::from("7/public/orders/000000000000A100-uuid.parquet"),
            source_schema: "public".to_string(),
            source_table: "orders".to_string(),
            lsn_start: "0/100".parse().unwrap(),
            lsn_end: "0/A100".parse().unwrap(),
            row_count: 42,
            schema_version: 3,
            kind: FileKind::Stream,
        };
        let row = to_ready_row(9, &obj);
        assert_eq!(row.epoch, 9);
        assert_eq!(row.source_schema, "public");
        assert_eq!(row.source_table, "orders");
        assert_eq!(row.kind, "stream");
        assert_eq!(row.row_count, 42);
        assert_eq!(row.lsn_end, "0/A100".parse().unwrap());
        assert_eq!(row.schema_version, 3);
    }
}
