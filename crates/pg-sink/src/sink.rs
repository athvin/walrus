//! Arrow → Parquet → S3 PUT (§1.4) — **step (a) of the durability checkpoint** (§1.5).
//!
//! Encode a [`SealedBatch`] to Parquet with arrow-rs's `AsyncArrowWriter` and stream it straight into
//! an S3 object via `object_store`'s multipart `BufWriter` — never materialising the file on local
//! disk. `close()` (which completes the multipart) is the **durability point**: `put` returns a
//! [`WrittenObject`] only after it, because the manifest INSERT (PR 2.25) and the slot advance
//! (PR 2.26) must never get ahead of a batch that isn't durably in S3 (the WAL-bounding invariant).
//!
//! The object key is the epoch-namespaced layout `<epoch>/<schema>/<table>/<lsn_end>-<uuid>.parquet`,
//! with `lsn_end` as zero-padded 16-hex ([`common::Lsn`]'s `Display`) so keys sort in commit order.

use crate::batch::SealedBatch;
use common::Lsn;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use std::sync::Arc;

/// Whether the object holds streamed WAL rows or backfill snapshot rows (PR 2.29).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Stream,
    Snapshot,
}

impl FileKind {
    /// The `file_manifest.kind` string.
    pub fn as_str(self) -> &'static str {
        match self {
            FileKind::Stream => "stream",
            FileKind::Snapshot => "snapshot",
        }
    }
}

/// The result of a durable S3 PUT — everything PR 2.25 needs for the manifest row.
#[derive(Debug, Clone)]
pub struct WrittenObject {
    pub s3_uri: String,
    pub key: Path,
    pub source_schema: String,
    pub source_table: String,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub row_count: u64,
    pub schema_version: i64,
    pub kind: FileKind,
}

/// Encodes sealed batches to Parquet and PUTs them to S3, epoch-namespaced.
pub struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    epoch: i64,
}

impl ParquetSink {
    pub fn new(store: Arc<dyn ObjectStore>, bucket: String, epoch: i64) -> Self {
        ParquetSink {
            store,
            bucket,
            epoch,
        }
    }

    /// Best-effort delete of a staged object — used to clean up an aborted streamed txn's speculative
    /// files (PR 2.30), which have no manifest row pointing at them.
    pub async fn delete(&self, key: &Path) -> Result<(), SinkError> {
        self.store.delete(key).await?;
        Ok(())
    }

    /// `<epoch>/<schema>/<table>/<lsn_end>-<uuid>.parquet`. `lsn_end` is zero-padded 16-hex so keys
    /// sort by commit LSN; `uuid` matches the batch's `batch_id`-style file identity.
    pub fn object_key(&self, schema: &str, table: &str, lsn_end: Lsn, uuid: &str) -> Path {
        Path::from(format!(
            "{}/{}/{}/{}-{}.parquet",
            self.epoch, schema, table, lsn_end, uuid
        ))
    }

    /// Encode `batch` to Parquet (MICROS temporals + Snappy, inherited from the Arrow schema and the
    /// walrus writer properties) and stream it to S3 via multipart. Returns **only once durable**.
    /// Streamed WAL rows; the backfill (PR 2.29) uses [`Self::put_with_kind`].
    pub async fn put(&self, batch: SealedBatch) -> Result<WrittenObject, SinkError> {
        self.put_with_kind(batch, FileKind::Stream).await
    }

    /// As [`Self::put`], stamping the object's provenance (`stream` vs `snapshot`) — the manifest row's
    /// `kind` (PR 2.25). Snapshot files all share `lsn_end = consistent_point`, `id`-disambiguated.
    pub async fn put_with_kind(
        &self,
        batch: SealedBatch,
        kind: FileKind,
    ) -> Result<WrittenObject, SinkError> {
        let uuid = uuid::Uuid::new_v4().to_string();
        let key = self.object_key(&batch.schema, &batch.table, batch.lsn_end, &uuid);

        // Multipart streaming writer — no local temp file, no whole-batch buffering.
        let buf_writer = BufWriter::new(self.store.clone(), key.clone());
        let props = pg_to_arrow::default_writer_properties();
        let mut writer =
            AsyncArrowWriter::try_new(buf_writer, batch.record_batch.schema(), Some(props))
                .map_err(|e| SinkError::Encode(e.to_string()))?;
        writer
            .write(&batch.record_batch)
            .await
            .map_err(|e| SinkError::Encode(e.to_string()))?;
        // close() finalises the Parquet footer AND completes the multipart upload — the durability
        // point. Nothing downstream may observe this batch before this returns Ok.
        writer
            .close()
            .await
            .map_err(|e| SinkError::Encode(e.to_string()))?;

        Ok(WrittenObject {
            s3_uri: format!("s3://{}/{}", self.bucket, key),
            key,
            source_schema: batch.schema,
            source_table: batch.table,
            lsn_start: batch.lsn_start,
            lsn_end: batch.lsn_end,
            row_count: batch.row_count,
            schema_version: batch.schema_version,
            kind,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("parquet encode: {0}")]
    Encode(String),
    #[error(transparent)]
    Store(#[from] object_store::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink() -> ParquetSink {
        ParquetSink::new(
            Arc::new(object_store::memory::InMemory::new()),
            "walrus".into(),
            5,
        )
    }

    #[test]
    fn object_key_is_epoch_namespaced_and_lsn_sortable() {
        let s = sink();
        let lsn: Lsn = "0/1A2B3C".parse().unwrap();
        let key = s.object_key("public", "orders", lsn, "abcd");
        // <epoch>/<schema>/<table>/<lsn_end 16-hex>-<uuid>.parquet
        assert_eq!(key.as_ref(), format!("5/public/orders/{lsn}-abcd.parquet"));
        assert_eq!(lsn.to_string().len(), 16, "lsn is zero-padded 16-hex");

        // Zero-padded 16-hex means byte-lexical order matches commit-LSN order.
        let lo = s.object_key("public", "orders", "0/100".parse().unwrap(), "u");
        let hi = s.object_key("public", "orders", "1/0".parse().unwrap(), "u");
        assert!(lo.as_ref() < hi.as_ref(), "keys sort by commit LSN");
    }
}
