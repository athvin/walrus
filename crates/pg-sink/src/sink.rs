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

/// Whether the object holds streamed WAL rows, backfill snapshot rows (PR 2.29), or a **speculative
/// open-txn spill** (PR 4.3 fix). A `Spill` file is a *single* streamed transaction's rows written
/// before its commit LSN is known, so its rows carry a placeholder `commit_lsn`; the real commit LSN is
/// the file's `lsn_end`, stamped onto the manifest at `Stream Commit`. The loader therefore treats
/// `lsn_end` — not the per-row placeholder — as the authoritative `commit_lsn` for a `Spill` file, which
/// keeps commit-order correct (architecture.md §1.6). A multi-txn `Stream` batch keeps its per-row LSNs.
///
/// This is `control::ManifestKind`, the canonical enum for the `file_manifest.kind` column;
/// re-exported here under the sink-local name the writer path already uses (PR 8.2).
pub use control::ManifestKind as FileKind;

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

/// Encodes sealed batches to Parquet and PUTs them to S3, epoch-namespaced. Cheap to clone (the
/// store is an `Arc`) — reload exporters (PR 6.5) each carry their own handle.
#[derive(Clone)]
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
        // Flush-latency + throughput instrumentation (PR 4.10); no-op until a recorder is installed.
        let flush_start = std::time::Instant::now();
        let rows = batch.record_batch.num_rows() as u64;

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
        common::metrics::record_batch_flush(flush_start.elapsed().as_secs_f64(), rows);

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
#[path = "sink_test.rs"]
mod tests;
