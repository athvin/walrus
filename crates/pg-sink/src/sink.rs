//! Arrow → Parquet → S3 PUT (§1.4) — **step (a) of the durability checkpoint** (§1.5).
//!
//! Encode a [`SealedBatch`] to Parquet with arrow-rs's [`AsyncArrowWriter`] and stream it straight
//! into an S3 object via `object_store`'s multipart [`BufWriter`] — never materialising the file on
//! local disk. `close()` (which completes the multipart) is the **durability point**: `put` returns a
//! [`WrittenObject`] only after it, because the manifest INSERT and the slot advance
//! must never get ahead of a batch that isn't durably in S3 (the WAL-bounding invariant).
//!
//! The object key is the epoch-namespaced layout `<epoch>/<schema>/<table>/<lsn_end>-<uuid>.parquet`,
//! with `lsn_end` as zero-padded 16-hex ([`common::Lsn`]'s `Display`) so keys sort in commit order.

use crate::batch::SealedBatch;
use common::{EpochNo, Lsn, SchemaVersionNo};
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use std::sync::Arc;

/// Whether the object holds streamed WAL rows, fenced reload rows, or a **speculative open-txn
/// spill**. The canonical enum also retains the legacy `Snapshot` wire value for old manifests. A
/// `Spill` file is a *single* streamed transaction's rows written
/// before its commit LSN is known, so its rows carry a placeholder `commit_lsn`; the real commit LSN is
/// the file's `lsn_end`, stamped onto the manifest at `Stream Commit`. The loader therefore treats
/// `lsn_end` — not the per-row placeholder — as the authoritative `commit_lsn` for a `Spill` file, which
/// keeps commit-order correct (architecture.md §1.6). A multi-txn `Stream` batch keeps its per-row LSNs.
///
/// This is `control::ManifestKind`, the canonical enum for the `file_manifest.kind` column;
/// re-exported here under the sink-local name the writer path already uses.
pub use control::ManifestKind as FileKind;

/// The result of a durable S3 PUT — everything the manifest writer needs for its row.
#[derive(Debug, Clone)]
pub struct WrittenObject {
    /// The full `s3://…` URI, as it will be written to the manifest row.
    pub s3_uri: String,
    /// The same object as a store-relative key — what a later delete needs, since the store API
    /// takes a key rather than a URI.
    pub key: Path,
    /// Source schema of the rows in the file.
    pub source_schema: String,
    /// Source table of the rows in the file.
    pub source_table: String,
    /// Commit LSN of the first transaction in the file.
    pub lsn_start: Lsn,
    /// Commit LSN of the last transaction in the file — the loader's claim-order key.
    pub lsn_end: Lsn,
    /// Rows written.
    pub row_count: u64,
    /// The shape the rows were encoded against.
    pub schema_version: SchemaVersionNo,
    /// Which producer wrote it, which is how the loader routes the file.
    pub kind: FileKind,
}

/// Encodes sealed batches to Parquet and PUTs them to S3, epoch-namespaced. Cheap to clone (the
/// store is an `Arc`) — reload exporters each carry their own handle.
#[derive(Clone, Debug)]
pub struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    epoch: EpochNo,
}

impl ParquetSink {
    /// `bucket` is *stored*, so it is taken as `impl Into<String>` and converted exactly once here:
    /// `app::establish_stream` hands over the owned name from its
    /// [`SinkConfig`](crate::config::SinkConfig), fixtures pass a `&str` literal, and no call site
    /// has to spell `.to_string()` to reach an owned `String`.
    ///
    /// That same parameter is why `#[must_use]` is written out here while [`Self::object_key`] got
    /// it from the lint: `clippy::must_use_candidate` treats a generic argument as possibly
    /// side-effecting and skips the function. Constructing a sink PUTs nothing by itself.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, bucket: impl Into<String>, epoch: EpochNo) -> Self {
        ParquetSink {
            store,
            bucket: bucket.into(),
            epoch,
        }
    }

    /// Best-effort delete of a staged object — used to clean up an aborted streamed txn's speculative
    /// files, which have no manifest row pointing at them.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::Store`] if the object store cannot delete `key`.
    pub async fn delete(&self, key: &Path) -> Result<(), SinkError> {
        self.store.delete(key).await?;
        Ok(())
    }

    /// `<epoch>/<schema>/<table>/<lsn_end>-<uuid>.parquet`. `lsn_end` is zero-padded 16-hex so keys
    /// sort by commit LSN; `uuid` matches the batch's `batch_id`-style file identity.
    #[must_use]
    pub fn object_key(&self, schema: &str, table: &str, lsn_end: Lsn, uuid: &str) -> Path {
        Path::from(format!(
            "{}/{}/{}/{}-{}.parquet",
            self.epoch, schema, table, lsn_end, uuid
        ))
    }

    /// Encode `batch` to Parquet (MICROS temporals + Snappy, inherited from the Arrow schema and the
    /// walrus writer properties) and stream it to S3 via multipart. Returns **only once durable**.
    /// Streamed WAL rows; reload and spill writers use [`Self::put_with_kind`].
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::Encode`] if Parquet encoding or multipart finalization fails, or
    /// [`SinkError::Store`] for an object-store transport failure.
    pub async fn put(&self, batch: SealedBatch) -> Result<WrittenObject, SinkError> {
        self.put_with_kind(batch, FileKind::Stream).await
    }

    /// As [`Self::put`], stamping the object's provenance in the manifest row's `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`SinkError::Encode`] if the Arrow batch cannot be encoded/finalized as Parquet, or
    /// [`SinkError::Store`] if multipart upload to the object store fails.
    pub async fn put_with_kind(
        &self,
        batch: SealedBatch,
        kind: FileKind,
    ) -> Result<WrittenObject, SinkError> {
        let uuid = uuid::Uuid::new_v4().to_string();
        let key = self.object_key(&batch.schema, &batch.table, batch.lsn_end, &uuid);
        // Flush-latency + throughput instrumentation; no-op until a recorder is installed.
        let flush_start = std::time::Instant::now();
        let rows = u64::try_from(batch.record_batch.num_rows()).unwrap_or(u64::MAX);

        // Multipart streaming upload — no local temp file.
        let buf_writer = BufWriter::new(Arc::clone(&self.store), key.clone());
        let props = pg_to_arrow::default_writer_properties();
        // The ENCODE stays on this task: `AsyncArrowWriter` wraps a synchronous `ArrowWriter`, so
        // `write` and `close` compress the whole batch before they await — the `async` in that name is
        // the upload, not the compression. This remains on the task because no profile identifies
        // compression as a scheduler bottleneck; move it to the blocking pool if measurements do.
        let mut writer =
            AsyncArrowWriter::try_new(buf_writer, batch.record_batch.schema(), Some(props))?;
        writer.write(&batch.record_batch).await?;
        // close() finalises the Parquet footer AND completes the multipart upload — the durability
        // point. Nothing downstream may observe this batch before this returns Ok.
        writer.close().await?;
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

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SinkError {
    /// The engine's error itself, not its rendering: only the typed value tells a footer write
    /// apart from an out-of-spec schema, and `ParquetError::External` nests a further cause that
    /// `to_string()` drops.
    #[error("parquet encode: {0}")]
    Encode(#[source] parquet::errors::ParquetError),
    /// The object store rejected or could not complete the request. `transparent` because
    /// `object_store::Error` already names the operation and the path.
    #[error(transparent)]
    Store(#[from] object_store::Error),
}

/// Every Parquet failure on the writer path — builder, `write`, footer/multipart `close` — is the
/// same class, so `?` does the wrapping instead of an identical closure at each call. Flattening to
/// `common::Error`'s stringly variants happens at the process edge below, so everything upstream of
/// it still sees the engine failure whole.
impl From<parquet::errors::ParquetError> for SinkError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        SinkError::Encode(error)
    }
}

impl From<SinkError> for common::Error {
    fn from(e: SinkError) -> Self {
        match e {
            SinkError::Store(inner) => common::Error::ObjectStore(inner.to_string()),
            SinkError::Encode(source) => {
                common::Error::Internal(format!("parquet encode: {source}"))
            }
        }
    }
}

#[cfg(test)]
#[path = "sink_test.rs"]
mod tests;
