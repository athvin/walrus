//! Write a `RecordBatch` to Parquet with arrow-rs.
//!
//! The one rule (walrus-pg-sink.md §2.1): DuckDB reads Parquet's **native** logical types, so we
//! must **not** coerce temporals. arrow-rs already emits `TIMESTAMP(MICROS, isAdjustedToUTC=…)`
//! straight from `Timestamp(Microsecond, tz)` — coercing to NANOS/MILLIS is exactly the bug §2.1
//! warns about. So `default_writer_properties` only sets compression and leaves the temporal
//! encoding to arrow-rs. PR 2.11's conformance tests prove the round-trip through in-process DuckDB.

use crate::error::Error;
use arrow::array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// The walrus writer settings: Snappy compression + arrow-rs's native MICROS temporal encoding
/// (no NANOS/MILLIS coercion).
pub fn default_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

/// Stream one batch to Parquet using the walrus writer properties.
pub fn write_parquet<W: std::io::Write + Send>(batch: &RecordBatch, sink: W) -> Result<(), Error> {
    let mut writer = ArrowWriter::try_new(sink, batch.schema(), Some(default_writer_properties()))?;
    writer.write(batch)?;
    writer.close()?;
    Ok(())
}

/// Convenience: write one batch to an in-memory Parquet buffer.
pub fn write_parquet_bytes(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    write_parquet(batch, &mut buf)?;
    Ok(buf)
}

#[cfg(test)]
#[path = "parquet_test.rs"]
mod tests;
