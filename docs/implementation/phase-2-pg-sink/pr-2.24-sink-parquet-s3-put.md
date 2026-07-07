# PR 2.24 — Arrow → Parquet → S3 PUT (`object_store`)

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib),
> `pg-to-arrow` · **Est. size:** M · **Depends on:** PR 2.23 · **Unlocks:** PR 2.25

Take a `SealedBatch` (PR 2.23), encode it to Parquet with arrow-rs's `ArrowWriter` (MICROS temporals,
compression), and stream it straight into an S3 object via `object_store` — never fully materialising the
file on local disk. The object key is the design's epoch-namespaced layout:
`<epoch>/<source_schema>/<table>/<lsn_end>-<batch_uuid>.parquet`. **This is step (a) of the durability
checkpoint** — the PUT that must be durable *before* the manifest INSERT (PR 2.25) and *long* before the
slot advances (PR 2.26). Get this ordering seam right; 2.25 and 2.26 build directly on it.

## Why — learning objectives

By the end of this PR you will have practised:

- **`parquet::arrow::AsyncArrowWriter` into an `object_store` multipart sink** — writing Parquet without
  a local temp file, and why `close()` is the durability point.
- **`WriterProperties`** — pinning compression (Snappy/Zstd) and confirming temporals are **MICROS**
  logical types (the one rule that makes DuckDB infer the right types, §2.1).
- **Object key composition** — building the epoch-namespaced key from `SinkMeta` fields, with the
  zero-padded `lsn_end` so keys sort in commit order.
- **DuckDB read-back as a test oracle** — round-tripping the object through `read_parquet` to assert
  types + values survived (reusing the PR 2.11 conformance harness against S3).

## Read first

- `../../architecture.md` §1.4 Arrow conversion & Parquet write (`#14-arrow-conversion--parquet-write`) —
  `ArrowWriter`/`AsyncArrowWriter`, `object_store` multipart, and the **exact file-key layout**.
- `../../architecture.md` §1.5 The durability checkpoint (`#15-the-durability-checkpoint-wal-bounding-invariant`) —
  the ordering **PUT → COMMIT manifest → standby update**; this PR is the PUT.
- `../../architecture.md` §1.8 (`#18-single-slot-for-life--total-restart`) — why the key is
  **epoch-namespaced** (`s3://<bucket>/<epoch>/…`).
- `../../walrus-pg-sink.md` §2.1 (`#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types`) —
  temporals **MICROS**, real Parquet logical types (from PR 2.11).

## Scope

**In scope**

- `ParquetSink` that encodes a `SealedBatch` and PUTs it to S3 via `object_store` multipart.
- `WriterProperties`: compression + MICROS temporals (inherited from the Arrow schema), sane row-group size.
- Object key builder: `<epoch>/<source_schema>/<table>/<lsn_end>-<batch_uuid>.parquet`
  (`lsn_end` zero-padded 16-hex via `common::Lsn`).
- Stamp `sink_processed_at` (UTC `Z`) into the batch's meta column at write time (if not already).
- Return a `WrittenObject { s3_uri, key, lsn_start, lsn_end, row_count, schema_version }` for PR 2.25.

**Explicitly deferred** (do *not* build these here)

- Manifest `ready` INSERT → **PR 2.25**; advancing `confirmed_flush_lsn` → **PR 2.26**.
- **Speculative** staging for uncommitted streamed txns (write-but-invisible) → **PR 2.30**.
- Aggregate-memory-ceiling-triggered flushes → **PR 2.32**.

## Files to create / modify

```
crates/pg-sink/Cargo.toml            # + object_store = { version = "0.11", features = ["aws"] }, uuid = { version = "1", features = ["v4"] }
                                     # + parquet (via pg-to-arrow) / arrow already present
crates/pg-sink/src/sink.rs           # new — ParquetSink, object-key builder, WrittenObject
crates/pg-sink/src/consume.rs        # modify — on seal -> ParquetSink::put
crates/pg-sink/tests/parquet_put.rs  # new — compose (MinIO): flush -> object at key -> DuckDB read-back
```

## Skeleton

```rust
// crates/pg-sink/src/sink.rs
use common::Lsn;
use object_store::{ObjectStore, path::Path};
use crate::batch::SealedBatch;

/// The result of a durable S3 PUT — everything PR 2.25 needs for the manifest row.
pub struct WrittenObject {
    pub s3_uri: String,      // s3://<bucket>/<key>
    pub key: Path,           // <epoch>/<schema>/<table>/<lsn_end>-<uuid>.parquet
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub row_count: u64,
    pub schema_version: i64,
    pub kind: FileKind,      // Stream (Snapshot in PR 2.29)
}

#[derive(Clone, Copy)]
pub enum FileKind { Stream, Snapshot }

pub struct ParquetSink { store: std::sync::Arc<dyn ObjectStore>, bucket: String, epoch: i64 }

impl ParquetSink {
    /// Encode `batch` to Parquet (MICROS, compression) and PUT to S3. Returns only once durable.
    pub async fn put(&self, batch: SealedBatch) -> Result<WrittenObject, SinkError> { todo!() }

    /// <epoch>/<schema>/<table>/<lsn_end>-<uuid>.parquet — lsn_end zero-padded so keys sort by commit.
    pub fn object_key(&self, schema: &str, table: &str, lsn_end: Lsn) -> Path { todo!() }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("parquet encode: {0}")]
    Encode(String),
    #[error(transparent)]
    Store(#[from] object_store::Error),
}
```

```rust
// crates/pg-sink/tests/parquet_put.rs
#[tokio::test]
async fn flush_writes_object_at_expected_key() { todo!() }

#[tokio::test]
async fn object_reads_back_via_duckdb_with_correct_types() { todo!() }

#[tokio::test]
async fn key_is_epoch_namespaced_and_lsn_sortable() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] A `SealedBatch` is encoded to Parquet (temporals **MICROS**, compression set) and streamed to S3
      via `object_store` **multipart** — no full local temp file.
- [ ] The object key is exactly `<epoch>/<source_schema>/<table>/<lsn_end>-<batch_uuid>.parquet` with a
      **zero-padded 16-hex** `lsn_end`, so keys sort in commit order.
- [ ] `put` returns a `WrittenObject` (with `s3_uri`, `lsn_end`, `row_count`, `schema_version`) **only
      after** the upload is durable (`close()`/commit completed) — the load-bearing PUT-before-anything.
- [ ] `walrus_pg_sink_meta.sink_processed_at` is a UTC `Z` timestamp stamped at write time.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test parquet_put`: a flush lands an
        object at the expected key that DuckDB `read_parquet` reads back with the correct types + values.

## Hints & gotchas

- `close()` on the writer (or completing the multipart) is the **durability point** — do not return
  `WrittenObject` before it, or PR 2.26 could advance the slot past a batch that isn't in S3. This
  ordering is the entire WAL-bounding invariant (§1.5).
- Use `object_store`'s multipart / streaming `put` API so a huge batch doesn't buffer wholly in memory —
  the writer is generic over `W: Write + Send`; wire it to the store's writer, don't stage to `/tmp`.
- Keep the **MICROS** guarantee honest: DuckDB infers from Parquet-native logical types, so a temporal
  written as MILLIS/NANOS silently mistypes downstream. The Arrow schema from PR 2.9 already encodes
  MICROS — assert the written file's Parquet logical type in the read-back test.
- One object per `SealedBatch`; the `batch_uuid` in the key (and in the meta `batch_id`) must match, so
  the manifest row (PR 2.25) and the object are traceable to the same UUID.
- MinIO in compose needs path-style addressing and the test bucket pre-created (PR 0.6 harness); pass
  `AmazonS3Builder` the endpoint + `with_allow_http(true)` for the local stack.

## References

- Design: `../../architecture.md` §1.4, §1.5, §1.8; `../../walrus-pg-sink.md` §2.1.
- Prev: [PR 2.23](./pr-2.23-sink-batching-cadence.md) · Next: [PR 2.25](./pr-2.25-sink-manifest-insert.md) · [Roadmap](../README.md)
