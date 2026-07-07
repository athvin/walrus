# PR 2.23 — Micro-batching + cadence flush triggers

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/43

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib),
> `pg-to-arrow` · **Est. size:** M · **Depends on:** PR 2.22 · **Unlocks:** PR 2.24

Accumulate decoded changes into **per-table Arrow builders** and decide *when* to cut a Parquet file.
A batch flushes when **any** threshold trips — `max_fill_ms` (cadence), `max_rows`, or `max_bytes` — but
**never in the middle of a committed transaction's tail**: the flush boundary respects the commit line so
a batch never contains half a transaction. This PR produces a *sealed* in-memory batch ready to be
written; the actual Parquet/S3 write is PR 2.24.

## Why — learning objectives

By the end of this PR you will have practised:

- **Threshold logic with a fake clock** — an injectable time source so the `max_fill_ms` trigger is unit
  testable in milliseconds without real sleeps.
- **Commit-boundary correctness** — buffering rows against the open txn and only making them
  flush-eligible at `Commit`, so a flush never splits a transaction (§1.6 pre-echo).
- **Arrow builders per table** — appending `TupleValue`s through `pg-to-arrow` into typed builders, then
  `finish()`ing into a `RecordBatch` at seal time.
- **Tracking `lsn_end` = commit LSN** — the batch carries the commit LSN of its last transaction, the
  load-bearing key for the manifest (PR 2.25) and checkpoint (PR 2.26).

## Read first

- `../../architecture.md` §1.3 In-memory batching & cadence (`#13-in-memory-batching--cadence`) — the
  three per-batch triggers, the `max_inflight_bytes` aggregate ceiling (deferred to 2.32), and the
  "never split a committed txn's tail" rule.
- `../../architecture.md` §1.6 Large-transaction safety (`#16-large-transaction-safety`), the
  "never advance past an open txn" bullet — the commit-boundary invariant this PR encodes for small txns.
- `../../architecture.md` §1.4 (`#14-arrow-conversion--parquet-write`) — the `walrus_pg_sink_meta` JSON
  column that each buffered row carries; `lsn_end` = commit LSN of the file's last txn.
- Prior PR 2.10 (`TupleValue` → Arrow builders → RecordBatch), whose builders this batcher drives.

## Scope

**In scope**

- A per-`(schema_version, table)` `TableBatcher` wrapping `pg-to-arrow` builders + row/byte counters.
- Buffer rows for the **open** txn; make them flush-eligible only at `Commit` (small-txn path).
- Flush decision: trip on `max_fill_ms` (via injected clock), `max_rows`, or `max_bytes` — whichever
  first — but only at a commit boundary.
- Seal → a `SealedBatch { record_batch, table, schema_version, lsn_start, lsn_end, row_count }` (commit
  LSN populated), handed to a sink hook (no-op until PR 2.24).
- Populate `walrus_pg_sink_meta` per row (`op`, `commit_lsn`, `lsn`, `xid`, `kind='stream'`, …).

**Explicitly deferred** (do *not* build these here)

- Aggregate `max_inflight_bytes` ceiling + reactive backpressure/spill → **PR 2.32**.
- Streamed large-txn demux / speculative staging → **PR 2.30**.
- Actual Parquet encode + S3 PUT → **PR 2.24**; manifest INSERT → **PR 2.25**.

## Files to create / modify

```
crates/pg-sink/src/batch.rs          # new — TableBatcher, BatchTriggers, SealedBatch, Clock trait
crates/pg-sink/src/consume.rs        # modify — route decoded I/U/D into the batcher; Commit -> maybe seal
crates/pg-sink/tests/batching.rs     # (unit) fake-clock threshold tests live inline in batch.rs; this is optional integration
```

## Skeleton

```rust
// crates/pg-sink/src/batch.rs
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use common::{Lsn, TupleValue, SinkMeta};
use crate::relcache::CachedRelation;

/// Injectable clock so max_fill_ms is testable without sleeping.
pub trait Clock: Send + Sync { fn now(&self) -> std::time::Instant; }

#[derive(Clone, Copy)]
pub struct BatchTriggers { pub max_fill: std::time::Duration, pub max_rows: u64, pub max_bytes: u64 }

/// A finished, ready-to-write batch. `lsn_end` = commit LSN of the last txn (NOT max row lsn).
pub struct SealedBatch {
    pub record_batch: RecordBatch,
    pub schema: String,
    pub table: String,
    pub schema_version: i64,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub row_count: u64,
}

pub struct TableBatcher {
    rel: Arc<CachedRelation>,
    triggers: BatchTriggers,
    // open_txn rows buffered here; promoted to the batch only at Commit
    // opened_at: Instant, rows, bytes, lsn_start, pending_commit_lsn ...
}

impl TableBatcher {
    pub fn new(rel: Arc<CachedRelation>, triggers: BatchTriggers) -> Self { todo!() }

    /// Append one change to the OPEN txn buffer (not yet flush-eligible).
    pub fn push(&mut self, meta: SinkMeta, values: &[TupleValue]) -> Result<(), BatchError> { todo!() }

    /// Mark the open txn committed at `commit_lsn`; its rows are now flush-eligible.
    pub fn on_commit(&mut self, commit_lsn: Lsn) { todo!() }

    /// True iff a trigger trips AND we're at a commit boundary. `clock` drives max_fill.
    pub fn should_flush(&self, clock: &dyn Clock) -> bool { todo!() }

    /// Finish the Arrow builders into a SealedBatch and reset. Only valid at a commit boundary.
    pub fn seal(&mut self) -> Result<SealedBatch, BatchError> { todo!() }
}

#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("cannot seal mid-transaction (would split a committed txn tail)")]
    OpenTransaction,
    #[error(transparent)]
    Arrow(#[from] pg_to_arrow::BuildError),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn flushes_on_row_count_at_commit_boundary() { todo!() }
    #[test] fn flushes_on_byte_size_at_commit_boundary() { todo!() }
    #[test] fn flushes_on_max_fill_via_fake_clock() { todo!() }
    #[test] fn never_seals_with_an_open_transaction() { todo!() }
    #[test] fn lsn_end_equals_last_commit_lsn_not_max_row_lsn() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] Rows buffer against the **open** txn and become flush-eligible only at `Commit`; `seal()` on an
      open txn returns `BatchError::OpenTransaction`.
- [x] `should_flush` trips on **each** of `max_rows`, `max_bytes`, and `max_fill` — proven by three
      unit tests, the last using a **fake clock** (no real sleep).
- [x] A sealed batch's `lsn_end` is the **commit LSN** of its last transaction, not the max row LSN
      (a dedicated test builds a batch where those differ).
- [x] Each buffered row carries a populated `walrus_pg_sink_meta` (`op`, `commit_lsn`, `lsn`, `xid`,
      `kind`, table/schema, `schema_version`).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (unit fake-clock tests; `--workspace` stays green)
  - [x] `docker compose up --wait` then a compose check: a stream of inserts forms and seals ≥ 1 batch.

## Hints & gotchas

- The commit-boundary rule is the whole point: **a batch may span many small txns, but never a fraction
  of one.** Buffer under the open txn, promote at `Commit`, and only *then* evaluate triggers.
- Inject the clock behind a trait (`Clock`) so `max_fill` is testable; production uses `Instant::now()`,
  tests use a hand-advanced fake. Don't reach for `tokio::time` pause in the unit tests — keep them sync.
- `max_bytes` should track the **buffered Arrow size estimate**, not the eventual compressed Parquet size
  — you don't know that until you write. An approximate running byte counter is fine (etl uses the same
  shape).
- Don't populate `sink_processed_at` at `push` time — that's the *write* time; stamp it when the batch is
  actually PUT (PR 2.24). Here, leave it to be filled downstream (or stamp at seal and document it).
- This is still the small/whole-txn path. A `Stream Start` (large txn) must **not** funnel into
  `TableBatcher::push` as if committed — leave a clear TODO seam for PR 2.30's per-`xid` demux.

## References

- Design: `../../architecture.md` §1.3, §1.4, §1.6 (commit-boundary bullet).
- Prev: [PR 2.22](./pr-2.22-sink-relation-cache.md) · Next: [PR 2.24](./pr-2.24-sink-parquet-s3-put.md) · [Roadmap](../README.md)
