# PR 2.30 — Stream a large transaction: per-xid demux, speculative staging, commit-gate

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/50

> **Phase:** 2 — walrus-pg-sink · **Crates touched:** `pg-sink` · **Est. size:** L ·
> **Depends on:** PR 2.29 · **Unlocks:** PR 2.31

With `streaming='on'`, a transaction larger than `logical_decoding_work_mem` arrives **before its
commit**, chopped into interleaved `Stream Start … Stream Stop` blocks that finish with `Stream Commit`
or `Stream Abort`. This PR makes the sink correct under that: **demultiplex per `xid`**, **stage
speculatively to S3** to bound memory, but **write no `ready` manifest row until `Stream Commit`**, and
**hold `confirmed_flush_lsn` at the oldest open txn's begin LSN** so a crash can always re-stream an
incomplete or aborted transaction. A whole-txn `Stream Abort` deletes the speculative files.

## Why — learning objectives

By the end of this PR you will have practised:

- **Demultiplexing interleaved streams** — a `HashMap<xid, StreamedTxn>` keyed on the top-level xid,
  reassembling non-contiguous segments via the `Stream Start` first-segment flag.
- **Speculative-then-commit-gate visibility** — freeing memory (S3 PUT) is separable from making data
  visible (the `ready` manifest row); the second waits for `Stream Commit`.
- **Slot-hold invariant** — computing an "open-txn floor" and never advancing `confirmed_flush_lsn`
  past it.
- **Abort cleanup** — deleting speculative S3 objects on `Stream Abort {sub == top}`.

## Read first

- `../../architecture.md#16-large-transaction-safety` — the four rules: demux per xid, stage
  speculatively + commit-gate, never advance the slot past an open txn, and discard aborted txns
  (whole-txn here; sub-txn in PR 2.31).
- `../../proto-version.md#8-streaming-how-a-big-transaction-is-chopped-up` — the measured 8000-row txn
  → 22 blocks, the first-segment flag (`1` then `0`), "small txns never stream".
- `../../proto-version.md#9a-whole-transaction-aborts--and-when-you-decode-changes-what-you-see` — the
  **live walsender** decodes rows *before* the rollback is known, then emits a real
  `Stream Abort {top == sub}` — so aborted rows **do** arrive and must be thrown away.
- `../../examples/proto-version/run-tests.sh` — the live-wire assertions this PR's compose test mirrors.

## Scope

**In scope**

- A `StreamDemux` of per-xid `StreamedTxn` buffers, driven by `Stream Start/Stop/Commit/Abort` and the
  per-message xid carried on every streamed change.
- Speculative S3 staging of an open txn's sub-batches (via the PR 2.24 path) **without** a manifest row.
- `on_stream_commit` → promote the speculative files to `ready` (manifest INSERTs, `lsn_end = commit
  LSN`); `on_stream_abort {sub == top}` → delete speculative S3 objects, drop the buffer.
- `open_floor()` → the oldest open txn's begin/first-segment LSN; the checkpoint clamps
  `confirmed_flush_lsn` to it.

**Explicitly deferred** (do *not* build these here)

- **Rolled-back subtransaction exclusion** (`Stream Abort {sub != top}` inside a committing txn) →
  **PR 2.31** (`push_change`/`abort_subtxn`/`survivors` land there).
- The **`max_inflight_bytes`-triggered** proactive spill + pause-poll backstop → **PR 2.32** (this PR
  stages speculatively on the per-batch caps only).
- e2e large-txn + commit-order proof → **PR 4.3**.

## Files to create / modify

```
crates/pg-sink/src/stream_txn.rs     # new — StreamedTxn, StreamDemux, StagedFile
crates/pg-sink/src/sink.rs           # modify — route stream frames to the demux; clamp checkpoint
crates/pg-sink/src/checkpoint.rs     # modify — clamp confirmed_flush at open_floor()
crates/pg-sink/src/lib.rs            # modify — `pub mod stream_txn;`
crates/pg-sink/tests/streaming_large_txn.rs  # new — compose integration test
# no new Cargo deps
```

## Skeleton

```rust
// crates/pg-sink/src/stream_txn.rs
use std::collections::HashMap;
use common::Lsn;
use crate::pgoutput::DecodedChange;   // Insert/Update/Delete/Truncate produced by the decoder

/// A speculatively-staged S3 object for an open txn — no manifest row until Stream Commit.
pub struct StagedFile { pub s3_uri: String, pub row_count: u64 }

/// Per top-level xid buffer for an in-progress streamed transaction.
pub struct StreamedTxn {
    pub top_xid: u32,
    pub begin_lsn: Lsn,           // the floor confirmed_flush must not pass
    pub staged: Vec<StagedFile>,  // speculative S3 objects
    // per-table Arrow builders for not-yet-spilled rows …
}

pub struct StreamDemux {
    open: HashMap<u32, StreamedTxn>,   // keyed by top-level xid
}

impl StreamDemux {
    pub fn on_stream_start(&mut self, top_xid: u32, first_segment: bool, lsn: Lsn) { todo!() }
    pub fn on_change(&mut self, top_xid: u32, change: DecodedChange) { todo!() }
    pub fn on_stream_stop(&mut self) { todo!() }

    /// Promote this xid's speculative files to `ready` (manifest INSERTs, lsn_end = commit_lsn).
    pub async fn on_stream_commit(&mut self, top_xid: u32, commit_lsn: Lsn) -> Result<(), crate::Error> { todo!() }

    /// Whole-txn abort (sub == top): delete speculative S3 files, drop the buffer.
    pub async fn on_stream_abort(&mut self, top_xid: u32, sub_xid: u32) -> Result<(), crate::Error> { todo!() }

    /// Oldest open txn's begin LSN — confirmed_flush must never pass this.
    pub fn open_floor(&self) -> Option<Lsn> { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn demux_routes_interleaved_xids_to_their_buffers() { todo!() }
    #[test] fn stream_commit_promotes_speculative_files_to_ready() { todo!() }
    #[test] fn whole_txn_stream_abort_deletes_speculative_and_writes_no_ready() { todo!() }
    #[test] fn open_floor_is_oldest_open_txn_begin_lsn() { todo!() }
}
```

```rust
// crates/pg-sink/tests/streaming_large_txn.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn large_txn_single_ready_file_only_after_stream_commit() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] Under `logical_decoding_work_mem = 64kB`, an 8000-row committed transaction produces its
      `ready` file(s) **only after `Stream Commit`** — never a `ready` row while the txn is open.
- [x] `confirmed_flush_lsn` is **held** at the open txn's begin/first-segment LSN for the whole open
      window, then advances on commit.
- [x] A whole-txn `Stream Abort {sub == top}` **deletes** the speculative S3 objects and writes **no**
      `ready` row.
- [x] Interleaved blocks for multiple in-progress xids are reassembled correctly via the first-segment
      flag (unit test with two xids).
- [x] Docs/comments explain "freeing memory (PUT) ≠ advancing the slot", tied to the §1.6 rules.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test streaming_large_txn -- --ignored`
        asserting **`large_txn_single_ready_file_only_after_stream_commit`**.

## Hints & gotchas

- The per-message xid on each streamed change is the **sub-transaction** xid; `Stream Start` carries
  the **top-level** xid — key the demux buffer on the top-level, but *keep* the per-message xid on each
  row (PR 2.31 needs it). Don't collapse them now.
- A live walsender **does** stream rows for a txn that ends up aborting (proto §9a) — do not assume "no
  rows means aborted". The `Stream Abort` frame is your only discard signal.
- The open-txn floor must include **every** open xid, not just the one you're currently buffering;
  otherwise an interleaved older txn's WAL could be freed prematurely.
- Speculative files still land at the epoch-namespaced key layout — just don't insert a manifest row.
  On abort, the S3 delete is best-effort but must not leave a `ready` row pointing at a gone object.

## References

- Design: `../../architecture.md#16-large-transaction-safety`;
  `../../proto-version.md#8-streaming-how-a-big-transaction-is-chopped-up`,
  `#9a-whole-transaction-aborts--and-when-you-decode-changes-what-you-see`;
  `../../examples/proto-version/run-tests.sh`.
- Prev: [PR 2.29](./pr-2.29-sink-snapshot-backfill.md) ·
  Next: [PR 2.31](./pr-2.31-sink-subtransaction-exclusion.md) · [Roadmap](../README.md)
