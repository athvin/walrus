# PR 2.31 — Exclude rolled-back subtransaction rows (the flagship correctness test)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/51

> **Phase:** 2 — walrus-pg-sink · **Crates touched:** `pg-sink` · **Est. size:** L ·
> **Depends on:** PR 2.30 · **Unlocks:** PR 2.32

This is the subtle correctness case that silently corrupts a mirror if mishandled. Under `streaming
'on'`, a **savepoint that later rolls back** is *still streamed* inside an otherwise-committing
top-level transaction; only a `Stream Abort {sub != top}` tells you those rows died — while
`Stream Commit {top}` still makes the survivors visible. Because we stage rows to S3 **before** the
commit, the sink must track sub-transaction structure and **exclude exactly the aborted sub-xid's rows
when materialising the `ready` file**. The proof: 3000 kept-A + 3000 rolled-back + 3000 kept-B must
yield a `ready` file with **exactly 6000** rows — never the rolled-back ones — and none may reach raw.

## Why — learning objectives

By the end of this PR you will have practised:

- **Sub-transaction bookkeeping** — tagging every buffered/staged row with its per-message sub-xid, and
  the top-vs-sub distinction that makes savepoint rollback tractable.
- **Selective materialisation** — filtering *exactly one* sub-xid's rows out of a committing txn, not
  the coarse whole-txn drop from PR 2.30.
- **Reconciling with already-spilled files** — an aborted sub-xid may already have speculative S3
  objects; materialisation must not publish them.
- **Turning a proven wire capture into a Rust assertion** — reusing the proto-version §9b golden case.

## Read first

- `../../proto-version.md#9b-a-rolled-back-savepoint-inside-a-committed-transaction-the-dangerous-one`
  — the exact scenario: `INSERT xid=857:3000` (keep), `xid=858:2762` (discard),
  `xid=859:3000` (keep); `STREAM ABORT top=857 sub=858`; `STREAM COMMIT 857` → 6000 survivors.
- `../../architecture.md#16-large-transaction-safety` — "discard rolled-back *sub*transactions, not
  just aborted top-level txns … never let a rolled-back subtransaction's rows reach `<table>_raw`".
- `../../proto-version.md#7-the-per-message-xid-v2-why-it-exists` — the per-message xid is the
  **sub**-xid; `Stream Start` carries the **top** xid.
- `../../examples/proto-version/run-tests.sh` — the live-wire subtxn-abort assertion this mirrors.

## Scope

**In scope**

- Extend `StreamedTxn` (PR 2.30) to tag each change with its **sub-xid** and to drop exactly that
  sub-xid's rows on `Stream Abort {sub != top}` while the top-level continues toward commit.
- `survivors()` materialisation on `Stream Commit` excluding aborted sub-xids (and never publishing a
  speculative file that belongs solely to an aborted sub-xid).
- Guarantee aborted sub-xid rows never appear in any `ready` file (and thus never reach `<table>_raw`).

**Explicitly deferred** (do *not* build these here)

- Whole-txn `Stream Abort {sub == top}` cleanup → already landed in **PR 2.30**; this PR is the
  `sub != top` case only.
- `max_inflight_bytes` proactive spill (which complicates aborting *already-spilled* sub-xid files) →
  **PR 2.32**; keep the abort reconciliation in-memory here and note the interaction.
- e2e savepoint-rollback proof end-to-end into DuckDB → **PR 4.3**.

## Files to create / modify

```
crates/pg-sink/src/stream_txn.rs     # modify — sub-xid tagging, abort_subtxn, survivors()
crates/pg-sink/src/sink.rs           # modify — pass per-message sub-xid into the demux
crates/pg-sink/tests/subtransaction_exclusion.rs  # new — the flagship compose test
# no new Cargo deps
```

## Skeleton

```rust
// crates/pg-sink/src/stream_txn.rs  (extends PR 2.30)
use crate::pgoutput::DecodedChange;

impl super::StreamedTxn {
    /// Record a streamed change tagged with ITS sub-transaction xid (may equal top_xid).
    pub fn push_change(&mut self, sub_xid: u32, change: DecodedChange) { todo!() }

    /// Stream Abort {top, sub} with sub != top: drop exactly the rows tagged `sub_xid`.
    /// The top-level transaction continues toward Stream Commit.
    pub fn abort_subtxn(&mut self, sub_xid: u32) { todo!() }

    /// Rows to write into the `ready` file on Stream Commit: all buffered/staged changes
    /// EXCEPT those tagged with an aborted sub-xid, in commit order.
    pub fn survivors(&self) -> impl Iterator<Item = &DecodedChange> { todo!() }
}

#[cfg(test)]
mod subtxn_tests {
    use super::*;
    /// proto-version §9b: 3000 kept-A + 2762 rolled-back(858) + 3000 kept-B → 6000 survivors.
    #[test] fn subtxn_abort_excludes_only_the_aborted_subxid() { todo!() }
    #[test] fn survivors_are_emitted_in_commit_order() { todo!() }
    #[test] fn aborted_subxid_rows_never_reach_a_ready_file() { todo!() }
    #[test] fn nested_then_new_subxid_after_rollback_is_kept() { todo!() }  // 859 after 858 aborts
}
```

```rust
// crates/pg-sink/tests/subtransaction_exclusion.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn savepoint_rollback_ready_file_has_exactly_6000_rows() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] The proto-version §9b scenario (kept-A / rolled-back / kept-B) produces a `ready` file with
      **exactly 6000** rows — the 2762 rolled-back-savepoint rows are **never** present.
- [x] The top-level transaction still **commits** (survivors become visible) even though one of its
      sub-xids aborted.
- [x] Aborted sub-xid rows never appear in any `ready` file and therefore **never reach `<table>_raw`**.
- [x] A new sub-xid opened *after* the rollback (kept-B, xid 859) is **kept**.
- [x] Survivors preserve commit order (kept-A before kept-B).
- [x] Docs/comments explain top-vs-sub xid and why "no `Stream Abort` for a whole txn decoded by the
      SQL functions" does not apply to a live walsender.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then
        `cargo test -p pg-sink --test subtransaction_exclusion -- --ignored` asserting
        **`savepoint_rollback_ready_file_has_exactly_6000_rows`**.

## Hints & gotchas

- The abort names the **sub-xid**; your per-row tag must be the sub-xid the decoder attached, not the
  block's top-level xid — mixing them up silently keeps or drops the wrong 2762 rows.
- The rolled-back savepoint may be only **partially streamed** (2762 of an intended 3000) — you exclude
  whatever arrived tagged with that sub-xid, not a fixed count.
- If PR 2.32's proactive spill has *already* pushed an aborted sub-xid's rows to a speculative S3 file,
  you must not publish that file — for now keep the buffer in memory until commit and leave a `// TODO
  (2.32): reconcile already-spilled aborted sub-xid files` marker.
- This is the flagship: assert **exactly 6000**, not "≥ 6000" — an off-by-one that lets rolled-back
  rows through is precisely the silent corruption this test exists to catch.

## References

- Design:
  `../../proto-version.md#9b-a-rolled-back-savepoint-inside-a-committed-transaction-the-dangerous-one`,
  `#7-the-per-message-xid-v2-why-it-exists`;
  `../../architecture.md#16-large-transaction-safety`;
  `../../examples/proto-version/run-tests.sh`.
- Prev: [PR 2.30](./pr-2.30-sink-streaming-large-txn.md) ·
  Next: [PR 2.32](./pr-2.32-sink-max-inflight-bytes.md) · [Roadmap](../README.md)
