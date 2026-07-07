# PR 3.10 — Snapshot/stream boundary through the transform

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` · **Est. size:** M ·
> **Depends on:** PR 3.9 · **Unlocks:** PR 3.11

The loader has no special "snapshot mode": `kind='snapshot'` files are appended into `<table>_raw` like
any other `ready` file, and the **transform collapses the overlap by `(commit_lsn, lsn)`**. A streamed
change whose `commit_lsn > consistent_point` naturally beats the snapshot row (which carries `commit_lsn
= consistent_point`), so the mirror ends at the stream value with zero loss and zero dupes. Two edges
this PR proves through the transform: (1) snapshot-then-overlapping-stream resolves to the stream value,
and (2) **equal-`lsn_end` snapshot files split across multiple loader batches are all applied** — none
skipped by the watermark (the exact case the PR 3.7 guard's break-face-A closes).

## Why — learning objectives

By the end of this PR you will have practised:

- **One code path for snapshot + stream** — no bespoke backfill mode; snapshot rows are just more raw
  rows the window ranks.
- **The `consistent_point` boundary** — why snapshot rows share `commit_lsn = consistent_point` and how
  a later stream change out-ranks them by tuple.
- **Equal-`lsn_end` claim semantics** — proving the `(lsn_end, id)` claim + queue-delete frontier
  applies *all* split-batch snapshot files, not just the first.
- **Closing the straddle end-to-end** — validating PR 3.7's guard against the real snapshot data flow.

## Read first

- `../../architecture.md#17-snapshot--backfill-bootstrap` — exported-snapshot backfill; snapshot files'
  `lsn_end = consistent_point`; the snapshot/stream dedup boundary.
- `../../walrus-loader.md#7-straddling-the-watermark--the-per-pk-max-applied-commit-lsn-guard` — break
  face A (equal-`commit_lsn` snapshot straddle) and the `>=`-bound + guard resolution from PR 3.7.
- `../../walrus-loader.md#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue` — why the
  claim is `ORDER BY lsn_end, id` and **not** `lsn_end > raw_appended_lsn` (equal-`lsn_end` snapshot
  files).
- `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` — the
  "Exported-snapshot backfill boundary" bullet.

## Scope

**In scope**

- Confirm/verify Phase A appends `kind='snapshot'` files with `commit_lsn = consistent_point` promoted
  correctly (they flow through PR 3.2's verbatim append unchanged).
- Verify the transform resolves snapshot vs overlapping stream to the **stream** value by `(commit_lsn,
  lsn)`.
- Verify equal-`lsn_end` snapshot files split across batches are **all** applied (claim + queue-delete +
  the PR 3.7 `>=` bound + guard) — none skipped.
- A compose test seeding a snapshot manifest + overlapping stream manifest and asserting the mirror.

**Explicitly deferred** (do *not* build these here)

- The sink's exported-snapshot production (`SNAPSHOT 'export'`, `COPY`) → that's sink **PR 2.29**;
  here you seed snapshot Parquet + manifest directly.
- Total-restart / epoch re-snapshot rebuild → **PR 4.6**.
- Full-rebuild baseline union → **PR 3.11**.

## Files to create / modify

```
crates/loader/src/phase_a.rs       # modify (if needed) — ensure snapshot files claim/append identically
crates/loader/tests/snapshot_boundary.rs  # new — compose: snapshot + overlapping stream → mirror = stream
crates/loader/tests/transform.rs   # modify — hermetic equal-consistent_point straddle cases
```

## Skeleton

```rust
// crates/loader/tests/transform.rs  (hermetic, in-memory)
/// snapshot row (commit_lsn=consistent_point) + a later stream change on the same PK → stream wins.
#[test] fn overlapping_stream_change_outranks_the_snapshot_row() { todo!() }
/// a no-stream key's snapshot row with commit_lsn == transformed_lsn is still applied (break face A).
#[test] fn no_stream_key_snapshot_row_at_boundary_is_applied() { todo!() }
```

```rust
// crates/loader/tests/snapshot_boundary.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn snapshot_then_overlapping_stream_yields_stream_value() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn equal_lsn_end_snapshot_files_split_across_batches_all_applied() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] A snapshot load followed by an **overlapping** stream change on the same PK → the mirror ends at
      the **stream** value (`commit_lsn > consistent_point` out-ranks the snapshot row by tuple).
- [ ] **Equal-`lsn_end`** snapshot files split across multiple loader batches are **all** applied —
      none skipped by the watermark (the claim uses `(lsn_end, id)` + queue-delete, and the PR 3.7 `>=`
      bound + guard handle the boundary key).
- [ ] A no-stream key whose snapshot row carries `commit_lsn == transformed_lsn` still lands in the
      mirror (break face A stays closed).
- [ ] Snapshot files flow through the **same** Phase A / Phase B path as stream files — no special mode.
- [ ] Zero loss and zero dupes across the snapshot/stream boundary.
- [ ] Docs/comments explain why `lsn_end`-only watermark filtering would drop equal-`lsn_end` snapshot
      files.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p loader --test snapshot_boundary -- --ignored`
        asserting **`snapshot_then_overlapping_stream_yields_stream_value`** and
        **`equal_lsn_end_snapshot_files_split_across_batches_all_applied`**.

## Hints & gotchas

- Snapshot rows all share `consistent_point` as **both** `lsn_end` (the manifest) and `commit_lsn` (the
  promoted column). The `id` tiebreaker in the claim, plus the queue-delete frontier, is what applies
  every one of a large snapshot's many files — the watermark alone can't distinguish them.
- This PR is largely a **verification** PR: if PRs 3.2/3.3/3.7 are correct, snapshot flows for free. The
  value is proving the split-batch and equal-boundary cases explicitly — they're the ones that silently
  drop a key if the `>=` bound or `id` tiebreak is wrong.
- Give the stream change a strictly greater `commit_lsn` than `consistent_point` in the overlap test so
  the tuple ordering is unambiguous — a same-`commit_lsn` collision is the *straddle* case, tested
  separately.
- Seed snapshot Parquet with `kind='snapshot'` and the right promoted `op` (`'i'` for a COPY-style row)
  so the transform's `NOT MATCHED → INSERT` branch fires for no-stream keys.
- Don't advance `transformed_lsn` past `consistent_point` until all snapshot files at that `lsn_end` are
  appended — in v1 Phase A drains the claim before Phase B, and the PR 3.7 guard backstops the rest.

## References

- Design: `../../architecture.md#17-snapshot--backfill-bootstrap`,
  `#verification-how-well-prove-it-works-end-to-end-later`;
  `../../walrus-loader.md#7-straddling-the-watermark--the-per-pk-max-applied-commit-lsn-guard`,
  `#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue`.
- Prev: [PR 3.9](./pr-3.9-loader-ddl-destructive.md) ·
  Next: [PR 3.11](./pr-3.11-loader-full-rebuild-compaction.md) · [Roadmap](../README.md)
