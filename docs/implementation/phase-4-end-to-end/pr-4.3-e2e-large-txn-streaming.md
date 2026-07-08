# PR 4.3 — End-to-end large-txn streaming: bounded memory, commit-order, subtxn-abort

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/69 (surfaced + fixed two streaming
> commit-order bugs in #68: spilled-file `commit_lsn` placeholder + out-of-commit-order `ready` rows)

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `tests/e2e` · **Est. size:** L ·
> **Depends on:** PR 4.2 · **Unlocks:** PR 4.4

Joins the three hardest streaming-correctness stories into one end-to-end suite, each proving a distinct
`architecture.md` verification bullet: (1) **large-txn** — a multi-million-row transaction with
`streaming='on'` stays bounded (the `max_inflight_bytes` ceiling fires, open-txn buffers spill to S3) and
appears **atomically after commit**, while an *aborted* large txn leaks **nothing** to DuckDB; (2)
**commit-order** — a large txn that commits *after* a smaller, later-started txn (overlapping PK) is **not
skipped** and the mirror reflects true last-writer-by-commit-LSN; (3) **subtxn-abort** — a committed
top-level txn with a rolled-back savepoint materialises only the surviving rows in `<table>_raw`.

## Why — learning objectives

By the end of this PR you will have practised:

- **Proving bounded resource use under load** — asserting in-flight memory + slot size stay under the cap
  while a huge txn streams, rather than trusting the batching code in isolation.
- **The commit-LSN ordering invariant, end-to-end** — the regression guard for the row-LSN bug: files
  apply in `(lsn_end, id)` commit order and no transaction is silently dropped.
- **Speculative-staging semantics** — an aborted top-level txn deletes its speculative S3 files and never
  produces a `ready` manifest row; a rolled-back subtransaction is excluded from an otherwise-committed txn.
- **Chaos-adjacent e2e** — overlapping transactions, savepoints, and mid-txn aborts driven from SQL.

## Read first

- `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` — the **"Large txn"**,
  **"Commit-order under streaming"**, and **"Streamed sub-transaction abort"** bullets; each is one test
  here.
- `../../architecture.md#16-large-transaction-safety` — speculative staging, commit-gating, and holding
  `confirmed_flush_lsn` at the oldest open txn's begin LSN.
- `../../architecture.md#13-in-memory-batching--cadence` — the aggregate `max_inflight_bytes` ceiling and
  the open-txn spill.
- `../examples/proto-version/run-tests.sh` §9b + the `stream_abort_*` vectors — the subtxn-abort shape
  already unit-proven in PR 2.31 (the e2e mirror of it).

## Scope

**In scope**

- `large_txn_is_atomic_and_bounded`: one txn inserting N rows (N large enough to exceed
  `logical_decoding_work_mem=64kB` and the configured `max_inflight_bytes`); assert the spill-count /
  in-flight-bytes metrics rise **but stay bounded**, the txn appears **all-or-nothing after commit**, and
  slot size does not grow without bound.
- `aborted_large_txn_leaves_nothing`: same shape but `ROLLBACK`; assert no `ready` manifest row, no
  surviving Parquet, and **zero** rows in `<table>_raw`/mirror.
- `late_committing_large_txn_not_skipped`: txn A (large, starts first, commits **last**) and txn B (small,
  starts later, commits first), both touching one shared PK; assert final mirror == A's value and files
  applied in commit order.
- `rolled_back_savepoint_never_materializes`: 3000 kept-A + 3000 rolled-back-savepoint + 3000 kept-B in
  one committed txn; assert `<table>_raw` has **exactly 6000** rows, none from the rolled-back subxact.

**Explicitly deferred** (do *not* build these here)

- Crash-during-large-txn recovery → **PR 4.4**. WAL-runaway pause/catch-up → **PR 4.5**.
- Tuning the exact ceiling value (Open Q14) — this asserts *bounded*, not *optimal*.

## Files to create / modify

```
tests/e2e/tests/large_txn.rs         # new — atomic+bounded, aborted, late-commit-order
tests/e2e/tests/subtxn_abort.rs      # new — rolled-back savepoint exclusion (6000 exactly)
tests/e2e/src/lib.rs                 # modify — add metric_scrape() + slot_retained_bytes() helpers
# no new deps
```

## Skeleton

```rust
// tests/e2e/tests/large_txn.rs
#![cfg(feature = "it")]
use e2e::Harness;

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn large_txn_is_atomic_and_bounded() {
    // BEGIN; INSERT N rows into orders; COMMIT.
    // While streaming: assert in_flight_bytes stays <= ceiling and spill_count increments.
    // After commit: assert all N rows present atomically; slot retained bytes bounded.
    todo!()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn aborted_large_txn_leaves_nothing() { todo!() }

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn late_committing_large_txn_not_skipped() {
    // A: BEGIN, insert PK=1 val='A' (large, stays open).
    // B: BEGIN later, insert/update PK=1 val='B', COMMIT first.
    // A: COMMIT last. Assert mirror.PK1 == 'A' (A's commit_lsn is greater) and files apply in (lsn_end,id) order.
    todo!()
}
```

```rust
// tests/e2e/tests/subtxn_abort.rs
#![cfg(feature = "it")]
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn rolled_back_savepoint_never_materializes() {
    // BEGIN; insert 3000 (A); SAVEPOINT s; insert 3000 (rolled back); ROLLBACK TO s; insert 3000 (B); COMMIT.
    // Assert COUNT(*) in orders_raw for this txn == 6000, none of the rolled-back rows present.
    todo!()
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] The large committed txn appears **atomically after commit**; during streaming the `in_flight_bytes`
      metric stays ≤ the ceiling and `spill_count` increments; slot retained bytes stay bounded.
- [x] The aborted large txn produces **no** `ready` row, no surviving Parquet, and **zero** rows in
      `<table>_raw` / mirror.
- [x] The late-committing large txn is **not skipped**: files apply in `(lsn_end, id)` commit order and
      the mirror reflects last-writer-by-commit-LSN (the row-LSN-bug regression guard).
- [x] The committed txn with a rolled-back savepoint yields **exactly 6000** rows in `<table>_raw`, none
      from the rolled-back subtransaction.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test --workspace`
  - [x] `docker compose up --wait` then `cargo test -p e2e --features it -- --ignored` asserting
        **`large_txn_is_atomic_and_bounded`**, **`aborted_large_txn_leaves_nothing`**,
        **`late_committing_large_txn_not_skipped`**, and **`rolled_back_savepoint_never_materializes`**.

## Hints & gotchas

- The compose Postgres must run with `logical_decoding_work_mem=64kB` (from the PR 0.6 harness) so a few
  thousand rows actually trigger `streaming='on'` mid-txn — otherwise the txn buffers server-side and you
  never exercise the streaming path.
- "Bounded" is asserted against the **metric**, not a hope: scrape `in_flight_bytes` at the peak and check
  it against the configured ceiling, and read `pg_replication_slots` retained bytes for the slot.
- The commit-order test is the whole reason ordering keys on **commit LSN**, never max-row-LSN. To make it
  bite, ensure A's individual row LSNs are *lower* than B's but A's **commit** LSN is *higher*.
- The 6000-exact count is unforgiving on purpose: an off-by-one in subxact tracking shows up as 6001/8999.
  Assert the exact count, not "roughly 6000".

## References

- Design: `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` ("Large txn",
  "Commit-order under streaming", "Streamed sub-transaction abort");
  `../../architecture.md#16-large-transaction-safety`, `#13-in-memory-batching--cadence`;
  `../examples/proto-version/run-tests.sh`.
- Prev: [PR 4.2](./pr-4.2-e2e-type-matrix.md) · Next: [PR 4.4](./pr-4.4-e2e-crash-safety.md) ·
  [Roadmap](../README.md)
