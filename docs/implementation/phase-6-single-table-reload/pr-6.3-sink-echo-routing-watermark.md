# PR 6.3 — echo routing: consume `reload_signal`, resolve the watermark `L_i`

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/95

> **Phase:** 6 — single-table reload · **Crates touched:** `pg-sink` ·
> **Est. size:** M · **Depends on:** PR 6.2 · **Unlocks:** PR 6.4

This PR implements **echo-wait**, the chosen watermark mechanism
([reload H1](../../single-table-reload.md#h1--the-export-has-no-consistent-point-the-critical-one)):
the exporter INSERTs a signal row, and when the sink sees its *own* insert come back through the
replication stream, the decoded **commit LSN** of that transaction is the chunk's low watermark
`L_i`. The consume path learns to recognise `walrus.reload_signal` as internal (the `ddl_audit`
precedent) — consumed for control, **never** batched, never written to Parquet — and to resolve a
waiter registry that the export engine (PR 6.5) will block on. The embedded `wal_insert_lsn` from
PR 6.2 is asserted as `embedded < commit LSN` on every echo: a free cross-check and the diagnostic
if the echo path ever misbehaves. This PR also retires a design open question by writing the dated
note on the commit-visibility race.

## Why — learning objectives

By the end of this PR you will have practised:

- **The low-watermark stamping rule and its direction** — why `L_i` must be captured *before* the
  chunk's MVCC visibility point, and how stamping late silently loses updates (H1's algebra).
- **Message-time vs commit-time LSNs** — the signal's `Insert` message arrives first, but the
  watermark is the transaction's **commit** LSN; you buffer at Insert and resolve at Commit.
- **Cross-task handoff with oneshot channels** — a waiter registry keyed by
  `(reload_id, chunk_no)`, subscribed before the INSERT so the echo can never be missed.
- **Writing a decision note** — bounding a race you can't eliminate, and recording why the bound
  is acceptable (the `notes/` pattern from PR 5.9).

## Read first

- `../../single-table-reload.md` — H1 in full (echo-wait vs embedded read-back, and why echo-wait
  won), §6 first bullet (the commit-visibility race this PR's note must take a position on).
- `crates/pg-sink/src/relcache.rs` — `is_internal_table()` (~line 28), the two-table match this PR
  extends.
- `crates/pg-sink/src/consume.rs` — the internal-table routing (~lines 109–139): where `ddl_audit`
  inserts peel off before batching; `reload_signal` takes the same exit.
- `docs/implementation/notes/duckdb-lts-bump.md` — the dated-finding note format.

## Scope

**In scope**

- `is_internal_table()` learns `reload_signal`; the consume loop routes its decoded inserts to a
  new `WatermarkWaiters` registry instead of a `TableBatcher` — buffered at `Insert`, resolved at
  `Commit` with the transaction's commit LSN. Non-insert ops on the table (future pruning DELETEs)
  are ignored-but-acked.
- The cross-check: on every resolve, assert `embedded wal_insert_lsn < commit LSN`; a violation
  increments a counter metric and logs at error level (it means the race model is wrong — loud,
  not fatal).
- `docs/implementation/notes/commit-visibility-race.md`: the assumption ("a chunk SELECT issued
  after observing `L_i` in-stream sees every transaction with commit LSN ≤ `L_i`"), why the
  decode→ship→receive round-trip practically closes it, what `synchronous_commit=off` does to the
  window, and the chosen bound (accept as-is, or one extra echo round-trip — decide and say why).

**Explicitly deferred** (do *not* build these here)

- Anyone actually *writing* signal rows or awaiting the receiver → **PR 6.5**. This PR's compose
  test inserts manually via psql.
- Echo **timeout** policy (how long the exporter waits before declaring the publication broken) →
  **PR 6.5**, where the wait has an owner.
- Read-only watermarking via `pg_current_snapshot()` (Debezium ≥ 2.4) — note it in the race note
  as the future alternative; do not build.

## Files to create / modify

```
crates/pg-sink/src/relcache.rs                          # modify — is_internal_table + reload_signal
crates/pg-sink/src/reload_signal.rs                     # new — Echo, WatermarkWaiters
crates/pg-sink/src/consume.rs                           # modify — route signal inserts; resolve at Commit
crates/pg-sink/src/lib.rs                               # modify — pub mod reload_signal;
crates/common/src/metrics.rs                            # modify — crosscheck-violation counter
docs/implementation/notes/commit-visibility-race.md     # new — the dated decision note
crates/pg-sink/tests/reload_echo.rs                     # new — compose: internal routing end-to-end
```

## Skeleton

```rust
// crates/pg-sink/src/reload_signal.rs

use common::Lsn;
use tokio::sync::oneshot;

/// One resolved echo: the authoritative stamp and its cross-check.
#[derive(Debug, Clone, Copy)]
pub struct Echo {
    /// The signal transaction's decoded COMMIT LSN — this IS the chunk's low watermark L_i (H1).
    pub commit_lsn: Lsn,
    /// The row's embedded wal_insert_lsn — strictly earlier than commit_lsn, or the model is wrong.
    pub embedded_lsn: Lsn,
}

/// Registry of in-flight watermark waits. Exporters subscribe BEFORE inserting the signal row
/// (subscribe-then-insert — the echo can arrive fast; the reverse order can miss it).
#[derive(Default)]
pub struct WatermarkWaiters { /* Mutex<HashMap<(i64, i64), oneshot::Sender<Echo>>> */ }

impl WatermarkWaiters {
    pub fn subscribe(&self, reload_id: i64, chunk_no: i64) -> oneshot::Receiver<Echo> { todo!() }

    /// Called from the consume path at the COMMIT of a transaction that carried a reload_signal
    /// insert. Runs the cross-check (embedded < commit; violation → metric + error log). An
    /// unsubscribed echo (e.g. resolved after an exporter crash) is dropped with a debug log.
    pub fn resolve(&self, reload_id: i64, chunk_no: i64, echo: Echo) { todo!() }
}

/// Decoded reload_signal insert held between its Insert message and its Commit.
pub struct PendingSignal { /* reload_id, chunk_no, embedded_lsn */ }

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn subscribe_then_resolve_delivers_commit_lsn() { todo!() }
    #[test] fn crosscheck_violation_counts_and_still_resolves() { todo!() }
    #[test] fn resolve_without_subscriber_is_a_quiet_noop() { todo!() }
    #[test] fn non_insert_ops_on_signal_table_are_ignored() { todo!() }
}
```

```rust
// crates/pg-sink/tests/reload_echo.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn signal_insert_resolves_waiter_and_never_reaches_parquet() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `is_internal_table("walrus", "reload_signal")` is true; a decoded signal insert never reaches
      a `TableBatcher`, a Parquet file, or a manifest row (compose-asserted).
- [x] A subscribed waiter resolves with the signal transaction's **commit** LSN, not the Insert
      message's LSN — proven by a unit test with synthetic Insert + Commit frames.
- [x] Every resolve asserts `embedded_lsn < commit_lsn`; a synthetic violation increments the
      counter and error-logs without panicking the consume loop.
- [x] Signals inside the consume path are acked like any consumed record — the slot's
      `confirmed_flush` advances past them normally (no special retention).
- [x] `notes/commit-visibility-race.md` exists, is dated, states the assumption, the chosen bound,
      and the revisit trigger; the read-only `pg_current_snapshot()` alternative is named.
- [x] Docs/comments explain subscribe-then-insert and buffer-at-Insert/resolve-at-Commit.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test reload_echo -- --ignored`
        asserting **`signal_insert_resolves_waiter_and_never_reaches_parquet`**.

## What completed looks like

```
$ docker compose up --wait
$ psql $SOURCE_URL -c "INSERT INTO walrus.reload_signal (reload_id, chunk_no) VALUES (42, 1)"
# sink log:
INFO  reload_signal echo  reload_id=42 chunk_no=1 commit_lsn=0/1A2C310 embedded_lsn=0/1A2C2D8
# and no new object for walrus.reload_signal appears in MinIO, no file_manifest row:
$ psql $CONTROL_URL -c "SELECT count(*) FROM walrus.file_manifest WHERE source_table='reload_signal'"
 count
-------
     0
```

## Hints & gotchas

- The commit LSN belongs to the `Commit` message, which arrives *after* the `Insert` — hold a
  `PendingSignal` on the in-flight transaction and resolve when it commits. The sink's own signal
  transactions are tiny single-row commits and will never hit the streaming (`Stream*`) path, but
  handle the streamed case defensively anyway (resolve at `Stream Commit`) — a comment explaining
  why it "can't happen" plus code that survives it anyway is the house style.
- Subscribe-then-insert matters: the echo round-trip can be faster than your next `await`. The
  registry must already hold the sender when the INSERT commits.
- A subtransaction-aborted signal insert (PR 2.31 machinery) must NOT resolve the waiter — the
  commit never carried it. State this in a test name even if the test is trivial.
- Don't leak senders: an exporter that gives up (timeout, PR 6.5) drops its receiver; `resolve` on
  a closed channel is fine, but stale entries need eviction — key removal on resolve + a
  drop-guard or sweep.
- Keep `WatermarkWaiters` in the sink's shared state next to the existing `InternalTables` wiring
  so PR 6.5 can hand it to exporter tasks without re-plumbing.

## References

- Design: `../../single-table-reload.md` H1, §6 (race), references (DBLog paper, Debezium
  read-only-snapshots blog); `../../walrus-pg-sink.md` §3.5 (internal-table consume precedent).
- Prev: [PR 6.2](./pr-6.2-source-reload-signal-table.md) ·
  Next: [PR 6.4](./pr-6.4-sink-reload-controller.md) · [Roadmap](../README.md)
