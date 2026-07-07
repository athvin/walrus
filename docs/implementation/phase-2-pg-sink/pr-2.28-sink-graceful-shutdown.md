# PR 2.28 — Drain cleanly on SIGTERM and never drop the slot

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/48

> **Phase:** 2 — walrus-pg-sink · **Crates touched:** `pg-sink` · **Est. size:** M ·
> **Depends on:** PR 2.27 · **Unlocks:** PR 2.29

On Kubernetes the pod is `SIGTERM`ed and given a bounded grace period before `SIGKILL`. This PR turns
the `CancellationToken` wired in PR 2.18 into an **ordered drain**: stop requesting WAL → flush and
commit the in-flight committed batch → send a final standby status update advancing
`confirmed_flush_lsn` (never past an open streamed txn) → `CopyDone` and close the connection → and
critically **never drop the slot**. Because the slot persists across the connection, a graceful
shutdown is just a *resume*; the drain only minimises the replay the loader would de-duplicate anyway.

## Why — learning objectives

By the end of this PR you will have practised:

- **`tokio::select!` shutdown** — a cancellation branch that *breaks the loop*, then a post-loop drain
  that is not itself cancellable.
- **The exact K8s termination sequence** — preStop-then-SIGTERM (sequential, one shared budget), why
  we skip preStop, and why the process must receive SIGTERM as PID 1 / exec form.
- **Ordering a durable shutdown** — flush-committed-only, do *not* force open speculative buffers,
  final feedback, `CopyDone`, leave the slot alone.
- **At-least-once → effectively-once** — reasoning about why an ungraceful `SIGKILL` is still correct.

## Read first

- `../../walrus-pg-sink.md#45-graceful-shutdown--the-missing-piece` — the four-step termination
  sequence, the five-step drain order, "why step 5 is the whole point", the grace-period and
  `wal_sender_timeout`-during-drain tuning notes.
- `../../architecture.md#16-large-transaction-safety` — never advance `confirmed_flush_lsn` past the
  oldest still-open streamed transaction (the drain obeys this too).
- `../../architecture.md#15-the-durability-checkpoint-wal-bounding-invariant` — the batch you flush on
  drain follows the same PUT → manifest → feedback ordering as any other.

## Scope

**In scope**

- A `drain()` method invoked once the loop's cancellation branch fires, implementing the five ordered
  steps: stop consuming, flush + commit the in-flight **committed** batch, final standby update,
  `CopyDone` + clean close, and **no** `DROP_REPLICATION_SLOT`.
- Dropping (not forcing out) any **open, uncommitted** streamed-txn speculative buffers — they have no
  `Stream Commit`/`Abort` yet, so they re-stream on resume.
- Mapping a completed drain to `ExitCode::Success` within the grace window.

**Explicitly deferred** (do *not* build these here)

- The actual open-txn floor bookkeeping and speculative staging → **PR 2.30** (this PR calls into the
  floor accessor with a single-buffer placeholder until then).
- Dockerfile PID-1 / `tini` / exec-form entrypoint → **PR 4.8**; `terminationGracePeriodSeconds` and
  the no-preStop wiring → **PR 4.9**.
- The loader's (different) drain → **PR 3.12**.

## Files to create / modify

```
crates/pg-sink/src/shutdown.rs       # new — Drain, DrainOutcome, drain ordering
crates/pg-sink/src/sink.rs           # modify — select! cancellation branch → call drain()
crates/pg-sink/src/main.rs           # modify — SIGTERM → cancel; map DrainOutcome → ExitCode
crates/pg-sink/tests/graceful_shutdown.rs   # new — compose integration test
# no new Cargo deps
```

## Skeleton

```rust
// crates/pg-sink/src/shutdown.rs
use common::Lsn;

/// Outcome of a drain attempt (the caller maps this to an ExitCode).
#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Committed batch flushed, feedback sent, connection closed — slot left in place.
    Drained { confirmed_flush: Lsn },
    /// Nothing in flight; clean close only.
    Empty,
}

impl crate::sink::SinkLoop {
    /// Ordered SIGTERM drain. Runs to completion (not cancellable); the caller bounds it by the
    /// K8s grace period. NEVER issues DROP_REPLICATION_SLOT.
    pub async fn drain(&mut self) -> Result<DrainOutcome, crate::Error> {
        // 1. stop requesting new WAL          (the select! loop has already exited)
        // 2. flush + COMMIT the in-flight COMMITTED batch (Arrow → Parquet → S3 → manifest);
        //    DROP open uncommitted speculative buffers — do NOT force them out.
        // 3. final standby status update: confirmed_flush = last durable batch's lsn_end,
        //    capped at self.open_txn_floor()  (never past an open streamed txn).
        // 4. CopyDone + close the replication connection cleanly.
        // 5. return — the slot persists; a replacement pod resumes from confirmed_flush_lsn.
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn drain_flushes_and_commits_the_inflight_committed_batch() { todo!() }
    #[test] fn drain_drops_open_speculative_buffers_without_forcing() { todo!() }
    #[test] fn drain_never_advances_confirmed_flush_past_open_txn_floor() { todo!() }
    #[test] fn drain_never_issues_drop_replication_slot() { todo!() }
}
```

```rust
// crates/pg-sink/tests/graceful_shutdown.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn sigterm_mid_stream_drains_commits_and_resumes() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] On `SIGTERM` the loop stops consuming, the in-flight **committed** batch is flushed to S3 and its
      manifest row committed, a **final** standby update advances `confirmed_flush_lsn`, `CopyDone` is
      sent, and the process exits `0` — all within the grace window.
- [x] Open, uncommitted streamed-txn buffers are **dropped, not forced out** (no orphan speculative S3
      objects), and the final `confirmed_flush_lsn` is **never** past the open-txn floor.
- [x] `DROP_REPLICATION_SLOT` is **never** issued on a normal shutdown (grep-assert in the test).
- [x] A restarted sink **resumes** from `confirmed_flush_lsn`; any re-streamed changes are de-duplicated
      downstream (documented as at-least-once → effectively-once).
- [x] Docs/comments explain the preStop-then-SIGTERM sequence and the PID-1/exec-form requirement
      (implementation of the Dockerfile deferred to PR 4.8).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test graceful_shutdown -- --ignored`
        asserting **`sigterm_mid_stream_drains_commits_and_resumes`**.

## Hints & gotchas

- The drain runs **after** the `select!` loop exits — do not put it *inside* a cancellable branch or a
  slow S3 PUT will be aborted mid-flight. Bound it externally by the grace period, not by cancellation.
- Forcing open speculative buffers out on shutdown would orphan S3 files with no `Stream Commit`/`Abort`
  to resolve them — the design explicitly says drop them and re-stream.
- A long drain that stops replying to keepalives can be severed at `wal_sender_timeout` (60s). That is
  harmless to correctness (slot persists) but can drop the *final* feedback — keep the drain < 60s or
  keep replying to keepalives during it.
- Idempotency check: killing between the S3 PUT and the standby update must leave *no loss* — the
  changes simply re-stream. Assert this in the compose test by comparing pre/post row counts.

## References

- Design: `../../walrus-pg-sink.md#45-graceful-shutdown--the-missing-piece`;
  `../../architecture.md#16-large-transaction-safety`,
  `#15-the-durability-checkpoint-wal-bounding-invariant`.
- Prev: [PR 2.27](./pr-2.27-sink-heartbeat-liveness.md) ·
  Next: [PR 2.29](./pr-2.29-sink-snapshot-backfill.md) · [Roadmap](../README.md)
