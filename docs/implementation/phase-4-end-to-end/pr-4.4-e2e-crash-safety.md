# PR 4.4 — End-to-end crash safety: effectively-once via checkpoint replay

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `tests/e2e` · **Est. size:** M ·
> **Depends on:** PR 4.3 · **Unlocks:** PR 4.5

Proves the `architecture.md` **"Crash safety"** verification bullet: **`SIGKILL` the sink mid-batch and
the loader mid-MERGE**, then restart both and assert the pipeline reaches the same state it would have
without the crash — **effectively-once, no loss, no regressions**. This is the payoff of every durability
rule built earlier: the sink advances `confirmed_flush_lsn` only after S3 + manifest are durable, and the
loader's raw `APPEND` (`ON CONFLICT DO NOTHING`) + `MERGE` de-duplicate the at-least-once replay into an
effectively-once result.

## Why — learning objectives

By the end of this PR you will have practised:

- **Injecting crashes deterministically** — killing a process at a chosen point (mid-batch / mid-MERGE)
  rather than at a random wall-clock time, so the failure window is the one you mean to test.
- **Reasoning about at-least-once → effectively-once** — the slot resumes at its last checkpoint, re-streams
  a few changes, and the append dedup + MERGE collapse them; the end state is identical.
- **Watermark-driven convergence after chaos** — asserting both watermarks (`raw_appended_lsn`,
  `transformed_lsn`) recover and the mirror equals the source once the replacements catch up.

## Read first

- `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` — the **"Crash safety"**
  bullet: kill sink mid-batch + loader mid-MERGE → effectively-once via checkpoint replay.
- `../../architecture.md#15-the-durability-checkpoint-wal-bounding-invariant` — why a crash between S3 PUT
  and the standby update loses nothing (the slot has not advanced yet).
- `../../walrus-loader.md` §3–§4 — the raw `APPEND … ON CONFLICT DO NOTHING` + two-watermark commit that
  makes replay idempotent; §7 the max-applied guard that prevents resurrection on re-apply.

## Scope

**In scope**

- `sink_killed_mid_batch_loses_nothing`: drive a steady stream, `SIGKILL` the sink between a Parquet PUT
  and its standby-status update, restart it; assert the re-streamed changes de-duplicate and the mirror is
  correct (no loss, no dupes).
- `loader_killed_mid_merge_is_idempotent`: `SIGKILL` the loader after Phase-A append but before/during the
  Phase-B `MERGE` commit, restart it; assert both watermarks recover consistently and the mirror equals
  the source (no half-applied MERGE, no double-apply, no resurrected deletes).
- A convergence assertion comparing the final mirror row-by-row to the source after both replacements
  catch up.

**Explicitly deferred** (do *not* build these here)

- Slot **loss** (WAL gone) → **PR 4.6** total-restart; this PR is a *resume*, not a re-sync.
- WAL-runaway / heartbeat under a paused loader → **PR 4.5**.
- The graceful (SIGTERM) drain is already covered by PRs 2.28 / 3.12; this PR is the *ungraceful* path.

## Files to create / modify

```
tests/e2e/tests/crash_safety.rs      # new — sink-mid-batch + loader-mid-merge kills
tests/e2e/src/lib.rs                 # modify — kill_sink()/kill_loader() + restart_*() + assert_mirror_equals_source()
# no new deps
```

## Skeleton

```rust
// tests/e2e/tests/crash_safety.rs
#![cfg(feature = "it")]
use e2e::Harness;

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn sink_killed_mid_batch_loses_nothing() {
    // Start a steady INSERT workload; SIGKILL the sink between a Parquet PUT and its standby update.
    // Restart the sink; stop the workload; await convergence.
    // Assert: mirror == source (re-streamed changes de-duplicated); no gaps, no dupes.
    todo!()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn loader_killed_mid_merge_is_idempotent() {
    // Seed a batch; SIGKILL the loader during Phase-B MERGE; restart it.
    // Assert: raw_appended_lsn and transformed_lsn recover consistently; mirror == source;
    //         a delete that had been applied does NOT resurrect (max-applied guard holds).
    todo!()
}
```

```rust
// tests/e2e/src/lib.rs  (additions)
impl Harness {
    /// SIGKILL (not SIGTERM) the named child, then respawn it fresh.
    pub async fn kill_sink(&mut self) -> anyhow::Result<()> { todo!() }
    pub async fn restart_sink(&mut self) -> anyhow::Result<()> { todo!() }
    pub async fn kill_loader(&mut self) -> anyhow::Result<()> { todo!() }
    pub async fn restart_loader(&mut self) -> anyhow::Result<()> { todo!() }
    /// Row-by-row equality of the DuckDB mirror against the current source table.
    pub async fn assert_mirror_equals_source(&self, table: &str) -> anyhow::Result<()> { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] A sink `SIGKILL`ed **mid-batch** (between S3 PUT and standby update) restarts, re-streams from
      `confirmed_flush_lsn`, and the mirror converges to the source with **no loss and no dupes**.
- [ ] A loader `SIGKILL`ed **mid-MERGE** restarts with both watermarks consistent and the mirror equal to
      the source — no half-applied MERGE, no double-apply, no resurrected deletes.
- [ ] The result is **effectively-once**: the final mirror is identical to a run with no crash.
- [ ] The kills are `SIGKILL` (ungraceful), distinct from the SIGTERM drains in PRs 2.28 / 3.12.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `docker compose up --wait` then `cargo test -p e2e --features it -- --ignored` asserting
        **`sink_killed_mid_batch_loses_nothing`** and **`loader_killed_mid_merge_is_idempotent`**.

## Hints & gotchas

- Use `SIGKILL`, never `SIGTERM`, for the crash tests — `SIGTERM` triggers the graceful drain and you'd be
  testing the wrong path. Send signal 9 to the child PID directly.
- The precise crash window matters. To hit "mid-batch", pause the workload right after you observe a new
  Parquet object appear but *before* `confirmed_flush_lsn` advances (poll the slot). To hit "mid-MERGE",
  crash after `raw_appended_lsn` moves but before `transformed_lsn` does.
- The loader's idempotency rests on `ON CONFLICT DO NOTHING` in the raw append **and** the per-PK
  max-applied-commit-LSN guard (PR 3.7). If a re-apply resurrects a deleted row, that guard regressed —
  fail loudly.
- Convergence is eventual: after restart, both replacements need at least one poll cycle. Wait on the
  watermarks, then compare, then assert — never assert immediately after restart.

## References

- Design: `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` ("Crash safety");
  `../../architecture.md#15-the-durability-checkpoint-wal-bounding-invariant`;
  `../../walrus-loader.md` §3–§4, §7.
- Prev: [PR 4.3](./pr-4.3-e2e-large-txn-streaming.md) · Next: [PR 4.5](./pr-4.5-e2e-wal-runaway-heartbeat.md) ·
  [Roadmap](../README.md)
