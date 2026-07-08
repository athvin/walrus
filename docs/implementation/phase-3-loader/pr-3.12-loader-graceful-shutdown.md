# PR 3.12 — Graceful SIGTERM drain + full-rebuild abort

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/65

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` · **Est. size:** M ·
> **Depends on:** PR 3.11 · **Unlocks:** PR 4.1 (phase boundary)

The loader's clean exit — the mirror image of the sink's WAL drain. On `SIGTERM`, each per-table worker
drains **in order**: stop claiming new files → finish the in-flight Phase A append (+ commit the
`raw_appended_lsn` advance and manifest delete atomically) → finish the in-flight Phase B transform
(+ commit `transformed_lsn`) → release the ownership lease → `CHECKPOINT` and close the file so no stale
lock is left. Crucially, an in-flight **periodic full-rebuild is aborted** (it's idempotent self-heal and
re-runs next cycle) so it can't blow the grace budget. Every restart is a resume from the two watermarks;
graceful drain just minimizes replay and avoids leaving a stale DuckDB lock for bootstrap to untangle.

## Why — learning objectives

By the end of this PR you will have practised:

- **Ordered drain via `CancellationToken`** — stop-intake-first, then finish-in-flight, then release.
- **Two-database atomic commit on shutdown** — keeping the DuckDB append and the control-DB
  watermark+delete txn atomic even under SIGTERM.
- **Selective abort** — cancelling the heavy full-rebuild while *forcing completion* of the incremental
  append+transform.
- **PID-1 signal delivery** — exec-form / `tini` so `SIGTERM` reaches the Rust process, not a shell.

## Read first

- `../../walrus-loader.md#85-graceful-shutdown--the-sigterm-drain` — the exact 6-step drain, the
  sink-vs-loader table, the ⚠ full-rebuild-abort callout, and grace-period sizing (exclude the rebuild).
- `../../walrus-loader.md#4-two-phase-apply--append-then-transform` — the atomic control-DB txn that
  Phase A must complete on drain; the crash-window table (ungraceful kill is still absorbed).
- `../../walrus-loader.md#86-decommission-and-node-drain` — node-drain interplay, PVC reattach, and why
  the PDB must not block eviction.

## Scope

**In scope**

- Wire `SIGTERM` → a `CancellationToken` that each per-table worker observes at drain points.
- The ordered drain: stop claiming → finish Phase A (+ atomic `raw_appended_lsn` advance + manifest
  delete) → finish Phase B (+ `transformed_lsn`) → release lease → `CHECKPOINT` + close file.
- **Abort** any in-flight full-rebuild (roll back the transient rebuild; it re-runs next cycle).
- Skip `preStop`; ensure the process is PID 1 / exec-form so the grace budget is fully the drain's.

**Explicitly deferred** (do *not* build these here)

- The Dockerfile `tini`/exec-form wiring → **PR 4.8** (note the requirement here).
- K8s `terminationGracePeriodSeconds` / PDB manifest → **PR 4.9** (state the sizing rule here).
- Crash-safety e2e (ungraceful `SIGKILL` mid-MERGE) → **PR 4.4** (this PR is *graceful* drain).

## Files to create / modify

```
crates/loader/src/shutdown.rs      # new — install SIGTERM handler → CancellationToken; drain orchestration
crates/loader/src/apply_loop.rs    # modify — observe the token at drain points; finish-in-flight
crates/loader/src/compaction.rs    # modify — full_rebuild honors the cancel token (abort + rollback)
crates/loader/src/main.rs          # modify — spawn drain; exit 0 within grace
crates/loader/tests/shutdown.rs    # new — compose: SIGTERM mid-apply → both watermarks committed, lease released
```

## Skeleton

```rust
// crates/loader/src/shutdown.rs
/// SIGTERM → cancel. Each per-table worker drains in order before the process exits 0.
pub async fn drain_on_sigterm(workers: Vec<WorkerHandle>, cancel: CancellationToken)
    -> Result<(), LoaderError> {
    // cancel.cancel();  // 1. stop claiming
    // for w in workers {
    //   w.finish_phase_a().await?;   // 2. finish append + atomic (raw_appended_lsn advance + DELETE)
    //   w.finish_phase_b().await?;   // 3. finish transform + commit transformed_lsn
    //   w.abort_full_rebuild();      //    idempotent self-heal → abort, do not wait
    //   w.release_lease().await?;    // 4. release ownership lease
    //   w.checkpoint_and_close()?;   // 5. CHECKPOINT + close file (no stale lock)
    // }                              // 6. never touch the slot (loader doesn't own it)
    todo!()
}
```

```rust
// crates/loader/src/apply_loop.rs  (extends PR 3.4)
impl Worker {
    /// At a drain point: stop new claims but COMPLETE the in-flight Phase A atomically.
    pub async fn finish_phase_a(&mut self) -> Result<(), LoaderError> { todo!() }
    pub async fn finish_phase_b(&mut self) -> Result<(), LoaderError> { todo!() }
    /// Abort an in-flight full-rebuild (roll back the transient rebuild; it re-runs next cycle).
    pub fn abort_full_rebuild(&mut self) { todo!() }
}
```

```rust
// crates/loader/tests/shutdown.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn sigterm_mid_apply_commits_both_watermarks_and_releases_lease() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn in_flight_full_rebuild_is_aborted_on_sigterm() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn a_replacement_loader_resumes_from_the_two_watermarks() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] On `SIGTERM` mid-apply, the worker stops claiming, **finishes** the in-flight Phase A (with the
      atomic `raw_appended_lsn` advance + manifest delete) and Phase B (`transformed_lsn`), then exits 0
      within grace — **both watermarks committed**.
- [x] The ownership **lease is released** and the DuckDB file is `CHECKPOINT`ed + closed cleanly — **no
      stale lock** for the next bootstrap.
- [x] An in-flight **full-rebuild is aborted** (rolled back) on `SIGTERM`, not waited on — it re-runs
      next cycle.
- [x] A **replacement** loader started afterward **resumes** from the two watermarks with no data loss
      and no duplicate application.
- [x] The loader never touches the replication slot.
- [x] Docs/comments state the PID-1/exec-form requirement (deferred wiring → PR 4.8) and the grace-sizing
      rule (measured incremental worst case; exclude the full-rebuild; skip `preStop`).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p loader --test shutdown -- --ignored`
        asserting **`sigterm_mid_apply_commits_both_watermarks_and_releases_lease`** and
        **`in_flight_full_rebuild_is_aborted_on_sigterm`**.

## Hints & gotchas

- Keep the Phase A DuckDB append-commit and the control-DB `raw_appended_lsn`+DELETE txn **atomic even
  under drain** — the drain must not create a new crash window; if you can't finish both, don't advance
  the watermark (the queue + `ON CONFLICT` absorbs a re-claim).
- The full-rebuild is the **only** thing you abort — everything incremental you *complete*. Grace-period
  sizing (PR 4.9) is the incremental worst case precisely because the rebuild is excluded.
- **Skip `preStop`** — a non-serving consumer gains nothing from it, and any preStop time is subtracted
  from the same grace budget the drain needs. Let `SIGTERM` arrive at T=0.
- Releasing the lease **before** closing the file is fine because the file lock is the second fence; but
  don't release it before the watermark commits, or a fast replacement could double-apply the tail
  (the row PK still absorbs it, but avoid the churn).
- There is **no `wal_sender_timeout` analogue** — the loader drain is bounded only by the grace period
  and DuckDB commit latency, a genuine simplification vs the sink.
- Make sure the SIGTERM handler cancels the token *once* and is idempotent — a double-SIGTERM during
  drain must not skip steps.

## References

- Design: `../../walrus-loader.md#85-graceful-shutdown--the-sigterm-drain`,
  `#4-two-phase-apply--append-then-transform`, `#86-decommission-and-node-drain`;
  `../../walrus-pg-sink.md#46-the-loaders-shutdown-differs`.
- Prev: [PR 3.11](./pr-3.11-loader-full-rebuild-compaction.md) ·
  Next: [PR 4.1](../phase-4-end-to-end/pr-4.1-e2e-thin-slice.md) · [Roadmap](../README.md)
