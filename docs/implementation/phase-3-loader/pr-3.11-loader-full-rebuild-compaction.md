# PR 3.11 — Periodic full-rebuild / compaction + retention prune

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/64

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` · **Est. size:** L ·
> **Depends on:** PR 3.10 · **Unlocks:** PR 3.12

The self-heal-and-reclaim job. On a **slower, per-table** cadence, the loader rebuilds the mirror from
scratch — `CREATE OR REPLACE TABLE <table> AS SELECT …` over **retained raw ∪ the current mirror injected
as an LSN-floor baseline** — dropping `op='d'` winners, then prunes `<table>_raw` below the retention
floor. This is the *only* thing that actually reclaims disk (DuckDB `DELETE` merely tombstones;
`VACUUM FULL` is unimplemented), and it is what guarantees a value whose last real write was already
pruned is **never lost** (the mirror baseline). It runs on the **same worker thread**, serialized after
an apply cycle, needs the exclusive writer, and ~2× transient space.

## Why — learning objectives

By the end of this PR you will have practised:

- **Atomic table replacement** — `CREATE OR REPLACE TABLE … AS SELECT` (readers see the old table until
  commit) as the reclamation primitive.
- **The LSN-floor baseline union** — unioning the current mirror as a baseline row per PK so pruned raw
  history can't lose a current value (or an unchanged-TOAST value).
- **DuckDB storage truth** — `DELETE` tombstones only; real reclamation rides the rebuild.
- **Same-thread serialization** — running the heavy job on the table's own worker after an apply cycle,
  no quiescing dance.

## Read first

- `../../walrus-loader.md#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild` — the incremental-vs-
  full-rebuild table; the baseline "retained raw ∪ current mirror injected as an LSN-floor baseline".
- `../../walrus-loader.md#94-retention-and-compaction-realities` — the space-reclamation truth table
  (`DELETE`/`CHECKPOINT`/`VACUUM FULL`/full-rebuild), the atomicity + 2×-space + exclusive-writer cost.
- `../../walrus-loader.md#84-steady-state--the-apply-loop-cadences-and-the-pdb` — the three cadences
  (poll / compaction / retention) and the ⚠ "run it on the same worker thread, serialized after an
  apply cycle" recommendation.

## Scope

**In scope**

- A per-table **compaction cadence** knob (distinct from the poll interval) + a **raw-retention window**
  knob (e.g. ~7d / last-K).
- The full-rebuild statement: `CREATE OR REPLACE TABLE <table> AS SELECT …` over retained raw ∪ mirror
  baseline, reusing the transform's dedup/collapse (incl. TRUNCATE, TOAST, guard columns), dropping
  `op='d'` winners.
- Retention prune: `DELETE FROM <table>_raw WHERE commit_lsn < :retention_floor` (below the floor,
  behind `transformed_lsn`).
- Serialize the rebuild on the table's worker thread, after an apply cycle.

**Explicitly deferred** (do *not* build these here)

- **Aborting** an in-flight rebuild on SIGTERM → **PR 3.12** (leave a cancellation hook here).
- `COPY FROM DATABASE` full-file rewrite → optional; `CREATE OR REPLACE` suffices for v1.
- Metrics for reclaimed bytes / rebuild duration → **PR 4.10**.

## Files to create / modify

```
crates/loader/src/compaction.rs    # new — full_rebuild() + prune_raw(); reuse transform dedup/collapse
crates/loader/src/apply_loop.rs    # modify — schedule compaction on its own cadence, same worker thread
crates/loader/src/transform.rs     # modify — dedup that unions the mirror baseline (LSN floor)
crates/loader/tests/compaction.rs  # new — unit/compose: rebuild identical + pruned value survives
```

## Skeleton

```rust
// crates/loader/src/compaction.rs
/// Atomic self-heal + reclaim. Runs on the table's own worker thread, serialized after an apply cycle.
pub struct RebuildOpts { pub retention_floor: common::Lsn }

/// CREATE OR REPLACE TABLE <table> AS SELECT <collapse over retained raw ∪ mirror-baseline, drop op='d'>.
/// Atomic: readers see the OLD table until COMMIT; needs the exclusive writer + ~2× transient space.
pub fn full_rebuild(conn: &duckdb::Connection, t: &TransformSql, opts: &RebuildOpts,
                    cancel: &CancellationToken) -> Result<(), LoaderError> { todo!() }

/// Reclaim: DELETE raw below the floor (behind transformed_lsn), then CHECKPOINT.
pub fn prune_raw(conn: &duckdb::Connection, t: &TransformSql, floor: &common::Lsn)
    -> Result<u64, LoaderError> { todo!() }
```

```rust
// crates/loader/tests/compaction.rs
/// Full-rebuild yields a mirror IDENTICAL to the incremental one over the same history.
#[test] fn full_rebuild_matches_incremental_mirror() { todo!() }                 // hermetic
/// A raw value pruned below the floor still survives via the mirror LSN-floor baseline.
#[test] fn pruned_value_survives_via_mirror_baseline() { todo!() }               // hermetic
/// op='d' winners are dropped (not resurrected) by the rebuild.
#[test] fn deleted_keys_stay_absent_after_rebuild() { todo!() }                  // hermetic
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn rebuild_reclaims_space_and_prune_keeps_mirror_correct() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] The full-rebuild produces a mirror **identical** to the incremental transform over the same
      history (same rows, same values, `op='d'` winners dropped).
- [x] A raw value pruned below the retention floor **survives** because the rebuild unions the current
      mirror as an LSN-floor baseline (nothing lost, incl. resolved unchanged-TOAST values).
- [x] The rebuild is **atomic** (`CREATE OR REPLACE … AS SELECT`; readers see the old table until
      commit) and reclaims space where ordinary `DELETE`/`CHECKPOINT` would not.
- [x] Compaction and retention are **distinct** cadence knobs from the poll interval, per-table
      overridable; the rebuild runs on the **same worker thread**, serialized after an apply cycle.
- [x] A leftover cancellation hook exists for PR 3.12 to abort an in-flight rebuild.
- [x] Docs/comments state the DuckDB storage truth (`DELETE` tombstones; `VACUUM FULL` unimplemented;
      reclamation rides the rebuild).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p loader --test compaction -- --ignored`
        asserting **`rebuild_reclaims_space_and_prune_keeps_mirror_correct`**.

## Hints & gotchas

- **Union the mirror baseline into the rebuild source**, tagged at the LSN floor, so a PK whose only raw
  evidence was pruned still contributes its current value — omitting this is a silent data-loss bug the
  moment retention bites.
- The rebuild reuses the transform's dedup/collapse (TRUNCATE, TOAST back-scan, the `_applied_*` guard) —
  keep it driven by the same `TransformSql` template so it can't drift from the incremental path.
- Prune raw **only below the retention floor and behind `transformed_lsn`** — never delete rows the
  incremental transform still needs, and never the current-mirror baseline's evidence for an
  as-yet-unpruned PK.
- `DELETE` doesn't shrink the file; the space comes from the `CREATE OR REPLACE`. Assert reclamation by
  measuring file size before/after the rebuild, not after the prune.
- Run it **after** an apply cycle on the same thread — no separate connection, no quiescing dance — so it
  can never contend with that table's own writer.
- Size the compaction cadence for low-traffic windows; it holds the exclusive writer and doubles
  transient space for the duration.

## References

- Design: `../../walrus-loader.md#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild`,
  `#94-retention-and-compaction-realities`,
  `#84-steady-state--the-apply-loop-cadences-and-the-pdb`.
- Prev: [PR 3.10](./pr-3.10-loader-snapshot-stream-boundary.md) ·
  Next: [PR 3.12](./pr-3.12-loader-graceful-shutdown.md) · [Roadmap](../README.md)
