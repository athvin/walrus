# PR 3.4 — Phase B wiring + advance `transformed_lsn`, and the per-table apply loop

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` · **Est. size:** M ·
> **Depends on:** PR 3.3 · **Unlocks:** PR 3.5

Now connect the transform (PR 3.3) to real data. Phase B reads only `commit_lsn > transformed_lsn` from
`<table>_raw`, runs the dedup + `MERGE` **inside one DuckDB transaction**, commits, then advances
`transformed_lsn` to the max commit LSN applied. Wrap Phase A (PR 3.2) + Phase B into a **per-table
apply loop** on the poll cadence, stamping `last_poll_completed_at` every cycle. After this PR the
mirror `<table>` equals the current source shape end-to-end from seeded Parquet — and a crash between
phases just re-runs harmlessly.

## Why — learning objectives

By the end of this PR you will have practised:

- **Idempotent-by-watermark transform** — reading `commit_lsn > transformed_lsn` so a re-run produces
  the same winners with no bespoke dedup.
- **Atomic DuckDB apply** — dedup→`TEMP`→`MERGE`→commit as one transaction, then advance the watermark.
- **The two-watermark discipline** — `transformed_lsn ≤ raw_appended_lsn` always (the control-plane
  `CHECK` enforces it).
- **A poll-cadence apply loop** — `tokio::select!` over an interval + shutdown token, one worker per
  table, stamping liveness every cycle.

## Read first

- `../../walrus-loader.md#4-two-phase-apply--append-then-transform` — Phase B "runs off `<table>_raw`,
  never re-reads the manifest", advances `transformed_lsn = max(commit_lsn)`; the crash-window table.
- `../../walrus-loader.md#84-steady-state--the-apply-loop-cadences-and-the-pdb` — one apply loop per
  table, Phase A + Phase B share one poll cadence in v1 (two txns).
- `../../architecture.md#21-the-raw-to-mirror-transform-model` — the `BEGIN … MERGE … COMMIT; advance
  transformed_lsn` shape you are wiring.

## Scope

**In scope**

- `run_phase_b(ctx)`: render the transform (PR 3.3) for the table, run dedup + `MERGE` in one DuckDB
  txn over `commit_lsn > transformed_lsn`, commit, then advance `transformed_lsn = max(commit_lsn)`
  applied (via `control::loader_checkpoint`).
- The per-table **apply loop**: `Phase A` then `Phase B` per poll interval; stamp
  `last_poll_completed_at` every cycle (even a no-op poll).
- Idempotent re-run: running Phase B twice over the same tail leaves the mirror unchanged.

**Explicitly deferred** (do *not* build these here)

- TRUNCATE `(Ct, Lt)` handling → **PR 3.5** (leave the `op<>'t'` window filter; wipe added there).
- Unchanged-TOAST resolution → **PR 3.6**.
- The `_applied_commit_lsn` straddle guard → **PR 3.7**.
- Pending-DDL apply at a `schema_version` boundary → **PR 3.8 / 3.9**.
- Compaction / full-rebuild cadence → **PR 3.11** (this loop is the incremental cadence only).

## Files to create / modify

```
crates/loader/src/phase_b.rs        # new — render transform, run in one DuckDB txn, advance transformed_lsn
crates/loader/src/apply_loop.rs     # new — per-table poll loop (Phase A + Phase B + liveness stamp)
crates/control/src/loader_checkpoint.rs  # modify — advance_transformed_lsn(txn)
crates/loader/tests/phase_b.rs      # new — compose integration (seed Parquet → mirror == source)
```

## Skeleton

```rust
// crates/loader/src/phase_b.rs
/// Transform the un-transformed tail into the mirror, in one DuckDB txn, then advance transformed_lsn.
pub async fn run_phase_b(ctx: &TableCtx) -> Result<Option<common::Lsn>, LoaderError> {
    // let t = TransformSql::from_relation(&ctx.rel);
    // DuckDB txn: apply_transform(&conn, &t, &ctx.transformed_lsn)?;  COMMIT
    // max_applied = SELECT max(commit_lsn) FROM <table>_raw WHERE commit_lsn > :transformed_lsn
    // control txn: advance_transformed_lsn(max_applied)  (CHECK transformed_lsn <= raw_appended_lsn)
    todo!()
}

// crates/loader/src/apply_loop.rs
/// One worker per table. Phase A + Phase B share one poll cadence in v1.
pub async fn apply_loop(ctx: TableCtx, shutdown: CancellationToken) -> Result<(), LoaderError> {
    // loop {
    //   tokio::select! { _ = shutdown.cancelled() => break, _ = tick(poll_interval) => {} }
    //   run_phase_a(&ctx).await?;
    //   run_phase_b(&ctx).await?;
    //   ctx.state.stamp_last_poll_completed();   // EVERY cycle, even a no-op poll
    // }
    todo!()
}
```

```rust
// crates/loader/tests/phase_b.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn mirror_equals_current_source_after_transform() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn transformed_lsn_advances_to_max_applied_commit_lsn() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn re_running_phase_b_is_idempotent() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] After Phase A + Phase B over seeded Parquet, `<table>` equals the **current** source state (one
      row per PK, current values) — the §3.3 collapse rule now runs on real appended rows.
- [ ] `transformed_lsn` advances to `max(commit_lsn)` applied and **never** exceeds `raw_appended_lsn`
      (the control-plane `CHECK` holds).
- [ ] Phase B reads only `commit_lsn > transformed_lsn` and **never** re-reads the manifest.
- [ ] Re-running Phase B over the same tail leaves the mirror **byte-identical** (idempotent).
- [ ] The apply loop stamps `last_poll_completed_at` **every** cycle, including a no-op poll, and exits
      cleanly on the shutdown token.
- [ ] A crash simulated between Phase A and Phase B is absorbed by a plain Phase-B re-run (no manual step).
- [ ] Docs/comments explain why Phase B is naturally idempotent (watermark + LWW dedup → same winners).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p loader --test phase_b -- --ignored`
        asserting **`mirror_equals_current_source_after_transform`** and
        **`re_running_phase_b_is_idempotent`**.

## Hints & gotchas

- Advance `transformed_lsn` **after** the DuckDB `MERGE` commits, in a separate control-DB txn — the two
  databases can't share a transaction, and Phase B is designed to be safely re-runnable if you crash in
  between.
- The `MERGE` reconciles against the **whole** mirror, not just the batch — that's what keeps
  cross-batch state (a key updated last cycle, deleted this cycle) correct while cost stays O(new events).
- Don't advance `transformed_lsn` past a `commit_lsn` whose rows aren't all appended yet — in v1 Phase A
  runs first each cycle so this holds, but PR 3.7 / 3.10 harden the snapshot-straddle edge; leave a
  `// TODO(3.7): equal-commit_lsn snapshot straddle` marker at the advance site.
- Keep the poll interval, compaction cadence, and retention window as **distinct** config knobs even
  though only the poll interval is wired here — PR 3.11 fills the other two.
- One worker thread per `.duckdb` file is the whole parallelism model; do not share a DuckDB connection
  across tables.

## References

- Design: `../../walrus-loader.md#4-two-phase-apply--append-then-transform`,
  `#84-steady-state--the-apply-loop-cadences-and-the-pdb`;
  `../../architecture.md#21-the-raw-to-mirror-transform-model`.
- Prev: [PR 3.3](./pr-3.3-loader-transform-template.md) ·
  Next: [PR 3.5](./pr-3.5-loader-truncate.md) · [Roadmap](../README.md)
