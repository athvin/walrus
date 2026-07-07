# PR 3.7 — The per-PK max-applied-commit-LSN guard (⚠ extends architecture.md)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/60

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` · **Est. size:** M ·
> **Depends on:** PR 3.6 · **Unlocks:** PR 3.8

The one substantive *addition* the loader doc makes to `architecture.md`. The incremental `MERGE`
applies the window winner **unconditionally** — correct only if a winner's `(commit_lsn, lsn)` is always
≥ the tuple that last shaped the mirror row it touches. Two faces break that: (A) the equal-`commit_lsn`
**snapshot straddle** (a snapshot row shares `consistent_point` as its `commit_lsn`, and a strict
`> :transformed_lsn` window excludes it *forever*), and (B) a **delete + re-insert straddling the
watermark** resurrecting a killed row. This PR carries two hidden columns — `_applied_commit_lsn` and
`_applied_lsn` — on the mirror and guards **every** `MERGE` branch so a stale winner can never overwrite
or resurrect. The periodic full-rebuild (PR 3.11) is the safety net regardless; this makes the
*incremental* path correct on its own.

## Why — learning objectives

By the end of this PR you will have practised:

- **A self-correcting MERGE** — guarding each branch with `(s.commit_lsn, s.lsn) >
  (t._applied_commit_lsn, t._applied_lsn)` so a stale event is a no-op regardless of watermark timing.
- **Hidden/internal columns** — keeping `_applied_*` out of user-facing projections (or in a sibling
  shadow table if a byte-identical mirror is required).
- **Boundary bounds** — pairing the guard with a `>=` low bound (or per-PK re-scan) so equal-LSN
  snapshot straddles are re-examined, and the guard rejects only genuinely-stale writes.

## Read first

- `../../walrus-loader.md#7-straddling-the-watermark--the-per-pk-max-applied-commit-lsn-guard` — the
  whole section: break faces A/B, the guarded 3-branch `MERGE`, and options (a) guard column vs (b)
  tuple-boundary watermark (prefer a).
- `../../architecture.md#17-snapshot--backfill-bootstrap` — why all snapshot rows carry `commit_lsn =
  consistent_point` (the equal-`commit_lsn` straddle source).
- `../../walrus-loader.md#55-truncate--a-wipe-keyed-on-the-commit_lsn-lsn-tuple` — the tuple-boundary
  treatment this guard mirrors.

## Scope

**In scope**

- Add hidden `_applied_commit_lsn` / `_applied_lsn` columns to `<table>` (created here; back-fill
  existing rows from their current winner or a sentinel low LSN).
- Rewrite the `MERGE` branches to set `_applied_*` on INSERT/UPDATE and to gate DELETE/UPDATE on
  `(s.commit_lsn, s.lsn) > (t._applied_commit_lsn, t._applied_lsn)`.
- Relax the window low bound to `>=` (or a per-PK re-scan) so an equal-`commit_lsn` snapshot row is
  re-examined and applied when it is genuinely newer per the guard.
- Keep `_applied_*` out of user-facing projections.

**Explicitly deferred** (do *not* build these here)

- The full snapshot/stream overlap end-to-end (equal-`lsn_end` split-batch files) → **PR 3.10** (this
  PR fixes the *per-PK* guard; 3.10 proves the boundary through the transform).
- Option (b) tuple-boundary Phase-B watermark → documented alternative; ship option (a).
- The full-rebuild self-heal → **PR 3.11** (the safety net that holds even without this guard).

## Files to create / modify

```
crates/loader/src/transform.rs     # modify — guarded MERGE branches; _applied_* projection rules
crates/loader/src/transform.sql    # modify — guard predicates + _applied_* set/insert
crates/loader/src/duck.rs          # modify — add _applied_commit_lsn/_applied_lsn to <table> at ensure/migrate
crates/loader/tests/transform.rs   # modify — hermetic straddle unit tests
```

## Skeleton

```rust
// crates/loader/src/transform.rs  (extends PR 3.3–3.6)
impl TransformSql {
    /// MERGE INTO <table> t USING _batch s ON t.<pk> = s.<pk>
    ///   WHEN MATCHED AND s.op='d'
    ///        AND (s.commit_lsn, s.lsn) > (t._applied_commit_lsn, t._applied_lsn) THEN DELETE
    ///   WHEN MATCHED
    ///        AND (s.commit_lsn, s.lsn) > (t._applied_commit_lsn, t._applied_lsn) THEN
    ///        UPDATE SET <non-key cols>=s.<col>, _applied_commit_lsn=s.commit_lsn, _applied_lsn=s.lsn
    ///   WHEN NOT MATCHED AND s.op<>'d' THEN
    ///        INSERT (<cols>,_applied_commit_lsn,_applied_lsn) VALUES (s.<cols>,s.commit_lsn,s.lsn);
    pub fn guarded_merge_sql(&self) -> String { todo!() } // overrides PR 3.3 merge_sql

    /// Window low bound relaxed to `>=` so equal-commit_lsn snapshot rows are re-examined.
    pub fn dedup_sql(&self) -> String { todo!() }
}
```

```rust
// crates/loader/tests/transform.rs  (hermetic, in-memory)
/// Break face B: delete + reinsert straddling the watermark must not resurrect.
#[test] fn stale_delete_reinsert_across_watermark_does_not_resurrect() { todo!() }
/// Break face A: snapshot row with commit_lsn == transformed_lsn is NOT dropped.
#[test] fn equal_commit_lsn_snapshot_row_is_still_applied() { todo!() }
/// The guard rejects a genuinely-stale winner but applies a newer one.
#[test] fn guard_applies_newer_and_rejects_stale_by_tuple() { todo!() }
/// _applied_* never appear in a user-facing SELECT * of the mirror.
#[test] fn applied_columns_are_hidden_from_user_projections() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] A delete + re-insert **straddling** the watermark does **not** resurrect the killed row (break
      face B) — the guard makes a stale winner a no-op.
- [x] A snapshot row whose `commit_lsn == transformed_lsn` is **not** silently dropped (break face A) —
      the relaxed `>=` bound re-examines it and the guard applies it.
- [x] Every `MERGE` branch (DELETE / UPDATE / INSERT) maintains `_applied_commit_lsn` / `_applied_lsn`
      and gates the mutating branches on the tuple comparison.
- [x] `_applied_*` do **not** appear in user-facing projections of `<table>` (hidden, or in a shadow
      table if a byte-identical mirror is required).
- [x] The incremental path is now self-correcting; a comment states the full-rebuild remains the safety
      net regardless.
- [x] Hermetic in-memory unit tests (no docker compose).
- [x] Docs/comments flag this as **⚠ extends architecture.md** (Open Q8/Q13) and name the two break faces.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader --test transform` (and `--workspace` stays green) asserting
        **`stale_delete_reinsert_across_watermark_does_not_resurrect`** and
        **`equal_commit_lsn_snapshot_row_is_still_applied`**.

## Hints & gotchas

- The guard and the relaxed low bound work **together**: `>=` alone would re-apply already-applied rows
  every cycle; the tuple guard makes those re-applications no-ops, so the mirror stays idempotent while
  the snapshot straddle is closed.
- Back-fill `_applied_*` for rows created before this PR from their existing winner's `(commit_lsn,
  lsn)` if recoverable, else a low sentinel LSN (`0/0`) — a too-low seed just means the first real event
  wins, which is correct.
- Row-value comparison `(a, b) > (c, d)` in DuckDB is lexicographic — exactly the `(commit_lsn, lsn)`
  ordering you want; do not decompose it into `a > c OR (a = c AND b > d)` by hand (error-prone).
- If a byte-identical mirror to source is a hard requirement, put `_applied_*` in a **sibling shadow
  table** keyed by PK and join it in the `MERGE` `ON` — the DoD's "hidden from projections" box then
  means the shadow table, not `EXCLUDE`.
- This is an *incremental-path* correctness upgrade, not a replacement for the full-rebuild — keep the
  PR 3.11 self-heal; the two are complementary.

## References

- Design: `../../walrus-loader.md#7-straddling-the-watermark--the-per-pk-max-applied-commit-lsn-guard`;
  `../../architecture.md#17-snapshot--backfill-bootstrap`, `#open-questions--risks` (Q8/Q13).
- Prev: [PR 3.6](./pr-3.6-loader-unchanged-toast.md) ·
  Next: [PR 3.8](./pr-3.8-loader-ddl-additive.md) · [Roadmap](../README.md)
