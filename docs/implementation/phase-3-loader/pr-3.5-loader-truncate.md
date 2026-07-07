# PR 3.5 — TRUNCATE: a mirror wipe keyed on the `(commit_lsn, lsn)` tuple

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` · **Est. size:** M ·
> **Depends on:** PR 3.4 · **Unlocks:** PR 3.6

A pgoutput `TRUNCATE` carries **no tuple and no PK**, so it can't be a `MERGE` join branch. Instead the
transform handles it as a **pre-step wipe**: find the latest truncate `(Ct, Lt)` in the un-transformed
tail, `DELETE FROM <table>` if one exists, then add `AND (:Ct IS NULL OR (commit_lsn, lsn) > (:Ct, :Lt))`
to the dedup window so only rows **strictly after the truncate tuple** repopulate the mirror. The
subtlety this PR exists to nail: the boundary is the **tuple**, not the scalar `commit_lsn` — a
same-transaction `TRUNCATE; INSERT` shares one `commit_lsn`, so a scalar filter would wrongly drop the
post-truncate inserts.

## Why — learning objectives

By the end of this PR you will have practised:

- **Tuple-boundary comparison in SQL** — `(commit_lsn, lsn) > (Ct, Lt)` row-value comparison, and why
  the scalar `commit_lsn > Ct` is a data-loss bug for same-commit `TRUNCATE; INSERT`.
- **Operations without a PK** — modelling a table-wide wipe outside the per-key `MERGE`.
- **Raw-vs-mirror asymmetry** — the `t` op stays a logged row in `<table>_raw` (raw is never truncated),
  while `<table>` is emptied and repopulated.

## Read first

- `../../walrus-loader.md#55-truncate--a-wipe-keyed-on-the-commit_lsn-lsn-tuple` — the `(Ct, Lt)` query,
  the pre-`MERGE` `DELETE`, and the tuple-boundary window filter.
- `../../architecture.md#21-the-raw-to-mirror-transform-model` — step 0 (the truncate pre-step) in the
  canonical SQL, and the same-txn `TRUNCATE; INSERT` note.
- `../../architecture.md#per-change-type-handling-schema-evolution-semantics` — the `TRUNCATE` row:
  mirror emptied as of `(Ct, Lt)`; raw keeps the `t` op as a logged row.

## Scope

**In scope**

- Extend `TransformSql` (PR 3.3) with the truncate pre-step: query `(Ct, Lt)` = latest `op='t'` in the
  tail; `DELETE FROM <table> WHERE :Ct IS NOT NULL`; add `(:Ct IS NULL OR (commit_lsn, lsn) > (:Ct, :Lt))`
  to the dedup window.
- Advance `transformed_lsn` to include the truncate's `commit_lsn` even when only a `t` row is present.
- Keep the `t` op appended verbatim in `<table>_raw` (raw is never wiped).

**Explicitly deferred** (do *not* build these here)

- Unchanged-TOAST resolution → **PR 3.6**.
- The `_applied_commit_lsn` straddle guard interaction → **PR 3.7** (the truncate pre-step is the
  *template* the guard mirrors).
- Multiple truncates in one tail beyond "latest wins" — the `ORDER BY … LIMIT 1` already picks the
  latest; no per-truncate replay.

## Files to create / modify

```
crates/loader/src/transform.rs     # modify — truncate pre-step + tuple-boundary window filter
crates/loader/src/transform.sql    # modify — DELETE FROM <table> WHERE :Ct IS NOT NULL; window bound
crates/loader/tests/transform.rs   # modify — truncate unit tests (hermetic, in-memory)
```

## Skeleton

```rust
// crates/loader/src/transform.rs  (extends PR 3.3)
pub struct TruncateBoundary { pub ct: Option<common::Lsn>, pub lt: Option<common::Lsn> }

impl TransformSql {
    /// SELECT commit_lsn, lsn FROM <table>_raw
    ///   WHERE commit_lsn > :transformed_lsn AND op='t'
    ///   ORDER BY commit_lsn DESC, lsn DESC LIMIT 1;   -- (NULL, NULL) if none
    pub fn latest_truncate(&self, conn: &duckdb::Connection, transformed_lsn: &common::Lsn)
        -> Result<TruncateBoundary, LoaderError> { todo!() }

    /// DELETE FROM <table> WHERE :Ct IS NOT NULL  (no-op if the tail has no truncate).
    pub fn truncate_wipe_sql(&self) -> String { todo!() }

    /// The window now also filters `(:Ct IS NULL OR (commit_lsn, lsn) > (:Ct, :Lt))`.
    pub fn dedup_sql(&self) -> String { todo!() } // overrides PR 3.3
}
```

```rust
// crates/loader/tests/transform.rs  (hermetic, in-memory)
#[test] fn truncate_then_reinsert_keeps_only_post_truncate_rows() { todo!() }
#[test] fn same_commit_truncate_then_insert_survives_tuple_boundary() { todo!() } // shared commit_lsn
#[test] fn scalar_commit_lsn_boundary_would_drop_same_commit_inserts() { todo!() } // counterfactual
#[test] fn transformed_lsn_advances_past_a_truncate_only_tail() { todo!() }
#[test] fn raw_retains_the_truncate_op_row() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] A `TRUNCATE` followed by re-`INSERT`s in the tail → the mirror is emptied as of the truncate and
      holds **only** the post-truncate rows.
- [ ] A **same-transaction** `TRUNCATE; INSERT …` (shared `commit_lsn`, distinct `lsn`) → the
      post-truncate inserts **survive** — the tuple boundary `(commit_lsn, lsn) > (Ct, Lt)` keeps them.
- [ ] A counterfactual test demonstrates a **scalar** `commit_lsn > Ct` filter would drop those inserts
      (proving why the tuple is required).
- [ ] `transformed_lsn` advances past a truncate-only tail (a bare `TRUNCATE` never stalls the pipeline).
- [ ] `<table>_raw` **retains** the `t` op as a logged row — raw is never truncated.
- [ ] These are hermetic in-memory unit tests (no docker compose).
- [ ] Docs/comments state the tuple-not-scalar invariant at the boundary.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader --test transform` (and `--workspace` stays green) asserting
        **`same_commit_truncate_then_insert_survives_tuple_boundary`** and
        **`truncate_then_reinsert_keeps_only_post_truncate_rows`**.

## Hints & gotchas

- `(Ct, Lt)` are **both NULL** when the tail has no truncate; every downstream predicate must be
  NULL-safe (`:Ct IS NULL OR …`) or a no-truncate cycle silently deletes nothing *and* filters nothing —
  test the no-truncate path too.
- Run the `DELETE FROM <table>` **inside the same DuckDB transaction** as the dedup + `MERGE` so a wipe
  and its repopulation are atomic — a crash mid-way must not leave an empty mirror.
- The truncate wipe deletes **all** mirror rows, not just the ones in the batch — a `TRUNCATE` means the
  whole source table was emptied, so mirror rows from earlier cycles must go too.
- `lsn` orders same-commit ops: within one txn a `TRUNCATE` (`t`) has a lower `lsn` than the follow-up
  `INSERT`s, so `(commit_lsn, lsn) > (Ct, Lt)` keeps exactly the inserts. Verify your seed rows encode
  that ordering.
- Still advance `transformed_lsn` to include the truncate's `commit_lsn` even if `_batch` ends up empty
  — otherwise the same truncate re-fires every cycle.

## References

- Design: `../../walrus-loader.md#55-truncate--a-wipe-keyed-on-the-commit_lsn-lsn-tuple`;
  `../../architecture.md#21-the-raw-to-mirror-transform-model`,
  `#per-change-type-handling-schema-evolution-semantics`.
- Prev: [PR 3.4](./pr-3.4-loader-phase-b.md) ·
  Next: [PR 3.6](./pr-3.6-loader-unchanged-toast.md) · [Roadmap](../README.md)
