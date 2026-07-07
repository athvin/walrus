# PR 3.6 — Unchanged-TOAST resolution: the raw back-scan

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/59

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` · **Est. size:** M ·
> **Depends on:** PR 3.5 · **Unlocks:** PR 3.7

pgoutput's `'u'` placeholder means a large out-of-line TOAST column **was not modified, so its value is
absent from the wire** — semantically distinct from a real SQL `NULL` (`'n'`). `<table>_raw` stores the
sentinel verbatim; the **transform must resolve it**. For each column named in the winner's
`unchanged_toast` list, substitute the **last non-sentinel value for that PK, found by scanning
`<table>_raw` backward from the winner's `(commit_lsn, lsn)`**, falling back to the current mirror value
only as a last resort. The case that forces the back-scan (not a mirror lookup): when the write that set
the TOAST value and a later unchanged-TOAST update land in the **same batch**, the mirror has no row for
that PK yet — a mirror-only lookup writes `NULL` and silently drops the real value.

## Why — learning objectives

By the end of this PR you will have practised:

- **NULL vs unchanged-TOAST** — treating the sentinel as "value unknown, go find it", never as `NULL`.
- **A correlated back-scan in SQL** — the last non-sentinel value per PK at or before the winner's
  `(commit_lsn, lsn)`, expressed set-based (window / lateral), not a per-row loop.
- **`COALESCE` fallback ordering** — raw back-scan first, current mirror value last.
- **Same-batch reasoning** — why "just read the current mirror" loses the value when the setter and the
  unchanged-TOAST update share one transform window.

## Read first

- `../../walrus-loader.md#56-unchanged-toast-resolution--the-raw-back-scan` — the per-column back-scan,
  the same-batch worked example (`INSERT big='X'@100`, `UPDATE big=<sentinel>@200`), and the
  `COALESCE(raw back-scan, current mirror)` resolution.
- `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` — the "Intra-batch TOAST
  carry-forward" bullet: mirror must end `big='X'`, **not** NULL.
- `../../proto-version.md` (§5, TupleData / the unchanged-TOAST placeholder) — how the sentinel appears
  on the wire and why it differs from `'n'`.

## Scope

**In scope**

- Represent the sentinel distinctly in `<table>_raw` (a value the transform can test for per column) —
  reuse how the sink/pg-to-arrow encodes `TupleValue::UnchangedToast`.
- Extend the transform's step 2: for each column in the winner's `unchanged_toast` list, replace the
  sentinel with the last non-sentinel value for that PK from `<table>_raw` at or before the winner's
  `(commit_lsn, lsn)`; `COALESCE` to the current `<table>` value last.
- Ship it as part of the shared template so the hermetic tests exercise the exact production SQL.

**Explicitly deferred** (do *not* build these here)

- The full-rebuild's extra mirror-baseline union (which rescues a TOAST value whose last real write was
  already pruned) → **PR 3.11**.
- Multi-column TOAST interplay with DDL type changes → covered by the DDL PRs **3.8 / 3.9**.
- The `_applied_commit_lsn` guard → **PR 3.7**.

## Files to create / modify

```
crates/loader/src/transform.rs     # modify — resolve_unchanged_toast column substitution
crates/loader/src/transform.sql    # modify — per-column COALESCE(back-scan, mirror) in _batch build
crates/loader/tests/transform.rs   # modify — hermetic unchanged-TOAST unit tests
```

## Skeleton

```rust
// crates/loader/src/transform.rs  (extends PR 3.3/3.5)
impl TransformSql {
    /// For each toastable column, emit a resolved projection into _batch:
    ///   COALESCE(
    ///     -- last non-sentinel value for this PK at or before the winner's (commit_lsn, lsn):
    ///     (SELECT r.<col> FROM <table>_raw r
    ///        WHERE r.<pk> = s.<pk> AND r.<col> IS NOT <sentinel>
    ///          AND (r.commit_lsn, r.lsn) <= (s.commit_lsn, s.lsn)
    ///        ORDER BY r.commit_lsn DESC, r.lsn DESC LIMIT 1),
    ///     t.<col>   -- current mirror value, last resort
    ///   )
    /// only when the winner's unchanged_toast list names <col>; otherwise pass s.<col> through.
    pub fn resolve_toast_sql(&self, toastable: &[String]) -> String { todo!() }
}
```

```rust
// crates/loader/tests/transform.rs  (hermetic, in-memory)
/// §5.6 worked case: INSERT big='X'@100, UPDATE big=<sentinel>@200, same PK, mirror empty.
#[test] fn same_batch_unchanged_toast_carries_forward_the_prior_value() { todo!() } // mirror big='X', not NULL
#[test] fn unchanged_toast_falls_back_to_current_mirror_when_raw_has_none() { todo!() }
#[test] fn real_null_is_not_treated_as_unchanged_toast() { todo!() }               // 'n' stays NULL
#[test] fn non_toast_columns_pass_through_untouched() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] The §5.6 worked case — `INSERT big='X'` @100 then `UPDATE …, big=<sentinel>` @200 for the same
      PK, mirror empty — ends with the mirror row holding **`big='X'`**, resolved by back-scanning
      `<table>_raw`, **not** `NULL`.
- [x] When raw has no non-sentinel value for the column, resolution falls back to the current `<table>`
      value (last resort), not `NULL`.
- [x] A real SQL `NULL` (`op` payload `'n'`) is **never** treated as unchanged-TOAST — it stays `NULL`.
- [x] Non-toast / non-listed columns pass through untouched (the substitution is per named column only).
- [x] The back-scan is set-based (no per-row Rust loop), reading only `<table>_raw` (+ `<table>` for the
      fallback).
- [x] Hermetic in-memory unit tests (no docker compose).
- [x] Docs/comments explain why a mirror-only lookup loses the value in the same-batch case.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader --test transform` (and `--workspace` stays green) asserting
        **`same_batch_unchanged_toast_carries_forward_the_prior_value`**.

## Hints & gotchas

- The sentinel must be **distinguishable** from both a real value and a real `NULL`. Decide the encoding
  with the sink side (`TupleValue::UnchangedToast`) — a magic string collides with real data; prefer a
  side-channel (the winner's `unchanged_toast` JSON list drives *which* columns to resolve, so the
  stored column value can even be left NULL and the list is the signal).
- Scan `<= (s.commit_lsn, s.lsn)` (inclusive) — the winner itself may carry a real value for some
  columns and a sentinel for others; the resolution is per column, keyed off the winner's list.
- The back-scan bound `(r.commit_lsn, r.lsn) <= (winner)` prevents pulling a *newer* value that the
  winner already superseded — order matters, use the tuple.
- This runs **before** the `MERGE`, while building `_batch`, so the resolved value is what the mirror
  actually stores. Resolving after the MERGE would be too late.
- Keep the fallback to `t.<col>` (current mirror) *last* in `COALESCE` — it only fires when raw has been
  pruned below the setter, which the PR 3.11 full-rebuild baseline additionally protects.

## References

- Design: `../../walrus-loader.md#56-unchanged-toast-resolution--the-raw-back-scan`;
  `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later`;
  `../../proto-version.md`.
- Prev: [PR 3.5](./pr-3.5-loader-truncate.md) ·
  Next: [PR 3.7](./pr-3.7-loader-max-applied-lsn-guard.md) · [Roadmap](../README.md)
