# PR 4.2 — End-to-end type round-trip matrix + unchanged-TOAST carry-forward

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `tests/e2e` · **Est. size:** M ·
> **Depends on:** PR 4.1 · **Unlocks:** PR 4.3

Extends the thin slice into a **fidelity gauntlet**: one wide source table exercising every mapped type —
`numeric`, `jsonb`, arrays, `uuid`, `timestamptz`, `bytea`, NULLs, `interval`, `range` — plus an
**unchanged-TOAST** update, driven through the *live* pipeline and asserted for round-trip fidelity in the
`orders`-style mirror. It proves the `architecture.md` **"Types"** and **"Intra-batch TOAST
carry-forward"** verification bullets against real Postgres → Arrow → Parquet → DuckDB, not a unit fixture.

## Why — learning objectives

By the end of this PR you will have practised:

- **End-to-end type fidelity** — confirming the Tier-1/2/3 mappings proven in the pg-to-arrow conformance
  tests (PRs 2.11–2.16) survive the *full* transport, including DuckDB's `typeof`.
- **The unchanged-TOAST carry-forward through the transform** — a big TOASTed value written, then updated
  without touching that column, must land in the mirror as the *old* value, not NULL (loader §5.6).
- **Table-driven e2e assertions** — one `(setup, value, expected_typeof, expected_value)` matrix row per
  type, so a single failing type is named, not hidden in a wall of output.

## Read first

- `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` — the **"Types"** bullet
  (every mapped type incl. NULLs) and the **"Intra-batch TOAST carry-forward"** bullet (mirror ends with
  the old big value, not NULL).
- `../../walrus-pg-sink.md#2-data-type-conversion-postgres--arrow--parquet--duckdb` — the type table this
  matrix must cover; reuse the conformance expectations from PRs 2.11–2.16.
- `../../walrus-loader.md` §5.6 — unchanged-TOAST resolution by back-scanning `<table>_raw` from the
  winner's `(commit_lsn, lsn)`.

## Scope

**In scope**

- A `types_matrix` table added to the source schema (migration or test setup) with one column per mapped
  type family, plus a `big text` column marked for TOAST (long value + `STORAGE EXTENDED`).
- A table-driven test asserting, for each type: the mirror value round-trips **and** DuckDB `typeof`
  matches the expected physical type (timestamptz vs timestamp, Decimal(p,s), MICROS, `uuid`, etc.).
- An unchanged-TOAST test: `INSERT big='X…'` then `UPDATE other_col=…` (big untouched, replica identity
  yields the TOAST sentinel); assert the mirror keeps `big='X…'`, never NULL.
- NULL coverage: a row with every nullable column NULL round-trips as NULL (validity, not empty string).

**Explicitly deferred** (do *not* build these here)

- The Tier-2 geometric / multirange corners already asserted in unit conformance (PRs 2.13–2.14) stay
  there; e2e covers the representative set, not every corner.
- Large-txn behaviour → **PR 4.3**. DDL type evolution round-trip is exercised by loader PRs 3.8–3.9.

## Files to create / modify

```
tests/e2e/tests/type_matrix.rs       # new — table-driven fidelity + typeof asserts
tests/e2e/tests/unchanged_toast.rs   # new — TOAST carry-forward through the transform
migrations/source/000X_types_matrix.sql   # new (or inline setup) — the wide test table
# no new deps
```

## Skeleton

```rust
// tests/e2e/tests/type_matrix.rs
#![cfg(feature = "it")]
use e2e::Harness;

/// One row of the fidelity matrix.
struct TypeCase {
    column: &'static str,
    insert_sql_value: &'static str,   // e.g. "'12345.6789'::numeric(10,4)"
    expected_typeof: &'static str,    // DuckDB typeof, e.g. "DECIMAL(10,4)"
    expect_value: fn(&duckdb::types::Value) -> bool,
}

fn matrix() -> Vec<TypeCase> { todo!() /* numeric, jsonb, array, uuid, timestamptz, bytea, interval, range, NULLs */ }

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn every_mapped_type_round_trips_with_correct_typeof() {
    // For each case: INSERT into types_matrix, await transform, assert
    // typeof(<col>) == expected_typeof AND the value survives.
    todo!()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn all_nulls_round_trip_as_null() { todo!() }
```

```rust
// tests/e2e/tests/unchanged_toast.rs
#![cfg(feature = "it")]
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn unchanged_toast_update_keeps_old_big_value() {
    // INSERT big='X'*100000 ; UPDATE a small column only (big stays TOASTed → sentinel in WAL).
    // await transform; assert mirror.big == 'X'*100000, NOT NULL.
    todo!()
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Every matrix type round-trips to the mirror with the **expected DuckDB `typeof`** and value
      (`numeric`→`DECIMAL(p,s)`, `timestamptz`→`TIMESTAMP WITH TIME ZONE`/MICROS, `uuid`→`UUID`, arrays,
      `jsonb`, `bytea`, `interval`, `range`).
- [ ] An all-NULL row round-trips with every column NULL (not empty string / not 0).
- [ ] The unchanged-TOAST update leaves the mirror's big column at the **old value**, resolved by the
      raw back-scan — never NULL.
- [ ] Failures name the offending type (table-driven, one assert per case).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `docker compose up --wait` then `cargo test -p e2e --features it -- --ignored` asserting
        **`every_mapped_type_round_trips_with_correct_typeof`**, **`all_nulls_round_trip_as_null`**, and
        **`unchanged_toast_update_keeps_old_big_value`**.

## Hints & gotchas

- To force a real TOAST, the big value must exceed the ~2KB TOAST threshold **and** the column storage
  must allow out-of-line storage; a 100 KB string is unambiguous. With `REPLICA IDENTITY DEFAULT`, an
  update that doesn't change `big` sends the **unchanged-TOAST sentinel** — that is the whole point.
- `typeof` is the load-bearing assertion: a value can look right while the physical type is wrong (e.g.
  numeric silently coerced to DOUBLE for `p>38`). Assert both, mirroring the PR 2.11 conformance harness.
- `timestamptz` must come back MICROS with `WITH TIME ZONE`; a MILLIS or naive timestamp is a bug in the
  Arrow schema, not the test.
- Reuse the exact expected values from the pg-to-arrow conformance tests — do not invent new expectations
  here or the two layers can silently disagree.

## References

- Design: `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` ("Types",
  "Intra-batch TOAST carry-forward"); `../../walrus-pg-sink.md#2-data-type-conversion-postgres--arrow--parquet--duckdb`;
  `../../walrus-loader.md` §5.6.
- Prev: [PR 4.1](./pr-4.1-e2e-thin-slice.md) · Next: [PR 4.3](./pr-4.3-e2e-large-txn-streaming.md) ·
  [Roadmap](../README.md)
