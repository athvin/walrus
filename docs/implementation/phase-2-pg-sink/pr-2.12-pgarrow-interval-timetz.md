# PR 2.12 — Tier-2: `interval` (3 columns) + `timetz` (2 columns)

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow` · **Est. size:** M ·
> **Depends on:** PR 2.11 · **Unlocks:** PR 2.13

The first Tier-2 PR, and the one that **generalizes the mapping from "one source column → one Arrow field"
to "one source column → one-or-more fields."** Postgres `interval` and `timetz` each carry more than any
single Arrow/Parquet scalar can hold, so the sink emits them as multiple sibling columns the loader
recombines: `interval` → 3 signed integers, `timetz` → micros + offset. This PR introduces the
column-expansion seam that every remaining type PR (2.13–2.16) plugs into.

## Why — learning objectives

By the end of this PR you will have practised:

- **Widening an abstraction without breaking callers** — turning `tier1_field -> Field` into `emit_fields ->
  Vec<Field>` and threading the 1→N fan-out through schema build *and* batch append.
- **Lossless decomposition** — why `interval` must stay `(months, days, micros)` and never collapse to one int64.
- **Canonical-text parsing** — splitting `'1 year 2 mons 3 days 04:05:06.5'` into the three fields.
- **Timezone-of-a-time modelling** — `timetz` as micros-since-midnight + UTC offset seconds, with a
  round-trip-pinned sign convention.

## Read first

- [`../../walrus-pg-sink.md` §2.4 `interval`](../../walrus-pg-sink.md#24-tier-2-decompositions-column-by-column)
  — the three-field rationale, the rejected alternatives (arrow `Interval(MonthDayNano)` *errors*), and the
  **never-a-join-key** caveat (DuckDB normalizes intervals for equality/ordering).
- [`../../walrus-pg-sink.md` §2.4 `timetz`](../../walrus-pg-sink.md#24-tier-2-decompositions-column-by-column)
  — Arrow has no tz-aware time; carry `_micros` + `_offset_seconds`; don't drop the zone the way DMS does.

## Scope

**In scope**

- Introduce `emit_fields(col: &PgColumn) -> Result<Vec<Field>>` (Tier-1 returns a 1-element vec) and route
  `build_schema` through it; extend `BatchBuilder` so one `TupleValue` may fan out to several builders.
- `interval` (OID 1186) → `<c>_months INT32`, `<c>_days INT32`, `<c>_micros INT64` (all NULL ⇔ source NULL).
- `timetz` (OID 1266) → `<c>_micros INT64`, `<c>_offset_seconds INT32`.
- `parse_interval(&str) -> (i32,i32,i64)` and `parse_timetz(&str) -> (i64,i32)` helpers.
- Conformance cases appended to `tests/conformance.rs`.

**Explicitly deferred** (do *not* build these here)

- `range`/`multirange` → **PR 2.13**; geometric → **PR 2.14**.
- The loader-side recombine SQL (`to_months + to_days + to_microseconds`, TIMETZ rebuild) → loader phase (PR 3.x).
- Recording the decomposition in the `TypeDescriptor` (`emit[]`, `recombine`) → **PR 2.17**.

## Files to create / modify

```
crates/pg-to-arrow/src/schema.rs     # tier1_field -> emit_fields (Vec<Field>); build_schema routes through it
crates/pg-to-arrow/src/tier2.rs      # new: interval_fields, timetz_fields, parse_interval, parse_timetz
crates/pg-to-arrow/src/batch.rs      # append: interval -> 3 builders, timetz -> 2 builders; shared NULL flag
crates/pg-to-arrow/src/oids.rs       # + INTERVAL = 1186; TIMETZ = 1266
crates/pg-to-arrow/tests/conformance.rs # + interval 3-col rebuild, timetz offset-sign cases
```

## Skeleton

```rust
// crates/pg-to-arrow/src/schema.rs  (generalized seam)
/// One source column → the Arrow field(s) the sink emits for it. Tier-1 = one field; Tier-2 = several.
pub fn emit_fields(col: &PgColumn) -> Result<Vec<Field>, Error> { todo!() }
```

```rust
// crates/pg-to-arrow/src/tier2.rs
use arrow::datatypes::Field;

/// `<c>_months INT32`, `<c>_days INT32`, `<c>_micros INT64` — Postgres' un-normalized three-field interval.
pub fn interval_fields(name: &str) -> Vec<Field> { todo!() }

/// `<c>_micros BIGINT` (µs since midnight), `<c>_offset_seconds INTEGER` (UTC offset, sign pinned by test).
pub fn timetz_fields(name: &str) -> Vec<Field> { todo!() }

/// Parse canonical interval text (`"1 year 2 mons 3 days 04:05:06.5"`) into `(months, days, micros)`.
pub fn parse_interval(text: &str) -> Result<(i32, i32, i64), crate::error::Error> { todo!() }

/// Parse canonical `timetz` text (`"12:34:56.789+05:30"`) into `(micros_since_midnight, offset_seconds)`.
pub fn parse_timetz(text: &str) -> Result<(i64, i32), crate::error::Error> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn interval_years_months_days_time_split() { todo!() }
    #[test] fn interval_1_month_ne_30_days_ne_720_hours() { todo!() } // three fields stay independent
    #[test] fn timetz_positive_and_negative_offsets() { todo!() }
    #[test] fn interval_null_maps_all_three_columns_null() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `emit_fields` returns 3 fields for `interval` and 2 for `timetz`, named `<c>_months/_days/_micros` and
      `<c>_micros/_offset_seconds`; Tier-1 columns still return exactly one field (no regression to PR 2.9 tests).
- [ ] `BatchBuilder` appends an `interval` value to all three builders and a `timetz` value to both; a source
      `NULL` sets all sibling columns null in the same row.
- [ ] `parse_interval` keeps `'1 mon'`, `'30 days'`, `'720 hours'` as *distinct* `(1,0,0)`/`(0,30,0)`/`(0,0,…)`.
- [ ] Conformance: interval rebuilds to the right DuckDB `INTERVAL` (`to_months+to_days+to_microseconds`), and
      `timetz` offset sign round-trips (the test *pins* the convention).
- [ ] A comment on the interval column records the never-a-join-key caveat.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-to-arrow` and `cargo test -p pg-to-arrow --features conformance`

## Hints & gotchas

- **Do not** reach for `arrow`'s `Interval(MonthDayNano)` — arrow-rs *errors* writing it to Parquet (§2.4).
  Three plain integer columns are the whole point.
- The three interval fields share one logical NULL: model "all three NULL ⇔ source was NULL" and assert it,
  because `(0,0,0)` (a real zero interval) must be distinguishable from NULL.
- Pin the `timetz` offset sign with a test using a known value — Postgres text `+05:30` vs a stored
  `offset_seconds`; get it wrong and the loader's TIMETZ rebuild is silently shifted.
- When you widen `tier1_field` → `emit_fields`, update the PR 2.9 tests to compare against the flattened
  `Vec<Field>` (Tier-1 = length 1), rather than duplicating logic.
- Keep the source-column → emitted-column ordering stable and documented; PR 2.17's descriptor `emit[]` must
  list the same suffixes in the same order.

## References

- Design: [`../../walrus-pg-sink.md` §2.4](../../walrus-pg-sink.md#24-tier-2-decompositions-column-by-column).
- Prev: [PR 2.11](./pr-2.11-pgarrow-parquet-duckdb-conformance.md) · Next: [PR 2.13](./pr-2.13-pgarrow-range-multirange.md) · [Roadmap](../README.md)
