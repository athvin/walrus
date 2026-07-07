# PR 2.13 — Tier-2: `range` (5 flat columns) + `multirange` (`LIST<STRUCT>`)

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow` · **Est. size:** L ·
> **Depends on:** PR 2.12 · **Unlocks:** PR 2.14

DuckDB has no range type, so a Postgres range can never be a single mirror column. This PR emits each range
as **five flat sibling columns** (`_lower`, `_upper`, `_lower_inc`, `_upper_inc`, `_empty`) — from which a
range is losslessly reconstructable — and each multirange as a **`LIST<STRUCT>`** of members. The subtle
work is the encoding rules: NULL vs empty vs unbounded must stay three distinct states.

## Why — learning objectives

By the end of this PR you will have practised:

- **Encoding a sum-of-states losslessly** — distinguishing whole-column NULL, `empty`, and `unbounded`
  bounds with a uniform five-flag scheme.
- **Element-type dispatch** — the `_lower`/`_upper` Arrow type depends on the range *family*
  (`int4range → INT32`, `tstzrange → TIMESTAMPTZ(UTC)`, …).
- **Nested Arrow builders** — `ListBuilder<StructBuilder>` for multirange members.
- **Parsing Postgres range/multirange literals** — `[1,10)`, `empty`, `(,5]`, `{[1,4),[7,9)}`.

## Read first

- [`../../walrus-pg-sink.md` §2.4 range](../../walrus-pg-sink.md#24-tier-2-decompositions-column-by-column)
  — the five columns, the element-type-per-family table, and the uniform NULL/empty/unbounded encoding rules
  (`lower_inf`/`upper_inf` are *derived*, not stored).
- [`../../walrus-pg-sink.md` §2.4 multirange](../../walrus-pg-sink.md#24-tier-2-decompositions-column-by-column)
  — `LIST(STRUCT(lower, upper, lower_inc, upper_inc))`; empty multirange = empty list, distinct from NULL list.

## Scope

**In scope**

- `range` families → 5 fields: `<c>_lower <elem>`, `<c>_upper <elem>`, `<c>_lower_inc BOOL`, `<c>_upper_inc BOOL`,
  `<c>_empty BOOL`, with element type by family (int4/int8/num/ts/tstz/date).
- `multirange` families → `<c> LIST(STRUCT(lower, upper, lower_inc, upper_inc))`.
- Encoding rules: whole NULL → all five NULL; empty → `_empty=true` + bounds NULL; unbounded side → that bound
  NULL with `_empty=false`.
- `parse_range` / `parse_multirange` helpers; conformance cases (empty / unbounded / discrete `[)` canonical).

**Explicitly deferred** (do *not* build these here)

- `numrange` with unconstrained numeric element → carry `_lower`/`_upper` as `VARCHAR` per **PR 2.15** rules
  (wire the fallback here, prove it there).
- Geometric types → **PR 2.14**; descriptor `emit[]`/`recombine` for ranges → **PR 2.17**.
- Loader-side range reconstruction → loader phase.

## Files to create / modify

```
crates/pg-to-arrow/src/tier2.rs      # + range_fields(name, family), multirange_field(name, family)
crates/pg-to-arrow/src/range.rs      # new: RangeFamily, ParsedRange, parse_range, parse_multirange, elem_data_type
crates/pg-to-arrow/src/schema.rs     # emit_fields: dispatch range/multirange families
crates/pg-to-arrow/src/batch.rs      # append: range -> 5 builders; multirange -> ListBuilder<StructBuilder>
crates/pg-to-arrow/src/oids.rs       # + range + multirange OIDs (int4range 3904 … datemultirange 4535)
crates/pg-to-arrow/tests/conformance.rs # + empty / unbounded / discrete-canonical / multirange list cases
```

## Skeleton

```rust
// crates/pg-to-arrow/src/range.rs
use arrow::datatypes::DataType;

/// The six built-in range/multirange families (element type + canonicalization differ per family).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFamily { Int4, Int8, Num, Ts, TsTz, Date }

impl RangeFamily {
    pub fn from_range_oid(oid: u32) -> Option<Self> { todo!() }
    pub fn from_multirange_oid(oid: u32) -> Option<Self> { todo!() }
    /// Arrow element type for `_lower`/`_upper` (num-unconstrained falls back to Utf8, PR 2.15).
    pub fn elem_data_type(self, atttypmod: i32) -> DataType { todo!() }
}

/// One parsed range: `None` bound = unbounded on that side (unless `empty`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRange {
    pub empty: bool,
    pub lower: Option<String>, pub upper: Option<String>,
    pub lower_inc: bool, pub upper_inc: bool,
}

/// Parse `[1,10)` / `empty` / `(,5]` / `[2024-01-01,)` into a `ParsedRange`.
pub fn parse_range(text: &str) -> Result<ParsedRange, crate::error::Error> { todo!() }

/// Parse `{[1,4),[7,9)}` (and `{}`) into member ranges (members are non-empty, non-null).
pub fn parse_multirange(text: &str) -> Result<Vec<ParsedRange>, crate::error::Error> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn empty_sets_empty_true_and_bounds_null() { todo!() }
    #[test] fn unbounded_lower_is_null_with_empty_false() { todo!() }        // distinct from empty
    #[test] fn discrete_int4range_canonicalizes_to_half_open() { todo!() }   // [1,10) form
    #[test] fn multirange_parses_members_and_empty_list() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `emit_fields` returns the 5 correctly-typed range columns (element type per family) and a single
      `LIST(STRUCT(...))` field for multiranges.
- [ ] Encoding proven distinct: whole-column NULL → all five NULL; `empty` → `_empty=true` + both bounds NULL;
      unbounded-lower → `_lower` NULL with `_empty=false`.
- [ ] `int4range`/`int8range`/`daterange` canonicalize to `[)` and still emit all five flags uniformly;
      continuous `numrange`/`tsrange`/`tstzrange` preserve arbitrary inclusivity.
- [ ] `multirange` round-trips member count/order; empty multirange = empty list ≠ NULL list.
- [ ] Conformance: read_parquet reproduces bounds, inclusivity, and `_empty` for each case.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-to-arrow` and `cargo test -p pg-to-arrow --features conformance`

## Hints & gotchas

- **Three states, not two.** `NULL` (whole column), `empty` (`isempty` true), and `unbounded` (a `NULL` bound
  with `_empty=false`) are all different — the whole reason `_empty` is a separate flag. Do not conflate an
  unbounded bound with a NULL range.
- `lower_inf`/`upper_inf` are **derivable** (`bound IS NULL AND NOT _empty`) — do *not* add columns for them.
- Populate from the semantics of `lower()`/`upper()`/`lower_inc()`/`upper_inc()`/`isempty()`; your parser just
  reconstructs those five values from the wire literal.
- `ListBuilder<StructBuilder>` needs the child struct fields declared up front; append members then
  `list.append(true)` per row (empty list = zero appends + `append(true)`; NULL row = `append(false)`).
- Wire the unconstrained-`numrange` element as `Utf8` now (a single match arm) but leave *proving* it to PR 2.15
  so this PR stays focused.

## References

- Design: [`../../walrus-pg-sink.md` §2.4](../../walrus-pg-sink.md#24-tier-2-decompositions-column-by-column).
- Prev: [PR 2.12](./pr-2.12-pgarrow-interval-timetz.md) · Next: [PR 2.14](./pr-2.14-pgarrow-geometric.md) · [Roadmap](../README.md)
