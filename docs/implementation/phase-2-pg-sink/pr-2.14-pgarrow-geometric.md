# PR 2.14 — Tier-2: geometric types → `STRUCT` / `LIST` of doubles

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/34

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow` · **Est. size:** M ·
> **Depends on:** PR 2.13 · **Unlocks:** PR 2.15

Postgres' native geometric types are all just doubles, so this PR carries them as queryable Arrow
`STRUCT`/`LIST` of doubles — `point → STRUCT(x,y)`, `box → STRUCT(p1,p2)`, `path → STRUCT(is_closed, points)`,
`polygon → LIST(STRUCT(x,y))`, and so on. The one correctness trap is `path`'s **mandatory** `is_closed`
flag (open vs closed paths render differently and dropping it is lossy). PostGIS is out of scope.

## Why — learning objectives

By the end of this PR you will have practised:

- **Nested `StructBuilder` / `ListBuilder<StructBuilder>`** — the deepest Arrow nesting in the crate.
- **Modelling a shape as fields** — mapping each geometric type's parametrization to named double fields.
- **A mandatory discriminator** — carrying `path.is_closed` because `[(…)]` (open) and `((…))` (closed) differ.
- **Parsing Postgres geometric literals** — `(x,y)`, `<(x,y),r>`, `{A,B,C}`, `[(…),(…)]`, `((…),(…),…)`.

## Read first

- [`../../walrus-pg-sink.md` §2.4 geometric types](../../walrus-pg-sink.md#24-tier-2-decompositions-column-by-column)
  — the exact `STRUCT`/`LIST` shapes for point/line/lseg/box/circle/path/polygon, and the `is_closed` mandate.
- [`../../walrus-pg-sink.md` §2.4 PostGIS](../../walrus-pg-sink.md#postgis) — why PostGIS `geometry`/`geography`
  (WKB+SRID) is deferred (DuckDB core `GEOMETRY` is v1.5; the loader pins 1.4.x LTS).

## Scope

**In scope**

- `point → STRUCT(x DOUBLE, y DOUBLE)`; `line → STRUCT(a,b,c)`; `lseg`/`box → STRUCT(p1 STRUCT(x,y), p2 STRUCT(x,y))`;
  `circle → STRUCT(x,y,r)`; `path → STRUCT(is_closed BOOL, points LIST(STRUCT(x,y)))`; `polygon → LIST(STRUCT(x,y))`.
- OID dispatch in `emit_fields`; nested builders in `BatchBuilder`; literal parsers.
- Conformance cases (nested read-back of each shape, incl. an open vs closed path).

**Explicitly deferred** (do *not* build these here)

- PostGIS `geometry`/`geography` (WKB+SRID) → **deferred entirely** until a source needs it / a DuckDB LTS bump.
- Tier-3 VARCHAR carriers → **PR 2.15**; descriptor entries for geometric → **PR 2.17**.

## Files to create / modify

```
crates/pg-to-arrow/src/geometric.rs  # new: geometric_field(name, oid), parsers, Point/Path/... helpers
crates/pg-to-arrow/src/schema.rs     # emit_fields: dispatch geometric OIDs
crates/pg-to-arrow/src/batch.rs      # append: nested Struct/List builders per geometric type
crates/pg-to-arrow/src/oids.rs       # + POINT 600, LSEG 601, PATH 602, BOX 603, POLYGON 604, LINE 628, CIRCLE 718
crates/pg-to-arrow/tests/conformance.rs # + one case per geometric type; open vs closed path
```

## Skeleton

```rust
// crates/pg-to-arrow/src/geometric.rs
use arrow::datatypes::Field;

/// The single emitted Arrow field for a geometric column (a STRUCT or a LIST<STRUCT> of doubles).
pub fn geometric_field(name: &str, type_oid: u32) -> Option<Field> { todo!() }

#[derive(Debug, Clone, Copy, PartialEq)] pub struct Pt { pub x: f64, pub y: f64 }

pub fn parse_point(text: &str) -> Result<Pt, crate::error::Error> { todo!() }        // "(x,y)"
pub fn parse_box(text: &str) -> Result<(Pt, Pt), crate::error::Error> { todo!() }    // "(x1,y1),(x2,y2)"
pub fn parse_circle(text: &str) -> Result<(Pt, f64), crate::error::Error> { todo!() }// "<(x,y),r>"
pub fn parse_line(text: &str) -> Result<(f64, f64, f64), crate::error::Error> { todo!() } // "{A,B,C}"

/// Returns (is_closed, points): `[(...)]` = open, `((...))` = closed — the flag is MANDATORY.
pub fn parse_path(text: &str) -> Result<(bool, Vec<Pt>), crate::error::Error> { todo!() }
pub fn parse_polygon(text: &str) -> Result<Vec<Pt>, crate::error::Error> { todo!() } // "((x,y),...)"

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn point_struct_x_y() { todo!() }
    #[test] fn box_is_two_nested_points() { todo!() }
    #[test] fn path_open_vs_closed_sets_is_closed() { todo!() }   // the lossy trap
    #[test] fn polygon_is_list_of_points() { todo!() }
    #[test] fn circle_carries_radius() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `emit_fields` returns the documented `STRUCT`/`LIST` shape for each of point/line/lseg/box/circle/path/polygon.
- [x] `path` always carries `is_closed`, and a test proves an open path (`[(…)]`) and closed path (`((…))`)
      differ only by that flag.
- [x] Nested builders round-trip: `box.p1.x`, `polygon[0].y`, `path.points[…]` read back correctly in DuckDB.
- [x] PostGIS types are *not* handled here (they map to `NotTier1`/unsupported, deferred by design).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-to-arrow` and `cargo test -p pg-to-arrow --features conformance`

## Hints & gotchas

- **`path.is_closed` is not optional.** Postgres renders a closed path with double parens and an open one with
  brackets; without the flag you cannot tell them apart on read-back — the design calls this out as lossy.
- `StructBuilder` requires you to append to *every* child field for *every* row (append a null-struct by
  appending nulls to all children and `struct.append(false)`), or the child arrays drift out of length lock-step.
- Reuse the `Pt` `STRUCT(x,y)` field definition everywhere (box, path points, polygon) so the nested types
  are literally the same `Field` — DuckDB compares the full nested type on read-back.
- Don't over-engineer parsing: the geometric literal grammar is small and regular; a focused hand parser beats
  pulling in a grammar crate for seven shapes.
- Leave PostGIS a clear `TODO`/`unsupported` arm with a comment pointing at §2.4 PostGIS and the DuckDB-LTS reason.

## References

- Design: [`../../walrus-pg-sink.md` §2.4](../../walrus-pg-sink.md#24-tier-2-decompositions-column-by-column),
  [PostGIS](../../walrus-pg-sink.md#postgis).
- Prev: [PR 2.13](./pr-2.13-pgarrow-range-multirange.md) · Next: [PR 2.15](./pr-2.15-pgarrow-tier3-text-carriers.md) · [Roadmap](../README.md)
