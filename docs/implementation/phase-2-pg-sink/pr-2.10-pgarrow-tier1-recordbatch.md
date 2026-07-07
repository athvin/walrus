# PR 2.10 — Tier-1 `TupleValue` → Arrow builders → `RecordBatch`

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/30

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow`, `common` ·
> **Est. size:** M · **Depends on:** PR 2.9 · **Unlocks:** PR 2.11

With the schema in hand, this PR fills it with data. It turns decoded `TupleValue`s plus a `SinkMeta`
into an Arrow `RecordBatch` via typed column builders — parsing canonical Postgres text into the right
Arrow representation, mapping `Null` and `UnchangedToast` onto the validity bitmap, and serializing the
provenance into the `walrus_pg_sink_meta` column. This is the "rows in, columns out" heart of the sink.

## Why — learning objectives

By the end of this PR you will have practised:

- **Arrow builders & `make_builder`** — `Box<dyn ArrayBuilder>` and the downcast dance to append typed values.
- **Text → typed parsing** — pgoutput ships values as canonical *text*; you parse `"42"` → `Int32`, `"t"` → `bool`, etc.
- **The validity bitmap** — NULL vs a present value; and why an **unchanged-TOAST** placeholder also appends
  a null here while its column name is recorded in `SinkMeta`.
- **A stateful builder API** — `append_row` / `finish`, keeping per-column builders in lockstep.
- **`thiserror` error context** — attributing a parse failure to a specific column.

## Read first

- [`../../walrus-pg-sink.md` §2](../../walrus-pg-sink.md#2-data-type-conversion-postgres--arrow--parquet--duckdb)
  — Arrow is the single intermediate representation; the sink builds a `RecordBatch` then writes Parquet.
- [`../../walrus-pg-sink.md` §2.7](../../walrus-pg-sink.md#27-special-values-nulls-and-the-gotchas-we-inherit)
  — NULL (`'n'`) vs unchanged-TOAST (`'u'`): the validity bitmap encodes NULL; the TOAST sentinel is carried
  and resolved by the loader — a transform concern, **recorded in meta** here.
- `common::TupleValue` (`Null | UnchangedToast | Text | Binary`) and `common::SinkMeta` (PR 1.1/1.2).

## Scope

**In scope**

- `BatchBuilder` over a `PgRelation`: `new`, `append_row(values, &SinkMeta)`, `len`/`is_empty`, `finish() -> RecordBatch`.
- Value parsing for the Tier-1 types from PR 2.9 (bool, ints, floats, decimal, utf8, binary, date/time/timestamp(tz), json).
- `Null` and `UnchangedToast` → append a null on the validity bitmap; `UnchangedToast` column names come from
  `SinkMeta.unchanged_toast` (populated upstream — do **not** invent it here).
- Populate `walrus_pg_sink_meta` with `serde_json::to_string(&meta)` per row.

**Explicitly deferred** (do *not* build these here)

- Parquet serialization + DuckDB read-back → **PR 2.11**.
- Multi-column append for Tier-2/3 types → **PRs 2.12–2.16** (this builder handles one Arrow column per source column).
- Batching cadence / flush triggers (which live in the *binary*) → **PR 2.23**.

## Files to create / modify

```
crates/pg-to-arrow/Cargo.toml        # + serde_json = "1"   (jiff/chrono via common for temporal parse if needed)
crates/pg-to-arrow/src/batch.rs      # new: BatchBuilder, append_value, make_builders
crates/pg-to-arrow/src/error.rs      # + ValueParse / RowLenMismatch / Downcast variants
crates/pg-to-arrow/src/lib.rs        # + pub mod batch;
```

## Skeleton

```rust
// crates/pg-to-arrow/src/error.rs  (added variants)
#[error("column {column}: cannot parse {value:?} as {data_type}")]
ValueParse { column: String, value: String, data_type: String },
#[error("row has {got} values, relation has {expected} columns")]
RowLenMismatch { expected: usize, got: usize },
#[error("internal: builder downcast failed for column {column}")]
Downcast { column: String },
```

```rust
// crates/pg-to-arrow/src/batch.rs
use arrow::array::{ArrayBuilder, RecordBatch, StringBuilder};
use arrow::datatypes::SchemaRef;
use common::{PgRelation, SinkMeta, TupleValue};
use crate::error::Error;

/// Accumulates decoded rows for ONE relation into a single Arrow `RecordBatch`.
pub struct BatchBuilder {
    schema: SchemaRef,
    builders: Vec<Box<dyn ArrayBuilder>>, // one per source (Tier-1) column, in order
    meta: StringBuilder,                  // the trailing walrus_pg_sink_meta column
    rows: usize,
}

impl BatchBuilder {
    /// Build empty typed builders from the relation's Tier-1 Arrow schema (PR 2.9).
    pub fn new(rel: &PgRelation) -> Result<Self, Error> { todo!() }

    /// Append one decoded tuple + its provenance. `values.len()` must equal the source column count.
    pub fn append_row(&mut self, values: &[TupleValue], meta: &SinkMeta) -> Result<(), Error> { todo!() }

    pub fn len(&self) -> usize { self.rows }
    pub fn is_empty(&self) -> bool { self.rows == 0 }

    /// Finish all builders into arrays and assemble the `RecordBatch` (schema-checked).
    pub fn finish(self) -> Result<RecordBatch, Error> { todo!() }
}

/// Append one `TupleValue` to one typed builder. `Null`/`UnchangedToast` → `append_null`.
fn append_value(
    builder: &mut dyn ArrayBuilder,
    field: &arrow::datatypes::Field,
    value: &TupleValue,
) -> Result<(), Error> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn builds_a_batch_from_an_orders_insert() { todo!() }
    #[test] fn null_value_sets_validity_false() { todo!() }
    #[test] fn unchanged_toast_appends_null_and_is_listed_in_meta() { todo!() }
    #[test] fn wrong_arity_row_is_rejected() { todo!() }
    #[test] fn meta_column_holds_serialized_sink_meta_json() { todo!() }
    #[test] fn bad_int_text_reports_the_column_name() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `BatchBuilder::new` + repeated `append_row` + `finish` yields a `RecordBatch` whose column count =
      source columns + 1, and whose schema equals `schema::build_schema(rel)`.
- [x] An `orders` insert (`id=42, amount=19.99, created_at=…, note='hi'`) round-trips into the batch with the
      right typed arrays (assert via `as_primitive`/`as_string` downcasts).
- [x] `TupleValue::Null` and `TupleValue::UnchangedToast` both set validity false; a test asserts the TOAST
      column appears in the row's `SinkMeta.unchanged_toast` (populated upstream, echoed into the meta JSON).
- [x] A row with the wrong number of values fails with `Error::RowLenMismatch`; a non-numeric `"abc"` for an
      `Int32` column fails with `Error::ValueParse { column: "id", .. }` naming the column.
- [x] The `walrus_pg_sink_meta` value equals `serde_json::to_string(&meta)` for that row.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-to-arrow` (and `--workspace` stays green)

## Hints & gotchas

- `arrow::array::make_builder(&DataType, capacity)` gives you `Box<dyn ArrayBuilder>`; append via
  `builder.as_any_mut().downcast_mut::<Int32Builder>()` — a failed downcast is a *program* bug, map it to
  `Error::Downcast`, not `ValueParse`.
- Keep **all** builders (including `meta`) in lockstep: every `append_row` must push exactly one slot to
  every column, else `finish` produces ragged arrays. Append the null *and* still advance every builder.
- Decimal128 wants the unscaled `i128` at the field's scale — parse `"19.99"` against `Decimal128(10,2)`
  carefully (scale, sign, rounding is out-of-scope: reject values that don't fit the declared scale).
- Temporal text is canonical Postgres (`'2024-01-02 03:04:05.678901+00'`); convert to micros since the
  Arrow epoch. For `timestamptz`, values are already UTC-normalized upstream — store the micros, keep the `"UTC"` tz.
- Don't try to *resolve* unchanged-TOAST here — that's the loader's back-scan (PR 3.6). Your only job is
  append-null + trust the `SinkMeta.unchanged_toast` list the decoder produced.

## References

- Design: [`../../walrus-pg-sink.md` §2](../../walrus-pg-sink.md#2-data-type-conversion-postgres--arrow--parquet--duckdb),
  [§2.7](../../walrus-pg-sink.md#27-special-values-nulls-and-the-gotchas-we-inherit).
- Prev: [PR 2.9](./pr-2.9-pgarrow-tier1-schema.md) · Next: [PR 2.11](./pr-2.11-pgarrow-parquet-duckdb-conformance.md) · [Roadmap](../README.md)
