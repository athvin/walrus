# PR 2.9 — Build the Tier-1 Arrow schema from a `PgRelation`

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/29

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow` (new), `common` ·
> **Est. size:** M · **Depends on:** PR 1.2 (shape types), PR 2.8 (decoder complete) · **Unlocks:** PR 2.10

This PR opens the `pg-to-arrow` crate — the second half of the sink. It builds the **Arrow `Schema`**
for a source table from a neutral `PgRelation`, covering only **Tier-1** (native 1:1) columns plus the
trailing `walrus_pg_sink_meta` provenance column. No values yet, no Parquet yet — just the *shape* that
every later PR fills in. Getting the logical types exactly right here (MICROS everywhere, `Decimal128`
for `numeric(p≤38)`, the `timestamptz` vs `timestamp` split) is what makes the whole DuckDB read-back
work in PR 2.11.

## Why — learning objectives

By the end of this PR you will have practised:

- **A fresh library crate in the workspace** — wiring `pg-to-arrow` into the DAG so it depends only on `common`.
- **arrow-rs `DataType`/`Field`/`Schema`** — the difference between a *storage* type and a *logical* type.
- **Total-function mapping tables** — a `match` on Postgres type OIDs that is exhaustive-by-design and
  returns `None` (deferred) rather than guessing for anything not yet handled.
- **Postgres `atttypmod` arithmetic** — decoding a packed `numeric` typmod into `(precision, scale)`.
- **The "narrowest type system wins" rule** — why we pick MICROS and Decimal128 up front.

## Read first

- [`../../walrus-pg-sink.md` §2.1](../../walrus-pg-sink.md#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types)
  — DuckDB reads Parquet-*native* logical types; MICROS-for-all-temporals; arrow-rs defines the bytes.
- [`../../walrus-pg-sink.md` §2.3](../../walrus-pg-sink.md#23-the-full-type-table) — the full type table.
  The Tier-1 rows are exactly this PR's scope; note the two-numeric-cases warning.
- [`../../walrus-pg-sink.md` §2.2](../../walrus-pg-sink.md#22-the-three-tier-model) — the tier model, so
  you know *why* non-Tier-1 OIDs are deferred, not errors-forever.
- `common` `PgRelation`/`PgColumn`/`ReplicaIdentity` (PR 1.2) — the input you consume.

## Scope

**In scope**

- New crate `crates/pg-to-arrow` with a `thiserror` `Error` and a `pg_type_oids` constant module.
- `build_schema(&PgRelation) -> Result<Schema>` producing one Arrow `Field` per Tier-1 column, then the
  trailing `walrus_pg_sink_meta` Utf8 field.
- Tier-1 coverage only: `bool`, `int2/4/8`, `float4/8`, `numeric(p≤38)`→`Decimal128`, `text/varchar/bpchar/char`,
  `bytea`, `json/jsonb`, `date`, `time`→`Time64(µs)`, `timestamp`→`Timestamp(µs,None)`,
  `timestamptz`→`Timestamp(µs,"UTC")`.
- `numeric_precision_scale(atttypmod)` helper (VARHDRSZ math), returning `None` for unconstrained.

**Explicitly deferred** (do *not* build these here)

- `interval`, `timetz` decomposition → **PR 2.12**; `range`/`multirange` → **PR 2.13**; geometric → **PR 2.14**.
- Tier-3 VARCHAR carriers incl. `numeric` p>38 / unconstrained → **PR 2.15**; `uuid`/`enum` → **PR 2.16**.
- Building `RecordBatch`es / appending values → **PR 2.10**. Parquet + DuckDB conformance → **PR 2.11**.
- The per-column `TypeDescriptor` for `schema_registry` → **PR 2.17**.

## Files to create / modify

```
crates/pg-to-arrow/Cargo.toml        # new: arrow = "54"; common = { path = "../common" }; thiserror = "1"
crates/pg-to-arrow/src/lib.rs        # new: pub mod error, oids, schema; re-exports
crates/pg-to-arrow/src/error.rs      # new: Error (thiserror)
crates/pg-to-arrow/src/oids.rs       # new: pg type OID constants
crates/pg-to-arrow/src/schema.rs     # new: build_schema, tier1_field, tier1_data_type, numeric_precision_scale
Cargo.toml                           # + workspace member "crates/pg-to-arrow"
```

## Skeleton

```rust
// crates/pg-to-arrow/src/oids.rs — canonical (pg_catalog) base-type OIDs.
pub const BOOL: u32 = 16;      pub const BYTEA: u32 = 17;   pub const CHAR: u32 = 18;
pub const INT8: u32 = 20;      pub const INT2: u32 = 21;    pub const INT4: u32 = 23;
pub const TEXT: u32 = 25;      pub const JSON: u32 = 114;   pub const FLOAT4: u32 = 700;
pub const FLOAT8: u32 = 701;   pub const BPCHAR: u32 = 1042; pub const VARCHAR: u32 = 1043;
pub const DATE: u32 = 1082;    pub const TIME: u32 = 1083;  pub const TIMESTAMP: u32 = 1114;
pub const TIMESTAMPTZ: u32 = 1184; pub const NUMERIC: u32 = 1700; pub const JSONB: u32 = 3802;
```

```rust
// crates/pg-to-arrow/src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The column's type is real but not handled at this tier yet (Tier-2/3 land in later PRs).
    #[error("type oid {oid} (typmod {typmod}) is not a Tier-1 type")]
    NotTier1 { oid: u32, typmod: i32 },
    #[error("relation {relation} has no columns")]
    EmptyRelation { relation: String },
}
```

```rust
// crates/pg-to-arrow/src/schema.rs
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::{PgColumn, PgRelation};
use crate::error::Error;

/// The trailing provenance column every walrus Parquet file carries (JSON text).
pub const SINK_META_COLUMN: &str = "walrus_pg_sink_meta";

/// Full Arrow schema for `rel`: one field per Tier-1 column, then `walrus_pg_sink_meta` (Utf8, non-null).
pub fn build_schema(rel: &PgRelation) -> Result<Schema, Error> { todo!() }

/// One source column → one Arrow `Field`. Data fields stay `nullable` (old-images/tombstones are partial).
pub fn tier1_field(col: &PgColumn) -> Result<Field, Error> { todo!() }

/// Arrow `DataType` for a Tier-1 OID+typmod, or `None` if the type is not (yet) Tier-1.
pub fn tier1_data_type(type_oid: u32, atttypmod: i32) -> Option<DataType> {
    // e.g. TIMESTAMPTZ => Timestamp(Microsecond, Some("UTC".into())),
    //      TIMESTAMP   => Timestamp(Microsecond, None),
    //      TIME        => Time64(Microsecond), DATE => Date32,
    //      NUMERIC with p<=38 => Decimal128(p, s)  (p>38 / unconstrained is Tier-3, PR 2.15)
    todo!()
}

/// Decode a `numeric` atttypmod into `(precision, scale)`; `None` for unconstrained (-1). VARHDRSZ = 4.
pub fn numeric_precision_scale(atttypmod: i32) -> Option<(u8, i8)> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn orders_relation_maps_to_expected_tier1_schema() { todo!() }
    #[test] fn timestamptz_carries_utc_but_timestamp_does_not() { todo!() }
    #[test] fn numeric_10_2_is_decimal128_10_2() { todo!() }
    #[test] fn numeric_typmod_minus_one_is_unconstrained() { todo!() }
    #[test] fn meta_column_is_last_and_non_null_utf8() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `crates/pg-to-arrow` compiles as a workspace member depending on `common` only (no `pg-sink`/`control` edge).
- [x] A hand-built `orders` `PgRelation` (`id int4` PK, `amount numeric(10,2)`, `created_at timestamptz`,
      `note text`) → `build_schema` yields exactly those four Arrow fields + a trailing `walrus_pg_sink_meta` Utf8.
- [x] `timestamptz` → `Timestamp(Microsecond, Some("UTC"))`; `timestamp` → `Timestamp(Microsecond, None)`;
      `time` → `Time64(Microsecond)` (all **MICROS**, asserted).
- [x] `numeric(10,2)` → `Decimal128(10, 2)`; `numeric` typmod `-1` returns `None` from `numeric_precision_scale`.
- [x] Non-Tier-1 OIDs (e.g. `interval` 1186) return `Error::NotTier1`, not a panic or a wrong field.
- [x] Comments explain the nullable-data / non-null-meta invariant and the two-numeric-cases split.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-to-arrow` (and `--workspace` stays green)

## Hints & gotchas

- **MICROS is not optional.** `NANOS`+timezone silently downgrades in DuckDB and overflows `int64` at
  extreme dates; `MILLIS` truncates. Pin `TimeUnit::Microsecond` for *every* temporal field (§2.1).
- **Keep the two `numeric` cases in separate branches** — the design warns explicitly (§2.3): `p≤38` is a
  lossless `Decimal128`; unconstrained / `p>38` is a Tier-3 `VARCHAR` (PR 2.15). Never fold them together.
- `atttypmod` for `numeric` packs `((p<<16)|s)+4`; subtract VARHDRSZ (4) *before* unpacking. `-1` = unconstrained.
- Arrow's timezone field is `Option<Arc<str>>`; `Some("UTC".into())` is the marker DuckDB reads as `isAdjustedToUTC=true`.
- Return `None`/`NotTier1` for anything unhandled rather than a fallback field — a wrong-but-compiling
  mapping is the bug the conformance tests in PR 2.11 exist to catch, so fail loudly now.
- Data fields are `nullable(true)` because delete old-images and TOAST placeholders arrive partial; the
  mirror's PK-not-null is enforced downstream in the loader, not here.

## References

- Design: [`../../walrus-pg-sink.md` §2.1](../../walrus-pg-sink.md#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types),
  [§2.2](../../walrus-pg-sink.md#22-the-three-tier-model), [§2.3](../../walrus-pg-sink.md#23-the-full-type-table).
- Prev: [PR 2.8](./pr-2.8-pgoutput-two-phase.md) · Next: [PR 2.10](./pr-2.10-pgarrow-tier1-recordbatch.md) · [Roadmap](../README.md)
