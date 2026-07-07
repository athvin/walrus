<!--
  Task file — follows ../TEMPLATE.md. Spec + skeleton only; the learner writes the logic.
-->

# PR 1.2 — Add the Postgres shape types (the decoupling seam)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/16

> **Phase:** 1 — Shared core · **Crates touched:** `common` · **Est. size:** M ·
> **Depends on:** PR 1.1 · **Unlocks:** PR 1.6, PR 2.3, PR 2.9

This PR defines the **neutral value types** that sit between the decoder and everything downstream:
`PgRelation`, `PgColumn`, `ReplicaIdentity`, `TupleValue`, and `TypeDescriptor`. They live in
`common` on purpose — the pgoutput decoder (in `pg-sink`) *produces* them, `pg-to-arrow` *consumes*
them, `control` *persists* the descriptor, and `loader` *reads it back* to rebuild types. That single
decision is why `pg-to-arrow` is fully unit-testable without the decoder, and why no crate ever has to
depend on a binary. Get these shapes right and three later crates fall into place.

## Why — learning objectives

By the end of this PR you will have practised:

- **Designing a decoupling seam** — plain data types that let two subsystems evolve independently.
- **Enums that encode a protocol distinction** — `TupleValue::{Null, UnchangedToast, Text, Binary}` is
  the whole NULL-vs-TOAST-vs-value story from the wire, made a type the compiler can check.
- **`bytes::Bytes` for zero-copy binary** — `Binary(Bytes)` carries `'b'`-format column data without a
  needless `Vec<u8>` copy.
- **Modelling a numeric typmod** — `atttypmod` decodes to `numeric(p, s)`; you capture the raw modifier
  now so the type mapper can interpret it later.

## Read first

- `../../proto-version.md` §4 "The message catalog" — the Relation `'R'` layout: replica-identity char,
  and per column `Int8 flags` (bit 1 = key), name, `Int32 type OID`, `Int32 atttypmod`.
- `../../proto-version.md` §5 "TupleData and the unchanged-TOAST placeholder" — the `n`/`u`/`t`/`b`
  column formats that map one-to-one to `TupleValue`.
- `../../walrus-pg-sink.md` §2.6 "The type-mapping descriptor" — the `TypeDescriptor` JSON shape
  (`column`, `pg_type_oid`, `pg_type`, `tier`, `arrow`, `duckdb`, `emit[]`, `recombine`, `meta{}`).
- `../../examples/proto-version/decode_pgoutput.py` — the Python reference that already constructs these
  same shapes; mirror its field set.

## Scope

**In scope**

- `ReplicaIdentity` (`Default | Nothing | Full | Index`) from the `relreplident` char.
- `PgColumn` (name, `type_oid`, `type_modifier`, `is_key`) and `PgRelation` (oid, schema, name,
  replica identity, columns).
- `TupleValue` (`Null | UnchangedToast | Text(String) | Binary(Bytes)`).
- `TypeDescriptor` + its nested `meta` — the §2.6 descriptor, serde-typed, keyed later by `schema_version`.
- Construction + serde unit tests.

**Explicitly deferred** (do *not* build these here)

- Decoding *bytes* into these types → **PR 2.3 / 2.4** (pgoutput decoder).
- Turning `PgRelation` into an Arrow schema → **PR 2.9**; filling `TypeDescriptor.tier/emit/recombine`
  for every type → **PRs 2.9–2.17**.
- Persisting `TypeDescriptor` to `schema_registry` → **PR 1.6 / 2.17**.

## Files to create / modify

```
crates/common/Cargo.toml          # + bytes = "1"   (serde already added in 1.1)
crates/common/src/pg_shape.rs     # new — PgRelation, PgColumn, ReplicaIdentity, TupleValue
crates/common/src/type_descriptor.rs  # new — TypeDescriptor + TypeMeta + Tier
crates/common/src/lib.rs          # + pub mod pg_shape;  pub mod type_descriptor;  + re-exports
```

## Skeleton

```rust
// crates/common/src/pg_shape.rs
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Postgres `relreplident` — governs which old-image columns Update/Delete carry (proto §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    Default, // 'd' — key columns only
    Nothing, // 'n'
    Full,    // 'f' — the whole old row
    Index,   // 'i'
}

impl ReplicaIdentity {
    /// From the Relation message's `relreplident` byte.
    pub fn from_wire(c: u8) -> Result<Self, crate::Error> { todo!() }
}

/// One column of a relation, as seen in a Relation `'R'` message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgColumn {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32, // atttypmod; -1 = no modifier. numeric → (precision, scale)
    pub is_key: bool,       // flags bit 1
}

impl PgColumn {
    /// Decode `numeric(p, s)` from `type_modifier` when this column is a numeric; None otherwise / unconstrained.
    pub fn numeric_precision_scale(&self) -> Option<(u16, u16)> { todo!() }
}

/// The shape of a source table at one `schema_version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgRelation {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<PgColumn>,
}

impl PgRelation {
    /// Ordered key-column names (is_key) — the loader's MERGE/dedup key list.
    pub fn key_columns(&self) -> Vec<&str> { todo!() }
}

/// One column value inside a TupleData (proto §5). NOTE: `Null` (`'n'`) and `UnchangedToast` (`'u'`)
/// are DISTINCT — the loader resolves TOAST by back-scan; it must never be collapsed to NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    UnchangedToast,
    Text(String),   // 't' — textual representation
    Binary(Bytes),  // 'b' — binary representation
}
```

```rust
// crates/common/src/type_descriptor.rs
use serde::{Deserialize, Serialize};

/// The three-tier mapping model (walrus-pg-sink.md §2.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tier { One, Two, Three } // serialize as 1 | 2 | 3

/// Metadata Parquet/DuckDB lose on read; the loader re-applies it (§2.6).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeMeta {
    pub enum_labels: Option<Vec<String>>,     // ordered label set for enum
    pub bit_length: Option<u32>,              // n for bit(n)/varbit(n)
    pub char_length: Option<u32>,             // n + bpchar padding for char(n)/varchar(n)
    pub money_fraction_digits: Option<u32>,   // lc_monetary fractional digits
}

/// Per-column mapping descriptor written to `schema_registry` (§2.6). Makes "reconcile to exact
/// source shape" mechanical rather than a guess.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeDescriptor {
    pub column: String,
    pub pg_type_oid: u32,
    pub pg_type: String,
    pub tier: Tier,
    pub arrow: String,       // e.g. "Struct/Decomposed"
    pub duckdb: String,      // e.g. "INTERVAL"
    pub emit: Vec<String>,   // e.g. ["duration_months:INT32", …]  (flat cols this type expands to)
    pub recombine: Option<String>, // loader-side recombine expression; None for tier-1 scalars
    pub meta: TypeMeta,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_identity_from_wire_char() { todo!() }

    #[test]
    fn numeric_typmod_decodes_precision_and_scale() { todo!() /* 655366 → (10, 2) */ }

    #[test]
    fn key_columns_preserve_relation_order() { todo!() }

    #[test]
    fn tuple_value_null_and_unchanged_toast_are_distinct() { todo!() }

    #[test]
    fn type_descriptor_round_trips_the_docs_example() { todo!() /* the §2.6 interval descriptor */ }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `ReplicaIdentity::from_wire` maps `d/n/f/i` correctly and errors on anything else.
- [x] `PgColumn::numeric_precision_scale()` decodes the §4 example (`atttypmod 655366 → (10, 2)`).
- [x] `PgRelation::key_columns()` returns key columns **in relation order** (composite-PK safe).
- [x] `TupleValue` keeps `Null` and `UnchangedToast` as distinct variants; `Binary` uses `bytes::Bytes`.
- [x] `TypeDescriptor` serde-round-trips the §2.6 interval example (keys, `tier` as `2`, nested `meta`).
- [x] Docs/comments state that these types are the seam: decoder produces, pg-to-arrow/control/loader consume.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p common` (and `--workspace` stays green)

## Hints & gotchas

- **`atttypmod` for numeric is `(mod - 4)` unpacked:** precision = `((mod - 4) >> 16) & 0xFFFF`,
  scale = `(mod - 4) & 0xFFFF`. `-1` means unconstrained — return `None`, don't panic. This is the exact
  math PR 2.3 will lean on; encode it once, here.
- **Don't derive `Serialize` on `TupleValue` yet** — it has no stable JSON contract (it's an in-memory
  wire value, not a persisted document). Keep it `PartialEq` for tests; add serde only if a later PR needs it.
- **`Tier` must serialize as the integers `1/2/3`**, matching the `"tier": 2` in the §2.6 JSON — use
  `#[serde(rename = "1")]`-style or a manual impl, and test it.
- The design is emphatic that **NULL ≠ unchanged-TOAST** — a whole loader correctness story (PR 3.6)
  depends on this distinction surviving from wire to `<table>_raw`. Your enum is where it starts.
- `PgColumn.type_modifier` is `i32` (can be `-1`); `type_oid` is `u32`. Keep the widths honest — the wire
  is a signed Int32 for the modifier.

## References

- Design: `../../proto-version.md` §4–§5, `../../walrus-pg-sink.md` §2.2 / §2.6,
  `../../examples/proto-version/decode_pgoutput.py`.
- Prev: [PR 1.1](./pr-1.1-common-sink-meta.md) · Next: [PR 1.3](./pr-1.3-control-migrations.md) ·
  [Roadmap](../README.md)
