# PR 2.15 — Tier-3 canonical-text carriers (numeric>38, bit, inet, tsvector, pg_lsn, xid…)

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow` · **Est. size:** M ·
> **Depends on:** PR 2.14 · **Unlocks:** PR 2.16

Some Postgres types have no lossless structural target in Parquet/DuckDB. This PR carries them as their
**canonical text** in a single `VARCHAR` column — always exact as a string — and defers the lost *type
metadata* (bit length, etc.) to the descriptor. The headline case is `numeric`: `p≤38` stays a Tier-1
`Decimal128`, but **unconstrained / `p>38` must become `VARCHAR`**, because DuckDB's Parquet reader
downcasts any decimal with precision > 38 to `DOUBLE` — so `Decimal256` would silently lose digits.

## Why — learning objectives

By the end of this PR you will have practised:

- **Choosing exactness over structure** — why arbitrary-precision `numeric` is safer as text than as a fixed-scale decimal.
- **Knowing your downstream's ceiling** — DuckDB `DECIMAL` caps at precision 38; its reader downcasts `p>38` to `DOUBLE`.
- **A wide match on system types** — routing `bit`, `inet`, `tsvector`, `pg_lsn`, `xid`, … all to `Utf8`.
- **Keeping two branches of one type distinct** — the Tier-1 vs Tier-3 `numeric` split (the design's explicit warning).

## Read first

- [`../../walrus-pg-sink.md` §2.5](../../walrus-pg-sink.md#25-tier-3-canonical-text-carriers) — the value is
  lossless as text; metadata is re-applied by the descriptor.
- [`../../walrus-pg-sink.md` §2.5 unconstrained/`>38` numeric](../../walrus-pg-sink.md#unconstrained--38-digit-numeric)
  — the locked `VARCHAR` decision and the DuckDB `p>38 → DOUBLE` downcast proof (so *not* Decimal256).
- [`../../walrus-pg-sink.md` §2.3](../../walrus-pg-sink.md#23-the-full-type-table) — the Tier-3 rows and the
  "keep the two `numeric` cases distinct in code" callout.

## Scope

**In scope**

- `Utf8` (VARCHAR) carriers, value carried verbatim as canonical text, for:
  - `numeric` **unconstrained** (typmod −1) and declared **`p>38`** (the Tier-3 numeric branch);
  - `bit`/`varbit` → `'0'/'1'` string; `inet`/`cidr`/`macaddr`/`macaddr8`; `tsvector`/`tsquery`; `pg_lsn`;
    `xid`/`xid8`/`txid_snapshot`; `xml`.
- The numeric branch split: `p≤38` stays Tier-1 `Decimal128` (PR 2.9); everything else Tier-3 `VARCHAR`.
- Conformance cases proving each reads back as DuckDB `VARCHAR` with the exact string; **plus** an explicit
  test that a `p>38` decimal written as VARCHAR beats the DOUBLE-downcast (round-trips exactly).

**Explicitly deferred** (do *not* build these here)

- `uuid` (native `arrow.uuid`) + `enum` (labels) → **PR 2.16**.
- The metadata the loader re-applies (`bit_length`, `char_length`, `money_fraction_digits`, enum labels) →
  **PR 2.17** (`TypeDescriptor.meta`). This PR carries the *value*, not the lost metadata.
- Optional `inet` split into `_addr`/`_masklen`/`_family` (design marks it optional) → out of scope.

## Files to create / modify

```
crates/pg-to-arrow/src/tier3.rs      # new: is_tier3_text(oid, typmod), tier3_field(name)
crates/pg-to-arrow/src/schema.rs     # emit_fields: numeric p>38/unconstrained + system types -> Utf8
crates/pg-to-arrow/src/batch.rs      # append: Tier-3 -> StringBuilder (verbatim canonical text)
crates/pg-to-arrow/src/oids.rs       # + BIT 1560, VARBIT 1562, INET 869, CIDR 650, MACADDR 829,
                                       #   MACADDR8 774, TSVECTOR 3614, TSQUERY 3615, PG_LSN 3220,
                                       #   XID 28, XID8 5069, TXID_SNAPSHOT 2970, XML 142
crates/pg-to-arrow/tests/conformance.rs # + varchar read-back cases; numeric p>38 exactness case
```

## Skeleton

```rust
// crates/pg-to-arrow/src/tier3.rs
use arrow::datatypes::Field;
use crate::oids;

/// True if this OID+typmod is carried as canonical VARCHAR. NOTE: numeric with p<=38 is NOT Tier-3.
pub fn is_tier3_text(type_oid: u32, atttypmod: i32) -> bool {
    match type_oid {
        oids::NUMERIC => matches!(crate::schema::numeric_precision_scale(atttypmod), None | Some((39..=255, _))),
        oids::BIT | oids::VARBIT | oids::INET | oids::CIDR | oids::MACADDR | oids::MACADDR8
        | oids::TSVECTOR | oids::TSQUERY | oids::PG_LSN | oids::XID | oids::XID8
        | oids::TXID_SNAPSHOT | oids::XML => true,
        _ => false,
    }
}

/// A single nullable `Utf8` field carrying the canonical text.
pub fn tier3_field(name: &str) -> Field { todo!() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn numeric_p_le_38_is_not_tier3() { todo!() }          // stays Decimal128 (PR 2.9)
    #[test] fn numeric_unconstrained_is_tier3_varchar() { todo!() }
    #[test] fn numeric_p_gt_38_is_tier3_varchar() { todo!() }
    #[test] fn bit_and_inet_and_pglsn_are_varchar() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `numeric(10,2)` still maps to `Decimal128(10,2)` (no regression); `numeric` (unconstrained) and
      `numeric(40,10)` map to a single `Utf8` field with the value carried verbatim.
- [ ] `bit`/`varbit` carry `'0'/'1'` text; `inet`/`cidr`/`macaddr(8)`/`tsvector`/`tsquery`/`pg_lsn`/`xid(8)`/`xml`
      all map to `Utf8`.
- [ ] Conformance: each reads back as DuckDB `VARCHAR` with the exact canonical string; a `p>38` value written
      as VARCHAR round-trips **exactly** where a Decimal path would downcast to `DOUBLE` and lose digits.
- [ ] A comment records the DuckDB `p>38 → DOUBLE` downcast as the reason VARCHAR (not Decimal256) is used.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-to-arrow` and `cargo test -p pg-to-arrow --features conformance`

## Hints & gotchas

- **The numeric split is the whole point.** `numeric_precision_scale` returns `None` for unconstrained and
  `Some((p,s))` otherwise; Tier-3 is `None` *or* `p>38`. Get the boundary exactly right — `p==38` is Tier-1.
- Carry the value **verbatim**: don't re-canonicalize `bit` strings or normalize `inet` — the wire text is
  already canonical, and re-formatting risks a lossy round-trip.
- Prove the `p>38` claim with an actual conformance test: write a 40-digit decimal as VARCHAR, read it back,
  assert the string is byte-identical (and note a `DECIMAL`/`Decimal256` attempt would fail).
- `xml` is Tier-1-ish in the table (✅) but has no structural target beyond text — carrying it as `Utf8`
  here is fine; just don't claim a native XML DuckDB type.
- These are all single-column carriers — `emit_fields` returns a length-1 vec, same append path as any `Utf8`.

## References

- Design: [`../../walrus-pg-sink.md` §2.5](../../walrus-pg-sink.md#25-tier-3-canonical-text-carriers),
  [§2.3](../../walrus-pg-sink.md#23-the-full-type-table).
- Prev: [PR 2.14](./pr-2.14-pgarrow-geometric.md) · Next: [PR 2.16](./pr-2.16-pgarrow-uuid-enum.md) · [Roadmap](../README.md)
