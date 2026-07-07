# PR 2.17 — Build the per-column `TypeDescriptor` and persist it to `schema_registry`

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow`, `common`, `control` (test) ·
> **Est. size:** M · **Depends on:** PR 2.16, PR 1.6 (`schema_registry` model) · **Unlocks:** PR 2.18

The final Phase-2b PR closes the loop: for every source column, produce the **mapping descriptor** that
records exactly what Parquet/DuckDB collapse on read — tier, emitted columns, recombine expression, and the
metadata (`enum_labels`, `bit_length`, `char_length`, `money_fraction_digits`) the loader must re-apply.
This descriptor is what turns "reconcile to the exact source shape" from a guess into a mechanical
operation. `pg-to-arrow` builds it (a `common::TypeDescriptor`); `control` persists it into
`schema_registry` keyed by `schema_version`.

## Why — learning objectives

By the end of this PR you will have practised:

- **Making implicit knowledge explicit** — every tier decision from PRs 2.9–2.16 becomes one serializable record.
- **A single source of truth across a seam** — the sink writes the descriptor; the loader reads it back to rebuild types.
- **`serde` round-tripping a tagged model** — `TypeDescriptor` (in `common`) ⇄ JSON ⇄ `schema_registry` row.
- **A legal cross-crate test** — `control/tests` dev-depending on `pg-to-arrow` without creating a DAG cycle.

## Read first

- [`../../walrus-pg-sink.md` §2.6](../../walrus-pg-sink.md#26-the-type-mapping-descriptor-how-the-loader-rebuilds-exactly)
  — the descriptor JSON shape (`column`, `pg_type_oid`, `pg_type`, `tier`, `arrow`, `duckdb`, `emit[]`,
  `recombine`, `meta{…}`) and its purpose.
- [`../../walrus-pg-sink.md` §2.2](../../walrus-pg-sink.md#22-the-three-tier-model) — the tier recorded per column.
- `common::TypeDescriptor` (PR 1.2) and `control` `schema_registry` upsert/read (PR 1.6) — what you populate and persist.

## Scope

**In scope**

- `describe_column(&PgColumn) -> TypeDescriptor` in `pg-to-arrow`, deriving tier / `emit[]` / `recombine` /
  `meta` from the same dispatch the schema builder uses (single source of truth — no second mapping table).
- `describe_relation(&PgRelation) -> Vec<TypeDescriptor>` for the whole table.
- Populate `meta`: `enum_labels` (from the caller-supplied enum labels), `bit_length` (bit/varbit typmod),
  `char_length` (char/varchar typmod), `money_fraction_digits` (deferred/None).
- A `control/tests` compose round-trip writing one descriptor into `schema_registry` and reading it back equal.

**Explicitly deferred** (do *not* build these here)

- Bumping `schema_version` on DDL / cutting a fresh Parquet file → sink binary **PR 2.33**.
- Hydrating the relation cache + writing the registry row *from the live sink* → **PR 2.22**.
- The loader consuming the descriptor to recreate enum/bit/interval types → loader phase (PR 3.x).

## Files to create / modify

```
crates/pg-to-arrow/src/descriptor.rs  # new: describe_column, describe_relation, tier_of, emit_of, recombine_of
crates/pg-to-arrow/src/lib.rs         # + pub mod descriptor;
crates/control/tests/schema_registry_descriptor.rs # new: compose round-trip via schema_registry
crates/control/Cargo.toml             # [dev-dependencies] pg-to-arrow = { path = "../pg-to-arrow" }  (no cycle)
```

## Skeleton

```rust
// crates/pg-to-arrow/src/descriptor.rs
use common::{PgColumn, PgRelation, TypeDescriptor};

/// Derive the per-column mapping descriptor (§2.6) using the SAME dispatch as the schema/batch builders.
pub fn describe_column(col: &PgColumn) -> TypeDescriptor { todo!() }

/// Descriptors for every column of a relation, in column order.
pub fn describe_relation(rel: &PgRelation) -> Vec<TypeDescriptor> { todo!() }

/// Tier (1/2/3) for a column — must agree with `schema::emit_fields` and `tier3::is_tier3_text`.
fn tier_of(col: &PgColumn) -> u8 { todo!() }

/// The emitted `name:ARROW_TYPE` list (Tier-1 = one entry; Tier-2 = the sibling columns; Tier-3 = one VARCHAR).
fn emit_of(col: &PgColumn) -> Vec<String> { todo!() }

/// The loader's recombine expression, e.g. interval => "to_months(m)+to_days(d)+to_microseconds(us)".
fn recombine_of(col: &PgColumn) -> Option<String> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn interval_descriptor_has_three_emits_and_recombine() { todo!() }
    #[test] fn enum_descriptor_carries_ordered_labels() { todo!() }
    #[test] fn char_n_descriptor_carries_char_length() { todo!() }
    #[test] fn tier_matches_emit_fields_for_every_supported_oid() { todo!() } // cross-check the two dispatches
    #[test] fn descriptor_json_roundtrips() { todo!() }                       // serde <-> §2.6 shape
}
```

```rust
// crates/control/tests/schema_registry_descriptor.rs   (compose: needs control PG)
#[tokio::test]
#[ignore = "requires `docker compose up --wait` (control PG)"]
async fn schema_registry_roundtrips_a_type_descriptor() {
    // build a TypeDescriptor with pg_to_arrow::descriptor::describe_column(...),
    // upsert into schema_registry (schema_version = 1) via the control model,
    // read it back, assert the deserialized descriptor is equal.
    todo!()
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `describe_column` returns a `TypeDescriptor` matching §2.6 for representative columns: `interval`
      (tier 2, three `emit[]`, a `recombine`), `enum` (tier 3, `meta.enum_labels` = ordered set), `char(n)`
      (tier 1, `meta.char_length = n`).
- [ ] `tier_of` / `emit_of` agree with `schema::emit_fields` for **every** OID the crate supports (a test
      cross-checks the two dispatches so they can't drift).
- [ ] `TypeDescriptor` serializes to / deserializes from the §2.6 JSON shape (round-trip equal).
- [ ] Compose test writes a descriptor into `schema_registry` (keyed by `schema_version`) via `control` and
      reads back an equal descriptor; `control/tests` dev-depends on `pg-to-arrow` with **no DAG cycle**.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-to-arrow` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p control -- --ignored schema_registry_roundtrips_a_type_descriptor`

## Hints & gotchas

- **Do not build a second mapping table.** Derive tier/emit/recombine by calling the *same* functions the
  schema/batch builders use — a divergent descriptor is exactly the drift the loader can't detect. The
  cross-check test guards this.
- `emit_of` must list the sibling columns in the **same order** the schema emits them (interval:
  `months, days, micros`) — the loader positional-binds against this list.
- `meta` fields are `Option`: populate only what applies (`enum_labels` for enums, `bit_length` for bit/varbit,
  `char_length` for char(n)/varchar(n)); leave the rest `null` — `money` fraction digits are deferred.
- `control` depending on `pg-to-arrow` as a **dev-dependency** is legal (pg-to-arrow → common only, no back-edge);
  keep it in `[dev-dependencies]` so the production DAG (`control → common`) is unchanged.
- This descriptor is consumed live at sink bootstrap (PR 2.22) and by the loader; keep its serde stable and
  versioned-by-`schema_version`, since a schema change writes a *new* registry row rather than mutating one.

## References

- Design: [`../../walrus-pg-sink.md` §2.6](../../walrus-pg-sink.md#26-the-type-mapping-descriptor-how-the-loader-rebuilds-exactly),
  [§2.2](../../walrus-pg-sink.md#22-the-three-tier-model).
- Prev: [PR 2.16](./pr-2.16-pgarrow-uuid-enum.md) · Next: [PR 2.18](./pr-2.18-sink-skeleton-health-shutdown.md) · [Roadmap](../README.md)
