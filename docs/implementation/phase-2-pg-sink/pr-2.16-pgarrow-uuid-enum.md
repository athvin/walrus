# PR 2.16 — `uuid` (native via `arrow.uuid`) + `enum` (VARCHAR + ordered labels)

> **Phase:** 2 — walrus-pg-sink (2b: pg-to-arrow) · **Crates touched:** `pg-to-arrow` · **Est. size:** M ·
> **Depends on:** PR 2.15 · **Unlocks:** PR 2.17

Two types that hinge on one subtlety each. `uuid` can be a **native DuckDB `UUID`** — but *only* when
arrow-rs annotates the `FixedSizeBinary(16)` with the `arrow.uuid` canonical extension; a plain FSB(16)
reads back as `BLOB`. So the mapping is guarded by a CI `typeof == UUID` assertion and a pinned arrow-rs,
with a `VARCHAR + CAST` fallback. `enum` values are lossless as `VARCHAR`, but the **ordered label set** is
lost on the wire and must be carried (in the descriptor, PR 2.17) so the loader can recreate the DuckDB `ENUM`.

## Why — learning objectives

By the end of this PR you will have practised:

- **Arrow canonical extension types** — attaching `ARROW:extension:name = "arrow.uuid"` so Parquet gets the UUID logical type.
- **CI-guarded mappings** — asserting a behaviour that a dependency bump could silently change, with a fallback path.
- **`FixedSizeBinaryBuilder`** — parsing UUID text to 16 bytes and appending fixed-width binary.
- **Separating value from metadata** — enum *values* carried now (VARCHAR), enum *labels* deferred to the descriptor.

## Read first

- [`../../walrus-pg-sink.md` §2.4 uuid](../../walrus-pg-sink.md#uuid) — native `UUID` only with the extension;
  the `write → read_parquet → typeof == UUID` CI guard + pinned arrow-rs; the `VARCHAR + CAST(x AS UUID)` fallback.
- [`../../walrus-pg-sink.md` §2.5 enum](../../walrus-pg-sink.md#25-tier-3-canonical-text-carriers) — DuckDB's
  Parquet reader maps ENUM/UTF8 both to VARCHAR; the ordered label set is carried in the descriptor.
- [`../../walrus-pg-sink.md` §2.1](../../walrus-pg-sink.md#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types)
  — why only a real Parquet logical type (here: UUID) survives into DuckDB.

## Scope

**In scope**

- `uuid` (OID 2950) → `FixedSizeBinary(16)` **+ `arrow.uuid` extension metadata**, appended via
  `FixedSizeBinaryBuilder`; a fallback `uuid_as_varchar()` path selectable behind a flag/const.
- `enum` (typtype `'e'`, dynamic OID) → `Utf8` (VARCHAR), value carried verbatim.
- The CI guard: a conformance test asserting `typeof(uuid_col) == 'UUID'` (native), plus a value round-trip.
- `uuid` crate dependency for text↔bytes parsing.

**Explicitly deferred** (do *not* build these here)

- Recording the enum **ordered label set** and the uuid tier in the `TypeDescriptor` → **PR 2.17**.
- The loader recreating the DuckDB `ENUM` type and `CAST`ing → loader phase.
- Detecting enum-ness from the source catalog during registry hydration → sink binary **PR 2.22**
  (here, treat a caller-supplied enum marker / non-builtin OID as `enum → VARCHAR`).

## Files to create / modify

```
crates/pg-to-arrow/Cargo.toml        # + uuid = "1"
crates/pg-to-arrow/src/uuid_enum.rs  # new: uuid_field (extension), uuid_as_varchar, enum_field, parse_uuid_bytes
crates/pg-to-arrow/src/schema.rs     # emit_fields: uuid -> FSB(16)+ext (or Utf8 fallback); enum -> Utf8
crates/pg-to-arrow/src/batch.rs      # append: FixedSizeBinaryBuilder for uuid; StringBuilder for enum
crates/pg-to-arrow/src/oids.rs       # + UUID = 2950
crates/pg-to-arrow/tests/conformance.rs # + uuid typeof==UUID + value; uuid VARCHAR fallback; enum value
```

## Skeleton

```rust
// crates/pg-to-arrow/src/uuid_enum.rs
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;

pub const ARROW_UUID_EXTENSION: &str = "arrow.uuid";

/// `FixedSizeBinary(16)` carrying the `arrow.uuid` canonical extension → Parquet UUID → DuckDB `UUID`.
pub fn uuid_field(name: &str) -> Field {
    // Field::new(name, DataType::FixedSizeBinary(16), true)
    //     .with_metadata(HashMap::from([("ARROW:extension:name".into(), ARROW_UUID_EXTENSION.into())]))
    todo!()
}

/// Fallback path if a pinned arrow-rs release ever drops the annotation: carry canonical text + CAST on load.
pub fn uuid_as_varchar(name: &str) -> Field { todo!() }

/// enum → nullable `Utf8`; the ordered label set is carried by the descriptor (PR 2.17), not here.
pub fn enum_field(name: &str) -> Field { todo!() }

/// Parse canonical UUID text (`"550e8400-e29b-41d4-a716-446655440000"`) into 16 bytes.
pub fn parse_uuid_bytes(text: &str) -> Result<[u8; 16], crate::error::Error> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn uuid_field_carries_arrow_uuid_extension() { todo!() }
    #[test] fn parse_uuid_bytes_roundtrips() { todo!() }
    #[test] fn enum_is_plain_utf8() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `uuid_field` produces `FixedSizeBinary(16)` with `ARROW:extension:name = arrow.uuid`; append via
      `FixedSizeBinaryBuilder` from parsed 16-byte values.
- [ ] **CI guard:** a conformance test writes a uuid column, `read_parquet`s it, and asserts
      `typeof(col) == 'UUID'` **and** the value — this is the pinned-arrow-rs canary.
- [ ] A `uuid_as_varchar` fallback exists and its own conformance test proves `CAST(col AS UUID)` recovers the value.
- [ ] `enum` maps to `Utf8`, value carried verbatim, and reads back as DuckDB `VARCHAR` (label set not yet applied).
- [ ] `arrow`/`parquet`/`duckdb` are pinned to exact versions (comment references the UUID-annotation dependency).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-to-arrow` and `cargo test -p pg-to-arrow --features conformance`

## Hints & gotchas

- **Plain FSB(16) reads back as BLOB.** The extension metadata is the *only* thing that makes DuckDB see
  `UUID` — this is why the mapping is CI-guarded rather than trusted (§2.4 uuid).
- Keep the fallback real, not theoretical: the `uuid_as_varchar` path plus its `CAST` test is your escape
  hatch if an arrow-rs bump drops the annotation on the normal column path.
- `parse_uuid_bytes` should reject malformed input (wrong length/hyphens) with `ValueParse` — don't silently
  zero-pad. The `uuid` crate's `Uuid::parse_str(...).into_bytes()` is the clean route.
- Enum values are just strings here; do **not** try to encode the label set into the column — that's the
  descriptor's job in PR 2.17, and mixing them would couple the batch to catalog state.
- Enum OIDs are dynamic (≥16384); rely on a caller-provided "is enum" marker (or the `Type` message the
  decoder saw) rather than hard-coding OIDs — wire that marker minimally now, resolve fully in PR 2.22.

## References

- Design: [`../../walrus-pg-sink.md` §2.4 uuid](../../walrus-pg-sink.md#uuid),
  [§2.5 enum](../../walrus-pg-sink.md#25-tier-3-canonical-text-carriers),
  [§2.1](../../walrus-pg-sink.md#21-the-one-rule-that-governs-everything-duckdb-reads-parquet-native-types).
- Prev: [PR 2.15](./pr-2.15-pgarrow-tier3-text-carriers.md) · Next: [PR 2.17](./pr-2.17-pgarrow-type-descriptor.md) · [Roadmap](../README.md)
