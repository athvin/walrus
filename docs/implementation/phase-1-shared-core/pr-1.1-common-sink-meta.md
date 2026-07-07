<!--
  Task file — follows ../TEMPLATE.md. Spec + skeleton only; the learner writes the logic.
-->

# PR 1.1 — Add the `SinkMeta` provenance model

> **Phase:** 1 — Shared core · **Crates touched:** `common` · **Est. size:** M ·
> **Depends on:** PR 0.5 · **Unlocks:** PR 1.2

Every Parquet row walrus writes carries **one added column, `walrus_pg_sink_meta`** — a JSON
document bunching all batch/row provenance (op, LSNs, xid, epoch, schema version, source
identity, unchanged-TOAST list, sink identity, timestamps). This PR gives `common` the strongly
typed `SinkMeta` struct that both services agree on: the sink *serializes* it into the meta column,
the loader *reads it back* verbatim and promotes fields to typed columns. Getting the field names,
the enum spelling, and the **UTC RFC-3339 `Z`** datetime discipline exactly right here is what makes
the whole cross-service contract mechanical instead of stringly-typed.

## Why — learning objectives

By the end of this PR you will have practised:

- **serde with `#[serde(rename)]` / field-name discipline** — the JSON keys are a wire contract, not
  an implementation detail; a typo silently breaks the loader.
- **Modelling an enum as a tagged scalar** — `op` is `i | u | d | t`, serialized as a single char, not
  a Rust variant name.
- **UTC time handling** — every walrus datetime is RFC-3339 with a `Z` suffix; you learn to *make the
  type enforce that* rather than hoping callers remember.
- **Reusing the `Lsn` newtype** — `lsn` and `commit_lsn` are `Lsn` values that serialize as zero-padded
  16-hex so text order equals numeric order.

## Read first

- `../../architecture.md` §1.4 "Arrow conversion & Parquet write" — the `walrus_pg_sink_meta` JSON block
  (the exact keys, the `op` alphabet, the "everything lives in this one column" rule).
- `../../architecture.md` §1.4 "All datetimes walrus records as metadata are UTC" — the hard `Z` rule and
  *why* (LSN/commit ordering must stay comparable across instances).
- `../../architecture.md` "Coordination contract" — how the loader promotes `op`/`commit_lsn`/`lsn`/
  `sink_processed_at` out of this column into typed `<table>_raw` columns.

## Scope

**In scope**

- A `SinkMeta` struct with every field from the §1.4 block, plus an `Op` enum (`Insert|Update|Delete|Truncate`).
- serde `Serialize`/`Deserialize` producing **exactly** the documented JSON keys and value shapes.
- A `Kind` enum (`Snapshot | Stream`) for the `kind` field.
- A UTC-timestamp helper/newtype so `commit_ts` and `sink_processed_at` always render with `Z`.

**Explicitly deferred** (do *not* build these here)

- The Postgres shape types (`PgRelation`/`PgColumn`/`TupleValue`/`TypeDescriptor`) → **PR 1.2**.
- Actually *building* the Arrow `Utf8` meta column from `SinkMeta` → **PR 2.10** (pg-to-arrow).
- Persisting descriptors to `schema_registry` → **PR 1.6 / PR 2.17**.

## Files to create / modify

```
crates/common/Cargo.toml          # + serde = { version = "1", features = ["derive"] }
                                   # + serde_json = "1"
                                   # + jiff = "0.1"   (or time = "0.3" — pick one, justify in a comment)
crates/common/src/sink_meta.rs    # new — SinkMeta, Op, Kind, the UTC timestamp helper
crates/common/src/lib.rs          # + pub mod sink_meta;  + re-exports
```

## Skeleton

```rust
// crates/common/src/sink_meta.rs
use serde::{Deserialize, Serialize};
use crate::Lsn; // from PR 0.3

/// The change operation. Serializes to a single lowercase char: i | u | d | t.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    // TODO: map each variant to "i" | "u" | "d" | "t" via #[serde(rename = "…")].
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Where the row originated: an exported-snapshot backfill row vs a live WAL-stream row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Snapshot,
    Stream,
}

/// A UTC instant rendered as RFC-3339 with a `Z` suffix — walrus's only legal datetime form.
/// Wrap your chosen time lib so callers *cannot* accidentally emit a local/offset timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcTimestamp(/* jiff::Timestamp | time::OffsetDateTime */);

impl UtcTimestamp {
    pub fn now() -> Self { todo!() }
    /// Parse an RFC-3339 string; reject anything not normalized to UTC `Z`.
    pub fn parse_rfc3339(s: &str) -> Result<Self, crate::Error> { todo!() }
}
// TODO: impl Serialize/Deserialize for UtcTimestamp that ALWAYS renders "…Z".

/// The provenance document embedded (as JSON `Utf8`) in every Parquet row's `walrus_pg_sink_meta`.
/// Field order/keys are a cross-service wire contract — see architecture.md §1.4.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkMeta {
    pub op: Op,
    pub lsn: Lsn,               // per-row WAL LSN, zero-padded 16-hex (per-PK tiebreaker only)
    pub commit_lsn: Lsn,        // txn commit LSN — THE order/watermark key
    pub commit_ts: UtcTimestamp,
    pub xid: u32,
    pub epoch: i64,
    pub batch_id: String,       // uuid string
    pub schema_version: i64,
    pub source_schema: String,
    pub source_table: String,
    pub kind: Kind,
    pub unchanged_toast: Vec<String>,   // cols sent as unchanged-TOAST placeholders
    pub sink_instance: String,
    pub sink_processed_at: UtcTimestamp,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_serializes_as_single_char() { todo!() }

    #[test]
    fn meta_round_trips_exact_keys() { todo!() /* serialize → assert JSON keys/values incl. "u", "stream" */ }

    #[test]
    fn timestamps_always_render_with_z_suffix() { todo!() }

    #[test]
    fn lsn_fields_are_zero_padded_16_hex() { todo!() }

    #[test]
    fn deserializes_the_docs_example_block() { todo!() /* the §1.4 JSON verbatim */ }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `SinkMeta` has every field from architecture.md §1.4 with the exact JSON keys.
- [ ] `Op` serializes to `"i" | "u" | "d" | "t"`; `Kind` to `"snapshot" | "stream"`.
- [ ] `commit_ts` and `sink_processed_at` serialize as RFC-3339 with a **`Z`** suffix; a non-UTC input is rejected.
- [ ] `lsn` and `commit_lsn` serialize as zero-padded 16-hex strings (reuse `Lsn` from PR 0.3).
- [ ] A round-trip test deserializes the §1.4 example block and re-serializes to equal JSON.
- [ ] Docs/comments explain that these keys are a cross-service wire contract.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common` (and `--workspace` stays green)

## Hints & gotchas

- **Don't derive `rename_all` blindly on `Op`.** The wire wants single chars (`i`/`u`/`d`/`t`), not
  `"insert"`; use per-variant `#[serde(rename = "i")]`.
- **Serialize timestamps yourself.** `jiff::Timestamp`'s default Display is already RFC-3339 `Z`; `time`'s
  is not — if you pick `time`, format with `Rfc3339` and assert the `Z`. Either way, add the test that
  *fails* if a local offset ever leaks in.
- **`epoch` is `i64`, not `u64`** — it maps to Postgres `bigint` in the control tables later; keep the
  Rust and SQL widths aligned now to avoid a cast the day you persist it.
- The meta column is **provenance, not current state** — it stays in `<table>_raw`, is dropped from the
  `<table>` mirror. Nothing here needs to know that, but your doc comment should say where it goes.
- Keep `SinkMeta` `PartialEq` so tests can assert full-struct equality after a round-trip.

## References

- Design: `../../architecture.md` §1.4 (the `walrus_pg_sink_meta` block + the UTC `Z` rule),
  "Coordination contract".
- Prev: *(phase boundary — see [PR 0.5](../phase-0-foundations/pr-0.5-common-config.md))* ·
  Next: [PR 1.2](./pr-1.2-common-pg-shape-types.md) · [Roadmap](../README.md)
