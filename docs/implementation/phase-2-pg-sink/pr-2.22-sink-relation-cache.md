# PR 2.22 — Relation cache + Arrow schema per `schema_version`

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/42

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib),
> `pg-to-arrow`, `control` · **Est. size:** M · **Depends on:** PR 2.21 · **Unlocks:** PR 2.23

Every pgoutput `Relation` message describes a table's shape at a point in time; every later `Insert`/
`Update`/`Delete` references it by OID. This PR builds the **relation cache**: on each `Relation`, look
up (or build) the Tier-1 Arrow schema via `pg-to-arrow`, cache it keyed by `(relation_oid,
schema_version)`, and **persist the `TypeDescriptor` + schema into `schema_registry`** (via `control`)
so the loader can rebuild the exact types later. At bootstrap the cache is **hydrated** from
`schema_registry` (shared bootstrap step 7). This is the bridge from "decoded messages" to "typed Arrow."

## Why — learning objectives

By the end of this PR you will have practised:

- **A keyed cache with a versioned key** — `HashMap<(u32, i64), Arc<CachedRelation>>`, where the
  `schema_version` in the key is what makes a schema change a *new* entry, not a mutation.
- **Wiring three crates through the neutral seam** — the decoder produces `PgRelation` (in `common`),
  `pg-to-arrow` turns it into an Arrow `Schema` + `TypeDescriptor`, and `control` persists it.
- **Hydrate-on-boot** — reconstructing in-memory state from the durable `schema_registry` so a restart
  is a resume, not a cold rebuild.
- **`Arc` sharing** — the cached schema is read by the batching path (PR 2.23) without cloning per row.

## Read first

- `../../walrus-pg-sink.md` §2.6 The type-mapping descriptor (`#26-the-type-mapping-descriptor-how-the-loader-rebuilds-exactly`) —
  the `TypeDescriptor` shape and why it's persisted (the loader rebuilds types from it).
- `../../architecture.md` "Startup & bootstrap" `walrus-pg-sink` **step 7 — Hydrate schema registry**
  (`#startup--bootstrap-fail-fast-preflight`).
- `../../architecture.md` §1.8 Epoch (`#18-single-slot-for-life--total-restart`) — `schema_version` and
  epoch namespacing; a bumped version is a new registry row, never an in-place edit.
- Prior PRs: 2.9/2.10 (Arrow schema + RecordBatch from `PgRelation`), 2.17 (`TypeDescriptor` → `schema_registry`).

## Scope

**In scope**

- `RelationCache` keyed by `(relation_oid, schema_version)` → `Arc<CachedRelation>` (holds `PgRelation`,
  Arrow `SchemaRef`, and the per-column `TypeDescriptor`s).
- On a `Relation` message: build the Arrow schema + descriptors via `pg-to-arrow`, insert into the cache,
  and **upsert** the `schema_registry` row via `control` (idempotent on `(schema_version, relation)`).
- Hydrate the cache at bootstrap from `schema_registry` for each configured table at its current
  `schema_version`.
- Ignore internal tables (`walrus.ddl_audit`, `walrus.heartbeat`) — never registered/schematised.

**Explicitly deferred** (do *not* build these here)

- Bumping `schema_version` on a DDL event → **PR 2.33** (DDL capture).
- Building actual RecordBatches from tuples into the cache → **PR 2.23** (uses this cache read-only).
- Tier-2/Tier-3 column decompositions beyond what `pg-to-arrow` already emits (those PRs are 2.12–2.16).

## Files to create / modify

```
crates/pg-sink/src/relcache.rs       # new — RelationCache + CachedRelation
crates/pg-sink/src/consume.rs        # modify — on Relation: update cache + persist registry
crates/pg-sink/src/bootstrap.rs      # modify — hydrate cache from schema_registry (step 7)
crates/pg-sink/tests/relation_cache.rs  # new — compose: first Relation writes a registry row; schema matches
```

## Skeleton

```rust
// crates/pg-sink/src/relcache.rs
use std::sync::Arc;
use std::collections::HashMap;
use arrow::datatypes::SchemaRef;
use common::{PgRelation, TypeDescriptor};

/// Everything the batching path needs for one relation at one schema_version.
pub struct CachedRelation {
    pub relation: PgRelation,
    pub arrow_schema: SchemaRef,             // built by pg-to-arrow (Tier-1 + walrus_pg_sink_meta col)
    pub descriptors: Vec<TypeDescriptor>,    // per source column, for the loader to rebuild
    pub schema_version: i64,
}

#[derive(Default)]
pub struct RelationCache { by_key: HashMap<(u32, i64), Arc<CachedRelation>> }

impl RelationCache {
    pub fn get(&self, oid: u32, schema_version: i64) -> Option<Arc<CachedRelation>> { todo!() }

    /// Build Arrow schema + descriptors from a decoded Relation, cache, and return the entry.
    pub fn upsert_from_relation(
        &mut self,
        relation: PgRelation,
        schema_version: i64,
    ) -> Result<Arc<CachedRelation>, RelationError> { todo!() }

    /// Rebuild cache entries at bootstrap from persisted schema_registry rows (step 7).
    pub fn hydrate(&mut self, rows: Vec<control::SchemaRegistryRow>) -> Result<(), RelationError> { todo!() }
}

#[derive(Debug, thiserror::Error)]
pub enum RelationError {
    #[error("unsupported column type oid {oid} on {schema}.{table}")]
    UnsupportedType { oid: u32, schema: String, table: String },
    #[error(transparent)]
    Arrow(#[from] pg_to_arrow::SchemaError),
}
```

```rust
// crates/pg-sink/src/consume.rs  (added)
/// On a Relation message: cache it AND persist the registry row (idempotent).
pub async fn on_relation(
    cache: &mut RelationCache,
    control: &control::Control,
    relation: common::PgRelation,
    schema_version: i64,
) -> anyhow::Result<()> { todo!() }
```

```rust
// crates/pg-sink/tests/relation_cache.rs
#[tokio::test] async fn first_relation_writes_a_schema_registry_row() { todo!() }
#[tokio::test] async fn cached_arrow_schema_matches_expected_orders_shape() { todo!() }
#[tokio::test] async fn hydrate_reconstructs_cache_from_registry() { todo!() }
#[tokio::test] async fn internal_tables_are_never_registered() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] A `Relation` message builds a Tier-1 Arrow schema (incl. the `walrus_pg_sink_meta` `Utf8` column)
      via `pg-to-arrow` and caches it under `(relation_oid, schema_version)`.
- [x] The first sighting of a relation **upserts** a `schema_registry` row (via `control`); a repeat at
      the same `schema_version` is idempotent (no duplicate/no error).
- [x] Bootstrap **hydrates** the cache from `schema_registry`, so a restart doesn't need a fresh
      `Relation` to convert the first tuple.
- [x] `walrus.ddl_audit` and `walrus.heartbeat` are recognised as internal and **never** registered or
      schematised.
- [x] The cached `SchemaRef` for `orders` exactly matches the schema asserted in PR 2.9's unit test.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test relation_cache`: first `Relation`
        writes a registry row whose schema matches.

## Hints & gotchas

- Key the cache on **`(oid, schema_version)`**, not `oid` alone — a schema change (PR 2.33) must produce
  a *new* entry so in-flight batches at the old version still resolve. The `oid` can be reused by
  Postgres, but within an epoch+version it's stable.
- Persisting the registry row is a **write to the control DB**, so `on_relation` is `async`; keep the
  cache mutation and the DB upsert in a sensible order (build → cache → persist) and make the persist
  idempotent so a re-decoded `Relation` after reconnect is harmless.
- The `walrus_pg_sink_meta` column belongs to the **Arrow schema**, not the `PgRelation` — add it in the
  `pg-to-arrow` builder, not by faking a `PgColumn`.
- Don't try to *bump* `schema_version` here on a shape change — that's DDL capture (PR 2.33). Here the
  version is an input you receive (from the current registry / a later DDL event).
- Hydration reads persisted `TypeDescriptor`s; assert the round-trip (`TypeDescriptor` written in PR 2.17
  deserialises back to an identical descriptor) so a subtle serde drift is caught cheaply.

## References

- Design: `../../walrus-pg-sink.md` §2.6; `../../architecture.md` "Startup & bootstrap" step 7, §1.8.
- Prev: [PR 2.21](./pr-2.21-sink-wire-decoder.md) · Next: [PR 2.23](./pr-2.23-sink-batching-cadence.md) · [Roadmap](../README.md)
