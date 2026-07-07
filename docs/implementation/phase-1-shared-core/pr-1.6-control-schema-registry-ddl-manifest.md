<!--
  Task file — follows ../TEMPLATE.md. Spec + skeleton only; the learner writes the logic.
-->

# PR 1.6 — `schema_registry` + `ddl_manifest` models

> **Phase:** 1 — Shared core · **Crates touched:** `control` · **Est. size:** M ·
> **Depends on:** PR 1.3, PR 1.2 · **Unlocks:** PR 2.17, PR 2.22, PR 2.33, PR 3.8

The last two control tables are **history, not a queue** — they are *never pruned*, because they are the
schema record needed to reconstruct any table at any `schema_version`. This PR gives `control` the
models for both: `schema_registry` (the sink writes per-column `TypeDescriptor`s keyed by
`schema_version`; the loader reads them back to rebuild exact types), and `ddl_manifest` (one row per
schema-change event, stamped with the DDL's commit LSN, so the loader applies DDL at the right LSN
before transforming past it).

## Why — learning objectives

By the end of this PR you will have practised:

- **Persisting a typed document as `jsonb`** — storing/loading `Vec<TypeDescriptor>` (from `common`,
  PR 1.2) through `sqlx` with the `json`/`jsonb` codec, not hand-rolled strings.
- **Versioned, append-only schema history** — a registry keyed by `schema_version`, upserted once per
  version, read back verbatim; and a DDL log ordered by `c_lsn`.
- **The DDL-in-commit-order insight** — `ddl_audit` INSERTs ride the same slot as DML, so a `ddl_manifest`
  row's `c_lsn` orders it *relative to* the data; the loader gates on it.
- **Idempotent upsert of history rows** — writing the same `schema_version` twice must not duplicate.

## Read first

- `../../walrus-pg-sink.md` §2.6 "The type-mapping descriptor" — the per-column `TypeDescriptor` shape
  written into `schema_registry` (keyed by `schema_version`, referenced from the manifest).
- `../../architecture.md` "DDL capture" (~895) + "Per-change-type handling" (~930) — the sink writes a
  `ddl_manifest` row and bumps `schema_version` on a decoded `ddl_audit` insert; the loader applies the
  change at that LSN. Registry snapshots the **resulting column set** (schema-diff, not DDL-text replay).
- `../../walrus-pg-sink.md` §3.3 "The audit table" — the `ddl_audit` columns (`c_lsn`, `c_event`, `c_tag`,
  `c_obj_schema`, `c_obj_identity`, `c_rel_oid`, `c_columns` jsonb, `c_dropped`) the manifest row derives from.
- `../../architecture.md` "manifest is a work queue" — the contrast: these two tables are **never pruned**.

## Scope

**In scope**

- `schema_registry` model: upsert/read a **versioned** set of per-column `TypeDescriptor`s (jsonb) keyed by
  `(epoch, source_schema, source_table, schema_version)`, plus the resulting column list.
- `ddl_manifest` model: insert/read a schema-change event row stamped with `c_lsn` (commit LSN), `c_tag`,
  `c_event`, `c_rel_oid`, and the structured `c_columns` payload; queried in `c_lsn` order.
- `upsert_registry`, `read_registry(version)`, `read_latest_version`; `insert_ddl`, `read_pending_ddl(after_lsn)`.
- A compose test round-tripping a `TypeDescriptor` set and a `ddl_manifest` row.
- If PR 1.3 stubbed these tables, finalize their columns in a follow-up migration
  `migrations/control/0002_registry_ddl.sql`.

**Explicitly deferred** (do *not* build these here)

- *Producing* real `TypeDescriptor`s per type (tier/emit/recombine) → **PRs 2.9–2.17**.
- The sink actually decoding `ddl_audit` and calling `insert_ddl` + version bump → **PR 2.33**.
- The loader *applying* DDL to `<table>`/`<table>_raw` from these rows → **PRs 3.8 / 3.9**.
- The source-side `ddl_audit` table + triggers themselves → **PR 2.33** (`migrations/source/…`).

## Files to create / modify

```
migrations/control/0002_registry_ddl.sql  # new (or finalize the 1.3 stubs): schema_registry + ddl_manifest columns
crates/control/src/schema_registry.rs     # new — RegistryRow, upsert_registry, read_registry, read_latest_version
crates/control/src/ddl_manifest.rs        # new — DdlRow, insert_ddl, read_pending_ddl
crates/control/src/lib.rs                 # + pub mod schema_registry;  pub mod ddl_manifest;
crates/control/tests/registry_ddl.rs      # new — compose-gated: descriptor + ddl round-trip
crates/control/.sqlx/                     # regenerate offline query data
```

## Skeleton

```sql
-- migrations/control/0002_registry_ddl.sql
CREATE TABLE walrus.schema_registry (
  epoch          bigint NOT NULL,
  source_schema  text   NOT NULL,
  source_table   text   NOT NULL,
  schema_version bigint NOT NULL,
  descriptors    jsonb  NOT NULL,        -- Vec<TypeDescriptor> (common, PR 1.2)
  columns        jsonb  NOT NULL,        -- resulting column set snapshot (name/type/attnum/nullability/comment)
  created_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table, schema_version)   -- history; NEVER pruned
);

CREATE TABLE walrus.ddl_manifest (
  id             bigserial PRIMARY KEY,
  epoch          bigint NOT NULL,
  source_schema  text   NOT NULL,
  source_table   text   NOT NULL,
  c_lsn          pg_lsn NOT NULL,        -- commit LSN of the DDL (orders it vs DML) — from ddl_audit.c_lsn
  c_event        text   NOT NULL,        -- 'ddl_command_end' | 'sql_drop'
  c_tag          text   NOT NULL,        -- 'CREATE TABLE' | 'ALTER TABLE' | 'DROP TABLE' | 'COMMENT' | …
  c_rel_oid      oid,
  schema_version bigint NOT NULL,        -- the version this DDL produces
  c_columns      jsonb,                  -- structured resulting column set (walrus-pg-sink.md §3.3)
  c_dropped      jsonb,                  -- sql_drop dropped-objects payload
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ddl_manifest_lsn_idx ON walrus.ddl_manifest (epoch, source_schema, source_table, c_lsn);
```

```rust
// crates/control/src/schema_registry.rs
use sqlx::postgres::PgExecutor;
use common::TypeDescriptor;  // PR 1.2

#[derive(Debug, Clone)]
pub struct RegistryRow {
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub schema_version: i64,
    pub descriptors: Vec<TypeDescriptor>,      // sqlx::types::Json<Vec<TypeDescriptor>> on the wire
    pub columns: serde_json::Value,            // resulting column set snapshot
}

/// Write (or no-op if identical) the descriptor set for one schema_version. History — never overwritten
/// destructively; a repeated write of the same version must be idempotent.
pub async fn upsert_registry(ex: impl PgExecutor<'_>, row: &RegistryRow) -> Result<(), crate::ControlError> { todo!() }

/// Read the descriptors for an exact schema_version (loader rebuilds types from this).
pub async fn read_registry(ex: impl PgExecutor<'_>, epoch: i64, schema: &str, table: &str, version: i64)
    -> Result<Option<RegistryRow>, crate::ControlError> { todo!() }

/// The current (max) schema_version for a table, if any.
pub async fn read_latest_version(ex: impl PgExecutor<'_>, epoch: i64, schema: &str, table: &str)
    -> Result<Option<i64>, crate::ControlError> { todo!() }
```

```rust
// crates/control/src/ddl_manifest.rs
use sqlx::postgres::PgExecutor;
use common::Lsn;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DdlRow {
    pub id: i64,
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub c_lsn: Lsn,             // commit LSN — orders DDL relative to DML
    pub c_event: String,       // ddl_command_end | sql_drop
    pub c_tag: String,
    pub schema_version: i64,
    // c_columns / c_dropped as serde_json::Value when needed by 3.8/3.9
}

/// Record a decoded schema-change event (sink, PR 2.33). Stamped with the DDL's commit LSN.
pub async fn insert_ddl(ex: impl PgExecutor<'_>, row: &DdlRow) -> Result<i64, crate::ControlError> { todo!() }

/// DDL the loader must apply before transforming past `after_lsn`, in `c_lsn` order (PR 3.8/3.9).
pub async fn read_pending_ddl(ex: impl PgExecutor<'_>, epoch: i64, schema: &str, table: &str, after_lsn: Lsn)
    -> Result<Vec<DdlRow>, crate::ControlError> { todo!() }

#[cfg(test)]
mod tests { /* DB assertions live in tests/registry_ddl.rs */ }
```

```rust
// crates/control/tests/registry_ddl.rs   (compose-gated)
#[tokio::test]
async fn registry_round_trips_a_type_descriptor_set() { todo!() /* write Vec<TypeDescriptor> → read back equal */ }

#[tokio::test]
async fn upsert_registry_is_idempotent_per_version() { todo!() }

#[tokio::test]
async fn ddl_row_round_trips_with_commit_lsn() { todo!() }

#[tokio::test]
async fn read_pending_ddl_orders_by_c_lsn() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `schema_registry` stores/loads `Vec<TypeDescriptor>` (from `common`) as `jsonb`, keyed by
      `(epoch, schema, table, schema_version)`; a re-write of the same version is idempotent.
- [ ] `read_registry(version)` round-trips a descriptor set **byte-for-byte equal** to what was written.
- [ ] `read_latest_version` returns the max `schema_version` for a table (or `None`).
- [ ] `ddl_manifest` rows carry `c_lsn` (commit LSN) and `read_pending_ddl(after_lsn)` returns rows in
      `c_lsn` order.
- [ ] Comments state these tables are **history — never pruned** (contrast with the manifest queue).
- [ ] `.sqlx/` regenerated; `cargo sqlx prepare --check` passes.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p control` and — with services — `docker compose up --wait` then
        `cargo test -p control --test registry_ddl` (descriptor + ddl round-trip passes).

## Hints & gotchas

- **Use `sqlx::types::Json<T>`** (or `sqlx::query!("… $1::jsonb", Json(&v))`) to bind/read the descriptor
  vector — do **not** `to_string()` it yourself; the codec handles the jsonb round-trip and keeps the
  compile-time check honest.
- **`schema_version` is monotonic per table**, bumped by the sink on each structural change (PR 2.33), and
  every Parquet file is homogeneous in it (one version per file). The registry is the lookup that lets the
  loader rebuild the exact type for a given file's version — keep `read_registry` keyed on the *exact* version.
- **`c_lsn` orders DDL vs DML.** Because `ddl_audit` INSERTs ride the same slot, the manifest row's commit
  LSN is directly comparable to `file_manifest.lsn_end` and the checkpoints — the loader crosses a
  `schema_version` boundary by applying pending DDL whose `c_lsn` it is about to pass (PR 3.8). Don't invent
  a separate ordering.
- **Registry and DDL are append-only history** — a `DELETE` here would make old-version files
  un-reconstructable. If PR 1.3 already created stub tables, this PR's migration should `ALTER`/finalize
  them rather than drop-and-recreate.
- Keep `columns` / `c_columns` as `serde_json::Value` for now; PRs 3.8/3.9 give them a typed shape when the
  loader actually diffs `new − old`.

## References

- Design: `../../walrus-pg-sink.md` §2.6 (type-mapping descriptor) & §3.3 (audit table);
  `../../architecture.md` "DDL capture" / "Per-change-type handling" / "manifest is a work queue".
- Prev: [PR 1.5](./pr-1.5-control-checkpoint-replication-state.md) ·
  Next: *(phase boundary — Phase 2 begins at [PR 2.1](../phase-2-pg-sink/pr-2.1-pgoutput-scaffold-golden-vectors.md))* ·
  [Roadmap](../README.md)
