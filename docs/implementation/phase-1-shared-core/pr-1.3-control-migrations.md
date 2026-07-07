<!--
  Task file — follows ../TEMPLATE.md. Spec + skeleton only; the learner writes the logic.
-->

# PR 1.3 — Control-plane migrations + `sqlx::migrate!` runner

> **Phase:** 1 — Shared core · **Crates touched:** `control`, `migrations/control` · **Est. size:** M ·
> **Depends on:** PR 1.2, PR 0.6 · **Unlocks:** PR 1.4, PR 1.5, PR 1.6

This PR stands up the `control` crate and the **coordination contract** as real SQL: the five control
tables (`replication_state`, `file_manifest`, `loader_checkpoint`, `ddl_manifest`, `schema_registry`),
their indexes and CHECK constraints, plus a `sqlx::migrate!` runner and a connection pool. It's the
first PR that touches a live Postgres, so it also turns on the **compose integration job** and the
**sqlx offline** discipline the whole control layer depends on. No models yet — just the schema and the
ability to apply it and prove it landed.

## Why — learning objectives

By the end of this PR you will have practised:

- **`sqlx` migrations** — a versioned `migrations/` dir applied by `sqlx::migrate!()` at runtime, and the
  same files checked in CI.
- **`sqlx` offline mode** — `cargo sqlx prepare` so `cargo build` doesn't need a live DB; CI runs
  `cargo sqlx prepare --check`.
- **A tokio + `PgPool` connect path** — the async control-DB entrypoint every later `control` model uses.
- **Encoding invariants in DDL** — the partial index `WHERE status='ready'`, the
  `CHECK (transformed_lsn <= raw_appended_lsn)`, and `pg_lsn`-typed watermarks are correctness, not decoration.

## Read first

- `../../architecture.md` "Coordination contract (control-plane tables)" — the canonical `CREATE TABLE`
  statements for `replication_state`, `file_manifest` (+ partial index), `loader_checkpoint` (+ CHECK).
- `../../architecture.md` §1.4 / "Lifecycle — the manifest is a work queue" — why `lsn_end` is the
  **commit LSN**, why the manifest is a queue (rows deleted, not kept), why `ddl_manifest`/`schema_registry`
  are **never** pruned.
- `../../walrus-loader.md` §2 — the same tables annotated for what the *loader* reads (the claim index).

## Scope

**In scope**

- `migrations/control/0001_control_schema.sql` creating all five tables exactly per the contract, with:
  the `file_manifest` partial index `(epoch, source_schema, source_table, lsn_end, id) WHERE status='ready'`,
  the `loader_checkpoint` PK + `CHECK (transformed_lsn <= raw_appended_lsn)`, and `replication_state` PK.
- `control` crate: `PgPool` connect helper + `run_migrations()` wrapping `sqlx::migrate!`.
- A compose-gated integration test that runs the migrations and asserts the tables/indexes/constraints exist.
- `sqlx` offline prepared data (`.sqlx/`) committed; CI gains `cargo sqlx prepare --check`.

**Explicitly deferred** (do *not* build these here)

- Any row-level models (insert/claim/upsert) → **PRs 1.4–1.6**.
- `ddl_manifest` / `schema_registry` *column* detail beyond what the contract names → refined in **PR 1.6**.
- The source-side migrations (publication, ddl triggers, heartbeat) → **PRs 2.19 / 2.33**.
- Table-ownership / fencing token → **PR 3.1**.

## Files to create / modify

```
crates/control/Cargo.toml                 # + sqlx = { version = "0.8", features = ["runtime-tokio","tls-rustls","postgres","macros","migrate"] }
                                          # + tokio = { version = "1", features = ["macros","rt-multi-thread"] }
                                          # + thiserror = "1"   (lib error type)
crates/control/src/lib.rs                 # + pub mod db;  error type
crates/control/src/db.rs                  # new — connect(), run_migrations()
migrations/control/0001_control_schema.sql # new — the five tables + index + CHECK
crates/control/tests/migrations.rs        # new — compose-gated; applies + asserts schema
crates/control/.sqlx/                      # new — offline query data (empty until 1.4, but wired now)
Cargo.toml                                # + "crates/control" already a member (PR 0.1); confirm
```

## Skeleton

```sql
-- migrations/control/0001_control_schema.sql  (mirror architecture.md "Coordination contract" verbatim)
CREATE SCHEMA IF NOT EXISTS walrus;

CREATE TABLE walrus.replication_state (
  epoch        bigint PRIMARY KEY,
  slot_name    text NOT NULL,
  created_lsn  pg_lsn NOT NULL,
  status       text NOT NULL,        -- bootstrapping | streaming | total_restart
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE walrus.file_manifest (
  id             bigserial PRIMARY KEY,
  epoch          bigint NOT NULL,
  source_schema  text NOT NULL,
  source_table   text NOT NULL,
  s3_uri         text NOT NULL,
  kind           text NOT NULL,       -- 'snapshot' | 'stream'
  row_count      bigint NOT NULL,
  lsn_start      pg_lsn NOT NULL,
  lsn_end        pg_lsn NOT NULL,      -- COMMIT LSN of the file's last txn
  schema_version bigint NOT NULL,
  status         text NOT NULL DEFAULT 'ready',
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX file_manifest_claim_idx
  ON walrus.file_manifest (epoch, source_schema, source_table, lsn_end, id)
  WHERE status = 'ready';

CREATE TABLE walrus.loader_checkpoint (
  epoch            bigint NOT NULL,
  source_schema    text NOT NULL,
  source_table     text NOT NULL,
  raw_appended_lsn pg_lsn NOT NULL,
  transformed_lsn  pg_lsn NOT NULL,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table),
  CHECK (transformed_lsn <= raw_appended_lsn)
);

-- TODO: walrus.schema_registry (versioned per-column TypeDescriptors) and
--       walrus.ddl_manifest (schema-change events w/ c_lsn) — shape refined in PR 1.6,
--       but create the tables here so the migration set is complete.
```

```rust
// crates/control/src/db.rs
use sqlx::postgres::PgPool;

/// Terminal-vs-transient control-DB errors (thiserror). Invalid config / missing migration = terminal.
#[derive(Debug, thiserror::Error)]
pub enum ControlError { /* TODO: Connect(#[from] sqlx::Error), Migrate(...), ... */ }

/// Connect to the control Postgres (bounds-checked pool size from config, PR 0.5).
pub async fn connect(dsn: &str) -> Result<PgPool, ControlError> { todo!() }

/// Apply every migration in `migrations/control/` idempotently.
pub async fn run_migrations(pool: &PgPool) -> Result<(), ControlError> {
    // sqlx::migrate!("../../migrations/control").run(pool).await ...
    todo!()
}
```

```rust
// crates/control/tests/migrations.rs   (compose-gated: requires the control PG from PR 0.6)
#[tokio::test]
async fn migrations_create_all_tables() { todo!() }

#[tokio::test]
async fn file_manifest_partial_index_is_ready_only() { todo!() /* assert index def contains WHERE status='ready' */ }

#[tokio::test]
async fn checkpoint_check_rejects_transformed_ahead_of_raw() { todo!() /* INSERT violating CHECK → error */ }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `0001_control_schema.sql` creates all five contract tables with the exact column names/types.
- [ ] `file_manifest` has the **partial** claim index keyed `(epoch, source_schema, source_table, lsn_end, id) WHERE status='ready'`.
- [ ] `loader_checkpoint` has the composite PK and the `CHECK (transformed_lsn <= raw_appended_lsn)`.
- [ ] `run_migrations()` is idempotent (re-running is a no-op) and errors are terminal/transient-classified.
- [ ] Comments note the watermarks are **commit-LSN valued** and the manifest is a **queue** (rows deleted).
- [ ] `cargo sqlx prepare --check` passes; `.sqlx/` is committed and CI runs the offline check.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p control` (unit) and — with services — `docker compose up --wait` then
        `cargo test -p control --test migrations` proves the schema + the CHECK rejection.

## Hints & gotchas

- **`sqlx::migrate!` takes a path relative to the crate's `Cargo.toml`**, not the workspace root — from
  `crates/control` that's `"../../migrations/control"`. Get this wrong and it compiles but finds no migrations.
- **Use `pg_lsn`, not `text`, for LSN columns.** `pg_lsn` sorts as a WAL position and is the type the
  §1.4 contract specifies; your `Lsn` newtype (PR 0.3) already renders the compatible `X/Y` / 16-hex forms.
- **CI needs a `DATABASE_URL`** at prepare time (a throwaway compose PG) for `sqlx prepare`. The offline
  `.sqlx/` cache is what lets *other* devs and the non-DB CI job build without one.
- **`bigserial` PK on `file_manifest.id` is load-bearing** — it's the `(lsn_end, id)` tiebreaker that keeps
  equal-`lsn_end` snapshot files from being skipped (PR 1.4). Don't "simplify" it to a random UUID.
- Turn on the integration CI job here (README "CI grows" row for PR 1.3): compose up → migrate → assert →
  down. Keep the test `#[ignore]`-free but gated on an env flag if your unit CI can't reach Postgres.

## References

- Design: `../../architecture.md` "Coordination contract (control-plane tables)", §1.4 / "manifest is a
  work queue"; `../../walrus-loader.md` §2.
- Prev: [PR 1.2](./pr-1.2-common-pg-shape-types.md) · Next: [PR 1.4](./pr-1.4-control-file-manifest.md) ·
  [Roadmap](../README.md)
