# PR 3.9 — DDL apply: destructive changes (DROP COLUMN · lossy type → quarantine · DROP TABLE)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/62

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader`, `control` · **Est. size:** M ·
> **Depends on:** PR 3.8 · **Unlocks:** PR 3.10

The destructive half of schema evolution — where mirror and raw **diverge** most. `DROP COLUMN`
physically drops the column from `<table>` but the raw log **retains** it (nullable) to preserve verbatim
history. A **lossy / incompatible** `ALTER COLUMN TYPE` attempts the in-place cast on the mirror and, on
failure, **quarantines the table + alerts + stops** (an accepted terminal outcome in v1) while raw is
**widened to `VARCHAR`** so rows of multiple `schema_version`s coexist and no cast destroys history.
`DROP TABLE` retires both DuckDB tables and the file. This PR completes the taxonomy.

## Why — learning objectives

By the end of this PR you will have practised:

- **Mirror-vs-raw divergence** — physical drop on silver, retained-nullable on bronze; widen-to-VARCHAR
  on raw vs in-place cast on mirror.
- **Fail-safe quarantine** — treating a failed lossy cast as a **terminal, alerting** state rather than
  silently corrupting or dropping data.
- **Never re-cast history** — raw widens types (VARCHAR) but never re-casts existing stored values.
- **Table retirement** — dropping/retiring a `.duckdb` file's two tables on `DROP TABLE`.

## Read first

- `../../architecture.md#per-change-type-handling-schema-evolution-semantics` — the `DROP COLUMN`,
  `ALTER COLUMN TYPE (lossy/incompatible)`, and `DROP TABLE` rows (mirror action vs raw action).
- `../../walrus-pg-sink.md#3-ddl-capture--the-sinks-tap-on-the-source` — how the DDL event and the
  resulting column set were captured (the `sql_drop` trigger, the schema snapshot).
- `../../walrus-loader.md#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild` — mirror = exact
  current shape (physical drops, in-place casts); raw = additive superset that never destructively
  drops/casts.

## Scope

**In scope**

- Extend the DDL applier (PR 3.8) with destructive classes: `DROP COLUMN` (physical on mirror,
  retained-nullable on raw), lossy `ALTER COLUMN TYPE` (attempt mirror cast → on failure quarantine +
  alert + stop; raw widened to `VARCHAR`, no re-cast), `DROP TABLE` (retire both tables + file).
- A `quarantine` terminal state + alert path (surfaced on `/ready` degraded + a metric/log).
- Version-gated apply, same as additive: destructive DDL applied before crossing its LSN.

**Explicitly deferred** (do *not* build these here)

- Single-table reload out of quarantine (explicitly out of scope in v1) — quarantine is terminal.
- The periodic full-rebuild that would rebuild a healed table → **PR 3.11**.
- `CREATE TABLE` (add-to-publication) new-file bootstrap → registry/bootstrap path (reference only).

## Files to create / modify

```
crates/loader/src/ddl.rs           # modify — DestructiveChange + apply; quarantine on lossy failure
crates/loader/src/health.rs        # modify — /ready 'degraded' + quarantine surfacing
crates/control/src/ddl_manifest.rs # modify — record quarantine state / alert flag
crates/loader/tests/ddl_destructive.rs # new — compose/unit incl. quarantine terminal + alert
```

## Skeleton

```rust
// crates/loader/src/ddl.rs  (extends PR 3.8)
pub enum DestructiveChange {
    DropColumn { attnum: i32, name: String },        // mirror: physical DROP; raw: retain nullable
    LossyType  { attnum: i32, name: String, new_ty: common::TypeDescriptor }, // mirror cast | quarantine
    DropTable  { name: String },                     // retire both tables + file
}

/// Apply destructive changes. A lossy cast that fails on the mirror → Err(LoaderError::Quarantine …).
pub fn apply_destructive(conn: &duckdb::Connection, table: &str, changes: &[DestructiveChange])
    -> Result<(), LoaderError> {
    // DROP COLUMN: ALTER <table> DROP COLUMN;  <table>_raw keeps the col (nullable, retained history)
    // LOSSY TYPE : try ALTER <table> ALTER COLUMN ... TYPE ...;  on failure -> quarantine + alert + stop
    //              <table>_raw: ALTER ... TYPE VARCHAR (widen only; never re-cast existing rows)
    // DROP TABLE : drop <table> and <table>_raw; retire the file
    todo!()
}
```

```rust
// crates/loader/tests/ddl_destructive.rs
#[test] fn drop_column_physical_on_mirror_retained_nullable_on_raw() { todo!() }   // hermetic
#[test] fn lossy_type_change_widens_raw_to_varchar_without_recasting() { todo!() } // hermetic
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn lossy_cast_failure_quarantines_the_table_and_alerts() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn drop_table_retires_both_tables_and_the_file() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `DROP COLUMN` → the column is **physically dropped** from `<table>` but **retained nullable** in
      `<table>_raw`; post-drop files fill it NULL.
- [x] A **lossy/incompatible** `ALTER COLUMN TYPE` → the mirror cast is attempted; on failure the table
      is **quarantined + an alert fires + processing stops** (terminal, accepted v1 outcome); `<table>_raw`
      is **widened to `VARCHAR`** and existing rows are **never re-cast**.
- [x] `DROP TABLE` → both `<table>` and `<table>_raw` are retired and the file dropped.
- [x] Quarantine surfaces on `/ready` (degraded) and a metric/log, and does not silently continue.
- [x] Destructive DDL is applied **before** crossing its LSN (same version-gating as PR 3.8).
- [x] Raw never destructively drops or casts history — only additive widening.
- [x] Docs/comments state that single-table reload out of quarantine is out of scope in v1.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p loader --test ddl_destructive -- --ignored`
        asserting **`lossy_cast_failure_quarantines_the_table_and_alerts`**.

## Hints & gotchas

- The asymmetry is the whole point: **mirror = exact current shape** (so it drops/casts), **raw =
  history superset** (so it retains/widens). Getting these backwards silently loses CDC history or
  corrupts the current-state mirror.
- Widen raw to `VARCHAR` on a lossy change so rows of *both* `schema_version`s coexist in one column;
  never issue a `CAST` that could fail on historical raw values.
- Quarantine must be **loud** — a `CrashLoop`-adjacent terminal that pages someone, not a warning log.
  Route it through the same alert channel as the sink's fatal preflight failures.
- A failed in-place mirror cast can leave the mirror partially altered depending on the DuckDB version —
  wrap the attempt so a failure rolls back the mirror to its pre-cast shape before quarantining.
- Keep `DROP TABLE` idempotent (`IF EXISTS`) so a re-run after a crash mid-retire completes cleanly.

## References

- Design: `../../architecture.md#per-change-type-handling-schema-evolution-semantics`;
  `../../walrus-pg-sink.md#3-ddl-capture--the-sinks-tap-on-the-source`;
  `../../walrus-loader.md#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild`.
- Prev: [PR 3.8](./pr-3.8-loader-ddl-additive.md) ·
  Next: [PR 3.10](./pr-3.10-loader-snapshot-stream-boundary.md) · [Roadmap](../README.md)
