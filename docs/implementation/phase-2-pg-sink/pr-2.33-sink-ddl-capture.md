# PR 2.33 — Consume `ddl_audit` inserts: `ddl_manifest` row, version bump, fresh file

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/53

> **Phase:** 2 — walrus-pg-sink · **Crates touched:** `pg-sink`, `control`, `common`, `migrations/source` ·
> **Est. size:** L · **Depends on:** PR 2.32 · **Unlocks:** PR 3.1

Postgres logical decoding never emits DDL, so the source carries an event-trigger tap: an
`INSERT` into the **published** `walrus.ddl_audit` table rides the *same* slot as DML, in commit order.
This PR teaches the sink to consume that signal: recognise the `ddl_audit` relation OID, and on its
`INSERT` write a `ddl_manifest` row (stamped with the DDL's `c_lsn`), bump the affected table's
structural `schema_version`, and **cut a fresh Parquet file** so every file carries exactly one
`schema_version` (the homogeneous-file rule). The internal tables `ddl_audit` and `heartbeat` are
consumed for control only — **never** materialised as `<table>` / `<table>_raw`.

## Why — learning objectives

By the end of this PR you will have practised:

- **DDL-ordered-inline-with-DML** — why a *published* audit table is the only way to order schema
  changes against data changes with no separate polling channel.
- **Schema-diff, not DDL-text replay** — parsing the structured `c_columns` jsonb payload, not
  re-executing `c_ddl_text`.
- **The homogeneous-file rule** — forcing a file boundary at every structural bump so the loader can
  apply DDL at the correct LSN before the next-version data.
- **Internal-table recognition** — a relation OID set consumed for control and excluded from
  snapshot/materialisation.

## Read first

- `../../walrus-pg-sink.md#3-ddl-capture--the-sinks-tap-on-the-source` — §3.3 the audit table shape,
  §3.4 the two event triggers (`ddl_command_end` + `sql_drop`), §3.5 the sink's three-step consume
  (ddl_manifest row + version bump + cut a fresh file), §3.6 limitations & the Relation-message backstop.
- `../../architecture.md#ddl-capture-schema-evolution` — the `walrus.ddl_audit` DDL, "internal tables
  consumed specially, never materialized", and the structural-`schema_version`-vs-metadata-revision split.
- `../../architecture.md#per-change-type-handling-schema-evolution-semantics` — the per-change-type
  table (the sink only writes the manifest/version here; the loader *applies* the change in PR 3.8/3.9).

## Scope

**In scope**

- `migrations/source/0002_ddl_triggers.sql`: the `walrus.ddl_audit` table, `snapshot_columns()`,
  `intercept_ddl()` (`ddl_command_end`) + `intercept_drop()` (`sql_drop`), the sequence GRANT, and
  `ALTER PUBLICATION walrus_pub ADD TABLE walrus.ddl_audit`.
- Extending `InternalTables` (PR 2.27) with `ddl_audit_oid`.
- `DdlEvent::from_tuple` parsing a decoded `ddl_audit` insert; `DdlConsumer::consume` → write a
  `control` `ddl_manifest` row (`c_lsn`), bump the table's structural `schema_version`, and signal the
  batcher to **cut** the current Parquet file.
- Preflight verification that the audit table + both event triggers exist (missing → terminal).

**Explicitly deferred** (do *not* build these here)

- The loader **applying** the DDL to `<table>`/`<table>_raw` (additive → **PR 3.8**, destructive/lossy
  quarantine → **PR 3.9**).
- The metadata-only revision path (comments/constraints not gating data) beyond recording it →
  covered where the loader mirrors comments (**PR 3.8**).
- The periodic schema-diff audit job (defense in depth) → out of scope for v1 curriculum.

## Files to create / modify

```
migrations/source/0002_ddl_triggers.sql      # new — audit table + 2 event triggers + publication add
crates/pg-sink/src/ddl.rs                    # new — DdlEvent, DdlConsumer
crates/pg-sink/src/heartbeat.rs              # modify — add ddl_audit_oid to InternalTables
crates/pg-sink/src/sink.rs                   # modify — route ddl_audit OID; cut file on bump
crates/pg-sink/src/preflight.rs              # modify — verify audit table + triggers exist
crates/pg-sink/src/lib.rs                    # modify — `pub mod ddl;`
crates/pg-sink/tests/ddl_capture.rs          # new — compose integration test
# no new Cargo deps (control's ddl_manifest/schema_registry models exist from PR 1.6)
```

## Skeleton

```rust
// crates/pg-sink/src/ddl.rs
use common::{Lsn, PgRelation, TupleValue};

/// A decoded walrus.ddl_audit INSERT — the sink's only signal that the schema changed.
#[derive(Debug, Clone)]
pub struct DdlEvent {
    pub c_lsn: Lsn,                          // pg_current_wal_lsn() at capture — orders DDL vs data
    pub c_event: String,                     // 'ddl_command_end' | 'sql_drop'
    pub c_tag: String,                       // 'ALTER TABLE' | 'CREATE TABLE' | 'DROP TABLE' | 'COMMENT' | …
    pub rel_oid: u32,                        // affected pg_class OID
    pub columns: Option<serde_json::Value>,  // structured c_columns payload (the schema-diff input)
    pub dropped: Option<serde_json::Value>,  // c_dropped (sql_drop: dropped column identity / CASCADE)
}

impl DdlEvent {
    /// Extract from the decoded ddl_audit tuple by column position/name.
    pub fn from_tuple(rel: &PgRelation, values: &[TupleValue]) -> Result<Self, crate::Error> { todo!() }

    /// Structural (gates data + cuts a file) vs metadata-only (comment/constraint) revision.
    pub fn is_structural(&self) -> bool { todo!() }
}

pub struct DdlConsumer { /* control-plane client + per-table schema_version state */ }

impl DdlConsumer {
    /// 1) write a ddl_manifest row stamped with c_lsn; 2) bump the table's structural schema_version
    ///    (structural events only); 3) return the new version so the batcher cuts a fresh file.
    pub async fn consume(&mut self, ev: DdlEvent) -> Result<u64, crate::Error> { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn ddl_audit_insert_parses_into_event_with_c_lsn() { todo!() }
    #[test] fn alter_table_is_structural_comment_is_metadata_only() { todo!() }
    #[test] fn consume_writes_ddl_manifest_and_bumps_schema_version() { todo!() }
    #[test] fn internal_tables_ddl_audit_and_heartbeat_are_never_materialized() { todo!() }
}
```

```rust
// crates/pg-sink/tests/ddl_capture.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn alter_add_column_bumps_version_and_cuts_file() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] An `ALTER TABLE … ADD COLUMN` on the source produces a `ddl_manifest` row carrying the **DDL's
      `c_lsn`**, and the affected table's structural `schema_version` bumps by one.
- [x] The next Parquet file for that table carries the **new** `schema_version`; the prior file carries
      the **old** one — **one `schema_version` per file** (homogeneous-file rule), verified via the
      manifest.
- [x] A `COMMENT ON` is recorded but is **metadata-only** — it does **not** bump the structural version
      nor cut a file.
- [x] `walrus.ddl_audit` and `walrus.heartbeat` are recognised as internal and **never** written to a
      `<table>`/`<table>_raw` file or manifest row (beyond their control effects).
- [x] Preflight fails **terminal** if the audit table or either event trigger is missing.
- [x] Docs/comments explain schema-diff-not-replay and why the audit table must be in `walrus_pub`.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test ddl_capture -- --ignored`
        asserting **`alter_add_column_bumps_version_and_cuts_file`**.

## Hints & gotchas

- `ddl_command_end` fires **after execution but pre-commit**, so `snapshot_columns()` reads the
  *already-changed* catalog and the audit `INSERT` enters the WAL in that same transaction — that is
  what gives you the inline commit-order guarantee. Don't add a separate poll.
- Keep the audit row (drop AWS's in-txn DELETE) — a durable, replayable schema history; the sink acts
  on the decoded `INSERT` op regardless.
- A `DROP COLUMN`'s `ALTER` is visible in `ddl_command_end`; only the dropped-column *identity*
  (`object_type 'table column'`, `objsubid = attnum`) and CASCADE victims need the `sql_drop` trigger —
  don't rely on the overstated "empty for drops" claim.
- Event triggers are **not exhaustive** (globals fire nothing; `TRUNCATE` is a native pgoutput message,
  not an audit row). Add the Relation-message drift backstop as a `// TODO` note; full handling is the
  loader's job (3.8/3.9).
- Superuser is needed to `CREATE EVENT TRIGGER`, **not** to create the function; `SECURITY DEFINER` is a
  design choice so a non-owner's DDL still writes the protected audit table.

## References

- Design: `../../walrus-pg-sink.md#3-ddl-capture--the-sinks-tap-on-the-source`,
  `#35-how-the-sink-consumes-it`, `#36-limitations--backstops`;
  `../../architecture.md#ddl-capture-schema-evolution`,
  `#per-change-type-handling-schema-evolution-semantics`.
- Prev: [PR 2.32](./pr-2.32-sink-max-inflight-bytes.md) ·
  Next: [PR 3.1](../phase-3-loader/pr-3.1-loader-skeleton-bootstrap-lease.md) *(phase boundary → Phase 3
  walrus-loader)* · [Roadmap](../README.md)
