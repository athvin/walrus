# PR 2.19 — Source-side preflight (`wal_level`, headroom, publication, PK)

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib), `common` ·
> **Est. size:** M · **Depends on:** PR 2.18 · **Unlocks:** PR 2.20

Bootstrap step **`walrus-pg-sink` 1–3 + 6**: open a **replication-capable** connection to the source,
then assert every server-side precondition before a single byte of WAL is read — `wal_level = logical`,
server ≥ 14, slot / wal-sender headroom, that `walrus_pub` exists and **covers the configured tables
plus `walrus.ddl_audit` and `walrus.heartbeat`**, and that every published user table has a `PRIMARY
KEY` with a usable replica identity. Any mismatch is **terminal** — loud, greppable, `CrashLoopBackOff`.

## Why — learning objectives

By the end of this PR you will have practised:

- **`tokio-postgres` on a replication-mode connection** — connecting with `replication=database` and
  running ordinary catalog `SELECT`s over it (the same connection that will later `START_REPLICATION`).
- **Catalog introspection** — `pg_settings`, `server_version_num`, `pg_replication_slots`,
  `pg_publication` / `pg_publication_tables`, `pg_index` / `pg_class.relreplident`.
- **Terminal-error modelling** — turning "observed vs expected" mismatches into a `thiserror` variant
  that carries *which* precondition failed, mapped to a distinct `ExitCode`.
- **Idempotent SQL migrations** — `migrations/source/0001_publication.sql` as the one-time source setup
  the operator (or `manage_publication=true`) applies.

## Read first

- `../../architecture.md` §1.1 Source-side setup (`#11-source-side-setup-one-time-via-migrationjob`) —
  the `CREATE PUBLICATION` shape, the **mandatory-PK / REPLICA IDENTITY DEFAULT** hard edge, and that
  `walrus.ddl_audit` + `walrus.heartbeat` **must** be in the publication.
- `../../architecture.md` "Startup & bootstrap" `walrus-pg-sink` steps **1, 2, 3, 6**
  (`#startup--bootstrap-fail-fast-preflight`) — the exact assertions and their terminal/transient split.
- `../../walrus-pg-sink.md` §3.7 Preflight (`#37-preflight`) — DDL-capture prerequisite notes (existence
  check only here; installation is PR 2.33).

## Scope

**In scope**

- Replication-capable connect + verify `REPLICATION` privilege (transient if PG down, terminal if no priv).
- Assert `wal_level = logical`; `server_version_num >= 140000`; `max_replication_slots` /
  `max_wal_senders` have headroom over current usage.
- Verify `walrus_pub` exists and its table set **⊇ configured tables ∪ {walrus.ddl_audit, walrus.heartbeat}**
  (create when `manage_publication=true`, else terminal).
- Per-table PK preflight: each published **user** table has a `PRIMARY KEY` and replica identity `DEFAULT`
  (not `NOTHING`); keyless → terminal in `strict` (default), quarantine+alert+continue in `lenient`.
- `migrations/source/0001_publication.sql` (publication + heartbeat table/seed/grant).

**Explicitly deferred** (do *not* build these here)

- Slot verify / `CREATE_REPLICATION_SLOT` / reading `restart_lsn` + `confirmed_flush_lsn` → **PR 2.20**.
- DDL-audit **trigger** existence/install + `migrations/source/0002_ddl_triggers.sql` → **PR 2.33**.
- Heartbeat **write-grant usage** + round-trip → **PR 2.27**.
- Schema-registry hydration (step 7) → **PR 2.22**.

## Files to create / modify

```
crates/pg-sink/Cargo.toml            # + tokio-postgres = "0.7" (features = ["runtime"])
crates/pg-sink/src/preflight.rs      # new — SourcePreflight + assertions
crates/pg-sink/src/bootstrap.rs      # modify — call preflight after shared steps
migrations/source/0001_publication.sql   # new — CREATE PUBLICATION walrus_pub; heartbeat table + seed + grant
crates/pg-sink/tests/preflight.rs    # new — compose: good source passes; wrong wal_level -> terminal
```

## Skeleton

```rust
// crates/pg-sink/src/preflight.rs
use crate::config::SinkConfig;

/// Every server-side precondition the sink asserts before reading WAL (§1.1 / bootstrap 1-3,6).
pub struct SourcePreflight<'a> {
    client: &'a tokio_postgres::Client, // opened with replication=database
    cfg: &'a SinkConfig,
}

impl<'a> SourcePreflight<'a> {
    pub async fn assert_server_prereqs(&self) -> Result<ServerInfo, PreflightError> { todo!() }
    /// wal_level=logical, server_version_num >= 140000, slot/wal_sender headroom.
    pub async fn assert_publication_covers(&self) -> Result<(), PreflightError> { todo!() }
    /// walrus_pub ⊇ configured tables ∪ {ddl_audit, heartbeat}; create if manage_publication.
    pub async fn assert_tables_have_pk(&self, mode: PkMode) -> Result<PkReport, PreflightError> { todo!() }
}

pub struct ServerInfo { pub version_num: i32, pub wal_level: String }
pub enum PkMode { Strict, Lenient }
pub struct PkReport { pub ok: Vec<TableId>, pub quarantined: Vec<TableId> }
pub struct TableId { pub schema: String, pub table: String }

#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    #[error("wal_level is {found}, need 'logical'")]
    WalLevel { found: String },
    #[error("server_version_num {found} < 140000 (proto v2 needs PG14+)")]
    ServerTooOld { found: i32 },
    #[error("no headroom: {kind} {used}/{max}")]
    NoHeadroom { kind: &'static str, used: i32, max: i32 },
    #[error("publication {pub_name} missing table {schema}.{table}")]
    PublicationGap { pub_name: String, schema: String, table: String },
    #[error("table {schema}.{table} has no PRIMARY KEY / usable replica identity")]
    NoPrimaryKey { schema: String, table: String },
    #[error("missing REPLICATION privilege")]
    NoReplicationPriv,
}
```

```sql
-- migrations/source/0001_publication.sql   (idempotent)
CREATE SCHEMA IF NOT EXISTS walrus;
CREATE TABLE IF NOT EXISTS walrus.heartbeat (
  id integer PRIMARY KEY, beat_seq bigint NOT NULL DEFAULT 0,
  ts timestamptz NOT NULL, sink_instance text
);
INSERT INTO walrus.heartbeat (id, ts) VALUES (1, now()) ON CONFLICT DO NOTHING;
-- GRANT INSERT, UPDATE ON walrus.heartbeat TO <sink_role>;
-- CREATE PUBLICATION walrus_pub FOR TABLE ... WITH (publish_via_partition_root = true);
-- ALTER PUBLICATION walrus_pub ADD TABLE walrus.heartbeat;   -- (and walrus.ddl_audit later, PR 2.33)
```

```rust
// crates/pg-sink/tests/preflight.rs
#[tokio::test] async fn good_source_passes_all_assertions() { todo!() }
#[tokio::test] async fn wrong_wal_level_is_terminal() { todo!() }
#[tokio::test] async fn keyless_table_is_terminal_in_strict_mode() { todo!() }
#[tokio::test] async fn publication_missing_heartbeat_is_terminal() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Preflight opens a **replication-capable** connection and runs catalog queries over it.
- [ ] A correctly-configured source (wal_level=logical, PG≥14, headroom, `walrus_pub` covering tables +
      `ddl_audit` + `heartbeat`, every user table PK'd) **passes** with no error.
- [ ] `wal_level != logical`, PG < 14, no slot/wal-sender headroom, a publication gap, or a keyless
      table (strict) each produce the **matching `PreflightError`** and a distinct terminal `ExitCode`.
- [ ] `lenient` mode **quarantines + alerts + continues** on a keyless table (surfaced in `PkReport`).
- [ ] `migrations/source/0001_publication.sql` is idempotent (re-runnable) and creates/seeds
      `walrus.heartbeat`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test preflight`: good source passes,
        a source booted with `wal_level=replica` yields the terminal exit.

## Hints & gotchas

- The replication connection needs `replication=database` in the connect params; you can still run
  plain `SELECT`s on it — that's exactly the same connection PR 2.20 hands to `START_REPLICATION`.
- Read the server version from **`server_version_num`** (an integer like `160002`), not the text
  `version()` — string parsing is where these checks rot.
- Headroom means `count(*) FROM pg_replication_slots < max_replication_slots` **and** the analogous
  wal-sender check — a slot that already exists still counts, so compute *free* slots, not total.
- `pg_publication_tables` already expands `FOR ALL TABLES` and partition roots for you — prefer it over
  joining `pg_publication_rel` by hand.
- Replica identity lives in `pg_class.relreplident` (`d`=default, `n`=nothing, `f`=full, `i`=index);
  the hard edge is **reject `n`** and require a PK for `d`.
- Do **not** create the slot here — a slot needs the exported snapshot from a replication command
  (PR 2.20), and creating it early would strand a snapshot.

## References

- Design: `../../architecture.md` §1.1, "Startup & bootstrap" (`walrus-pg-sink` steps 1–3, 6);
  `../../walrus-pg-sink.md` §3.7.
- Prev: [PR 2.18](./pr-2.18-sink-skeleton-health-shutdown.md) · Next: [PR 2.20](./pr-2.20-sink-replication-connection-keepalive.md) · [Roadmap](../README.md)
