# PR 2.29 — Backfill existing rows from an exported snapshot, then stream

> **Phase:** 2 — walrus-pg-sink · **Crates touched:** `pg-sink`, `common` · **Est. size:** L ·
> **Depends on:** PR 2.28 · **Unlocks:** PR 2.30

Streaming from a fresh slot only sees changes *after* the slot's consistent point — the rows that
already exist must be backfilled. This PR creates the slot with an **exported snapshot**, captures its
`consistent_point` LSN and `snapshot_name`, and copies every published table under
`REPEATABLE READ` + `SET TRANSACTION SNAPSHOT` so the backfill is a single consistent MVCC read. Those
rows flow through the *same* Arrow → Parquet → S3 → manifest path as streamed changes — marked
`kind='snapshot'`, all sharing `lsn_end = consistent_point` — after which the sink streams from that
same point. There is no "COPY at an LSN": consistency comes from the snapshot, not a time-travel read.

## Why — learning objectives

By the end of this PR you will have practised:

- **`CREATE_REPLICATION_SLOT … (SNAPSHOT 'export')`** on a replication connection, and why the SQL
  helper `pg_create_logical_replication_slot()` (no `snapshot_name`) cannot be used here.
- **Exported-snapshot lifetime** — the connection must stay **open and idle** and every backfill
  session must `SET TRANSACTION SNAPSHOT` before it closes.
- **Streaming `COPY`** via `tokio-postgres` under a read-only `REPEATABLE READ` transaction, feeding
  the shared Arrow batch path.
- **Snapshot/stream provenance** — `kind='snapshot'`, shared `lsn_end`, disambiguated downstream by
  `manifest_id`, never by `lsn_end` alone.

## Read first

- `../../architecture.md#17-snapshot--backfill-bootstrap` — the four-step bootstrap: export snapshot,
  per-table copy under `SET TRANSACTION SNAPSHOT`, (deferred) parallel CTID copy, and the watermark
  handoff where snapshot files share `lsn_end = consistent_point`.
- `../../architecture.md#18-single-slot-for-life--total-restart` — one slot for life; the snapshot is
  taken exactly once at first bootstrap (and again only on the total-restart disaster path).
- `../../architecture.md#14-arrow-conversion--parquet-write` — the shared meta/`SinkMeta` shape the
  snapshot rows carry (`kind`, `commit_lsn`).

## Scope

**In scope**

- `create_slot_with_snapshot` → `ExportedSnapshot { consistent_point: Lsn, snapshot_name: String }`,
  keeping the replication connection open+idle.
- Per-table serial `Backfill::copy_table`: a read-only `REPEATABLE READ` txn, `SET TRANSACTION
  SNAPSHOT '<name>'`, streaming `COPY`/`SELECT` → the existing Arrow batch path.
- Stamping snapshot rows `kind='snapshot'`, `commit_lsn = consistent_point`; manifest rows
  `kind='snapshot'`, `lsn_end = consistent_point` (equal across all snapshot files, `id`-disambiguated).
- Handing off to the streaming loop **from** `consistent_point` after backfill completes.

**Explicitly deferred** (do *not* build these here)

- **Parallel CTID-range copy** (PeerDB-style, per-range `SET TRANSACTION SNAPSHOT`) → **PR 4.11**
  (Open Q9). The serial copy here changes nothing about the watermark handoff.
- The loader collapsing snapshot/stream overlap via the transform → **PR 3.10**.
- Re-snapshot on epoch bump / total-restart → **PR 4.6**.

## Files to create / modify

```
crates/pg-sink/src/snapshot.rs       # new — ExportedSnapshot, SnapshotConn, Backfill
crates/pg-sink/src/sink.rs           # modify — bootstrap: snapshot → backfill → stream from point
crates/pg-sink/src/config.rs         # modify — backfill copy batch size / statement_timeout
crates/pg-sink/src/lib.rs            # modify — `pub mod snapshot;`
crates/pg-sink/tests/snapshot_backfill.rs   # new — compose integration test
# no new Cargo deps (tokio-postgres COPY already present from PR 2.19)
```

## Skeleton

```rust
// crates/pg-sink/src/snapshot.rs
use common::{Lsn, PgRelation};

/// Returned by CREATE_REPLICATION_SLOT … (SNAPSHOT 'export').
#[derive(Debug, Clone)]
pub struct ExportedSnapshot {
    pub consistent_point: Lsn,
    pub snapshot_name: String,
}

/// Holds the replication connection that MUST stay open + idle so the exported
/// snapshot remains valid until every backfill session has attached to it.
pub struct SnapshotConn { /* repl connection handle */ }

impl SnapshotConn {
    /// CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput (SNAPSHOT 'export').
    pub async fn create_slot_with_snapshot(&mut self, slot: &str) -> Result<ExportedSnapshot, crate::Error> { todo!() }
}

/// One serial per-table copy under the exported snapshot.
pub struct Backfill<'a> { /* ordinary SQL client + sink batch path */ _p: std::marker::PhantomData<&'a ()> }

impl Backfill<'_> {
    /// BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT '<name>';
    /// then stream COPY <rel> → Arrow batches marked kind='snapshot', commit_lsn=consistent_point.
    /// Returns rows copied.
    pub async fn copy_table(&mut self, rel: &PgRelation, snap: &ExportedSnapshot) -> Result<u64, crate::Error> { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn snapshot_rows_carry_kind_snapshot_and_consistent_point_commit_lsn() { todo!() }
    #[test] fn copy_sql_uses_repeatable_read_and_set_transaction_snapshot() { todo!() }
    #[test] fn all_snapshot_manifest_files_share_lsn_end() { todo!() }
}
```

```rust
// crates/pg-sink/tests/snapshot_backfill.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn backfill_preloaded_rows_then_streams_post_consistent_point() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Preloaded rows in `orders`/`customers` produce `kind='snapshot'` manifest rows and snapshot
      Parquet objects; **all snapshot files share one `lsn_end = consistent_point`**, disambiguated by
      manifest `id`.
- [ ] Each table's copy runs in a read-only `REPEATABLE READ` transaction with
      `SET TRANSACTION SNAPSHOT '<snapshot_name>'` — asserted on the emitted SQL.
- [ ] A row written **during** backfill is not double-counted: it appears as a post-`consistent_point`
      **stream** change once streaming begins, not in the snapshot files.
- [ ] The replication connection stays open+idle for the snapshot's lifetime; a copy session that
      attaches after it closes is a terminal error (documented).
- [ ] Docs/comments state "no COPY at an LSN — consistency is the exported MVCC snapshot".
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test snapshot_backfill -- --ignored`
        asserting **`backfill_preloaded_rows_then_streams_post_consistent_point`**.

## Hints & gotchas

- The exported snapshot dies the moment **any other command** runs on the slot-creating connection or
  it closes — do the `CREATE_REPLICATION_SLOT`, capture the two values, then leave that connection
  strictly idle while the copy sessions attach.
- `walrus.heartbeat` and `walrus.ddl_audit` are **internal** — do **not** snapshot/backfill them (the
  generic "published CREATE TABLE → snapshot" rule excludes them; see the per-change-type table).
- Stream the `COPY` — don't buffer a whole large table in memory; feed it through the same
  `max_bytes`/`max_rows` batch caps as PR 2.23 so backfill respects the memory budget too.
- Snapshot rows have **no per-row commit boundary** in the source sense; set `commit_lsn =
  consistent_point` for every one so the loader's `(commit_lsn, lsn)` dedup lets any later streamed
  change win.

## References

- Design: `../../architecture.md#17-snapshot--backfill-bootstrap`,
  `#18-single-slot-for-life--total-restart`, `#14-arrow-conversion--parquet-write`.
- Prev: [PR 2.28](./pr-2.28-sink-graceful-shutdown.md) ·
  Next: [PR 2.30](./pr-2.30-sink-streaming-large-txn.md) · [Roadmap](../README.md)
