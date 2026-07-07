<!--
  Task file — follows ../TEMPLATE.md. Spec + skeleton only; the learner writes the logic.
-->

# PR 1.4 — `file_manifest` models: insert-ready, claim-in-commit-order, delete-claimed

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/18

> **Phase:** 1 — Shared core · **Crates touched:** `control` · **Est. size:** M ·
> **Depends on:** PR 1.3 · **Unlocks:** PR 2.25, PR 3.2

The `file_manifest` is a **work queue, not a history**. This PR gives `control` the three operations
that make that true: the sink's `insert_ready`, the loader's load-bearing `claim_ready(…) ORDER BY
lsn_end, id`, and `delete_claimed`. The single most important line in the whole file is the ordering:
**commit LSN, then `id` as the tiebreaker** — never max-row-LSN, or a late-committing large txn gets
silently dropped, and never `lsn_end > watermark`, or the many snapshot files that share
`consistent_point` get skipped.

## Why — learning objectives

By the end of this PR you will have practised:

- **`sqlx` typed queries** — `query_as!` mapping rows into a `ManifestRow` struct, checked at compile time.
- **Encoding a queue claim correctly** — `WHERE status='ready' ORDER BY lsn_end, id LIMIT n`, and *why*
  it is not `lsn_end > raw_appended_lsn`.
- **Batch delete with `= ANY($1)`** — retiring a set of claimed rows in one statement.
- **Commit-LSN ordering as a correctness property** — proving the `id` tiebreak on equal `lsn_end`.

## Read first

- `../../walrus-loader.md` §2 "The work-handoff contract" — the exact **claim query** (the load-bearing
  one), the note that it is *not* `lsn_end > raw_appended_lsn`, and "Why `lsn_end` is a COMMIT LSN".
- `../../architecture.md` "Coordination contract" / "Lifecycle — the manifest is a work queue" — insert
  `ready` after S3 durable; retire by **delete**, not a terminal state; `status='failed'` dead-letter.
- `../../proto-version.md` §10 "Interleaving and commit ordering" — the empirical proof that a txn which
  *started second committed first*, which is why ordering must key on commit LSN.

## Scope

**In scope**

- `ManifestRow` struct mirroring the readable columns (`id, epoch, source_schema, source_table, s3_uri,
  kind, row_count, lsn_start, lsn_end, schema_version, status`).
- `insert_ready(pool, NewManifestFile) -> id` — writes a `status='ready'` row with `lsn_end` = commit LSN.
- `claim_ready(pool, epoch, schema, table, limit) -> Vec<ManifestRow>` — `ORDER BY lsn_end, id`.
- `delete_claimed(txn, &[id])` — batch delete via `= ANY($1)`.
- `mark_failed(pool, id)` — dead-letter a poison file so it can't block the queue.
- A compose test proving claim order (incl. the equal-`lsn_end` `id` tiebreak) then delete.

**Explicitly deferred** (do *not* build these here)

- Advancing `raw_appended_lsn` in the *same* control txn as `delete_claimed` → **PR 3.2** (loader wires it).
- The actual S3 PUT and manifest insert from the sink → **PRs 2.24 / 2.25**.
- Partitioning `file_manifest` by day for bloat control → later ops (out of v1 scope).

## Files to create / modify

```
crates/control/src/manifest.rs    # new — ManifestRow, NewManifestFile, insert_ready, claim_ready, …
crates/control/src/lib.rs         # + pub mod manifest;
crates/control/tests/manifest.rs  # new — compose-gated: claim order + id tiebreak + delete
crates/control/.sqlx/             # regenerate offline query data for the new queries
```

## Skeleton

```rust
// crates/control/src/manifest.rs
use sqlx::postgres::{PgPool, PgExecutor};
use common::Lsn;   // PR 0.3

/// A `ready` file the loader can claim. Column set = what the claim query reads.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ManifestRow {
    pub id: i64,
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub s3_uri: String,
    pub kind: String,          // 'snapshot' | 'stream'  (consider a Kind enum reuse from common)
    pub row_count: i64,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,          // COMMIT LSN of the file's last txn
    pub schema_version: i64,
    pub status: String,        // 'ready' | 'failed'
}

/// What the sink inserts after its Parquet is durable in S3 (PR 2.25).
#[derive(Debug, Clone)]
pub struct NewManifestFile {
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub s3_uri: String,
    pub kind: String,
    pub row_count: i64,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub schema_version: i64,
}

/// Insert a `status='ready'` row; returns the new `id`.
pub async fn insert_ready(pool: impl PgExecutor<'_>, f: &NewManifestFile) -> Result<i64, crate::ControlError> { todo!() }

/// Claim the next `ready` files for a table IN COMMIT ORDER.
/// MUST be `ORDER BY lsn_end, id` — `id` breaks equal-`lsn_end` ties (snapshot files share consistent_point).
/// MUST NOT filter `lsn_end > raw_appended_lsn` (that would skip those equal-`lsn_end` files).
pub async fn claim_ready(
    pool: impl PgExecutor<'_>,
    epoch: i64,
    source_schema: &str,
    source_table: &str,
    limit: i64,
) -> Result<Vec<ManifestRow>, crate::ControlError> { todo!() }

/// Retire claimed rows — the queue's "done" is a DELETE, not a status flip.
pub async fn delete_claimed(txn: impl PgExecutor<'_>, ids: &[i64]) -> Result<u64, crate::ControlError> { todo!() }

/// Dead-letter a repeatedly-failing file so it can't block the queue.
pub async fn mark_failed(pool: impl PgExecutor<'_>, id: i64) -> Result<(), crate::ControlError> { todo!() }

#[cfg(test)]
mod tests { /* unit-level: nothing requires a DB here; DB assertions live in tests/manifest.rs */ }
```

```rust
// crates/control/tests/manifest.rs   (compose-gated)
#[tokio::test]
async fn claim_orders_by_lsn_end_then_id() {
    // Insert files with lsn_ends out of order + two files sharing one lsn_end; assert claim order
    // is (lsn_end ASC, id ASC), so the equal-lsn_end pair is claimed low-id first. todo!()
}

#[tokio::test]
async fn claim_does_not_skip_equal_lsn_end_snapshot_files() { todo!() }

#[tokio::test]
async fn delete_claimed_retires_exactly_the_given_ids() { todo!() }

#[tokio::test]
async fn mark_failed_removes_row_from_ready_claims() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `claim_ready` emits `WHERE status='ready' … ORDER BY lsn_end, id LIMIT $n` and is proven to
      order two equal-`lsn_end` files by ascending `id`.
- [x] There is **no** `lsn_end > raw_appended_lsn` predicate anywhere in the claim path (comment says why).
- [x] `insert_ready` writes `status='ready'` with `lsn_end` set to the commit LSN and returns the `id`.
- [x] `delete_claimed(&ids)` retires exactly those rows via `= ANY($1)`; `mark_failed` dead-letters one.
- [x] Compose test: seed manifest rows, claim in order (incl. tiebreak), then delete — all green.
- [x] `.sqlx/` regenerated; `cargo sqlx prepare --check` passes.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p control` and — with services — `docker compose up --wait` then
        `cargo test -p control --test manifest` (the named claim-order assertion passes).

## Hints & gotchas

- **The claim is not a `SELECT … FOR UPDATE SKIP LOCKED`** in v1 — there is exactly one loader worker per
  table (single-writer, PR 3.1), so a plain ordered `SELECT` is correct. Don't add locking the design
  doesn't ask for; note it as a sharding hook (PR 4.11) instead.
- **`ORDER BY lsn_end, id` uses the partial index** from PR 1.3 — verify the planner picks it (`EXPLAIN`),
  because that index's `WHERE status='ready'` is what keeps the scan cheap as the queue drains.
- **Deleting is the frontier advance, not the watermark.** Resist the urge to also bump `raw_appended_lsn`
  here — that must happen in the *loader's* control txn (PR 3.2) alongside the delete, after the DuckDB
  append commits. This model just provides `delete_claimed`.
- **`pg_lsn` ↔ `Lsn`:** implement `sqlx::Decode`/`Encode` for your `Lsn` newtype (or map via `PgLsn`) so
  `query_as!` binds cleanly; test that a round-tripped `lsn_end` keeps its zero-padded ordering.

## References

- Design: `../../walrus-loader.md` §2 (claim query + commit-LSN ordering), `../../architecture.md`
  "Coordination contract" / "manifest is a work queue", `../../proto-version.md` §10.
- Prev: [PR 1.3](./pr-1.3-control-migrations.md) ·
  Next: [PR 1.5](./pr-1.5-control-checkpoint-replication-state.md) · [Roadmap](../README.md)
