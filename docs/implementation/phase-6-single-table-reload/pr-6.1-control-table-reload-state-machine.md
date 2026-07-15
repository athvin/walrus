# PR 6.1 — control-plane state machine: `walrus.table_reload` + manifest `kind='reload'`

> **Status:** 📋 Planned

> **Phase:** 6 — single-table reload · **Crates touched:** `control`, `migrations/control` ·
> **Est. size:** M · **Depends on:** PR 5.9 · **Unlocks:** PR 6.2

The reload's brain lives in **control-pg, not the source database** — the loader has no source-DB
credentials and never will ([reload H4](../../single-table-reload.md#h4--one-status-row-in-the-source-db-written-by-three-services-is-the-wrong-state-store)).
This PR builds that brain: the `walrus.table_reload` state machine
(`requested → exporting → export_complete → complete`, `failed` terminal from the middle), a
partial unique index that makes "one non-terminal reload per table" a database guarantee, and the
`file_manifest` extension (`kind='reload'` + a nullable `reload_id`) that lets chunk files sort
into the loader's existing claim order. One deliberate deviation from the design doc: `reload_id`
is a **`bigserial`, not a UUID** — "latest reload_id wins" (H9 restart hygiene) becomes a numeric
comparison, and it fits the loader's `_walrus_meta` `v BIGINT` store verbatim; duplicate-request
idempotency moves to the partial unique index, which gives the same guarantee the UUID key was for.

## Why — learning objectives

By the end of this PR you will have practised:

- **State machines in SQL + Rust** — transitions as `UPDATE … WHERE status = $expected` so a lost
  race changes zero rows and surfaces as a typed error, never a silent double-claim.
- **Partial unique indexes as invariants** — `UNIQUE (schema, table) WHERE status NOT IN
  ('complete','failed')` turns an application rule into a DB guarantee.
- **Idempotency-key tradeoffs** — why a monotonic `bigserial` beats a UUID when "latest wins" is a
  correctness rule, and what you give up (client-supplied idempotency).
- **Transactional cleanup coupling** — `fail()` purges the reload's manifest rows in the *same*
  transaction, so a failed reload can never leave chunk files for the loader to trip on.

## Read first

- `../../single-table-reload.md` — H4/H5 (why control-pg owns state and signals are insert-only),
  H8 (the manifest interplay this PR's columns enable), H10 (the status set and who flips what),
  §5 (the end-to-end flow these states carry).
- `migrations/control/0001_control_schema.sql` + `0003_table_ownership.sql` — house migration
  style: numbered, idempotent, commented; `file_manifest`'s current columns (`kind` is already
  `'snapshot' | 'stream'`).
- `crates/control/src/manifest.rs` — the model/query idiom this PR's `reload.rs` mirrors
  (`sqlx::query_as!`, `Lsn` column casts, `ControlError`).
- `crates/control/src/checkpoint.rs` — the CHECK-guarded watermark style; `table_reload`'s
  LSN columns follow the same discipline.

## Scope

**In scope**

- `migrations/control/0004_table_reload.sql`: the `walrus.table_reload` table (columns in the
  skeleton), the partial unique index, and `ALTER TABLE walrus.file_manifest ADD COLUMN reload_id
  bigint` (nullable — stream/snapshot rows never set it). Document `'reload'` as a third `kind`
  value wherever the current two are documented (add it to the CHECK constraint if one exists).
- `crates/control/src/reload.rs`: `ReloadRow` / `ReloadFlavor` / `ReloadStatus` models and the
  transition functions — `request`, `claim_requested` (lease + `requested → exporting`),
  `renew_lease`, `advance_cursor`, `complete_export`, `complete`, `fail` (with the same-txn
  manifest purge), plus the read paths the other PRs need (`active_rebuilds`, `get`).
- Unit tests against compose control-pg proving the transitions, the unique index, and the purge.

**Explicitly deferred** (do *not* build these here)

- Anything that *drives* the state machine — sink pickup/lease loop → **PR 6.4**, chunk cursor use
  → **PR 6.5**, loader pause → **PR 6.6**, restart-on-DDL's fail-and-reissue → **PR 6.8**.
- The source-side signal table → **PR 6.2**. Nothing in-band lands in this PR.

## Files to create / modify

```
migrations/control/0004_table_reload.sql   # new — table_reload + partial unique idx + manifest reload_id
crates/control/src/reload.rs               # new — models + typed transitions
crates/control/src/lib.rs                  # modify — pub mod reload; re-exports
crates/control/tests/reload.rs             # new — compose integration test (transitions + index + purge)
.sqlx/                                     # regenerate — cargo sqlx prepare (offline CI gate from PR 1.3)
```

## Skeleton

```sql
-- migrations/control/0004_table_reload.sql  (shape)
CREATE TABLE IF NOT EXISTS walrus.table_reload (
    reload_id     bigserial PRIMARY KEY,      -- monotonic: "latest wins" is a numeric max (H9)
    epoch         bigint      NOT NULL,
    source_schema text        NOT NULL,
    source_table  text        NOT NULL,
    flavor        text        NOT NULL CHECK (flavor IN ('reload', 'resync')),
    status        text        NOT NULL DEFAULT 'requested'
                  CHECK (status IN ('requested','exporting','export_complete','complete','failed')),
    chunk_no      bigint      NOT NULL DEFAULT 0,   -- last COMPLETED chunk; 0 = none
    cursor_pk     jsonb,                            -- last PK bound (composite-safe), NULL = start
    first_lsn     pg_lsn,                           -- L₁: the reload's first watermark
    final_lsn     pg_lsn,                           -- H: set at export_complete
    schema_version bigint,                          -- the single version this attempt exports at
    restart_count int         NOT NULL DEFAULT 0,   -- DDL restarts consumed (PR 6.8 caps it)
    lease_holder  text,
    lease_expiry  timestamptz,
    error         text,
    requested_at  timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- One live reload per table — duplicate requests are a constraint violation, not a second export.
CREATE UNIQUE INDEX IF NOT EXISTS table_reload_one_live
    ON walrus.table_reload (epoch, source_schema, source_table)
    WHERE status NOT IN ('complete', 'failed');

ALTER TABLE walrus.file_manifest ADD COLUMN IF NOT EXISTS reload_id bigint;  -- NULL for stream/snapshot
```

```rust
// crates/control/src/reload.rs

use crate::ControlError;
use common::Lsn;
use sqlx::PgExecutor;

/// `reload` rebuilds (clear + re-export, the quarantine-recovery flavor); `resync` merges over the
/// live mirror and tolerates phantoms (H3). They share every state below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadFlavor { Reload, Resync }

/// requested → exporting → export_complete → complete; `failed` terminal from the two middle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReloadStatus { Requested, Exporting, ExportComplete, Complete, Failed }

#[derive(Debug, Clone)]
pub struct ReloadRow { /* mirrors the migration's columns; Lsn for the pg_lsn's */ }

/// INSERT a request. A second non-terminal request for the same table hits the partial unique
/// index — map that to a typed error, don't let it surface as a raw sqlx failure.
pub async fn request(
    executor: impl PgExecutor<'_>,
    epoch: i64,
    source_schema: &str,
    source_table: &str,
    flavor: ReloadFlavor,
) -> Result<i64, ControlError> { todo!() }

/// Claim up to `limit` `requested` rows: set lease, flip to `exporting`. Optimistic — the WHERE
/// carries `status = 'requested'`, so a raced claim returns zero rows and nobody double-exports.
pub async fn claim_requested(
    executor: impl PgExecutor<'_>,
    epoch: i64,
    holder: &str,
    lease_ttl_secs: i64,
    limit: i64,
) -> Result<Vec<ReloadRow>, ControlError> { todo!() }

pub async fn renew_lease(/* … reload_id, holder, ttl */) -> Result<(), ControlError> { todo!() }

/// Record chunk `n` done: bump chunk_no, store the new PK bound; on the FIRST chunk also freeze
/// `first_lsn = L₁` and `schema_version` (both immutable afterwards — assert, don't overwrite).
pub async fn advance_cursor(/* … */) -> Result<(), ControlError> { todo!() }

pub async fn complete_export(/* exporting → export_complete, final_lsn = H */) -> Result<(), ControlError> { todo!() }
pub async fn complete(/* export_complete → complete (the loader calls this, PR 6.9) */) -> Result<(), ControlError> { todo!() }

/// exporting|export_complete → failed, and — in the SAME transaction — delete this reload's
/// `kind='reload'` manifest rows. A failed reload must leave nothing for the loader to claim (H9).
pub async fn fail(/* … reload_id, reason */) -> Result<(), ControlError> { todo!() }

/// Tables with a live `flavor='reload'` reload — the loader-pause predicate's input (PR 6.6).
pub async fn active_rebuilds(/* … epoch */) -> Result<Vec<ReloadRow>, ControlError> { todo!() }

#[cfg(test)]
mod tests {
    // shapes only — these run against compose control-pg (see crates/control/tests/reload.rs)
}
```

```rust
// crates/control/tests/reload.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn full_status_walk_and_duplicate_request_rejected() { todo!() }

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn wrong_state_transition_changes_zero_rows() { todo!() }

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn fail_purges_this_reloads_manifest_rows_only() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Migration `0004` applies cleanly to both a fresh control DB and one already at `0003`;
      existing `file_manifest` rows are untouched (`reload_id IS NULL`).
- [ ] A second `request()` for a table with a live reload returns a **typed** already-in-progress
      error (unique-violation mapped, not a raw `sqlx::Error`); after `complete`/`failed` a new
      request succeeds.
- [ ] Every transition is a guarded UPDATE: calling `complete_export` on a `requested` row (or any
      other illegal jump) changes zero rows and errors — asserted by test.
- [ ] `advance_cursor` freezes `first_lsn` and `schema_version` on chunk 1 and never overwrites them.
- [ ] `fail()` deletes exactly that reload's `kind='reload'` manifest rows in the same transaction —
      other reloads' and stream/snapshot rows survive (asserted).
- [ ] `insert_ready` accepts `kind='reload'` with a `reload_id`; claim order (`lsn_end, id`) is
      unchanged.
- [ ] Docs/comments explain the bigserial-over-UUID decision and the partial-index invariant.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p control` (and `--workspace` stays green); `cargo sqlx prepare --check`
  - [ ] `docker compose up --wait` then `cargo test -p control --test reload -- --ignored`

## What completed looks like

```
$ docker compose up --wait && cargo test -p control --test reload -- --ignored   # all pass, then:

$ psql $CONTROL_URL -c "INSERT INTO walrus.table_reload (epoch, source_schema, source_table, flavor)
                        VALUES (1, 'public', 'orders', 'reload') RETURNING reload_id, status"
 reload_id | status
-----------+-----------
         1 | requested

$ psql $CONTROL_URL -c "INSERT INTO walrus.table_reload (epoch, source_schema, source_table, flavor)
                        VALUES (1, 'public', 'orders', 'reload')"
ERROR:  duplicate key value violates unique constraint "table_reload_one_live"
```

The status walk itself (`requested → exporting → export_complete → complete`) is only reachable
through the Rust transition functions — the integration test's output is the demo for that.

## Hints & gotchas

- Model `ReloadStatus`/`ReloadFlavor` as Rust enums with explicit `as_str`/`from_str` (or sqlx
  `Type`), and keep the SQL CHECKs as the second line of defense — belt and braces, like
  `loader_checkpoint`'s CHECK.
- `updated_at` won't maintain itself — either set it in every UPDATE or add a trigger; pick one and
  say why in a comment (the house has no trigger precedent; leaning on explicit SET is consistent).
- The partial unique index means `request()` can race another request and lose — map
  `sqlx::Error::Database` with the constraint name to your typed error; don't string-match the
  message.
- `fail()`'s purge needs `DELETE FROM walrus.file_manifest WHERE reload_id = $1` — that's the
  payoff of the nullable column; no `kind` filter needed since only reload files carry a
  `reload_id`. Consider an index on `reload_id` if you expect large chunk counts.
- Don't add a `superseded` status for DDL restarts — PR 6.8 reuses `failed` with an explanatory
  `error` text, keeping the status set at five. Leave a comment breadcrumb.

## References

- Design: `../../single-table-reload.md` H4, H5, H8, H10, §5;
  `../../architecture.md#18-single-slot-for-life--total-restart` (quarantine — the customer);
  `../../deferred-goals.md#1-single-table-reload--re-sync-while-streaming`.
- Prev: [PR 5.9](../phase-5-performance-and-ci/pr-5.9-dependency-debt-sweep.md) *(phase boundary →
  Phase 6 single-table reload)* · Next: [PR 6.2](./pr-6.2-source-reload-signal-table.md) ·
  [Roadmap](../README.md)
