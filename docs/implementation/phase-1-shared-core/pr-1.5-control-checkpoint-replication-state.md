<!--
  Task file — follows ../TEMPLATE.md. Spec + skeleton only; the learner writes the logic.
-->

# PR 1.5 — Two-watermark checkpoint + epoch (`loader_checkpoint`, `replication_state`)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/19

> **Phase:** 1 — Shared core · **Crates touched:** `control` · **Est. size:** M ·
> **Depends on:** PR 1.3 · **Unlocks:** PR 3.1, PR 3.2, PR 3.4

The loader tracks its progress with **two commit-LSN watermarks per table**: `raw_appended_lsn`
(Phase A — the CDC log is durable up to here) and `transformed_lsn` (Phase B — the mirror is derived up
to here), bound by `transformed_lsn <= raw_appended_lsn`. This PR gives `control` the upsert/read for
that checkpoint (honoring the CHECK) and the `replication_state` epoch read/insert that namespaces all
state. These are the models PR 3.2 and PR 3.4 advance inside their two independent control transactions.

## Why — learning objectives

By the end of this PR you will have practised:

- **UPSERT with `ON CONFLICT … DO UPDATE`** — advance a watermark for a `(epoch, schema, table)` key,
  inserting the first time.
- **Letting the database enforce an invariant** — the `CHECK (transformed_lsn <= raw_appended_lsn)`
  is your safety net; the model must surface its violation as a typed terminal error, not a panic.
- **Independent-watermark discipline** — Phase A advances `raw_appended_lsn`; Phase B advances
  `transformed_lsn`; they never move together, and the mirror is never ahead of the log.
- **Epoch as a generation id** — every piece of state is namespaced by epoch (§1.8); the loader reads it
  at bootstrap.

## Read first

- `../../architecture.md` "Coordination contract" — `loader_checkpoint` (both watermarks, the CHECK) and
  `replication_state` (epoch PK, `slot_name`, `created_lsn`, `status`).
- `../../walrus-loader.md` §4 "Two-phase apply" — Phase A's control txn advances `raw_appended_lsn`;
  Phase B advances `transformed_lsn` "to the max commit LSN applied"; both are commit-LSN valued.
- `../../architecture.md` §1.4 / "Why every progress key is a COMMIT LSN" — the watermarks watermark on
  commit LSN, never max-row-LSN.

## Scope

**In scope**

- `Checkpoint { epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn }` model.
- `read_checkpoint(...) -> Option<Checkpoint>` and `ensure_checkpoint(...)` (create at zero if absent).
- `advance_raw_appended(txn, key, lsn)` and `advance_transformed(txn, key, lsn)` — separate UPSERTs,
  each taking an executor so the caller controls the transaction.
- `replication_state`: `read_current_epoch(pool) -> Option<ReplicationState>` and
  `insert_epoch(pool, ReplicationState)`.
- A compose test proving the CHECK rejects `transformed_lsn > raw_appended_lsn` and that both advance
  independently and idempotently.

**Explicitly deferred** (do *not* build these here)

- The *ordering* of the Phase-A "advance + delete claimed" in one txn → **PR 3.2** (uses these + PR 1.4).
- Epoch **bump / total-restart** on slot loss → **PR 4.6**.
- Table-ownership lease / fencing token → **PR 3.1**.

## Files to create / modify

```
crates/control/src/checkpoint.rs         # new — Checkpoint, read/ensure/advance_*
crates/control/src/replication_state.rs  # new — ReplicationState, read_current_epoch, insert_epoch
crates/control/src/lib.rs                # + pub mod checkpoint;  pub mod replication_state;
crates/control/tests/checkpoint.rs       # new — compose-gated: CHECK + independent advance
crates/control/.sqlx/                    # regenerate offline query data
```

## Skeleton

```rust
// crates/control/src/checkpoint.rs
use sqlx::postgres::PgExecutor;
use common::Lsn;

/// Per-table, per-epoch progress. INVARIANT (DB-enforced): transformed_lsn <= raw_appended_lsn.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Checkpoint {
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub raw_appended_lsn: Lsn, // Phase A frontier (commit LSN)
    pub transformed_lsn: Lsn,  // Phase B frontier (commit LSN), <= raw_appended_lsn
}

/// Read the checkpoint for a table, if one exists yet.
pub async fn read_checkpoint(ex: impl PgExecutor<'_>, epoch: i64, schema: &str, table: &str)
    -> Result<Option<Checkpoint>, crate::ControlError> { todo!() }

/// Create the row at (0/0, 0/0) if missing; no-op if present. Called at loader bootstrap (PR 3.1).
pub async fn ensure_checkpoint(ex: impl PgExecutor<'_>, epoch: i64, schema: &str, table: &str)
    -> Result<(), crate::ControlError> { todo!() }

/// Phase A: advance `raw_appended_lsn` (UPSERT). Caller passes the txn so this can share the
/// control-DB transaction that also deletes claimed manifest rows (PR 3.2).
pub async fn advance_raw_appended(ex: impl PgExecutor<'_>, epoch: i64, schema: &str, table: &str, lsn: Lsn)
    -> Result<(), crate::ControlError> { todo!() }

/// Phase B: advance `transformed_lsn` (UPSERT). Must never exceed raw_appended_lsn (the CHECK guards it).
pub async fn advance_transformed(ex: impl PgExecutor<'_>, epoch: i64, schema: &str, table: &str, lsn: Lsn)
    -> Result<(), crate::ControlError> { todo!() }
```

```rust
// crates/control/src/replication_state.rs
use sqlx::postgres::PgExecutor;
use common::Lsn;

/// One row per slot lifetime; a new slot = a new epoch (architecture §1.8). Namespaces ALL state.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReplicationState {
    pub epoch: i64,
    pub slot_name: String,
    pub created_lsn: Lsn,   // consistent snapshot LSN at slot creation
    pub status: String,     // bootstrapping | streaming | total_restart
}

/// The highest-epoch (current) generation, if bootstrap has run.
pub async fn read_current_epoch(ex: impl PgExecutor<'_>) -> Result<Option<ReplicationState>, crate::ControlError> { todo!() }

/// Insert a new generation row (a new slot). Epoch bump / total-restart lands in PR 4.6.
pub async fn insert_epoch(ex: impl PgExecutor<'_>, s: &ReplicationState) -> Result<(), crate::ControlError> { todo!() }

#[cfg(test)]
mod tests { /* DB assertions live in tests/checkpoint.rs */ }
```

```rust
// crates/control/tests/checkpoint.rs   (compose-gated)
#[tokio::test]
async fn ensure_then_advance_raw_then_transformed() { todo!() }

#[tokio::test]
async fn check_rejects_transformed_ahead_of_raw() {
    // advance_transformed to a value > raw_appended_lsn → DB CHECK violation surfaced as a typed error. todo!()
}

#[tokio::test]
async fn advances_are_idempotent_and_monotonic() { todo!() /* re-advancing to the same/lower LSN is safe */ }

#[tokio::test]
async fn read_current_epoch_returns_highest_generation() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `ensure_checkpoint` creates a `(0/0, 0/0)` row idempotently; `read_checkpoint` returns it.
- [x] `advance_raw_appended` and `advance_transformed` are **separate** UPSERTs, each accepting a caller
      executor (so Phase A can share a txn with the manifest delete in PR 3.2).
- [x] A compose test proves the DB **CHECK rejects** `transformed_lsn > raw_appended_lsn`, surfaced as a
      typed terminal `ControlError` (not a panic / raw sqlx error leaking).
- [x] `read_current_epoch` returns the highest-epoch `replication_state`; `insert_epoch` inserts one.
- [x] Comments state both watermarks are **commit-LSN valued** and advance **independently**.
- [x] `.sqlx/` regenerated; `cargo sqlx prepare --check` passes.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p control` and — with services — `docker compose up --wait` then
        `cargo test -p control --test checkpoint` (the CHECK-rejection assertion passes).

## Hints & gotchas

- **The CHECK fires on the *row*, not the statement** — so advancing `transformed_lsn` above the current
  `raw_appended_lsn` fails even in a valid UPSERT. Map Postgres error code `23514` (check_violation) to a
  distinct terminal variant so callers can tell "programming bug" from "transient".
- **Don't advance backwards silently.** In v1 the caller always advances forward, but a defensive
  `GREATEST(existing, excluded)` in the `DO UPDATE` makes re-runs after a crash harmless — decide and
  document which you do; the tests should pin it.
- **`ensure_checkpoint` vs `advance_*`:** bootstrap (PR 3.1) calls `ensure` once; the loop calls `advance`.
  Keep them separate so a fresh table starts at `0/0` without a spurious "advance to zero".
- **Epoch is `bigint`/`i64` everywhere** — matches `SinkMeta.epoch` (PR 1.1) and the S3 prefix. Keep the
  type identical across `common` and `control` so nothing casts.
- These are the two watermarks the crash-window table in loader §4 reasons about — a comment linking
  Phase A/B to the two functions will save the next reader (you, in PR 3.2/3.4) a lot of page-flipping.

## References

- Design: `../../architecture.md` "Coordination contract" / "Why every progress key is a COMMIT LSN";
  `../../walrus-loader.md` §4 "Two-phase apply".
- Prev: [PR 1.4](./pr-1.4-control-file-manifest.md) ·
  Next: [PR 1.6](./pr-1.6-control-schema-registry-ddl-manifest.md) · [Roadmap](../README.md)
