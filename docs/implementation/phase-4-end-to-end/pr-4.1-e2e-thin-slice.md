# PR 4.1 — Wire both services end-to-end: the thin vertical slice

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `tests/e2e` (new) · **Est. size:** M ·
> **Depends on:** PR 3.12 · **Unlocks:** PR 4.2

This is the first time `walrus-pg-sink` and `walrus-loader` run **together** against the compose stack.
It stands up a new integration crate `tests/e2e` (behind an `it` feature) that starts both binaries,
does an `INSERT` / `UPDATE` / `DELETE` on `orders`, and asserts the full chain: Parquet lands in MinIO,
the CDC row lands **verbatim in `orders_raw`** (with `walrus_pg_sink_meta` intact and `op` / `commit_lsn`
/ `lsn` / `sink_processed_at` promoted to typed columns), and the derived `orders` mirror row equals the
current source row after the transform. It proves the `architecture.md` **"Local harness"** verification
bullet.

## Why — learning objectives

By the end of this PR you will have practised:

- **A cross-service integration crate** — a `tests/e2e` member that depends on nothing in the DAG but
  drives both bins as black boxes over the compose stack, gated by `--features it`.
- **Orchestrating two processes in a test** — spawn `pg-sink` + `loader`, poll their `/ready` endpoints,
  drive the source, then poll for convergence with a bounded deadline (never a blind `sleep`).
- **Asserting the two-hop contract** — S3 object present, `orders_raw` verbatim, `orders` mirror equals
  source — the eventual-consistency guarantee, proven, not assumed.
- **`commit_lsn` watermark reasoning** — waiting on `transformed_lsn` to cross the change's commit LSN.

## Read first

- `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` — the **"Local harness"**
  bullet is exactly what this PR implements; read it verbatim.
- `../../architecture.md#component-2--data-sink-walrus-loader` — the raw-append-then-transform contract
  the assertions check (`<table>_raw` verbatim, `<table>` mirror == current source).
- `../../walrus-loader.md` §3–§6 — what "mirror equals current source" means after dedup + MERGE.
- `../examples/proto-version/01-setup.sql` — the `orders` single-PK schema the slice uses.

## Scope

**In scope**

- A new `tests/e2e` crate with an `it` feature and a small harness module that: spawns both binaries
  pointed at the compose Postgres/MinIO, waits for `/ready`, and exposes `source_exec`, `s3_list`,
  `duckdb_query`, and `await_transformed_past(lsn)` helpers.
- One test: `insert_update_delete_reaches_mirror` doing `INSERT`→`UPDATE`→`DELETE` on `orders`.
- Assertions: (a) ≥1 Parquet object under the epoch/schema/table prefix; (b) `orders_raw` holds the
  three CDC rows verbatim with intact meta + promoted columns; (c) `orders` mirror reflects the final
  state (row gone after the `DELETE`).

**Explicitly deferred** (do *not* build these here)

- The full type matrix + unchanged-TOAST → **PR 4.2**.
- Large-txn / commit-order / subtxn-abort → **PR 4.3**. Crash safety → **PR 4.4**.
- Running e2e in CI as a first-class job — the workflow gate lands with the image build in **PR 4.8**;
  here it runs locally behind `--features it`.

## Files to create / modify

```
tests/e2e/Cargo.toml                 # new — member crate; [features] it = []
tests/e2e/src/lib.rs                 # new — Harness: spawn both bins, ready-poll, helpers
tests/e2e/tests/thin_slice.rs        # new — insert_update_delete_reaches_mirror
Cargo.toml                           # modify — add "tests/e2e" to [workspace] members
# deps (dev/normal in tests/e2e): tokio, sqlx, duckdb (bundled), object_store (aws),
#   anyhow, serde_json — all already pinned at workspace level
```

## Skeleton

```rust
// tests/e2e/src/lib.rs

/// A running walrus stack: source PG + control PG + MinIO (compose) with a live
/// pg-sink and loader spawned as child processes. Drop stops both cleanly.
pub struct Harness { /* child handles, connection strings, epoch, bucket */ }

impl Harness {
    /// Bring up both binaries against the already-running compose stack and block
    /// until each reports `/ready`. Fails fast if either bootstrap errors.
    pub async fn start() -> anyhow::Result<Self> { todo!() }

    /// Run SQL on the SOURCE database and return rows affected.
    pub async fn source_exec(&self, sql: &str) -> anyhow::Result<u64> { todo!() }

    /// List S3 object keys under `<epoch>/<schema>/<table>/`.
    pub async fn s3_list(&self, table: &str) -> anyhow::Result<Vec<String>> { todo!() }

    /// Query the loader's per-table DuckDB file (mirror or `_raw`).
    pub fn duckdb_query(&self, table: &str, sql: &str) -> anyhow::Result<Vec<duckdb::types::Value>> { todo!() }

    /// Poll `loader_checkpoint.transformed_lsn` until it passes `lsn` or the deadline.
    pub async fn await_transformed_past(&self, lsn: common::Lsn, deadline: std::time::Duration) -> anyhow::Result<()> { todo!() }
}

#[cfg(all(test, feature = "it"))]
mod smoke { /* re-exported helpers compile only under the it feature */ }
```

```rust
// tests/e2e/tests/thin_slice.rs
#![cfg(feature = "it")]
use e2e::Harness;

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn insert_update_delete_reaches_mirror() {
    // INSERT one orders row, UPDATE it, DELETE it on the source.
    // await_transformed_past(delete_commit_lsn).
    // assert: >=1 Parquet object under the orders prefix in MinIO;
    //         orders_raw has 3 verbatim rows (op i/u/d, meta JSON intact, promoted cols);
    //         orders mirror has 0 rows for that PK (delete won).
    todo!()
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `tests/e2e` is a workspace member with an `it` feature; `cargo build --workspace` (no `it`) still
      builds it with zero tests active.
- [ ] `Harness::start` spawns both bins against compose, waits on `/ready`, and stops them on `Drop`.
- [ ] After `INSERT`/`UPDATE`/`DELETE` on `orders`: at least one Parquet object exists under
      `<epoch>/public/orders/`, `orders_raw` holds the three CDC rows **verbatim** (meta JSON present;
      `op`/`commit_lsn`/`lsn`/`sink_processed_at` promoted), and the `orders` mirror equals the current
      source (the row is absent after the `DELETE`).
- [ ] The test waits on `transformed_lsn`, never a fixed `sleep`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace` (the e2e test is `#[ignore]` without services)
  - [ ] `docker compose up --wait` then
        `cargo test -p e2e --features it -- --ignored` asserting **`insert_update_delete_reaches_mirror`**.

## Hints & gotchas

- Spawn the bins with `tokio::process::Command`; capture their stdout/stderr so a failed assertion prints
  each service's `tracing` log. Kill them on `Drop` — a leaked `pg-sink` holds the slot and the next run's
  bootstrap will block.
- Convergence is **eventual**: poll `transformed_lsn` against the `DELETE`'s commit LSN with a generous
  deadline (both services run their own poll cadence). A blind `sleep` is flaky; a watermark wait is not.
- `orders_raw` keeps *all three* ops — the raw log is append-only. Only the **mirror** collapses to the
  final state. Assert both, or you can't tell an append bug from a transform bug.
- DuckDB is single-writer: the loader owns the `.duckdb` file. Open it **read-only** from the test
  (`ACCESS_MODE=READ_ONLY`) so you don't fight the loader's lock.

## References

- Design: `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` ("Local harness"),
  `#component-2--data-sink-walrus-loader`; `../../walrus-loader.md` §3–§6.
- Prev: [PR 3.12](../phase-3-loader/pr-3.12-loader-graceful-shutdown.md) ·
  Next: [PR 4.2](./pr-4.2-e2e-type-matrix.md) · [Roadmap](../README.md)
