# PR 3.1 — `walrus-loader` skeleton: bootstrap (lease · DuckDB open · checkpoints) + health

> **Phase:** 3 — walrus-loader · **Crates touched:** `loader` (bin+lib), `control` · **Est. size:** L ·
> **Depends on:** PR 2.33 (phase boundary) · **Unlocks:** PR 3.2

The loader's first vertical slice: a binary that comes up, **proves exclusive ownership of each
`.duckdb` file**, and refuses to touch data until it can. This is the loader analogue of the sink's
fail-fast bootstrap — but where the sink gets single-writer for free from the Postgres active-slot
rule, the loader has to *assemble* it from a control-plane **ownership lease** + an RWO PVC + DuckDB's
own file lock. After this PR: `orders.duckdb` exists with both `orders` and `orders_raw`, the lease is
held, both watermarks are loaded, S3 read is verified, and the health endpoints report liveness — but
no manifest file is claimed yet.

## Why — learning objectives

By the end of this PR you will have practised:

- **Ordered fail-fast bootstrap** — modelling each step as transient (retry to a deadline) vs terminal
  (exit non-zero → `CrashLoopBackOff`), reusing `common::ExitCode`.
- **Cooperative single-writer fencing** — a `walrus.table_ownership` row with a monotonic
  `fencing_token`, acquired *before* the DuckDB file lock is taken.
- **The `duckdb` crate (bundled)** — `Connection::open`, `CREATE TABLE IF NOT EXISTS`, and the
  read-write open lock that DuckDB takes on the file.
- **Stale-lock recovery** — distinguishing a *live* owner (lease still renewed → terminal) from a
  *dead PID's stale lock* (lease expired → reclaim + open).
- **K8s probes without the lag trap** — `/startup`, `/ready` (leases held + files open), `/healthz`
  (an in-memory `last_poll_completed_at`, never backlog lag).

## Read first

- `../../walrus-loader.md#81-topology-and-the-single-writer-problem` — the lease / fencing-token
  mechanism and the "no `.duckdb` write until lease held **and** file lock taken" invariant.
- `../../walrus-loader.md#82-startup--the-ordered-fail-fast-bootstrap` — the exact 5-step sequence and
  the transient/terminal table (incl. the stale-DuckDB-lock ⚠ callout).
- `../../walrus-loader.md#83-probes--readiness-liveness-and-the-catch-up-lag-trap` — why readiness ≠
  caught-up and liveness must ignore *all* lag; the `last_poll_completed_at` progress stamp.
- `../../architecture.md#coordination-contract-control-plane-tables` — `loader_checkpoint` shape and
  the `CHECK (transformed_lsn <= raw_appended_lsn)` invariant.

## Scope

**In scope**

- `loader` bin (`main.rs`) + `lib.rs` split; `anyhow` → `common::ExitCode` mapping at `main`.
- `control::table_ownership` model + a `migrations/control/` migration for `walrus.table_ownership`.
- Bootstrap: acquire lease → open/create `.duckdb` + take the file lock → ensure `<table>` **and**
  `<table>_raw` → load both watermarks → verify S3 read (list/GET the staged prefix).
- Stale-lock recovery (expired lease → reclaim; live lease → terminal, distinct exit code).
- `axum` health server: `/startup`, `/ready`, `/healthz` reading an in-memory `LoaderState`.

**Explicitly deferred** (do *not* build these here)

- Claiming manifest files / Phase A append → **PR 3.2**.
- The transform and Phase B → **PR 3.3 / 3.4**.
- Lease renewal-under-node-drain TTL tuning + fencing-token sharding → **PR 4.9 / 4.11** (the token is
  inert while `replicas=1`; just persist it now).
- Schema-reconcile of evolved tables (DDL apply at bootstrap) → **PR 3.8 / 3.9**; create the *initial*
  shape only.

## Files to create / modify

```
crates/loader/Cargo.toml           # + duckdb = { version = "1.4", features = ["bundled"] }
                                    # + tokio, anyhow, axum, object_store, tracing; common, control (path)
crates/loader/src/main.rs          # new — anyhow main; init_tracing; bootstrap; serve health; await SIGTERM
crates/loader/src/lib.rs           # new — pub mod bootstrap, health, duck, lease; LoaderState
crates/loader/src/bootstrap.rs     # new — ordered fail-fast bootstrap
crates/loader/src/duck.rs          # new — open/create .duckdb, ensure both tables, load watermarks
crates/loader/src/lease.rs         # new — acquire/renew the ownership lease (wraps control)
crates/loader/src/health.rs        # new — axum router + LoaderState probes
crates/control/src/table_ownership.rs  # new — acquire_lease / renew_lease / release_lease
migrations/control/000X_table_ownership.sql  # new — walrus.table_ownership
crates/loader/tests/bootstrap.rs   # new — compose integration test
```

## Skeleton

```rust
// crates/control/src/table_ownership.rs
/// One row per owned (epoch, schema, table); the fencing token is monotonic per key.
pub struct Lease { pub fencing_token: i64, pub owner_pod: String, pub lease_expiry: OffsetDateTime }

/// Conditional acquire: succeeds iff the lease is free (expired) or already ours.
/// Returns `Ok(None)` when a *live* owner holds it (caller maps to a terminal exit).
pub async fn acquire_lease(
    pool: &sqlx::PgPool, epoch: i64, schema: &str, table: &str, self_pod: &str, ttl: Duration,
) -> Result<Option<Lease>, control::Error> { todo!() }

pub async fn renew_lease(pool: &sqlx::PgPool, key: &TableKey, self_pod: &str, ttl: Duration)
    -> Result<(), control::Error> { todo!() }
pub async fn release_lease(pool: &sqlx::PgPool, key: &TableKey, self_pod: &str)
    -> Result<(), control::Error> { todo!() }
```

```rust
// crates/loader/src/duck.rs
/// Owns one .duckdb file's read-write connection (holds DuckDB's single-writer file lock).
pub struct TableDb { conn: duckdb::Connection, /* … */ }

impl TableDb {
    /// Open (or create) the file in read-write mode; a stale lock left by a dead PID is a terminal
    /// error *here* — the caller has already proven the lease is reclaimable in `bootstrap`.
    pub fn open(path: &Path) -> Result<Self, LoaderError> { todo!() }
    /// CREATE TABLE IF NOT EXISTS for BOTH <table> (mirror) and <table>_raw (CDC log + composite PK).
    pub fn ensure_tables(&self, rel: &common::PgRelation) -> Result<(), LoaderError> { todo!() }
}

// crates/loader/src/bootstrap.rs
pub struct Checkpoints { pub raw_appended_lsn: common::Lsn, pub transformed_lsn: common::Lsn }

/// The ordered, fail-fast bootstrap (loader §8.2 steps 1–5). Terminal errors exit non-zero.
pub async fn bootstrap(cfg: &Config, state: &LoaderState) -> Result<Vec<OwnedTable>, LoaderError> {
    // 1. control PG reachable + acquire ownership lease   (live lease -> ExitCode::LeaseContended)
    // 2. open/create .duckdb + file lock + ensure both tables
    // 3. load both checkpoints (CHECK transformed_lsn <= raw_appended_lsn)
    // 4. (initial shape only; DDL reconcile deferred)
    // 5. verify S3 read path
    todo!()
}

// crates/loader/src/health.rs
#[derive(Default)]
pub struct LoaderState { pub last_poll_completed_at: parking_lot::Mutex<Option<Instant>>, /* … */ }
pub fn router(state: Arc<LoaderState>) -> axum::Router { todo!() } // /startup /ready /healthz
```

```rust
// crates/loader/tests/bootstrap.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn bootstrap_creates_duckdb_with_both_tables_and_takes_the_lease() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn second_instance_with_live_lease_exits_terminal() { todo!() }
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn stale_lock_expired_lease_is_reclaimed_and_opened() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Bootstrap creates `orders.duckdb` containing **both** `orders` and `orders_raw`, acquires the
      ownership lease, loads both watermarks, and passes the S3 read check.
- [ ] The lease + DuckDB file lock are acquired **before** any watermark is read (the fence precedes
      the read-then-write).
- [ ] A second instance started against a **live** lease exits with a distinct terminal `ExitCode`.
- [ ] A **stale** lock behind an **expired** lease is reclaimed and the file opened (no manual step).
- [ ] `/startup` gates the slow bootstrap; `/ready` = leases held + files open; `/healthz` reflects
      `last_poll_completed_at` and never any lag metric.
- [ ] A corrupt checkpoint (`transformed_lsn > raw_appended_lsn`) is a terminal error.
- [ ] Docs/comments explain lease-then-lock ordering and the fencing token's dormancy under `replicas=1`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p loader --test bootstrap -- --ignored`
        asserting **`bootstrap_creates_duckdb_with_both_tables_and_takes_the_lease`** and
        **`second_instance_with_live_lease_exits_terminal`**.

## Hints & gotchas

- DuckDB takes its file lock on **open in read-write mode** — that lock is the *second* fence, never
  the first. If you open before proving the lease is reclaimable, a still-live owner's lock will just
  make you fail opaquely instead of with the clean `LeaseContended` terminal code.
- Renew the lease on an interval **well under** the TTL, off the apply-loop thread, so a busy Phase A/B
  can't let the lease lapse and let a phantom second writer in.
- `duckdb` with `bundled` compiles DuckDB from source — the first build is slow; wire the CI cache now
  (this is also the crate PR 2.11's conformance harness introduced, so it may already be cached).
- Do **not** gate `/ready` on "backlog drained" — a legitimately-behind loader is still *ready*
  (§8.3). Gating readiness on lag flaps a busy pod out of rotation.
- `last_poll_completed_at` must be stamped **every** cycle, even a no-op poll; otherwise an idle-but-
  healthy loader trips liveness. (There's no poll yet — stamp it once at end of bootstrap so `/healthz`
  is green.)

## References

- Design: `../../walrus-loader.md#81-topology-and-the-single-writer-problem`,
  `#82-startup--the-ordered-fail-fast-bootstrap`,
  `#83-probes--readiness-liveness-and-the-catch-up-lag-trap`;
  `../../architecture.md#coordination-contract-control-plane-tables`,
  `#startup--bootstrap-fail-fast-preflight`.
- Prev: [PR 2.33](../phase-2-pg-sink/pr-2.33-sink-ddl-capture.md) ·
  Next: [PR 3.2](./pr-3.2-loader-phase-a-append.md) · [Roadmap](../README.md)
