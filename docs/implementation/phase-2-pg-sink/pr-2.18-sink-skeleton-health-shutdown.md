# PR 2.18 — Sink binary skeleton: bootstrap scaffold, health endpoints, SIGTERM

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib), `common` ·
> **Est. size:** M · **Depends on:** PR 2.17 · **Unlocks:** PR 2.19

This PR turns `pg-sink` from a pure decoder library into a **runnable service**: a `tokio` `main`
that loads validated config, calls `init_tracing`, runs an **ordered, fail-fast bootstrap
scaffold** (control-DB + S3 reachability, transient-vs-terminal), serves the three K8s health
endpoints on `axum`, and exits cleanly on `SIGTERM` via a `CancellationToken`. No WAL is read yet —
this is the pod lifecycle shell that every later 2.x PR fills in.

## Why — learning objectives

By the end of this PR you will have practised:

- **`anyhow` in a binary, mapped to `common::ExitCode`** — the "context in the loop, exit code at
  `main`" idiom that makes a broken deploy greppable in `kubectl logs`.
- **Ordered fail-fast bootstrap** — modelling *terminal* vs *transient* preconditions so a rollout
  where Postgres is "still coming up" retries, but a misconfig crashes loudly.
- **`axum` state + graceful shutdown** — an `Arc<HealthState>` shared across handlers, and
  `axum::serve(...).with_graceful_shutdown(token)`.
- **`tokio::select!` + `CancellationToken` + `tokio::signal::unix`** — one cancellation source fanned
  out to every task, so SIGTERM unwinds the whole process without a leaked handle.

## Read first

- `../../walrus-pg-sink.md` §4.2 Startup (`#42-startup`) and §4.3 Probes (`#43-probes--get-these-exactly-right`) —
  the ordered `initContainers`/`startupProbe` model and the **exact** probe semantics (liveness = deadlock
  only, never slot lag).
- `../../architecture.md` "Startup & bootstrap (fail-fast preflight)" (`#startup--bootstrap-fail-fast-preflight`) —
  the **Shared bootstrap** steps 1–4 (config → control PG + migration version → object store canary → bind
  health) this PR scaffolds; transient-vs-terminal taxonomy.
- `../../walrus-pg-sink.md` §4.5 (`#45-graceful-shutdown--the-missing-piece`) — why the process must be
  PID-1 / exec-form so SIGTERM is not swallowed (full drain is PR 2.28; here just cancel cleanly).

## Scope

**In scope**

- `main.rs` (thin) + `lib.rs` split so `pg-sink/tests/` keeps importing `pgoutput`.
- `SinkConfig` (serde-typed, bounds-validated; invalid → terminal).
- Bootstrap steps **shared 1–4 only**: load/validate config, control-PG reachable + migration version
  check, object-store canary `head`/`put`/`get`, bind health endpoints.
- `axum` server exposing `GET /startup`, `GET /ready`, `GET /healthz` backed by a shared `HealthState`.
- SIGTERM/SIGINT → `CancellationToken`; server + (future) loop shut down on cancel.

**Explicitly deferred** (do *not* build these here)

- Source-side preflight (`wal_level`, publication, PK) → **PR 2.19**.
- `START_REPLICATION` / any WAL read → **PR 2.20**.
- Schema-registry hydration (shared bootstrap step 7) → **PR 2.22**.
- The real drain sequence (flush → manifest → final standby) → **PR 2.28**.

## Files to create / modify

```
crates/pg-sink/Cargo.toml          # + axum = "0.7", tokio-util = "0.7", tokio = { features=["signal","rt-multi-thread","macros"] }
                                    # + object_store = "0.11", anyhow = "1", tracing = "0.1"
                                    # + sqlx (control) via `control` crate dep; serde, humantime-serde
crates/pg-sink/src/main.rs         # new — thin: parse config, run(), map anyhow::Error -> ExitCode
crates/pg-sink/src/lib.rs          # new — pub mod {config, health, bootstrap, shutdown, pgoutput}
crates/pg-sink/src/config.rs       # new — SinkConfig + validate()
crates/pg-sink/src/health.rs       # new — HealthState + axum router + serve
crates/pg-sink/src/bootstrap.rs    # new — ordered fail-fast scaffold (shared steps 1-4)
crates/pg-sink/src/shutdown.rs     # new — install_signal_handlers() -> CancellationToken
crates/pg-sink/tests/health.rs     # new — start server, assert /startup -> /ready transition
```

## Skeleton

```rust
// crates/pg-sink/src/config.rs
use std::time::Duration;

/// Fully-validated sink configuration. Invalid config is a *terminal* bootstrap error.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SinkConfig {
    pub control_db_url: String,
    pub source_db_url: String,
    pub s3_bucket: String,
    pub slot_name: String,
    pub publication_name: String,
    #[serde(with = "humantime_serde")]
    pub max_fill: Duration,     // cadence — see PR 2.23 / §1.3
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_inflight_bytes: u64,
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
}

impl SinkConfig {
    /// Load from env/file, then bounds-check. `Err` => terminal exit.
    pub fn load() -> Result<Self, crate::config::ConfigError> { todo!() }
    fn validate(&self) -> Result<(), crate::config::ConfigError> { todo!() }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError { /* Missing(&'static str), OutOfBounds { field, .. } */ }
```

```rust
// crates/pg-sink/src/health.rs
use std::sync::Arc;

/// Snapshot the probes read. `Startup` gates the other two (§4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase { Bootstrapping, Ready }

#[derive(Debug)]
pub struct HealthState { /* phase: RwLock<Phase>, degraded: AtomicBool, ... */ }

impl HealthState {
    pub fn new() -> Arc<Self> { todo!() }
    pub fn mark_ready(&self) { todo!() }
    pub fn phase(&self) -> Phase { todo!() }
}

/// `/startup` 200 iff bootstrap done; `/ready` 200 iff Ready & not terminating;
/// `/healthz` 200 unless the replication loop is deadlocked (liveness = deadlock only).
pub fn router(state: Arc<HealthState>) -> axum::Router { todo!() }

pub async fn serve(
    addr: std::net::SocketAddr,
    state: Arc<HealthState>,
    shutdown: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> { todo!() }
```

```rust
// crates/pg-sink/src/bootstrap.rs — ordered, fail-fast. Retries *transient* deps to
// `startup_deadline`, then terminal. Terminal(wrong config/migration behind) -> non-zero exit.
pub async fn run_shared(cfg: &crate::config::SinkConfig) -> anyhow::Result<BootstrapCtx> { todo!() }
pub struct BootstrapCtx { /* control_pool, object_store, ... */ }

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("terminal: {0}")] Terminal(String),
    #[error("transient: {0}")] Transient(String),
}

// crates/pg-sink/src/main.rs — map anyhow::Error -> common::ExitCode at the top of main ONLY.
fn main() -> std::process::ExitCode { todo!() } // init_tracing; SinkConfig::load; block_on(run)
async fn run(cfg: crate::config::SinkConfig) -> anyhow::Result<()> {
    // token = shutdown::install_signal_handlers(); health = HealthState::new();
    // spawn health::serve(..); run_shared(&cfg).await?; health.mark_ready(); token.cancelled().await
    todo!()
}
```

```rust
// crates/pg-sink/tests/health.rs
#[tokio::test] async fn startup_gates_ready_until_bootstrap_completes() { todo!() }
#[tokio::test] async fn sigterm_cancels_and_server_returns() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `pg-sink` builds as **bin + lib**; `pg-sink/tests/` still imports the `pgoutput` module.
- [ ] Invalid config (missing field / out-of-bounds threshold) → **terminal**, mapped to a distinct
      non-zero `common::ExitCode`; a *missing control DB* is treated as **transient** until the
      `startup_deadline`, then terminal.
- [ ] `/startup` returns non-200 during bootstrap and 200 after; `/ready` 200 only once `mark_ready`
      was called; `/healthz` reflects liveness (not lag).
- [ ] SIGTERM (and SIGINT) trip the `CancellationToken`; `axum` server returns via
      `with_graceful_shutdown` and the process exits 0.
- [ ] Docs/comments state the transient-vs-terminal rule and why liveness ≠ readiness.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then a compose smoke: process boots, `/startup`→`/ready` flips,
        and a *missing* control DB yields the mapped non-zero exit.

## Hints & gotchas

- Keep `main` tiny: build the runtime, call `run`, and do the **only** `anyhow::Error → ExitCode`
  mapping there. Everything below `main` returns `anyhow::Result<_>` with `.context(...)`.
- Bootstrap **order matters** — don't bind health before config validates, or a crashed config load
  leaves a half-open port. Bind health *after* the shared preconditions pass but *before* the main loop.
- `object_store`'s canary should exercise `put`+`get`+`delete` of a tiny key, not just `head` — some
  S3-compatibles (MinIO) answer `head` on a nonexistent bucket differently.
- Use **one** `CancellationToken`, `clone()`d into each task; child tasks call `token.cancelled()`
  inside `tokio::select!`. Do not spawn a detached signal task that outlives the token.
- The process must be able to be PID 1 — no shell wrapper that eats SIGTERM (Dockerfile is PR 4.8, but
  design for it now: exec-form entrypoint, direct signal handling).

## References

- Design: `../../walrus-pg-sink.md` §4.2–§4.3, §4.5; `../../architecture.md`
  "Startup & bootstrap (fail-fast preflight)" (shared steps 1–4).
- Prev: [PR 2.17](./pr-2.17-pgarrow-type-descriptor.md) · Next: [PR 2.19](./pr-2.19-sink-source-preflight.md) · [Roadmap](../README.md)
