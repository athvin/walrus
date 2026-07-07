# PR 0.5 — Typed, bounds-validated shared config loading

> **Phase:** 0 — Foundations & CI · **Crates touched:** `common` · **Est. size:** M ·
> **Depends on:** PR 0.4 · **Unlocks:** PR 0.6

Bootstrap step 1 for *both* services is "load & validate config; invalid → terminal." This PR gives
`common` the shared config type and a loader that reads env (+ optional file) into serde structs and
then **bounds-validates** them — an out-of-range cadence or an empty DB URL becomes a
`common::Error::Config` (terminal, from PR 0.2), *not* a panic three modules later. Service-specific
knobs extend this in their own crates; here we nail the shared spine and the load-then-validate shape.

## Why — learning objectives

By the end of this PR you will have practised:

- **serde-typed config** — env/file → structs, with `Default`s and typed durations.
- **Layered configuration** — file underneath, environment on top (12-factor), one merge.
- **Parse, don't validate (then validate anyway)** — bounds checks that turn bad input into a typed
  terminal error at the edge, so the rest of the code assumes-valid.
- **`humantime`-style durations** — `"250ms"` / `"5s"` in config instead of raw milliseconds.

## Read first

- [`../../architecture.md`](../../architecture.md#startup--bootstrap-fail-fast-preflight) "Startup &
  bootstrap" step 1 (~line 1319): *"schema-validate the ConfigMap/env; … cadence/threshold values
  within sane bounds. Invalid → terminal."*
- [`../../architecture.md`](../../architecture.md#kubernetes-deployment) "Kubernetes deployment"
  table, *Config / cadence* row (~line 1411) — the knob catalogue (sink `max_fill_ms` / `max_bytes` /
  `max_rows` / `max_inflight_bytes` + heartbeat; loader poll interval + compaction cadence). Note
  which are **shared** (this PR) vs **service-specific** (deferred).
- [`../README.md`](../README.md) "Conventions" row *Config* — serde-typed, env/file,
  **bounds-validated**, invalid → terminal.

## Scope

**In scope**

- `common::config::CommonConfig` — the fields both services need: `control_db_url`,
  `object_store` (bucket/endpoint/region), `telemetry: TelemetryConfig` (from PR 0.4),
  `startup_deadline: Duration`, `log_instance: String`.
- `CommonConfig::load()` — figment (Env with a `WALRUS_` prefix, `__` nesting) layered over an
  optional file at `WALRUS_CONFIG` (TOML/YAML); returns `Result<Self>`.
- `CommonConfig::validate(&self) -> Result<()>` — bounds checks; every failure is `Error::Config(..)`.
- `Duration` fields via `humantime_serde` (`"30s"`, `"250ms"`).
- Tests: a happy parse (env + file) and at least one **validation-failure** producing `Error::Config`.

**Explicitly deferred** (do *not* build these here)

- Per-service config structs (`SinkConfig` cadence/heartbeat, `LoaderConfig` poll/compaction) —
  they live in `pg-sink` / `loader` and *embed* `CommonConfig`: **PR 2.18** / **PR 3.1**.
- Actually *connecting* to the control DB or object store — bootstrap PRs **2.18 / 3.1**.
- Secrets management beyond reading an env var (no Vault/CSI) — out of scope for v1.

## Files to create / modify

```
crates/common/Cargo.toml         # + figment = { workspace = true, features = ["env","toml","yaml"] }
                                  # + humantime-serde = { workspace = true }
                                  #   (add figment = "0.10", humantime-serde = "1" to [workspace.dependencies])
crates/common/src/config.rs      # new — CommonConfig, ObjectStoreConfig, load(), validate()
crates/common/src/lib.rs         # + pub mod config; pub use config::CommonConfig;
```

## Skeleton

```rust
// crates/common/src/config.rs
use std::time::Duration;
use serde::Deserialize;
use crate::{Error, Result, telemetry::TelemetryConfig};

/// Configuration shared by both walrus services. Service-specific knobs embed this.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CommonConfig {
    /// Control Postgres connection string (holds manifest/checkpoint/registry).
    pub control_db_url: String,
    /// S3/MinIO staging bucket + endpoint.
    pub object_store: ObjectStoreConfig,
    /// Logging setup (PR 0.4).
    pub telemetry: TelemetryConfig,
    /// Bootstrap retry budget: transient deps are retried until this elapses, then terminal.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Human tag for this process instance, e.g. "walrus-pg-sink-0".
    pub instance: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ObjectStoreConfig {
    pub bucket: String,
    pub endpoint: Option<String>, // None = real AWS; Some = MinIO/localstack
    pub region: String,
}

impl Default for CommonConfig { fn default() -> Self { todo!() } }
impl Default for ObjectStoreConfig { fn default() -> Self { todo!() } }

impl CommonConfig {
    /// File (at `WALRUS_CONFIG`, optional) underneath, `WALRUS_`-prefixed env on top; then validate.
    pub fn load() -> Result<Self> { todo!() }

    /// Bounds-check. Any violation → `Error::Config(..)` (terminal). Called by `load`.
    pub fn validate(&self) -> Result<()> { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_from_env_over_file() { todo!() } // figment::Jail: set WALRUS_* env, assert fields

    #[test]
    fn empty_control_db_url_is_config_error() {
        // let err = cfg.validate().unwrap_err();
        // assert!(matches!(err, Error::Config(_)) && err.is_terminal());
        todo!()
    }

    #[test]
    fn zero_startup_deadline_is_rejected() { todo!() }

    #[test]
    fn humantime_durations_parse() { /* "30s" -> Duration::from_secs(30) */ todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `CommonConfig::load()` reads a file (when `WALRUS_CONFIG` is set) and overlays `WALRUS_`-prefixed
      env vars, deserialising into typed fields (durations via `humantime_serde`).
- [ ] `validate()` rejects at least: empty `control_db_url`, empty `object_store.bucket`, and a
      zero/absurd `startup_deadline` — each as `Error::Config(_)` (which is terminal, per PR 0.2).
- [ ] The happy-path parse test uses `figment::Jail` (or equivalent) so env manipulation is hermetic.
- [ ] Unknown/misspelled keys are rejected (`deny_unknown_fields`) so a typo'd ConfigMap key fails
      loud at boot, not silently as a default.
- [ ] `load()` calls `validate()` — an invalid config can never escape as an `Ok`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common` (and `--workspace` stays green)

## Hints & gotchas

- Use `figment::Jail::expect_with(|jail| { … })` in tests to set env vars and temp files **hermetically**
  — real `std::env::set_var` leaks across the shared test process and makes tests order-dependent.
- `#[serde(default)]` at the struct level + a `Default` impl means a missing key uses the default; pair
  it with `deny_unknown_fields` so *present-but-wrong* keys still error. That combination is the whole
  "sane defaults, loud typos" behaviour the design wants.
- Validate **relationships**, not just single fields, when they exist (e.g. later, `max_bytes` ≥ one
  row's worth). Here the shared fields are mostly independent, but keep `validate()` the one funnel.
- Don't validate connectivity here — `validate()` is pure/offline (no network). Reachability is a
  *transient* bootstrap check in the bins; config validity is *terminal* and must be decidable without
  a socket.
- Keep `TelemetryConfig` a nested field so one `CommonConfig::load()` gives the bin everything it needs
  to call `init_tracing(&cfg.telemetry)` first thing in `main`.

## References

- Design: [`../../architecture.md`](../../architecture.md#startup--bootstrap-fail-fast-preflight)
  "Startup & bootstrap" step 1; [`../../architecture.md`](../../architecture.md#kubernetes-deployment)
  "Kubernetes deployment" (Config / cadence row).
- Prev: [PR 0.4](./pr-0.4-common-telemetry.md) · Next:
  [PR 0.6](./pr-0.6-dev-harness-compose.md) · [Roadmap](../README.md)
