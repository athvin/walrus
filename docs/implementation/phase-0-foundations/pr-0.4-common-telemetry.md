# PR 0.4 — `init_tracing()` and the structured-field convention

> **Phase:** 0 — Foundations & CI · **Crates touched:** `common` · **Est. size:** S ·
> **Depends on:** PR 0.3 · **Unlocks:** PR 0.5

walrus never `println!`s. Every log line is a `tracing` event with **structured fields** — `xid`,
`commit_lsn`, `lsn`, `batch_uuid` — so a Grafana/Loki query can follow one transaction through both
services. This PR gives `common` a single `init_tracing()` that a binary calls once at the top of
`main`: env-filterable levels, human format for local dev, optional JSON for the cluster. It also
pins down the *convention* (which field names, spelled how) that every later PR must use.

## Why — learning objectives

By the end of this PR you will have practised:

- **`tracing` vs logging** — events + spans + fields instead of formatted strings.
- **`tracing-subscriber` composition** — `EnvFilter` + a fmt layer (pretty *or* JSON) via `Registry`.
- **Fallible one-time init** — a global subscriber can only be set once; model the second call as a
  handled outcome, not a panic.
- **Naming as a contract** — agreeing the field keys now so dashboards built in Phase 4 just work.

## Read first

- [`../../architecture.md`](../../architecture.md#observability) "Observability" (~line 1421) — the
  metric catalogue and the closing line: *"Structured `tracing` logs keyed by
  `xid`/`commit_lsn`/`lsn`/`batch_uuid`."* Those four keys are the convention this PR fixes.
- [`../README.md`](../README.md) "Conventions" row *Logging* — `tracing`; structured fields; never
  `println!`.

## Scope

**In scope**

- `common::telemetry::init_tracing(cfg: &TelemetryConfig) -> Result<()>` — builds and installs the
  global subscriber: `EnvFilter` from `cfg.filter` (falling back to `RUST_LOG`, then a default), and
  either a pretty or a JSON fmt layer per `cfg.json`.
- `TelemetryConfig { json: bool, filter: String }` (`serde`-derivable, with sane `Default`).
- A documented constant/module listing the canonical field keys (`FIELD_XID`, `FIELD_COMMIT_LSN`,
  `FIELD_LSN`, `FIELD_BATCH_UUID`, plus `sink_instance`, `epoch`, `schema_version`).
- A test proving init doesn't panic and a re-init is a handled (not fatal) outcome.

**Explicitly deferred** (do *not* build these here)

- Prometheus `/metrics` and the `metrics` crate → **PR 4.10** (this PR is logs, not metrics).
- OpenTelemetry / OTLP export — out of scope for v1.
- Wiring `init_tracing()` into an actual `main` → **PR 2.18** (sink) / **PR 3.1** (loader).
- Sourcing `TelemetryConfig` from the ConfigMap — it becomes a field of the shared config in **PR 0.5**.

## Files to create / modify

```
crates/common/Cargo.toml         # + tracing = { workspace = true }
                                  #   + tracing-subscriber = { workspace = true, features = ["env-filter","json"] }
                                  #   (add tracing = "0.1", tracing-subscriber = "0.3" to [workspace.dependencies])
crates/common/src/telemetry.rs   # new — init_tracing, TelemetryConfig, field-name constants
crates/common/src/lib.rs         # + pub mod telemetry; pub use telemetry::{init_tracing, TelemetryConfig};
```

## Skeleton

```rust
// crates/common/src/telemetry.rs
use serde::Deserialize;

/// Canonical structured-field keys. Use these constants at every `tracing` call site so
/// dashboards and log queries key on one spelling across both services.
pub mod fields {
    pub const XID: &str = "xid";
    pub const COMMIT_LSN: &str = "commit_lsn";
    pub const LSN: &str = "lsn";
    pub const BATCH_UUID: &str = "batch_uuid";
    pub const EPOCH: &str = "epoch";
    pub const SCHEMA_VERSION: &str = "schema_version";
    pub const SINK_INSTANCE: &str = "sink_instance";
}

/// How to render logs. `json` on in the cluster, off (pretty) for local dev.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Emit newline-delimited JSON (one object per event) instead of the pretty formatter.
    pub json: bool,
    /// `EnvFilter` directive, e.g. "info,walrus=debug". Empty → fall back to RUST_LOG, then a default.
    pub filter: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self { todo!() } // json: false, filter: "info"
}

/// Build the `EnvFilter` + fmt layer and install it as the global default subscriber.
///
/// Idempotent-ish: a second call must not panic — return the "already initialised" outcome
/// so tests and re-entrant bootstraps are safe.
pub fn init_tracing(cfg: &TelemetryConfig) -> crate::Result<()> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_with_defaults_does_not_panic() { todo!() }

    #[test]
    fn second_init_is_handled_not_fatal() { todo!() }

    #[test]
    fn json_flag_selects_json_formatter() { todo!() } // e.g. build the layer without installing and assert
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `init_tracing(&TelemetryConfig::default())` returns `Ok(())` and installs a subscriber without
      panicking.
- [ ] A **second** `init_tracing` call is a handled outcome (e.g. logs at `debug` and returns `Ok`),
      never a panic — global-subscriber double-install is expected under test.
- [ ] `cfg.filter` (or `RUST_LOG`, or the default) drives the level; `cfg.json` selects JSON vs pretty.
- [ ] The `fields` constants exist and are referenced from a doc comment as the required spelling.
- [ ] No `println!`/`eprintln!` anywhere in `common`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common` (and `--workspace` stays green)

## Hints & gotchas

- `tracing::subscriber::set_global_default` **errors** (does not panic) on the second call. Map that
  error to `Ok` (or a `debug!`) so unit tests that each init don't blow up the process — or use
  `try_init()` from `tracing_subscriber::util::SubscriberInitExt`.
- Because a global subscriber is process-wide, tests can't each install a *different* one — assert the
  *builder* logic (e.g. that the JSON branch is taken) separately from the single real install test,
  or run the install test with `#[test]` knowing later inits are no-ops.
- `EnvFilter::try_from_default_env()` reads `RUST_LOG`; fall back to `EnvFilter::new(&cfg.filter)` then
  a hardcoded default so a missing env var never means "silent".
- Log the **field**, not the format string: `info!(commit_lsn = %lsn, xid, "flushed batch")`, never
  `info!("flushed batch for xid {xid}")`. Use the `fields::*` names. Prefer `%` (Display) for `Lsn`.
- JSON layer needs the `json` feature on `tracing-subscriber`; env-filter needs `env-filter`. Enable
  both in `Cargo.toml` or the branches won't compile.

## References

- Design: [`../../architecture.md`](../../architecture.md#observability) "Observability" (structured
  `tracing` keyed by `xid`/`commit_lsn`/`lsn`/`batch_uuid`).
- Prev: [PR 0.3](./pr-0.3-common-lsn-newtype.md) · Next:
  [PR 0.5](./pr-0.5-common-config.md) · [Roadmap](../README.md)
