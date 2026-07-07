# PR 0.2 — `common` error taxonomy, terminal/transient split, and `ExitCode`

> **Phase:** 0 — Foundations & CI · **Crates touched:** `common` · **Est. size:** M ·
> **Depends on:** PR 0.1 · **Unlocks:** PR 0.3

Both services run an ordered **fail-fast bootstrap**: on Kubernetes a non-zero exit becomes
`CrashLoopBackOff`, so a broken deploy must be *loud and immediate*. This PR gives `common` the
vocabulary for that — a `thiserror` error enum that knows whether a failure is **transient** (retry
under the startup deadline) or **terminal** (die now), plus an `ExitCode` enum whose numbers are
**greppable in `kubectl logs`** so the *reason* survives into the crash event.

## Why — learning objectives

By the end of this PR you will have practised:

- **`thiserror` in a library** — modelling failure as data, not stringly-typed panics.
- **Modelling a property, not guessing it** — terminal-vs-transient is a method on the enum, tested,
  not a comment.
- **`#[repr(i32)]` C-like enums** — mapping a rich error to a small, stable set of process exit codes.
- **`std::process::ExitCode` / `From` conversions** — how a binary's `main` will surface this later.

## Read first

- [`../../architecture.md`](../../architecture.md#startup--bootstrap-fail-fast-preflight) "Startup &
  bootstrap (fail-fast preflight)" (~line 1301) — read the **"Transient vs terminal"** paragraph
  closely: which classes retry to a deadline vs die immediately, and why each terminal failure needs
  a "distinct exit code so the reason is greppable."
- [`../README.md`](../README.md) "Conventions" rows *Errors — libraries* and *Errors — binaries*
  (`thiserror` enums in libs; `anyhow` + `map to common::ExitCode` in bins).

## Scope

**In scope**

- `common::Error` (`thiserror`) with variants for the bootstrap precondition classes: invalid
  config, control-DB unreachable, object-store unreachable/unusable, source preflight mismatch,
  publication/slot missing, keyless-table (strict mode), lease contended, and an `Internal` catch-all.
- `Error::is_terminal()` / `is_transient()` — the classifier the retry loop keys on.
- `ExitCode` (`#[repr(i32)]`) with a distinct code per terminal class (`Success = 0`).
- `Error::exit_code(&self) -> ExitCode`.
- Inline unit tests for `Display`, classification, and the code mapping.

**Explicitly deferred** (do *not* build these here)

- Any retry/backoff loop or startup-deadline logic — that lives in the bin bootstraps (**PR 2.18** /
  **PR 3.1**).
- `anyhow` wiring and the actual `main() -> ExitCode` mapping — **PR 2.18**.
- Config *parsing* errors as a real type — this PR only names the `Config` variant; **PR 0.5**
  produces it.

## Files to create / modify

```
crates/common/Cargo.toml         # + thiserror = { workspace = true }  (add thiserror = "2" to [workspace.dependencies])
crates/common/src/error.rs       # new — Error, ExitCode, classifier
crates/common/src/lib.rs         # + pub mod error; pub use error::{Error, ExitCode, Result};
```

## Skeleton

```rust
// crates/common/src/error.rs
use thiserror::Error;

/// Library-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Every way a walrus service can fail a precondition or an operation.
///
/// Invariant: whether a variant is terminal or transient is decided by
/// [`Error::is_terminal`], never by inspecting the message string.
#[derive(Debug, Error)]
pub enum Error {
    /// Misconfiguration — ConfigMap/env failed schema or bounds validation. Always terminal.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// Control Postgres could not be reached. May be transient during a rollout.
    #[error("control database unavailable: {0}")]
    ControlDb(String),

    /// Object store (S3/MinIO) unreachable or the canary head/put/get failed.
    #[error("object store unavailable: {0}")]
    ObjectStore(String),

    /// Source-server prerequisite mismatch (wal_level, version, slot/wal_sender headroom). Terminal.
    #[error("source preflight failed: {0}")]
    Preflight(String),

    /// A published table has no usable replica identity, in strict mode. Terminal.
    #[error("table {table} has no usable key (strict mode)")]
    KeylessTable { table: String },

    /// Another loader holds the table-ownership lease. Terminal for this pod.
    #[error("table ownership lease contended: {0}")]
    LeaseContended(String),

    /// Anything not otherwise classified. Terminal.
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// True when retrying under the startup deadline can never help — die now, non-zero.
    pub fn is_terminal(&self) -> bool { todo!() }

    /// The complement of [`is_terminal`] — a dependency that may still be coming up.
    pub fn is_transient(&self) -> bool { !self.is_terminal() }

    /// The distinct process exit code for this failure (greppable in `kubectl logs`).
    pub fn exit_code(&self) -> ExitCode { todo!() }
}

/// Stable, distinct exit statuses. Numbers are a public contract — never renumber, only append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    Success = 0,
    Config = 10,
    ControlDb = 11,
    ObjectStore = 12,
    Preflight = 13,
    KeylessTable = 14,
    LeaseContended = 15,
    Internal = 70,
}

impl From<ExitCode> for std::process::ExitCode {
    fn from(code: ExitCode) -> Self { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_is_terminal_control_db_is_transient() { todo!() }

    #[test]
    fn each_terminal_variant_maps_to_a_distinct_exit_code() { todo!() }

    #[test]
    fn display_states_precondition_and_observed_value() { todo!() }

    #[test]
    fn exit_code_zero_is_success_only() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `is_terminal()` returns `true` for `Config`, `Preflight`, `KeylessTable`, `LeaseContended`,
      `Internal`; `false` for `ControlDb`, `ObjectStore`.
- [ ] `exit_code()` maps every terminal variant to a **distinct** non-zero `ExitCode`; only
      `ExitCode::Success` is `0`.
- [ ] `Display` for each variant names the precondition **and** the observed value (actionable log).
- [ ] `From<ExitCode> for std::process::ExitCode` exists (the seam bins use in `main`).
- [ ] Doc comment records the invariant "classification is a method, never string-matching".
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common` (and `--workspace` stays green)

## Hints & gotchas

- Keep the **numbers stable** — treat `ExitCode` values like a wire protocol; runbooks and alerts
  will grep them. Append new codes; never renumber existing ones.
- Prefer a `match self { … }` in `is_terminal()` with **no `_ =>` arm** so adding a future variant is
  a *compile error* until you classify it. That is the whole point of modelling it as data.
- Don't make `Error` carry `#[from]` for `sqlx::Error` / `object_store::Error` yet — those crates
  aren't dependencies of `common` and never will be (they'd invert the DAG). The bins convert *into*
  these string variants with `anyhow` context at the call site.
- `std::process::ExitCode::from(u8)` only takes a `u8`; your `i32` reprs must fit — keep codes small
  (< 125) to stay clear of shell-reserved statuses.

## References

- Design: [`../../architecture.md`](../../architecture.md#startup--bootstrap-fail-fast-preflight)
  "Startup & bootstrap (fail-fast preflight)" — Transient vs terminal.
- Prev: [PR 0.1](./pr-0.1-workspace-skeleton-and-ci.md) · Next:
  [PR 0.3](./pr-0.3-common-lsn-newtype.md) · [Roadmap](../README.md)
