# PR 0.1 — Scaffold the Cargo workspace and the first CI gate

> **Phase:** 0 — Foundations & CI · **Crates touched:** `common` (new), workspace root ·
> **Est. size:** M · **Depends on:** — (first PR) · **Unlocks:** PR 0.2

This PR turns a code-zero repo into a **compiling, lint-clean Cargo workspace** with exactly one
empty member crate (`common`) and a GitHub Actions gate that enforces `fmt` / `clippy` / `build` /
`test` on every push. Nothing does anything yet — the point is that "green" now has a precise,
machine-checked meaning that every later PR inherits.

## Why — learning objectives

By the end of this PR you will have practised:

- **Cargo workspaces** — `resolver = "2"`, shared `[workspace.lints]` and `[workspace.dependencies]`,
  member crates that inherit both.
- **Lint-as-policy** — enforcing `#![deny(warnings)]` centrally so no crate can opt out.
- **Toolchain pinning** — a `rust-toolchain.toml` so local and CI builds agree byte-for-byte.
- **CI as the definition of "green"** — the four commands the whole curriculum keys on.

## Read first

- [`../../architecture.md`](../../architecture.md#proposed-rust-workspace-layout) "Proposed Rust
  workspace layout" (~line 1440) — the five-crate target and why members drop the `walrus-` prefix.
- [`../../architecture.md`](../../architecture.md#phased-roadmap) "Phased roadmap" step 0 (~line
  1470) — this PR is the "Scaffold" bullet.
- [`../README.md`](../README.md) "Target workspace layout" + "Conventions" + "CI grows with the
  phases" — the DAG and the exact gate list this CI file implements.
- The repo-root [`README.md`](../../../README.md) — it already references `LICENSE`; this PR adds it.

## Scope

**In scope**

- Root `Cargo.toml`: `[workspace]` (resolver 2), `[workspace.lints]` denying warnings + clippy,
  `[workspace.dependencies]` (empty for now), member list `= ["crates/common"]`.
- `rust-toolchain.toml` pinning a specific stable channel + `rustfmt`, `clippy` components.
- `crates/common` as an **empty library** (`lib.rs` with a crate-level doc comment, no public API).
- `.gitignore` (including `/target` and the already-committed `docs/examples/proto-version/__pycache__`).
- MIT `LICENSE`.
- `.github/workflows/ci.yml` running the four gates on `push` / `pull_request`.

**Explicitly deferred** (do *not* build these here)

- Any real code in `common` — errors → **PR 0.2**, `Lsn` → **PR 0.3**, telemetry → **PR 0.4**,
  config → **PR 0.5**.
- The other four crates (`pg-to-arrow`, `control`, `pg-sink`, `loader`) — each is added to the member
  list by the PR that creates it.
- The `docker compose` CI job → **PR 0.6**; `cargo-deny` → **PR 4.7**.

## Files to create / modify

```
Cargo.toml                       # new — [workspace] resolver=2, lints, deps, members
rust-toolchain.toml              # new — pinned stable channel + components
.gitignore                       # new — /target, **/__pycache__, .env
LICENSE                          # new — MIT, © the repo owner
crates/common/Cargo.toml         # new — [lints] workspace = true
crates/common/src/lib.rs         # new — crate doc comment only
.github/workflows/ci.yml         # new — fmt / clippy / build / test
```

## Skeleton

```toml
# Cargo.toml (workspace root)
[workspace]
resolver = "2"
members = ["crates/common"]        # grows one crate at a time, per the DAG

[workspace.package]
edition = "2021"
license = "MIT"
rust-version = "1.XX"              # keep in sync with rust-toolchain.toml (MSRV formalised in PR 4.7)

[workspace.lints.rust]
warnings = "deny"                  # the `#![deny(warnings)]` policy, centralised

[workspace.lints.clippy]
all = "deny"

[workspace.dependencies]
# shared version pins land here as PRs add deps (thiserror 0.2, serde 1, tracing 0.1, …)
```

```toml
# crates/common/Cargo.toml
[package]
name = "common"
version = "0.1.0"
edition.workspace = true
license.workspace = true

[lints]
workspace = true                   # inherit deny(warnings) + clippy::all

[dependencies]
# empty until PR 0.2
```

```rust
// crates/common/src/lib.rs
//! Shared primitives for walrus: errors + exit codes, `Lsn`, telemetry, config,
//! `SinkMeta`, and the neutral Postgres shape types. Populated PR by PR (0.2 →).
```

```toml
# rust-toolchain.toml
[toolchain]
channel = "1.XX.0"                 # pin an explicit stable; bump deliberately
components = ["rustfmt", "clippy"]
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `cargo build --workspace` compiles the empty `common` crate.
- [ ] `.gitignore` untracks `/target`, `.env`, and the committed
      `docs/examples/proto-version/__pycache__/` (run `git rm -r --cached` on the latter).
- [ ] `LICENSE` exists (MIT) so the repo-root README's link resolves.
- [ ] `rust-toolchain.toml` pins one explicit stable channel; local + CI report the same `rustc -V`.
- [ ] CI runs on push/PR and is **red if any gate fails**.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace` (passes with zero tests)

## Hints & gotchas

- `[workspace.lints]` only takes effect in a member once that member declares `[lints] workspace =
  true` — it is **opt-in per crate**, not automatic. Forget it and `deny(warnings)` silently does
  nothing. Add it to every future crate's `Cargo.toml` on day one.
- Pin the toolchain to a concrete version (`1.XX.0`), not `stable` — otherwise a new stable release
  can turn a fresh clippy lint into a CI failure you never changed code for.
- In CI, cache `~/.cargo` and `target/` (e.g. `Swatinem/rust-cache`) so the empty build stays fast as
  crates land; run `fmt --check` first (cheapest) and fail fast.
- `resolver = "2"` matters even now — it is the edition-2021 default but must be explicit in a
  workspace root, and it changes feature unification once you add `dev-dependencies` in later PRs.

## References

- Design: [`../../architecture.md`](../../architecture.md#proposed-rust-workspace-layout) "Proposed
  Rust workspace layout"; [`../../architecture.md`](../../architecture.md#phased-roadmap) "Phased
  roadmap" (step 0).
- Prev: — (first PR) · Next: [PR 0.2](./pr-0.2-common-errors-exit-codes.md) · [Roadmap](../README.md)
