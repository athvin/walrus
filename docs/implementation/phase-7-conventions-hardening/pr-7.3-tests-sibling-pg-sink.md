<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.3 — Tests to sibling files: `pg-sink`

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 7 — conventions hardening · **Crates touched:** `pg-sink` ·
> **Est. size:** L · **Depends on:** PR 7.1 (pattern + Conventions row) · **Unlocks:** —

The largest test surface in the phase: `pg-sink`'s **21** files with inline test modules, including
the decoder-state monster `stream_txn.rs` (~224-line module) and `batch.rs` (~214), plus the one
**nested** case — `pgoutput/typmod.rs`, whose sibling lands at `pgoutput/typmod/tests.rs`. Same
mechanical transform as 7.1/7.2, larger blast radius, so it earns its own PR.

## Why — learning objectives

By the end of this PR you will have practised:

- **The nested-module resolution case** — a `mod tests;` inside a *submodule* (`pgoutput/typmod.rs`)
  resolves into that submodule's directory (`pgoutput/typmod/tests.rs`), not the crate root.
- **Refactoring at scale without drift** — 21 identical transforms, each independently compile-checked.

## Read first

- [PR 7.1](./pr-7.1-tests-sibling-common-control-loader.md) — the transform + module-resolution rules.
- `crates/pg-sink/src/pgoutput/mod.rs` — the existing `pub mod reader;` shows the same directory-child
  resolution you rely on for `typmod`.

## Scope

**In scope**

- Convert all **21** `pg-sink` files with an inline `#[cfg(test)] mod tests { … }` to
  `#[cfg(test)] mod tests;` + a sibling `src/<module>/tests.rs`. The set includes `stream_txn`,
  `batch`, `reload_signal`, `reload_export`, `snapshot`, `config`, `ddl`, `reload`, `heartbeat`,
  `relcache`, `preflight`, `memory`, `replication`, `bootstrap`, `checkpoint`, `epoch`, `manifest`,
  `health`, `sink`, `shutdown`, and the nested `pgoutput/typmod`.

**Explicitly deferred** (do *not* build these here)

- The Conventions "Tests" row / `TEMPLATE.md` edit — landed in **PR 7.1**.
- Any production-code change (the `unwrap`/`expect` fixes are **PR 7.6**, and some of them live in
  files touched here — do **not** fold them in; keep this PR a pure test relocation).

## Files to create / modify

```
crates/pg-sink/src/<module>.rs            # modify ×21 — inline block → `mod tests;`
crates/pg-sink/src/<module>/tests.rs      # new ×21 — moved bodies
crates/pg-sink/src/pgoutput/typmod.rs     # modify — nested case
crates/pg-sink/src/pgoutput/typmod/tests.rs  # new — nested sibling
```

## Skeleton

```rust
// AFTER — crates/pg-sink/src/stream_txn.rs (tail)
#[cfg(test)]
mod tests;
```

```rust
// AFTER — crates/pg-sink/src/pgoutput/typmod.rs (the nested case)
#[cfg(test)]
mod tests;   // resolves to crates/pg-sink/src/pgoutput/typmod/tests.rs
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] All 21 files carry `#[cfg(test)] mod tests;` with the body moved to `src/<module>/tests.rs`
      (and `pgoutput/typmod/tests.rs` for the nested one); no `mod tests {` brace-block remains in
      `pg-sink`.
- [ ] Pure relocation: `cargo test -p pg-sink` reports the same count as before; the diff is moves
      only; no `#[path]`; no `foo.rs`→`foo/mod.rs`; production code untouched.
- [ ] The nested `pgoutput/typmod` case compiles and its test runs (resolution into the submodule dir
      is correct).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)

## What completed looks like

```
$ grep -rl 'mod tests {' crates/pg-sink --include='*.rs' | wc -l
0
$ find crates/pg-sink -name tests.rs | wc -l
21
$ cargo test -p pg-sink 2>&1 | grep -Eo '[0-9]+ passed' | tail -1
<same count as before this PR>
```

## Hints & gotchas

- Some of these files (`reload_signal`, `reload_export`, `stream_txn`, `replication`) also contain the
  production `unwrap`/`expect` that **PR 7.6** fixes. This PR must **not** touch that production code —
  only the `#[cfg(test)]` block moves. Keeping the two PRs disjoint keeps each reviewable.
- Any `.unwrap()`/`.expect()` *inside* the moved test bodies is fine and stays — it will be legal once
  PR 7.7's `clippy.toml` allows unwrap/expect in tests; there is no lint pressure on tests before then.
- For the nested `pgoutput/typmod.rs`, create the directory `pgoutput/typmod/` next to the file; the
  child resolves there exactly as `pgoutput/reader.rs` already resolves from `pgoutput/mod.rs`.
- Work in batches (e.g. 5 files at a time) with `cargo test -p pg-sink` between batches; a missed
  `use super::*;` fails fast at compile time.

## References

- Design: `docs/implementation/README.md` "Conventions" (Tests); the transform is PR 7.1's.
- Prev: [PR 7.2](./pr-7.2-tests-sibling-pg-to-arrow.md) · Next:
  [PR 7.4](./pr-7.4-control-sql-query-file.md) · [Roadmap](../README.md)
