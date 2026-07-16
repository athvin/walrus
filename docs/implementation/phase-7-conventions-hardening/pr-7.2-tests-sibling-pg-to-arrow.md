<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.2 — Tests to sibling files: `pg-to-arrow`

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 7 — conventions hardening · **Crates touched:** `pg-to-arrow` ·
> **Est. size:** M · **Depends on:** PR 7.1 (pattern + Conventions row) · **Unlocks:** —

The same relocation as PR 7.1 (Go-style `src/foo.rs` → `src/foo_test.rs` via `#[path]`), applied to
`pg-to-arrow`'s **9** files with inline test modules. This crate holds the two largest test modules in
the sweep — `batch.rs` (~480 lines of RecordBatch-builder assertions) and `schema.rs` (~269 lines) —
so the payoff in source-file readability is the biggest here. The convention was set by 7.1; this PR
just executes it.

## Why — learning objectives

By the end of this PR you will have practised:

- **Applying an established convention** — the transform is fixed by 7.1; the skill is doing it
  faithfully at larger scale without drifting the layout.
- **Isolating a big test surface** — pulling a 480-line test module out of `batch.rs` so the builder
  code and its tests are each readable on their own.

## Read first

- [PR 7.1](./pr-7.1-tests-sibling-common-control-loader.md) — the mechanical transform and the
  module-resolution rules; this PR copies it verbatim.
- `docs/implementation/README.md` "Conventions" (Tests) — now already the sibling-file rule.

## Scope

**In scope**

- Convert the **9** `pg-to-arrow` files with an inline `#[cfg(test)] mod tests { … }` —
  `batch.rs`, `schema.rs`, `descriptor.rs`, `range.rs`, `parquet.rs`, `geometric.rs`, `tier3.rs`,
  `tier2.rs`, `uuid_enum.rs` — to `#[cfg(test)] #[path = "<module>_test.rs"] mod tests;` + a sibling
  `src/<module>_test.rs`.

**Explicitly deferred** (do *not* build these here)

- `pg-sink` extraction → **PR 7.3**.
- The Conventions "Tests" row / `TEMPLATE.md` edit — already landed in **PR 7.1**; do not re-edit.
- Any test-logic or conformance-harness change — pure relocation only.

## Files to create / modify

```
crates/pg-to-arrow/src/{batch,schema,descriptor,range,parquet,geometric,tier3,tier2,uuid_enum}.rs       # modify — inline block → #[path] mod tests;
crates/pg-to-arrow/src/{batch,schema,descriptor,range,parquet,geometric,tier3,tier2,uuid_enum}_test.rs  # new — moved bodies
```

## Skeleton

```rust
// AFTER — crates/pg-to-arrow/src/batch.rs (tail): the ~480-line block becomes three lines
#[cfg(test)]
#[path = "batch_test.rs"]
mod tests;
```

```rust
// AFTER — crates/pg-to-arrow/src/batch_test.rs (new): the moved body
use super::*;

#[test]
fn tier1_recordbatch_roundtrips() { /* … unchanged … */ }
// … the rest of the module, verbatim, un-indented one level …
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] All 9 files carry `#[cfg(test)] #[path = "<module>_test.rs"] mod tests;` with the body moved to
      `src/<module>_test.rs`; no `mod tests {` brace-block remains in `pg-to-arrow`.
- [ ] Pure relocation: `cargo test -p pg-to-arrow` (and the `conformance` feature) reports the same
      count as before (67 tests); the diff is moves only; no `foo.rs`→`foo/mod.rs` conversion.
- [ ] Docs/comments unchanged except where a moved doc-comment now needs its `//!`/`//` home.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-to-arrow` (and `--workspace` stays green)

## What completed looks like

```
$ grep -rl 'mod tests {' crates/pg-to-arrow --include='*.rs' | wc -l
0
$ find crates/pg-to-arrow -name '*_test.rs' | wc -l
9
$ wc -l crates/pg-to-arrow/src/batch.rs   # was ~1360, now ~880 (production surface only)
```

`batch.rs` reads as builder code; `batch_test.rs` reads as its assertions. Same green suite.

## Hints & gotchas

- `batch.rs` and `schema.rs` are the big ones — extract them first and run `cargo test -p pg-to-arrow`
  to catch any missed `use` before touching the small files.
- Watch for test helpers defined *inside* the old `mod tests { … }` (fake builders, sample
  descriptors): they move with the body into `tests.rs` and stay private to it — no change needed.
- If a test referenced a `pub(crate)`/private item via `super::`, it still resolves; if it reached a
  *sibling* module's private item (unusual), it used `crate::…` and that path is unaffected.

## References

- Design: `docs/implementation/README.md` "Conventions" (Tests); the transform is PR 7.1's.
- Prev: [PR 7.1](./pr-7.1-tests-sibling-common-control-loader.md) · Next:
  [PR 7.3](./pr-7.3-tests-sibling-pg-sink.md) · [Roadmap](../README.md)
