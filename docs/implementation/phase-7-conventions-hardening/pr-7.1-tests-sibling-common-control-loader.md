<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.1 — Tests to sibling files: `common`, `control`, `loader`

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 7 — conventions hardening · **Crates touched:** `common`, `control`, `loader`, docs ·
> **Est. size:** M · **Depends on:** — · **Unlocks:** PR 7.2, PR 7.3 (they copy the pattern set here)

The first of three mechanical sweeps that lift every inline `#[cfg(test)] mod tests { … }` block into
a sibling in-crate file, `src/foo.rs` → `src/foo/tests.rs`. It stays a unit test — an in-crate child
module with full private-item access (`use super::*` still resolves) — it just lives in its own file.
This PR does the three small crates (14 files total) **and sets the pattern**: it edits the roadmap
Conventions "Tests" row and the `TEMPLATE.md` skeleton so PRs 7.2/7.3 and every future task inherit
the new layout. Not a single test body changes; the value is that the source files shrink to their
production surface and the test file is where you look for tests.

## Why — learning objectives

By the end of this PR you will have practised:

- **Rust module resolution** — why `mod tests;` inside `src/foo.rs` resolves to `src/foo/tests.rs`
  (the non-`mod.rs` path rule), and why `foo.rs` need **not** become `foo/mod.rs`.
- **Visibility across files** — a child module still sees its parent's private items regardless of
  which file it lives in; `super` is a module-graph edge, not a filesystem one.
- **Behaviour-preserving refactor discipline** — proving a change is a pure relocation (identical
  `cargo test` count, `git` shows only moves) rather than a rewrite.

## Read first

- `docs/implementation/README.md` "Conventions" table (the **Tests** row you are changing) and the
  "Testing layers" prose beneath it.
- `docs/implementation/TEMPLATE.md` — the Skeleton block still shows the inline layout; you update it.
- The [Rust reference on module file paths](https://doc.rust-lang.org/reference/items/modules.html)
  — the `foo.rs` + `foo/` coexistence rule.

## Scope

**In scope**

- Convert the **14** files across `common` (8), `control` (2), `loader` (4) that carry an inline
  `#[cfg(test)] mod tests { … }` module: replace the block with `#[cfg(test)] mod tests;` and move the
  body verbatim (the `use super::*;` and every test fn) into a new sibling `src/<module>/tests.rs`.
- Set the phase's test-layout convention: edit the README Conventions **Tests** row and the
  `TEMPLATE.md` **Skeleton** + **Files to create / modify** blocks to model `src/<module>/tests.rs`.

**Explicitly deferred** (do *not* build these here)

- `pg-to-arrow` test extraction → **PR 7.2**.
- `pg-sink` test extraction → **PR 7.3**.
- Any change to test *logic*, helper sharing, or moving unit tests into `tests/` integration dirs —
  this is a pure relocation, nothing else.

## Files to create / modify

```
crates/common/src/{lsn,config,error,pg_shape,sink_meta,telemetry,type_descriptor,metrics}.rs  # modify — inline block → `mod tests;`
crates/common/src/{lsn,config,error,pg_shape,sink_meta,telemetry,type_descriptor,metrics}/tests.rs  # new — moved bodies
crates/control/src/{reload,db}.rs           # modify
crates/control/src/{reload,db}/tests.rs     # new
crates/loader/src/{duck,epoch,phase_a,health}.rs        # modify
crates/loader/src/{duck,epoch,phase_a,health}/tests.rs  # new
docs/implementation/README.md               # modify — Conventions "Tests" row (+ Testing-layers prose if it implies inline)
docs/implementation/TEMPLATE.md             # modify — Skeleton test block + Files example
```

## Skeleton

<!-- These are refactors, so the "shape" is the before→after of the mechanical transform,
     not a `todo!()` body. Apply it identically to all 14 files. -->

```rust
// BEFORE — crates/common/src/lsn.rs (tail)
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_orders() { /* … */ }
}
```

```rust
// AFTER — crates/common/src/lsn.rs (tail)
#[cfg(test)]
mod tests;
```

```rust
// AFTER — crates/common/src/lsn/tests.rs  (new file — the body, un-indented one level)
use super::*;

#[test]
fn parses_and_orders() { /* … unchanged … */ }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] All 14 files now carry `#[cfg(test)] mod tests;`; the matching `src/<module>/tests.rs` holds the
      moved body; **no** `#[cfg(test)] mod tests {` brace-block remains in the three crates.
- [ ] Pure relocation: `cargo test -p common -p control -p loader` reports the **same test count** as
      before; the diff is moves only (no test-body edits); no `#[path]` used; no `foo.rs` was turned
      into `foo/mod.rs`.
- [ ] The nested / crate-root cases are handled correctly (none in these three crates today — assert
      that `lib.rs`/`main.rs` carry no inline tests, so the `src/tests.rs` special case is unused).
- [ ] README Conventions **Tests** row updated to the sibling-file rule; `TEMPLATE.md` Skeleton +
      Files example updated so 7.2/7.3 and future tasks model it.
- [ ] Docs/comments explain the module-resolution rule where a reader might be surprised.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common -p control -p loader` (and `--workspace` stays green)

## What completed looks like

```
$ grep -rl 'mod tests {' crates/common crates/control crates/loader --include='*.rs' | wc -l
0
$ find crates/common crates/control crates/loader -name tests.rs | wc -l
14
$ cargo test -p common -p control -p loader 2>&1 | grep -Eo '[0-9]+ passed' | tail -1
<same count as before this PR>
```

Fourteen source files got shorter, fourteen `tests.rs` appeared, and the suite didn't move an inch.

## Hints & gotchas

- **`src/foo.rs` and `src/foo/tests.rs` coexist** — you do *not* rename `foo.rs` to `foo/mod.rs`
  (that is the old 2015 layout and is unnecessary here). You only create the `src/foo/` directory to
  hold `tests.rs`.
- `use super::*;` keeps working from the moved file because `tests` is still a child of `foo`; the
  same is true for any `use crate::…` the module already had, and for access to **private** items.
- Do the transform one crate at a time and run `cargo test -p <crate>` between them — a misplaced
  sibling simply fails to compile, so the compiler is your safety net.
- No file in these three crates has extra attributes on the `mod tests` line and each opens with
  `use super::*;` — the transform is uniform. Un-indent the body exactly one level so `fmt` is a no-op.
- Update the README **Tests** row and `TEMPLATE.md` in *this* PR (it owns the pattern); 7.2/7.3 only
  reference the now-current convention.

## References

- Design: `docs/implementation/README.md` "Conventions" (Tests) — the rule this phase rewrites.
- Prev: — · Next: [PR 7.2](./pr-7.2-tests-sibling-pg-to-arrow.md) · [Roadmap](../README.md)
