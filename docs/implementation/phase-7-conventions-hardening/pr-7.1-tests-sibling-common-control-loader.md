<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.1 — Tests to sibling files: `common`, `control`, `loader`

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/106

> **Phase:** 7 — conventions hardening · **Crates touched:** `common`, `control`, `loader`, docs ·
> **Est. size:** M · **Depends on:** — · **Unlocks:** PR 7.2, PR 7.3 (they copy the pattern set here)

The first of three mechanical sweeps that lift every inline `#[cfg(test)] mod tests { … }` block into
a **Go-style sibling file**, `src/foo.rs` → `src/foo_test.rs`. It stays a unit test — an in-crate
child module with full private-item access (`use super::*` still resolves) — it just lives in its own
`_test.rs` file next to the module it tests. Because a bodyless `mod tests;` would resolve to
`src/foo/tests.rs` (the default directory rule), the sibling-`_test.rs` layout is wired with an
explicit `#[path]`. This PR does the three small crates (14 files total) **and sets the pattern**: it
edits the roadmap Conventions "Tests" row and the `TEMPLATE.md` skeleton so PRs 7.2/7.3 and every
future task inherit the layout. Not a single test body changes; the value is that the source files
shrink to their production surface and `foo_test.rs` is where you look for foo's tests.

## Why — learning objectives

By the end of this PR you will have practised:

- **The `#[path]` attribute** — why `#[cfg(test)] #[path = "foo_test.rs"] mod tests;` inside
  `src/foo.rs` resolves to `src/foo_test.rs` (path relative to the containing file's directory),
  whereas a bare `mod tests;` would look in `src/foo/`.
- **Visibility across files** — a child module still sees its parent's private items regardless of
  which file it lives in; `super` is a module-graph edge, not a filesystem one.
- **Behaviour-preserving refactor discipline** — proving a change is a pure relocation (identical
  `cargo test` count, `git` shows only moves) rather than a rewrite.

## Read first

- `docs/implementation/README.md` "Conventions" table (the **Tests** row you are changing) and the
  "Testing layers" prose beneath it.
- `docs/implementation/TEMPLATE.md` — the Skeleton block still shows the inline layout; you update it.
- The [Rust reference on the `path` attribute](https://doc.rust-lang.org/reference/items/modules.html#the-path-attribute)
  — resolution relative to the current file's directory.

## Scope

**In scope**

- Convert the **14** files across `common` (8), `control` (2), `loader` (4) that carry an inline
  `#[cfg(test)] mod tests { … }` module: replace the block with
  `#[cfg(test)] #[path = "<module>_test.rs"] mod tests;` and move the body verbatim (the
  `use super::*;` and every test fn) into a new sibling `src/<module>_test.rs`.
- Set the phase's test-layout convention: edit the README Conventions **Tests** row and the
  `TEMPLATE.md` **Skeleton** + **Files to create / modify** + header blocks to model `src/foo_test.rs`.

**Explicitly deferred** (do *not* build these here)

- `pg-to-arrow` test extraction → **PR 7.2**.
- `pg-sink` test extraction → **PR 7.3**.
- Any change to test *logic*, helper sharing, or moving unit tests into `tests/` integration dirs —
  this is a pure relocation, nothing else.

## Files to create / modify

```
crates/common/src/{lsn,config,error,pg_shape,sink_meta,telemetry,type_descriptor,metrics}.rs  # modify — inline block → #[path] mod tests;
crates/common/src/{lsn,config,error,pg_shape,sink_meta,telemetry,type_descriptor,metrics}_test.rs  # new — moved bodies
crates/control/src/{reload,db}.rs           # modify
crates/control/src/{reload,db}_test.rs      # new
crates/loader/src/{duck,epoch,phase_a,health}.rs        # modify
crates/loader/src/{duck,epoch,phase_a,health}_test.rs   # new
docs/implementation/README.md               # modify — Conventions "Tests" row
docs/implementation/TEMPLATE.md             # modify — header bullet + Skeleton test block + Files example
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
#[path = "lsn_test.rs"]
mod tests;
```

```rust
// AFTER — crates/common/src/lsn_test.rs  (new file — the body, un-indented one level)
use super::*;

#[test]
fn parses_and_orders() { /* … unchanged … */ }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] All 14 files now carry `#[cfg(test)] #[path = "<module>_test.rs"] mod tests;`; the matching
      `src/<module>_test.rs` holds the moved body; **no** `#[cfg(test)] mod tests {` brace-block
      remains in the three crates.
- [x] Pure relocation: `cargo test -p common -p control -p loader` reports the **same test count** as
      before (55 unit tests across the 14 modules); the diff is moves only (no test-body edits); no
      `foo.rs` was turned into `foo/mod.rs`.
- [x] The crate-root case is confirmed absent (no `lib.rs`/`main.rs` carries an inline test module, so
      no `src/lib_test.rs` special case is needed).
- [x] README Conventions **Tests** row updated to the `foo_test.rs` rule; `TEMPLATE.md` header +
      Skeleton + Files example updated so 7.2/7.3 and future tasks model it.
- [x] Docs/comments explain the `#[path]` resolution where a reader might be surprised.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p common -p control -p loader` (and `--workspace` stays green)

## What completed looks like

```
$ grep -rl 'mod tests {' crates/common crates/control crates/loader --include='*.rs' | wc -l
0
$ find crates/common crates/control crates/loader -name '*_test.rs' | wc -l
14
$ cargo test -p common -p control -p loader 2>&1 | grep -Eo '[0-9]+ passed' | tail -1
<same count as before this PR>
```

Fourteen source files got shorter, fourteen `*_test.rs` appeared next to them, and the suite didn't
move an inch.

## Hints & gotchas

- **A bare `mod tests;` will NOT find `foo_test.rs`** — the default resolver looks in `src/foo/`. The
  Go-style sibling requires the explicit `#[path = "foo_test.rs"]`; it resolves relative to the
  directory of the file that declares the `mod` (here `src/`).
- `use super::*;` keeps working from the moved file because `tests` is still a child of `foo`; the
  same is true for any `use crate::…` the module already had, and for access to **private** items.
- `src/foo.rs` and its sibling `src/foo_test.rs` are just two files in `src/` — you do *not* rename
  `foo.rs` to `foo/mod.rs`, and no subdirectory is created.
- No file in these three crates has extra attributes on the `mod tests` line and each opens with
  `use super::*;` — the transform is uniform. Un-indent the body one level so `fmt` is a no-op (or run
  `cargo fmt` and let it dedent).
- Update the README **Tests** row and `TEMPLATE.md` in *this* PR (it owns the pattern); 7.2/7.3 only
  reference the now-current convention.

## References

- Design: `docs/implementation/README.md` "Conventions" (Tests) — the rule this phase rewrites.
- Prev: — · Next: [PR 7.2](./pr-7.2-tests-sibling-pg-to-arrow.md) · [Roadmap](../README.md)
