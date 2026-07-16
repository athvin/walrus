<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.7 — Deny `unwrap_used` / `expect_used`, allow in tests

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 7 — conventions hardening · **Crates touched:** workspace root, `loader`, `pg-sink`,
> `pg-to-arrow`, `tests/e2e`, docs · **Est. size:** S ·
> **Depends on:** PR 7.1–7.6 (a tree with no production offenders) · **Unlocks:** PR 7.8

The flip. With PR 7.6 having cleaned every production offender, add `unwrap_used = "deny"` and
`expect_used = "deny"` to `[workspace.lints.clippy]` and a repo-root `clippy.toml` that re-allows both
**in tests**. The whole point of the fix-then-flip split is here: CI going green on this tiny config
PR *is the proof* that production is clean. The one non-obvious part is scoping — `clippy.toml` covers
`#[cfg(test)]` modules and `tests/` dirs, but **not** benches (`harness = false`) or the e2e harness
*library*, so those carry a file-level `#![allow]`.

## Why — learning objectives

By the end of this PR you will have practised:

- **`clippy::restriction` lints** — `unwrap_used`/`expect_used` are opt-in restriction lints (not part
  of `clippy::all`), so denying them alongside `all = "deny"` needs no `priority` juggling.
- **`clippy.toml` test scoping** — `allow-unwrap-in-tests` / `allow-expect-in-tests` and exactly which
  compilation units they *don't* reach.
- **Reading `--all-targets`** — why a `harness = false` bench and a non-`#[cfg(test)]` helper lib
  compile *without* the test cfg and therefore trip the deny.

## Read first

- PR 7.6 — the offenders it removed (this PR must find zero new ones).
- `Cargo.toml` `[workspace.lints]` — where `rust.warnings` / `clippy.all` already live.
- [clippy.toml configuration](https://doc.rust-lang.org/clippy/configuration.html) —
  `allow-unwrap-in-tests` / `allow-expect-in-tests`.
- `tests/e2e/src/lib.rs` — a normal `[lib]` (no `#[cfg(test)]`) that `clippy.toml` will **not** exempt.

## Scope

**In scope**

- Add to `Cargo.toml` `[workspace.lints.clippy]`: `unwrap_used = "deny"`, `expect_used = "deny"`.
- New repo-root `clippy.toml`: `allow-unwrap-in-tests = true`, `allow-expect-in-tests = true`.
- Add a file-level `#![allow(clippy::unwrap_used, clippy::expect_used)]` to the compilation units
  `clippy.toml` can't reach: the **4 bench files** (`loader/benches/*`, `pg-sink/benches/*`,
  `pg-to-arrow/benches/*` — `harness = false`) and **`tests/e2e/src/lib.rs`** (the harness lib).
- Edit README Conventions **Lints** row + add the **CI grows** row (from PR 7.7); add the no-unwrap
  bullet to `TEMPLATE.md`'s header conventions.

**Explicitly deferred** (do *not* build these here)

- Any production `unwrap`/`expect` fix → already done in **PR 7.6**; if `clippy` finds one here, it's a
  7.6 miss — fix it in this PR and note it, don't expand scope.
- Enabling `unimplemented`/`unreachable`/`panic` restriction lints — out of scope.

## Files to create / modify

```
Cargo.toml                                  # + unwrap_used/expect_used = "deny" in [workspace.lints.clippy]
clippy.toml                                 # new — allow-unwrap-in-tests / allow-expect-in-tests
crates/loader/benches/*.rs                  # + #![allow(clippy::unwrap_used, clippy::expect_used)]
crates/pg-sink/benches/*.rs                 # + same
crates/pg-to-arrow/benches/*.rs             # + same
tests/e2e/src/lib.rs                        # + same (non-test-cfg harness lib)
docs/implementation/README.md               # modify — Conventions "Lints" row + "CI grows" row
docs/implementation/TEMPLATE.md             # modify — header conventions bullet (no-unwrap)
```

## Skeleton

```toml
# Cargo.toml — [workspace.lints.clippy]
all = "deny"
unwrap_used = "deny"     # restriction lint; not in `all`, so no priority conflict
expect_used = "deny"
```

```toml
# clippy.toml (repo root)
allow-unwrap-in-tests = true
allow-expect-in-tests = true
```

```rust
// top of each bench file and tests/e2e/src/lib.rs — clippy.toml can't reach these
#![allow(clippy::unwrap_used, clippy::expect_used)]
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `[workspace.lints.clippy]` denies `unwrap_used` and `expect_used`; `clippy.toml` allows both in
      tests; the 4 benches + `tests/e2e/src/lib.rs` carry the file-level allow.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` is green **with zero new
      `#[allow]`s in production code** — the green build is the proof the tree is clean.
- [ ] Inline `#[cfg(test)] mod tests` and `tests/` integration files still compile with their
      `unwrap`/`expect` (confirming the `clippy.toml` exemption works after the 7.1–7.3 relocation).
- [ ] README Conventions **Lints** row updated; a **CI grows** row added (from PR 7.7); `TEMPLATE.md`
      header gains the no-unwrap bullet.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`

## What completed looks like

```
$ cargo clippy --all-targets --all-features -- -D warnings
    Finished — 0 warnings          # unwrap_used/expect_used denied; tests exempt via clippy.toml
$ grep -A2 '\[workspace.lints.clippy\]' Cargo.toml
unwrap_used = "deny"
expect_used = "deny"
```

The compiler now forbids a production `unwrap`; the tests kept theirs; CI is the enforcement.

## Hints & gotchas

- `unwrap_used`/`expect_used` are `clippy::restriction`, **not** in `clippy::all`, so adding them next
  to `all = "deny"` needs **no** `priority` field — the priority mechanism is only for overriding a
  *group* you're also setting.
- `clippy.toml`'s allow-in-tests covers `#[cfg(test)]` modules **and** `crates/*/tests/*.rs` (Cargo
  builds those with `--cfg test`, so even top-level test-helper fns are exempt) — but there is **no**
  allow-in-benches key, and `tests/e2e/src/lib.rs` is a plain library compiled *without* `--cfg test`.
  Those five files are why the file-level `#![allow]` exists; don't try to solve them in `clippy.toml`.
- If `clippy` flags a production site here, PR 7.6 missed it — fix the *code* (don't `#[allow]` it) and
  mention it in the PR description.
- `rust-toolchain.toml` pins clippy to 1.95; both lints and both `clippy.toml` keys are stable well
  before that, so no MSRV concern.

## References

- Design: `docs/implementation/README.md` "Conventions" (Lints) + "CI grows".
- Prev: [PR 7.6](./pr-7.6-fix-unwrap-expect.md) · Next:
  [PR 7.8](./pr-7.8-identifier-convention-audit.md) · [Roadmap](../README.md)
