<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.

  Conventions this template assumes (see ../README.md for the full list):
    * libs return `thiserror` errors; binaries use `anyhow`.
    * every crate is `#![deny(warnings)]`-clean under `clippy --all-targets`.
    * "green" always means: cargo fmt --check && cargo clippy --all-targets -D warnings && cargo test.
    * skeletons are compilable SHAPES with `todo!()` — you write the logic.
-->

# PR X.Y — <imperative title, e.g. "Add the `Lsn` newtype">

> **Phase:** <n — name> · **Crates touched:** `<crate>` … · **Est. size:** <S | M | L> ·
> **Depends on:** PR <a.b> · **Unlocks:** PR <c.d>

<One-paragraph statement of what this PR delivers and why it exists now — the
single vertical slice of behaviour that will be true after it merges.>

## Why — learning objectives

By the end of this PR you will have practised:

- **<Rust concept>** — <one line on where it shows up here>.
- **<CDC/system concept>** — <one line>.
- <…>

## Read first

- `docs/<doc>.md` <§ or heading> — <what to take from it>.
- <golden vectors / example file, if any> — <how it's used>.
- External: <source number + title from architecture.md "Sources">, if relevant.

## Scope

**In scope**

- <the minimal set of behaviours this PR adds>

**Explicitly deferred** (do *not* build these here)

- <thing> → **PR <n.m>**. <why it waits>

## Files to create / modify

```
<crate>/Cargo.toml                 # + <dep> = "x.y"
<crate>/src/<module>.rs            # new
<crate>/tests/<name>.rs            # new (if integration/golden tests)
```

## Skeleton

<!-- Give the SHAPES: public signatures, enums, error variants, test names.
     Bodies are `todo!()`. The learner fills them in. Keep it compilable-shaped. -->

```rust
// <crate>/src/<module>.rs

/// <doc comment stating the contract>
pub struct Foo { /* … */ }

impl Foo {
    pub fn new(/* … */) -> Result<Self, crate::Error> { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn <descriptive_test_name>() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] <behaviour 1 — a concrete, testable assertion>
- [ ] <behaviour 2>
- [ ] Docs/comments explain any non-obvious invariant.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p <crate>` (and `--workspace` stays green)
  - [ ] <if applicable> `docker compose up --wait` then `<the integration assertion>`

## Hints & gotchas

- <senior-dev tip: an idiom, a pitfall the design doc already warns about, a
  crate-API sharp edge>.
- <…>

## References

- Design: `docs/architecture.md` <§>, `docs/walrus-pg-sink.md` <§>, `docs/walrus-loader.md` <§>.
- Prev: [PR <a.b>](<file>) · Next: [PR <c.d>](<file>) · [Roadmap](../README.md)
