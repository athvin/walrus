<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 8.1 — One audited `sql_literal()` helper for single-quote escaping

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 8 — cleanup · **Crates touched:** `common`, `loader`, `pg-sink` ·
> **Est. size:** S · **Depends on:** PR 7.8 (phase 7 complete) · **Unlocks:** —

The rule "escape a SQL string literal by doubling its single quotes" is currently
hand-rolled in **six production spots across four files**, with no shared function — two of
them (`pg-sink/src/reload_export.rs:578` and `pg-sink/src/preflight.rs:438`) are
byte-for-byte the same `format!("'{}'", s.replace('\'', "''"))`. This PR gives that rule
one home — `common::sql::sql_literal` — and routes every caller through it, so the escaping
is defined and tested **once**.

Honest scope note: the inputs here are largely internal (S3 credentials, S3 URIs, LSN
text), so this is *not* patching a known injection — it is collapsing a duplicated escaping
rule that could drift into one auditable function. Frame it that way; don't oversell it as
a security fix.

## Why — learning objectives

By the end of this PR you will have practised:

- **Placing a shared `pub fn` at the right layer** — `common` is the lowest crate, so a
  helper there is reachable from `loader` and `pg-sink` alike.
- **`&str` → `String` ownership** — the helper borrows and returns an owned, escaped copy.
- **Doc-tests as executable documentation** — a `/// ```` example that `cargo test` runs
  is the cheapest possible proof of `O'Brien` → `O''Brien`.
- **DRY as a safety property** — one escaping definition can't disagree with itself.

## Read first

- `crates/loader/src/duck.rs` lines **132** (`let esc = |v: &str| …`), **162**, **176** —
  three sites in one file (a local closure and two inline calls).
- `crates/loader/src/ddl.rs:232`, `crates/pg-sink/src/reload_export.rs:578`,
  `crates/pg-sink/src/preflight.rs:438` — the other three (the last two identical).
- `crates/common/src/lsn.rs` — the shape of an existing small `common` module + its sibling
  `lsn_test.rs`, the pattern to copy.

## Scope

**In scope**

- New `crates/common/src/sql.rs` exporting `pub fn sql_literal(s: &str) -> String` that
  doubles every `'`; re-export it from `common`'s `lib.rs`.
- A Go-style sibling `crates/common/src/sql_test.rs`.
- Swap all **six** production call sites to `common::sql::sql_literal`, deleting the local
  `esc` closure in `duck.rs`.

**Explicitly deferred** (do *not* build these here)

- Identifier (double-quote) escaping / a `QualifiedName` newtype → **PR 8.4**. This PR is
  only string-*literal* (single-quote) escaping; `"` doubling for identifiers is a separate
  concern.
- Touching the test-only site `pg-to-arrow/tests/conformance.rs:63` is optional — swap it
  for consistency if trivial, but it is not load-bearing.

## Files to create / modify

```
crates/common/src/sql.rs           # new — sql_literal()
crates/common/src/sql_test.rs      # new — unit tests (Go-style sibling via #[path])
crates/common/src/lib.rs           # + pub mod sql;  (and re-export if the crate re-exports)
crates/loader/src/duck.rs          # modify — drop `esc` closure; 3 call sites → sql_literal
crates/loader/src/ddl.rs           # modify — 1 call site
crates/pg-sink/src/reload_export.rs# modify — 1 call site
crates/pg-sink/src/preflight.rs    # modify — 1 call site
```

## Skeleton

```rust
// crates/common/src/sql.rs

/// Escape a string for interpolation as a **single-quoted SQL string literal**: double every
/// `'`. The caller still supplies the surrounding quotes (`format!("'{}'", sql_literal(s))`).
///
/// This is literal escaping only — it is *not* identifier quoting (that doubles `"`).
///
/// ```
/// # use common::sql::sql_literal;
/// assert_eq!(sql_literal("O'Brien"), "O''Brien");
/// assert_eq!(sql_literal("plain"), "plain");
/// ```
pub fn sql_literal(s: &str) -> String {
    todo!()
}

#[cfg(test)]
#[path = "sql_test.rs"]
mod tests;
```

```rust
// crates/common/src/sql_test.rs

use super::*;

#[test]
fn doubles_single_quotes() { todo!() }

#[test]
fn leaves_clean_strings_untouched() { todo!() }

#[test]
fn empty_string_is_empty() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `common::sql::sql_literal` exists, is documented, and has a passing doc-test.
- [ ] All six production call sites use it; `rg -n "replace\('\\\\''" crates -g '!*_test.rs'`
      returns **no** production hits (only the optional conformance test may remain).
- [ ] The `esc` closure in `duck.rs` is gone; behaviour at every swapped site is unchanged.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common` (and `--workspace` stays green)

## What completed looks like

```
$ rg -n "replace\('\\\\''" crates -g '!*_test.rs'
$ # (no output — every production single-quote escape now goes through common::sql)

$ rg -n "sql_literal" crates -g '!*_test.rs' | wc -l
6
```

The `.replace('\'', "''")` bucket is empty in production; the `sql_literal` bucket has
exactly the six call sites.

## Hints & gotchas

- Keep the surrounding quotes at the call site (`format!("'{}'", sql_literal(x))`), not
  inside the helper — several callers embed the value mid-statement and add quotes
  themselves; a helper that also quotes would double them.
- `duck.rs:162`/`176` and `configure_s3`'s `esc` all reduce to the same call — verify each
  by eye, because a couple wrap the result in quotes and a couple don't.
- Don't widen scope into identifier quoting (`"`); that's PR 8.4's `QualifiedName`. Mixing
  the two escaping rules in one helper is exactly the confusion this PR removes.

## References

- Design: `docs/implementation/README.md` "Conventions" (SQL location) — this extends the
  same "SQL is reviewable, not a buried string" spirit to escaping.
- Prev: — · Next: [PR 8.2](./pr-8.2-manifest-kind-status-enums.md) ·
  [Phase 8](./README.md) · [Roadmap](../README.md)
