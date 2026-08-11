<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.5 — Return `Cow<'_, str>` from `sql_literal` so the common no-quote case never allocates

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `common`, `loader`, `pg-sink` ·
> **Est. size:** M · **Depends on:** PR 9.4 · **Unlocks:** PR 9.6

`Cow` appears **0 times** in the entire tree — crates *and* tests. The cleanest place to introduce it
is the helper PR 8.1 built: `common::sql::sql_literal` (`crates/common/src/sql.rs:14`) is a bare
`s.replace('\'', "''")`, an **unconditional `String` allocation on every call**, even though almost
none of the identifiers, LSN strings, S3 URIs and comment texts it escapes contain a single quote.
It has **6 production call sites** across `loader` and `pg-sink`. After this PR the signature is
`pub fn sql_literal(s: &str) -> Cow<'_, str>`: the no-quote path returns `Cow::Borrowed` and
allocates nothing, the quote path returns `Cow::Owned` exactly as before, every call site compiles
unchanged through `Deref`, and the PR 8.1 doctest keeps passing because `Cow<'_, str>` implements
`PartialEq<&str>`.

Be honest in the PR body about the size of the win: these are short strings on a per-statement path,
not a per-row one, so this is an **API-shape** change (the caller stops paying for an allocation it
usually does not need) rather than a benchmarked speedup. Do not claim a measurement you did not
take — `anti-premature-optimize` and `perf-profile-first` are rules from this same corpus.

## Why — learning objectives

- **`Cow<'a, T>` is conditional ownership** — one enum, two variants: `Cow::Borrowed(&'a str)` costs
  nothing, `Cow::Owned(String)` is the escape hatch when you genuinely had to build a new value. The
  rule's decision table ("usually borrow, sometimes own → yes") is the whole judgement.
- **`Deref` + `PartialEq` are what make it a drop-in** — `Cow<'_, str>` derefs to `str`, so `&cow`
  coerces to `&str` at every existing call site; it implements `Display`, so `format!("'{}'", …)`
  still works; and `impl PartialEq<&str> for Cow<'_, str>` is why the existing doctest and the three
  existing `sql_test.rs` cases need **no** edits.
- **Lifetime elision on the return type** — `fn sql_literal(s: &str) -> Cow<'_, str>` needs no named
  lifetime: one input reference, so elision rule 2 ties the output to it. (PR 9.6 makes that a lint.)
- **walrus's hand-built statement rendering** — literal escaping (double the `'`) and identifier
  quoting (double the `"`) are *different jobs*; `sql_literal` is only the first, and this PR must not
  blur that line.

## Read first

- [`own-cow-conditional`](../../.claude/skills/rust-skills/rules/own-cow-conditional.md) — take the
  `normalize_path` example (the exact shape of this refactor) and the "when to use `Cow`" table.
- `crates/common/src/sql.rs` — all 21 lines: the doc comment, the doctest at :9-13, the one-line body
  at :14-16, and the sibling-test wiring at :18-20.
- `crates/common/src/sql_test.rs` — the three existing cases (`doubles_single_quotes`,
  `leaves_clean_strings_untouched`, `empty_string_is_empty`); the two new ones join them.
- The six production call sites, so you can check each still compiles by coercion rather than by
  edit: `crates/loader/src/duck.rs:136` (the `let esc = common::sql::sql_literal;` alias used five
  times inside `configure_s3`), `:166` and `:180` (both inside `append_parquet`, which runs for every
  staged file of every Phase-A cycle), `crates/loader/src/ddl.rs:233` (the `COMMENT` literal),
  `crates/pg-sink/src/reload_export.rs:578` and `crates/pg-sink/src/preflight.rs:438` (the two
  identical `format!("'{}'", …)` wrappers).
- `docs/implementation/phase-8-cleanup/pr-8.1-sql-literal-helper.md` — why this helper exists at all,
  and its explicit "the caller supplies the surrounding quotes" contract.

## Scope

**In scope**

- Change `common::sql::sql_literal` to `pub fn sql_literal(s: &str) -> Cow<'_, str>`, returning
  `Cow::Borrowed(s)` when `s` contains no `'` and `Cow::Owned(s.replace('\'', "''"))` when it does.
- Update the doc comment to state the borrowed/owned contract; **keep** the existing doctest working
  rather than rewriting or deleting it.
- Add two cases to `crates/common/src/sql_test.rs` asserting the *variant*, not just the value:
  `matches!(sql_literal("plain"), Cow::Borrowed(_))` and `matches!(sql_literal("O'Brien"),
  Cow::Owned(_))`.
- Touch call sites **only** where the compiler demands it. All six are expected to compile unchanged
  via `Deref`/`Display`; if one does not, add the minimal `&` or `.as_ref()`, never a `.to_string()`.

**Explicitly deferred** (do *not* build these here)

- The other **200 `format!` sites** and **152 `to_string()` calls**. This PR converts one function,
  not a style.
- Any `.sql` file or the committed `.sqlx` offline cache — there is no Docker on this machine to
  regenerate the cache, so the SQL side must stay byte-identical.
- Identifier quoting (`crates/pg-sink/src/preflight.rs:442`'s `ident`, `duck.rs`'s inline
  `c.replace('"', "\"\"")`). Different rule, different function, not this PR.
- Making `Cow` a project-wide convention. One motivated site is the exercise.

## Files to create / modify

```
crates/common/src/sql.rs        # signature → Cow<'_, str>; two-arm body; doc comment updated
crates/common/src/sql_test.rs   # + two variant assertions (Borrowed vs Owned)
crates/loader/src/duck.rs       # only if a call site needs an explicit borrow (expected: no change)
crates/loader/src/ddl.rs        # "
crates/pg-sink/src/reload_export.rs  # "
crates/pg-sink/src/preflight.rs      # "
```

## Skeleton

```rust
// crates/common/src/sql.rs

use std::borrow::Cow;

/// Escape a string for interpolation as a **single-quoted SQL string literal** by doubling every
/// `'`. The caller supplies the surrounding quotes (`format!("'{}'", sql_literal(s))`) — or
/// substitutes the result into a template whose placeholder already sits inside quotes.
///
/// Returns [`Cow::Borrowed`] when there is nothing to escape (the overwhelmingly common case: LSN
/// text, S3 URIs, identifiers) and [`Cow::Owned`] only when a `'` was actually doubled.
///
/// This is literal escaping only; it is **not** identifier quoting (that doubles `"`).
///
/// ```
/// use common::sql::sql_literal;
/// assert_eq!(sql_literal("O'Brien"), "O''Brien");
/// assert_eq!(sql_literal("plain"), "plain");
/// ```
pub fn sql_literal(s: &str) -> Cow<'_, str> {
    todo!("Cow::Borrowed(s) when `s` has no '\\''; Cow::Owned(s.replace('\\'', \"''\")) otherwise")
}

#[cfg(test)]
#[path = "sql_test.rs"]
mod tests;
```

```rust
// crates/common/src/sql_test.rs — added alongside the three existing cases, which stay unchanged.

use std::borrow::Cow;

#[test]
fn clean_input_is_borrowed_not_allocated() {
    todo!("assert!(matches!(sql_literal(\"plain\"), Cow::Borrowed(_)))");
}

#[test]
fn quoted_input_is_owned() {
    todo!("assert!(matches!(sql_literal(\"O'Brien\"), Cow::Owned(_)))");
}
```

```rust
// No call site should need editing — this is the shape that proves it. Keep these as a mental
// checklist while compiling, not as new code.
//
//   crates/loader/src/duck.rs:136   let esc = common::sql::sql_literal;      // fn item, still fine
//                            :140   .replace("{region}", &esc(&s3.region))   // &Cow<str> → &str
//   crates/loader/src/duck.rs:166   let uri = common::sql::sql_literal(s3_uri);
//                            :171   self.columns_for(&uri, schema_version)?  // &Cow<str> → &str
//   crates/loader/src/duck.rs:180   format!("'{}'", common::sql::sql_literal(lsn))   // Display
//   crates/pg-sink/src/preflight.rs:438  format!("'{}'", common::sql::sql_literal(s)) // Display
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `common::sql::sql_literal` returns `Cow<'_, str>`, with an elided (not named) lifetime, and its
      doc comment states which input yields which variant.
- [ ] The PR 8.1 doctest at `crates/common/src/sql.rs` is **unchanged and still passing** — no
      `.to_string()`, `.into_owned()` or `&*` was bolted onto it to make it compile.
- [ ] `crates/common/src/sql_test.rs` gains two cases asserting the **variant**
      (`Cow::Borrowed` for clean input, `Cow::Owned` for input containing `'`); the three existing
      cases still pass untouched.
- [ ] All six production call sites still compile; any that needed an edit got a borrow, not an
      allocation. No `.to_string()` was added anywhere in the diff.
- [ ] No `.sql` file and no entry in the `.sqlx` offline cache changed.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common` (and `--workspace` stays green)

## What completed looks like

```
# --- on main today ---
$ grep -rn --include='*.rs' 'Cow' crates tests | wc -l
       0

# --- after this PR ---
$ grep -rn --include='*.rs' 'Cow' crates tests | wc -l
       7
$ grep -rn --include='*.rs' 'Cow' crates tests
crates/common/src/sql.rs:3:use std::borrow::Cow;
crates/common/src/sql.rs:12:/// Returns [`Cow::Borrowed`] when there is nothing to escape …
crates/common/src/sql.rs:24:pub fn sql_literal(s: &str) -> Cow<'_, str> {
crates/common/src/sql.rs:26:        Cow::Borrowed(s)
crates/common/src/sql.rs:28:        Cow::Owned(s.replace('\'', "''"))
crates/common/src/sql_test.rs:2:use std::borrow::Cow;
crates/common/src/sql_test.rs:…:    assert!(matches!(sql_literal("plain"), Cow::Borrowed(_)));
# => >= 4 (the `use`, the return type, and both match arms)

$ cargo test -p common
running … tests
test sql::tests::clean_input_is_borrowed_not_allocated ... ok
test sql::tests::quoted_input_is_owned ... ok
test sql::tests::doubles_single_quotes ... ok
test sql::tests::leaves_clean_strings_untouched ... ok
test sql::tests::empty_string_is_empty ... ok
   Doc-tests common
test crates/common/src/sql.rs - sql::sql_literal (line 9) ... ok
test result: ok. 0 failed
```

## Hints & gotchas

- **Do not reach for `replace` unconditionally and wrap it.** `Cow::Owned(s.replace(…))` on every
  input is strictly worse than today — you would pay the allocation *and* the enum. The whole point
  is the `if s.contains('\'')` (or `memchr`-free `find`) fast path that returns `Cow::Borrowed`.
- `impl<'a, 'b> PartialEq<&'b str> for Cow<'a, str>` is what keeps `assert_eq!(sql_literal("plain"),
  "plain")` compiling. If you find yourself editing an existing assertion, stop — you have changed
  the signature in a way the rule does not ask for.
- `matches!(x, Cow::Borrowed(_))` is the *only* way to prove the no-allocation claim; an
  `assert_eq!` on the value passes either way and tests nothing new. That is why the two new cases
  assert the variant.
- Deref coercion applies to `&Cow<str> → &str`, so `.replace("{uri}", &uri)` and
  `self.columns_for(&uri, …)` keep working. `Display for Cow<B>` (where `B: Display`) keeps
  `format!("'{}'", …)` working. Between them, expect a zero-diff call-site sweep.
- `let esc = common::sql::sql_literal;` in `configure_s3` binds a **fn item** with a late-bound
  lifetime; it survives the signature change untouched. Do not "helpfully" turn it into a closure.
- The returned `Cow` borrows from its argument, so it cannot outlive it. If you hit a borrow error at
  a call site, the fix is to bind the input to a local first — never `.into_owned()`.
- Unit tests are **Go-style sibling files** (`sql.rs` → `sql_test.rs` via `#[cfg(test)] #[path = …]
  mod tests;`), not inline `mod tests {}`. Add the two cases to the existing sibling file.
- `unwrap`/`expect` are denied in production by `[workspace.lints.clippy]`; `clippy::all` and
  `warnings` are already `deny`. `clippy.toml` re-allows unwrap/expect under `#[cfg(test)]`.
- Add no dependency: `Cow` is `std::borrow::Cow`. Anything new would have to clear `cargo deny`
  (advisories, the 8-license allow-list, bans) for no reason here.

## References

- Rule: [`own-cow-conditional`](../../.claude/skills/rust-skills/rules/own-cow-conditional.md)
- Design: `docs/implementation/phase-8-cleanup/pr-8.1-sql-literal-helper.md` — the helper's original
  contract ("the caller supplies the surrounding quotes"; literal escaping is not identifier quoting).
- Prev: [PR 9.4](./pr-9.4-own-copy-small.md) · Next: [PR 9.6](./pr-9.6-own-lifetime-elision.md) · [Phase 9](./README.md) · [Roadmap](../README.md)
