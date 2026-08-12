<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.2 — Take `Option<&str>` not `&Option<String>`, and pin the borrowed-argument lints

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/130

> **Readiness:** audited · **Outcome:** change
> **Gates:** fmt,clippy,test · **Test packages:** pg-to-arrow

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `pg-to-arrow` ·
> **Est. size:** S · **Depends on:** PR 9.1 · **Unlocks:** PR 9.3

The headline form of this rule is already satisfied: `&Vec<T>` and `&String` appear **zero** times
anywhere in the tree (crates *and* tests), because `clippy::ptr_arg` catches them transitively
through the `clippy::all = "deny"` walrus has had since PR 7.7. What survives is the one member of
the family that `all` does *not* cover — `clippy::ref_option`, a pedantic lint with exactly **1
site**: `crates/pg-to-arrow/src/batch.rs:592`, `fn opt_text_value(bound: &Option<String>) ->
TupleValue`, which also happens to be the single `&Option<` in production. This PR narrows that
signature to `Option<&str>`, moves its two callers to `Option::as_deref`, and pins both `ref_option`
and (explicitly, as documentation rather than inheritance) `ptr_arg` in `[workspace.lints.clippy]`.

## Why — learning objectives

- **Deref coercion at the API boundary** — `Vec<T> → &[T]`, `String → &str`, `Box<T> → &T`,
  `Arc<T> → &T`. A parameter typed as the *borrowed* form accepts strictly more callers at no cost.
- **`Option<&T>` beats `&Option<T>`** — one indirection instead of two, and it accepts a `None` the
  caller synthesised on the spot. `Option::as_deref` is the adapter that gets you there from an
  owning `Option<String>` without cloning.
- **pg-to-arrow's range-bound append path** — a range bound has *three* distinct states: a
  whole-column SQL `NULL`, an unbounded side (`None` with `empty == false`), and a real value.
  `opt_text_value` collapses the first two onto `TupleValue::Null` on purpose; do not lose that
  distinction upstream.

## Read first

- [`own-slice-over-vec`](../../../.claude/skills/rust-skills/rules/own-slice-over-vec.md) — take the
  Deref-coercion chain and the "when to accept owned types" carve-out from it. Note that walrus is
  already conformant for `&Vec`/`&String`; this ticket is about the `Option` corner it does not name.
- `crates/pg-to-arrow/src/batch.rs:590-597` — `opt_text_value`, the one site.
- `crates/pg-to-arrow/src/batch.rs:427-450` — `append_range`, its only caller (lines **444** and
  **445**, `&opt_text_value(&r.lower)` / `&opt_text_value(&r.upper)`).
- `crates/pg-to-arrow/src/range.rs:70-95` — `ParsedRange { empty, lower: Option<String>, upper:
  Option<String>, lower_inc, upper_inc }` and the doc comment explaining why `lower_inf`/`upper_inf`
  are derived, not stored.
- `Cargo.toml:15-21` — `[workspace.lints.clippy]`, where the two lints land.

## Baseline contract

- **Precondition:** Confirm `rule-present`, then inspect the immediate predecessor's named paths and
  symbols with `rg`. Historical line coordinates in the audit are navigation hints only; the
  named symbol and stated precondition are authoritative.
- **Allowed files:** The **Files to create / modify** block is exhaustive.

- Any other current-tree mismatch blocks before editing.

## Scope

**Baseline precondition.** Before editing, reproduce the task's authored finding from its named
source paths, symbols, counts, and read-only probes; run the full **Verification commands** block
after implementation. The named sites and allowed paths are the complete task boundary.

**Baseline mismatch.** If the current tree differs from that authored finding, **STOP and request
task re-authoring before editing.** Do not choose another site, implementation, evidence conclusion,
or outcome.

**In scope**

- Change `opt_text_value` to `fn opt_text_value(bound: Option<&str>) -> TupleValue` and rewrite its
  `match` arms accordingly.
- Update the two callers in `append_range` to pass `r.lower.as_deref()` / `r.upper.as_deref()`.
- Add `ref_option = "deny"` to `[workspace.lints.clippy]`, plus an **explicit** `ptr_arg = "deny"`
  with a comment saying it is already inherited from `clippy::all` and is pinned here so the whole
  borrowed-argument family is documented in one place rather than half-inherited.

**Explicitly deferred** (do *not* build these here)

- `impl AsRef<T>` parameters anywhere — that is Phase 13's `api-impl-asref`. `Option<&str>` is a
  concrete type, not a generic one; keep it that way.
- The `&[Box<dyn ArrayBuilder>]` and `&[FieldRef]` slice parameters in `batch.rs` — they are already
  in the borrowed form the rule asks for. Leave them alone.
- Widening any *other* signature "while you're in there". The lint decides what changes, not taste.

## Files to create / modify

```
Cargo.toml                        # + clippy::ref_option = "deny"  (+ explicit ptr_arg = "deny")
crates/pg-to-arrow/src/batch.rs   # :592 signature → Option<&str>; :444/:445 callers → .as_deref()
```

## Skeleton

```toml
# Cargo.toml — [workspace.lints.clippy]
# `ptr_arg` already arrives via `all = "deny"`; pinned explicitly so the borrowed-argument family is
# stated, not inherited. `ref_option` is pedantic (outside `all`) — no `priority` juggling needed.
ptr_arg = "deny"
ref_option = "deny"
```

```rust
// crates/pg-to-arrow/src/batch.rs — currently `fn opt_text_value(bound: &Option<String>)`.

/// A range bound as a `TupleValue`: `Some(text)` → `Text`, `None` (unbounded) → `Null`
/// (→ `append_null`).
fn opt_text_value(bound: Option<&str>) -> TupleValue {
    todo!("Some(s) => TupleValue::Text(s.to_owned()), None => TupleValue::Null")
}
```

```rust
// crates/pg-to-arrow/src/batch.rs — inside `append_range`, replacing lines 444-445.
// `ParsedRange.lower`/`.upper` stay `Option<String>`; `as_deref()` hands out `Option<&str>` with no
// allocation and no clone.
append_value(builders[0].as_mut(), &fields[0], &opt_text_value(todo!("r.lower.as_deref()")))?;
append_value(builders[1].as_mut(), &fields[1], &opt_text_value(todo!("r.upper.as_deref()")))?;
```

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-slice-over-vec.md
focused-test = cargo test -p pg-to-arrow
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `opt_text_value` takes `Option<&str>`; `grep -rn --include='*.rs' --exclude='*_test.rs'
      '&Option<' crates/*/src` returns **no** hits.
- [x] Both callers in `append_range` use `.as_deref()`; no `.clone()` or `.to_string()` was added to
      compensate anywhere in `append_range`.
- [x] `[workspace.lints.clippy]` in `Cargo.toml` contains `ref_option = "deny"` and an explicit
      `ptr_arg = "deny"` with the "inherited from `all`, pinned for documentation" comment.
- [x] Range behaviour is unchanged: a whole-column NULL, an unbounded bound, and a real bound still
      produce the same three Arrow outcomes — `cargo test -p pg-to-arrow` (including the range
      conformance vectors under `crates/pg-to-arrow/tests/`) is green with no test edits.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-to-arrow` (and `--workspace` stays green)

## What completed looks like

```
# --- on main today ---
$ grep -rn --include='*.rs' --exclude='*_test.rs' '&Option<' crates/*/src | wc -l
       1
$ grep -rn --include='*.rs' --exclude='*_test.rs' '&Option<' crates/*/src
crates/pg-to-arrow/src/batch.rs:592:fn opt_text_value(bound: &Option<String>) -> TupleValue {

# --- after this PR ---
$ grep -rn --include='*.rs' --exclude='*_test.rs' '&Option<' crates/*/src | wc -l
       0
# => 0 (signature becomes `bound: Option<&str>`, callers use `.as_deref()`), plus
#    `clippy::ref_option = "deny"` and an explicit `clippy::ptr_arg = "deny"` pinned in
#    [workspace.lints.clippy] so the family is documented rather than inherited

$ cargo clippy --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

## Hints & gotchas

- `Option::as_deref` needs the receiver by reference (`r.lower.as_deref()` borrows `r.lower`), so
  `r` stays usable for `r.lower_inc` / `r.upper_inc` / `r.empty` two lines later. Do **not** reach
  for `as_ref().map(String::as_str)` — same result, more noise.
- `clippy::ref_option` is **pedantic** (outside `clippy::all`); `clippy::ptr_arg` is **style** and
  therefore already inside `all`. Both go in at `"deny"`, so listing them alongside `all = "deny"`
  needs no `priority` field. Say that in the comment — the next reader will wonder.
- Watch the semantics: `opt_text_value(None)` must keep producing `TupleValue::Null`, which
  `append_value` turns into `append_null()`. That is *deliberately* the same Arrow output as a
  whole-column SQL NULL — `range.rs`'s doc comment explains why the distinction is carried by the
  sibling `_lower_inc`/`_upper_inc`/`_empty` boolean builders, not by the bound itself.
- `TupleValue::Text` wants an owned `String`; `s.to_owned()` on a `&str` is a real conversion, not
  an implicit clone, so it will not trip the `implicit_clone` deny PR 9.1 just added.
- Unit tests here are **sibling files** (`batch_test.rs`), not inline `mod tests {}`; the range
  conformance vectors live in `crates/pg-to-arrow/tests/`. You should need to change neither.
- `unwrap`/`expect` are denied in production by `[workspace.lints.clippy]`; `clippy::all` and
  `warnings` are already `deny`. Do not introduce either while reshaping the match.
- Do not touch any `.sql` file or the committed `.sqlx` cache (no Docker locally to regenerate it),
  and add no dependency — anything new must clear `cargo deny`.

## References

- Rule: [`own-slice-over-vec`](../../../.claude/skills/rust-skills/rules/own-slice-over-vec.md)
- Design: `docs/architecture.md` § "Data type translation (Postgres → Arrow → Parquet)" — the Tier-2
  range fan-out (`_lower`, `_upper`, `_lower_inc`, `_upper_inc`, `_empty`).
- Prev: [PR 9.1](./pr-9.1-own-borrow-over-clone.md) · Next: [PR 9.3](./pr-9.3-own-clone-explicit.md) · [Roadmap](../README.md)
