<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.3 — Reuse the allocation with `clone_from` in the additive-DDL rename fold

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Readiness:** audited · **Outcome:** change
> **Gates:** fmt,clippy,test · **Test packages:** loader

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `loader` ·
> **Est. size:** S · **Depends on:** PR 9.2 · **Unlocks:** PR 9.4

`Clone` has two halves and walrus only uses one: `clone_from` appears **0 times** in the entire tree
(crates *and* tests). `clippy::assigning_clones` — pedantic, unconfigured — finds exactly one place
where that costs something real: `crates/loader/src/ddl.rs:226`, `cur = to.clone();` inside the
`AdditiveChange::RenameTable` arm of `apply_additive`'s fold. `cur` is the "current table name"
accumulator threaded through every change in the batch; assigning a fresh `clone()` to it *drops*
its existing `String` buffer and heap-allocates a new one on every rename, when `String::clone_from`
would reuse the capacity it already has. This PR makes that one call site use the allocation-reusing
half, pins the lint at `deny`, and adds the rename-fold test `ddl_additive.rs` is currently missing.

## Why — learning objectives

- **`Clone::clone_from` is the allocation-reusing half of the trait** — `x = y.clone()` builds a new
  value and drops the old one (capacity thrown away); `x.clone_from(&y)` overwrites in place and
  keeps the buffer when it is already big enough. The derive gives you a correct-but-naive default;
  reaching for `clone_from` is how you opt into the fast one.
- **`clippy::assigning_clones` as the mechanical finder** — you do not audit assignments by eye; you
  let the lint prove the shape `lhs = rhs.clone()` and then decide whether reuse is meaningful.
- **The loader's additive-DDL applier** — `apply_additive` folds a `&[AdditiveChange]` into one SQL
  batch, and a `RenameTable` must carry the *new* name forward so every later `ALTER`/`COMMENT` in
  the same batch targets the renamed table, and the recreated user view uses the final name.

## Read first

- [`own-clone-explicit`](../../../.claude/skills/rust-skills/rules/own-clone-explicit.md) — take the
  "`clone_from` optimization" section from it (`buffer = source.clone()` vs
  `buffer.clone_from(&source)`), and the "derive vs manual `Clone`" boundary that keeps this ticket
  from turning into a hand-written-`impl` exercise.
- `crates/loader/src/ddl.rs:183-256` — `apply_additive`: `let mut cur = table.to_string();` at :191,
  the five `AdditiveChange` arms, `cur = to.clone();` at :226, and the trailing
  `if structural { sql.push_str(&user_view_sql(&cur)); }`.
- `crates/loader/tests/ddl_additive.rs:1-70` — the hermetic harness (`mem(&rel(...))` opens a
  `TableDb` on `:memory:`) plus the `columns_of` / `data_type_of` probes the four existing tests use.
- `Cargo.toml:15-21` — `[workspace.lints.clippy]`.

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

- `crates/loader/src/ddl.rs:226` — replace `cur = to.clone();` with `cur.clone_from(to);`.
- Add `assigning_clones = "deny"` to `[workspace.lints.clippy]` in `Cargo.toml`.
- Add one hermetic test to `crates/loader/tests/ddl_additive.rs` covering `RenameTable` **followed by
  another change in the same batch**, proving the fold still targets the renamed table (the existing
  four tests never exercise `RenameTable` at all, so nothing today would catch a broken `cur`).

**Explicitly deferred** (do *not* build these here)

- Hand-writing any `impl Clone`. All 72 production `Clone` impls stay **derived** — the rule's
  "custom `Clone` implementation" section is illustrative, not a work item.
- The 28 `derive(Copy)` types and the 44 `Clone`-without-`Copy` types — PR 9.4 does that audit.
- Any other `x = y.clone()` the lint does *not* flag, and any change to `apply_destructive` or the
  `diff`/`diff_additive` pair.

## Files to create / modify

```
Cargo.toml                            # + clippy::assigning_clones = "deny"
crates/loader/src/ddl.rs              # :226 — `cur = to.clone()` → `cur.clone_from(to)`
crates/loader/tests/ddl_additive.rs   # + hermetic rename-then-add-column fold test
```

## Skeleton

```toml
# Cargo.toml — [workspace.lints.clippy]
# Pedantic (outside `clippy::all`): flags `lhs = rhs.clone()` where `Clone::clone_from` would reuse
# the existing allocation. One site today (the DDL rename fold); denied so a new one can't creep in.
assigning_clones = "deny"
```

```rust
// crates/loader/src/ddl.rs — inside `apply_additive`'s `AdditiveChange::RenameTable { from, to }` arm.
// `cur: String` is the fold accumulator; `to: &String` (the arm binds by reference).
AdditiveChange::RenameTable { from, to } => {
    sql.push_str(&format!(
        "ALTER TABLE \"{cur}\" RENAME TO \"{to}\"; \
         ALTER TABLE \"{cur}_raw\" RENAME TO \"{to}_raw\"; \
         DROP VIEW IF EXISTS \"{from}_current\";"
    ));
    // Reuse `cur`'s buffer instead of dropping it for a fresh allocation.
    todo!("cur.clone_from(to)");
    structural = true;
}
```

```rust
// crates/loader/tests/ddl_additive.rs — new hermetic test.
// ---- RENAME TABLE carries the new name through the rest of the fold. ----
#[test]
fn rename_table_carries_current_name_through_the_fold() {
    let db = mem(&rel("orders", vec![col("id", 23, true)]));
    apply_additive(
        db.conn(),
        "orders",
        &[
            AdditiveChange::RenameTable {
                from: "orders".into(),
                to: "purchases".into(),
            },
            // Applied AFTER the rename → must land on `purchases`, not `orders`.
            AdditiveChange::AddColumn(col("note", 25, false)),
        ],
    )
    .unwrap();

    // Mirror + CDC log both renamed, the follow-on column landed on the new names, and the user view
    // was recreated under the final name while the stale one was dropped.
    todo!("assert columns_of(db.conn(), \"purchases\") contains \"note\"");
    todo!("assert columns_of(db.conn(), \"purchases_raw\") contains \"note\"");
    todo!("assert the `purchases_current` view exists and `orders_current` does not");
}
```

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-clone-explicit.md
focused-test = cargo test -p loader
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `crates/loader/src/ddl.rs` uses `cur.clone_from(to)` in the `RenameTable` arm; no other
      assignment in the file was rewritten.
- [ ] `[workspace.lints.clippy]` in `Cargo.toml` contains `assigning_clones = "deny"`, with a comment
      saying it is pedantic (outside `clippy::all`) and therefore needs no `priority`.
- [ ] `crates/loader/tests/ddl_additive.rs` gains a hermetic (`:memory:`, no compose, no `#[ignore]`)
      test that applies `RenameTable` **and then** a second change in the same batch, asserting the
      second change landed on the renamed table and that `<new>_current` exists while `orders_current`
      is gone.
- [ ] No `impl Clone` was hand-written; every `Clone` in `loader` is still derived.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader` (and `--workspace` stays green)

## What completed looks like

```
# --- on main today ---
$ grep -rn --include='*.rs' 'clone_from' crates tests | wc -l
       0

# --- after this PR ---
$ grep -rn --include='*.rs' 'clone_from' crates tests | wc -l
       1
$ grep -rn --include='*.rs' 'clone_from' crates tests
crates/loader/src/ddl.rs:226:                cur.clone_from(to);
# => 1 (crates/loader/src/ddl.rs), plus `clippy::assigning_clones = "deny"` in
#    [workspace.lints.clippy] and `cargo clippy --all-targets --all-features -- -D warnings` green

$ cargo test -p loader --test ddl_additive
running 6 tests
test rename_table_carries_current_name_through_the_fold ... ok
...
test result: ok. 6 passed; 0 failed; 1 ignored
```

## Hints & gotchas

- The arm binds `to: &String` (pattern-matching on `&AdditiveChange`), and `Clone::clone_from` is
  `fn clone_from(&mut self, source: &Self)` — so `cur.clone_from(to)` type-checks directly. Writing
  `cur.clone_from(&to.clone())` would defeat the entire point; writing `cur.clone_from(&to)` gives
  you a `&&String` and will not compile.
- Be honest about the win in the PR body: a rename batch is rare and the strings are short, so this
  is a *correct-idiom* change, not a measured speedup. The lint is the deliverable; the allocation
  saved is a bonus. Do not claim a benchmark you did not run — `anti-premature-optimize` and
  `perf-profile-first` are rules from this same corpus.
- `RenameTable` sets `structural = true`, which is what makes `apply_additive` append
  `user_view_sql(&cur)` at the end — with the *final* name. Your test should assert that, because it
  is the behaviour a broken `cur` would silently corrupt.
- `crates/loader/tests/ddl_additive.rs` starts with
  `#![allow(clippy::unwrap_used, clippy::expect_used)]` — an integration test, so `unwrap` in setup is
  fine there. Production code is still under the workspace `unwrap_used`/`expect_used` deny.
- Keep the new test **hermetic**: `mem(&rel(...))` uses `Connection::open_in_memory()` via
  `TableDb::open(Path::new(":memory:"))`. Do **not** mark it `#[ignore]` and do not require compose —
  the `#[ignore]` test in that file is the only one that needs Docker, and there is none locally.
- Do not touch any `.sql` file or the committed `.sqlx` offline cache. This PR renders DuckDB DDL
  through Rust `format!` only.
- `clippy::all` + `warnings` are already `deny` workspace-wide, so the new lint needs no per-crate
  opt-in — every member already declares `[lints] workspace = true`.

## References

- Rule: [`own-clone-explicit`](../../../.claude/skills/rust-skills/rules/own-clone-explicit.md)
- Design: `docs/architecture.md` § "DDL capture (schema evolution)" — the additive-change taxonomy
  `apply_additive` implements.
- Prev: [PR 9.2](./pr-9.2-own-slice-over-vec.md) · Next: [PR 9.4](./pr-9.4-own-copy-small.md) · [Roadmap](../README.md)
