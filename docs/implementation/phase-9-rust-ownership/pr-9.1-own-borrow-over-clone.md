<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.1 — Delete the redundant and implicit clones the borrow checker never needed

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/128

> **Readiness:** audited · **Outcome:** change
> **Gates:** fmt,clippy,test · **Test packages:** loader,pg-sink,pg-to-arrow

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `loader`, `pg-to-arrow`, `pg-sink` ·
> **Est. size:** S · **Depends on:** PR 8.5 · **Unlocks:** PR 9.2

Production makes **168 `.clone()` calls and 152 `to_string()` calls** (`crates/*/src`, `*_test.rs`
excluded). Almost all of them are load-bearing — you genuinely need owned data to put in a `Vec`, a
`String` to hand to `format!`, an `Arc` bump to share a handle. This PR does *not* audit them by
hand. It turns on the two clippy lints that can prove waste mechanically and fixes the **exactly 4
sites** they find: `clippy::redundant_clone` ×2 (`crates/loader/src/duck.rs:118` and
`crates/pg-sink/src/snapshot_test.rs:91`) and `clippy::implicit_clone` ×2
(`crates/pg-to-arrow/src/batch.rs:356` and `:490`, both `.to_string()` on the `&String` that
arrow-rs's `Field::name()` hands back). Neither lint is configured anywhere in `Cargo.toml` or
`clippy.toml` today, so after this PR the compiler — not a reviewer's eye — keeps the count at zero.

## Why — learning objectives

- **`clippy::redundant_clone` vs `clippy::implicit_clone`** — the first proves a *move* would have
  sufficed (the original is never read again); the second proves a `Deref`-driven `.to_string()` on a
  `&String` is a silent deep copy where `.clone()` (or a `&str` binding) is the honest spelling.
- **`Deref` coercion as a footgun** — `&String` derefs to `str`, so `to_string()` resolves to
  `ToString for str` and allocates, even though the receiver was already an owned `String`.
- **The two hot renderers you are editing** — the loader's DuckDB DDL renderer
  (`TableDb::ensure_tables_planned`, which builds the `<table>_raw` composite PK list) and
  pg-to-arrow's per-value `append_value` path, which runs once per column per row.

## Read first

- [`own-borrow-over-clone`](../../../.claude/skills/rust-skills/rules/own-borrow-over-clone.md) — take
  the "when clone is acceptable" list from it: storing owned data, crossing a thread boundary, and
  `Copy` types are all fine. This PR only removes clones that are none of those three.
- `crates/loader/src/duck.rs:77-124` — `ensure_tables_planned`. `keys` is built at :77, last *read*
  at :98 (`keys.join(", ")` inside the `primary_key` branch), then cloned at :118 into `raw_pk`.
- `crates/pg-to-arrow/src/batch.rs:250-360` — the `downcast!` macro and `append_value`, whose
  `let col = field.name();` binds a `&String`; and `:483-492` — `multirange_elem_type`, whose error
  arm reads `column: field.name().to_string()`.
- `crates/pg-sink/src/snapshot_test.rs:55-95` — `meta` is built, asserted on, then passed to
  `b.push(meta.clone(), …)` at :91 and never read again.
- `Cargo.toml:10-21` — `[workspace.lints.rust]` / `[workspace.lints.clippy]`, and the comment at
  :17-19 explaining why lints outside `clippy::all` need no `priority` juggling.

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

- Add `redundant_clone = "deny"` and `implicit_clone = "deny"` to `[workspace.lints.clippy]` in
  `Cargo.toml`, with a one-line comment recording that neither is in `clippy::all` (nursery and
  pedantic respectively), so no `priority` is needed.
- `crates/loader/src/duck.rs:118` — move `keys` into `raw_pk` instead of cloning it.
- `crates/pg-to-arrow/src/batch.rs` — bind `col` as a `&str` in `append_value` (which also fixes the
  `downcast!` expansions inside it) and use `.clone()` in `multirange_elem_type:490`.
- `crates/pg-sink/src/snapshot_test.rs:91` — pass `meta` by move.

**Explicitly deferred** (do *not* build these here)

- The other **164** production clones. The `Arc` refcount bumps are PR 9.8's job (`Arc::clone` +
  `clone_on_ref_ptr`), the conditional `String` allocations are PR 9.5's (`Cow`), and the rest are
  genuinely-needed owned data — do not "optimise" them by eye.
- Any function-signature change. Widening a parameter from `&String` to `&str` is PR 9.2.
- `clippy::assigning_clones` / `clone_from` — that is PR 9.3.

## Files to create / modify

```
Cargo.toml                            # + clippy::redundant_clone / implicit_clone = "deny"
crates/loader/src/duck.rs             # :118 — `keys.clone()` → move `keys` into `raw_pk`
crates/pg-to-arrow/src/batch.rs       # :267 — bind `col` as `&str`; :490 — `.to_string()` → `.clone()`
crates/pg-sink/src/snapshot_test.rs   # :91 — `meta.clone()` → `meta` (its last use)
```

## Skeleton

```toml
# Cargo.toml — [workspace.lints.clippy], alongside the existing `all` / `unwrap_used` / `expect_used`.
# `redundant_clone` is nursery and `implicit_clone` is pedantic — neither is in `clippy::all`, so (as
# with unwrap_used/expect_used above) there is no group-vs-lint `priority` to juggle.
redundant_clone = "deny"
implicit_clone = "deny"
```

```rust
// crates/loader/src/duck.rs — inside `ensure_tables_planned`, replacing line 118.
// `keys` is last read at :98; the composite raw PK is "the mirror keys, plus the two walrus columns",
// so `raw_pk` can simply take ownership.
let mut raw_pk = todo!("move `keys` here instead of cloning it");
raw_pk.push("\"_walrus_sink_processed_at\"".into());
raw_pk.push("\"_walrus_lsn\"".into());
```

```rust
// crates/pg-to-arrow/src/batch.rs — `append_value`, currently `let col = field.name();`
fn append_value(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    value: &TupleValue,
) -> Result<(), Error> {
    // arrow-rs `Field::name()` returns `&String`. Ascribing `&str` makes the deref explicit once,
    // so every downstream `col.to_string()` (including the ones the `downcast!` macro expands to)
    // is a real &str → String conversion rather than an implicit deep clone.
    let col: &str = todo!("field.name()");
    // ... unchanged ...
}

// crates/pg-to-arrow/src/batch.rs:483 — `multirange_elem_type`'s error arm.
fn multirange_elem_type(field: &Field) -> Result<DataType, Error> {
    // ... unchanged List/Struct walk ...
    Err(Error::Downcast {
        column: todo!("field.name() is already an owned String behind a reference — clone it"),
    })
}
```

```rust
// crates/pg-sink/src/snapshot_test.rs — inside the snapshot-batcher test, replacing line 91.
b.push(
    todo!("`meta` is never read after this call — move it"),
    &[TupleValue::Text("1".into()), TupleValue::Text("new".into())],
);
```

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-borrow-over-clone.md
focused-test = cargo test -p loader -p pg-sink -p pg-to-arrow
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `[workspace.lints.clippy]` in `Cargo.toml` contains `redundant_clone = "deny"` **and**
      `implicit_clone = "deny"`, each explained by a comment in the style of the existing
      `unwrap_used`/`expect_used` note.
- [x] `crates/loader/src/duck.rs` no longer clones `keys`; `cargo test -p loader --test ddl_additive`
      is green, proving the rendered `{raw_pk}` list is byte-identical.
- [x] `crates/pg-to-arrow/src/batch.rs` calls `.to_string()` on no `&String`: `append_value` binds
      `col: &str`, and `multirange_elem_type` uses `.clone()`.
- [x] `crates/pg-sink/src/snapshot_test.rs:91` passes `meta` by move.
- [x] `cargo clippy --all-targets --all-features -- -D warnings` reports **zero**
      `redundant_clone` / `implicit_clone` diagnostics (4 sites → 0).
- [x] No function signature, no `.sql` file, and no `.sqlx` cache entry changed.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p loader` (and `--workspace` stays green)

## What completed looks like

```
# --- on main today ---
$ grep -cE 'redundant_clone|implicit_clone' Cargo.toml
0
# => 0 (both lints unconfigured; the pedantic+nursery sweep reports 4 sites:
#       loader 1, pg-to-arrow 2, pg-sink 1)

# --- after this PR ---
$ grep -cE 'redundant_clone|implicit_clone' Cargo.toml
2
# => 2 (both = "deny" in [workspace.lints.clippy])

$ cargo clippy --all-targets --all-features -- -D warnings
    Checking common v0.1.0 …
    Checking pg-to-arrow v0.1.0 …
    Checking loader v0.1.0 …
    Checking pg-sink v0.1.0 …
    Finished `dev` profile [unoptimized + debuginfo] target(s)
# => green, i.e. 4 sites -> 0
```

## Hints & gotchas

- `clippy::redundant_clone` lives in **nursery** precisely because it has had false positives
  historically. Denying it is still the right call here (4 real hits, 0 noise today) — but if it ever
  fires on something that genuinely needs the clone, the fix is an item-level `#[allow(...)]` with a
  one-line reason, never deleting a clone the borrow checker actually wants.
- `clippy::implicit_clone` is **pedantic**. Neither lint is a member of `clippy::all`, so you do
  **not** need `priority = -1` on `all` — the same reasoning the `unwrap_used`/`expect_used` comment
  at `Cargo.toml:17-19` already records. Keep that comment style.
- arrow-rs's `Field::name()` returns `&String`, not `&str`. That single fact is the whole
  `implicit_clone` story here; `append_range`/`append_geometric` bind `col` the same way, so if
  clippy flags them too, apply the same `&str` ascription rather than sprinkling `.clone()`.
- `snapshot_test.rs` is a **Go-style sibling test file** (`#[cfg(test)] #[path = …] mod tests;`), not
  an inline `mod tests {}`. `clippy.toml` re-allows `unwrap`/`expect` there, but the clone lints still
  apply because `--all-targets` compiles it.
- `clippy::all` and `warnings` are already `deny` at the workspace level, and every member declares
  `[lints] workspace = true` — so a new lint added to `Cargo.toml` takes effect everywhere with no
  per-crate edit.
- Do not touch any `.sql` file or the committed `.sqlx` offline cache — there is no Docker on this
  machine to regenerate it.
- No new dependency is needed. If you reach for one, it must clear `cargo deny` (advisories, the
  8-license allow-list, bans) — a bad trade for a 4-line fix.

## References

- Rule: [`own-borrow-over-clone`](../../../.claude/skills/rust-skills/rules/own-borrow-over-clone.md)
- Design: `docs/architecture.md` § "Data type translation (Postgres → Arrow → Parquet)" — the
  per-value append path this PR touches.
- Prev: [PR 8.5](../phase-8-cleanup/pr-8.5-nits-cluster.md) *(phase boundary → Phase 9 Rust ownership & borrowing)* · Next: [PR 9.2](./pr-9.2-own-slice-over-vec.md) · [Roadmap](../README.md)
