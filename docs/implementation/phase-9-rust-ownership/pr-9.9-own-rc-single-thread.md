<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.9 — Swap the loader's Parquet-column cache from `Arc<Vec<String>>` to `Rc<[String]>`

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Readiness:** audited · **Outcome:** change
> **Gates:** fmt,clippy,test · **Test packages:** loader

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `loader` ·
> **Est. size:** M · **Depends on:** PR 9.8 · **Unlocks:** PR 9.10

`Rc` appears **0** times in the tree — and yet the loader owns a documented single-threaded island.
`crates/loader/src/duck.rs:40` holds `parquet_cols: RefCell<HashMap<i64, Arc<Vec<String>>>>`, and the
field's own doc comment (`:38-39`) already states the reason it can: `TableDb` is used
single-threaded because `duckdb::Connection` is `!Send`, one per apply worker on a `LocalSet` —
confirmed at `crates/loader/src/main.rs:106` (`LocalSet::new()`) and `:132` (`local.spawn_local(…)`).
So the two `Arc::clone` calls at `:199`/`:204` pay an atomic read-modify-write on every Phase-A cache
hit that **no thread can ever observe**, and `Arc<Vec<String>>` is additionally the
`clippy::rc_buffer` shape: a pointer to a pointer to the data where `Rc<[String]>` is one hop. After
this PR, `duck.rs` contains no `Arc` at all, and the type system — not a comment — states that the
cache is thread-local.

## Why — learning objectives

- **`Rc` vs `Arc`** — non-atomic vs atomic refcounting, `!Send`/`!Sync` as a *compiler-enforced
  design constraint* rather than a limitation, and why `Rc<[T]>` beats `Rc<Vec<T>>`.
- **The loader's concurrency model** — one worker per `.duckdb` file on a `LocalSet`, and the PR 5.8
  per-`schema_version` Parquet column cache that makes an N-file cycle do one `DESCRIBE`, not N.

## Read first

- [`own-rc-single-thread`](../../../.claude/skills/rust-skills/rules/own-rc-single-thread.md) — take the
  Arc-vs-Rc decision table and the "prefer `Rc::clone(&x)` to `x.clone()`" key point. Skip the
  `Weak`/cycle-breaking section: this cache is a flat map with no back-references and PR 9.9
  introduces no cycles.
- `crates/loader/src/duck.rs:31-53` — `TableDb`, `TableDb::open`, and the field doc that already
  argues single-threadedness.
- `crates/loader/src/duck.rs:159-206` — `append_parquet` (the caller) and `columns_for` / the private
  `parquet_columns` introspection it memoises.
- `crates/loader/src/main.rs:104-140` — `LocalSet::new()` + `local.spawn_local(…)`: the reason
  `!Send` state is legal here. Note `tokio::spawn` is used elsewhere in the crate (`lease.rs:40`,
  `compaction.rs:73`) — neither captures a `TableDb`.
- `crates/loader/src/duck_test.rs` — `cached_schema_versions()` and the assertion that two v1 files
  produce exactly one cached introspection. That test is the behavioural contract to keep green.

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

- `parquet_cols: RefCell<HashMap<i64, Rc<[String]>>>` and
  `fn columns_for(&self, uri: &str, schema_version: i64) -> Result<Rc<[String]>, LoaderError>`.
- `Arc::clone` → `Rc::clone` at both sites; `Vec<String>` → `Rc<[String]>` via `.into()` on the
  freshly introspected list.
- Delete `use std::sync::Arc;` (`duck.rs:16`) and add `use std::rc::Rc;`.
- Refresh the field doc comment so it explains **`Rc`** (single-threaded refcount, no atomics) as
  well as `RefCell` (interior mutability behind `&self`) — the comment already makes the `!Send`
  argument; extend it, don't rewrite it.
- Add `rc_buffer = "deny"` to `[workspace.lints.clippy]` so `Rc<Vec<_>>` / `Arc<String>` shapes
  cannot come back.

**Explicitly deferred** (do *not* build these here)

- `state: Arc<LoaderState>` stays `Arc`: it is genuinely shared with the axum health task on the
  tokio runtime pool (`main.rs:58`), so `Arc` is required. PR 9.8 already made those bumps explicit.
- No `Weak`, no cycle-breaking, no `Rc<RefCell<T>>` sharing pattern — the cache is owned by exactly
  one `TableDb`.
- No crate outside `loader` changes; `pg-sink`'s `Arc<dyn Clock>` / `Arc<CachedRelation>` are real
  cross-task handles and are out of scope.
- Do not change `TableDb`'s public method signatures other than the private `columns_for` return
  type, and do not touch the DuckDB SQL templates under `crates/loader/sql/`.

## Files to create / modify

```
Cargo.toml                        # + clippy::rc_buffer = "deny"
crates/loader/src/duck.rs         # use std::rc::Rc (drop std::sync::Arc); field type,
                                  #   columns_for signature + both refcount bumps, field doc
```

## Skeleton

```rust
// crates/loader/src/duck.rs — imports
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc; // replaces `use std::sync::Arc;` (line 16 today)

/// Owns one table's `.duckdb` connection (mirror `<table>` + CDC log `<table>_raw`).
pub struct TableDb {
    conn: duckdb::Connection,
    /// Parquet column lists by `schema_version` (PR 5.8). …existing rationale kept…
    ///
    /// `RefCell` for interior mutability behind `&self`; **`Rc`, not `Arc`**, because `TableDb` is
    /// single-threaded by construction — `duckdb::Connection` is `!Send`, one `TableDb` per apply
    /// worker on a `LocalSet` (`main.rs:106`/`:132`). `Rc<[String]>` rather than `Rc<Vec<String>>`
    /// keeps the read one indirection instead of two (`clippy::rc_buffer`).
    parquet_cols: RefCell<HashMap<i64, Rc<[String]>>>,
}
```

```rust
// crates/loader/src/duck.rs:197 — the whole point of the PR, in five lines
impl TableDb {
    /// The Parquet column list for `schema_version`, introspecting `uri` **once** per version and
    /// caching it (PR 5.8; sound by the homogeneous-file rule — see [`TableDb::parquet_cols`]).
    fn columns_for(&self, uri: &str, schema_version: i64) -> Result<Rc<[String]>, LoaderError> {
        if let Some(cols) = self.parquet_cols.borrow().get(&schema_version) {
            return Ok(Rc::clone(cols));
        }
        let cols: Rc<[String]> = self.parquet_columns(uri)?.into();
        self.parquet_cols
            .borrow_mut()
            .insert(schema_version, Rc::clone(&cols));
        Ok(cols)
    }

    /// Unchanged: the `DESCRIBE`-based introspection this memoises.
    fn parquet_columns(&self, uri: &str) -> Result<Vec<String>, LoaderError> {
        todo!("unchanged")
    }
}
```

```rust
// crates/loader/src/duck.rs:173 — the caller needs NO edit: Rc<[String]> derefs to [String].
let file_cols = self.columns_for(&uri, schema_version)?;
let quoted = file_cols
    .iter()
    .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
    .collect::<Vec<_>>()
    .join(", ");
```

```toml
# Cargo.toml — [workspace.lints.clippy], appended below expect_used
# No refcounted pointer to a growable buffer (PR 9.9, `own-rc-single-thread`): `Rc<Vec<T>>` /
# `Arc<String>` are two indirections where `Rc<[T]>` / `Arc<str>` are one, and the inner buffer can
# never be mutated through the shared pointer anyway. A single `clippy::restriction` lint, like
# unwrap_used/expect_used — not in `clippy::all`, so no `priority` needed.
rc_buffer = "deny"
```

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-rc-single-thread.md
focused-test = cargo test -p loader
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `crates/loader/src/duck.rs` contains **zero** occurrences of `Arc` (the import at `:16` is
      gone), and `Rc` appears in the field type, the `columns_for` return type, and both bumps.
- [ ] The cache value type is `Rc<[String]>`, not `Rc<Vec<String>>`, built with `.into()` from the
      `Vec<String>` that `parquet_columns` returns.
- [ ] Both refcount bumps are written `Rc::clone(…)`, never `.clone()` (this is also what
      `clone_on_ref_ptr`, denied in PR 9.8, now requires).
- [ ] The `parquet_cols` doc comment explains the `Rc`-not-`Arc` choice and cites the `!Send`
      `LocalSet` model, so the next reader does not "fix" it back to `Arc`.
- [ ] `Cargo.toml` `[workspace.lints.clippy]` contains `rc_buffer = "deny"` with a comment.
- [ ] `append_parquet` and every other caller compile **unchanged** — the deref to `[String]` covers
      them; if a call site needed editing, the return type is wrong.
- [ ] `cargo test -p loader` passes, including `cached_schema_versions` in
      `crates/loader/src/duck_test.rs` (two same-version files ⇒ one cached introspection).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader` (and `--workspace` stays green)

## What completed looks like

```
# BEFORE (on main) — no Rc anywhere; the loader's thread-local cache pays for atomics
$ grep -rnE --include='*.rs' '(^|[^A-Za-z_])Rc<' crates tests | wc -l
0
$ grep -c 'Arc' crates/loader/src/duck.rs
6      # lines 16, 40, 197, 199, 201, 204

# AFTER
$ grep -rnE --include='*.rs' '(^|[^A-Za-z_])Rc<' crates tests | wc -l
3      # >= 3: field type, columns_for return type, the Rc::clone sites
$ grep -c 'Arc' crates/loader/src/duck.rs
0

$ grep -c 'rc_buffer' Cargo.toml
1

$ cargo test -p loader duck
running 6 tests
test duck::tests::cached_schema_versions … ok
test result: ok. 6 passed; 0 failed
```

## Hints & gotchas

- **`Vec<String> → Rc<[String]>` via `.into()`** copies the `String` handles into a fresh allocation
  **once per `schema_version`** — that one-time cost is what buys a single-indirection read and no
  atomic RMW on every subsequent cache hit. Do not try to avoid it with `Rc::new(vec)`; that is
  exactly the `rc_buffer` shape you are removing.
- **This compiles only because the island is real.** `Rc` is `!Send` + `!Sync`, so the moment someone
  swaps `local.spawn_local(…)` (`main.rs:132`) for `tokio::spawn`, the compiler stops them. That is
  the feature. If you get "`Rc<[String]>` cannot be sent between threads safely", do not reach for
  `Arc` — find the `spawn` that should have been `spawn_local`.
- `TableDb` was **already** `!Send` (it owns `duckdb::Connection`), so this adds no new constraint;
  `cargo test -p loader` should not need a single test rewritten.
- `Rc<[String]>` derefs to `[String]`, so `.iter()`, `.len()`, indexing and slicing all work at the
  call sites unchanged. If you find yourself writing `(*cols).clone()` or `cols.to_vec()`, stop —
  you have reintroduced the copy the cache exists to avoid.
- `clippy::rc_buffer` is a `restriction` lint: enable the **single** lint, never
  `clippy::restriction` as a group. Same reasoning as `unwrap_used`/`expect_used` at `Cargo.toml:17`.
- Leaving `use std::sync::Arc;` behind is not merely untidy — `warnings = "deny"` turns the resulting
  `unused_imports` into a build failure.
- `cargo test -p loader` compiles bundled DuckDB from source: cold builds take ~20 minutes, cached
  ~3. Budget for it; it is not hung.
- Do not touch `crates/loader/sql/duckdb/templates/*.sql` or the `.sqlx` offline cache — there is no
  Docker on this machine to regenerate the cache, and this change is Rust-side only.

## References

- Rule: [`own-rc-single-thread`](../../../.claude/skills/rust-skills/rules/own-rc-single-thread.md)
- Design: `docs/walrus-loader.md` §8.1 (one read-write `.duckdb` connection per table — the
  single-writer fence that makes the one-worker-per-file model, and this cache, single-threaded).
- Prev: [PR 9.8](./pr-9.8-own-arc-shared.md) · Next: [PR 9.10](./pr-9.10-own-refcell-interior.md) · [Roadmap](../README.md)
