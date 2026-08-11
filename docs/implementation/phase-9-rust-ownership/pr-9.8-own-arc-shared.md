<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.8 — Make every refcount bump explicit with `Arc::clone` and deny `clone_on_ref_ptr`

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Readiness:** audited · **Outcome:** change
> **Gates:** fmt,clippy,test · **Test packages:** loader,pg-sink,pg-to-arrow

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `loader`, `pg-sink`, `pg-to-arrow` ·
> **Est. size:** M · **Depends on:** PR 9.7 · **Unlocks:** PR 9.9

The pinned-toolchain `--all-targets` audit reports **55** `clone_on_ref_ptr` calls in 21 files
(56 diagnostics because one library site is compiled twice), while only 2 existing `Arc::clone(`
calls state their cost explicitly. Every audited
shared-ownership bump currently hides inside a bare `.clone()`, so at a glance you cannot tell a
1-instruction atomic increment from a deep copy of a `HashMap`. The affected handles include
`state: Arc<LoaderState>`,
`waiters: Arc<WatermarkWaiters>`, `cached: Arc<CachedRelation>`, `clock: Arc<dyn Clock>`,
`store: Arc<dyn ObjectStore>`, `semaphore: Arc<Semaphore>`, `current_reload_id: Arc<AtomicI64>` and
arrow's `SchemaRef`. After this PR the syntax says which one it is, and
`clippy::clone_on_ref_ptr` — a `restriction` lint, the same category as the `unwrap_used` /
`expect_used` pair walrus has denied since PR 7.7 — keeps it that way. **Nothing about the sharing
changes; only the call syntax does.**

## Why — learning objectives

- **`Arc::clone(&x)` is a refcount bump, not a copy** — the explicit form is a readability tool, and
  clippy `restriction` lints are how a project encodes policy the compiler has no opinion about.
- **walrus's shared handles** — the health state the axum probe task reads, the relation cache shared
  with in-flight batchers, the object-store handle, and the reload semaphore/waiter registry.

## Read first

- [`own-arc-shared`](../../../.claude/skills/rust-skills/rules/own-arc-shared.md) — take the
  `Arc::clone(&a)` idiom and the "clone the Arc once outside the loop, pass `&` inside" note. Its
  `Arc<RwLock<T>>` cache example is **not** a model for walrus (see PR 9.12 — walrus has no `RwLock`
  and wants none).
- `crates/loader/src/duck.rs:197-206` — `TableDb::columns_for`, the **only** place in the tree that
  already writes `Arc::clone`. Copy its shape, including `Arc::clone(cols)` (no `&`) when the binding
  is already a reference.
- `crates/loader/src/health.rs:20-34` + `crates/loader/src/main.rs:49,58,117` — `LoaderState::new()`
  returns `Arc<Self>`; the handle goes to the axum server task *and* to every `TableCtx`.
- `crates/pg-sink/src/reload.rs:142-149,228-260,375-450` — `ExportDeps` / `ReloadController`: the
  `Arc<WatermarkWaiters>`, `Arc<Semaphore>` and `Arc<AtomicI64>` handed to each exporter task.
  Note `sink: crate::sink::ParquetSink` is a plain `Clone` struct, **not** an `Arc`.
- `crates/pg-sink/src/consume.rs:409-420,530-540` and `crates/pg-sink/src/stream_txn.rs:99-110,
  215-250,388-425` — `Arc<dyn Clock>` and `Arc<CachedRelation>` on the batcher-creation path.
- `Cargo.toml:15-21` — the existing `restriction`-lint precedent and its comment.

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

- Add `clone_on_ref_ptr = "deny"` to `[workspace.lints.clippy]`, with a comment tying it to the
  existing `unwrap_used`/`expect_used` precedent (single restriction lints, never the whole group).
- Convert the 55 audited calls in the exact allowlist below to `Arc::clone(&x)` (or
  `Arc::clone(x)` where `x` is already a `&Arc<_>`). A different finding set is a baseline mismatch
  and blocks; it does not authorize another path.
- Test files and benches count: `--all-targets` is part of the gate, and `clippy.toml`'s
  test-only escape hatches cover `unwrap`/`expect` only, not this lint.

**Explicitly deferred** (do *not* build these here)

- `sqlx::PgPool::clone` and `CancellationToken::clone` are untouched: neither is an `Arc` at the type
  level so the lint does not fire, and both are documented cheap-handle clones.
- **No sharing is restructured.** Do not hoist a clone out of a loop, do not change a signature from
  `Arc<T>` to `&T`, do not introduce `Weak`. Behaviour must be identical.
- `crates/pg-to-arrow/src/batch.rs:145` `self.schema.fields().clone()` clones a `Fields` (a newtype
  wrapping `Arc<[FieldRef]>`), not an `Arc` — the lint does not fire and the line stays as-is.
- Converting `Arc` → `Rc` where the atomics are pointless is PR 9.9's job, and it deliberately lands
  *after* this one so the bumps it moves are already explicit.

## Files to create / modify

```
Cargo.toml                             # + clippy::clone_on_ref_ptr = "deny"
crates/loader/src/main.rs              # :58, :117 — state.clone() → Arc::clone(&state)
crates/loader/tests/ddl_destructive.rs # one Arc bump in the integration fixture
crates/pg-sink/src/main.rs             # :70 state, :145 waiters
crates/pg-sink/src/bootstrap.rs        # :70 object_store handle
crates/pg-sink/src/sink.rs             # :100 self.store.clone() (Arc<dyn ObjectStore>)
crates/pg-sink/src/consume.rs          # :532 clock, :536 cached
crates/pg-sink/src/relcache.rs         # two cached-relation Arc bumps
crates/pg-sink/src/stream_txn.rs       # :220, :246, :393, :421 clock + cached
crates/pg-sink/src/snapshot.rs         # :166 clock
crates/pg-sink/src/reload.rs           # :170 waiters, :381/:513 semaphore, :418 waiters,
                                       #   :430/:446 current_reload_id
crates/pg-sink/src/batch_test.rs       # one Arc bump in the sibling unit test
crates/pg-sink/src/reload_test.rs      # four Arc bumps in the sibling unit test
crates/pg-sink/src/snapshot_test.rs    # one Arc bump in the sibling unit test
crates/pg-sink/tests/health.rs
crates/pg-sink/tests/manifest_insert.rs
crates/pg-sink/tests/parquet_put.rs
crates/pg-sink/tests/reload_ddl.rs
crates/pg-sink/tests/reload_export.rs
crates/pg-sink/tests/reload_metrics.rs
crates/pg-sink/tests/reload_recovery.rs
crates/pg-to-arrow/src/batch.rs        # :199 self.schema.clone() (SchemaRef = Arc<Schema>)
```

## Skeleton

```rust
// crates/loader/src/main.rs:58 and :117 — `state: Arc<LoaderState>` (health.rs:32 returns Arc<Self>)
//
// before: let server = tokio::spawn(health::serve_on(listener, state.clone(), token.clone()));
// after:
let server = tokio::spawn(health::serve_on(
    listener,
    Arc::clone(&state),
    token.clone(), // CancellationToken is not an Arc — the lint does not fire; leave it
));
```

```rust
// crates/pg-sink/src/reload.rs:381 — `semaphore: Arc<Semaphore>`; `try_acquire_owned` consumes an
// owned Arc<Semaphore>, so the clone is load-bearing, not incidental.
//
// before: let permit = match self.semaphore.clone().try_acquire_owned() { … };
// after:
let permit = match Arc::clone(&self.semaphore).try_acquire_owned() {
    Ok(p) => p,
    Err(_) => todo!("unchanged — the existing at-capacity arm"),
};
```

```rust
// crates/pg-to-arrow/src/batch.rs:199 — `schema: SchemaRef` (= Arc<Schema>); the alias does not
// hide the Arc from the lint.
pub fn finish(mut self) -> Result<RecordBatch, Error> {
    // …unchanged builder drain…
    Ok(RecordBatch::try_new(Arc::clone(&self.schema), arrays)?)
}
```

```toml
# Cargo.toml — [workspace.lints.clippy], appended below expect_used
# Shared-ownership bumps must SAY they are bumps (PR 9.8, `own-arc-shared`). Like unwrap_used /
# expect_used this is a single `clippy::restriction` lint — enable the lint, never the group — so it
# is not in `clippy::all` and needs no `priority`.
clone_on_ref_ptr = "deny"
```

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-arc-shared.md
focused-test = cargo test -p loader -p pg-sink -p pg-to-arrow
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `Cargo.toml` `[workspace.lints.clippy]` contains `clone_on_ref_ptr = "deny"`, with a comment
      naming it a `restriction` lint and pointing at the `unwrap_used`/`expect_used` precedent.
- [ ] `grep -rn --include='*.rs' 'Arc::clone(' crates tests | wc -l` is `>= 22`.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` is green with **zero**
      `#[allow(clippy::clone_on_ref_ptr)]` anywhere in the tree.
- [ ] No `PgPool`/`CancellationToken` clone was rewritten, and no `Arc<T>` parameter became `&T` —
      the diff is call syntax only.
- [ ] `crates/pg-to-arrow/src/batch.rs:145` (`self.schema.fields().clone()`) is unchanged.
- [ ] The loader and sink integration suites still pass unchanged — no test assertion needed a
      rewrite, because nothing observable changed.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader -p pg-sink -p pg-to-arrow` (and `--workspace` stays green)

## What completed looks like

```
# BEFORE (on main) — 43 Arc<…> in production, but only two bumps admit to being bumps
$ grep -rn --include='*.rs' 'Arc::clone(' crates tests | wc -l
2
$ grep -rn --include='*.rs' 'Arc::clone(' crates tests
crates/loader/src/duck.rs:199:            return Ok(Arc::clone(cols));
crates/loader/src/duck.rs:204:            .insert(schema_version, Arc::clone(&cols));

# AFTER
$ grep -rn --include='*.rs' 'Arc::clone(' crates tests | wc -l
27      # >= 22

$ grep -c 'clone_on_ref_ptr' Cargo.toml
1

$ cargo clippy --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

## Hints & gotchas

- **Let clippy drive.** Add the lint line first, run
  `cargo clippy --all-targets --all-features 2>&1 | grep -A2 clone_on_ref_ptr`, and fix exactly what
  it names. Hunting `.clone()` by grep will make you "fix" `String`/`Vec` clones that are not this
  PR's business (they belong to PRs 9.1 and 9.5).
- **`&` or no `&`?** `Arc::clone` takes `&Arc<T>`. If the binding is already a reference — as at
  `duck.rs:199`, where `cols: &Arc<Vec<String>>` came out of `HashMap::get` — write
  `Arc::clone(cols)`. Adding a second `&` is the most common compile error here.
- **Struct-literal fields read oddly** (`state: Arc::clone(&state),` at `main.rs:117`) and that is the
  point: the noise is proportional to "this is another owner of shared state".
- `Semaphore::try_acquire_owned`/`acquire_owned` take `Arc<Self>` **by value**, so
  `Arc::clone(&self.semaphore).try_acquire_owned()` is the correct shape — do not try to pass `&`.
- `SchemaRef` is a type alias for `Arc<Schema>` and `arrow`'s `Fields` is a newtype around
  `Arc<[FieldRef]>`. The lint sees through the alias but not through the newtype: `batch.rs:199`
  converts, `batch.rs:145` does not.
- The lint fires in `#[cfg(test)]` code and in benches too. `clippy.toml` only relaxes
  `unwrap`/`expect` in tests — there is no equivalent knob for this lint, and you should not add one.
- `unwrap`/`expect` remain denied in production and `clippy::all` + `warnings` are already `deny`, so
  a stray unused `use std::sync::Arc;` (or a missing one) fails the build rather than warning.
- Unit tests are **sibling files** (`foo.rs` → `foo_test.rs`), never inline `mod tests {}` — if you
  touch a test, touch the sibling.
- No `.sql` file and no `.sqlx` cache entry is involved; do not regenerate anything (there is no
  Docker on this machine).

## References

- Rule: [`own-arc-shared`](../../../.claude/skills/rust-skills/rules/own-arc-shared.md)
- Design: `docs/architecture.md` §1.3 (in-memory batching — the shared relation cache and clock) and
  `docs/single-table-reload.md` H1 (the waiter registry shared between the decode loop and exporters).
- Prev: [PR 9.7](./pr-9.7-own-move-large.md) · Next: [PR 9.9](./pr-9.9-own-rc-single-thread.md) · [Roadmap](../README.md)
