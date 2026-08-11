<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.10 — Use `Cell` and `RefCell` for the per-worker latches a thread-safe `Mutex` is guarding for nothing

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Readiness:** audited · **Outcome:** change
> **Gates:** fmt,clippy,test · **Test packages:** loader

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `loader` ·
> **Est. size:** M · **Depends on:** PR 9.9 · **Unlocks:** PR 9.11

`TableCtx` is `!Send` by construction — it owns a `TableDb`, therefore a `duckdb::Connection` — and
it is moved into `spawn_local` on a `LocalSet` (`crates/loader/src/main.rs:110-133`). Yet it reaches
for `parking_lot::Mutex` **twice**, purely to get interior mutability behind `run_phase_a(&ctx)`:
`phase_a.rs:41` `pause_logged: Mutex<Option<i64>>` and `:46`
`resync_ids: Mutex<HashSet<i64>>` (three `parking_lot::Mutex` mentions in the file, counting the
`pause_began` parameter at `:53`). Both are per-table by construction — one `TableCtx` per worker,
no second thread in sight — so the lock buys nothing and costs a reader the question "who else takes
this?". `Option<i64>` is `Copy`, so it wants `Cell`; the set wants `RefCell`. The tree already has
exactly one `RefCell` (`crates/loader/src/duck.rs:40`) using precisely this argument; this PR makes
it two more, and leaves the loader with **no** mutex outside `health.rs`.

## Why — learning objectives

- **`Cell` vs `RefCell` vs `Mutex`** — no borrow flags at all, runtime borrow checking, and
  cross-thread synchronisation; picking the weakest tool that compiles is the design signal.
- **The loader's per-table latches** — the claim-pause latch (PR 6.6) that logs "why am I idle" once
  per pause, and the resync-id cache (PR 6.10) that keeps chunks 2…n a plain append.

## Read first

- [`own-refcell-interior`](../../../.claude/skills/rust-skills/rules/own-refcell-interior.md) — take
  the "mutate through `&self`" motivation and especially the **Cell for Copy types** section
  (`get`/`set`, no runtime borrow flags, cannot panic). Ignore the `Rc<RefCell<T>>` shared-handles
  pattern: `TableCtx` is owned by exactly one worker and is never shared.
- `crates/loader/src/phase_a.rs:19-68` — `TableCtx`'s field docs (they already say "per-table by
  construction … interior mutability so `run_phase_a(&ctx)` keeps its shared-ref signature") and
  `pause_began`, the only function that touches the latch.
- `crates/loader/src/phase_a.rs:238-275` — `route_reload_file`, where `resync_ids` is read (`:248`)
  and written (`:270`) inside an **async** fn.
- `crates/loader/src/main.rs:104-140` — `LocalSet` + `spawn_local`, and the `TableCtx` literal at
  `:110-124` whose `Default::default()` initialisers keep working unchanged.
- `crates/loader/src/duck.rs:36-51` — the in-tree precedent: a `RefCell` field whose doc comment
  makes the `!Send`-single-threaded argument you are about to reuse.

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

- `pub pause_logged: Cell<Option<i64>>` and `pub resync_ids: RefCell<HashSet<i64>>` on `TableCtx`,
  with the field docs extended to say *why* `Cell`/`RefCell` and not `Mutex`.
- `pause_began(logged: &Cell<Option<i64>>, live: Option<i64>) -> Option<i64>` — same semantics,
  rewritten around `get`/`set` (the `match` on `(*slot, live)` becomes a `match` on
  `(logged.get(), live)`).
- `route_reload_file`: `ctx.resync_ids.borrow().contains(&id)` / `ctx.resync_ids.borrow_mut()
  .insert(id)`.
- The four integration-test assertions that read the latch through the lock
  (`crates/loader/tests/phase_a.rs:290,297,316` and `crates/loader/tests/reload_resync.rs:337`)
  become `ctx.pause_logged.get()`.

**Explicitly deferred** (do *not* build these here)

- The two genuinely cross-thread mutexes stay: `crates/loader/src/health.rs:28`
  (`last_poll_completed_at`, written by every apply worker and read by the axum probe task) and
  `crates/pg-sink/src/reload_signal.rs:45` (`waiters`, shared between the decode loop and exporter
  tasks). The latter is PR 9.11's subject.
- `parking_lot` stays the workspace's cross-thread mutex (PR 7.6) and stays a direct dependency —
  `health.rs` still uses it.
- `run_phase_a(&ctx)` keeps its `&TableCtx` signature; the per-table worker model does not change;
  no field becomes `&mut`.
- No `Rc<RefCell<_>>` sharing is introduced — `TableCtx` has exactly one owner.

## Files to create / modify

```
crates/loader/src/phase_a.rs           # :41 Cell, :46 RefCell, :53 pause_began signature + body,
                                       #   :248/:270 borrow()/borrow_mut(); + use std::cell::{Cell, RefCell}
crates/loader/tests/phase_a.rs         # :290, :297, :316 — *ctx.pause_logged.lock() → .get()
crates/loader/tests/reload_resync.rs   # :337 — same
```

`crates/loader/src/main.rs:122-123` and the other five `TableCtx` literals in `crates/loader/tests/`
need **no** edit: they all initialise these fields with `Default::default()`, and `Cell`/`RefCell`
are `Default` whenever their contents are.

## Skeleton

```rust
// crates/loader/src/phase_a.rs — imports + the two fields
use std::cell::{Cell, RefCell};
use std::collections::HashSet;

pub struct TableCtx {
    // …pool / epoch / schema / table / rel / db / state / max_files / intervals unchanged…
    /// The reload_id whose claim pause was already logged (PR 6.6) — a paused table says *why* it
    /// is idle once per pause, not once per poll. Per-table by construction (one `TableCtx` per
    /// worker, `!Send` because it owns a `duckdb::Connection`), so this needs interior mutability
    /// behind `run_phase_a(&ctx)` — **not** synchronisation. `Option<i64>` is `Copy`, so `Cell`:
    /// no borrow flags, no runtime panic path.
    pub pause_logged: Cell<Option<i64>>,
    /// reload_ids already identified as `resync` (PR 6.10). …existing rationale kept… `RefCell`
    /// rather than `Cell` only because `HashSet` is not `Copy`; same single-worker argument.
    pub resync_ids: RefCell<HashSet<i64>>,
}
```

```rust
// crates/loader/src/phase_a.rs:52 — same three arms, no guard
pub(crate) fn pause_began(logged: &Cell<Option<i64>>, live: Option<i64>) -> Option<i64> {
    match (logged.get(), live) {
        (prev, Some(id)) if prev != Some(id) => {
            logged.set(Some(id));
            Some(id)
        }
        (_, None) => {
            logged.set(None);
            None
        }
        _ => None,
    }
}
```

```rust
// crates/loader/src/phase_a.rs:248 and :270 — inside the async `route_reload_file`
async fn route_reload_file(ctx: &TableCtx, f: &control::ManifestRow) -> Result<bool, LoaderError> {
    // …file_reload_id extraction unchanged…
    // Borrow lives only for the `if` condition — never across the awaits below.
    if ctx.resync_ids.borrow().contains(&file_reload_id) {
        return Ok(true);
    }
    // …recorded / </>/== arms unchanged, including the `control::reload::get(…).await?` …
    if row.flavor == control::ReloadFlavor::Resync {
        ctx.resync_ids.borrow_mut().insert(file_reload_id);
        return Ok(true);
    }
    todo!("the rebuild arm — unchanged")
}
```

```rust
// crates/loader/tests/phase_a.rs:290 (and :297, :316; reload_resync.rs:337)
assert_eq!(
    ctx.pause_logged.get(),
    Some(reload_id),
    "the pause is latched (logged once)"
);
```

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-refcell-interior.md
focused-test = cargo test -p loader
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `grep -c 'parking_lot::Mutex' crates/loader/src/phase_a.rs` is `0`; the file uses
      `Cell<Option<i64>>` and `RefCell<HashSet<i64>>`.
- [ ] `pause_began` takes `&Cell<Option<i64>>` and keeps all three arms and their exact semantics:
      a new reload latches and returns `Some(id)`, the same reload returns `None`, `None` clears.
- [ ] `route_reload_file` uses `borrow()` / `borrow_mut()`, and **no** `Ref`/`RefMut` is alive across
      an `.await` (each borrow is confined to one condition or one statement).
- [ ] The four test assertions read `ctx.pause_logged.get()`; no test needed a semantic change.
- [ ] `crates/loader/src/main.rs` is untouched — `Default::default()` still initialises both fields.
- [ ] `parking_lot` remains a `loader` dependency (still used by `health.rs`) and
      `crates/loader/src/health.rs:28` is unchanged.
- [ ] `cargo test -p loader` passes, including the pause-latch assertions in
      `crates/loader/tests/phase_a.rs` and `reload_resync.rs`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader` (and `--workspace` stays green)

## What completed looks like

```
# BEFORE (on main) — a thread-safe lock protecting state only one thread can reach
$ grep -c 'parking_lot::Mutex' crates/loader/src/phase_a.rs
3      # lines 41, 46, 53
$ grep -rn --include='*.rs' 'RefCell' crates tests | wc -l
4      # all four in crates/loader/src/duck.rs

# AFTER
$ grep -c 'parking_lot::Mutex' crates/loader/src/phase_a.rs
0
$ grep -rn --include='*.rs' 'RefCell' crates tests | wc -l
7      # >= 6: duck.rs's four, plus the import, field and two borrows in phase_a.rs

$ cargo test -p loader --test phase_a
test claims_pause_while_a_rebuild_is_live … ok
test result: ok. 9 passed; 0 failed
```

## Hints & gotchas

- **`Cell` first, `RefCell` only when forced.** `Cell::get`/`set` need `T: Copy` and have no failure
  mode at all; `RefCell` adds a runtime borrow flag and a **panic** path. `Option<i64>` is `Copy`, so
  the latch must be a `Cell` — using `RefCell` there would be strictly worse.
- **`RefCell::borrow()` panics on an overlapping borrow, and this is production code with a zero-panic
  policy.** Keep every borrow inside a single expression or statement, exactly as
  `TableDb::columns_for` does (`duck.rs:198,203`). In particular, never write
  `let ids = ctx.resync_ids.borrow(); … .await`. A borrow held across an `.await` is the one shape
  that can actually deadlock a re-entered worker.
- `if ctx.resync_ids.borrow().contains(&id) { … }` is safe: a temporary in an `if` **condition** is
  dropped before the block runs. The equivalent inside `match`'s scrutinee is *not* — that is
  precisely PR 9.11's bug in `pg-sink`.
- After this change `TableCtx` becomes `!Sync` as well as `!Send`. Nothing requires either today
  (`run_phase_a(&ctx)` is only awaited on the `LocalSet`), but if `cargo test -p loader` reports
  "`RefCell<HashSet<i64>>` cannot be shared between threads safely", find the `tokio::spawn` that
  captured `&ctx` and make it `spawn_local`. Do **not** revert to `Mutex` to silence it.
- `clippy::mut_mutex_lock` and friends will not help you here; the mechanical probe is the
  `parking_lot::Mutex` count in `phase_a.rs`, so quote it in the PR body.
- Unit tests are **sibling files**; the assertions you are editing live in the crate's
  `tests/` integration suite (`crates/loader/tests/`), which is the right place for them — do not
  move them.
- `cargo test -p loader` builds bundled DuckDB (~20 min cold, ~3 min cached) and several loader
  integration tests need `docker compose up --wait` for control-pg. Run the compose stack once and
  keep it up.
- No `.sql` file and no `.sqlx` cache entry changes here.

## References

- Rule: [`own-refcell-interior`](../../../.claude/skills/rust-skills/rules/own-refcell-interior.md)
- Design: `docs/single-table-reload.md` H8 (watermarks + the frozen frontier a live rebuild
  imposes — what the pause latch reports) and H3 (refresh vs rebuild — the `resync` flavor the id
  cache at `phase_a.rs:46` memoises).
- Prev: [PR 9.9](./pr-9.9-own-rc-single-thread.md) · Next: [PR 9.11](./pr-9.11-own-mutex-interior.md) · [Roadmap](../README.md)
