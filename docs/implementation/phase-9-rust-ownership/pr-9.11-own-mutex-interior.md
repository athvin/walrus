<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.11 — Drop the mutex guard before the match body in the reload watermark registry

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Readiness:** audited · **Outcome:** change
> **Gates:** fmt,clippy,test · **Test packages:** pg-sink

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `pg-sink` ·
> **Est. size:** S · **Depends on:** PR 9.10 · **Unlocks:** PR 9.12

After PR 9.10 exactly **two** `Mutex`es remain in the tree, and both are genuinely cross-thread. One
of them has a real defect. `crates/pg-sink/src/reload_signal.rs:80` writes
`match self.waiters.lock().remove(&(reload_id, chunk_no))`, and a temporary in a `match` **scrutinee**
lives until the end of the whole `match` — so the `parking_lot` guard is held across the entire body
(`:80-98`): two `tracing::` macro expansions **and** a `oneshot::Sender::send`. This runs on the
sink's decode hot path, and while it is held every exporter task's `subscribe` blocks on the same
lock. `clippy::significant_drop_in_scrutinee` reports it — exactly 1 site, span ending at `:98` —
and neither that lint nor `clippy::significant_drop_tightening` is configured anywhere. This PR binds
the removal to a local so the guard drops at the end of that statement, and denies both lints so the
shape cannot come back.

## Why — learning objectives

- **Lock scope and guard drop points** — why a `match` scrutinee extends a temporary's lifetime to
  the end of the `match`, and why `parking_lot`'s non-poisoning `.lock()` (returning the guard
  directly, no `Result`) is the walrus choice under the PR 7.7 `unwrap_used`/`expect_used` deny.
- **The reload echo registry** — the chunk-watermark handoff shared between the decode loop (which
  resolves) and the exporter tasks (which subscribe), PRs 6.3 / 6.5.

## Read first

- [`own-mutex-interior`](../../../.claude/skills/rust-skills/rules/own-mutex-interior.md) — take the
  "When to Use What" table and the `parking_lot` section. Ignore its **Mutex Poisoning** examples:
  they are `std::sync::Mutex` shapes that walrus deliberately does not have (PR 7.6), and copying
  `lock().unwrap()` in here would violate `clippy::unwrap_used = "deny"`.
- `crates/pg-sink/src/reload_signal.rs:1-18` — the module header explaining subscribe-then-insert
  and buffer-at-`Insert` / resolve-at-`Commit`. The lock scope is part of that contract: `subscribe`
  must be able to run while a `resolve` is in flight.
- `crates/pg-sink/src/reload_signal.rs:43-99` — `WatermarkWaiters`, `subscribe` (`:55-59`, one
  insert under the lock) and `resolve` (`:67-99`, the offender).
- `crates/pg-sink/src/reload_signal_test.rs` — the seven existing tests, especially
  `resolve_without_subscriber_is_a_quiet_noop` and
  `dropped_receiver_then_resolve_is_fine_and_entry_is_evicted`. They are the behavioural contract to
  keep green.
- `Cargo.toml:38-41` — the comment recording *why* `parking_lot` is a direct dependency.

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

- Rewrite `WatermarkWaiters::resolve` so the guard is released before the body: bind
  `self.waiters.lock().remove(&(reload_id, chunk_no))` to a local, then `match` the local. The
  cross-check block above it (`:68-79`) is untouched — it never takes the lock.
- Add both lints to `[workspace.lints.clippy]`, `= "deny"`:
  `significant_drop_in_scrutinee` and `significant_drop_tightening`, with a comment naming the site
  they were introduced for.
- Add one regression test to `crates/pg-sink/src/reload_signal_test.rs`: after `resolve`, a fresh
  `subscribe` on the same `(reload_id, chunk_no)` gets a working receiver — i.e. the entry really was
  evicted and the registry is usable again.

**Explicitly deferred** (do *not* build these here)

- Do **not** swap `parking_lot::Mutex` for `std::sync::Mutex` or anything else. It is the
  workspace's deliberate non-poisoning choice from PR 7.6, precisely so `.lock()` needs no
  `unwrap`/`expect` under the PR 7.7 deny.
- Do not touch `crates/loader/src/health.rs:28`: both of its `.lock()`s hold nothing across a call
  (`stamp_poll` is one assignment, `is_live` is one `is_some()`), and neither lint fires there.
- Do not restructure the registry (no sharding, no `DashMap`, no `RwLock` — see PR 9.12), and do not
  change `subscribe`'s replace-the-stale-sender semantics.
- No new dependency; nothing here would clear `cargo deny` faster than zero new crates does.

## Files to create / modify

```
Cargo.toml                                  # + the two significant_drop lints
crates/pg-sink/src/reload_signal.rs         # resolve(): bind the removal, then match the local
crates/pg-sink/src/reload_signal_test.rs    # + resubscribe-after-resolve regression test
```

## Skeleton

```rust
// crates/pg-sink/src/reload_signal.rs:67 — only the tail of `resolve` changes.
impl WatermarkWaiters {
    pub fn resolve(&self, reload_id: i64, chunk_no: i64, echo: Echo) {
        if echo.embedded_lsn >= echo.commit_lsn {
            todo!("unchanged: counter + metric + the cross-check error log (:68-79)");
        }
        // The guard's ONLY job is the removal. Binding it to a local ends the temporary at this
        // statement, so the two `tracing::` expansions and the `oneshot` send below run unlocked —
        // an exporter's `subscribe` is never blocked behind a log line on the decode hot path.
        let waiter = self.waiters.lock().remove(&(reload_id, chunk_no));
        match waiter {
            Some(tx) => {
                if tx.send(echo).is_err() {
                    tracing::debug!(reload_id, chunk_no, "echo resolved after waiter gave up");
                } else {
                    todo!("unchanged: the `reload_signal echo` info log (:86-92)");
                }
            }
            None => {
                tracing::debug!(reload_id, chunk_no, "echo with no subscriber; dropped");
            }
        }
    }
}
```

```rust
// crates/pg-sink/src/reload_signal_test.rs — the registry is usable again after a resolve
#[test]
fn resolve_evicts_so_the_same_chunk_can_resubscribe() {
    let waiters = WatermarkWaiters::default();
    let mut first = waiters.subscribe(7, 0);
    waiters.resolve(
        7,
        0,
        Echo { commit_lsn: lsn("0/20"), embedded_lsn: lsn("0/10") },
    );
    assert_eq!(first.try_recv().expect("resolved").commit_lsn, lsn("0/20"));

    // Same key again: the previous entry was removed, so this is a fresh, resolvable wait.
    let mut second = waiters.subscribe(7, 0);
    waiters.resolve(
        7,
        0,
        Echo { commit_lsn: lsn("0/40"), embedded_lsn: lsn("0/30") },
    );
    assert_eq!(second.try_recv().expect("resolved").commit_lsn, lsn("0/40"));
}
```

```toml
# Cargo.toml — [workspace.lints.clippy], appended below expect_used
# Lock scope is a correctness property on the decode hot path (PR 9.11, `own-mutex-interior`): a
# `match self.x.lock().remove(..)` holds the guard for the whole body. Both lints are nursery (not in
# `clippy::all`, so no `priority` needed) and both are at 0 sites after reload_signal.rs::resolve is
# fixed.
significant_drop_in_scrutinee = "deny"
significant_drop_tightening = "deny"
```

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-mutex-interior.md
focused-test = cargo test -p pg-sink
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `WatermarkWaiters::resolve` binds the `remove` result to a local and matches the local; the
      guard is provably released before `tx.send(echo)` and before both `tracing::` calls.
- [ ] The comment above that `let` says *why* (a scrutinee temporary lives to the end of the
      `match`), so nobody folds it back into the `match`.
- [ ] `Cargo.toml` `[workspace.lints.clippy]` contains `significant_drop_in_scrutinee = "deny"` and
      `significant_drop_tightening = "deny"`, with a comment.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` is green with **no**
      `#[allow(clippy::significant_drop_…)]` anywhere.
- [ ] `crates/pg-sink/src/reload_signal.rs` still uses `parking_lot::Mutex`; `subscribe`'s
      replace-the-stale-sender behaviour, the cross-check counter, and the metric call are unchanged.
- [ ] `crates/loader/src/health.rs` is untouched.
- [ ] The seven existing tests in `reload_signal_test.rs` pass unchanged, plus the new
      resubscribe-after-resolve test.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)

## What completed looks like

```
# BEFORE (on main) — the nursery sweep names exactly one site
$ grep -c 'significant_drop' Cargo.toml
0

$ cargo clippy --all-targets --all-features -- -W clippy::nursery 2>&1 | grep -A3 significant_drop_in_scrutinee
warning: temporary with significant `Drop` in `match` scrutinee will live until the end of the `match` expression
   --> crates/pg-sink/src/reload_signal.rs:80:15
    |
 80 |           match self.waiters.lock().remove(&(reload_id, chunk_no)) {
    |                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

# AFTER
$ grep -c 'significant_drop' Cargo.toml
2

$ cargo clippy --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)

$ cargo test -p pg-sink reload_signal
running 8 tests
test reload_signal::tests::resolve_evicts_so_the_same_chunk_can_resubscribe … ok
test result: ok. 8 passed; 0 failed
```

## Hints & gotchas

- **The rule you are learning, precisely:** a temporary created in a `match` scrutinee is dropped at
  the end of the enclosing *statement*, and the whole `match` is that statement. Binding it with
  `let x = …;` makes the `let` the statement instead. (Rust 2024 changed `if let` temporary scopes —
  it did **not** change `match`, and this workspace is edition 2021 anyway.)
- You cannot `drop(self.waiters.lock())` to fix this: the guard must outlive the `remove` call. Bind
  first — that is the whole trick.
- `tx.send(echo)` **moves** the sender out of the map; that move must happen after the removal and
  outside the lock, which the `let` gives you for free.
- **Land PR 9.10 first.** `significant_drop_tightening` also fires on
  `crates/loader/src/phase_a.rs`'s `let mut slot = logged.lock(); match (*slot, live)` shape; 9.10
  deletes those mutexes. Denying the lint before 9.10 merges turns this into a two-crate PR.
- `significant_drop_tightening` is a nursery lint with known false positives. If it fires somewhere
  the "fix" would hurt readability, restructure so the guard's scope is a single statement, or bind
  a named guard and `drop(guard)` explicitly — do **not** add a blanket `#[allow]` for a lint you
  introduced in the same PR.
- `parking_lot::MutexGuard` has no `Result`, so there is no `unwrap` to write here; that is exactly
  the property PR 7.6 bought and PR 7.7 depends on. Do not "modernise" to `std::sync::Mutex`.
- Unit tests are **sibling files** (`reload_signal.rs` → `reload_signal_test.rs`), never an inline
  `mod tests {}`. The existing tests are plain sync `#[test]`s using `let mut rx = …; rx.try_recv()`
  and the file-local `lsn(…)` helper — follow that shape rather than introducing `#[tokio::test]`.
- `unwrap`/`expect` are allowed in tests via `clippy.toml` but stay denied in production.
- No `.sql` file and no `.sqlx` cache entry is involved.

## References

- Rule: [`own-mutex-interior`](../../../.claude/skills/rust-skills/rules/own-mutex-interior.md)
- Design: `docs/single-table-reload.md` H1 (the echo-wait watermark — why the registry is shared
  between the decode loop and the exporter tasks) and
  `docs/implementation/notes/commit-visibility-race.md` (the race the cross-check bounds).
- Prev: [PR 9.10](./pr-9.10-own-refcell-interior.md) · Next: [PR 9.12](./pr-9.12-own-rwlock-readers.md) · [Roadmap](../README.md)
