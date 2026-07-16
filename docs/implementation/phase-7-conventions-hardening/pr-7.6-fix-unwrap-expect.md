<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 7.6 — Remove production `unwrap` / `expect`

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 7 — conventions hardening · **Crates touched:** `common`, `loader`, `pg-sink`,
> `pg-to-arrow` · **Est. size:** M · **Depends on:** PR 7.1–7.5 (rebase onto the cleaned tree) ·
> **Unlocks:** PR 7.7 (the lint flip lands on a clean tree)

Remediate every production `unwrap`/`expect` — ~21 call sites across 8 files (control is already
clean) — so PR 7.7 can turn the lint to `deny` with nothing to catch. This PR makes **no lint change**;
CI is still permissive, so it can be reviewed purely for *correctness*: does swapping a poisoning
`Mutex` for `parking_lot` change semantics? Does propagating a decode error alter a code path? Each
offender gets the idiomatic fix for its class — a lock that can't poison, a typed error at a fallible
boundary, or a restructure that removes the "can't happen" lookup entirely.

## Why — learning objectives

By the end of this PR you will have practised:

- **`parking_lot::Mutex`** — a lock whose `.lock()` returns the guard directly (no `LockResult`, no
  poisoning), which is exactly why it satisfies a hard `unwrap`/`expect` ban with no ceremony.
- **Pushing panics to typed errors** — turning `try_into().expect("8 bytes")` into a `DecodeError`
  returned with `?`, even when a length guard already makes it infallible.
- **Designing away invariants** — replacing `map.get_mut(&k).expect("exists")` with a single borrow
  (`let Some(x) = … else { … }`) or threading a value through an enum variant instead of recovering it
  from an `Option`.

## Read first

- `crates/pg-sink/src/shutdown.rs:94-109` — the **house pattern** for signal-handler registration
  (`match { Ok(s)=>s, Err(e)=>{ log; cancel; return } }`); the loader copies it.
- `crates/pg-sink/src/pgoutput/error.rs` — the existing `DecodeError` the reader sites return.
- The Phase 7 plan's per-site table (the 21 offenders and their classes).

## Scope

**In scope** — fix each production offender by class:

- **Mutex poison** (`loader/health.rs`, `loader/phase_a.rs`, `pg-sink/reload_signal.rs`): switch those
  `std::sync::Mutex` to `parking_lot::Mutex` (`.lock()` returns the guard; no `.unwrap()`/`.expect()`).
  Add `parking_lot = "0.12"` to `[workspace.dependencies]` and `parking_lot.workspace = true` to
  `loader` + `pg-sink`; update the free-fn signature `phase_a::pause_began(&Mutex<…>)`.
- **Signal handlers** (`loader/shutdown.rs`): replace the two `.expect(...)` inside the spawned
  `async move` with the pg-sink `match`-log-cancel-return pattern (a `?` can't escape the task). Keep
  the post-select re-`recv()` loop.
- **Binary-slice reads** (`pg-sink/pgoutput/reader.rs`, `pg-sink/replication.rs`): return a typed error
  (`DecodeError` / an `anyhow!`/small `WireError`) via `.map_err(…)?` in place of
  `try_into().unwrap()`/`.expect("N bytes")`.
- **Invariant expects** (`pg-sink/stream_txn.rs`, `pg-sink/reload_export.rs`, `pg-to-arrow/batch.rs`):
  restructure — `let Some(txn) = self.open.get_mut(&top) else { continue };`; thread the watermark via
  a `Drained { final_lsn }` variant; `.ok_or_else(|| …)?` for the PK-column lookup;
  `if let Some(mc) = self.meta_const.as_deref() { … }`.
- **Recorder install** (`common/metrics.rs`): map the `.expect(...)` to the crate error at the init
  boundary, or a narrowly-scoped documented `#[allow]` if genuinely install-once-infallible.

**Explicitly deferred** (do *not* build these here)

- Adding the `deny` lint + `clippy.toml` → **PR 7.7** (this PR leaves the lint untouched on purpose).
- `backfill.rs`'s `unimplemented!` and `batch.rs`'s `unreachable!` — **not** caught by
  `unwrap_used`/`expect_used` (different `clippy::restriction` lints, not enabled); leave them.

## Files to create / modify

```
Cargo.toml                                  # + parking_lot in [workspace.dependencies]
crates/loader/Cargo.toml                    # + parking_lot.workspace = true
crates/pg-sink/Cargo.toml                   # + parking_lot.workspace = true
crates/loader/src/{health,phase_a,shutdown}.rs          # mutex + signal fixes
crates/pg-sink/src/{reload_signal,pgoutput/reader,replication,stream_txn,reload_export}.rs  # fixes
crates/pg-to-arrow/src/batch.rs             # Option restructure
crates/common/src/metrics.rs                # recorder-install fix
```

## Skeleton

```rust
// parking_lot swap — reload_signal.rs (and health.rs / phase_a.rs)
use parking_lot::Mutex;                       // was std::sync::Mutex
self.waiters.lock().insert(key, tx);          // no .unwrap()/.expect(); no poisoning
```

```rust
// typed decode error — pgoutput/reader.rs
pub fn int16(&mut self) -> Result<u16, DecodeError> {
    self.need(2)?;
    let arr: [u8; 2] = self.buf[self.pos..self.pos + 2]
        .try_into()
        .map_err(|_| DecodeError::short(2, self.pos))?;
    self.pos += 2;
    Ok(u16::from_be_bytes(arr))
}
```

```rust
// design away the invariant — stream_txn.rs
let Some(txn) = self.open.get_mut(&top) else { continue };   // was .expect("top exists")
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] No production `.unwrap()`/`.expect(...)` remains in `common`, `loader`, `pg-sink`, `pg-to-arrow`
      (verified by grep over non-test regions); test-side `unwrap`/`expect` is untouched.
- [ ] The `parking_lot::Mutex` swap is behaviour-preserving (guards held over the same critical
      sections; `Debug`/`Default` derives still hold); `cargo deny check` stays green (parking_lot is
      already in `Cargo.lock`, MIT/Apache).
- [ ] The decode-error propagation compiles through all callers; existing decoder/replication tests
      still pass.
- [ ] The invariant restructures are provably equivalent (single borrow, threaded value) — a comment
      records why the removed `expect` could never fire.
- [ ] No lint config changed in this PR (that's 7.7).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`

## What completed looks like

```
$ # production regions only (exclude #[cfg(test)] modules and tests/ dirs)
$ grep -rn '\.unwrap()\|\.expect(' crates/*/src \
    | grep -v '/tests.rs' | grep -vf <(git grep -l '#!\[cfg(test)\]')
(no matches)
$ cargo test --workspace
test result: ok.        # behaviour unchanged
```

The tree is clean of production `unwrap`/`expect` — PR 7.7 can now flip the lint and CI will prove it.

## Hints & gotchas

- `parking_lot 0.12` is **already in `Cargo.lock`** (pulled transitively) — adding it as a direct dep
  introduces no new crate and no new license; `cargo deny` needs no edit.
- Do **not** try to `?` out of the signal-handler `.expect()` — those live inside a `tokio::spawn`ed
  `async move`; a `?` there can't reach the caller. Copy pg-sink's `match`/log/cancel/return shape.
- The `reload_export` "watermark before draining" expect is best fixed *structurally* — return the LSN
  in the `Drained { final_lsn }` variant so there is no `Option` to recover — rather than papering it
  with `unwrap_or_else` (which would risk a wrong watermark).
- Leave `unreachable!`/`unimplemented!` alone: `unwrap_used`/`expect_used` don't cover them, and 7.7
  doesn't enable the lints that do.
- Rebase this PR onto 7.1–7.5 first so you're fixing the *final* tree (the test-extraction PRs move
  some of these files; the SQL PRs rewrite adjacent call sites).

## References

- Design: `docs/implementation/README.md` "Conventions" (Errors — libraries/binaries; Lints).
- Prev: [PR 7.5](./pr-7.5-loader-duckdb-templates.md) · Next:
  [PR 7.7](./pr-7.7-deny-unwrap-expect-lint.md) · [Roadmap](../README.md)
