# Lock choice: why walrus has no RwLock (PR 9.12)

> **Status:** evaluated — `own-rwlock-readers` deliberately **not** adopted; two locks remain, both
> write-dominated, and `parking_lot::Mutex` is correct for both.

## What the rule asks

The rule recommends `RwLock<T>` when reads significantly outnumber writes, because concurrent
readers can then proceed without serializing on a mutex. Its own exception table makes the boundary
explicit: when writes exceed 20% of operations or the guard is held very briefly, `Mutex` is the
better choice. Both remaining walrus locks fall on that side of the boundary.

## The two locks, measured

| Field | Accessor | Access | Guard hold time |
|---|---|---|---|
| `WatermarkWaiters::waiters` (`crates/pg-sink/src/reload_signal.rs:46`) | `subscribe` | Write: one `HashMap::insert` | One map operation |
| `WatermarkWaiters::waiters` (`crates/pg-sink/src/reload_signal.rs:46`) | `resolve` | Write: one `HashMap::remove` | One map operation; the guard drops before notification or logging |
| `LoaderState::last_poll_completed_at` (`crates/loader/src/health.rs:29`) | `stamp_poll` | Write: replace the timestamp after every apply-worker poll cycle | One assignment |
| `LoaderState::last_poll_completed_at` (`crates/loader/src/health.rs:29`) | `is_live` | Read: the kubelet `/healthz` probe checks `is_some()` | One predicate |

The reload registry is 100% writes: it has no read-only accessor. The health timestamp is also
write-dominated because every apply worker stamps every poll cycle while only the kubelet health
probe reads it. Neither field has multiple concurrent readers doing enough work to amortize reader
tracking.

## Why RwLock loses (twice)

`RwLock` would add reader bookkeeping to the reload registry despite having zero reads. On the
health timestamp it would optimize the less-frequent probe while charging every poll-cycle write.
All four critical sections are a single map operation, assignment, or predicate, so there is no
long read-side work for concurrent readers to overlap. A mutex keeps the shorter and more accurate
API for both access patterns.

## Why parking_lot, not std::sync

Walrus deliberately uses `parking_lot::Mutex`: its guards are non-poisoning and `lock()` returns the
guard directly. `std::sync::Mutex` and `std::sync::RwLock` return `Result` from their locking APIs
because a panic can poison the lock, which invites `unwrap()` or `expect()`. Those calls are denied
workspace-wide by `clippy::unwrap_used` and `clippy::expect_used` since PR 7.7; PR 7.6 made
`parking_lot` a direct dependency to keep that policy and the locking API aligned.

## The guard

`scripts/check-lock-choice.sh` scans production field declarations under `crates/*/src`, excluding
sibling `*_test.rs` files. Every `Mutex<...>` or `RwLock<...>` field must have a `// LOCK-CHOICE:`
justification immediately above it. The script self-tests both acceptance and rejection with a
disposable source tree, and the CI `gates` job runs it immediately after checkout, before toolchain
installation or the DuckDB build.

## When to revisit

Reconsider `RwLock` only when a concrete field is genuinely read-dominated—well below 20% writes—
and its read guards are held long enough that concurrent execution repays reader-tracking overhead.
The proposal must also identify the expected reader concurrency, writer-starvation behavior, and
why `parking_lot::RwLock` remains compatible with the workspace's non-poisoning API policy. A type
name alone is not evidence; update this measurement and add the new field's `LOCK-CHOICE` rationale.
