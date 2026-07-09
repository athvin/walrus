# PR 4.6 — Total-restart: epoch bump on slot loss, and never on a transient disconnect

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/73

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `pg-sink`, `loader`, `control`,
> `tests/e2e` · **Est. size:** L · **Depends on:** PR 4.5 · **Unlocks:** PR 4.7

The disaster path. When — on a **successful** source connection — the sink finds the single lifelong slot
**absent** (`pg_replication_slots` empty) or **invalidated** (`wal_status='lost'`), the change history
since `confirmed_flush_lsn` is permanently gone and the only correct recovery is a whole-system re-sync:
**bump the epoch**, create a new slot with a fresh exported snapshot, retire old-epoch state, and have the
loaders **rebuild every `.duckdb` file** (re-append the new-epoch snapshot into `<table>_raw`, re-derive
`<table>`) resetting **both** watermarks. It is loud, guarded against false positives — a **transient
disconnect is not slot loss** — and it proves the `architecture.md` **"Slot loss / total-restart"** bullet
and §1.8.

## Why — learning objectives

By the end of this PR you will have practised:

- **Distinguishing a disaster from a hiccup** — only "connected, slot absent/lost" triggers total-restart;
  a connection failure is retried with backoff and must **never** bump the epoch.
- **Epoch as a generation boundary** — everything (S3 prefix, manifest, both watermarks, `.duckdb` files)
  is namespaced by epoch, so a generation can be abandoned and rebuilt as a unit.
- **A cross-service state transition** — the sink bumps the epoch in `replication_state`; the loaders
  detect the new epoch and rebuild, all coordinated through the control plane.
- **Nuke-and-repave, safely** — alerting loudly, retiring old-epoch S3 to its TTL, and re-snapshotting all
  tables under the new epoch.

## Read first

- `../../architecture.md#18-single-slot-for-life--total-restart` — the four-step total-restart procedure,
  epoch namespacing, and the "guarded against false positives" rule (transient disconnect ≠ slot loss).
- `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` — the **"Slot loss /
  total-restart"** bullet (epoch bump, new slot, re-snapshot, both tables rebuilt, both watermarks reset;
  transient disconnect does NOT trigger it).
- `../../architecture.md#17-snapshot--backfill-bootstrap` — the fresh exported snapshot + `consistent_point`
  the new epoch's slot captures (reuse the PR 2.29 backfill path).
- `../../architecture.md#startup--bootstrap-fail-fast-preflight` — the backoff retry that a transient
  failure takes instead.

## Scope

**In scope**

- `pg-sink/src/epoch.rs`: a `SlotStatus` classifier (`Healthy` / `Absent` / `Invalidated` / `Unreachable`)
  and a `total_restart()` transition that bumps `replication_state.epoch`, creates a new slot with a fresh
  exported snapshot, and retires the old-epoch manifest queue.
- The **false-positive guard**: `Unreachable` (connection failed) routes to the bootstrap backoff retry
  and **never** bumps the epoch; only `Absent` / `Invalidated` on a *successful* connection triggers it.
- Loader-side epoch detection: on seeing a newer epoch in `replication_state`, rebuild each `.duckdb` file
  (re-append the new-epoch snapshot into `<table>_raw`, re-derive `<table>`) and **reset both watermarks**.
- A compose chaos test that drops the slot mid-run and asserts the full transition.

**Explicitly deferred** (do *not* build these here)

- Old-epoch S3 GC beyond leaving it to its bucket lifecycle TTL — no active delete in v1.
- Per-table reload without total-restart → a **deferred design goal** (see PR 4.11); total-restart always
  rebuilds *every* table together.
- `DROP_REPLICATION_SLOT` on decommission (a different, explicit path) — not touched here.

## Files to create / modify

```
crates/pg-sink/src/epoch.rs          # new — SlotStatus classifier + total_restart() transition
crates/pg-sink/src/sink.rs           # modify — call classify_slot() on each (re)connect; route Unreachable to backoff
crates/control/src/replication_state.rs   # modify — bump_epoch(), read current epoch (CHECK-guarded)
crates/loader/src/epoch.rs           # new — detect newer epoch → rebuild every .duckdb + reset both watermarks
tests/e2e/tests/total_restart.rs     # new — drop-slot chaos + transient-disconnect-is-not-slot-loss
# no new deps
```

## Skeleton

```rust
// crates/pg-sink/src/epoch.rs
use common::Lsn;

/// Result of inspecting the slot on a connection attempt. Only Absent/Invalidated —
/// observed on a SUCCESSFUL connection — are slot loss. Unreachable is a hiccup.
#[derive(Debug, PartialEq, Eq)]
pub enum SlotStatus {
    Healthy { confirmed_flush: Lsn },
    Absent,                 // connected; pg_replication_slots has no row → total-restart
    Invalidated,            // connected; wal_status = 'lost'        → total-restart
    Unreachable,            // connection failed → backoff retry, NEVER total-restart
}

/// Classify the slot over a live source connection. Distinguishes "connected but slot gone"
/// (disaster) from "could not connect" (transient) — the whole false-positive guard.
pub async fn classify_slot(/* source conn, slot_name */) -> SlotStatus { todo!() }

/// The nuke-and-repave transition. Bumps the epoch, creates a new slot with a fresh exported
/// snapshot + consistent_point, and retires the old-epoch manifest queue. LOUD (alerts).
pub async fn total_restart(/* control, source */) -> Result<u64 /* new epoch */, crate::Error> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn unreachable_never_triggers_total_restart() { todo!() }
    #[test] fn absent_or_invalidated_on_success_triggers_total_restart() { todo!() }
}
```

```rust
// crates/loader/src/epoch.rs
/// On detecting a newer epoch than the loader's local state, rebuild every .duckdb file:
/// re-append the new-epoch snapshot into <table>_raw, re-derive <table>, reset BOTH watermarks.
pub async fn rebuild_for_new_epoch(/* control, duckdb files */) -> Result<(), crate::Error> { todo!() }
```

```rust
// tests/e2e/tests/total_restart.rs
#![cfg(feature = "it")]
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn dropping_the_slot_triggers_epoch_bump_and_full_rebuild() {
    // Run steadily; DROP the slot on the source mid-run.
    // Assert: epoch bumps, new slot created, all tables re-snapshotted, every .duckdb rebuilt
    //         (raw re-appended, mirror re-derived), BOTH watermarks reset; mirror == source.
    todo!()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn transient_disconnect_does_not_trigger_total_restart() {
    // Bounce the source connection (network blip) WITHOUT dropping the slot.
    // Assert: epoch UNCHANGED, sink resumes from confirmed_flush_lsn, no re-snapshot.
    todo!()
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] On a **successful** connection with the slot **absent** or `wal_status='lost'`, the sink bumps
      `replication_state.epoch`, creates a new slot with a fresh exported snapshot, and retires old-epoch
      state (loud alert).
- [x] A **transient disconnect** (connection failed) routes to the bootstrap backoff retry and leaves the
      epoch **unchanged** — no re-snapshot, resume from `confirmed_flush_lsn`.
- [x] Loaders detect the new epoch and rebuild **every** `.duckdb` file (both `<table>` and `<table>_raw`),
      **resetting both watermarks**; the mirror converges to the source under the new epoch.
- [x] The new epoch namespaces the S3 prefix, manifest rows, and checkpoints; old-epoch S3 is left to its
      lifecycle TTL.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink -p loader -p control` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p e2e --features it -- --ignored` asserting
        **`dropping_the_slot_triggers_epoch_bump_and_full_rebuild`** and
        **`transient_disconnect_does_not_trigger_total_restart`**.

## Hints & gotchas

- The single most dangerous bug here is a **false positive**: treating a network blip as slot loss would
  nuke and re-snapshot the whole system on every hiccup. The classifier must only return `Absent` /
  `Invalidated` when the connection *succeeded* and the catalog authoritatively says the slot is gone.
- `wal_status='lost'` (from `pg_replication_slots`) is the invalidation signal after
  `max_slot_wal_keep_size` is exceeded — distinct from an empty result (dropped slot). Handle both.
- Total-restart is **whole-system** by construction: never rebuild a single `.duckdb`. Every table shares
  the epoch and is rebuilt together — per-table reload is a deferred goal (PR 4.11), not this.
- Reuse the PR 2.29 exported-snapshot path for the new slot; do not invent a second snapshot mechanism.
- Make it **loud** — a total-restart is an operator-visible disaster event; emit an error-level structured
  log + a metric the PR 4.10 alert can fire on.

## References

- Design: `../../architecture.md#18-single-slot-for-life--total-restart`;
  `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` ("Slot loss /
  total-restart"); `../../architecture.md#17-snapshot--backfill-bootstrap`,
  `#startup--bootstrap-fail-fast-preflight`.
- Prev: [PR 4.5](./pr-4.5-e2e-wal-runaway-heartbeat.md) · Next: [PR 4.7](./pr-4.7-ci-cargo-deny.md) ·
  [Roadmap](../README.md)
