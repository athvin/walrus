# PR 2.26 — Durability checkpoint: advance `confirmed_flush_lsn` only after S3 + manifest

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib) ·
> **Est. size:** M · **Depends on:** PR 2.25 · **Unlocks:** PR 2.27

The final link, and the **heart of the whole sink**: only *after* a batch's Parquet is durable in S3
(PR 2.24) **and** its `file_manifest` row is committed (PR 2.25) does the sink send a standby status
update advancing **`confirmed_flush_lsn`** to the batch's `lsn_end`. This is the WAL-bounding invariant —
slot lag is bounded to at most one in-flight batch, and a crash before the checkpoint just re-streams
from the last confirmed LSN (at-least-once, no loss). Crucially, `confirmed_flush_lsn` is a **different
LSN** from the unconditional keepalive/received LSN of PR 2.20: durability moves the former; liveness
moves the latter.

## Why — learning objectives

By the end of this PR you will have practised:

- **Separating two LSNs in one feedback channel** — the standby reply's `write` (keepalive/received) vs
  `flush`/`apply` (= `confirmed_flush_lsn`, durability), and why conflating them causes disconnects *or*
  data loss depending on which way you err.
- **Encoding a crash-safety invariant in code** — the strict PUT → COMMIT manifest → advance ordering,
  so every reachable interleaving is at-least-once.
- **Bounded slot lag as backpressure** — not advancing the slot *is* the backpressure when S3/manifest
  slows; the slot grows only to `max_slot_wal_keep_size` (alert well before).
- **Testing crash windows** — killing the process between PUT and the standby update and asserting
  re-stream on restart.

## Read first

- `../../architecture.md` §1.5 The durability checkpoint (`#15-the-durability-checkpoint-wal-bounding-invariant`) —
  the **Invariant** box and the per-batch ordering; `restart_lsn` vs `confirmed_flush_lsn`; "this gate
  advances only `confirmed_flush_lsn`."
- `../../architecture.md` §1.9 (`#19-slot-liveness--heartbeat--keepalive`), the two-LSN bullet — keepalive
  is unconditional and separate from this durability advance.
- `../../architecture.md` §1.6 (`#16-large-transaction-safety`), the "never advance past an open txn"
  bullet — the ceiling this PR must respect (full large-txn handling is PR 2.30).
- Prior PR 2.20 (`StandbyStatus` / `send_standby_status`) — the wire path this PR now drives with the
  durable LSN.

## Scope

**In scope**

- After `flush_batch` (PR 2.25) returns a durable `WrittenObject`, advance the tracked
  `confirmed_flush_lsn` to `obj.lsn_end` and send a standby status update carrying it as `flush`/`apply`.
- Keep the **keepalive** path (PR 2.20) advancing `write` independently and unconditionally — the two
  LSNs are tracked separately and both sent in the `'r'` reply.
- Never advance `confirmed_flush_lsn` past the oldest still-open streamed txn's floor (a simple guard;
  the streamed machinery is PR 2.30 — here small/whole txns make this a no-op ceiling).
- Structured log on each checkpoint advance (`commit_lsn`, `lsn_end`, `confirmed_flush_lsn`).

**Explicitly deferred** (do *not* build these here)

- Idle heartbeat that *creates* a fresh LSN to confirm when tables are idle → **PR 2.27**.
- Holding `confirmed_flush_lsn` at an open streamed txn's begin LSN across real streamed batches → **PR 2.30**.
- Graceful-shutdown final standby update → **PR 2.28**.

## Files to create / modify

```
crates/pg-sink/src/checkpoint.rs     # new — DurabilityCheckpoint: tracks confirmed_flush_lsn, guards open-txn floor
crates/pg-sink/src/consume.rs        # modify — after flush_batch, checkpoint.advance(obj.lsn_end)
crates/pg-sink/tests/durability.rs   # new — compose: slot advances only after flush; crash re-streams
```

## Skeleton

```rust
// crates/pg-sink/src/checkpoint.rs
use common::Lsn;
use crate::replication::{ReplicationStream, StandbyStatus};

/// Owns the slot-advancing LSN. Distinct from the keepalive/received LSN (PR 2.20).
pub struct DurabilityCheckpoint {
    confirmed_flush: Lsn,        // moves ONLY after S3 + manifest durable
    received: Lsn,               // keepalive/written LSN — moves unconditionally
    open_txn_floor: Option<Lsn>, // never advance confirmed_flush past this (PR 2.30 fills it)
}

impl DurabilityCheckpoint {
    pub fn new(resume_lsn: Lsn) -> Self { todo!() }

    /// Batch durable (PUT + manifest committed): advance confirmed_flush to lsn_end, clamped to the floor.
    pub fn on_batch_durable(&mut self, lsn_end: Lsn) { todo!() }

    /// Unconditional keepalive: bump the received LSN (does NOT touch confirmed_flush).
    pub fn on_received(&mut self, wal_end: Lsn) { todo!() }

    /// The standby reply: write = received (keepalive), flush/apply = confirmed_flush (durable).
    pub fn standby_status(&self, reply_requested: bool) -> StandbyStatus { todo!() }

    pub async fn send(&self, stream: &mut ReplicationStream, reply_requested: bool) -> anyhow::Result<()> { todo!() }
}
```

```rust
// crates/pg-sink/tests/durability.rs
/// The slot's confirmed_flush_lsn only reaches lsn_end AFTER the S3 object + manifest row exist.
#[tokio::test]
async fn slot_advances_only_after_s3_and_manifest_durable() { todo!() }

/// Kill between PUT and the standby update -> on restart the batch re-streams (at-least-once, no loss).
#[tokio::test]
async fn crash_between_put_and_standby_restreams_without_loss() { todo!() }

/// Keepalive `write` advances during a stalled flush while `flush`/`apply` (confirmed_flush) hold.
#[tokio::test]
async fn keepalive_lsn_moves_while_confirmed_flush_holds() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `confirmed_flush_lsn` advances to a batch's `lsn_end` **only after** both the S3 PUT and the
      manifest commit succeeded — the ordering is enforced, not incidental.
- [ ] The standby reply carries **two distinct LSNs**: `write` = keepalive/received (unconditional),
      `flush`/`apply` = `confirmed_flush_lsn` (durable) — a stalled flush advances the former, not the latter.
- [ ] `confirmed_flush_lsn` is never advanced past the open-txn floor (a no-op guard for now; wired for PR 2.30).
- [ ] A kill between the PUT and the standby update causes the batch to **re-stream** on restart with no
      data loss (the loader's dedup would absorb the replay downstream).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test durability`: the slot advances to
        `lsn_end` only after flush; a crash between PUT and standby re-streams.

## Hints & gotchas

- **This is the invariant the whole sink exists for.** Advancing `confirmed_flush_lsn` before the PUT is
  durable loses data on crash; advancing it *as* your keepalive (only after durability) causes walsender
  disconnects. Two LSNs, two rules — keep them in separate fields and never collapse them.
- Verify the advance against the **server**, not just your in-memory number: assert
  `pg_replication_slots.confirmed_flush_lsn` moved (there's a round-trip; the server may batch feedback).
- `restart_lsn` follows `confirmed_flush_lsn` (the WAL Postgres still needs). Monitor both; the test can
  read both from `pg_replication_slots`.
- The open-txn floor is `None` until PR 2.30 introduces streamed txns — but wire the `clamp` now so 2.30
  is a data change, not a control-flow change. Document that small/whole txns leave the floor unset.
- Slot lag bounded to "one in-flight batch" is the *feature*: if S3/manifest slows, **do not** advance —
  the slot growing (up to the safety cap) is correct backpressure, and the alert (PR 4.10) fires well
  before `max_slot_wal_keep_size`.

## References

- Design: `../../architecture.md` §1.5 (invariant), §1.9 (two-LSN), §1.6 (open-txn ceiling).
- Prev: [PR 2.25](./pr-2.25-sink-manifest-insert.md) · Next: [PR 2.27](./pr-2.27-sink-heartbeat-liveness.md) · [Roadmap](../README.md)
