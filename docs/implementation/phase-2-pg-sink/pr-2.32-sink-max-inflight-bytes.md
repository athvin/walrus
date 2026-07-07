# PR 2.32 — Enforce an aggregate `max_inflight_bytes` ceiling with spill + pause-poll

> **Phase:** 2 — walrus-pg-sink · **Crates touched:** `pg-sink`, `common` · **Est. size:** M ·
> **Depends on:** PR 2.31 · **Unlocks:** PR 2.33

The per-batch `max_bytes`/`max_rows` caps from PR 2.23 bound *one* batch; they do nothing to stop the
sum of all in-flight `(table, xid)` builders from OOM-killing the pod when one giant open transaction
streams faster than S3 drains. This PR adds the **aggregate, process-wide** `max_inflight_bytes`
ceiling: on crossing it, flush **committed** batches the normal way and **spill open-txn buffers
speculatively** to S3 (PR 2.30's staging), and — only if S3 still can't keep up — fall back to a
reactive **pause-poll** backstop with hysteresis. Freeing memory and advancing the slot stay separable.

## Why — learning objectives

By the end of this PR you will have practised:

- **Aggregate accounting** — summing bytes across a `HashMap<(table, xid), u64>`, distinct from any
  single batch's cap.
- **Shed ordering** — prefer the cheap, correctness-free move (flush committed) before the expensive
  one (spill open txn), before the last resort (stop requesting WAL).
- **Hysteresis** — an activate/resume band (≈ 0.85 / 0.75) so the backstop doesn't flap.
- **Bounded-memory reasoning** — why pausing intake trades OOM-risk for bounded slot growth (the same
  trade §1.5 makes), and why `logical_decoding_work_mem` does **not** bound *our* memory.

## Read first

- `../../architecture.md#13-in-memory-batching--cadence` — the four cadence triggers, the aggregate
  `max_inflight_bytes` ceiling, the shed order (committed → speculative spill → pause-poll backstop),
  and the sizing rule (a *fraction* of the pod limit).
- `../../architecture.md#15-the-durability-checkpoint-wal-bounding-invariant` — "a memory-ceiling flush
  is just another durable batch"; freeing memory ≠ advancing the slot.
- `../../walrus-pg-sink.md#44-steady-state` — size `max_inflight_bytes` **below** the container memory
  `limit` (request = limit → Guaranteed QoS) so a graceful spill beats a cgroup OOM-kill.

## Scope

**In scope**

- An `InflightMeter` tracking per-`(table, xid)` bytes and the aggregate total vs `max_inflight_bytes`.
- A `ShedAction` decision: `FlushCommitted` → `SpillOpenTxn(xid)` → `PausePoll`, applied when the
  ceiling is crossed; spill reuses PR 2.30's speculative S3 staging (no manifest row, no slot advance
  past the open-txn floor).
- A `Backpressure` hysteresis gate (activate ≈ 0.85, resume ≈ 0.75) driving the pause-poll backstop.
- A `spill_count` counter incremented on each speculative spill (logged via `tracing`).

**Explicitly deferred** (do *not* build these here)

- Prometheus export of `spill_count` / in-flight bytes / retained-WAL → **PR 4.10**.
- Final tuning of the ratios and per-stream division (Open Q14) → non-blocking; defaults here.
- e2e WAL-runaway proof that in-flight bytes **and** slot size stay bounded → **PR 4.5**.

## Files to create / modify

```
crates/pg-sink/src/memory.rs         # new — InflightMeter, ShedAction, Backpressure
crates/pg-sink/src/sink.rs           # modify — meter each builder; act on ShedAction
crates/pg-sink/src/config.rs         # modify — max_inflight_bytes, activate/resume ratios (validated)
crates/pg-sink/src/lib.rs            # modify — `pub mod memory;`
crates/pg-sink/tests/max_inflight_bytes.rs   # new — compose integration test
# no new Cargo deps
```

## Skeleton

```rust
// crates/pg-sink/src/memory.rs
use std::collections::HashMap;

pub type TableId = u32;   // pg relation OID (or a stable table id)

/// Aggregate, process-wide in-memory accounting across all (table, xid) Arrow builders.
pub struct InflightMeter {
    ceiling_bytes: u64,
    by_stream: HashMap<(TableId, u32), u64>,   // (table, xid) → buffered bytes
}

impl InflightMeter {
    pub fn new(ceiling_bytes: u64) -> Self { todo!() }
    pub fn add(&mut self, key: (TableId, u32), bytes: u64) { todo!() }
    pub fn release(&mut self, key: (TableId, u32)) { todo!() }
    pub fn total(&self) -> u64 { todo!() }
    pub fn over_ceiling(&self) -> bool { todo!() }
    /// The largest open-txn stream — the best spill candidate.
    pub fn largest_open(&self) -> Option<(TableId, u32)> { todo!() }
}

/// What to do when the ceiling is crossed — cheapest correctness-free move first.
#[derive(Debug, PartialEq, Eq)]
pub enum ShedAction {
    FlushCommitted,        // normal path frees memory AND may advance the slot (to the open-txn floor)
    SpillOpenTxn(TableId, u32),  // speculative S3 staging — frees memory, slot NOT advanced
    PausePoll,             // reactive backstop: stop requesting WAL until memory drains
}

/// Hysteresis so the pause-poll backstop doesn't flap around the ceiling.
pub struct Backpressure { activate_ratio: f64, resume_ratio: f64, paused: bool }

impl Backpressure {
    /// Returns whether intake should be PAUSED after this tick (activate high, resume low).
    pub fn tick(&mut self, total: u64, ceiling: u64) -> bool { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn over_ceiling_when_sum_across_streams_exceeds_budget() { todo!() }
    #[test] fn shed_prefers_committed_then_spill_then_pause() { todo!() }
    #[test] fn hysteresis_pauses_at_activate_resumes_at_lower_ratio() { todo!() }
    #[test] fn spill_of_open_txn_does_not_advance_the_slot() { todo!() }
}
```

```rust
// crates/pg-sink/tests/max_inflight_bytes.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn large_txn_low_ceiling_spills_and_stays_bounded() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] A large transaction under a deliberately **low** `max_inflight_bytes` increments the
      `spill_count` counter (speculative spill occurs), and aggregate in-flight bytes stay bounded.
- [ ] The shed order is honoured: committed batches flush first; only open-txn buffers spill; pause-poll
      is the last resort.
- [ ] A speculative spill **frees memory but does not advance `confirmed_flush_lsn`** (still clamped at
      the open-txn floor from PR 2.30).
- [ ] The pause-poll backstop pauses at the activate ratio and resumes only at the lower resume ratio
      (no flapping) — unit-tested.
- [ ] Config is bounds-validated: `max_inflight_bytes > 0`, `resume_ratio < activate_ratio < 1.0`;
      invalid → terminal.
- [ ] Docs/comments state `logical_decoding_work_mem` does **not** bound *our* memory and the ceiling
      must sit below the pod limit.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test max_inflight_bytes -- --ignored`
        asserting **`large_txn_low_ceiling_spills_and_stays_bounded`**.

## Hints & gotchas

- `max_inflight_bytes` is **aggregate** and distinct from the per-batch `max_bytes` — do not conflate;
  one bounds *total* buffered memory, the other bounds a single Parquet file's size.
- Estimating buffered bytes from Arrow builders is approximate — use the builder's capacity/len, not a
  post-serialization size, and account for it *before* you cross the limit.
- Spilling an **open** txn's buffer must keep the speculative-file semantics from PR 2.30 (no manifest
  row); do not accidentally publish it. Interacts with PR 2.31: a spilled buffer of an
  aborted sub-xid must not be published on commit.
- Hysteresis prevents a restart storm on the reader — a single-threshold pause/resume flaps badly under
  a steady large stream. Keep the resume ratio meaningfully below activate.

## References

- Design: `../../architecture.md#13-in-memory-batching--cadence`,
  `#15-the-durability-checkpoint-wal-bounding-invariant`;
  `../../walrus-pg-sink.md#44-steady-state`.
- Prev: [PR 2.31](./pr-2.31-sink-subtransaction-exclusion.md) ·
  Next: [PR 2.33](./pr-2.33-sink-ddl-capture.md) · [Roadmap](../README.md)
