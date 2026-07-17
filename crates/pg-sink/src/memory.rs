//! Aggregate, process-wide in-memory accounting + backpressure (§1.3). The per-batch `max_bytes`/
//! `max_rows` caps (PR 2.23) bound **one** batch; they do nothing to stop the *sum* of all in-flight
//! `(table, xid)` Arrow builders from OOM-killing the pod when a giant open transaction streams faster
//! than S3 drains. This module adds the aggregate `max_inflight_bytes` ceiling and the shed order.
//!
//! **`logical_decoding_work_mem` does NOT bound *our* memory** — it bounds the *source's* reorder
//! buffer (when it decides to stream), not the sink's buffered Arrow. So the ceiling must sit **below
//! the pod memory limit** (with request = limit for Guaranteed QoS) so a graceful spill beats a cgroup
//! OOM-kill.
//!
//! **Shed order** (cheapest, correctness-free move first): **flush committed** batches (frees memory
//! *and* may advance the slot to the open-txn floor) → **spill open-txn buffers** speculatively to S3
//! (frees memory, slot NOT advanced past the floor) → **pause-poll** (stop requesting WAL) as the last
//! resort. Freeing memory and advancing the slot stay separable (§1.5).

use std::collections::HashMap;

/// A pg relation OID (a stable table id).
pub type TableId = u32;

/// Aggregate, process-wide accounting across all `(table, xid)` Arrow builders — distinct from any
/// single batch's `max_bytes`.
#[derive(Debug)]
pub struct InflightMeter {
    ceiling_bytes: u64,
    total: u64,
    by_stream: HashMap<(TableId, u32), u64>,
}

impl InflightMeter {
    pub fn new(ceiling_bytes: u64) -> Self {
        InflightMeter {
            ceiling_bytes,
            total: 0,
            by_stream: HashMap::new(),
        }
    }

    /// Account `bytes` more buffered for `(table, xid)`.
    pub fn add(&mut self, key: (TableId, u32), bytes: u64) {
        *self.by_stream.entry(key).or_insert(0) += bytes;
        self.total += bytes;
    }

    /// Drop all accounting for `(table, xid)` (its buffer was flushed or spilled).
    pub fn release(&mut self, key: (TableId, u32)) {
        if let Some(bytes) = self.by_stream.remove(&key) {
            self.total -= bytes;
        }
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn ceiling(&self) -> u64 {
        self.ceiling_bytes
    }

    pub fn over_ceiling(&self) -> bool {
        self.total > self.ceiling_bytes
    }

    /// The largest in-flight `(table, xid)` stream — the best spill candidate.
    pub fn largest_open(&self) -> Option<(TableId, u32)> {
        self.by_stream
            .iter()
            .max_by_key(|(_, &bytes)| bytes)
            .map(|(&k, _)| k)
    }
}

/// What to do when the ceiling is crossed — cheapest correctness-free move first.
#[derive(Debug, PartialEq, Eq)]
pub enum ShedAction {
    /// Normal path: frees memory AND may advance the slot (to the open-txn floor).
    FlushCommitted,
    /// Speculative S3 staging of an open txn's buffer — frees memory, slot NOT advanced.
    SpillOpenTxn(TableId, u32),
    /// Reactive backstop: stop requesting WAL until memory drains.
    PausePoll,
}

/// Decide the shed action when over the ceiling: committed first (if any), then spill the largest open
/// stream, then pause. `None` when under the ceiling.
pub fn decide(meter: &InflightMeter, has_committed: bool) -> Option<ShedAction> {
    if !meter.over_ceiling() {
        return None;
    }
    if has_committed {
        return Some(ShedAction::FlushCommitted);
    }
    match meter.largest_open() {
        Some((t, x)) => Some(ShedAction::SpillOpenTxn(t, x)),
        None => Some(ShedAction::PausePoll),
    }
}

/// Hysteresis so the pause-poll backstop doesn't flap around the ceiling: pause at the high `activate`
/// ratio, resume only at the lower `resume` ratio.
#[derive(Debug)]
pub struct Backpressure {
    activate_ratio: f64,
    resume_ratio: f64,
    paused: bool,
}

impl Backpressure {
    pub fn new(activate_ratio: f64, resume_ratio: f64) -> Self {
        Backpressure {
            activate_ratio,
            resume_ratio,
            paused: false,
        }
    }

    /// Update from the current total vs ceiling; returns whether intake should be PAUSED afterwards.
    pub fn tick(&mut self, total: u64, ceiling: u64) -> bool {
        let ratio = if ceiling == 0 {
            f64::INFINITY
        } else {
            total as f64 / ceiling as f64
        };
        if self.paused {
            if ratio <= self.resume_ratio {
                self.paused = false;
            }
        } else if ratio >= self.activate_ratio {
            self.paused = true;
        }
        self.paused
    }

    pub fn is_paused(&self) -> bool {
        self.paused
    }
}

#[cfg(test)]
#[path = "memory_test.rs"]
mod tests;
