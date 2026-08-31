//! Aggregate, process-wide in-memory accounting + backpressure (§1.3). The per-batch `max_bytes`/
//! `max_rows` caps bound **one** batch; they do nothing to stop the *sum* of all in-flight
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

use std::collections::{BinaryHeap, HashMap};
use std::num::NonZeroU64;

/// A pg relation OID (a stable table id).
///
/// A newtype, not an alias: every in-flight stream is keyed by `(TableId, xid)` — two `u32`s that an
/// alias lets a caller transpose silently, which is precisely the mix-up this accounting cannot
/// detect (both halves are small opaque integers, and a swapped key simply meters the wrong stream).
/// The transparent representation keeps it exactly one `u32` wide inside the per-row
/// `StreamedChange` — asserted directly below, as every other transparent walrus id asserts its own
/// layout, and again indirectly by that row's move-cost budget in `stream_txn.rs`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u32);

const _: () =
    assert!(size_of::<TableId>() == size_of::<u32>() && align_of::<TableId>() == align_of::<u32>());

/// Aggregate, process-wide accounting across all `(table, xid)` Arrow builders — distinct from any
/// single batch's `max_bytes`.
#[derive(Debug)]
pub struct InflightMeter {
    ceiling_bytes: NonZeroU64,
    total: u64,
    by_stream: HashMap<(TableId, u32), u64>,
}

impl InflightMeter {
    /// An empty meter with the ceiling shedding is measured against.
    ///
    /// `NonZeroU64` rather than `u64` because a zero ceiling would put the meter permanently over
    /// the limit before a single row arrived; the config layer rejects it, and the type keeps that
    /// rejection from having to be re-checked here.
    #[must_use]
    pub fn new(ceiling_bytes: NonZeroU64) -> Self {
        InflightMeter {
            ceiling_bytes,
            total: 0,
            by_stream: HashMap::new(),
        }
    }

    /// Account `bytes` more buffered for `(table, xid)`.
    ///
    /// This gauge saturates because its consumer only needs to know whether memory is over the
    /// ceiling. At the integer bound, `u64::MAX` preserves that answer; returning an error would
    /// leave the shedding caller with no more accurate value to record.
    pub fn add(&mut self, key: (TableId, u32), bytes: u64) {
        let stream = self.by_stream.entry(key).or_insert(0);
        *stream = stream.saturating_add(bytes);
        self.total = self.total.saturating_add(bytes);
    }

    /// Drop all accounting for `(table, xid)` (its buffer was flushed or spilled).
    ///
    /// The normal path clamps at zero rather than wrapping. Once the aggregate reaches `u64::MAX`,
    /// it no longer records the overflow amount, so release recomputes the saturating sum of the
    /// remaining streams to keep the gauge conservative.
    pub fn release(&mut self, key: (TableId, u32)) {
        if let Some(bytes) = self.by_stream.remove(&key) {
            self.total = if self.total == u64::MAX {
                self.by_stream
                    .values()
                    .fold(0_u64, |total, &stream| total.saturating_add(stream))
            } else {
                self.total.saturating_sub(bytes)
            };
        }
    }

    /// Aggregate buffered bytes across every open stream. Saturating, so at `u64::MAX` this is a
    /// floor on the true figure rather than the figure itself — which is all the ceiling test needs.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.total
    }

    /// The configured ceiling this meter was built with. Constant for the meter's lifetime.
    #[must_use]
    pub const fn ceiling(&self) -> NonZeroU64 {
        self.ceiling_bytes
    }

    /// Whether the aggregate has passed the ceiling — the single question shedding is driven by.
    #[must_use = "the ceiling check drives shedding — ignoring it silently disables backpressure"]
    pub const fn is_over_ceiling(&self) -> bool {
        self.total > self.ceiling_bytes.get()
    }

    /// A one-shot snapshot of spill candidates, largest first: `(bytes, table_id, xid)`.
    ///
    /// `by_stream` remains the authoritative accounting store. This heap is built for one shed
    /// episode, drained by it, and dropped; it is never kept in sync with [`Self::add`] or
    /// [`Self::release`], because `BinaryHeap` supports neither priority updates nor arbitrary
    /// removal. Inside the drain loop priorities only ever fall: it calls `release`, never `add`.
    /// A popped candidate is therefore only a hint that the caller re-validates against the live
    /// meter and owner index.
    ///
    /// Tuple ordering provides a deterministic total order: bytes, then table id, then xid.
    ///
    /// Allocates and heapifies one entry per open stream — hence `to_`: build it once per shed
    /// episode, never per candidate.
    #[must_use = "the heap snapshot must be drained to select spill candidates"]
    pub fn to_spill_order(&self) -> BinaryHeap<(u64, TableId, u32)> {
        self.by_stream
            .iter()
            .map(|(&(table_id, xid), &bytes)| (bytes, table_id, xid))
            .collect()
    }

    /// The largest in-flight `(table, xid)` stream — the best spill candidate. Uses the same
    /// deterministic tie-break as [`Self::to_spill_order`].
    #[must_use]
    pub fn largest_open(&self) -> Option<(TableId, u32)> {
        self.by_stream
            .iter()
            .map(|(&(table_id, xid), &bytes)| (bytes, table_id, xid))
            .max()
            .map(|(_bytes, table_id, xid)| (table_id, xid))
    }
}

/// What to do when the ceiling is crossed — cheapest correctness-free move first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[must_use]
pub fn decide(meter: &InflightMeter, has_committed: bool) -> Option<ShedAction> {
    if !meter.is_over_ceiling() {
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

/// A ratio in the open unit interval `(0.0, 1.0)`. Constructed only through [`Ratio::new`], so a
/// `Ratio` in hand is always in range — the check happens once, at the edge.
///
/// `#[serde(try_from = "f64")]` makes the deserializer that same edge instead of a second one: serde
/// reads a bare JSON/TOML/environment number (the wire shape stays numeric) and hands it to
/// [`TryFrom<f64>`], so an out-of-range knob is a config-load failure rather than a `Ratio` that
/// exists — however briefly — outside its interval and poisons the backpressure comparisons. There
/// is no paired `into = "f64"`: nothing serializes a walrus config back out.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, serde::Deserialize)]
#[serde(try_from = "f64")]
pub struct Ratio(f64);

/// Why a raw `f64` was rejected as a [`Ratio`].
///
/// A candidate is either not finite or finite-and-outside the open unit interval, so these two
/// variants are the whole taxonomy and this enum is deliberately exhaustive — no `#[non_exhaustive]`.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum RatioError {
    /// A special IEEE-754 value would poison the backpressure comparisons.
    #[error("ratio {0} is not finite — NaN and infinities disable the backstop")]
    NonFinite(f64),
    /// A finite value lies outside the open unit interval.
    #[error("ratio {0} is out of range — require 0.0 < r < 1.0")]
    OutOfRange(f64),
}

impl Ratio {
    /// Parse a raw ratio. `NaN`, `0.0`, `1.0` and anything outside the open interval are rejected.
    ///
    /// Callable in a const initializer, so a compile-time ratio gets the same edge check an
    /// operator-supplied one does. `clippy::missing_const_for_fn` does not ask for this `const` —
    /// its const model still rejects every float operation — but `f64::is_finite` and the two
    /// primitive comparisons below are const-stable at this workspace's 1.95 MSRV.
    ///
    /// # Errors
    ///
    /// Returns [`RatioError::NonFinite`] for `NaN` or either infinity, and
    /// [`RatioError::OutOfRange`] unless a finite value is strictly between zero and one.
    pub const fn new(raw: f64) -> Result<Self, RatioError> {
        if !raw.is_finite() {
            return Err(RatioError::NonFinite(raw));
        }
        if 0.0 < raw && raw < 1.0 {
            Ok(Ratio(raw))
        } else {
            Err(RatioError::OutOfRange(raw))
        }
    }

    /// The validated ratio as a plain `f64`, for the one arithmetic comparison it exists for.
    #[must_use]
    pub const fn as_f64(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for Ratio {
    type Error = RatioError;

    /// The operator-facing spelling of [`Ratio::new`], and the conversion the derived
    /// `try_from = "f64"` deserializer runs — one range check serving both paths.
    ///
    /// # Errors
    ///
    /// Returns [`RatioError::NonFinite`] for `NaN` or either infinity, and
    /// [`RatioError::OutOfRange`] unless a finite `raw` is strictly between zero and one.
    fn try_from(raw: f64) -> Result<Self, RatioError> {
        Ratio::new(raw)
    }
}

/// The pause/resume band for the pause-poll backstop: pause at `activate`, resume only at the lower
/// `resume`. The gap is what stops intake flapping around the ceiling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HysteresisBand {
    activate: Ratio,
    resume: Ratio,
}

/// Why two in-range ratios still did not form a band.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("require 0 < resume ({resume}) < activate ({activate}) < 1.0")]
pub struct BandError {
    /// The rejected activation ratio, as supplied.
    pub activate: f64,
    /// The rejected resume ratio, as supplied. Kept alongside `activate` because the constraint is
    /// between the two, so neither number explains the rejection alone.
    pub resume: f64,
}

impl HysteresisBand {
    /// The shipped default band. Its private fields cannot bypass the constructor elsewhere.
    pub const DEFAULT: HysteresisBand = HysteresisBand {
        activate: Ratio(0.85),
        resume: Ratio(0.75),
    };

    /// Build a band whose resume threshold is strictly below its activation threshold.
    ///
    /// Not `const`, unlike [`Ratio::new`]: the ordering test is [`Ratio`]'s derived
    /// [`PartialOrd`], and a trait method is not callable in a const context on stable.
    ///
    /// # Errors
    ///
    /// Returns [`BandError`] when `resume` is greater than or equal to `activate`.
    pub fn new(activate: Ratio, resume: Ratio) -> Result<Self, BandError> {
        if resume < activate {
            Ok(HysteresisBand { activate, resume })
        } else {
            Err(BandError {
                activate: activate.as_f64(),
                resume: resume.as_f64(),
            })
        }
    }

    /// The high ratio at which intake pauses.
    #[must_use]
    pub const fn activate(self) -> Ratio {
        self.activate
    }

    /// The low ratio at which intake resumes. Strictly below [`activate`](Self::activate) — that
    /// gap is the hysteresis.
    #[must_use]
    pub const fn resume(self) -> Ratio {
        self.resume
    }
}

/// Hysteresis so the pause-poll backstop doesn't flap around the ceiling: pause at the high `activate`
/// ratio, resume only at the lower `resume` ratio.
#[allow(
    missing_copy_implementations,
    reason = "copying this mutable hysteresis state could silently detach pause transitions"
)]
#[derive(Debug)]
pub struct Backpressure {
    band: HysteresisBand,
    paused: bool,
}

impl Backpressure {
    /// A backpressure latch over `band`, starting un-paused.
    #[must_use]
    pub const fn new(band: HysteresisBand) -> Self {
        Backpressure {
            band,
            paused: false,
        }
    }

    /// Update from the current total vs ceiling; returns whether intake should be PAUSED afterwards.
    /// The non-zero ceiling makes the ratio total, so this path needs no divide-by-zero fallback.
    ///
    /// `const` for the same reason [`Ratio::new`] is: the float division and comparisons are what
    /// clippy's const model cannot see through, not something the compiler refuses to evaluate.
    pub const fn tick(&mut self, total: u64, ceiling: NonZeroU64) -> bool {
        let ratio = total as f64 / ceiling.get() as f64;
        if self.paused {
            if ratio <= self.band.resume().as_f64() {
                self.paused = false;
            }
        } else if ratio >= self.band.activate().as_f64() {
            self.paused = true;
        }
        self.paused
    }

    /// The latch's current state, without advancing it. Read this to decide whether to poll the
    /// replication stream; call [`tick`](Self::tick) to update it.
    #[must_use]
    pub const fn is_paused(&self) -> bool {
        self.paused
    }
}

#[cfg(test)]
#[path = "memory_test.rs"]
mod tests;
