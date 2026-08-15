//! Micro-batching + cadence flush triggers (§1.3, §1.6).
//!
//! Accumulate decoded changes into a per-table Arrow builder and decide *when* to cut a file. A batch
//! flushes when **any** threshold trips — `max_fill` (cadence), `max_rows`, or `max_bytes` — but
//! **never in the middle of a committed transaction's tail**: rows buffer against the open txn and
//! become flush-eligible only at `Commit`, so a batch may span many small txns but never a fraction of
//! one (§1.6). This PR seals an in-memory `RecordBatch`; the Parquet/S3 write is PR 2.24.
//!
//! `lsn_end` is the **commit LSN** of the batch's last transaction — the load-bearing key for the
//! manifest (PR 2.25) and checkpoint (PR 2.26), and deliberately *not* the max per-row LSN.
//!
//! ## Dynamic dispatch in `pg-sink` — the deliberate list
//!
//! | Site | Dispatch | Why |
//! |---|---|---|
//! | `Clock` (this module) | **static** (`C: Clock`) | one production impl on a per-commit path |
//! | `Arc<dyn ObjectStore>` (`sink.rs`) | dynamic | backend is chosen from config at runtime |
//! | `Box<dyn ArrayBuilder>` (`pg-to-arrow`) | dynamic | one heterogeneous builder per column |

use crate::relcache::CachedRelation;
use arrow::record_batch::RecordBatch;
use common::{Lsn, SchemaVersionNo, SinkMeta, TupleValue, UtcTimestamp};
use pg_to_arrow::BatchBuilder;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod private {
    pub trait Sealed {}
}

/// Injectable clock so `max_fill` is testable without sleeping. Production has exactly one impl
/// ([`SystemClock`]); the trait exists **for that test seam**, not as dead generality (audited
/// PR 8.5, kept by design). Since PR 19.5 the seam is **statically dispatched**: every owner is
/// generic over `C: Clock`, so the one production instantiation inlines and no vtable rides the
/// per-commit path. `Arc<SystemClock>` / `Arc<FakeClock>` satisfy the bound via the delegating impls
/// below (PR 19.4).
///
/// This trait is sealed: it can be called and stored from anywhere, but only `pg-sink` can implement
/// it.
///
/// ```compile_fail
/// use std::time::Instant;
///
/// #[derive(Debug)]
/// struct WallClock;
///
/// impl pg_sink::batch::Clock for WallClock {
///     fn now(&self) -> Instant {
///         Instant::now()
///     }
/// }
/// ```
pub trait Clock: private::Sealed + Send + Sync + fmt::Debug {
    fn now(&self) -> Instant;

    /// Return the instant `after` from now, or `None` if it would overflow [`Instant`].
    ///
    /// This generic convenience method is available to concrete clocks. The `Self: Sized` gate
    /// excludes it from the vtable so [`Clock`] remains dyn-compatible.
    fn deadline<D: Into<Duration>>(&self, after: D) -> Option<Instant>
    where
        Self: Sized,
    {
        self.now().checked_add(after.into())
    }
}

/// Dyn-compatibility guard. Adding an ungated generic method, a bare-`Self` return, or an
/// associated const to [`Clock`] breaks compilation here, which is exactly the intent.
const _: fn(&dyn Clock) -> std::time::Instant = |c| c.now();

/// The wall clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl private::Sealed for SystemClock {}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Delegating wrapper impls preserve the private seal while allowing a shared or borrowed clock to
/// satisfy a `C: Clock` bound — the std pattern (`impl<R: Read + ?Sized> Read for &mut R`). Walrus
/// clocks are held behind an `Arc` because batchers share one, so without these impls the generic
/// form is unusable.
///
/// `?Sized` is load-bearing: it keeps a trait-object clock behind `Arc` covered. Both `Clock` impls
/// target wrapper types; a bare `impl<T: Clock> Clock for T` would collide with
/// `impl Clock for SystemClock` (E0119).
impl<T: Clock + ?Sized> private::Sealed for std::sync::Arc<T> {}

impl<T: Clock + ?Sized> Clock for std::sync::Arc<T> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

impl<T: Clock + ?Sized> private::Sealed for &T {}

impl<T: Clock + ?Sized> Clock for &T {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

/// The three per-batch flush triggers. Whichever trips first (at a commit boundary) cuts the file.
#[derive(Clone, Copy, Debug)]
pub struct BatchTriggers {
    pub max_fill: Duration,
    pub max_rows: NonZeroU64,
    pub max_bytes: NonZeroU64,
}

/// A finished, ready-to-write batch. `lsn_end` = commit LSN of the last txn (NOT the max row LSN).
#[derive(Debug)]
pub struct SealedBatch {
    pub record_batch: RecordBatch,
    pub schema: String,
    pub table: String,
    pub schema_version: SchemaVersionNo,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub row_count: u64,
}

/// Accumulates one table's committed changes into an Arrow builder until a trigger trips.
#[derive(Debug)]
pub struct TableBatcher<C> {
    rel: Arc<CachedRelation>,
    triggers: BatchTriggers,
    clock: C,
    /// Committed (flush-eligible) rows.
    builder: BatchBuilder,
    /// Rows of the currently-open transaction — not yet flush-eligible.
    pending: Vec<(SinkMeta, Vec<TupleValue>)>,
    pending_bytes: u64,
    committed_rows: u64,
    committed_bytes: u64,
    /// Commit LSN of the batch's first / last committed txn. `None` until the first commit.
    first_commit_lsn: Option<Lsn>,
    last_commit_lsn: Option<Lsn>,
    /// When the first committed row landed (drives `max_fill`).
    opened_at: Option<Instant>,
    /// The file id shared by every row of this batch (assigned when it opens; the manifest, PR 2.25,
    /// keys on it). `None` until the first row is pushed.
    batch_id: Option<String>,
}

impl<C: Clock> TableBatcher<C> {
    /// Create an empty batcher for one cached relation.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::Arrow`] if the relation cannot produce a supported Arrow schema or
    /// typed builder set.
    pub fn new(
        rel: Arc<CachedRelation>,
        triggers: BatchTriggers,
        clock: C,
    ) -> Result<Self, BatchError> {
        let builder = BatchBuilder::new(&rel.relation)?;
        Ok(TableBatcher {
            rel,
            triggers,
            clock,
            builder,
            pending: Vec::new(),
            pending_bytes: 0,
            committed_rows: 0,
            committed_bytes: 0,
            first_commit_lsn: None,
            last_commit_lsn: None,
            opened_at: None,
            batch_id: None,
        })
    }

    /// Append one change to the OPEN txn buffer (not yet flush-eligible). Its `meta.commit_lsn` and
    /// `meta.batch_id` are patched at [`Self::on_commit`].
    pub fn push(&mut self, meta: SinkMeta, values: &[TupleValue]) {
        self.batch_id.get_or_insert_with(|| {
            format!("{}.{}-{}", meta.source_schema, meta.source_table, meta.lsn)
        });
        self.pending_bytes += estimate_row_bytes(values);
        self.pending.push((meta, values.to_vec()));
    }

    /// Whether an open transaction's rows are buffered (not a commit boundary).
    #[must_use]
    pub fn has_open_txn(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Promote the open txn's rows to the committed builder at `(commit_lsn, commit_ts)`; they are now
    /// flush-eligible. `commit_lsn` and `commit_ts` are known only at Commit, so per-row metas were
    /// pushed with placeholders and get the real transaction values stamped here (PR 5.9). A commit
    /// with no rows for this table is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::Unassigned`] if buffered rows have no assigned batch id, or
    /// [`BatchError::Arrow`] if a buffered value or its provenance cannot be appended to the
    /// relation's Arrow builders.
    pub fn on_commit(
        &mut self,
        commit_lsn: Lsn,
        commit_ts: UtcTimestamp,
    ) -> Result<(), BatchError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let batch_id = self.batch_id.clone().ok_or(BatchError::Unassigned)?;
        if self.opened_at.is_none() {
            self.opened_at = Some(self.clock.now());
        }
        self.first_commit_lsn.get_or_insert(commit_lsn);
        self.last_commit_lsn = Some(commit_lsn);
        // Draining keeps the allocation so the next transaction refills the same pending buffer.
        for (mut meta, values) in self.pending.drain(..) {
            meta.commit_lsn = commit_lsn;
            meta.commit_ts = commit_ts;
            meta.batch_id.clone_from(&batch_id);
            self.builder.append_row(&values, &meta)?;
            self.committed_rows += 1;
        }
        self.committed_bytes += std::mem::take(&mut self.pending_bytes);
        Ok(())
    }

    /// True iff a trigger trips **and** we're at a commit boundary (no open txn, ≥1 committed row).
    #[must_use]
    pub fn should_flush(&self) -> bool {
        if self.has_open_txn() || self.committed_rows == 0 {
            return false;
        }
        self.committed_rows >= self.triggers.max_rows.get()
            || self.committed_bytes >= self.triggers.max_bytes.get()
            || self.opened_at.is_some_and(|t| {
                self.clock.now().saturating_duration_since(t) >= self.triggers.max_fill
            })
    }

    /// Finish the Arrow builders into a `SealedBatch` and reset. Errors if an open txn would be split.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::OpenTransaction`] if uncommitted rows remain,
    /// [`BatchError::Empty`] if no committed rows exist, [`BatchError::Unassigned`] if committed
    /// rows have no assigned id or LSN bounds, or [`BatchError::Arrow`] if finishing or rebuilding
    /// the typed Arrow batch fails.
    pub fn seal(&mut self) -> Result<SealedBatch, BatchError> {
        if self.has_open_txn() {
            return Err(BatchError::OpenTransaction);
        }
        if self.committed_rows == 0 {
            return Err(BatchError::Empty);
        }
        let (Some(_batch_id), Some(lsn_start), Some(lsn_end)) = (
            self.batch_id.as_deref(),
            self.first_commit_lsn,
            self.last_commit_lsn,
        ) else {
            return Err(BatchError::Unassigned);
        };
        let builder = std::mem::replace(&mut self.builder, BatchBuilder::new(&self.rel.relation)?);
        let record_batch = builder.finish()?;
        let sealed = SealedBatch {
            record_batch,
            schema: self.rel.relation.schema.clone(),
            table: self.rel.relation.name.clone(),
            schema_version: self.rel.schema_version,
            lsn_start,
            lsn_end,
            row_count: self.committed_rows,
        };
        self.committed_rows = 0;
        self.committed_bytes = 0;
        self.first_commit_lsn = None;
        self.last_commit_lsn = None;
        self.opened_at = None;
        self.batch_id = None;
        Ok(sealed)
    }

    #[must_use]
    pub fn committed_rows(&self) -> u64 {
        self.committed_rows
    }

    /// The commit LSN of the earliest committed-but-unsealed row, or `None` if nothing is buffered.
    /// The durability floor an idle heartbeat must not advance `confirmed_flush` past (PR 2.27): those
    /// rows are not yet in S3, so a slot advance beyond them would lose them on crash. Open-txn
    /// (uncommitted) rows do **not** count — their future commit LSN re-streams regardless.
    #[must_use]
    pub fn undurable_floor(&self) -> Option<Lsn> {
        (self.committed_rows > 0)
            .then_some(self.first_commit_lsn)
            .flatten()
    }

    /// **Drop** the open (uncommitted) transaction's speculative buffer — on a graceful drain (PR 2.28)
    /// these have no `Commit` yet, so forcing them out would orphan an S3 object with no way to resolve
    /// it; they simply re-stream on resume (at-least-once). Committed rows are untouched.
    pub fn drop_open_txn(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
    }

    /// Seal the in-flight **committed** batch on drain: drop any open speculative buffer first, then
    /// seal iff there are committed rows. `None` when nothing committed is in flight.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::Unassigned`] for inconsistent committed state or the
    /// [`BatchError::Arrow`] produced while sealing committed rows. The open transaction is
    /// deliberately discarded first, so [`BatchError::OpenTransaction`] is not expected here.
    pub fn drain_committed(&mut self) -> Result<Option<SealedBatch>, BatchError> {
        self.drop_open_txn();
        if self.committed_rows == 0 {
            return Ok(None);
        }
        self.seal().map(Some)
    }
}

/// A rough running byte estimate of the buffered Arrow size (not the compressed Parquet size, which
/// isn't known until write) — enough to drive the `max_bytes` trigger.
fn estimate_row_bytes(values: &[TupleValue]) -> u64 {
    const META_OVERHEAD: u64 = 96; // the walrus_pg_sink_meta JSON per row, roughly
    let value_bytes: u64 = values
        .iter()
        .map(|v| match v {
            TupleValue::Text(s) => u64::try_from(s.len()).unwrap_or(u64::MAX),
            TupleValue::Binary(b) => u64::try_from(b.len()).unwrap_or(u64::MAX),
            TupleValue::Null | TupleValue::UnchangedToast => 1,
        })
        .sum();
    META_OVERHEAD + value_bytes
}

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BatchError {
    #[error("cannot seal mid-transaction (would split a committed txn tail)")]
    OpenTransaction,
    #[error("nothing to seal (empty batch)")]
    Empty,
    /// Committed rows exist but the batch id or its commit-LSN bounds were never assigned.
    #[error("batch has committed rows but no assigned batch id / commit-LSN bounds")]
    Unassigned,
    #[error(transparent)]
    Arrow(#[from] pg_to_arrow::Error),
}

#[cfg(test)]
#[path = "batch_test.rs"]
mod tests;
