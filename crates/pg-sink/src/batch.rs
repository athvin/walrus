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

use crate::relcache::CachedRelation;
use arrow::record_batch::RecordBatch;
use common::{Lsn, SinkMeta, TupleValue};
use pg_to_arrow::BatchBuilder;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Injectable clock so `max_fill` is testable without sleeping. Production uses [`SystemClock`].
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// The wall clock.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// The three per-batch flush triggers. Whichever trips first (at a commit boundary) cuts the file.
#[derive(Clone, Copy, Debug)]
pub struct BatchTriggers {
    pub max_fill: Duration,
    pub max_rows: u64,
    pub max_bytes: u64,
}

/// A finished, ready-to-write batch. `lsn_end` = commit LSN of the last txn (NOT the max row LSN).
#[derive(Debug)]
pub struct SealedBatch {
    pub record_batch: RecordBatch,
    pub schema: String,
    pub table: String,
    pub schema_version: i64,
    pub lsn_start: Lsn,
    pub lsn_end: Lsn,
    pub row_count: u64,
}

/// Accumulates one table's committed changes into an Arrow builder until a trigger trips.
pub struct TableBatcher {
    rel: Arc<CachedRelation>,
    triggers: BatchTriggers,
    clock: Arc<dyn Clock>,
    /// Committed (flush-eligible) rows.
    builder: BatchBuilder,
    /// Rows of the currently-open transaction — not yet flush-eligible.
    pending: Vec<(SinkMeta, Vec<TupleValue>)>,
    pending_bytes: u64,
    committed_rows: u64,
    committed_bytes: u64,
    /// Commit LSN of the batch's first / last committed txn.
    first_commit_lsn: Option<Lsn>,
    last_commit_lsn: Lsn,
    /// When the first committed row landed (drives `max_fill`).
    opened_at: Option<Instant>,
    /// The file id shared by every row of this batch (assigned when it opens; the manifest, PR 2.25,
    /// keys on it). Empty until the first row is pushed.
    batch_id: String,
}

impl TableBatcher {
    pub fn new(
        rel: Arc<CachedRelation>,
        triggers: BatchTriggers,
        clock: Arc<dyn Clock>,
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
            last_commit_lsn: Lsn::ZERO,
            opened_at: None,
            batch_id: String::new(),
        })
    }

    /// Append one change to the OPEN txn buffer (not yet flush-eligible). Its `meta.commit_lsn` and
    /// `meta.batch_id` are patched at [`Self::on_commit`].
    pub fn push(&mut self, meta: SinkMeta, values: &[TupleValue]) {
        if self.batch_id.is_empty() {
            // Assign the file id when the batch opens; every row shares it.
            self.batch_id = format!("{}.{}-{}", meta.source_schema, meta.source_table, meta.lsn);
        }
        self.pending_bytes += estimate_row_bytes(values);
        self.pending.push((meta, values.to_vec()));
    }

    /// Whether an open transaction's rows are buffered (not a commit boundary).
    pub fn has_open_txn(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Promote the open txn's rows to the committed builder at `commit_lsn`; they are now
    /// flush-eligible. A commit with no rows for this table is a no-op.
    pub fn on_commit(&mut self, commit_lsn: Lsn) -> Result<(), BatchError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.opened_at.is_none() {
            self.opened_at = Some(self.clock.now());
        }
        self.first_commit_lsn.get_or_insert(commit_lsn);
        self.last_commit_lsn = commit_lsn;
        for (mut meta, values) in std::mem::take(&mut self.pending) {
            meta.commit_lsn = commit_lsn;
            meta.batch_id = self.batch_id.clone();
            self.builder.append_row(&values, &meta)?;
            self.committed_rows += 1;
        }
        self.committed_bytes += std::mem::take(&mut self.pending_bytes);
        Ok(())
    }

    /// True iff a trigger trips **and** we're at a commit boundary (no open txn, ≥1 committed row).
    pub fn should_flush(&self) -> bool {
        if self.has_open_txn() || self.committed_rows == 0 {
            return false;
        }
        self.committed_rows >= self.triggers.max_rows
            || self.committed_bytes >= self.triggers.max_bytes
            || self.opened_at.is_some_and(|t| {
                self.clock.now().saturating_duration_since(t) >= self.triggers.max_fill
            })
    }

    /// Finish the Arrow builders into a `SealedBatch` and reset. Errors if an open txn would be split.
    pub fn seal(&mut self) -> Result<SealedBatch, BatchError> {
        if self.has_open_txn() {
            return Err(BatchError::OpenTransaction);
        }
        if self.committed_rows == 0 {
            return Err(BatchError::Empty);
        }
        let builder = std::mem::replace(&mut self.builder, BatchBuilder::new(&self.rel.relation)?);
        let record_batch = builder.finish()?;
        let sealed = SealedBatch {
            record_batch,
            schema: self.rel.relation.schema.clone(),
            table: self.rel.relation.name.clone(),
            schema_version: self.rel.schema_version,
            lsn_start: self.first_commit_lsn.unwrap_or(Lsn::ZERO),
            lsn_end: self.last_commit_lsn,
            row_count: self.committed_rows,
        };
        self.committed_rows = 0;
        self.committed_bytes = 0;
        self.first_commit_lsn = None;
        self.last_commit_lsn = Lsn::ZERO;
        self.opened_at = None;
        self.batch_id = String::new();
        Ok(sealed)
    }

    pub fn committed_rows(&self) -> u64 {
        self.committed_rows
    }

    /// The commit LSN of the earliest committed-but-unsealed row, or `None` if nothing is buffered.
    /// The durability floor an idle heartbeat must not advance `confirmed_flush` past (PR 2.27): those
    /// rows are not yet in S3, so a slot advance beyond them would lose them on crash. Open-txn
    /// (uncommitted) rows do **not** count — their future commit LSN re-streams regardless.
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
            TupleValue::Text(s) => s.len() as u64,
            TupleValue::Binary(b) => b.len() as u64,
            TupleValue::Null | TupleValue::UnchangedToast => 1,
        })
        .sum();
    META_OVERHEAD + value_bytes
}

#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("cannot seal mid-transaction (would split a committed txn tail)")]
    OpenTransaction,
    #[error("nothing to seal (empty batch)")]
    Empty,
    #[error(transparent)]
    Arrow(#[from] pg_to_arrow::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relcache::RelationCache;
    use common::{Kind, Op, PgColumn, PgRelation, ReplicaIdentity, UtcTimestamp};
    use pg_to_arrow::oids;
    use std::sync::Mutex;

    /// A hand-advanced clock for the `max_fill` test.
    struct FakeClock {
        base: Instant,
        offset: Mutex<Duration>,
    }
    impl FakeClock {
        fn new() -> Arc<Self> {
            Arc::new(FakeClock {
                base: Instant::now(),
                offset: Mutex::new(Duration::ZERO),
            })
        }
        fn advance(&self, d: Duration) {
            *self.offset.lock().unwrap() += d;
        }
    }
    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            self.base + *self.offset.lock().unwrap()
        }
    }

    fn cached() -> Arc<CachedRelation> {
        let rel = PgRelation {
            oid: 42,
            schema: "public".to_string(),
            name: "widgets".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                PgColumn {
                    name: "id".into(),
                    type_oid: oids::INT4,
                    type_modifier: -1,
                    is_key: true,
                },
                PgColumn {
                    name: "note".into(),
                    type_oid: oids::TEXT,
                    type_modifier: -1,
                    is_key: false,
                },
            ],
        };
        RelationCache::default()
            .upsert_from_relation(rel, 1)
            .unwrap()
    }

    fn meta(lsn: &str) -> SinkMeta {
        SinkMeta {
            op: Op::Insert,
            lsn: lsn.parse().unwrap(),
            commit_lsn: Lsn::ZERO, // patched at on_commit
            commit_ts: UtcTimestamp::parse_rfc3339("2026-07-07T12:00:00Z").unwrap(),
            xid: 7,
            epoch: 1,
            batch_id: "b1".into(),
            schema_version: 1,
            source_schema: "public".into(),
            source_table: "widgets".into(),
            kind: Kind::Stream,
            unchanged_toast: vec![],
            sink_instance: "walrus-pg-sink-0".into(),
            sink_processed_at: UtcTimestamp::parse_rfc3339("2026-07-07T12:00:00Z").unwrap(),
        }
    }

    fn row(id: &str) -> Vec<TupleValue> {
        vec![TupleValue::Text(id.into()), TupleValue::Text("hi".into())]
    }

    fn triggers(max_rows: u64, max_bytes: u64, max_fill: Duration) -> BatchTriggers {
        BatchTriggers {
            max_rows,
            max_bytes,
            max_fill,
        }
    }

    #[test]
    fn flushes_on_row_count_at_commit_boundary() {
        let mut b = TableBatcher::new(
            cached(),
            triggers(2, u64::MAX, Duration::from_secs(3600)),
            Arc::new(SystemClock),
        )
        .unwrap();
        b.push(meta("0/10"), &row("1"));
        b.push(meta("0/20"), &row("2"));
        assert!(!b.should_flush(), "open txn is never flush-eligible");
        b.on_commit("0/30".parse().unwrap()).unwrap();
        assert!(b.should_flush(), "2 committed rows hit max_rows=2");
        let sealed = b.seal().unwrap();
        assert_eq!(sealed.row_count, 2);
        assert!(!b.should_flush(), "reset after seal");
    }

    #[test]
    fn flushes_on_byte_size_at_commit_boundary() {
        let mut b = TableBatcher::new(
            cached(),
            triggers(u64::MAX, 50, Duration::from_secs(3600)),
            Arc::new(SystemClock),
        )
        .unwrap();
        // One row (~96 overhead + a few value bytes) exceeds the tiny 50-byte ceiling.
        b.push(meta("0/10"), &row("1"));
        b.on_commit("0/30".parse().unwrap()).unwrap();
        assert!(b.should_flush(), "committed bytes exceed max_bytes=50");
    }

    #[test]
    fn flushes_on_max_fill_via_fake_clock() {
        let clock = FakeClock::new();
        let mut b = TableBatcher::new(
            cached(),
            triggers(u64::MAX, u64::MAX, Duration::from_millis(100)),
            clock.clone(),
        )
        .unwrap();
        b.push(meta("0/10"), &row("1"));
        b.on_commit("0/30".parse().unwrap()).unwrap();
        assert!(!b.should_flush(), "no wall time has elapsed yet");
        clock.advance(Duration::from_millis(150));
        assert!(b.should_flush(), "max_fill tripped via the fake clock");
    }

    #[test]
    fn never_seals_with_an_open_transaction() {
        let mut b = TableBatcher::new(
            cached(),
            triggers(1, u64::MAX, Duration::from_secs(3600)),
            Arc::new(SystemClock),
        )
        .unwrap();
        b.push(meta("0/10"), &row("1")); // open txn, no commit
        assert!(matches!(b.seal(), Err(BatchError::OpenTransaction)));
    }

    #[test]
    fn drain_seals_committed_rows_and_drops_the_open_txn() {
        let mut b = TableBatcher::new(
            cached(),
            triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)), // never auto-flushes
            Arc::new(SystemClock),
        )
        .unwrap();
        // A committed txn (flush-eligible, but under all thresholds) plus an OPEN speculative txn.
        b.push(meta("0/10"), &row("1"));
        b.on_commit("0/20".parse().unwrap()).unwrap();
        b.push(meta("0/30"), &row("2")); // open, uncommitted
        assert!(b.has_open_txn());
        let sealed = b
            .drain_committed()
            .unwrap()
            .expect("committed rows seal on drain");
        assert_eq!(sealed.row_count, 1, "only the committed row is sealed");
        assert_eq!(sealed.lsn_end, "0/20".parse().unwrap());
        assert!(
            !b.has_open_txn(),
            "the open speculative buffer was dropped, not forced out"
        );
        assert_eq!(b.committed_rows(), 0, "batch reset after drain");
    }

    #[test]
    fn drain_with_nothing_committed_is_a_noop() {
        let mut b = TableBatcher::new(
            cached(),
            triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
            Arc::new(SystemClock),
        )
        .unwrap();
        b.push(meta("0/10"), &row("1")); // open only, never committed
        assert!(
            b.drain_committed().unwrap().is_none(),
            "no committed rows → nothing to seal"
        );
        assert!(!b.has_open_txn(), "the open buffer is still dropped");
    }

    #[test]
    fn lsn_end_equals_last_commit_lsn_not_max_row_lsn() {
        let mut b = TableBatcher::new(
            cached(),
            triggers(u64::MAX, u64::MAX, Duration::from_secs(3600)),
            Arc::new(SystemClock),
        )
        .unwrap();
        // Row LSNs are HIGHER than the commit LSN — lsn_end must still be the commit LSN.
        b.push(meta("0/500"), &row("1"));
        b.push(meta("0/600"), &row("2"));
        b.on_commit("0/100".parse().unwrap()).unwrap();
        let sealed = b.seal().unwrap();
        assert_eq!(
            sealed.lsn_end,
            "0/100".parse().unwrap(),
            "lsn_end is the commit LSN"
        );
        assert_eq!(sealed.lsn_start, "0/100".parse().unwrap());
    }
}
