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
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
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
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
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
    b.on_commit("0/30".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
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
    b.on_commit("0/20".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
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
    b.on_commit("0/100".parse().unwrap(), UtcTimestamp::now())
        .unwrap();
    let sealed = b.seal().unwrap();
    assert_eq!(
        sealed.lsn_end,
        "0/100".parse().unwrap(),
        "lsn_end is the commit LSN"
    );
    assert_eq!(sealed.lsn_start, "0/100".parse().unwrap());
}
