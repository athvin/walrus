use super::*;
use crate::batch::SystemClock;
use common::{PgColumn, PgRelation, ReplicaIdentity};
use pg_to_arrow::oids;
use std::num::NonZeroU64;
use std::time::Duration;

fn nz(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

fn cache() -> RelationCache {
    let rel = PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
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
    let mut c = RelationCache::default();
    c.upsert_from_relation(rel, common::SchemaVersionNo(1))
        .unwrap();
    c
}

fn insert_id(id: i32, sub_xid: u32) -> Message {
    Message::Insert {
        xid: Some(sub_xid),
        relation_oid: 42,
        new: vec![
            TupleValue::Text(id.to_string()),
            TupleValue::Text("n".into()),
        ],
    }
}

fn demux(ceiling: u64) -> StreamDemux {
    StreamDemux::new(
        BatchTriggers {
            max_rows: nz(100_000),
            max_bytes: NonZeroU64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        common::EpochNo(1),
        "test",
        nz(ceiling),
    )
}

fn mem_sink() -> ParquetSink {
    ParquetSink::new(
        Arc::new(object_store::memory::InMemory::new()),
        "walrus".into(),
        common::EpochNo(1),
    )
}

#[tokio::test]
async fn spill_resolves_the_owning_txn_without_scanning_buffered_changes() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(105_000);

    for (top, rows) in [(100_u32, 500_u32), (200, 550), (300, 500)] {
        d.on_stream_start(top, true, Lsn::new(u64::from(top)));
        for row in 0..rows {
            let values = vec![
                TupleValue::Text((top * 10_000 + row).to_string()),
                TupleValue::Text("n".into()),
            ];
            d.claim_stream((42, top), top, estimate_change_bytes(&values));
            d.open.get_mut(&top).unwrap().push_change(StreamedChange {
                sub_xid: top,
                oid: 42,
                op: Op::Insert,
                values,
                lsn: Lsn::new(u64::from(row)),
            });
        }
    }

    assert_eq!(d.owner_len(), 3);
    d.spill_if_over_ceiling(&cache, &sink).await.unwrap();

    assert_eq!(d.spill_count(), 1);
    assert_eq!(d.owner_len(), 2);
    assert_eq!(d.survivor_count(200), 0);
    for top in [100_u32, 300] {
        assert_eq!(d.survivor_count(top), 500);
        let lsns: Vec<Lsn> = d.open[&top]
            .changes
            .iter()
            .map(|change| change.lsn)
            .collect();
        assert_eq!(lsns, (0_u64..500).map(Lsn::new).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn owner_index_is_emptied_by_stream_commit() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100));
    for (id, sub_xid, lsn) in [(1, 100, Lsn::new(1)), (2, 101, Lsn::new(2))] {
        d.on_change(&cache, &insert_id(id, sub_xid), &sink, lsn)
            .await
            .unwrap();
    }
    assert_eq!(d.owner_len(), 2);

    d.on_stream_commit(100, Lsn::new(900), UtcTimestamp::now(), &cache, &sink)
        .await
        .unwrap();

    assert_eq!(d.owner_len(), 0);
}

#[tokio::test]
async fn owner_index_is_emptied_by_a_whole_txn_abort() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100));
    for (id, sub_xid, lsn) in [(1, 100, Lsn::new(1)), (2, 101, Lsn::new(2))] {
        d.on_change(&cache, &insert_id(id, sub_xid), &sink, lsn)
            .await
            .unwrap();
    }
    assert_eq!(d.owner_len(), 2);

    d.on_stream_abort(100, 100, &sink).await;

    assert_eq!(d.owner_len(), 0);
}

#[tokio::test]
async fn a_subtxn_abort_leaves_the_index_and_the_buffer_alone() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, Lsn::new(100));
    for (id, sub_xid, lsn) in [(1, 101, Lsn::new(1)), (2, 102, Lsn::new(2))] {
        d.on_change(&cache, &insert_id(id, sub_xid), &sink, lsn)
            .await
            .unwrap();
    }
    let owner_len = d.owner_len();
    let buffered_len = d.open[&100].changes.len();

    d.on_stream_abort(100, 101, &sink).await;

    assert_eq!(d.owner_len(), owner_len);
    assert_eq!(d.open[&100].changes.len(), buffered_len);
    assert_eq!(d.survivor_count(100), 1);
    let files = d
        .on_stream_commit(100, Lsn::new(900), UtcTimestamp::now(), &cache, &sink)
        .await
        .unwrap();
    assert_eq!(files.iter().map(|file| file.row_count).sum::<u64>(), 1);
    assert_eq!(d.owner_len(), 0);
}

#[tokio::test]
async fn demux_routes_interleaved_xids_to_their_buffers() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(u64::MAX); // no spill
    d.on_stream_start(100, true, "0/100".parse().unwrap());
    d.on_change(&cache, &insert_id(1, 100), &sink, "0/101".parse().unwrap())
        .await
        .unwrap();
    d.on_stream_stop();
    d.on_stream_start(200, true, "0/200".parse().unwrap());
    d.on_change(&cache, &insert_id(2, 200), &sink, "0/201".parse().unwrap())
        .await
        .unwrap();
    d.on_change(&cache, &insert_id(3, 200), &sink, "0/202".parse().unwrap())
        .await
        .unwrap();
    d.on_stream_stop();
    d.on_stream_start(100, false, "0/300".parse().unwrap());
    d.on_change(&cache, &insert_id(4, 100), &sink, "0/301".parse().unwrap())
        .await
        .unwrap();
    assert_eq!(d.survivor_count(100), 2);
    assert_eq!(d.survivor_count(200), 2);
}

#[test]
fn open_floor_is_oldest_open_txn_begin_lsn() {
    let mut d = demux(u64::MAX);
    assert_eq!(d.open_floor(), None);
    d.on_stream_start(100, true, "0/500".parse().unwrap());
    d.on_stream_start(200, true, "0/900".parse().unwrap());
    assert_eq!(d.open_floor(), Some("0/500".parse().unwrap()));
}

#[tokio::test]
async fn stream_commit_materialises_survivors_stamped_with_commit_lsn() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, "0/100".parse().unwrap());
    d.on_change(&cache, &insert_id(1, 100), &sink, "0/101".parse().unwrap())
        .await
        .unwrap();
    d.on_change(&cache, &insert_id(2, 100), &sink, "0/102".parse().unwrap())
        .await
        .unwrap();
    let commit: Lsn = "0/900".parse().unwrap();
    let files = d
        .on_stream_commit(100, commit, UtcTimestamp::now(), &cache, &sink)
        .await
        .unwrap();
    assert_eq!(files.iter().map(|f| f.row_count).sum::<u64>(), 2);
    assert!(files.iter().all(|f| f.lsn_end == commit));
    assert_eq!(d.open_floor(), None);
}

#[tokio::test]
async fn commit_materialises_exactly_what_survivors_reports() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(u64::MAX);
    let top_xid = 857;
    d.on_stream_start(top_xid, true, "0/100".parse().unwrap());
    for (id, sub_xid, lsn) in [
        (1, top_xid, "0/101"),
        (2, 858, "0/102"),
        (3, top_xid, "0/103"),
    ] {
        d.on_change(&cache, &insert_id(id, sub_xid), &sink, lsn.parse().unwrap())
            .await
            .unwrap();
    }
    d.on_stream_abort(top_xid, 858, &sink).await;
    let expected = d.survivor_count(top_xid);

    let files = d
        .on_stream_commit(
            top_xid,
            "0/900".parse().unwrap(),
            UtcTimestamp::now(),
            &cache,
            &sink,
        )
        .await
        .unwrap();

    assert_eq!(
        files.iter().map(|f| f.row_count).sum::<u64>(),
        u64::try_from(expected).unwrap()
    );
}

#[test]
fn survivors_borrows_only_the_aborted_set() {
    assert!(include_str!("stream_txn.rs").contains("let aborted = &self.aborted;"));

    let mut txn = StreamedTxn::new("0/100".parse().unwrap());
    txn.push_change(StreamedChange {
        sub_xid: 857,
        oid: 42,
        op: Op::Insert,
        values: Vec::new(),
        lsn: "0/101".parse().unwrap(),
    });
    let survivors = txn.survivors();
    let begin_lsn = txn.begin_lsn;
    let staged_len = txn.staged.len();

    assert_eq!(begin_lsn, "0/100".parse().unwrap());
    assert_eq!(staged_len, 0);
    assert_eq!(survivors.count(), 1);
}

#[tokio::test]
async fn whole_txn_stream_abort_drops_the_buffer() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(u64::MAX);
    d.on_stream_start(100, true, "0/100".parse().unwrap());
    d.on_change(&cache, &insert_id(1, 100), &sink, "0/101".parse().unwrap())
        .await
        .unwrap();
    d.on_stream_abort(100, 100, &sink).await; // sub == top
    assert_eq!(d.open_floor(), None);
}

/// proto §9b: 3000 kept-A + rolled-back savepoint + 3000 kept-B → exactly 6000 survivors.
#[tokio::test]
async fn subtxn_abort_excludes_only_the_aborted_subxid() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(u64::MAX); // no spill: pure in-memory exclusion
    let begin: Lsn = "0/1000".parse().unwrap();
    d.on_stream_start(857, true, begin);
    for i in 0..3000 {
        d.on_change(&cache, &insert_id(10_000 + i, 857), &sink, begin)
            .await
            .unwrap();
    }
    for i in 0..2762 {
        d.on_change(&cache, &insert_id(20_000 + i, 858), &sink, begin)
            .await
            .unwrap();
    }
    d.on_stream_abort(857, 858, &sink).await; // sub != top
    for i in 0..3000 {
        d.on_change(&cache, &insert_id(30_000 + i, 859), &sink, begin)
            .await
            .unwrap();
    }
    assert_eq!(d.survivor_count(857), 6000);
    let files = d
        .on_stream_commit(
            857,
            "0/9000".parse().unwrap(),
            UtcTimestamp::now(),
            &cache,
            &sink,
        )
        .await
        .unwrap();
    assert_eq!(
        files.iter().map(|f| f.row_count).sum::<u64>(),
        6000,
        "exactly 6000 — never the rolled-back rows"
    );
}

/// A LOW ceiling forces speculative spills; the aborted sub-xid's spilled file is still excluded.
#[tokio::test]
async fn low_ceiling_spills_yet_still_excludes_the_aborted_subxid() {
    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(500); // tiny ceiling → spill early and often
    let begin: Lsn = "0/1000".parse().unwrap();
    d.on_stream_start(857, true, begin);
    for i in 0..200 {
        d.on_change(&cache, &insert_id(10_000 + i, 857), &sink, begin)
            .await
            .unwrap(); // kept
    }
    for i in 0..200 {
        d.on_change(&cache, &insert_id(20_000 + i, 858), &sink, begin)
            .await
            .unwrap(); // rolled back
    }
    assert!(
        d.spill_count() > 0,
        "the low ceiling forced speculative spills"
    );
    d.on_stream_abort(857, 858, &sink).await;
    for i in 0..200 {
        d.on_change(&cache, &insert_id(30_000 + i, 859), &sink, begin)
            .await
            .unwrap(); // kept
    }
    let commit: Lsn = "0/9000".parse().unwrap();
    let files = d
        .on_stream_commit(857, commit, UtcTimestamp::now(), &cache, &sink)
        .await
        .unwrap();
    assert_eq!(
        files.iter().map(|f| f.row_count).sum::<u64>(),
        400,
        "even with spilling, the aborted sub-xid (200 rows) is excluded → 400 survivors"
    );
    // PR 4.3 fix: promoted spills are tagged `Spill` (their per-row commit_lsn is a placeholder, so the
    // loader must stamp `lsn_end`), and EVERY returned file — spill or survivor — carries the real
    // commit LSN as `lsn_end`.
    assert!(
        files.iter().any(|f| f.kind == FileKind::Spill),
        "at least one promoted spill is tagged FileKind::Spill"
    );
    assert!(
        files.iter().all(|f| f.lsn_end == commit),
        "every file carries the real commit LSN in lsn_end"
    );
}

#[tokio::test]
async fn spill_preserves_commit_order_of_the_surviving_rows() {
    assert!(include_str!("stream_txn.rs").contains("std::mem::take(&mut self.changes)"));

    let (cache, sink) = (cache(), mem_sink());
    let mut d = demux(250);
    let top = 857;
    d.on_stream_start(top, true, "0/100".parse().unwrap());

    for (id, sub_xid, lsn) in [
        (1, 858, "0/101"),
        (2, 857, "0/102"),
        (3, 859, "0/103"),
        (4, 857, "0/104"),
        (5, 857, "0/105"),
    ] {
        let values = vec![
            TupleValue::Text(id.to_string()),
            TupleValue::Text("n".into()),
        ];
        d.claim_stream((42, sub_xid), top, estimate_change_bytes(&values));
        d.open.get_mut(&top).unwrap().push_change(StreamedChange {
            sub_xid,
            oid: 42,
            op: Op::Insert,
            values,
            lsn: lsn.parse().unwrap(),
        });
    }

    d.spill_if_over_ceiling(&cache, &sink).await.unwrap();

    let surviving_lsns: Vec<Lsn> = d.open[&top].changes.iter().map(|c| c.lsn).collect();
    assert_eq!(
        surviving_lsns,
        vec!["0/101".parse().unwrap(), "0/103".parse().unwrap()],
        "partitioning the spill candidate must preserve survivor commit order"
    );
}

// Regression note (PR 26.2): the existing HashSet/BTreeSet membership indexes in loader/ddl.rs,
// pg-sink/preflight.rs, and pg-sink/reload_export.rs stay sets. XID_PREFIXED stays a 7-byte slice
// scan, and reload_signal/heartbeat/ddl column lookups stay Vec::position because they need indices.
