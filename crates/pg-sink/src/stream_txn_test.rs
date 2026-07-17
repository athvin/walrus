use super::*;
use crate::batch::SystemClock;
use common::{PgColumn, PgRelation, ReplicaIdentity};
use pg_to_arrow::oids;
use std::time::Duration;

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
    c.upsert_from_relation(rel, 1).unwrap();
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
            max_rows: 100_000,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        1,
        "test".into(),
        ceiling,
    )
}

fn mem_sink() -> ParquetSink {
    ParquetSink::new(
        Arc::new(object_store::memory::InMemory::new()),
        "walrus".into(),
        1,
    )
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
