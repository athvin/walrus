//! Large-transaction streaming + sub-transaction exclusion (§1.6, proto §8/§9). With `streaming='on'`,
//! a transaction larger than `logical_decoding_work_mem` arrives **before its commit**, chopped into
//! interleaved `Stream Start … Stream Stop` blocks that finish with `Stream Commit` or `Stream Abort`.
//! This module makes the sink correct under that:
//!
//! 1. **Demultiplex per top-level `xid`** — a [`StreamDemux`] of per-xid [`StreamedTxn`] buffers,
//!    reassembling non-contiguous segments via the `Stream Start` first-segment flag.
//! 2. **Commit-gate visibility** — a buffered txn's rows become a `ready` manifest file **only on
//!    `Stream Commit`**; nothing is visible before it.
//! 3. **Hold the slot** — [`StreamDemux::open_floor`] is the oldest open txn's begin LSN; the
//!    checkpoint clamps `confirmed_flush_lsn` to it, so a crash always re-streams an incomplete txn.
//! 4. **Discard aborts** — a whole-txn `Stream Abort {sub == top}` drops the buffer entirely; a
//!    **sub-transaction** abort `Stream Abort {sub != top}` (PR 2.31, the dangerous savepoint case,
//!    proto §9b) drops **exactly** that sub-xid's rows while the top-level continues to commit.
//!
//! **top vs sub xid (proto §7).** `Stream Start` carries the **top-level** xid; every streamed change
//! carries its **sub**-transaction xid (which equals the top for the main branch). The abort names the
//! **sub**-xid — so each buffered row is tagged with its sub-xid and `survivors()` excludes exactly the
//! aborted ones. Mixing them up silently keeps or drops the wrong rows: the *silent corruption* this
//! module exists to prevent. A live walsender **does** stream a rolled-back savepoint's rows before the
//! abort is known (unlike the SQL decoding functions), so the `Stream Abort` frame is the only signal.
//!
//! **Memory (PR 2.31 state).** Rows are buffered **in memory** until `Stream Commit` so an aborted
//! sub-xid's rows can be excluded before anything is written. The `max_inflight_bytes`-triggered
//! proactive spill — which must spill *per sub-xid* so an aborted sub-xid's already-spilled file can be
//! dropped without contaminating survivors — is **PR 2.32**.

use crate::batch::{BatchTriggers, Clock, TableBatcher};
use crate::pgoutput::Message;
use crate::relcache::RelationCache;
use crate::sink::{ParquetSink, WrittenObject};
use anyhow::Context;
use common::{Kind, Lsn, Op, SinkMeta, TupleValue, UtcTimestamp};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// One streamed change, tagged with **its** sub-transaction xid (proto §7).
#[derive(Clone)]
struct StreamedChange {
    sub_xid: u32,
    oid: u32,
    op: Op,
    values: Vec<TupleValue>,
    lsn: Lsn,
}

/// Per top-level xid buffer for an in-progress streamed transaction.
struct StreamedTxn {
    /// The floor `confirmed_flush` must not pass while this txn is open (its first-segment LSN).
    begin_lsn: Lsn,
    /// Buffered changes in commit order, each tagged with its sub-xid.
    changes: Vec<StreamedChange>,
    /// Sub-xids that rolled back (`Stream Abort {sub != top}`) — excluded from `survivors`.
    aborted: HashSet<u32>,
}

impl StreamedTxn {
    fn new(begin_lsn: Lsn) -> Self {
        StreamedTxn {
            begin_lsn,
            changes: Vec::new(),
            aborted: HashSet::new(),
        }
    }

    /// Record a streamed change tagged with its sub-xid (may equal the top-level xid).
    fn push_change(&mut self, change: StreamedChange) {
        self.changes.push(change);
    }

    /// `Stream Abort {sub != top}`: mark this sub-xid's rows dead. The top-level txn continues toward
    /// `Stream Commit`; the rows stay buffered but `survivors` skips them (freed at commit).
    fn abort_subtxn(&mut self, sub_xid: u32) {
        self.aborted.insert(sub_xid);
    }

    /// The rows to materialise on `Stream Commit`: every buffered change **except** those tagged with
    /// an aborted sub-xid, in commit order.
    fn survivors(&self) -> impl Iterator<Item = &StreamedChange> {
        self.changes
            .iter()
            .filter(move |c| !self.aborted.contains(&c.sub_xid))
    }
}

/// Demultiplexes interleaved streamed transactions and commit-gates their visibility. It is **DB-free**
/// — `on_stream_commit` returns the objects to `record_ready`; the caller does the manifest INSERT.
pub struct StreamDemux {
    open: HashMap<u32, StreamedTxn>,
    /// The top-level xid of the currently-open `Stream Start … Stream Stop` block; changes route here.
    current_top: Option<u32>,
    triggers: BatchTriggers,
    clock: Arc<dyn Clock>,
    epoch: i64,
    schema_version: i64,
    sink_instance: String,
}

impl StreamDemux {
    pub fn new(
        triggers: BatchTriggers,
        clock: Arc<dyn Clock>,
        epoch: i64,
        schema_version: i64,
        sink_instance: String,
    ) -> Self {
        StreamDemux {
            open: HashMap::new(),
            current_top: None,
            triggers,
            clock,
            epoch,
            schema_version,
            sink_instance,
        }
    }

    /// `Stream Start`: open (first segment) or resume (later segment) the top-level xid's buffer, and
    /// mark it the current block so subsequent changes route to it.
    pub fn on_stream_start(&mut self, top_xid: u32, _first_segment: bool, lsn: Lsn) {
        // `begin_lsn` is set on first sight and never moved (later segments keep the original floor).
        self.open
            .entry(top_xid)
            .or_insert_with(|| StreamedTxn::new(lsn));
        self.current_top = Some(top_xid);
    }

    /// `Stream Stop`: the block ended (the txn may resume with a later segment).
    pub fn on_stream_stop(&mut self) {
        self.current_top = None;
    }

    /// A streamed `Insert`/`Update`/`Delete` inside the current block: buffer it against the current
    /// top-level xid, tagged with the change's **sub**-xid. In-memory only (PR 2.31); no S3 write yet.
    pub fn on_change(&mut self, msg: &Message, lsn: Lsn) -> anyhow::Result<()> {
        let top = self
            .current_top
            .context("streamed change arrived outside a Stream Start block")?;
        let (oid, op, values, sub_xid) = match msg {
            Message::Insert {
                relation_oid,
                new,
                xid,
            } => (*relation_oid, Op::Insert, new.clone(), xid.unwrap_or(top)),
            Message::Update {
                relation_oid,
                new,
                xid,
                ..
            } => (*relation_oid, Op::Update, new.clone(), xid.unwrap_or(top)),
            Message::Delete {
                relation_oid,
                old,
                xid,
                ..
            } => (*relation_oid, Op::Delete, old.clone(), xid.unwrap_or(top)),
            _ => return Ok(()),
        };
        let txn = self
            .open
            .get_mut(&top)
            .context("no open buffer for the current stream block")?;
        txn.push_change(StreamedChange {
            sub_xid,
            oid,
            op,
            values,
            lsn,
        });
        Ok(())
    }

    /// `Stream Abort {top, sub}`. **sub == top** (whole-txn abort, PR 2.30): drop the buffer entirely —
    /// no `ready` row is ever written. **sub != top** (rolled-back savepoint, PR 2.31): drop exactly
    /// that sub-xid's rows; the top-level txn stays open and will still commit its survivors.
    pub fn on_stream_abort(&mut self, top_xid: u32, sub_xid: u32) {
        if top_xid == sub_xid {
            if self.current_top == Some(top_xid) {
                self.current_top = None;
            }
            if let Some(txn) = self.open.remove(&top_xid) {
                tracing::info!(
                    top_xid,
                    rows = txn.changes.len(),
                    "whole-txn abort: dropped buffer"
                );
            }
            return;
        }
        if let Some(txn) = self.open.get_mut(&top_xid) {
            txn.abort_subtxn(sub_xid);
            tracing::info!(
                top_xid,
                sub_xid,
                "sub-txn abort: rolled-back savepoint rows excluded"
            );
        }
    }

    /// `Stream Commit`: materialise the **survivors** (aborted sub-xids excluded) into `ready` files,
    /// stamped with the real `commit_lsn`, and return them for the caller to `record_ready`. The output
    /// is chunked by the per-batch caps. The buffer is dropped.
    pub async fn on_stream_commit(
        &mut self,
        top_xid: u32,
        commit_lsn: Lsn,
        cache: &RelationCache,
        sink: &ParquetSink,
    ) -> anyhow::Result<Vec<WrittenObject>> {
        if self.current_top == Some(top_xid) {
            self.current_top = None;
        }
        let Some(txn) = self.open.remove(&top_xid) else {
            tracing::warn!(
                top_xid,
                "Stream Commit for an unknown xid; nothing to materialise"
            );
            return Ok(Vec::new());
        };
        let (triggers, clock, epoch, schema_version, instance) = (
            self.triggers,
            self.clock.clone(),
            self.epoch,
            self.schema_version,
            self.sink_instance.clone(),
        );
        let mut batchers: HashMap<u32, TableBatcher> = HashMap::new();
        let mut out = Vec::new();
        for c in txn.survivors() {
            let Some(cached) = cache.get(c.oid, schema_version) else {
                tracing::warn!(
                    relation_oid = c.oid,
                    "committed streamed change for an un-cached relation; skipping"
                );
                continue;
            };
            let meta = SinkMeta {
                op: c.op,
                lsn: c.lsn,
                commit_lsn, // NOW known — the streamed rows carry the real commit LSN
                commit_ts: UtcTimestamp::now(),
                xid: c.sub_xid,
                epoch,
                batch_id: String::new(),
                schema_version,
                source_schema: cached.relation.schema.clone(),
                source_table: cached.relation.name.clone(),
                kind: Kind::Stream,
                unchanged_toast: vec![],
                sink_instance: instance.clone(),
                sink_processed_at: UtcTimestamp::now(),
            };
            let batcher = match batchers.entry(c.oid) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(
                    TableBatcher::new(cached.clone(), triggers, clock.clone())
                        .context("open streamed materialise batcher")?,
                ),
            };
            batcher.push(meta, &c.values);
            batcher
                .on_commit(commit_lsn)
                .context("promote streamed survivors")?;
            if batcher.should_flush() {
                out.push(
                    sink.put(batcher.seal()?)
                        .await
                        .context("materialise streamed sub-batch")?,
                );
            }
        }
        for batcher in batchers.values_mut() {
            if batcher.committed_rows() > 0 {
                out.push(
                    sink.put(batcher.seal()?)
                        .await
                        .context("materialise final streamed batch")?,
                );
            }
        }
        Ok(out)
    }

    /// The oldest open txn's begin LSN — `confirmed_flush` must never pass this (§1.6). `None` when no
    /// streamed txn is open (a no-op ceiling for the checkpoint).
    pub fn open_floor(&self) -> Option<Lsn> {
        self.open.values().map(|t| t.begin_lsn).min()
    }

    #[cfg(test)]
    fn survivor_count(&self, top_xid: u32) -> usize {
        self.open
            .get(&top_xid)
            .map(|t| t.survivors().count())
            .unwrap_or(0)
    }
}

/// A streamed change carries its sub-xid; a non-streamed change never enters the demux.
pub fn is_streamed_change(msg: &Message) -> bool {
    matches!(
        msg,
        Message::Insert { xid: Some(_), .. }
            | Message::Update { xid: Some(_), .. }
            | Message::Delete { xid: Some(_), .. }
    )
}

#[cfg(test)]
mod tests {
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

    fn insert(id: &str, sub_xid: u32) -> Message {
        Message::Insert {
            xid: Some(sub_xid),
            relation_oid: 42,
            new: vec![TupleValue::Text(id.into()), TupleValue::Text("n".into())],
        }
    }

    fn demux() -> StreamDemux {
        StreamDemux::new(
            BatchTriggers {
                max_rows: 100_000,
                max_bytes: u64::MAX,
                max_fill: Duration::from_secs(3600),
            },
            Arc::new(SystemClock),
            1,
            1,
            "test".into(),
        )
    }

    fn mem_sink() -> ParquetSink {
        ParquetSink::new(
            Arc::new(object_store::memory::InMemory::new()),
            "walrus".into(),
            1,
        )
    }

    #[test]
    fn demux_routes_interleaved_xids_to_their_buffers() {
        let mut d = demux();
        // Interleave: open 100 (one row); stop; open 200 (two rows); stop; resume 100 (one more).
        d.on_stream_start(100, true, "0/100".parse().unwrap());
        d.on_change(&insert("1", 100), "0/101".parse().unwrap())
            .unwrap();
        d.on_stream_stop();
        d.on_stream_start(200, true, "0/200".parse().unwrap());
        d.on_change(&insert("2", 200), "0/201".parse().unwrap())
            .unwrap();
        d.on_change(&insert("3", 200), "0/202".parse().unwrap())
            .unwrap();
        d.on_stream_stop();
        d.on_stream_start(100, false, "0/300".parse().unwrap());
        d.on_change(&insert("4", 100), "0/301".parse().unwrap())
            .unwrap();
        assert_eq!(
            d.survivor_count(100),
            2,
            "xid 100 got its 2 interleaved rows"
        );
        assert_eq!(d.survivor_count(200), 2, "xid 200 got its 2 rows");
    }

    #[test]
    fn open_floor_is_oldest_open_txn_begin_lsn() {
        let mut d = demux();
        assert_eq!(d.open_floor(), None, "no open txn → no floor");
        d.on_stream_start(100, true, "0/500".parse().unwrap());
        d.on_stream_start(200, true, "0/900".parse().unwrap());
        assert_eq!(
            d.open_floor(),
            Some("0/500".parse().unwrap()),
            "floor is the OLDEST begin LSN"
        );
    }

    #[tokio::test]
    async fn stream_commit_materialises_survivors_stamped_with_commit_lsn() {
        let cache = cache();
        let sink = mem_sink();
        let mut d = demux();
        d.on_stream_start(100, true, "0/100".parse().unwrap());
        d.on_change(&insert("1", 100), "0/101".parse().unwrap())
            .unwrap();
        d.on_change(&insert("2", 100), "0/102".parse().unwrap())
            .unwrap();
        let commit: Lsn = "0/900".parse().unwrap();
        let files = d
            .on_stream_commit(100, commit, &cache, &sink)
            .await
            .unwrap();
        assert!(!files.is_empty(), "commit materialises the buffered rows");
        assert_eq!(files.iter().map(|f| f.row_count).sum::<u64>(), 2);
        assert!(
            files.iter().all(|f| f.lsn_end == commit),
            "every file stamped with commit_lsn"
        );
        assert_eq!(d.open_floor(), None, "the committed txn is no longer open");
    }

    #[test]
    fn whole_txn_stream_abort_drops_the_buffer() {
        let mut d = demux();
        d.on_stream_start(100, true, "0/100".parse().unwrap());
        d.on_change(&insert("1", 100), "0/101".parse().unwrap())
            .unwrap();
        d.on_stream_abort(100, 100); // sub == top
        assert_eq!(d.open_floor(), None, "whole-txn abort dropped the buffer");
    }

    // ---- PR 2.31: the flagship sub-transaction-exclusion assertions (proto §9b) ----

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

    /// proto §9b: top=857 keeps 3000 (kept-A), savepoint=858 rolls back 2762, savepoint=859 keeps 3000
    /// (kept-B). `Stream Abort {top=857, sub=858}` then `Stream Commit 857` → exactly 6000 survivors.
    #[tokio::test]
    async fn subtxn_abort_excludes_only_the_aborted_subxid() {
        let cache = cache();
        let sink = mem_sink();
        let mut d = demux();
        let begin: Lsn = "0/1000".parse().unwrap();
        d.on_stream_start(857, true, begin);
        for i in 0..3000 {
            d.on_change(&insert_id(10_000 + i, 857), begin).unwrap(); // kept-A (top branch)
        }
        for i in 0..2762 {
            d.on_change(&insert_id(20_000 + i, 858), begin).unwrap(); // rolled-back savepoint
        }
        d.on_stream_abort(857, 858); // sub != top → drop ONLY 858's rows; 857 stays open
        for i in 0..3000 {
            d.on_change(&insert_id(30_000 + i, 859), begin).unwrap(); // kept-B (new savepoint)
        }
        assert_eq!(
            d.survivor_count(857),
            6000,
            "exactly 6000 survivors: 3000 kept-A + 3000 kept-B"
        );

        let commit: Lsn = "0/9000".parse().unwrap();
        let files = d
            .on_stream_commit(857, commit, &cache, &sink)
            .await
            .unwrap();
        let rows: u64 = files.iter().map(|f| f.row_count).sum();
        assert_eq!(
            rows, 6000,
            "the ready file has EXACTLY 6000 rows — never the 2762 rolled-back ones"
        );
    }

    #[tokio::test]
    async fn survivors_are_emitted_in_commit_order() {
        // kept-A (id 1) buffered before the abort; kept-B (id 2) after — order preserved.
        let mut d = demux();
        let begin: Lsn = "0/1000".parse().unwrap();
        d.on_stream_start(857, true, begin);
        d.on_change(&insert_id(1, 857), begin).unwrap();
        d.on_change(&insert_id(9, 858), begin).unwrap();
        d.on_stream_abort(857, 858);
        d.on_change(&insert_id(2, 859), begin).unwrap();
        let ids: Vec<i32> = d.open[&857]
            .survivors()
            .map(|c| match &c.values[0] {
                TupleValue::Text(s) => s.parse().unwrap(),
                _ => -1,
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 2],
            "survivors keep commit order, aborted 9 excluded"
        );
    }

    #[tokio::test]
    async fn nested_then_new_subxid_after_rollback_is_kept() {
        let cache = cache();
        let sink = mem_sink();
        let mut d = demux();
        let begin: Lsn = "0/1000".parse().unwrap();
        d.on_stream_start(857, true, begin);
        d.on_change(&insert_id(1, 858), begin).unwrap(); // to-be-aborted
        d.on_stream_abort(857, 858);
        d.on_change(&insert_id(2, 859), begin).unwrap(); // opened AFTER the rollback → kept
        assert_eq!(d.survivor_count(857), 1);
        let files = d
            .on_stream_commit(857, "0/9000".parse().unwrap(), &cache, &sink)
            .await
            .unwrap();
        assert_eq!(
            files.iter().map(|f| f.row_count).sum::<u64>(),
            1,
            "the post-rollback sub-xid is kept"
        );
    }
}
