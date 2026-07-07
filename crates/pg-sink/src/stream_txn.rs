//! Large-transaction streaming (§1.6). With `streaming='on'`, a transaction larger than
//! `logical_decoding_work_mem` arrives **before its commit**, chopped into interleaved
//! `Stream Start … Stream Stop` blocks that finish with `Stream Commit` or `Stream Abort`. This module
//! makes the sink correct under that:
//!
//! 1. **Demultiplex per top-level `xid`** — a [`StreamDemux`] of per-xid [`StreamedTxn`] buffers,
//!    reassembling non-contiguous segments via the `Stream Start` first-segment flag.
//! 2. **Stage speculatively** — an open txn's sub-batches are PUT to S3 (the PR 2.24 path) to bound
//!    memory, **but with no manifest row**. *Freeing memory (the PUT) is NOT the same as advancing the
//!    slot or making data visible (the `ready` manifest row).*
//! 3. **Commit-gate visibility** — `on_stream_commit` promotes the speculative files to `ready`
//!    (`lsn_end = commit_lsn`); nothing is visible before `Stream Commit`.
//! 4. **Hold the slot** — [`StreamDemux::open_floor`] is the oldest open txn's begin LSN; the
//!    checkpoint clamps `confirmed_flush_lsn` to it, so a crash always re-streams an incomplete txn.
//! 5. **Discard aborts** — a whole-txn `Stream Abort {sub == top}` deletes the speculative S3 objects
//!    and writes no `ready` row (a live walsender *does* stream rows for a txn that later aborts,
//!    proto §9a — the abort frame is the only discard signal).
//!
//! Rolled-back **subtransactions** (`Stream Abort {sub != top}`) are PR 2.31; the per-message *sub*-xid
//! is preserved on each row here so 2.31 can exclude them.

use crate::batch::{BatchTriggers, Clock, TableBatcher};
use crate::pgoutput::Message;
use crate::relcache::RelationCache;
use crate::sink::{ParquetSink, WrittenObject};
use anyhow::Context;
use common::{Kind, Lsn, Op, SinkMeta, UtcTimestamp};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

/// Per top-level xid buffer for an in-progress streamed transaction.
struct StreamedTxn {
    /// The floor `confirmed_flush` must not pass while this txn is open (its first-segment LSN).
    begin_lsn: Lsn,
    /// Speculatively-staged S3 objects — no manifest row until `Stream Commit`.
    staged: Vec<WrittenObject>,
    /// Not-yet-spilled rows, per table oid.
    batchers: HashMap<u32, TableBatcher>,
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
        self.open.entry(top_xid).or_insert_with(|| StreamedTxn {
            begin_lsn: lsn,
            staged: Vec::new(),
            batchers: HashMap::new(),
        });
        self.current_top = Some(top_xid);
    }

    /// `Stream Stop`: the block ended (the txn may resume with a later segment).
    pub fn on_stream_stop(&mut self) {
        self.current_top = None;
    }

    /// A streamed `Insert`/`Update`/`Delete` inside the current block: buffer it against the current
    /// top-level xid, speculatively PUT to S3 (no manifest) when a per-batch cap trips.
    pub async fn on_change(
        &mut self,
        cache: &RelationCache,
        msg: &Message,
        sink: &ParquetSink,
        lsn: Lsn,
    ) -> anyhow::Result<()> {
        let top = self
            .current_top
            .context("streamed change arrived outside a Stream Start block")?;
        let (oid, op, values, sub_xid) = match msg {
            Message::Insert {
                relation_oid,
                new,
                xid,
            } => (
                *relation_oid,
                Op::Insert,
                new.as_slice(),
                xid.unwrap_or(top),
            ),
            Message::Update {
                relation_oid,
                new,
                xid,
                ..
            } => (
                *relation_oid,
                Op::Update,
                new.as_slice(),
                xid.unwrap_or(top),
            ),
            Message::Delete {
                relation_oid,
                old,
                xid,
                ..
            } => (
                *relation_oid,
                Op::Delete,
                old.as_slice(),
                xid.unwrap_or(top),
            ),
            _ => return Ok(()),
        };
        let Some(cached) = cache.get(oid, self.schema_version) else {
            tracing::warn!(
                relation_oid = oid,
                "streamed change for an un-cached relation; skipping"
            );
            return Ok(());
        };
        // Copy field context out of `self` before borrowing `self.open` mutably (disjoint borrows).
        let (triggers, clock, epoch, schema_version, instance) = (
            self.triggers,
            self.clock.clone(),
            self.epoch,
            self.schema_version,
            self.sink_instance.clone(),
        );
        let txn = self
            .open
            .get_mut(&top)
            .context("no open buffer for the current stream block")?;
        let meta = SinkMeta {
            op,
            lsn,
            // The real commit LSN is unknown until Stream Commit; the manifest row (authoritative) gets
            // it there. Per-row we stamp the txn's begin LSN as a lower-bound placeholder.
            commit_lsn: txn.begin_lsn,
            commit_ts: UtcTimestamp::now(),
            xid: sub_xid, // the SUB-transaction xid — PR 2.31 needs it to exclude rolled-back subtxns
            epoch,
            batch_id: String::new(),
            schema_version,
            source_schema: cached.relation.schema.clone(),
            source_table: cached.relation.name.clone(),
            kind: Kind::Stream,
            unchanged_toast: vec![],
            sink_instance: instance,
            sink_processed_at: UtcTimestamp::now(),
        };
        let batcher = match txn.batchers.entry(oid) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(
                TableBatcher::new(cached.clone(), triggers, clock)
                    .context("open streamed batcher")?,
            ),
        };
        batcher.push(meta, values);
        // Promote at the begin LSN so the rows become sealable; a cap trip spills speculatively.
        batcher
            .on_commit(txn.begin_lsn)
            .context("promote streamed rows")?;
        if batcher.should_flush() {
            let sealed = batcher.seal().context("seal speculative sub-batch")?;
            let obj = sink.put(sealed).await.context("speculative PUT")?;
            tracing::info!(top_xid = top, uri = %obj.s3_uri, "spilled speculative sub-batch (no manifest)");
            txn.staged.push(obj);
        }
        Ok(())
    }

    /// `Stream Commit`: seal any remaining buffered rows, then return every staged object stamped with
    /// the real `commit_lsn` so the caller writes the `ready` manifest rows. The buffer is dropped.
    pub async fn on_stream_commit(
        &mut self,
        top_xid: u32,
        commit_lsn: Lsn,
        sink: &ParquetSink,
    ) -> anyhow::Result<Vec<WrittenObject>> {
        if self.current_top == Some(top_xid) {
            self.current_top = None;
        }
        let Some(mut txn) = self.open.remove(&top_xid) else {
            tracing::warn!(
                top_xid,
                "Stream Commit for an unknown xid; nothing to promote"
            );
            return Ok(Vec::new());
        };
        for batcher in txn.batchers.values_mut() {
            if batcher.committed_rows() > 0 {
                let sealed = batcher.seal().context("seal final streamed sub-batch")?;
                txn.staged
                    .push(sink.put(sealed).await.context("final speculative PUT")?);
            }
        }
        // Stamp the authoritative commit LSN — only NOW are these files visible.
        for obj in &mut txn.staged {
            obj.lsn_end = commit_lsn;
        }
        Ok(txn.staged)
    }

    /// `Stream Abort`: a **whole-txn** abort (`sub == top`) deletes the speculative S3 objects and drops
    /// the buffer — no `ready` row is ever written. A sub-txn abort (`sub != top`) is PR 2.31.
    pub async fn on_stream_abort(
        &mut self,
        top_xid: u32,
        sub_xid: u32,
        sink: &ParquetSink,
    ) -> anyhow::Result<()> {
        if top_xid != sub_xid {
            return Ok(()); // rolled-back subtransaction inside a committing txn — PR 2.31
        }
        if self.current_top == Some(top_xid) {
            self.current_top = None;
        }
        if let Some(txn) = self.open.remove(&top_xid) {
            for obj in &txn.staged {
                // Best-effort: the object has no manifest row, so a leaked file is harmless (GC'd
                // later); what must never happen is a `ready` row pointing at a deleted object.
                if let Err(e) = sink.delete(&obj.key).await {
                    tracing::warn!(uri = %obj.s3_uri, error = %e, "abort: speculative delete failed");
                }
            }
            tracing::info!(
                top_xid,
                staged = txn.staged.len(),
                "whole-txn abort: discarded speculative files"
            );
        }
        Ok(())
    }

    /// The oldest open txn's begin LSN — `confirmed_flush` must never pass this (§1.6). `None` when no
    /// streamed txn is open (a no-op ceiling for the checkpoint).
    pub fn open_floor(&self) -> Option<Lsn> {
        self.open.values().map(|t| t.begin_lsn).min()
    }

    #[cfg(test)]
    fn buffered_rows(&self, top_xid: u32) -> u64 {
        self.open
            .get(&top_xid)
            .map(|t| t.batchers.values().map(TableBatcher::committed_rows).sum())
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
    use crate::sink::ParquetSink;
    use common::{PgColumn, PgRelation, ReplicaIdentity, TupleValue};
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

    fn demux(max_rows: u64) -> StreamDemux {
        StreamDemux::new(
            BatchTriggers {
                max_rows,
                max_bytes: u64::MAX,
                max_fill: Duration::from_secs(3600),
            },
            Arc::new(SystemClock),
            1,
            1,
            "test".into(),
        )
    }

    fn mem_sink() -> (ParquetSink, Arc<dyn object_store::ObjectStore>) {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        (ParquetSink::new(store.clone(), "walrus".into(), 1), store)
    }

    #[tokio::test]
    async fn demux_routes_interleaved_xids_to_their_buffers() {
        let cache = cache();
        let (sink, _store) = mem_sink();
        let mut d = demux(u64::MAX); // never spills — rows stay buffered
                                     // Interleave: open 100, one row; stop; open 200, two rows; stop; resume 100, one more row.
        d.on_stream_start(100, true, "0/100".parse().unwrap());
        d.on_change(&cache, &insert("1", 100), &sink, "0/101".parse().unwrap())
            .await
            .unwrap();
        d.on_stream_stop();
        d.on_stream_start(200, true, "0/200".parse().unwrap());
        d.on_change(&cache, &insert("2", 200), &sink, "0/201".parse().unwrap())
            .await
            .unwrap();
        d.on_change(&cache, &insert("3", 200), &sink, "0/202".parse().unwrap())
            .await
            .unwrap();
        d.on_stream_stop();
        d.on_stream_start(100, false, "0/300".parse().unwrap());
        d.on_change(&cache, &insert("4", 100), &sink, "0/301".parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            d.buffered_rows(100),
            2,
            "xid 100 got its 2 interleaved rows"
        );
        assert_eq!(d.buffered_rows(200), 2, "xid 200 got its 2 rows");
    }

    #[tokio::test]
    async fn open_floor_is_oldest_open_txn_begin_lsn() {
        let mut d = demux(u64::MAX);
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
    async fn stream_commit_stamps_commit_lsn_and_returns_files() {
        let cache = cache();
        let (sink, _store) = mem_sink();
        let mut d = demux(1); // spill every row → speculative files
        d.on_stream_start(100, true, "0/100".parse().unwrap());
        d.on_change(&cache, &insert("1", 100), &sink, "0/101".parse().unwrap())
            .await
            .unwrap();
        d.on_change(&cache, &insert("2", 100), &sink, "0/102".parse().unwrap())
            .await
            .unwrap();
        let commit: Lsn = "0/900".parse().unwrap();
        let files = d.on_stream_commit(100, commit, &sink).await.unwrap();
        assert!(
            !files.is_empty(),
            "commit returns the staged files to promote"
        );
        assert!(
            files.iter().all(|f| f.lsn_end == commit),
            "every file stamped with commit_lsn"
        );
        assert_eq!(d.open_floor(), None, "the committed txn is no longer open");
    }

    #[tokio::test]
    async fn whole_txn_stream_abort_deletes_speculative_and_writes_no_ready() {
        let cache = cache();
        let (sink, store) = mem_sink();
        let mut d = demux(1); // spill every row
        d.on_stream_start(100, true, "0/100".parse().unwrap());
        d.on_change(&cache, &insert("1", 100), &sink, "0/101".parse().unwrap())
            .await
            .unwrap();
        d.on_change(&cache, &insert("2", 100), &sink, "0/102".parse().unwrap())
            .await
            .unwrap();
        // Grab the staged keys before the abort, then assert they are gone after.
        let keys: Vec<_> = d.open[&100].staged.iter().map(|o| o.key.clone()).collect();
        assert!(!keys.is_empty(), "rows spilled speculatively");
        d.on_stream_abort(100, 100, &sink).await.unwrap();
        assert_eq!(d.open_floor(), None, "aborted txn dropped");
        for k in keys {
            use object_store::ObjectStore;
            assert!(
                store.head(&k).await.is_err(),
                "speculative object deleted on whole-txn abort"
            );
        }
    }
}
