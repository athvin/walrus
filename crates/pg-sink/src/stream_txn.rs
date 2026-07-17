//! Large-transaction streaming + sub-transaction exclusion + memory-ceiling spill (§1.6, §1.3, proto
//! §8/§9). With `streaming='on'`, a transaction larger than `logical_decoding_work_mem` arrives
//! **before its commit**, chopped into interleaved `Stream Start … Stream Stop` blocks that finish with
//! `Stream Commit` or `Stream Abort`. This module makes the sink correct under that:
//!
//! 1. **Demultiplex per top-level `xid`** — a [`StreamDemux`] of per-xid [`StreamedTxn`] buffers,
//!    reassembling non-contiguous segments via the `Stream Start` first-segment flag.
//! 2. **Commit-gate visibility** — a txn's rows become a `ready` manifest file **only on `Stream
//!    Commit`**; nothing is visible before it.
//! 3. **Hold the slot** — [`StreamDemux::open_floor`] is the oldest open txn's begin LSN; the checkpoint
//!    clamps `confirmed_flush_lsn` to it, so a crash always re-streams an incomplete txn.
//! 4. **Discard aborts** — a whole-txn `Stream Abort {sub == top}` drops the buffer entirely; a
//!    **sub-transaction** abort `Stream Abort {sub != top}` (the dangerous savepoint case, proto §9b)
//!    drops **exactly** that sub-xid's rows while the top-level continues to commit.
//! 5. **Bound memory** — when the aggregate [`InflightMeter`] crosses `max_inflight_bytes`, the largest
//!    open `(table, sub-xid)` buffer is **spilled speculatively** to S3 (PR 2.30 staging) — **no
//!    manifest row, slot NOT advanced** (§1.5). Spilling is **per sub-xid** so an aborted sub-xid's
//!    already-spilled file can be dropped without contaminating survivors (the PR 2.31 interaction).
//!
//! **top vs sub xid (proto §7).** `Stream Start` carries the **top-level** xid; every streamed change
//! carries its **sub**-xid. The abort names the sub-xid — each buffered/spilled row is tagged with its
//! sub-xid so `survivors` excludes exactly the aborted ones. *Freeing memory (the spill) is NOT
//! advancing the slot or making data visible (the `ready` row).*

use crate::batch::{BatchTriggers, Clock, TableBatcher};
use crate::memory::InflightMeter;
use crate::pgoutput::Message;
use crate::relcache::RelationCache;
use crate::sink::{FileKind, ParquetSink, WrittenObject};
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

/// A speculatively-spilled S3 object for one `(sub_xid)` of an open txn — no manifest row until commit.
struct StagedSpill {
    sub_xid: u32,
    written: WrittenObject,
}

/// Per top-level xid buffer for an in-progress streamed transaction.
struct StreamedTxn {
    /// The floor `confirmed_flush` must not pass while this txn is open (its first-segment LSN).
    begin_lsn: Lsn,
    /// Buffered (not-yet-spilled) changes in commit order, each tagged with its sub-xid.
    changes: Vec<StreamedChange>,
    /// Speculatively-spilled files, each homogeneous in one sub-xid (droppable on that sub-xid's abort).
    staged: Vec<StagedSpill>,
    /// Sub-xids that rolled back (`Stream Abort {sub != top}`) — excluded from `survivors`.
    aborted: HashSet<u32>,
}

impl StreamedTxn {
    fn new(begin_lsn: Lsn) -> Self {
        StreamedTxn {
            begin_lsn,
            changes: Vec::new(),
            staged: Vec::new(),
            aborted: HashSet::new(),
        }
    }

    fn push_change(&mut self, change: StreamedChange) {
        self.changes.push(change);
    }

    fn abort_subtxn(&mut self, sub_xid: u32) {
        self.aborted.insert(sub_xid);
    }

    /// The buffered (in-memory) rows that survive to commit: every change **except** aborted sub-xids.
    /// (Commit materialisation inlines this filter; this accessor backs the tests + `survivor_count`.)
    #[cfg(test)]
    fn survivors(&self) -> impl Iterator<Item = &StreamedChange> {
        self.changes
            .iter()
            .filter(move |c| !self.aborted.contains(&c.sub_xid))
    }
}

/// Demultiplexes interleaved streamed transactions, commit-gates visibility, and spills under memory
/// pressure. **DB-free** — `on_stream_commit` returns the objects to `record_ready`.
pub struct StreamDemux {
    open: HashMap<u32, StreamedTxn>,
    /// The top-level xid of the currently-open `Stream Start … Stream Stop` block; changes route here.
    current_top: Option<u32>,
    triggers: BatchTriggers,
    clock: Arc<dyn Clock>,
    epoch: i64,
    sink_instance: String,
    meter: InflightMeter,
    spill_count: u64,
}

impl StreamDemux {
    pub fn new(
        triggers: BatchTriggers,
        clock: Arc<dyn Clock>,
        epoch: i64,
        sink_instance: String,
        max_inflight_bytes: u64,
    ) -> Self {
        StreamDemux {
            open: HashMap::new(),
            current_top: None,
            triggers,
            clock,
            epoch,
            sink_instance,
            meter: InflightMeter::new(max_inflight_bytes),
            spill_count: 0,
        }
    }

    /// Total speculative spills so far (metric; PR 4.10 exports it).
    pub fn spill_count(&self) -> u64 {
        self.spill_count
    }

    /// `Stream Start`: open (first segment) or resume (later segment) the top-level xid's buffer.
    pub fn on_stream_start(&mut self, top_xid: u32, _first_segment: bool, lsn: Lsn) {
        self.open
            .entry(top_xid)
            .or_insert_with(|| StreamedTxn::new(lsn));
        self.current_top = Some(top_xid);
    }

    /// `Stream Stop`: the block ended (the txn may resume with a later segment).
    pub fn on_stream_stop(&mut self) {
        self.current_top = None;
    }

    /// A streamed change: buffer it against the current top-level xid, tagged with its sub-xid, and
    /// meter its bytes. If the aggregate ceiling is crossed, spill the largest open `(table, sub-xid)`
    /// buffer speculatively (no manifest row, slot not advanced).
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
        let bytes = estimate_change_bytes(&values);
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
        self.meter.add((oid, sub_xid), bytes);
        self.spill_if_over_ceiling(cache, sink).await
    }

    /// While over the aggregate ceiling, spill the largest open `(table, sub-xid)` buffer to a
    /// speculative S3 object (frees memory; **not** a manifest row, **not** a slot advance).
    async fn spill_if_over_ceiling(
        &mut self,
        cache: &RelationCache,
        sink: &ParquetSink,
    ) -> anyhow::Result<()> {
        while self.meter.over_ceiling() {
            let Some((oid, sub_xid)) = self.meter.largest_open() else {
                break;
            };
            // Find the top-level txn holding this (table, sub-xid) and drain exactly those rows.
            let Some(top) = self
                .open
                .iter()
                .find(|(_, t)| {
                    t.changes
                        .iter()
                        .any(|c| c.oid == oid && c.sub_xid == sub_xid)
                })
                .map(|(&k, _)| k)
            else {
                self.meter.release((oid, sub_xid)); // stale accounting; nothing buffered
                continue;
            };
            let (triggers, clock, epoch, instance) = (
                self.triggers,
                self.clock.clone(),
                self.epoch,
                self.sink_instance.clone(),
            );
            let (begin, rows) = {
                let txn = self.open.get_mut(&top).expect("top exists");
                let mut take = Vec::new();
                txn.changes.retain(|c| {
                    if c.oid == oid && c.sub_xid == sub_xid {
                        take.push(c.clone());
                        false
                    } else {
                        true
                    }
                });
                (txn.begin_lsn, take)
            };
            self.meter.release((oid, sub_xid));
            let Some(cached) = cache.latest_for(oid) else {
                continue; // shape not cached (shouldn't happen mid-stream) — nothing to spill
            };
            let mut batcher =
                TableBatcher::new(cached.clone(), triggers, clock).context("open spill batcher")?;
            for c in &rows {
                let meta = SinkMeta {
                    op: c.op,
                    lsn: c.lsn,
                    commit_lsn: begin, // placeholder until commit stamps the real one on the manifest
                    // Best-effort: the real commit_ts arrives only at Stream Commit, but this spill file
                    // is already durable in S3 by then (like commit_lsn, which the loader overrides via
                    // the manifest lsn_end; commit_ts has no such override) — so spilled rows carry the
                    // spill-time instant, always within the transaction's lifetime (PR 5.9).
                    commit_ts: UtcTimestamp::now(),
                    xid: c.sub_xid,
                    epoch,
                    batch_id: String::new(),
                    schema_version: cached.schema_version,
                    source_schema: cached.relation.schema.clone(),
                    source_table: cached.relation.name.clone(),
                    kind: Kind::Stream,
                    unchanged_toast: vec![],
                    sink_instance: instance.clone(),
                    sink_processed_at: UtcTimestamp::now(),
                };
                batcher.push(meta, &c.values);
            }
            // The spill-time instant stands in for commit_ts here (see the meta comment above).
            batcher
                .on_commit(begin, UtcTimestamp::now())
                .context("promote spill rows")?;
            if batcher.committed_rows() == 0 {
                continue;
            }
            // Tag as `Spill`: these rows carry a placeholder `commit_lsn` (`begin`) because the real commit
            // LSN is not yet known. `on_stream_commit` corrects the manifest `lsn_end` to the commit LSN;
            // the loader then reads `lsn_end` as the authoritative per-row `commit_lsn` for a `spill` file.
            let written = sink
                .put_with_kind(batcher.seal()?, FileKind::Spill)
                .await
                .context("speculative spill PUT")?;
            self.spill_count += 1;
            common::metrics::inc_spill(); // memory-ceiling speculative spill (PR 4.10)
            tracing::info!(
                top_xid = top,
                sub_xid,
                oid,
                spill_count = self.spill_count,
                uri = %written.s3_uri,
                "spilled open-txn buffer speculatively (no manifest, slot held)"
            );
            self.open
                .get_mut(&top)
                .expect("top exists")
                .staged
                .push(StagedSpill { sub_xid, written });
        }
        Ok(())
    }

    /// `Stream Abort {top, sub}`. **sub == top** (whole-txn): drop the buffer AND delete its speculative
    /// files. **sub != top** (rolled-back savepoint): mark the sub-xid dead and delete only ITS
    /// speculative files; the top-level txn stays open and commits its survivors.
    pub async fn on_stream_abort(&mut self, top_xid: u32, sub_xid: u32, sink: &ParquetSink) {
        if top_xid == sub_xid {
            common::metrics::inc_aborted_txn(); // whole-txn abort (PR 4.10)
            if self.current_top == Some(top_xid) {
                self.current_top = None;
            }
            if let Some(txn) = self.open.remove(&top_xid) {
                self.release_txn_meter(&txn);
                for s in &txn.staged {
                    let _ = sink.delete(&s.written.key).await;
                }
                tracing::info!(
                    top_xid,
                    rows = txn.changes.len(),
                    staged = txn.staged.len(),
                    "whole-txn abort"
                );
            }
            return;
        }
        let doomed: Vec<_> = match self.open.get_mut(&top_xid) {
            Some(txn) => {
                txn.abort_subtxn(sub_xid);
                let doomed = txn
                    .staged
                    .iter()
                    .filter(|s| s.sub_xid == sub_xid)
                    .map(|s| s.written.key.clone())
                    .collect::<Vec<_>>();
                txn.staged.retain(|s| s.sub_xid != sub_xid);
                doomed
            }
            None => return,
        };
        for key in &doomed {
            let _ = sink.delete(key).await;
        }
        tracing::info!(
            top_xid,
            sub_xid,
            dropped_spills = doomed.len(),
            "sub-txn abort: savepoint rows excluded"
        );
    }

    /// `Stream Commit`: publish the (non-aborted) speculative spills stamped with the real `commit_lsn`,
    /// and materialise the in-memory survivors, returning every object for the caller to `record_ready`.
    pub async fn on_stream_commit(
        &mut self,
        top_xid: u32,
        commit_lsn: Lsn,
        commit_ts: UtcTimestamp,
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
        self.release_txn_meter(&txn);
        let StreamedTxn {
            changes,
            staged,
            aborted,
            ..
        } = txn;
        let mut out = Vec::new();
        // Publish speculative spills whose sub-xid did NOT abort, stamped with the real commit LSN.
        for spill in staged {
            if aborted.contains(&spill.sub_xid) {
                let _ = sink.delete(&spill.written.key).await; // defensive; usually deleted on abort
                continue;
            }
            let mut w = spill.written;
            w.lsn_end = commit_lsn;
            out.push(w);
        }
        // Materialise the still-in-memory survivors.
        let (triggers, clock, epoch, instance) = (
            self.triggers,
            self.clock.clone(),
            self.epoch,
            self.sink_instance.clone(),
        );
        let mut batchers: HashMap<u32, TableBatcher> = HashMap::new();
        for c in changes.iter().filter(|c| !aborted.contains(&c.sub_xid)) {
            let Some(cached) = cache.latest_for(c.oid) else {
                continue;
            };
            let meta = SinkMeta {
                op: c.op,
                lsn: c.lsn,
                commit_lsn,
                commit_ts, // the real Stream-Commit timestamp (also re-stamped by on_commit below)
                xid: c.sub_xid,
                epoch,
                batch_id: String::new(),
                schema_version: cached.schema_version,
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
                .on_commit(commit_lsn, commit_ts)
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
    /// streamed txn is open.
    pub fn open_floor(&self) -> Option<Lsn> {
        self.open.values().map(|t| t.begin_lsn).min()
    }

    fn release_txn_meter(&mut self, txn: &StreamedTxn) {
        let keys: HashSet<(u32, u32)> = txn.changes.iter().map(|c| (c.oid, c.sub_xid)).collect();
        for key in keys {
            self.meter.release(key);
        }
    }

    #[cfg(test)]
    fn survivor_count(&self, top_xid: u32) -> usize {
        self.open
            .get(&top_xid)
            .map(|t| t.survivors().count())
            .unwrap_or(0)
    }
}

/// A rough per-change byte estimate (Arrow-buffered size, not serialized Parquet) for the meter.
fn estimate_change_bytes(values: &[TupleValue]) -> u64 {
    const META_OVERHEAD: u64 = 96;
    META_OVERHEAD
        + values
            .iter()
            .map(|v| match v {
                TupleValue::Text(s) => s.len() as u64,
                TupleValue::Binary(b) => b.len() as u64,
                TupleValue::Null | TupleValue::UnchangedToast => 1,
            })
            .sum::<u64>()
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
#[path = "stream_txn_test.rs"]
mod tests;
