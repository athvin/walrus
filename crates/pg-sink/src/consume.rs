//! The decode loop: join the live [`ReplicationStream`] (PR 2.20) to the sync, pure `pgoutput`
//! decoder (PRs 2.2–2.8). The Rust analogue of the proof harness's `run-tests.sh` — an `INSERT` now
//! decodes to `Begin → Relation → Insert → Commit` against a real Postgres. No Arrow / batching / S3.
//!
//! **The seam that kept the decoder testable:** `pgoutput::parse_message` stays **sync + pure**; this
//! loop owns the I/O (`.await`s a frame) and calls the decoder synchronously on the returned `Bytes`.
//! The `StreamCtx` (are we inside a `Stream Start`/`Stop` block?) is threaded across frames by the
//! loop, since a v2 sub-xid prefix appears *only inside* a stream. Small txns still arrive whole at
//! commit (no stream frames), and `StreamCtx` handles both shapes with no special-casing here.

use crate::batch::{BatchTriggers, Clock, SealedBatch, TableBatcher};
use crate::health::HealthState;
use crate::heartbeat::{Heartbeat, InternalTables};
use crate::pgoutput::{self, Message, Reader, StreamCtx};
use crate::relcache::{is_internal_table, RelationCache};
use crate::replication::{ReplicationMessage, ReplicationStream};
use anyhow::Context;
use common::{Kind, Lsn, Op, SinkMeta, TupleValue, UtcTimestamp};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;

/// Drive the stream: decode each `XLogData`, register each `Relation` (cache + schema_registry), route
/// I/U/D into per-table batchers (sealing at commit boundaries), PUT sealed batches to S3, keep
/// keepalives answered (inside `ReplicationStream`), and exit cleanly on cancel or stream end.
// The loop driver wires together the full pipeline (cache, router, sink, control pool); its arity is
// intrinsic, not a code smell.
#[allow(clippy::too_many_arguments)]
pub async fn run_decode_loop(
    stream: &mut ReplicationStream,
    token: CancellationToken,
    cache: &mut RelationCache,
    router: &mut BatchRouter,
    sink: &crate::sink::ParquetSink,
    checkpoint: &mut crate::checkpoint::DurabilityCheckpoint,
    demux: &mut crate::stream_txn::StreamDemux,
    ddl: &mut crate::ddl::DdlConsumer,
    heartbeat: &mut Heartbeat,
    health: &HealthState,
    pool: &sqlx::PgPool,
    epoch: i64,
    _schema_version: i64, // structural version now rides the DDL consumer / cached shape (PR 2.33)
    waiters: &crate::reload_signal::WatermarkWaiters,
) -> anyhow::Result<()> {
    let mut ctx = StreamCtx::default();
    let mut internal = InternalTables::default();
    // reload_signal echoes buffered between their Insert and their transaction's fate (PR 6.3):
    // the watermark is the COMMIT LSN, which only the Commit message carries.
    let mut pending_signals = crate::reload_signal::PendingSignals::default();
    // Idle windows are monotonic (`tokio::time::Instant`); `last_activity` moves on every user change,
    // never on keepalives or the heartbeat's own round-trip.
    let mut last_activity = Instant::now();
    // Whether the transaction currently decoding carries the heartbeat change (its Commit lets the
    // checkpoint advance on an idle publication).
    let mut txn_has_heartbeat = false;
    // Check idleness at the beat cadence; the first (immediate) tick is a no-op (just-started).
    let mut beat_check = tokio::time::interval(heartbeat.idle_after());
    beat_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                // SIGTERM/SIGINT: the loop has stopped consuming — now run the ordered drain (NOT
                // cancellable; the caller bounds it by the K8s grace period). The slot is never dropped.
                tracing::info!("decode loop cancelled; draining");
                health.mark_terminating();
                let outcome = crate::shutdown::drain(stream, router, sink, checkpoint, pool, epoch)
                    .await
                    .context("graceful drain")?;
                tracing::info!(?outcome, "drain complete; slot left in place, resume on restart");
                return Ok(());
            }
            _ = beat_check.tick() => {
                // The beat fires over a SEPARATE SQL connection only when idle on both clocks; a
                // failure is logged and surfaced as `degraded`, never fatal (liveness never self-harms).
                let now = Instant::now();
                match heartbeat.maybe_beat(now, last_activity).await {
                    Ok(Some(seq)) => tracing::info!(beat_seq = seq, "fired idle heartbeat"),
                    Ok(None) => {}
                    Err(e) => tracing::warn!(error = %e, "heartbeat beat failed"),
                }
                health.set_degraded(heartbeat.degraded(now));
            }
            frame = stream.next() => {
                match frame.context("read replication frame")? {
                    None => {
                        tracing::info!("replication stream ended");
                        return Ok(());
                    }
                    Some(frame) => {
                        // The change's LSN is the XLogData frame's start (pgoutput change messages
                        // carry no per-change LSN of their own).
                        let frame_lsn = match &frame {
                            ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                            ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
                        };
                        if let Some(msg) = on_frame(&mut ctx, frame)? {
                            trace_message(&msg);
                            match &msg {
                                Message::Relation { relation, .. } => {
                                    // Learn walrus.heartbeat / walrus.ddl_audit OIDs BEFORE their change
                                    // arrives (Relation always precedes the change in the same txn).
                                    internal.note_relation(relation);
                                    // Register user tables at their CURRENT structural version (bumped by
                                    // DDL capture, PR 2.33) so the new-shape file carries the new version.
                                    let version = ddl.version_of(&relation.schema, &relation.name);
                                    on_relation(cache, pool, epoch, relation.clone(), version).await?;
                                }
                                // The DDL signal: a walrus.ddl_audit INSERT. Write a ddl_manifest row,
                                // bump the affected table's structural schema_version, and cut a fresh
                                // file. NEVER materialised as user data.
                                Message::Insert { relation_oid, new, .. }
                                    if internal.is_ddl_audit(*relation_oid) =>
                                {
                                    if let Some(rel) = internal.ddl_audit_rel() {
                                        let ev = crate::ddl::DdlEvent::from_tuple(rel, new)
                                            .context("parse ddl_audit tuple")?;
                                        let structural = ddl
                                            .consume(pool, &ev)
                                            .await
                                            .context("consume ddl event")?;
                                        if let Some(new_version) = structural {
                                            // Cut the old-version file for that table, then flush it.
                                            let sealed = router.cut_table(cache, &ev.source_schema, &ev.source_table)?;
                                            flush_sealed(sealed, stream, sink, checkpoint, pool, epoch).await?;
                                            tracing::info!(
                                                source_table = %format_args!("{}.{}", ev.source_schema, ev.source_table),
                                                c_tag = %ev.c_tag,
                                                schema_version = new_version,
                                                c_lsn = %ev.c_lsn,
                                                "DDL: manifest + version bump + file cut"
                                            );
                                        } else {
                                            tracing::info!(c_tag = %ev.c_tag, "DDL: metadata-only (recorded, no bump)");
                                        }
                                    }
                                }
                                // The reload echo (PR 6.3): the sink's own signal INSERT returning
                                // through the stream. Buffered here; the waiter resolves at the
                                // transaction's Commit with its commit LSN (= the chunk watermark
                                // L_i). NEVER batched, never a Parquet file or manifest row.
                                Message::Insert { relation_oid, new, xid }
                                    if internal.is_reload_signal(*relation_oid) =>
                                {
                                    if let Some(rel) = internal.reload_signal_rel() {
                                        match crate::reload_signal::PendingSignal::from_tuple(rel, new, *xid) {
                                            Some(sig) => pending_signals.push(sig),
                                            None => tracing::warn!(
                                                "malformed walrus.reload_signal tuple; ignoring"
                                            ),
                                        }
                                    }
                                }
                                // The heartbeat round-trip: record it, mark the txn, and NEVER stage it
                                // to S3 / a manifest row — it is control-plane, not user data. Other
                                // internal tables' non-signal ops (a ddl_audit UPDATE, a reload_signal
                                // UPDATE — neither should happen) are consumed-and-ignored here: only
                                // the heartbeat's tuple carries a beat_seq.
                                Message::Insert { relation_oid, new, .. }
                                | Message::Update { relation_oid, new, .. }
                                    if internal.is_internal(*relation_oid) =>
                                {
                                    if internal.is_heartbeat(*relation_oid) {
                                        if let Some(seq) = internal.beat_seq_of(new) {
                                            heartbeat.observe_return(seq, Instant::now());
                                            tracing::info!(beat_seq = seq, "heartbeat round-trip observed");
                                        }
                                        txn_has_heartbeat = true;
                                    }
                                }
                                // Non-insert ops on internal tables — e.g. the future reload_signal
                                // pruning DELETEs (PR 6.11's runbook) — are consumed-and-ignored:
                                // acked like any record, never routed toward a batcher.
                                Message::Delete { relation_oid, .. }
                                    if internal.is_internal(*relation_oid) => {}
                                Message::Commit { commit_lsn, .. } => {
                                    // First seal/flush any user batch this commit made eligible.
                                    flush_sealed(
                                        router.route(cache, &msg, frame_lsn, _schema_version)?,
                                        stream, sink, checkpoint, pool, epoch,
                                    )
                                    .await?;
                                    // Resolve any signal echoes this transaction carried: its commit
                                    // LSN IS the chunk watermark L_i (PR 6.3). The signal txn needs no
                                    // special ack — confirmed_flush passes it like any consumed record.
                                    pending_signals.on_commit(*commit_lsn, waiters);
                                    // Then, for an idle heartbeat-only txn, advance to its commit LSN —
                                    // but never past un-durable user data (a floor the flush above just
                                    // cleared if it was eligible).
                                    if std::mem::take(&mut txn_has_heartbeat) {
                                        if let Some(floor) = router.undurable_floor() {
                                            tracing::warn!(
                                                floor = %floor,
                                                "heartbeat: un-durable buffered data precedes the beat; holding confirmed_flush"
                                            );
                                        } else {
                                            checkpoint.on_batch_durable(*commit_lsn);
                                            checkpoint
                                                .send(stream, false)
                                                .await
                                                .context("send heartbeat standby status")?;
                                            tracing::info!(
                                                confirmed_flush = %checkpoint.confirmed_flush(),
                                                "idle heartbeat advanced confirmed_flush"
                                            );
                                        }
                                        health.set_degraded(heartbeat.degraded(Instant::now()));
                                    }
                                }
                                // --- Large-transaction streaming (§1.6, PR 2.30). A txn over
                                // logical_decoding_work_mem arrives BEFORE its commit as interleaved
                                // Stream blocks; the demux stages speculatively and commit-gates.
                                Message::StreamStart { xid, first_segment } => {
                                    demux.on_stream_start(*xid, *first_segment, frame_lsn);
                                    checkpoint.set_open_txn_floor(demux.open_floor());
                                }
                                Message::StreamStop => demux.on_stream_stop(),
                                m @ (Message::Insert { xid: Some(_), .. }
                                | Message::Update { xid: Some(_), .. }
                                | Message::Delete { xid: Some(_), .. }) => {
                                    last_activity = Instant::now();
                                    demux.on_change(cache, m, sink, frame_lsn).await?;
                                }
                                Message::StreamCommit {
                                    xid,
                                    commit_lsn,
                                    commit_ts,
                                    ..
                                } => {
                                    // Commit-order fence (architecture.md §1.6): any regular (non-streamed)
                                    // txn that committed WHILE this large txn was streaming is still buffered
                                    // in the router — small batches flush on the `max_fill` cadence, not per
                                    // commit — and its commit LSN is LOWER than this one. Flush those `ready`
                                    // rows FIRST so the manifest stays in commit-LSN order. Otherwise this
                                    // txn's file (higher `lsn_end`) becomes `ready` first, the loader
                                    // transforms + advances `transformed_lsn` past it, and the late,
                                    // lower-LSN regular file is then permanently skipped by the `>= ` window.
                                    // (The slot stays clamped to this still-open txn's floor until its
                                    // `on_batch_durable` below, so draining early never advances past it.)
                                    flush_sealed(
                                        router.drain_committed()?,
                                        stream, sink, checkpoint, pool, epoch,
                                    )
                                    .await?;
                                    // Materialise the survivors (aborted sub-xids excluded) to `ready`
                                    // (lsn_end = commit_lsn), then advance the slot — clamped to any
                                    // still-older open txn.
                                    let objs = demux
                                        .on_stream_commit(
                                            *xid,
                                            *commit_lsn,
                                            UtcTimestamp::from_pg_micros(*commit_ts)?,
                                            cache,
                                            sink,
                                        )
                                        .await?;
                                    for obj in &objs {
                                        crate::manifest::record_ready(pool, epoch, obj)
                                            .await
                                            .context("commit streamed manifest ready row")?;
                                    }
                                    checkpoint.set_open_txn_floor(demux.open_floor());
                                    checkpoint.on_batch_durable(*commit_lsn);
                                    checkpoint
                                        .send(stream, false)
                                        .await
                                        .context("send streamed-commit standby status")?;
                                    // Can't-happen defense (PR 6.3): a single-row signal txn never
                                    // streams, but if one somehow did, its surviving echo resolves here.
                                    pending_signals.on_stream_commit(*commit_lsn, waiters);
                                    tracing::info!(
                                        xid,
                                        files = objs.len(),
                                        commit_lsn = %commit_lsn,
                                        confirmed_flush = %checkpoint.confirmed_flush(),
                                        "streamed txn committed → ready"
                                    );
                                }
                                Message::StreamAbort { top_xid, sub_xid } => {
                                    // sub == top → whole-txn drop; sub != top → exclude the rolled-back
                                    // savepoint's rows (proto §9b) while the top-level txn commits on.
                                    demux.on_stream_abort(*top_xid, *sub_xid, sink).await;
                                    checkpoint.set_open_txn_floor(demux.open_floor());
                                    // An aborted (sub)transaction's signal echo must never resolve a
                                    // waiter — the commit never carried it (PR 6.3).
                                    pending_signals.on_stream_abort(*top_xid, *sub_xid);
                                }
                                other => {
                                    // A user change is activity — it suppresses the idle beat.
                                    if matches!(other, Message::Insert { .. } | Message::Update { .. } | Message::Delete { .. }) {
                                        last_activity = Instant::now();
                                    }
                                    flush_sealed(
                                        router.route(cache, other, frame_lsn, _schema_version)?,
                                        stream, sink, checkpoint, pool, epoch,
                                    )
                                    .await?;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// PUT each sealed batch, commit its manifest row, then advance the durability checkpoint to its
/// `lsn_end` and tell the server — the strict (a) PUT → (b) manifest → (c) slot ordering of §1.5.
async fn flush_sealed(
    sealed: Vec<SealedBatch>,
    stream: &mut ReplicationStream,
    sink: &crate::sink::ParquetSink,
    checkpoint: &mut crate::checkpoint::DurabilityCheckpoint,
    pool: &sqlx::PgPool,
    epoch: i64,
) -> anyhow::Result<()> {
    for batch in sealed {
        // Durability steps (a) PUT then (b) commit the manifest row — pumping unconditional keepalive
        // throughout so a slow or stalled S3 flush can't starve the walsender past `wal_sender_timeout`
        // (§1.9). The flush touches the object store + control DB, never the replication socket.
        let written = flush_batch_keepalive(stream, sink, pool, epoch, batch).await?;
        // Step (c): ONLY now advance confirmed_flush and tell the server.
        checkpoint.on_batch_durable(written.lsn_end);
        checkpoint
            .send(stream, false)
            .await
            .context("send durability standby status")?;
        tracing::info!(
            uri = %written.s3_uri,
            lsn_end = %written.lsn_end,
            confirmed_flush = %checkpoint.confirmed_flush(),
            "durable: object + manifest + slot advanced"
        );
    }
    Ok(())
}

/// Await the durable flush ([`flush_batch`]: S3 PUT + manifest commit) while pumping **unconditional**
/// keepalive feedback on the stream every feedback interval — so a slow or stalled S3 PUT can't starve
/// the walsender past `wal_sender_timeout` (§1.9). The flush future touches the object store and control
/// DB, never the replication socket, so the keepalive rides concurrently; and `tokio::select!` runs the
/// *chosen* branch's body to completion (the flush future is parked, never dropped, while a keepalive
/// sends), so a feedback frame is never cancelled mid-write. `confirmed_flush` is untouched here — it
/// advances only after this returns (the caller's `on_batch_durable`), so the pumped feedback carries
/// the pre-flush durable baseline (received advances as `write`, `flush`/`apply` hold — the two-LSN rule).
async fn flush_batch_keepalive(
    stream: &mut ReplicationStream,
    sink: &crate::sink::ParquetSink,
    ex: &sqlx::PgPool,
    epoch: i64,
    batch: SealedBatch,
) -> anyhow::Result<crate::sink::WrittenObject> {
    let flush = flush_batch(sink, ex, epoch, batch);
    tokio::pin!(flush);
    loop {
        let budget = stream.feedback_budget();
        tokio::select! {
            biased;
            written = &mut flush => return written,
            _ = sleep(budget) => stream
                .send_received_feedback(false)
                .await
                .context("keepalive during a stalled flush")?,
        }
    }
}

/// Flush a sealed batch durably: **(a)** PUT the Parquet object to S3, **then (b)** commit the
/// `file_manifest` `ready` row — never the other way round (§1.5). Step (c) — advancing the slot to
/// `obj.lsn_end` — is PR 2.26. A crash between (a) and (b) is safe: the batch re-streams (no `ready`
/// row was committed), at-least-once.
pub async fn flush_batch(
    sink: &crate::sink::ParquetSink,
    ex: impl sqlx::PgExecutor<'_>,
    epoch: i64,
    batch: crate::batch::SealedBatch,
) -> anyhow::Result<crate::sink::WrittenObject> {
    flush_batch_kind(sink, ex, epoch, batch, crate::sink::FileKind::Stream).await
}

/// As [`flush_batch`], stamping the object + manifest `kind` — the backfill (PR 2.29) flushes with
/// [`crate::sink::FileKind::Snapshot`].
pub async fn flush_batch_kind(
    sink: &crate::sink::ParquetSink,
    ex: impl sqlx::PgExecutor<'_>,
    epoch: i64,
    batch: crate::batch::SealedBatch,
    kind: crate::sink::FileKind,
) -> anyhow::Result<crate::sink::WrittenObject> {
    // (a) durable in S3.
    let obj = sink
        .put_with_kind(batch, kind)
        .await
        .context("PUT parquet object to S3 (durability a)")?;
    // (b) committed in the control DB.
    crate::manifest::record_ready(ex, epoch, &obj)
        .await
        .context("commit manifest ready row (durability b)")?;
    Ok(obj)
}

/// Routes decoded changes into per-table [`TableBatcher`]s and seals at commit boundaries. Owns the
/// per-table batchers + the sink context stamped into each row's `walrus_pg_sink_meta`.
pub struct BatchRouter {
    batchers: HashMap<u32, TableBatcher>,
    triggers: BatchTriggers,
    clock: Arc<dyn Clock>,
    epoch: i64,
    sink_instance: String,
    /// The current transaction's top-level xid (from `Begin`), used when a change carries no xid
    /// (non-streamed txns).
    txn_xid: u32,
}

impl BatchRouter {
    pub fn new(
        triggers: BatchTriggers,
        clock: Arc<dyn Clock>,
        epoch: i64,
        sink_instance: String,
    ) -> Self {
        BatchRouter {
            batchers: HashMap::new(),
            triggers,
            clock,
            epoch,
            sink_instance,
            txn_xid: 0,
        }
    }

    /// Route one decoded message. `Begin` sets the txn context; `I/U/D` buffer against the open txn;
    /// `Commit` promotes them and returns any batches that a trigger sealed. Streamed large txns
    /// (`Stream*`) and `Truncate`/`Message` are deferred (PR 2.30 / 2.27 / 2.33).
    pub fn route(
        &mut self,
        cache: &RelationCache,
        msg: &Message,
        frame_lsn: Lsn,
        schema_version: i64,
    ) -> anyhow::Result<Vec<SealedBatch>> {
        match msg {
            Message::Begin { xid, .. } => {
                self.txn_xid = *xid;
                Ok(Vec::new())
            }
            Message::Insert {
                relation_oid,
                new,
                xid,
            } => {
                self.push(
                    cache,
                    *relation_oid,
                    Op::Insert,
                    new,
                    frame_lsn,
                    xid.unwrap_or(self.txn_xid),
                    schema_version,
                )?;
                Ok(Vec::new())
            }
            Message::Update {
                relation_oid,
                new,
                xid,
                ..
            } => {
                self.push(
                    cache,
                    *relation_oid,
                    Op::Update,
                    new,
                    frame_lsn,
                    xid.unwrap_or(self.txn_xid),
                    schema_version,
                )?;
                Ok(Vec::new())
            }
            Message::Delete {
                relation_oid,
                old,
                xid,
                ..
            } => {
                // The old-key tuple is full-width (non-key columns as NULL under DEFAULT identity).
                self.push(
                    cache,
                    *relation_oid,
                    Op::Delete,
                    old,
                    frame_lsn,
                    xid.unwrap_or(self.txn_xid),
                    schema_version,
                )?;
                Ok(Vec::new())
            }
            Message::Commit {
                commit_lsn,
                commit_ts,
                ..
            } => self.commit(*commit_lsn, UtcTimestamp::from_pg_micros(*commit_ts)?),
            _ => Ok(Vec::new()),
        }
    }

    // Each parameter is one provenance field the per-row `SinkMeta` must carry (oid, op, values, lsn,
    // xid, schema_version) — the arity mirrors the CDC contract, not an extractable clump.
    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        cache: &RelationCache,
        oid: u32,
        op: Op,
        values: &[TupleValue],
        frame_lsn: Lsn,
        xid: u32,
        _schema_version: i64, // the version now rides the cached shape (bumped by DDL capture, PR 2.33)
    ) -> anyhow::Result<()> {
        // Always the LATEST cached shape for this OID — so a change after a DDL bump lands in a
        // new-version file (the homogeneous-file rule; the batcher was cut on the bump).
        let Some(cached) = cache.latest_for(oid) else {
            tracing::warn!(
                relation_oid = oid,
                "change for a relation with no cached shape yet; skipping"
            );
            return Ok(());
        };
        let triggers = self.triggers;
        let clock = self.clock.clone();
        let batcher = match self.batchers.entry(oid) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(
                TableBatcher::new(cached.clone(), triggers, clock)
                    .context("create table batcher")?,
            ),
        };
        let meta = SinkMeta {
            op,
            lsn: frame_lsn,
            commit_lsn: Lsn::ZERO, // patched at the batcher's on_commit
            commit_ts: UtcTimestamp::now(), // placeholder — patched at on_commit from Commit's ts (PR 5.9)
            xid,
            epoch: self.epoch,
            batch_id: String::new(), // assigned by the batcher when the batch opens
            schema_version: cached.schema_version,
            source_schema: cached.relation.schema.clone(),
            source_table: cached.relation.name.clone(),
            kind: Kind::Stream,
            unchanged_toast: vec![],
            sink_instance: self.sink_instance.clone(),
            sink_processed_at: UtcTimestamp::now(),
        };
        batcher.push(meta, values);
        Ok(())
    }

    /// Cut the current file for `schema.table` (PR 2.33): force-seal its batcher so the pre-DDL rows
    /// flush at the old `schema_version`, and drop the batcher so the next change rebuilds it from the
    /// new-version shape. Returns the sealed old-version batch, if any.
    pub fn cut_table(
        &mut self,
        cache: &RelationCache,
        schema: &str,
        table: &str,
    ) -> anyhow::Result<Vec<SealedBatch>> {
        let Some(oid) = cache.oid_for(schema, table) else {
            return Ok(Vec::new()); // never buffered this table yet — nothing to cut
        };
        let mut sealed = Vec::new();
        if let Some(mut batcher) = self.batchers.remove(&oid) {
            if let Some(batch) = batcher.drain_committed().context("cut table on DDL bump")? {
                sealed.push(batch);
            }
        }
        Ok(sealed)
    }

    fn commit(
        &mut self,
        commit_lsn: Lsn,
        commit_ts: UtcTimestamp,
    ) -> anyhow::Result<Vec<SealedBatch>> {
        let mut sealed = Vec::new();
        for batcher in self.batchers.values_mut() {
            batcher
                .on_commit(commit_lsn, commit_ts)
                .context("promote committed rows")?;
            if batcher.should_flush() {
                sealed.push(batcher.seal().context("seal batch")?);
            }
        }
        Ok(sealed)
    }

    /// The earliest commit LSN of any committed-but-unsealed row across all tables, or `None` if
    /// nothing is buffered. An idle heartbeat must not advance `confirmed_flush` past this (PR 2.27).
    pub fn undurable_floor(&self) -> Option<Lsn> {
        self.batchers
            .values()
            .filter_map(TableBatcher::undurable_floor)
            .min()
    }

    /// Graceful-drain seal (PR 2.28): seal every table's in-flight **committed** batch, dropping any
    /// open speculative buffers. The returned batches are flushed with the usual PUT → manifest → slot
    /// ordering before the final standby update.
    pub fn drain_committed(&mut self) -> anyhow::Result<Vec<SealedBatch>> {
        let mut sealed = Vec::new();
        for batcher in self.batchers.values_mut() {
            if let Some(batch) = batcher.drain_committed().context("drain committed batch")? {
                sealed.push(batch);
            }
        }
        Ok(sealed)
    }
}

/// On a `Relation` message: build the Arrow schema + descriptors, cache them under
/// `(oid, schema_version)`, and **persist** the `schema_registry` row (idempotent on
/// `(epoch, schema, table, version)`). Internal walrus tables are never registered. The persist is a
/// control-DB write, so this is `async`; the order is build → cache → persist.
pub async fn on_relation(
    cache: &mut RelationCache,
    ex: impl sqlx::PgExecutor<'_>,
    epoch: i64,
    relation: common::PgRelation,
    schema_version: i64,
) -> anyhow::Result<()> {
    if is_internal_table(&relation.schema, &relation.name) {
        return Ok(());
    }
    let cached = cache
        .upsert_from_relation(relation, schema_version)
        .context("build Arrow schema for relation")?;
    let row = control::RegistryRow {
        epoch,
        source_schema: cached.relation.schema.clone(),
        source_table: cached.relation.name.clone(),
        schema_version,
        descriptors: cached.descriptors.clone(),
        columns: serde_json::to_value(&cached.relation).context("serialize relation snapshot")?,
    };
    control::upsert_registry(ex, &row)
        .await
        .context("upsert schema_registry")?;
    tracing::info!(
        source_table = %format_args!("{}.{}", cached.relation.schema, cached.relation.name),
        schema_version,
        "registered relation"
    );
    Ok(())
}

/// Route one live frame. A keepalive is a no-op here (its feedback is sent inside the stream); an
/// `XLogData` payload is decoded by the **existing** `pgoutput::parse_message`, which updates `ctx`
/// on `Stream Start`/`Stop`. A decode error on a real message is a bug — fail loud.
pub fn on_frame(ctx: &mut StreamCtx, frame: ReplicationMessage) -> anyhow::Result<Option<Message>> {
    match frame {
        ReplicationMessage::Keepalive { .. } => Ok(None),
        ReplicationMessage::XLogData { data, .. } => {
            let mut reader = Reader::new(&data);
            let msg = pgoutput::parse_message(&mut reader, ctx)
                .context("decode pgoutput XLogData payload")?;
            Ok(Some(msg))
        }
    }
}

/// Structured log for one decoded message — **fields, not string interpolation**, so logs stay
/// queryable (`op`, `source_table`, `commit_lsn`, `lsn`, `xid`).
fn trace_message(msg: &Message) {
    match msg {
        Message::Begin { final_lsn, xid, .. } => {
            tracing::info!(op = "begin", xid, final_lsn = %final_lsn, "decoded")
        }
        Message::Commit {
            commit_lsn,
            end_lsn,
            ..
        } => tracing::info!(op = "commit", commit_lsn = %commit_lsn, end_lsn = %end_lsn, "decoded"),
        Message::Origin { commit_lsn, name } => {
            tracing::info!(op = "origin", commit_lsn = %commit_lsn, name, "decoded")
        }
        Message::Relation { xid, relation } => tracing::info!(
            op = "relation",
            xid = ?xid,
            source_table = %format_args!("{}.{}", relation.schema, relation.name),
            relation_oid = relation.oid,
            "decoded"
        ),
        Message::Type { xid, oid, name, .. } => {
            tracing::info!(op = "type", xid = ?xid, type_oid = oid, name, "decoded")
        }
        Message::Insert {
            xid,
            relation_oid,
            new,
        } => tracing::info!(op = "insert", xid = ?xid, relation_oid, cols = new.len(), "decoded"),
        Message::Update {
            xid, relation_oid, ..
        } => tracing::info!(op = "update", xid = ?xid, relation_oid, "decoded"),
        Message::Delete {
            xid, relation_oid, ..
        } => tracing::info!(op = "delete", xid = ?xid, relation_oid, "decoded"),
        Message::Truncate { xid, relations, .. } => {
            tracing::info!(op = "truncate", xid = ?xid, relations = relations.len(), "decoded")
        }
        Message::Message {
            xid,
            transactional,
            lsn,
            prefix,
            ..
        } => {
            tracing::info!(op = "message", xid = ?xid, transactional, lsn = %lsn, prefix, "decoded")
        }
        Message::StreamStart { xid, first_segment } => {
            tracing::info!(op = "stream_start", xid, first_segment, "decoded")
        }
        Message::StreamStop => tracing::info!(op = "stream_stop", "decoded"),
        Message::StreamCommit {
            xid, commit_lsn, ..
        } => tracing::info!(op = "stream_commit", xid, commit_lsn = %commit_lsn, "decoded"),
        Message::StreamAbort { top_xid, sub_xid } => {
            tracing::info!(op = "stream_abort", top_xid, sub_xid, "decoded")
        }
        // Two-phase (v3) frames never occur at v2; log opaquely rather than special-case.
        other => tracing::info!(op = "other", detail = ?other, "decoded"),
    }
}
