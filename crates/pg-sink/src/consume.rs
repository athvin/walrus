//! The decode loop: join the live [`ReplicationStream`] to the sync, pure [`pgoutput`]
//! decoder. The Rust analogue of the proof harness's `run-tests.sh` — an `INSERT` now
//! decodes to `Begin → Relation → Insert → Commit` against a real Postgres. No Arrow / batching / S3.
//!
//! **The seam that kept the decoder testable:** [`pgoutput::parse_message`] stays **sync + pure**;
//! this loop owns the I/O (`.await`s a frame) and calls the decoder synchronously on the returned
//! `Bytes`. The [`StreamCtx`] (are we inside a `Stream Start`/`Stop` block?) is threaded across
//! frames by the loop, since a v2 sub-xid prefix appears *only inside* a stream. Small txns still
//! arrive whole at commit (no stream frames), and [`StreamCtx`] handles both shapes with no
//! special-casing here.

use crate::batch::{BatchTriggers, Clock, SealedBatch, TableBatcher};
use crate::health::HealthState;
use crate::heartbeat::{Heartbeat, InternalTables};
use crate::pgoutput::{self, Message, Reader, StreamCtx};
use crate::relcache::{RelationCache, is_internal_table};
use crate::replication::{ReplicationMessage, ReplicationStream};
use anyhow::Context;
use common::{EpochNo, Kind, Lsn, Op, SinkMeta, TupleValue, UtcTimestamp};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

/// A missing required field at decode-loop build time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("decode loop builder: missing required field `{0}`")]
pub struct DecodeLoopError(&'static str);

/// The outcome carried beyond the pinned frame future's borrow of the replication stream.
enum FrameEvent {
    Cancelled,
    Frame(anyhow::Result<Option<ReplicationMessage>>),
}

fn transaction_scope(
    xid: Option<u32>,
    current_top: Option<u32>,
) -> anyhow::Result<crate::ddl::TransactionScope> {
    match xid {
        Some(sub_xid) => Ok(crate::ddl::TransactionScope::Streamed {
            top_xid: current_top.context("xid-prefixed message arrived outside StreamStart")?,
            sub_xid,
        }),
        None => Ok(crate::ddl::TransactionScope::Ordinary),
    }
}

/// The fully wired decode loop. Construct it with [`DecodeLoop::builder`]; [`DecodeLoop::run`]
/// consumes the wiring so its mutable borrows cannot escape or be reused concurrently.
#[derive(Debug)]
pub struct DecodeLoop<'a, C> {
    stream: &'a mut ReplicationStream,
    token: CancellationToken,
    cache: &'a mut RelationCache,
    router: &'a mut BatchRouter<C>,
    sink: &'a crate::sink::ParquetSink,
    checkpoint: &'a mut crate::checkpoint::DurabilityCheckpoint,
    demux: &'a mut crate::stream_txn::StreamDemux<C>,
    ddl: &'a mut crate::ddl::DdlConsumer,
    heartbeat: &'a mut Heartbeat,
    health: &'a HealthState,
    pool: &'a sqlx::PgPool,
    epoch: EpochNo,
    waiters: &'a crate::reload_signal::WatermarkWaiters,
}

/// Wires a [`DecodeLoop`]. Every setter consumes and returns the builder, so the chain must be kept:
/// a dropped intermediate silently discards that field.
///
/// Ignoring a setter's return value is a compile error:
///
/// ```compile_fail
/// #![deny(unused_must_use)]
/// # let builder = pg_sink::consume::DecodeLoop::<
/// #     std::sync::Arc<pg_sink::batch::SystemClock>,
/// # >::builder();
/// builder.epoch(common::EpochNo(7));
/// ```
#[must_use = "a builder does nothing until you call build()"]
#[derive(Debug)]
pub struct DecodeLoopBuilder<'a, C> {
    stream: Option<&'a mut ReplicationStream>,
    token: Option<CancellationToken>,
    cache: Option<&'a mut RelationCache>,
    router: Option<&'a mut BatchRouter<C>>,
    sink: Option<&'a crate::sink::ParquetSink>,
    checkpoint: Option<&'a mut crate::checkpoint::DurabilityCheckpoint>,
    demux: Option<&'a mut crate::stream_txn::StreamDemux<C>>,
    ddl: Option<&'a mut crate::ddl::DdlConsumer>,
    heartbeat: Option<&'a mut Heartbeat>,
    health: Option<&'a HealthState>,
    pool: Option<&'a sqlx::PgPool>,
    epoch: Option<EpochNo>,
    waiters: Option<&'a crate::reload_signal::WatermarkWaiters>,
}

impl<C> Default for DecodeLoopBuilder<'_, C> {
    fn default() -> Self {
        Self {
            stream: None,
            token: None,
            cache: None,
            router: None,
            sink: None,
            checkpoint: None,
            demux: None,
            ddl: None,
            heartbeat: None,
            health: None,
            pool: None,
            epoch: None,
            waiters: None,
        }
    }
}

impl<'a, C> DecodeLoop<'a, C> {
    /// An empty [`DecodeLoopBuilder`]. Every field is required, so this is the only constructor —
    /// there is no `new` that could leave a collaborator unwired.
    pub fn builder() -> DecodeLoopBuilder<'a, C> {
        DecodeLoopBuilder::default()
    }
}

impl<C: Clock + Clone> DecodeLoop<'_, C> {
    /// Drive the stream: decode each `XLogData`, register each `Relation` (cache + schema_registry),
    /// route I/U/D into per-table batchers (sealing at commit boundaries), PUT sealed batches to S3,
    /// keep keepalives answered, and exit cleanly on cancel or stream end.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] when replication I/O or decoding fails, a relation/DDL/reload
    /// invariant is invalid, Arrow batching or S3/manifest durability fails, or a control-plane
    /// operation cannot complete. Context on the returned chain identifies the failed stage.
    ///
    /// # Panics
    ///
    /// Panics if the wired [`Heartbeat`]'s `idle_after` is zero: it is this loop's idle-check
    /// cadence, and [`tokio::time::interval`] rejects a zero period. `config.rs` bounds the
    /// configured value above zero.
    pub async fn run(self) -> anyhow::Result<()> {
        let DecodeLoop {
            stream,
            token,
            cache,
            router,
            sink,
            checkpoint,
            demux,
            ddl,
            heartbeat,
            health,
            pool,
            epoch,
            waiters,
        } = self;
        // `BatchRouter::route` retains its integration-test seam, but the cached relation owns the
        // structural version now; this compatibility argument is intentionally ignored there.
        let schema_version = common::SchemaVersionNo(0);
        let mut ctx = StreamCtx::default();
        let mut internal = InternalTables::default();
        // reload_signal echoes buffered between their Insert and their transaction's fate:
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
            let event = {
                // Keep one frame future alive across heartbeat ticks. `next()` may be parked in a
                // feedback write, where dropping it would leave a partial CopyBoth frame.
                let frame = stream.next();
                tokio::pin!(frame);
                loop {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => break FrameEvent::Cancelled,
                        _ = beat_check.tick() => {
                            // The beat fires over a SEPARATE SQL connection only when idle on both clocks; a
                            // failure is logged and surfaced as `degraded`, never fatal (liveness never self-harms).
                            let now = Instant::now();
                            match heartbeat.maybe_beat(now, last_activity).await {
                                Ok(Some(seq)) => tracing::info!(beat_seq = seq, "fired idle heartbeat"),
                                Ok(None) => {}
                                Err(e) => tracing::warn!(error = %e, "heartbeat beat failed"),
                            }
                            health.set_degraded(heartbeat.is_degraded(now));
                        }
                        res = &mut frame => break FrameEvent::Frame(res),
                    }
                }
            };

            match event {
                FrameEvent::Cancelled => {
                    // SIGTERM/SIGINT: the loop has stopped consuming — now run the ordered drain (NOT
                    // cancellable; the caller bounds it by the K8s grace period). The slot is never dropped.
                    tracing::info!("decode loop cancelled; draining");
                    health.mark_terminating();
                    let outcome =
                        crate::shutdown::drain(stream, router, sink, checkpoint, pool, epoch)
                            .await
                            .context("graceful drain")?;
                    tracing::info!(
                        ?outcome,
                        "drain complete; slot left in place, resume on restart"
                    );
                    return Ok(());
                }
                FrameEvent::Frame(frame) => {
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
                                    Message::Relation { relation, xid } => {
                                        // Learn walrus.heartbeat / walrus.ddl_audit OIDs BEFORE their change
                                        // arrives (Relation always precedes the change in the same txn).
                                        internal.note_relation(relation);
                                        if !is_internal_table(&relation.schema, &relation.name) {
                                            let scope =
                                                transaction_scope(*xid, demux.current_top())?;
                                            let version = ddl.version_for(
                                                scope,
                                                &relation.schema,
                                                &relation.name,
                                            );
                                            let row = cache_relation(
                                                cache,
                                                epoch,
                                                relation.clone(),
                                                version,
                                            )?
                                            .context(
                                                "user Relation unexpectedly classified internal",
                                            )?;
                                            if let crate::ddl::TransactionScope::Streamed {
                                                top_xid,
                                                ..
                                            } = scope
                                            {
                                                demux.bind_relation(top_xid, relation.oid, version);
                                            } else {
                                                router.bind_relation(relation.oid, version);
                                            }
                                            if ddl.is_provisional(
                                                &relation.schema,
                                                &relation.name,
                                                version,
                                            ) {
                                                ddl.stage_registry(scope, row);
                                            } else {
                                                persist_registry(pool, &row).await?;
                                            }
                                        }
                                    }
                                    // The DDL signal is provisional until this source transaction commits.
                                    // Its structured snapshot is usable immediately INSIDE the transaction,
                                    // while manifest/registry publication waits for Commit/StreamCommit.
                                    Message::Insert {
                                        relation_oid,
                                        new,
                                        xid,
                                    } if internal.is_ddl_audit(*relation_oid) => {
                                        if let Some(rel) = internal.ddl_audit_rel() {
                                            let ev = crate::ddl::DdlEvent::from_tuple(rel, new)
                                                .context("parse ddl_audit tuple")?;
                                            let scope =
                                                transaction_scope(*xid, demux.current_top())?;
                                            let previous = ev
                                                .c_rel_oid
                                                .and_then(|oid| cache.latest_for(oid))
                                                .or_else(|| {
                                                    cache.latest_for_name(
                                                        &ev.source_schema,
                                                        &ev.source_table,
                                                    )
                                                })
                                                .map(|cached| cached.relation.clone());
                                            let observation = ddl.observe(scope, ev.clone());
                                            if let Some(new_version) =
                                                observation.structural_version
                                            {
                                                // Cache the trigger's authoritative post-change catalog
                                                // snapshot now, but commit-gate its registry row.
                                                if let Some(after) =
                                                    ev.relation_after(previous.as_ref())?
                                                    && let Some(row) = cache_relation(
                                                        cache,
                                                        epoch,
                                                        after,
                                                        new_version,
                                                    )?
                                                {
                                                    if observation.replay {
                                                        persist_registry(pool, &row).await?;
                                                    } else {
                                                        ddl.stage_registry(scope, row);
                                                    }
                                                }
                                                // Cut committed old-version rows now; preserve any open
                                                // ordinary pre-DDL rows as a commit-gated segment.
                                                let sealed = router.cut_table(
                                                    cache,
                                                    &ev.source_schema,
                                                    &ev.source_table,
                                                )?;
                                                flush_sealed(
                                                    sealed,
                                                    router.undurable_floor(),
                                                    stream,
                                                    sink,
                                                    checkpoint,
                                                    pool,
                                                    epoch,
                                                )
                                                .await?;
                                                tracing::info!(
                                                    source_table = %format_args!("{}.{}", ev.source_schema, ev.source_table),
                                                    c_tag = %ev.c_tag,
                                                    schema_version = %new_version,
                                                    capture_lsn = %ev.capture_lsn,
                                                    replay = observation.replay,
                                                    "DDL staged: provisional version + file boundary"
                                                );
                                            } else {
                                                tracing::info!(
                                                    c_tag = %ev.c_tag,
                                                    replay = observation.replay,
                                                    "DDL staged: metadata-only"
                                                );
                                            }
                                        }
                                    }
                                    // The reload echo: the sink's own signal INSERT returning
                                    // through the stream. Buffered here; the waiter resolves at the
                                    // transaction's Commit with its commit LSN (= the chunk watermark
                                    // L_i). NEVER batched, never a Parquet file or manifest row.
                                    Message::Insert {
                                        relation_oid,
                                        new,
                                        xid,
                                    } if internal.is_reload_signal(*relation_oid) => {
                                        if let Some(rel) = internal.reload_signal_rel() {
                                            match crate::reload_signal::PendingSignal::from_tuple(
                                                rel, new, *xid,
                                            ) {
                                                Ok(sig) => pending_signals.push(sig),
                                                Err(error) => tracing::warn!(
                                                    %error,
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
                                    Message::Insert {
                                        relation_oid, new, ..
                                    }
                                    | Message::Update {
                                        relation_oid, new, ..
                                    } if internal.is_internal(*relation_oid) => {
                                        if internal.is_heartbeat(*relation_oid) {
                                            if let Some(seq) = internal.beat_seq_of(new) {
                                                heartbeat.observe_return(seq, Instant::now());
                                                tracing::info!(
                                                    beat_seq = seq,
                                                    "heartbeat round-trip observed"
                                                );
                                            }
                                            txn_has_heartbeat = true;
                                        }
                                    }
                                    // Non-insert ops on internal tables — e.g. operator-run
                                    // reload_signal pruning DELETEs — are consumed-and-ignored:
                                    // acked like any record, never routed toward a batcher.
                                    Message::Delete { relation_oid, .. }
                                        if internal.is_internal(*relation_oid) => {}
                                    m @ Message::Commit { commit_lsn, .. } => {
                                        // First seal/flush any user batch this commit made eligible.
                                        let sealed =
                                            router.route(cache, m, frame_lsn, schema_version)?;
                                        ddl.on_commit(pool, *commit_lsn)
                                            .await
                                            .context("commit ordinary DDL state")?;
                                        flush_sealed(
                                            sealed,
                                            router.undurable_floor(),
                                            stream,
                                            sink,
                                            checkpoint,
                                            pool,
                                            epoch,
                                        )
                                        .await?;
                                        // Resolve any signal echoes this transaction carried: its commit
                                        // LSN IS the chunk watermark L_i. The signal txn needs no
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
                                            health.set_degraded(
                                                heartbeat.is_degraded(Instant::now()),
                                            );
                                        }
                                    }
                                    // --- Large-transaction streaming (§1.6). A txn over
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
                                            router.undurable_floor(),
                                            stream,
                                            sink,
                                            checkpoint,
                                            pool,
                                            epoch,
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
                                        ddl.on_stream_commit(pool, *xid, *commit_lsn)
                                            .await
                                            .context("commit streamed DDL state")?;
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
                                        // Can't-happen defense: a single-row signal txn never
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
                                        for (schema, table, version) in
                                            ddl.on_stream_abort(*top_xid, *sub_xid)
                                        {
                                            cache.remove_version(&schema, &table, version);
                                        }
                                        checkpoint.set_open_txn_floor(demux.open_floor());
                                        // An aborted (sub)transaction's signal echo must never resolve a
                                        // waiter — the commit never carried it.
                                        pending_signals.on_stream_abort(*top_xid, *sub_xid);
                                    }
                                    other => {
                                        // A user change is activity — it suppresses the idle beat.
                                        if matches!(
                                            other,
                                            Message::Insert { .. }
                                                | Message::Update { .. }
                                                | Message::Delete { .. }
                                        ) {
                                            last_activity = Instant::now();
                                        }
                                        flush_sealed(
                                            router.route(
                                                cache,
                                                other,
                                                frame_lsn,
                                                schema_version,
                                            )?,
                                            router.undurable_floor(),
                                            stream,
                                            sink,
                                            checkpoint,
                                            pool,
                                            epoch,
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
}

impl<'a, C> DecodeLoopBuilder<'a, C> {
    /// The started replication stream the loop pulls CopyBoth frames from. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn stream(mut self, stream: &'a mut ReplicationStream) -> Self {
        self.stream = Some(stream);
        self
    }

    /// The shutdown token. Cancelling it makes the loop return after the current frame rather than
    /// mid-message, which is what lets the drain publish what is already committed. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub fn token(mut self, token: CancellationToken) -> Self {
        self.token = Some(token);
        self
    }

    /// The relation cache the loop resolves each message's OID against, and updates when a Relation
    /// message announces a new shape. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn cache(mut self, cache: &'a mut RelationCache) -> Self {
        self.cache = Some(cache);
        self
    }

    /// The per-table batcher set that decoded rows are routed into. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn router(mut self, router: &'a mut BatchRouter<C>) -> Self {
        self.router = Some(router);
        self
    }

    /// Where sealed batches are written as Parquet. Shared (`&`) rather than exclusive: the object
    /// store handles concurrent PUTs itself. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn sink(mut self, sink: &'a crate::sink::ParquetSink) -> Self {
        self.sink = Some(sink);
        self
    }

    /// The durability checkpoint that decides what LSN may be confirmed back to the source. Only it
    /// may advance `confirmed_flush`, and only after a batch is durable. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn checkpoint(
        mut self,
        checkpoint: &'a mut crate::checkpoint::DurabilityCheckpoint,
    ) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }

    /// The streamed-transaction demultiplexer, which buffers in-progress transactions separately so
    /// an uncommitted one is never published. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn demux(mut self, demux: &'a mut crate::stream_txn::StreamDemux<C>) -> Self {
        self.demux = Some(demux);
        self
    }

    /// The DDL consumer that turns `ddl_audit` rows into schema-version bumps. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn ddl(mut self, ddl: &'a mut crate::ddl::DdlConsumer) -> Self {
        self.ddl = Some(ddl);
        self
    }

    /// The heartbeat, which both fires beats while the source is idle and observes their return.
    /// Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn heartbeat(mut self, heartbeat: &'a mut Heartbeat) -> Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    /// The probe state the loop marks degraded or live. Shared, since the health server reads it
    /// concurrently. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn health(mut self, health: &'a HealthState) -> Self {
        self.health = Some(health);
        self
    }

    /// Control-Postgres handle for manifest and registry writes. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn pool(mut self, pool: &'a sqlx::PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// The generation every row and manifest row this loop produces is stamped with. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn epoch(mut self, epoch: EpochNo) -> Self {
        self.epoch = Some(epoch);
        self
    }

    /// The watermark waiters a decoded reload signal resolves, which is how a chunk export learns
    /// its commit LSN. Required.
    #[must_use = "builder methods return the modified builder — chain or assign"]
    pub const fn waiters(mut self, waiters: &'a crate::reload_signal::WatermarkWaiters) -> Self {
        self.waiters = Some(waiters);
        self
    }

    /// Construct the fully wired loop, naming the first missing required field.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeLoopError`] naming the first setter omitted from the builder chain.
    pub fn build(self) -> Result<DecodeLoop<'a, C>, DecodeLoopError> {
        Ok(DecodeLoop {
            stream: self.stream.ok_or(DecodeLoopError("stream"))?,
            token: self.token.ok_or(DecodeLoopError("token"))?,
            cache: self.cache.ok_or(DecodeLoopError("cache"))?,
            router: self.router.ok_or(DecodeLoopError("router"))?,
            sink: self.sink.ok_or(DecodeLoopError("sink"))?,
            checkpoint: self.checkpoint.ok_or(DecodeLoopError("checkpoint"))?,
            demux: self.demux.ok_or(DecodeLoopError("demux"))?,
            ddl: self.ddl.ok_or(DecodeLoopError("ddl"))?,
            heartbeat: self.heartbeat.ok_or(DecodeLoopError("heartbeat"))?,
            health: self.health.ok_or(DecodeLoopError("health"))?,
            pool: self.pool.ok_or(DecodeLoopError("pool"))?,
            epoch: self.epoch.ok_or(DecodeLoopError("epoch"))?,
            waiters: self.waiters.ok_or(DecodeLoopError("waiters"))?,
        })
    }
}

/// PUT every sealed batch and commit every manifest row before advancing the durability checkpoint.
/// Several files can share one transaction's commit LSN (multiple tables, or the pre/post-DDL schema
/// segments of one table); acknowledging after only the first sibling would make a crash lose the
/// rest. An older committed-but-unsealed batch is an additional fence: its commit has not reached S3,
/// so the slot cannot advance through its LSN yet.
async fn flush_sealed(
    sealed: Vec<SealedBatch>,
    undurable_floor: Option<Lsn>,
    stream: &mut ReplicationStream,
    sink: &crate::sink::ParquetSink,
    checkpoint: &mut crate::checkpoint::DurabilityCheckpoint,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) -> anyhow::Result<()> {
    let mut max_durable = None;
    for batch in sealed {
        // Durability steps (a) PUT then (b) commit the manifest row — pumping unconditional keepalive
        // throughout so a slow or stalled S3 flush can't starve the walsender past `wal_sender_timeout`
        // (§1.9). The flush touches the object store + control DB, never the replication socket.
        let written = flush_batch_keepalive(stream, sink, pool, epoch, batch).await?;
        max_durable =
            Some(max_durable.map_or(written.lsn_end, |lsn: Lsn| lsn.max(written.lsn_end)));
        tracing::info!(
            uri = %written.s3_uri,
            lsn_end = %written.lsn_end,
            "durable: object + manifest committed"
        );
    }
    let Some(frontier) = durable_frontier(max_durable, undurable_floor) else {
        if let (Some(durable), Some(floor)) = (max_durable, undurable_floor)
            && floor <= durable
        {
            tracing::info!(
                durable_through = %durable,
                undurable_floor = %floor,
                confirmed_flush = %checkpoint.confirmed_flush(),
                "slot held behind an older committed-but-unsealed batch"
            );
        }
        return Ok(());
    };
    // Step (c): all files at/below this frontier are now represented by durable manifests.
    checkpoint.on_batch_durable(frontier);
    checkpoint
        .send(stream, false)
        .await
        .context("send durability standby status")?;
    tracing::info!(
        durable_through = %frontier,
        confirmed_flush = %checkpoint.confirmed_flush(),
        "durable file group complete; slot advanced"
    );
    Ok(())
}

/// Highest newly-durable LSN that is strictly before any committed-but-unsealed row.
fn durable_frontier(max_durable: Option<Lsn>, undurable_floor: Option<Lsn>) -> Option<Lsn> {
    match (max_durable, undurable_floor) {
        (Some(durable), Some(floor)) if floor <= durable => None,
        (durable, _) => durable,
    }
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
    epoch: EpochNo,
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
/// `obj.lsn_end` — happens in the checkpoint caller. A crash between (a) and (b) is safe: the batch
/// re-streams (no `ready`
/// row was committed), at-least-once.
///
/// ## Cancel safety
///
/// **Not cancel-safe as a composite durability operation.** The object PUT may finish before the
/// manifest insert, so dropping this future can leave an unreferenced object. The keepalive path
/// pins this future and polls it by mutable reference; other callers await it directly. A retry is
/// safe because a manifest is never recorded before its object is durable.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if Parquet encoding/S3 durability or the subsequent manifest insert
/// fails. The manifest is never written before the object is durable.
pub async fn flush_batch(
    sink: &crate::sink::ParquetSink,
    ex: impl sqlx::PgExecutor<'_>,
    epoch: EpochNo,
    batch: crate::batch::SealedBatch,
) -> anyhow::Result<crate::sink::WrittenObject> {
    flush_batch_kind(sink, ex, epoch, batch, crate::sink::FileKind::Stream).await
}

/// As [`flush_batch`], stamping the object + manifest `kind` — the backfill flushes with
/// [`crate::sink::FileKind::Snapshot`].
///
/// ## Cancel safety
///
/// **Not cancel-safe as a composite durability operation.** Cancellation after the PUT but before
/// the manifest commit can orphan an object. Callers either pin the enclosing [`flush_batch`]
/// future or await this operation outside a `select!` race; replay safely regenerates the row.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if [`crate::sink::SinkError`] prevents the durable object write or a
/// [`control::ControlError`] prevents recording its ready manifest row.
pub async fn flush_batch_kind(
    sink: &crate::sink::ParquetSink,
    ex: impl sqlx::PgExecutor<'_>,
    epoch: EpochNo,
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
#[derive(Debug)]
pub struct BatchRouter<C> {
    // HASHER-CHOICE: std's default. This is the tree's densest map — one `entry` per decoded row —
    // but no profile implicates hashing, and the keys are source-derived, so collision resistance
    // remains preferable to a speculative faster hasher.
    batchers: HashMap<u32, TableBatcher<C>>,
    /// Relation version currently bound by pgoutput's latest ordinary `Relation` message for an OID.
    bindings: HashMap<u32, common::SchemaVersionNo>,
    /// Pre-DDL segments from the current ordinary transaction. They are promoted and force-sealed only
    /// when that transaction's Commit arrives, so a boundary never discards speculative rows.
    pending_cuts: Vec<TableBatcher<C>>,
    triggers: BatchTriggers,
    clock: C,
    epoch: EpochNo,
    sink_instance: String,
    /// The current transaction's top-level xid (from `Begin`), used when a change carries no xid
    /// (non-streamed txns).
    txn_xid: u32,
}

/// One decoded change's live provenance before its cached relation supplies the structural shape.
struct RowSource<'a> {
    oid: u32,
    op: Op,
    values: &'a [TupleValue],
    frame_lsn: Lsn,
    xid: u32,
}

impl<C: Clock + Clone> BatchRouter<C> {
    /// A fresh router with no batchers. Pure — nothing is registered anywhere, so a discarded call
    /// builds and drops the routing table. `clippy::must_use_candidate` skips it (the `C` type
    /// parameter and the `impl Into<String>` argument both read to that lint as possibly
    /// side-effecting), which is why the attribute is written out; same for
    /// [`StreamDemux::new`](crate::stream_txn::StreamDemux::new).
    #[must_use]
    pub fn new(
        triggers: BatchTriggers,
        clock: C,
        epoch: EpochNo,
        sink_instance: impl Into<String>,
    ) -> Self {
        let sink_instance = sink_instance.into();
        BatchRouter {
            batchers: HashMap::new(),
            bindings: HashMap::new(),
            pending_cuts: Vec::new(),
            triggers,
            clock,
            epoch,
            sink_instance,
            txn_xid: 0,
        }
    }

    /// Bind subsequent ordinary changes for `oid` to the exact relation version announced by pgoutput.
    pub fn bind_relation(&mut self, oid: u32, version: common::SchemaVersionNo) {
        self.bindings.insert(oid, version);
    }

    /// Route one decoded message. `Begin` sets the txn context; `I/U/D` buffer against the open txn;
    /// `Commit` promotes them and returns any batches that a trigger sealed. The outer decode loop
    /// sends streamed transactions through `StreamDemux`; `Truncate` and logical `Message` frames do
    /// not create row batches here.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if a commit timestamp is invalid, a relation cannot create a batcher,
    /// or buffered rows cannot be promoted and sealed through [`crate::batch::BatchError`].
    pub fn route(
        &mut self,
        cache: &RelationCache,
        msg: &Message,
        frame_lsn: Lsn,
        _schema_version: common::SchemaVersionNo,
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
                    RowSource {
                        oid: *relation_oid,
                        op: Op::Insert,
                        values: new,
                        frame_lsn,
                        xid: xid.unwrap_or(self.txn_xid),
                    },
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
                    RowSource {
                        oid: *relation_oid,
                        op: Op::Update,
                        values: new,
                        frame_lsn,
                        xid: xid.unwrap_or(self.txn_xid),
                    },
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
                    RowSource {
                        oid: *relation_oid,
                        op: Op::Delete,
                        values: old,
                        frame_lsn,
                        xid: xid.unwrap_or(self.txn_xid),
                    },
                )?;
                Ok(Vec::new())
            }
            Message::Commit {
                commit_lsn,
                commit_ts,
                ..
            } => self.commit(*commit_lsn, UtcTimestamp::from_pg_micros(*commit_ts)?),
            // Wildcard is deliberate: Message is #[non_exhaustive], and this dispatcher ignores other families.
            _ => Ok(Vec::new()),
        }
    }

    fn push(&mut self, cache: &RelationCache, row: RowSource<'_>) -> anyhow::Result<()> {
        // A transaction-local DDL can put a provisional newer shape in the shared cache. Route by the
        // Relation message that actually preceded this ordinary change, never by a global `latest` read.
        let cached = self
            .bindings
            .get(&row.oid)
            .and_then(|version| cache.get(row.oid, *version))
            .or_else(|| cache.latest_for(row.oid));
        let Some(cached) = cached else {
            tracing::warn!(
                relation_oid = row.oid,
                "change for a relation with no cached shape yet; skipping"
            );
            return Ok(());
        };
        let batcher = match self.batchers.entry(row.oid) {
            Entry::Occupied(e) => e.into_mut(),
            // The clock clone stays inside this arm. `C` is an `Arc` in production, so hoisting it
            // above the match would bump and drop the refcount on every row for the overwhelmingly
            // common occupied hit; `batchers`, `triggers`, and `clock` are disjoint fields, so the
            // borrow checker is content reading them under the entry's `&mut self.batchers`.
            Entry::Vacant(e) => e.insert(
                TableBatcher::new(Arc::clone(&cached), self.triggers, self.clock.clone())
                    .context("create table batcher")?,
            ),
        };
        let meta = SinkMeta {
            op: row.op,
            lsn: row.frame_lsn,
            commit_lsn: Lsn::ZERO, // patched at the batcher's on_commit
            commit_ts: UtcTimestamp::now(), // placeholder — patched at on_commit from Commit's ts
            xid: row.xid,
            epoch: self.epoch,
            batch_id: String::new(), // assigned by the batcher when the batch opens
            schema_version: cached.schema_version,
            source_schema: cached.relation.schema.clone(),
            source_table: cached.relation.name.clone(),
            kind: Kind::Stream,
            unchanged_toast: Box::default(),
            sink_instance: self.sink_instance.clone(),
            sink_processed_at: UtcTimestamp::now(),
        };
        batcher.push(meta, row.values);
        Ok(())
    }

    /// Cut the current file for `schema.table` without discarding an open ordinary transaction.
    ///
    /// A batcher with speculative pre-DDL rows moves to `pending_cuts`; Commit promotes and force-seals
    /// that old-version segment. A batcher containing only earlier committed rows can be sealed now.
    /// The next post-DDL change opens a fresh batcher at its Relation-bound version.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if committed rows cannot be sealed through
    /// [`crate::batch::BatchError`].
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
        if let Some(mut batcher) = self.batchers.remove(&oid)
            && let Some(batch) = if batcher.has_open_txn() {
                self.pending_cuts.push(batcher);
                None
            } else {
                batcher.drain_committed().context("cut table on DDL bump")?
            }
        {
            sealed.push(batch);
        }
        Ok(sealed)
    }

    fn commit(
        &mut self,
        commit_lsn: Lsn,
        commit_ts: UtcTimestamp,
    ) -> anyhow::Result<Vec<SealedBatch>> {
        let mut sealed = Vec::new();
        for mut batcher in self.pending_cuts.drain(..) {
            batcher
                .on_commit(commit_lsn, commit_ts)
                .context("promote pre-DDL transaction segment")?;
            if batcher.committed_rows() > 0 {
                sealed.push(batcher.seal().context("seal pre-DDL transaction segment")?);
            }
        }
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
    /// nothing is buffered. An idle heartbeat must not advance `confirmed_flush` past this.
    pub fn undurable_floor(&self) -> Option<Lsn> {
        self.batchers
            .values()
            .chain(self.pending_cuts.iter())
            .filter_map(TableBatcher::undurable_floor)
            .min()
    }

    /// Graceful-drain seal: seal every table's in-flight **committed** batch, dropping any
    /// open speculative buffers. The returned batches are flushed with the usual PUT → manifest → slot
    /// ordering before the final standby update.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if any table's committed Arrow batch cannot be sealed.
    pub fn drain_committed(&mut self) -> anyhow::Result<Vec<SealedBatch>> {
        let mut sealed = Vec::new();
        for batcher in &mut self.pending_cuts {
            if let Some(batch) = batcher
                .drain_committed()
                .context("drain committed pre-DDL segment")?
            {
                sealed.push(batch);
            }
        }
        self.pending_cuts.clear();
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
///
/// # Errors
///
/// Returns [`anyhow::Error`] if the relation cannot be mapped to Arrow, its snapshot cannot be
/// serialized, or [`control::ControlError`] prevents the registry upsert.
pub async fn on_relation(
    cache: &mut RelationCache,
    ex: impl sqlx::PgExecutor<'_>,
    epoch: EpochNo,
    relation: common::PgRelation,
    schema_version: common::SchemaVersionNo,
) -> anyhow::Result<()> {
    let Some(row) = cache_relation(cache, epoch, relation, schema_version)? else {
        return Ok(());
    };
    persist_registry(ex, &row).await
}

/// Build/cache one user relation and return the control row that may be persisted now or commit-gated
/// by [`crate::ddl::DdlConsumer`]. Internal walrus relations return `None`.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if the relation cannot be mapped to Arrow or serialized.
pub fn cache_relation(
    cache: &mut RelationCache,
    epoch: EpochNo,
    relation: common::PgRelation,
    schema_version: common::SchemaVersionNo,
) -> anyhow::Result<Option<control::RegistryRow>> {
    if is_internal_table(&relation.schema, &relation.name) {
        return Ok(None);
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
    Ok(Some(row))
}

/// Persist a previously cached registry row.
///
/// # Errors
///
/// Returns [`anyhow::Error`] if control Postgres rejects the idempotent registry upsert.
pub async fn persist_registry(
    ex: impl sqlx::PgExecutor<'_>,
    row: &control::RegistryRow,
) -> anyhow::Result<()> {
    control::upsert_registry(ex, row)
        .await
        .context("upsert schema_registry")?;
    tracing::info!(
        source_table = %format_args!("{}.{}", row.source_schema, row.source_table),
        schema_version = %row.schema_version,
        "registered relation"
    );
    Ok(())
}

/// Route one live frame. A keepalive is a no-op here (its feedback is sent inside the stream); an
/// `XLogData` payload is decoded by the **existing** `pgoutput::parse_message`, which updates `ctx`
/// on `Stream Start`/`Stop`. A decode error on a real message is a bug — fail loud.
///
/// # Errors
///
/// Returns [`anyhow::Error`] wrapping [`crate::pgoutput::DecodeError`] when an `XLogData` payload is
/// malformed or violates pgoutput protocol state.
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
///
/// Every arm is `trace!`, the level this function's name has always claimed. It fires once per
/// decoded WAL record — per *row* for the change families — so at `info` a single busy source
/// buries the events an operator actually reads: the durability line each flushed file writes, the
/// DDL bumps, the heartbeat round-trips. Per-record decode detail is what
/// `RUST_LOG=pg_sink=trace` is for; the loop's own lifecycle events around it stay at `info`.
fn trace_message(msg: &Message) {
    match msg {
        Message::Begin { final_lsn, xid, .. } => {
            tracing::trace!(op = "begin", xid, final_lsn = %final_lsn, "decoded");
        }
        Message::Commit {
            commit_lsn,
            end_lsn,
            ..
        } => {
            tracing::trace!(op = "commit", commit_lsn = %commit_lsn, end_lsn = %end_lsn, "decoded");
        }
        Message::Origin { commit_lsn, name } => {
            tracing::trace!(op = "origin", commit_lsn = %commit_lsn, name, "decoded");
        }
        Message::Relation { xid, relation } => tracing::trace!(
            op = "relation",
            xid = ?xid,
            source_table = %format_args!("{}.{}", relation.schema, relation.name),
            relation_oid = relation.oid,
            "decoded"
        ),
        Message::Type { xid, oid, name, .. } => {
            tracing::trace!(op = "type", xid = ?xid, type_oid = oid, name, "decoded");
        }
        Message::Insert {
            xid,
            relation_oid,
            new,
        } => tracing::trace!(op = "insert", xid = ?xid, relation_oid, cols = new.len(), "decoded"),
        Message::Update {
            xid, relation_oid, ..
        } => tracing::trace!(op = "update", xid = ?xid, relation_oid, "decoded"),
        Message::Delete {
            xid, relation_oid, ..
        } => tracing::trace!(op = "delete", xid = ?xid, relation_oid, "decoded"),
        Message::Truncate { xid, relations, .. } => {
            tracing::trace!(op = "truncate", xid = ?xid, relations = relations.len(), "decoded");
        }
        Message::Message {
            xid,
            transactional,
            lsn,
            prefix,
            ..
        } => tracing::trace!(
            op = "message",
            xid = ?xid,
            transactional,
            lsn = %lsn,
            prefix,
            "decoded"
        ),
        Message::StreamStart { xid, first_segment } => {
            tracing::trace!(op = "stream_start", xid, first_segment, "decoded");
        }
        Message::StreamStop => tracing::trace!(op = "stream_stop", "decoded"),
        Message::StreamCommit {
            xid, commit_lsn, ..
        } => tracing::trace!(op = "stream_commit", xid, commit_lsn = %commit_lsn, "decoded"),
        Message::StreamAbort { top_xid, sub_xid } => {
            tracing::trace!(op = "stream_abort", top_xid, sub_xid, "decoded");
        }
        // Two-phase (v3) frames never occur at v2; log opaquely rather than special-case.
        other => tracing::trace!(op = "other", detail = ?other, "decoded"),
    }
}

#[cfg(test)]
#[path = "consume_test.rs"]
mod tests;
