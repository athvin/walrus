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
use crate::pgoutput::{self, Message, Reader, StreamCtx};
use crate::relcache::{is_internal_table, RelationCache};
use crate::replication::{ReplicationMessage, ReplicationStream};
use anyhow::Context;
use common::{Kind, Lsn, Op, SinkMeta, TupleValue, UtcTimestamp};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
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
    pool: &sqlx::PgPool,
    epoch: i64,
    schema_version: i64,
) -> anyhow::Result<()> {
    let mut ctx = StreamCtx::default();
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("decode loop cancelled");
                return Ok(());
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
                                    on_relation(cache, pool, epoch, relation.clone(), schema_version)
                                        .await?;
                                }
                                other => {
                                    for sealed in router.route(cache, other, frame_lsn, schema_version)? {
                                        // Durability steps (a) PUT then (b) commit the manifest row.
                                        let written = flush_batch(sink, pool, epoch, sealed).await?;
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
                                }
                            }
                        }
                    }
                }
            }
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
    // (a) durable in S3.
    let obj = sink
        .put(batch)
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
            Message::Commit { commit_lsn, .. } => self.commit(*commit_lsn),
            _ => Ok(Vec::new()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        cache: &RelationCache,
        oid: u32,
        op: Op,
        values: &[TupleValue],
        frame_lsn: Lsn,
        xid: u32,
        schema_version: i64,
    ) -> anyhow::Result<()> {
        let Some(cached) = cache.get(oid, schema_version) else {
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
            commit_ts: UtcTimestamp::now(), // TODO: source commit_ts (Begin) needs a from-micros ctor
            xid,
            epoch: self.epoch,
            batch_id: String::new(), // assigned by the batcher when the batch opens
            schema_version,
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

    fn commit(&mut self, commit_lsn: Lsn) -> anyhow::Result<Vec<SealedBatch>> {
        let mut sealed = Vec::new();
        for batcher in self.batchers.values_mut() {
            batcher
                .on_commit(commit_lsn)
                .context("promote committed rows")?;
            if batcher.should_flush() {
                sealed.push(batcher.seal().context("seal batch")?);
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
