//! The decode loop: join the live [`ReplicationStream`] (PR 2.20) to the sync, pure `pgoutput`
//! decoder (PRs 2.2–2.8). The Rust analogue of the proof harness's `run-tests.sh` — an `INSERT` now
//! decodes to `Begin → Relation → Insert → Commit` against a real Postgres. No Arrow / batching / S3.
//!
//! **The seam that kept the decoder testable:** `pgoutput::parse_message` stays **sync + pure**; this
//! loop owns the I/O (`.await`s a frame) and calls the decoder synchronously on the returned `Bytes`.
//! The `StreamCtx` (are we inside a `Stream Start`/`Stop` block?) is threaded across frames by the
//! loop, since a v2 sub-xid prefix appears *only inside* a stream. Small txns still arrive whole at
//! commit (no stream frames), and `StreamCtx` handles both shapes with no special-casing here.

use crate::pgoutput::{self, Message, Reader, StreamCtx};
use crate::replication::{ReplicationMessage, ReplicationStream};
use anyhow::Context;
use tokio_util::sync::CancellationToken;

/// Drive the stream: decode each `XLogData`, keep keepalives answered (inside `ReplicationStream`),
/// and exit cleanly on cancel or stream end.
pub async fn run_decode_loop(
    stream: &mut ReplicationStream,
    token: CancellationToken,
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
                        if let Some(msg) = on_frame(&mut ctx, frame)? {
                            trace_message(&msg);
                        }
                    }
                }
            }
        }
    }
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
