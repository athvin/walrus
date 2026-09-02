//! The durability checkpoint — **the heart of the whole sink** (§1.5).
//!
//! Only *after* a batch's Parquet is durable in S3 **and** its `file_manifest` row is
//! committed does [`DurabilityCheckpoint::on_batch_durable`] advance `confirmed_flush_lsn`
//! to the batch's `lsn_end`; the next standby status update carries it as `flush`/`apply`. That is the
//! WAL-bounding invariant: slot lag is bounded to at most one in-flight batch, and a crash before the
//! checkpoint just re-streams from the last confirmed LSN (at-least-once, no loss).
//!
//! **Two LSNs, two rules (§1.9):** `confirmed_flush` (durable) moves only here; the *received*
//! keepalive LSN — `write` — is owned by [`ReplicationStream`] and moves **unconditionally** on every
//! frame. Conflating them causes disconnects (if you gate keepalives on
//! durability) or data loss (if you advance `confirmed_flush` as your keepalive). We keep them apart:
//! this struct owns `confirmed_flush`; the stream owns `received`.

use crate::replication::{ReplicationStream, StandbyStatus};
use common::Lsn;
use std::collections::HashMap;

/// The streamed-transaction view in the durability checkpoint disagrees with the protocol demux.
/// Treating either case as recoverable could lift the WAL-retention fence while a transaction is
/// still open, so the decode loop stops instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StreamCheckpointError {
    /// A second first segment tried to register an already-open top-level transaction.
    #[error("durability checkpoint already contains open streamed top xid {top_xid}")]
    AlreadyOpen {
        /// Top-level xid carried by `StreamStart`.
        top_xid: u32,
    },
    /// A commit or whole-transaction abort tried to remove a transaction that was never registered.
    #[error("durability checkpoint does not contain streamed top xid {top_xid}")]
    NotOpen {
        /// Top-level xid carried by `StreamCommit` or `StreamAbort`.
        top_xid: u32,
    },
}

/// Owns the slot-advancing `confirmed_flush_lsn`. Distinct from the stream's received LSN.
#[allow(
    missing_copy_implementations,
    reason = "copying this mutable durability state could silently detach checkpoint advances"
)]
#[derive(Debug, Clone)]
pub struct DurabilityCheckpoint {
    confirmed_flush: Lsn,
    /// Highest LSN whose object/control durability has completed. It may run ahead while an open
    /// streamed transaction clamps feedback; remembering it lets commit/abort lift that clamp without
    /// waiting for another batch to become durable.
    durable_high_water: Lsn,
    /// For each still-open streamed transaction, the position that was already confirmed *before*
    /// its first `StreamStart` was processed. Feedback is clamped to the oldest such ceiling.
    ///
    /// Using the `StreamStart` record's own LSN as the clamp is unsafe: acknowledging that exact LSN
    /// can let PostgreSQL restart after the record, leaving replay without the state needed to decode
    /// the transaction's continuation. Capturing the prior confirmed position is conservative and
    /// remains safe for interleaved proto-v2 transactions.
    open_stream_ceilings: HashMap<u32, Lsn>,
}

impl DurabilityCheckpoint {
    /// A checkpoint starting at the LSN the slot is resuming from — the position already known
    /// durable, not zero, so a restart never re-confirms ground the source has moved past.
    #[must_use]
    pub fn new(resume_lsn: Lsn) -> Self {
        DurabilityCheckpoint {
            confirmed_flush: resume_lsn,
            durable_high_water: resume_lsn,
            open_stream_ceilings: HashMap::new(),
        }
    }

    /// The LSN it is safe to tell the source it may discard. This is what frees WAL on the source,
    /// so it must never run ahead of what walrus has actually made durable.
    #[must_use]
    pub const fn confirmed_flush(&self) -> Lsn {
        self.confirmed_flush
    }

    /// Capture the only safe ceiling for a new streamed transaction. Call this before processing its
    /// first `StreamStart`, then pass the value to [`Self::on_stream_start`] only after the protocol
    /// demux accepted that start. Merely receiving a `StreamStart` must never advance feedback.
    #[must_use]
    pub const fn capture_pre_stream_start_ceiling(&self) -> Lsn {
        self.confirmed_flush
    }

    /// Register a top-level streamed transaction after its first `StreamStart` was accepted by the
    /// protocol demux. `pre_start_ceiling` must have been captured with
    /// [`Self::capture_pre_stream_start_ceiling`] before that start was processed.
    ///
    /// # Errors
    ///
    /// Returns [`StreamCheckpointError::AlreadyOpen`] if the top-level xid is already registered.
    /// State is unchanged on error.
    pub fn on_stream_start(
        &mut self,
        top_xid: u32,
        pre_start_ceiling: Lsn,
    ) -> Result<(), StreamCheckpointError> {
        if self.open_stream_ceilings.contains_key(&top_xid) {
            return Err(StreamCheckpointError::AlreadyOpen { top_xid });
        }
        self.open_stream_ceilings.insert(top_xid, pre_start_ceiling);
        self.recompute_confirmed();
        Ok(())
    }

    /// Remove a committed or wholly aborted top-level streamed transaction from the feedback fence.
    ///
    /// Returns `true` when removing this ceiling made an already-durable high-water mark immediately
    /// confirmable. The caller must then send a standby status even though no new batch was written.
    ///
    /// # Errors
    ///
    /// Returns [`StreamCheckpointError::NotOpen`] if the top-level xid is not registered. State is
    /// unchanged on error.
    pub fn on_stream_end(&mut self, top_xid: u32) -> Result<bool, StreamCheckpointError> {
        if self.open_stream_ceilings.remove(&top_xid).is_none() {
            return Err(StreamCheckpointError::NotOpen { top_xid });
        }
        let before = self.confirmed_flush;
        self.recompute_confirmed();
        Ok(self.confirmed_flush > before)
    }

    /// A batch is durable (PUT + manifest committed): advance `confirmed_flush` to `lsn_end`, clamped
    /// to every open transaction's pre-start ceiling and never regressing. **Call ONLY after
    /// `flush_batch` succeeded.**
    pub fn on_batch_durable(&mut self, lsn_end: Lsn) {
        self.durable_high_water = self.durable_high_water.max(lsn_end);
        self.recompute_confirmed();
    }

    fn recompute_confirmed(&mut self) {
        let oldest_ceiling = self.open_stream_ceilings.values().copied().min();
        let target = oldest_ceiling.map_or(self.durable_high_water, |ceiling| {
            self.durable_high_water.min(ceiling)
        });
        self.confirmed_flush = self.confirmed_flush.max(target);
    }

    /// The standby reply: `write` = the stream's received/keepalive LSN (unconditional), `flush`/`apply`
    /// = `confirmed_flush` (durable). A stalled flush advances `write` (via the stream) but not these.
    #[must_use]
    pub const fn standby_status(&self, received: Lsn, reply_requested: bool) -> StandbyStatus {
        StandbyStatus {
            write: received,
            flush: self.confirmed_flush,
            apply: self.confirmed_flush,
            reply_requested,
        }
    }

    /// Send a standby status carrying the durable `confirmed_flush`, and sync it onto the stream so the
    /// stream's own periodic keepalive reports the same `flush` (never a stale one).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if the replication socket cannot write or flush the standby status.
    pub async fn send(
        &self,
        stream: &mut ReplicationStream,
        reply_requested: bool,
    ) -> anyhow::Result<()> {
        stream.set_durable(self.confirmed_flush);
        let status = self.standby_status(stream.last_received(), reply_requested);
        stream.send_standby_status(status).await
    }
}

#[cfg(test)]
#[path = "checkpoint_test.rs"]
mod tests;
