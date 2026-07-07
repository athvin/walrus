//! The durability checkpoint — **the heart of the whole sink** (§1.5).
//!
//! Only *after* a batch's Parquet is durable in S3 (PR 2.24) **and** its `file_manifest` row is
//! committed (PR 2.25) does [`DurabilityCheckpoint::on_batch_durable`] advance `confirmed_flush_lsn`
//! to the batch's `lsn_end`; the next standby status update carries it as `flush`/`apply`. That is the
//! WAL-bounding invariant: slot lag is bounded to at most one in-flight batch, and a crash before the
//! checkpoint just re-streams from the last confirmed LSN (at-least-once, no loss).
//!
//! **Two LSNs, two rules (§1.9):** `confirmed_flush` (durable) moves only here; the *received*
//! keepalive LSN — `write` — is owned by [`ReplicationStream`] and moves **unconditionally** on every
//! frame (that liveness path is PR 2.20). Conflating them causes disconnects (if you gate keepalives on
//! durability) or data loss (if you advance `confirmed_flush` as your keepalive). We keep them apart:
//! this struct owns `confirmed_flush`; the stream owns `received`.

use crate::replication::{ReplicationStream, StandbyStatus};
use common::Lsn;

/// Owns the slot-advancing `confirmed_flush_lsn`. Distinct from the stream's received LSN.
#[derive(Debug, Clone)]
pub struct DurabilityCheckpoint {
    confirmed_flush: Lsn,
    /// `confirmed_flush` is never advanced past this floor — the begin LSN of the oldest still-open
    /// streamed txn (§1.6). `None` while only small/whole txns flow (a no-op ceiling); PR 2.30 fills it.
    open_txn_floor: Option<Lsn>,
}

impl DurabilityCheckpoint {
    pub fn new(resume_lsn: Lsn) -> Self {
        DurabilityCheckpoint {
            confirmed_flush: resume_lsn,
            open_txn_floor: None,
        }
    }

    pub fn confirmed_flush(&self) -> Lsn {
        self.confirmed_flush
    }

    /// Set the open-txn floor (PR 2.30). `None` = no open streamed txn; small/whole txns leave it unset.
    pub fn set_open_txn_floor(&mut self, floor: Option<Lsn>) {
        self.open_txn_floor = floor;
    }

    /// A batch is durable (PUT + manifest committed): advance `confirmed_flush` to `lsn_end`, clamped
    /// to the open-txn floor and never regressing. **Call ONLY after `flush_batch` succeeded.**
    pub fn on_batch_durable(&mut self, lsn_end: Lsn) {
        let target = match self.open_txn_floor {
            Some(floor) => lsn_end.min(floor),
            None => lsn_end,
        };
        self.confirmed_flush = self.confirmed_flush.max(target);
    }

    /// The standby reply: `write` = the stream's received/keepalive LSN (unconditional), `flush`/`apply`
    /// = `confirmed_flush` (durable). A stalled flush advances `write` (via the stream) but not these.
    pub fn standby_status(&self, received: Lsn, reply_requested: bool) -> StandbyStatus {
        StandbyStatus {
            write: received,
            flush: self.confirmed_flush,
            apply: self.confirmed_flush,
            reply_requested,
        }
    }

    /// Send a standby status carrying the durable `confirmed_flush`, and sync it onto the stream so the
    /// stream's own periodic keepalive reports the same `flush` (never a stale one).
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
mod tests {
    use super::*;

    #[test]
    fn advances_confirmed_flush_only_forward() {
        let mut cp = DurabilityCheckpoint::new("0/100".parse().unwrap());
        cp.on_batch_durable("0/200".parse().unwrap());
        assert_eq!(cp.confirmed_flush(), "0/200".parse().unwrap());
        // A lower/older batch never regresses the confirmed LSN.
        cp.on_batch_durable("0/150".parse().unwrap());
        assert_eq!(cp.confirmed_flush(), "0/200".parse().unwrap());
    }

    #[test]
    fn clamps_to_the_open_txn_floor() {
        let mut cp = DurabilityCheckpoint::new(Lsn::ZERO);
        cp.set_open_txn_floor(Some("0/A0".parse().unwrap()));
        // A durable batch at 0/500 cannot advance past the open txn's floor 0/A0.
        cp.on_batch_durable("0/500".parse().unwrap());
        assert_eq!(cp.confirmed_flush(), "0/A0".parse().unwrap());
        // Once the floor lifts (txn committed), the next batch advances freely.
        cp.set_open_txn_floor(None);
        cp.on_batch_durable("0/500".parse().unwrap());
        assert_eq!(cp.confirmed_flush(), "0/500".parse().unwrap());
    }

    #[test]
    fn standby_status_carries_two_distinct_lsns() {
        let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
        cp.on_batch_durable("0/40".parse().unwrap());
        // write (received/keepalive) is ahead of flush (confirmed_flush) during a stall.
        let s = cp.standby_status("0/900".parse().unwrap(), false);
        assert_eq!(
            s.write,
            "0/900".parse().unwrap(),
            "received advances unconditionally"
        );
        assert_eq!(
            s.flush,
            "0/40".parse().unwrap(),
            "flush holds at the durable LSN"
        );
        assert_eq!(s.apply, s.flush);
    }
}
