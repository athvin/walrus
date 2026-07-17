//! Echo-wait watermark capture (reload H1, PR 6.3).
//!
//! The exporter (PR 6.5) INSERTs a `walrus.reload_signal` row and blocks on a waiter; when the
//! sink decodes its *own* insert coming back through the replication stream, the transaction's
//! **commit LSN** is the chunk's low watermark `L_i` — the value chunk rows are stamped with. Two
//! rules make the handoff race-free and are worth stating where the code can't show them:
//!
//! - **Subscribe-then-insert.** The exporter subscribes BEFORE writing the signal row: the echo
//!   round-trip can be faster than the exporter's next `await`, so the registry must already hold
//!   the sender when the INSERT commits. (The reverse order can miss the echo forever.)
//! - **Buffer at `Insert`, resolve at `Commit`.** The watermark is the transaction's commit LSN —
//!   a property of the `Commit` message, which arrives *after* the `Insert` — so the decoded
//!   insert is held as a [`PendingSignal`] until its transaction's fate is known.
//!
//! The row's embedded `wal_insert_lsn` (PR 6.2) is never the stamp — it is the free cross-check:
//! an insert's WAL position strictly precedes its commit record, so `embedded < commit` on every
//! echo, or the watermark model itself is broken (metric + error log, never a panic — see
//! `docs/implementation/notes/commit-visibility-race.md` for the race this bounds).

use common::Lsn;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::oneshot;

/// One resolved echo: the authoritative stamp and its cross-check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Echo {
    /// The signal transaction's decoded COMMIT LSN — this IS the chunk's low watermark `L_i` (H1).
    pub commit_lsn: Lsn,
    /// The row's embedded `wal_insert_lsn` — strictly earlier than `commit_lsn`, or the model is
    /// wrong (the cross-check, never the stamp).
    pub embedded_lsn: Lsn,
}

/// Registry of in-flight watermark waits, keyed by `(reload_id, chunk_no)`.
///
/// Shared (`Arc`) between the decode loop (which resolves) and the exporter tasks (which
/// subscribe, PR 6.5) — kept next to the loop's `InternalTables` wiring so no re-plumbing is
/// needed. Senders never leak: an entry is removed on resolve, a re-subscribe for the same key
/// replaces (drops) the stale sender, and an exporter that gives up simply drops its receiver —
/// `resolve` on a closed channel is a quiet no-op.
#[derive(Debug, Default)]
pub struct WatermarkWaiters {
    waiters: Mutex<HashMap<(i64, i64), oneshot::Sender<Echo>>>,
    /// Cross-check violations observed (mirrors the Prometheus counter so unit tests — which run
    /// without a recorder — can assert the count).
    crosscheck_violations: AtomicU64,
}

impl WatermarkWaiters {
    /// Register interest in chunk `(reload_id, chunk_no)`'s echo. Call BEFORE inserting the
    /// signal row (subscribe-then-insert). A duplicate subscribe replaces the stale sender —
    /// the previous receiver resolves as `Err(Closed)`, which the exporter treats as superseded.
    pub fn subscribe(&self, reload_id: i64, chunk_no: i64) -> oneshot::Receiver<Echo> {
        let (tx, rx) = oneshot::channel();
        self.waiters
            .lock()
            .expect("waiters mutex poisoned")
            .insert((reload_id, chunk_no), tx);
        rx
    }

    /// Deliver an echo from the consume path (at the `Commit` of a transaction that carried a
    /// signal insert). Runs the cross-check first: `embedded < commit`, or the counter ticks and
    /// an error is logged — loud, not fatal, and the waiter still resolves (the commit LSN is
    /// still the only defensible stamp). An unsubscribed echo (e.g. redelivered WAL after an
    /// exporter crash — recovery is control-pg's job, never WAL replay) is dropped with a debug
    /// log.
    pub fn resolve(&self, reload_id: i64, chunk_no: i64, echo: Echo) {
        if echo.embedded_lsn >= echo.commit_lsn {
            self.crosscheck_violations.fetch_add(1, Ordering::Relaxed);
            common::metrics::record_reload_crosscheck_violation();
            tracing::error!(
                reload_id,
                chunk_no,
                embedded_lsn = %echo.embedded_lsn,
                commit_lsn = %echo.commit_lsn,
                "reload echo cross-check VIOLATION: embedded wal_insert_lsn >= commit LSN — \
                 the watermark model is wrong; stop reloads and investigate"
            );
        }
        match self
            .waiters
            .lock()
            .expect("waiters mutex poisoned")
            .remove(&(reload_id, chunk_no))
        {
            Some(tx) => {
                if tx.send(echo).is_err() {
                    // The exporter gave up (timeout) and dropped its receiver — fine.
                    tracing::debug!(reload_id, chunk_no, "echo resolved after waiter gave up");
                } else {
                    tracing::info!(
                        reload_id,
                        chunk_no,
                        commit_lsn = %echo.commit_lsn,
                        embedded_lsn = %echo.embedded_lsn,
                        "reload_signal echo"
                    );
                }
            }
            None => {
                tracing::debug!(reload_id, chunk_no, "echo with no subscriber; dropped");
            }
        }
    }

    /// Cross-check violations seen so far (the unit-testable mirror of the Prometheus counter).
    pub fn crosscheck_violations(&self) -> u64 {
        self.crosscheck_violations.load(Ordering::Relaxed)
    }
}

/// A decoded `reload_signal` insert held between its `Insert` message and its transaction's fate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingSignal {
    pub reload_id: i64,
    pub chunk_no: i64,
    pub embedded_lsn: Lsn,
    /// The per-message xid — `Some` only inside a streamed transaction (which a single-row signal
    /// txn can never be; kept so the defensive stream paths stay precise).
    pub xid: Option<u32>,
}

impl PendingSignal {
    /// Parse a decoded signal tuple by column NAME from the noted relation shape (internal tables
    /// are never in the `RelationCache`, so the shape comes from `InternalTables`). A malformed
    /// tuple returns `None` — the caller logs and drops it; it can never wedge the loop.
    pub fn from_tuple(
        rel: &common::PgRelation,
        new: &[common::TupleValue],
        xid: Option<u32>,
    ) -> Option<Self> {
        let text = |name: &str| -> Option<&str> {
            let idx = rel.columns.iter().position(|c| c.name == name)?;
            match new.get(idx)? {
                common::TupleValue::Text(s) => Some(s.as_str()),
                _ => None,
            }
        };
        Some(PendingSignal {
            reload_id: text("reload_id")?.parse().ok()?,
            chunk_no: text("chunk_no")?.parse().ok()?,
            embedded_lsn: text("wal_insert_lsn")?.parse().ok()?,
            xid,
        })
    }
}

/// The decode loop's between-Insert-and-Commit buffer.
///
/// A signal transaction is a tiny single-row commit and can never be the "largest in-progress
/// transaction" streaming selects, so the `Stream*` paths below are can't-happen defenses in the
/// house style: a comment saying why, plus code that survives it anyway — including the one subtle
/// case that MUST stay correct, a subtransaction-aborted signal insert (its per-message xid is the
/// sub-xid `StreamAbort` names, so it is dropped precisely and never resolves a waiter).
#[derive(Debug, Default)]
pub struct PendingSignals {
    pending: Vec<PendingSignal>,
}

impl PendingSignals {
    pub fn push(&mut self, signal: PendingSignal) {
        self.pending.push(signal);
    }

    /// An ordinary (non-streamed) transaction committed: every buffered non-streamed signal in it
    /// resolves with this commit LSN.
    pub fn on_commit(&mut self, commit_lsn: Lsn, waiters: &WatermarkWaiters) {
        for sig in extract(&mut self.pending, |s| s.xid.is_none()) {
            waiters.resolve(
                sig.reload_id,
                sig.chunk_no,
                Echo {
                    commit_lsn,
                    embedded_lsn: sig.embedded_lsn,
                },
            );
        }
    }

    /// Can't-happen defense: a signal insert arrived inside a streamed transaction. The surviving
    /// (non-aborted) buffered streamed signals resolve at its `Stream Commit` — with a warning,
    /// because a streamed signal txn means something upstream changed.
    pub fn on_stream_commit(&mut self, commit_lsn: Lsn, waiters: &WatermarkWaiters) {
        for sig in extract(&mut self.pending, |s| s.xid.is_some()) {
            tracing::warn!(
                reload_id = sig.reload_id,
                chunk_no = sig.chunk_no,
                "reload_signal echo arrived inside a STREAMED transaction (single-row signal \
                 txns should never stream); resolving at Stream Commit"
            );
            waiters.resolve(
                sig.reload_id,
                sig.chunk_no,
                Echo {
                    commit_lsn,
                    embedded_lsn: sig.embedded_lsn,
                },
            );
        }
    }

    /// `Stream Abort`: `sub == top` aborts the whole transaction (drop every signal buffered under
    /// it); `sub != top` is a rolled-back savepoint — drop exactly the signals tagged with that
    /// sub-xid (the per-message xid IS the sub-xid), because the commit never carried them and
    /// resolving would stamp a chunk with a watermark for rows that don't exist.
    pub fn on_stream_abort(&mut self, top_xid: u32, sub_xid: u32) {
        let dropped = extract(&mut self.pending, |s| {
            s.xid == Some(sub_xid) || (top_xid == sub_xid && s.xid.is_some())
        });
        for sig in &dropped {
            tracing::warn!(
                reload_id = sig.reload_id,
                chunk_no = sig.chunk_no,
                top_xid,
                sub_xid,
                "buffered reload_signal dropped by Stream Abort (never resolves a waiter)"
            );
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

/// Drain every element matching `pred` out of `v`, preserving order.
fn extract<T>(v: &mut Vec<T>, mut pred: impl FnMut(&T) -> bool) -> Vec<T> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < v.len() {
        if pred(&v[i]) {
            out.push(v.remove(i));
        } else {
            i += 1;
        }
    }
    out
}

#[cfg(test)]
#[path = "reload_signal_test.rs"]
mod tests;
