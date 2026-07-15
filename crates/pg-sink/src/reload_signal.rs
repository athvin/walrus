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
mod tests {
    use super::*;
    use common::{PgColumn, PgRelation, ReplicaIdentity, TupleValue};

    fn lsn(s: &str) -> Lsn {
        s.parse().unwrap()
    }

    fn signal_rel() -> PgRelation {
        let col = |name: &str, is_key: bool| PgColumn {
            name: name.to_string(),
            type_oid: 20,
            type_modifier: -1,
            is_key,
        };
        PgRelation {
            oid: 90001,
            schema: "walrus".to_string(),
            name: "reload_signal".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                col("reload_id", true),
                col("chunk_no", true),
                col("wal_insert_lsn", false),
                col("inserted_at", false),
            ],
        }
    }

    fn tuple(reload_id: &str, chunk_no: &str, wal_lsn: &str) -> Vec<TupleValue> {
        vec![
            TupleValue::Text(reload_id.into()),
            TupleValue::Text(chunk_no.into()),
            TupleValue::Text(wal_lsn.into()),
            TupleValue::Text("2026-07-15T00:00:00Z".into()),
        ]
    }

    #[test]
    fn subscribe_then_resolve_delivers_commit_lsn() {
        let waiters = WatermarkWaiters::default();
        let mut rx = waiters.subscribe(42, 1);

        // Buffered at Insert, resolved at Commit — the receiver gets the COMMIT LSN, not the
        // insert's message/frame LSN.
        let mut pending = PendingSignals::default();
        let sig = PendingSignal::from_tuple(&signal_rel(), &tuple("42", "1", "0/100"), None)
            .expect("well-formed tuple");
        pending.push(sig);
        pending.on_commit(lsn("0/180"), &waiters);

        let echo = rx.try_recv().expect("resolved at commit");
        assert_eq!(echo.commit_lsn, lsn("0/180"));
        assert_eq!(echo.embedded_lsn, lsn("0/100"));
        assert!(pending.is_empty());
        assert_eq!(waiters.crosscheck_violations(), 0);
    }

    #[test]
    fn crosscheck_violation_counts_and_still_resolves() {
        let waiters = WatermarkWaiters::default();
        let mut rx = waiters.subscribe(7, 3);

        // embedded >= commit is impossible under the model — loud (counter + error log), never
        // fatal, and the waiter STILL resolves with the commit LSN.
        waiters.resolve(
            7,
            3,
            Echo {
                commit_lsn: lsn("0/100"),
                embedded_lsn: lsn("0/200"),
            },
        );
        assert_eq!(waiters.crosscheck_violations(), 1);
        let echo = rx.try_recv().expect("still resolves");
        assert_eq!(echo.commit_lsn, lsn("0/100"));
    }

    #[test]
    fn resolve_without_subscriber_is_a_quiet_noop() {
        let waiters = WatermarkWaiters::default();
        waiters.resolve(
            1,
            1,
            Echo {
                commit_lsn: lsn("0/200"),
                embedded_lsn: lsn("0/100"),
            },
        ); // no panic, no entry left behind
        assert_eq!(waiters.crosscheck_violations(), 0);
    }

    #[test]
    fn dropped_receiver_then_resolve_is_fine_and_entry_is_evicted() {
        let waiters = WatermarkWaiters::default();
        let rx = waiters.subscribe(5, 1);
        drop(rx); // the exporter timed out (PR 6.5) and walked away
        waiters.resolve(
            5,
            1,
            Echo {
                commit_lsn: lsn("0/200"),
                embedded_lsn: lsn("0/100"),
            },
        );
        // The key is gone: a later subscribe starts fresh.
        let mut rx2 = waiters.subscribe(5, 1);
        assert!(rx2.try_recv().is_err(), "fresh channel, nothing delivered");
    }

    #[test]
    fn non_insert_ops_on_signal_table_are_ignored() {
        // The consume loop never buffers Update/Delete on the signal table — only Insert reaches
        // `PendingSignal::from_tuple`. What this module can pin: a Delete's old-key tuple (PK
        // cols only, rest NULL) does not parse as a signal, so even a mis-routed one is dropped.
        let rel = signal_rel();
        let delete_old_key = vec![
            TupleValue::Text("42".into()),
            TupleValue::Text("1".into()),
            TupleValue::Null, // wal_insert_lsn not in the old-key image
            TupleValue::Null,
        ];
        assert!(PendingSignal::from_tuple(&rel, &delete_old_key, None).is_none());
    }

    #[test]
    fn subtransaction_aborted_signal_never_resolves_the_waiter() {
        // proto-version.md §9b: a rolled-back savepoint's rows ARE streamed; only the Stream
        // Abort naming its sub-xid says to drop them. A signal insert tagged with that sub-xid
        // must never resolve — the commit never carried it.
        let waiters = WatermarkWaiters::default();
        let mut rx_aborted = waiters.subscribe(9, 1);
        let mut rx_survivor = waiters.subscribe(9, 2);

        let mut pending = PendingSignals::default();
        let rel = signal_rel();
        let aborted =
            PendingSignal::from_tuple(&rel, &tuple("9", "1", "0/100"), Some(858)).unwrap();
        let survivor =
            PendingSignal::from_tuple(&rel, &tuple("9", "2", "0/110"), Some(859)).unwrap();
        pending.push(aborted);
        pending.push(survivor);

        // The savepoint (sub 858 of top 857) rolls back; the top-level txn later commits.
        pending.on_stream_abort(857, 858);
        pending.on_stream_commit(lsn("0/200"), &waiters);

        assert!(
            rx_aborted.try_recv().is_err(),
            "aborted-savepoint signal must never resolve"
        );
        let echo = rx_survivor.try_recv().expect("survivor resolves");
        assert_eq!(echo.commit_lsn, lsn("0/200"));
    }

    #[test]
    fn whole_txn_stream_abort_drops_every_buffered_signal() {
        let waiters = WatermarkWaiters::default();
        let mut rx = waiters.subscribe(9, 1);
        let mut pending = PendingSignals::default();
        pending.push(
            PendingSignal::from_tuple(&signal_rel(), &tuple("9", "1", "0/100"), Some(866)).unwrap(),
        );
        pending.on_stream_abort(866, 866); // sub == top ⇒ whole-txn abort
        assert!(pending.is_empty());
        pending.on_stream_commit(lsn("0/200"), &waiters); // nothing left to resolve
        assert!(rx.try_recv().is_err());
    }
}
