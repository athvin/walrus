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
    let aborted = PendingSignal::from_tuple(&rel, &tuple("9", "1", "0/100"), Some(858)).unwrap();
    let survivor = PendingSignal::from_tuple(&rel, &tuple("9", "2", "0/110"), Some(859)).unwrap();
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
