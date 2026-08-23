use super::*;
use common::{PgColumn, PgRelation, ReloadId, ReplicaIdentity, TupleValue};

fn lsn(s: &str) -> Lsn {
    s.parse().unwrap()
}

fn reload(id: i64) -> ReloadId {
    ReloadId(id)
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
    let mut rx = waiters.subscribe(reload(42), 1);

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
    let mut rx = waiters.subscribe(reload(7), 3);

    // embedded >= commit is impossible under the model — loud (counter + error log), never
    // fatal, and the waiter STILL resolves with the commit LSN.
    waiters.resolve(
        reload(7),
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
        reload(1),
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
    let rx = waiters.subscribe(reload(5), 1);
    drop(rx); // the exporter timed out (PR 6.5) and walked away
    waiters.resolve(
        reload(5),
        1,
        Echo {
            commit_lsn: lsn("0/200"),
            embedded_lsn: lsn("0/100"),
        },
    );
    // The key is gone: a later subscribe starts fresh.
    let mut rx2 = waiters.subscribe(reload(5), 1);
    assert!(rx2.try_recv().is_err(), "fresh channel, nothing delivered");
}

/// The exporter subscribes before inserting the signal. If that insert fails, dropping the
/// subscription must remove the registry entry even though no echo can arrive.
#[test]
fn subscribe_then_failed_insert_leaves_no_waiter() {
    let waiters = WatermarkWaiters::default();
    {
        let _guard = waiters.subscribe(reload(42), 7);
        assert_eq!(waiters.waiter_count(), 1);
    }
    assert_eq!(
        waiters.waiter_count(),
        0,
        "the guard must unsubscribe on drop"
    );
}

#[test]
fn stale_guard_drop_does_not_evict_the_live_waiter() {
    let waiters = WatermarkWaiters::default();
    let stale = waiters.subscribe(reload(42), 7);
    let mut live = waiters.subscribe(reload(42), 7);

    drop(stale);
    assert_eq!(waiters.waiter_count(), 1, "the live subscription survives");

    waiters.resolve(
        reload(42),
        7,
        Echo {
            commit_lsn: lsn("0/200"),
            embedded_lsn: lsn("0/100"),
        },
    );
    assert_eq!(
        live.try_recv().expect("live waiter resolves").commit_lsn,
        lsn("0/200")
    );
}

#[test]
fn resolve_then_drop_is_a_no_op() {
    let waiters = WatermarkWaiters::default();
    let guard = waiters.subscribe(reload(42), 7);
    assert_eq!(waiters.waiter_count(), 1);

    waiters.resolve(
        reload(42),
        7,
        Echo {
            commit_lsn: lsn("0/200"),
            embedded_lsn: lsn("0/100"),
        },
    );
    assert_eq!(waiters.waiter_count(), 0);

    drop(guard);
    assert_eq!(waiters.waiter_count(), 0);
}

#[test]
fn resolve_evicts_so_the_same_chunk_can_resubscribe() {
    let waiters = WatermarkWaiters::default();
    let mut first = waiters.subscribe(reload(7), 0);
    waiters.resolve(
        reload(7),
        0,
        Echo {
            commit_lsn: lsn("0/20"),
            embedded_lsn: lsn("0/10"),
        },
    );
    assert_eq!(first.try_recv().expect("resolved").commit_lsn, lsn("0/20"));

    // Same key again: the previous entry was removed, so this is a fresh, resolvable wait.
    let mut second = waiters.subscribe(reload(7), 0);
    waiters.resolve(
        reload(7),
        0,
        Echo {
            commit_lsn: lsn("0/40"),
            embedded_lsn: lsn("0/30"),
        },
    );
    assert_eq!(second.try_recv().expect("resolved").commit_lsn, lsn("0/40"));
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
    let mut rx_aborted = waiters.subscribe(reload(9), 1);
    let mut rx_survivor = waiters.subscribe(reload(9), 2);

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
    let mut rx = waiters.subscribe(reload(9), 1);
    let mut pending = PendingSignals::default();
    pending.push(
        PendingSignal::from_tuple(&signal_rel(), &tuple("9", "1", "0/100"), Some(866)).unwrap(),
    );
    pending.on_stream_abort(866, 866); // sub == top ⇒ whole-txn abort
    assert!(pending.is_empty());
    pending.on_stream_commit(lsn("0/200"), &waiters); // nothing left to resolve
    assert!(rx.try_recv().is_err());
}

#[test]
fn extract_preserves_capacity_and_both_relative_orders() {
    let mut values = Vec::with_capacity(64);
    values.extend([1, 2, 3, 4, 5, 6]);
    let capacity = values.capacity();

    let drained = extract(&mut values, |value| value % 2 == 0);

    assert_eq!(drained, [2, 4, 6]);
    assert_eq!(values, [1, 3, 5]);
    assert_eq!(values.capacity(), capacity);
}

#[test]
fn extract_all_matches_leaves_reusable_capacity() {
    let mut values = Vec::with_capacity(64);
    values.extend([1, 2, 3]);
    let capacity = values.capacity();

    assert_eq!(extract(&mut values, |_| true), [1, 2, 3]);
    assert!(values.is_empty());
    assert_eq!(values.capacity(), capacity);
}

#[test]
fn extract_predicate_panic_keeps_capacity_and_unvisited_tail() {
    let mut values = Vec::with_capacity(64);
    values.extend([1, 2, 3, 4, 5, 6]);
    let capacity = values.capacity();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _drained = extract(&mut values, |value| {
            assert_ne!(*value, 4, "predicate panic seam");
            value % 2 == 0
        });
    }));

    assert!(result.is_err());
    assert_eq!(values, [1, 3, 4, 5, 6]);
    assert_eq!(values.capacity(), capacity);
}
