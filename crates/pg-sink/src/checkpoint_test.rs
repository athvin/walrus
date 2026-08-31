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
