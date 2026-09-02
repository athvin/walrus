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
fn stream_start_itself_is_never_acknowledged() {
    let mut cp = DurabilityCheckpoint::new("0/100".parse().unwrap());
    cp.on_batch_durable("0/500".parse().unwrap());
    let before_start = cp.capture_pre_stream_start_ceiling();

    cp.on_stream_start(7, before_start).unwrap();
    // Model durable work through and beyond a StreamStart at 0/600. Feedback stays at the position
    // captured before the start, rather than acknowledging the StreamStart record itself.
    cp.on_batch_durable("0/900".parse().unwrap());
    assert_eq!(cp.confirmed_flush(), "0/500".parse().unwrap());

    // Commit/whole abort removes the fence and exposes the remembered durable high-water mark.
    assert!(cp.on_stream_end(7).unwrap());
    assert_eq!(cp.confirmed_flush(), "0/900".parse().unwrap());
}

#[test]
fn interleaved_streams_keep_every_pre_start_ceiling() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    cp.on_batch_durable("0/80".parse().unwrap());
    let first_ceiling = cp.capture_pre_stream_start_ceiling();
    cp.on_stream_start(100, first_ceiling).unwrap();
    cp.on_batch_durable("0/500".parse().unwrap());

    // A second top-level transaction starts while the first one holds feedback. Its safe captured
    // ceiling is independently retained, so ending xid 100 cannot accidentally release xid 200.
    let second_ceiling = cp.capture_pre_stream_start_ceiling();
    cp.on_stream_start(200, second_ceiling).unwrap();
    assert!(!cp.on_stream_end(100).unwrap());
    assert_eq!(cp.confirmed_flush(), "0/80".parse().unwrap());
    assert!(cp.on_stream_end(200).unwrap());
    assert_eq!(cp.confirmed_flush(), "0/500".parse().unwrap());
}

#[test]
fn checkpoint_rejects_stream_state_drift_without_mutating_the_fence() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    let ceiling = cp.capture_pre_stream_start_ceiling();
    cp.on_stream_start(100, ceiling).unwrap();
    assert_eq!(
        cp.on_stream_start(100, "0/20".parse().unwrap()),
        Err(StreamCheckpointError::AlreadyOpen { top_xid: 100 })
    );
    cp.on_batch_durable("0/500".parse().unwrap());
    assert_eq!(cp.confirmed_flush(), "0/10".parse().unwrap());
    assert_eq!(
        cp.on_stream_end(200),
        Err(StreamCheckpointError::NotOpen { top_xid: 200 })
    );
    assert_eq!(cp.confirmed_flush(), "0/10".parse().unwrap());
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
