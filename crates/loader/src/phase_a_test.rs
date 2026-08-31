use super::{pause_began, raw_append_lag_bytes};
use common::Lsn;
use std::cell::Cell;

#[test]
fn empty_queue_is_zero_lag() {
    assert_eq!(raw_append_lag_bytes(None, Lsn::new(100)), 0);
}

#[test]
fn lag_is_head_minus_frontier() {
    assert_eq!(
        raw_append_lag_bytes(Some(Lsn::new(500)), Lsn::new(200)),
        300
    );
    assert_eq!(raw_append_lag_bytes(Some(Lsn::new(200)), Lsn::new(200)), 0);
}

#[test]
fn frontier_ahead_of_queue_saturates_to_zero() {
    // A just-advanced frontier can momentarily lead a stale MAX read — never underflow.
    assert_eq!(raw_append_lag_bytes(Some(Lsn::new(100)), Lsn::new(300)), 0);
}

#[test]
fn pause_logs_once_per_pause_and_relatches_on_a_new_reload() {
    let latch = Cell::new(None);
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(7))),
        Some(common::ReloadId(7)),
        "a new pause logs"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(7))),
        None,
        "same pause: silent on later polls"
    );
    assert_eq!(
        pause_began(&latch, None),
        None,
        "lifted: silent, latch cleared"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(8))),
        Some(common::ReloadId(8)),
        "the next reload logs again"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(9))),
        Some(common::ReloadId(9)),
        "a superseding reload logs without an intervening lift"
    );
}
