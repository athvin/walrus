use super::*;

#[test]
fn ready_and_live_are_independent() {
    let s = LoaderState::new();
    assert!(!s.is_ready(), "not ready until bootstrap");
    assert!(!s.is_live(), "not live until the first poll stamp");
    s.stamp_poll();
    assert!(s.is_live(), "a stamped cycle → live");
    assert!(!s.is_ready(), "live does not imply ready");
    s.mark_ready();
    assert!(s.is_ready());
}

#[test]
fn quarantine_degrades_ready_but_not_startup() {
    let s = LoaderState::new();
    s.mark_ready();
    assert!(s.is_ready() && s.is_started(), "ready after bootstrap");

    s.quarantine();
    assert!(s.is_quarantined(), "quarantine latched");
    assert!(!s.is_ready(), "/ready degrades on quarantine");
    assert!(
        s.is_started(),
        "/startup stays satisfied — bootstrap did complete"
    );

    // The one exit (PR 6.7): a reload rebuild replaced the data — /ready recovers.
    s.clear_quarantine();
    assert!(!s.is_quarantined(), "the rebuild clears the latch");
    assert!(s.is_ready(), "/ready recovers after the rebuild");
}
