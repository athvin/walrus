use super::*;

#[test]
fn unreachable_never_triggers_total_restart() {
    // A connection hiccup must route to Retry — never FreshSlot (which would nuke + re-snapshot the
    // whole system on every transient blip). This is the load-bearing false-positive guard.
    assert_eq!(decide(&SlotStatus::Unreachable), SlotAction::Retry);
    assert_ne!(decide(&SlotStatus::Unreachable), SlotAction::FreshSlot);
}

#[test]
fn absent_or_invalidated_on_success_triggers_total_restart() {
    // Both authoritative "connected, slot gone" states open a fresh slot (→ epoch bump when a prior
    // generation exists).
    assert_eq!(decide(&SlotStatus::Absent), SlotAction::FreshSlot);
    assert_eq!(decide(&SlotStatus::Invalidated), SlotAction::FreshSlot);
}

#[test]
fn healthy_resumes_from_confirmed_flush() {
    let cf: Lsn = "0/1234".parse().unwrap();
    assert_eq!(
        decide(&SlotStatus::Healthy {
            confirmed_flush: cf
        }),
        SlotAction::Resume {
            confirmed_flush: cf
        }
    );
}
