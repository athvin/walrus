use super::*;

#[test]
fn phase_gates_readiness() {
    let s = HealthState::new();
    assert_eq!(s.phase(), Phase::Bootstrapping);
    assert!(!s.is_ready());
    assert!(s.is_live(), "liveness is up from the start (deadlock-only)");

    s.mark_ready();
    assert_eq!(s.phase(), Phase::Ready);
    assert!(s.is_ready());

    // Terminating drops readiness but NOT liveness (§4.3).
    s.mark_terminating();
    assert!(!s.is_ready());
    assert!(s.is_live());
}

#[test]
fn degraded_does_not_affect_liveness() {
    let s = HealthState::new();
    s.mark_ready();
    s.set_degraded(true);
    assert!(s.is_degraded());
    assert!(s.is_live(), "high lag must never fail liveness");
}
