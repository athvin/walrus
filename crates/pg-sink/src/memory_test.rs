use super::*;

#[test]
fn over_ceiling_when_sum_across_streams_exceeds_budget() {
    let mut m = InflightMeter::new(1000);
    m.add((1, 100), 400);
    m.add((2, 100), 400);
    assert!(!m.over_ceiling(), "800 <= 1000");
    m.add((1, 200), 300); // total 1100 across THREE streams
    assert!(
        m.over_ceiling(),
        "the AGGREGATE exceeds the ceiling, not any single stream"
    );
    assert_eq!(m.total(), 1100);
    m.release((1, 100));
    assert_eq!(m.total(), 700);
    assert!(!m.over_ceiling());
}

#[test]
fn largest_open_picks_the_biggest_stream() {
    let mut m = InflightMeter::new(10_000);
    m.add((1, 100), 200);
    m.add((2, 101), 900);
    m.add((3, 102), 500);
    assert_eq!(m.largest_open(), Some((2, 101)));
}

#[test]
fn shed_prefers_committed_then_spill_then_pause() {
    let mut m = InflightMeter::new(100);
    assert_eq!(decide(&m, true), None, "under ceiling → no shedding");
    m.add((7, 55), 200); // over ceiling
    assert_eq!(
        decide(&m, true),
        Some(ShedAction::FlushCommitted),
        "committed flush is cheapest"
    );
    assert_eq!(
        decide(&m, false),
        Some(ShedAction::SpillOpenTxn(7, 55)),
        "no committed → spill the largest open stream"
    );
    let mut empty = InflightMeter::new(0); // over ceiling but nothing open
    empty.total = 1; // simulate a tiny over-count with no streams
    assert_eq!(
        decide(&empty, false),
        Some(ShedAction::PausePoll),
        "nothing to spill → pause"
    );
}

#[test]
fn hysteresis_pauses_at_activate_resumes_at_lower_ratio() {
    let mut bp = Backpressure::new(0.85, 0.75);
    let c = 1000;
    assert!(!bp.tick(800, c), "0.80 < activate 0.85 → not paused");
    assert!(bp.tick(860, c), "0.86 >= activate → paused");
    assert!(
        bp.tick(800, c),
        "0.80 still > resume 0.75 → STAYS paused (no flap)"
    );
    assert!(!bp.tick(740, c), "0.74 <= resume → resumes");
    assert!(!bp.tick(800, c), "0.80 < activate again → stays running");
}
