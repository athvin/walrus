use super::*;

fn nz(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).unwrap()
}

#[test]
fn over_ceiling_when_sum_across_streams_exceeds_budget() {
    let mut m = InflightMeter::new(nz(1000));
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
    let mut m = InflightMeter::new(nz(10_000));
    m.add((1, 100), 200);
    m.add((2, 101), 900);
    m.add((3, 102), 500);
    assert_eq!(m.largest_open(), Some((2, 101)));
}

#[test]
fn spill_order_pops_strictly_descending_by_bytes() {
    let mut m = InflightMeter::new(nz(10_000));
    m.add((1, 100), 200);
    m.add((2, 101), 900);
    m.add((3, 102), 500);

    let mut candidates = m.spill_order();
    assert_eq!(candidates.pop(), Some((900, 2, 101)));
    assert_eq!(candidates.pop(), Some((500, 3, 102)));
    assert_eq!(candidates.pop(), Some((200, 1, 100)));
    assert_eq!(candidates.pop(), None);
}

#[test]
fn spill_order_breaks_an_exact_byte_tie_deterministically() {
    let mut m = InflightMeter::new(nz(10_000));
    m.add((3, 9), 500);
    m.add((7, 1), 500);
    m.add((7, 2), 500);

    let drain = || {
        let mut candidates = m.spill_order();
        std::iter::from_fn(|| candidates.pop()).collect::<Vec<_>>()
    };
    let expected = vec![(500, 7, 2), (500, 7, 1), (500, 3, 9)];
    assert_eq!(drain(), expected);
    assert_eq!(drain(), expected);
}

#[test]
fn peek_agrees_with_largest_open() {
    let mut m = InflightMeter::new(nz(10_000));
    m.add((3, 9), 200);
    m.add((7, 1), 500);
    m.add((7, 2), 500);

    assert_eq!(
        m.spill_order()
            .peek()
            .map(|&(_bytes, table_id, xid)| (table_id, xid)),
        m.largest_open()
    );
}

#[test]
fn spill_order_of_an_empty_meter_is_empty() {
    let m = InflightMeter::new(nz(1));
    assert!(m.spill_order().is_empty());
    assert_eq!(m.largest_open(), None);
}

#[test]
fn shed_prefers_committed_then_spill_then_pause() {
    let mut m = InflightMeter::new(nz(100));
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
    let mut empty = InflightMeter::new(nz(1)); // over ceiling but nothing open
    empty.total = 2; // simulate a tiny over-count with no streams
    assert_eq!(
        decide(&empty, false),
        Some(ShedAction::PausePoll),
        "nothing to spill → pause"
    );
}

#[test]
fn hysteresis_pauses_at_activate_resumes_at_lower_ratio() {
    let mut bp = Backpressure::new(HysteresisBand::DEFAULT);
    let c = nz(1000);
    assert!(!bp.tick(800, c), "0.80 < activate 0.85 → not paused");
    assert!(bp.tick(860, c), "0.86 >= activate → paused");
    assert!(
        bp.tick(800, c),
        "0.80 still > resume 0.75 → STAYS paused (no flap)"
    );
    assert!(!bp.tick(740, c), "0.74 <= resume → resumes");
    assert!(!bp.tick(800, c), "0.80 < activate again → stays running");
}

#[test]
fn hysteresis_band_rejects_resume_at_or_above_activate() {
    let activate = Ratio::new(0.85).unwrap();
    let resume = Ratio::new(0.9).unwrap();
    assert!(HysteresisBand::new(activate, resume).is_err());
}

#[test]
fn ratio_rejects_the_closed_ends_and_nan() {
    assert!(Ratio::new(0.0).is_err());
    assert!(Ratio::new(1.0).is_err());
    assert!(Ratio::new(f64::NAN).is_err());
    assert!(Ratio::new(0.5).is_ok());
}

#[test]
fn ratio_rejects_non_finite_values_explicitly() {
    for raw in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let error = Ratio::new(raw).expect_err("non-finite ratio must be rejected");
        assert!(error.to_string().contains("finite"), "{raw}: {error}");
    }
}

#[test]
fn default_band_is_valid() {
    let band = HysteresisBand::DEFAULT;
    assert!(HysteresisBand::new(band.activate(), band.resume()).is_ok());
}

#[test]
fn add_saturates_at_u64_max_and_stays_over_ceiling() {
    let mut m = InflightMeter::new(nz(1_000));
    m.add((1, 100), u64::MAX);
    m.add((1, 100), 1);
    assert_eq!(m.total(), u64::MAX);
    assert!(m.over_ceiling());
}

#[test]
fn release_after_saturation_does_not_wrap_the_total() {
    let mut m = InflightMeter::new(nz(1_000));
    m.add((1, 100), u64::MAX);
    m.add((2, 200), u64::MAX);
    m.release((1, 100));
    assert_eq!(m.total(), u64::MAX);
    assert!(m.over_ceiling(), "the second saturated stream remains open");
    m.release((2, 200));
    assert_eq!(m.total(), 0);
    assert!(!m.over_ceiling());
}
