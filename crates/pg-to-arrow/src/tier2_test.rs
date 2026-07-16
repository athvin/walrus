use super::*;

#[test]
fn interval_years_months_days_time_split() {
    // 1 year → 12 mon, + 2 mons = 14; 3 days; 04:05:06.5 → micros.
    let expected_micros = (4 * 3600 + 5 * 60 + 6) * 1_000_000 + 500_000;
    assert_eq!(
        parse_interval("1 year 2 mons 3 days 04:05:06.5").unwrap(),
        (14, 3, expected_micros)
    );
}

#[test]
fn interval_1_month_ne_30_days_ne_720_hours() {
    // The three fields stay independent: none of these collapses into another.
    let a = parse_interval("1 mon").unwrap();
    let b = parse_interval("30 days").unwrap();
    let c = parse_interval("720:00:00").unwrap();
    assert_eq!(a, (1, 0, 0));
    assert_eq!(b, (0, 30, 0));
    assert_eq!(c, (0, 0, 720 * 3_600_000_000));
    assert_ne!(a, b);
    assert_ne!(b, c);
    assert_ne!(a, c);
}

#[test]
fn interval_negative_time_and_ago() {
    assert_eq!(parse_interval("-00:00:01").unwrap(), (0, 0, -1_000_000));
    // verbose `ago` negates every field.
    assert_eq!(parse_interval("@ 1 day ago").unwrap(), (0, -1, 0));
}

#[test]
fn timetz_positive_and_negative_offsets() {
    let micros = (12 * 3600 + 34 * 60 + 56) * 1_000_000 + 789_000;
    assert_eq!(
        parse_timetz("12:34:56.789+05:30").unwrap(),
        (micros, 19_800)
    );
    assert_eq!(
        parse_timetz("12:34:56-08").unwrap(),
        ((12 * 3600 + 34 * 60 + 56) * 1_000_000, -28_800)
    );
    // whole-hour east offset, no minutes.
    assert_eq!(parse_timetz("00:00:00+00").unwrap(), (0, 0));
}

#[test]
fn interval_and_timetz_reject_garbage() {
    assert!(parse_interval("1 fortnight").is_err());
    assert!(parse_timetz("12:34:56").is_err()); // no offset
}
