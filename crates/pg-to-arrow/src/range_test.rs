use super::*;

#[test]
fn family_oid_dispatch_and_element_types() {
    assert_eq!(
        RangeFamily::from_range_oid(oids::INT4RANGE),
        Some(RangeFamily::Int4)
    );
    assert_eq!(
        RangeFamily::from_multirange_oid(oids::TSTZMULTIRANGE),
        Some(RangeFamily::TsTz)
    );
    assert_eq!(RangeFamily::from_range_oid(9999), None);
    assert_eq!(RangeFamily::Int8.elem_data_type(-1), DataType::Int64);
    assert_eq!(
        RangeFamily::TsTz.elem_data_type(-1),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    // A range column carries no element typmod → unconstrained numrange falls back to VARCHAR.
    assert_eq!(RangeFamily::Num.elem_data_type(-1), DataType::Utf8);
}

#[test]
fn empty_sets_empty_true_and_bounds_null() {
    let r = parse_range("empty").unwrap();
    assert!(r.empty);
    assert_eq!(r.lower, None);
    assert_eq!(r.upper, None);
    assert!(!r.lower_inc && !r.upper_inc);
}

#[test]
fn unbounded_lower_is_null_with_empty_false() {
    // Distinct from empty: a NULL lower bound but a present range.
    let r = parse_range("(,10)").unwrap();
    assert!(!r.empty);
    assert_eq!(r.lower, None);
    assert_eq!(r.upper, Some("10".to_string()));
    assert!(!r.lower_inc, "an unbounded bound is never inclusive");
}

#[test]
fn discrete_int4range_canonicalizes_to_half_open() {
    // Postgres canonicalizes discrete ranges to `[)` before the wire; the parser reproduces it.
    let r = parse_range("[1,10)").unwrap();
    assert_eq!(r.lower, Some("1".to_string()));
    assert_eq!(r.upper, Some("10".to_string()));
    assert!(r.lower_inc);
    assert!(!r.upper_inc);
    assert!(!r.empty);
}

#[test]
fn continuous_range_preserves_arbitrary_inclusivity() {
    let r = parse_range("(1.5,9.5]").unwrap();
    assert!(!r.lower_inc);
    assert!(r.upper_inc);
}

#[test]
fn quoted_timestamp_bounds_are_unquoted() {
    let r = parse_range(r#"["2024-01-01 00:00:00","2024-01-02 00:00:00")"#).unwrap();
    assert_eq!(r.lower, Some("2024-01-01 00:00:00".to_string()));
    assert_eq!(r.upper, Some("2024-01-02 00:00:00".to_string()));
}

#[test]
fn multirange_parses_members_and_empty_list() {
    let ms = parse_multirange("{[1,4),[7,9)}").unwrap();
    assert_eq!(ms.len(), 2);
    assert_eq!(ms[0].lower, Some("1".to_string()));
    assert_eq!(ms[0].upper, Some("4".to_string()));
    assert_eq!(ms[1].lower, Some("7".to_string()));
    assert_eq!(ms[1].upper, Some("9".to_string()));
    // Empty multirange → zero members (the caller keeps this distinct from a NULL column).
    assert_eq!(parse_multirange("{}").unwrap().len(), 0);
}
