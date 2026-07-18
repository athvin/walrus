use super::*;

#[test]
fn display_is_the_inner_integer() {
    assert_eq!(ManifestId(42).to_string(), "42");
    assert_eq!(format!("{}", ManifestId(-1)), "-1");
}

#[test]
fn from_i64_and_back_round_trips() {
    let id = ManifestId::from(7);
    assert_eq!(id, ManifestId(7));
    assert_eq!(i64::from(id), 7);
}

#[test]
fn ordering_matches_the_inner_integer() {
    assert!(ManifestId(1) < ManifestId(2));
    let mut v = [ManifestId(3), ManifestId(1), ManifestId(2)];
    v.sort();
    assert_eq!(v, [ManifestId(1), ManifestId(2), ManifestId(3)]);
}
