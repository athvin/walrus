use super::*;

// The parse ⇄ as_str round-trip is the load-bearing contract: the sink writes `as_str()`, the loader
// reads it back via `FromStr`. If these two ever disagree, the stringly-typed drift this enum retired
// would silently return — so pin every variant, both directions, plus the reject case.

#[test]
fn manifest_kind_round_trips_every_variant() {
    for (k, s) in [
        (ManifestKind::Snapshot, "snapshot"),
        (ManifestKind::Stream, "stream"),
        (ManifestKind::Spill, "spill"),
        (ManifestKind::Reload, "reload"),
    ] {
        assert_eq!(k.as_str(), s);
        assert_eq!(s.parse::<ManifestKind>(), Ok(k));
    }
}

#[test]
fn a_rejected_kind_keeps_the_offending_input_as_data() {
    let err = "snapshottt".parse::<ManifestKind>().unwrap_err();
    assert_eq!(err.expected, "manifest kind");
    assert_eq!(err.input, "snapshottt");
}

#[test]
fn case_sensitivity_is_still_rejected_and_reported() {
    // Case matters — the DB stores exactly the lowercase form.
    let err = "Reload".parse::<ManifestKind>().unwrap_err();
    assert_eq!(err.expected, "manifest kind");
    assert_eq!(err.input, "Reload");
}

#[test]
fn manifest_status_round_trips_every_variant() {
    for (st, s) in [
        (ManifestStatus::Ready, "ready"),
        (ManifestStatus::Failed, "failed"),
    ] {
        assert_eq!(st.as_str(), s);
        assert_eq!(s.parse::<ManifestStatus>(), Ok(st));
    }
}

#[test]
fn a_rejected_status_keeps_the_offending_input_as_data() {
    let err = "claimed".parse::<ManifestStatus>().unwrap_err();
    assert_eq!(err.expected, "manifest status");
    assert_eq!(err.input, "claimed");
}

#[test]
fn empty_input_is_reported_verbatim() {
    let err = "".parse::<ManifestStatus>().unwrap_err();
    assert_eq!(err.expected, "manifest status");
    assert_eq!(err.input, "");
}
