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
fn manifest_kind_rejects_unknown() {
    assert!("snapshottt".parse::<ManifestKind>().is_err());
    assert!("".parse::<ManifestKind>().is_err());
    // Case matters — the DB stores exactly the lowercase form.
    assert!("Reload".parse::<ManifestKind>().is_err());
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
fn manifest_status_rejects_unknown() {
    assert!("claimed".parse::<ManifestStatus>().is_err());
    assert!("".parse::<ManifestStatus>().is_err());
}
