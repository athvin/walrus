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
    assert_eq!(err.column, "file_manifest.kind");
    assert_eq!(err.input, "snapshottt");
}

#[test]
fn case_sensitivity_is_still_rejected_and_reported() {
    // Case matters — the DB stores exactly the lowercase form.
    let err = "Reload".parse::<ManifestKind>().unwrap_err();
    assert_eq!(err.column, "file_manifest.kind");
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
    assert_eq!(err.column, "file_manifest.status");
    assert_eq!(err.input, "claimed");
}

#[test]
fn empty_input_is_reported_verbatim() {
    let err = "".parse::<ManifestStatus>().unwrap_err();
    assert_eq!(err.column, "file_manifest.status");
    assert_eq!(err.input, "");
}

fn grouped_row(id: i64, ordinal: i64) -> ManifestRow {
    ManifestRow {
        id: ManifestId(id),
        epoch: EpochNo(7),
        source_schema: "public".into(),
        source_table: "orders".into(),
        s3_uri: format!("s3://walrus/7/public/orders/{id}.parquet"),
        kind: ManifestKind::Spill,
        row_count: 2,
        object_size: 128,
        sha256: vec![u8::try_from(id).unwrap_or_default(); 32],
        lsn_start: Lsn::new(90),
        lsn_end: Lsn::new(100),
        schema_version: SchemaVersionNo(3),
        status: ManifestStatus::Ready,
        reload_id: None,
        stream_group_id: Some(ManifestGroupId(11)),
        stream_group_ordinal: Some(ordinal),
        stream_commit_ts: Some("2026-09-02T00:00:00Z".into()),
        stream_top_xid: Some(42),
        stream_group_expected_files: Some(2),
        stream_group_row_count: Some(4),
    }
}

#[test]
fn complete_stream_group_is_accepted() {
    validate_claimed_groups(&[grouped_row(1, 0), grouped_row(2, 1)]).unwrap();
}

#[test]
fn partial_or_duplicate_stream_group_is_rejected() {
    let partial = validate_claimed_groups(&[grouped_row(1, 0)]).unwrap_err();
    assert!(partial.to_string().contains("returned 1 children"));

    let duplicate = validate_claimed_groups(&[grouped_row(1, 0), grouped_row(2, 0)]).unwrap_err();
    assert!(duplicate.to_string().contains("invalid/duplicate ordinal"));
}
