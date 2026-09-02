use super::{
    classify_manifest_get_error, classify_manifest_object_error, pause_began, raw_append_lag_bytes,
    validate_object_fingerprint, validate_reload_manifest_at_f,
};
use crate::duck::{ReloadBuild, ReloadPhase};
use common::{EpochNo, Lsn, ManifestId, ReloadId, SchemaVersionNo};
use std::cell::Cell;

#[test]
fn empty_queue_is_zero_lag() {
    assert_eq!(raw_append_lag_bytes(None, Lsn::new(100)), 0);
}

#[test]
fn lag_is_head_minus_frontier() {
    assert_eq!(
        raw_append_lag_bytes(Some(Lsn::new(500)), Lsn::new(200)),
        300
    );
    assert_eq!(raw_append_lag_bytes(Some(Lsn::new(200)), Lsn::new(200)), 0);
}

#[test]
fn frontier_ahead_of_queue_saturates_to_zero() {
    // A just-advanced frontier can momentarily lead a stale MAX read — never underflow.
    assert_eq!(raw_append_lag_bytes(Some(Lsn::new(100)), Lsn::new(300)), 0);
}

#[test]
fn object_size_or_hash_mismatch_is_typed_corruption() {
    let expected = [7_u8; 32];
    assert!(validate_object_fingerprint("s3://b/k", 10, &expected, 10, &expected).is_ok());
    assert!(matches!(
        validate_object_fingerprint("s3://b/k", 10, &expected, 11, &expected),
        Err(crate::error::LoaderError::ObjectIntegrity { .. })
    ));
    assert!(matches!(
        validate_object_fingerprint("s3://b/k", 10, &expected, 10, &[8_u8; 32]),
        Err(crate::error::LoaderError::ObjectIntegrity { .. })
    ));
}

#[test]
fn missing_durable_object_is_typed_corruption_but_transport_failure_is_not() {
    let missing = object_store::Error::NotFound {
        path: "missing.parquet".to_string(),
        source: Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)),
    };
    assert!(matches!(
        classify_manifest_get_error("s3://b/missing.parquet", missing),
        crate::error::LoaderError::ObjectIntegrity { reason, .. }
            if reason == "durable manifest object is missing"
    ));

    let transport = object_store::Error::Generic {
        store: "test",
        source: Box::new(std::io::Error::from(std::io::ErrorKind::TimedOut)),
    };
    assert!(matches!(
        classify_manifest_get_error("s3://b/present.parquet", transport),
        crate::error::LoaderError::ObjectStore { .. }
    ));

    let missing_mid_stream = object_store::Error::NotFound {
        path: "vanished.parquet".to_string(),
        source: Box::new(std::io::Error::from(std::io::ErrorKind::NotFound)),
    };
    assert!(matches!(
        classify_manifest_object_error(
            "s3://b/vanished.parquet",
            "stream manifest object",
            missing_mid_stream,
        ),
        crate::error::LoaderError::ObjectIntegrity { .. }
    ));
}

#[test]
fn pause_logs_once_per_pause_and_relatches_on_a_new_reload() {
    let latch = Cell::new(None);
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(7))),
        Some(common::ReloadId(7)),
        "a new pause logs"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(7))),
        None,
        "same pause: silent on later polls"
    );
    assert_eq!(
        pause_began(&latch, None),
        None,
        "lifted: silent, latch cleared"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(8))),
        Some(common::ReloadId(8)),
        "the next reload logs again"
    );
    assert_eq!(
        pause_began(&latch, Some(common::ReloadId(9))),
        Some(common::ReloadId(9)),
        "a superseding reload logs without an intervening lift"
    );
}

#[test]
fn active_reload_manifest_must_name_the_build_and_be_stamped_exactly_at_f() {
    let f = Lsn::new(100);
    let build = ReloadBuild {
        reload_id: ReloadId(7),
        shadow_table: "orders__reload_7".into(),
        schema_version: SchemaVersionNo(3),
        start_lsn: f,
        final_lsn: Lsn::new(200),
        publication_nonce: uuid::Uuid::from_u128(1),
        raw_appended_lsn: f,
        transformed_lsn: f,
        phase: ReloadPhase::Building,
    };
    let mut file = control::ManifestRow {
        id: ManifestId(11),
        epoch: EpochNo(1),
        source_schema: "public".into(),
        source_table: "orders".into(),
        s3_uri: "s3://test/reload.parquet".into(),
        kind: control::ManifestKind::Reload,
        row_count: 1,
        object_size: 128,
        sha256: vec![7; 32],
        lsn_start: f,
        lsn_end: f,
        schema_version: SchemaVersionNo(3),
        status: control::ManifestStatus::Ready,
        reload_id: Some(build.reload_id),
        stream_group_id: None,
        stream_group_ordinal: None,
        stream_commit_ts: None,
        stream_top_xid: None,
        stream_group_expected_files: None,
        stream_group_row_count: None,
    };

    validate_reload_manifest_at_f(&file, &build).unwrap();

    file.lsn_end = Lsn::new(201);
    assert!(
        validate_reload_manifest_at_f(&file, &build)
            .unwrap_err()
            .to_string()
            .contains("do not equal active reload"),
        "a malformed baseline above H must fail before the generic H barrier"
    );

    file.lsn_end = f;
    file.reload_id = Some(ReloadId(8));
    assert!(validate_reload_manifest_at_f(&file, &build).is_err());
}
