use super::{pause_began, raw_append_lag_bytes, validate_reload_manifest_at_f};
use crate::duck::ReloadBuild;
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
    };
    let mut file = control::ManifestRow {
        id: ManifestId(11),
        epoch: EpochNo(1),
        source_schema: "public".into(),
        source_table: "orders".into(),
        s3_uri: "s3://test/reload.parquet".into(),
        kind: control::ManifestKind::Reload,
        row_count: 1,
        lsn_start: f,
        lsn_end: f,
        schema_version: SchemaVersionNo(3),
        status: control::ManifestStatus::Ready,
        reload_id: Some(build.reload_id),
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
