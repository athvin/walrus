use super::*;
use std::str::FromStr;

#[test]
fn status_and_flavor_round_trip_their_sql_strings() {
    // The strings are the contract with the migration's CHECK constraints AND with the
    // sqlx::Type derive (`rename_all`) — a drift in any of the three is a bug this catches.
    for status in [
        ReloadStatus::Requested,
        ReloadStatus::Exporting,
        ReloadStatus::ExportComplete,
        ReloadStatus::Complete,
        ReloadStatus::Failed,
    ] {
        assert_eq!(ReloadStatus::from_str(status.as_str()), Ok(status));
    }
    assert_eq!(ReloadStatus::ExportComplete.as_str(), "export_complete");

    for flavor in [ReloadFlavor::Reload, ReloadFlavor::Resync] {
        assert_eq!(ReloadFlavor::from_str(flavor.as_str()), Ok(flavor));
    }

    assert!(
        ReloadStatus::from_str("superseded").is_err(),
        "five statuses, ever"
    );
    assert!(ReloadFlavor::from_str("rebuild").is_err());
}

#[test]
fn rejected_reload_values_keep_the_offending_input_as_data() {
    let status_err = ReloadStatus::from_str("superseded").unwrap_err();
    assert_eq!(status_err.column, "table_reload.status");
    assert_eq!(status_err.input, "superseded");

    let flavor_err = ReloadFlavor::from_str("rebuild").unwrap_err();
    assert_eq!(flavor_err.column, "table_reload.flavor");
    assert_eq!(flavor_err.input, "rebuild");
}

#[test]
fn restart_cap_counts_the_successor_not_the_predecessor() {
    // The next attempt carries restart_count+1, so the cap is measured against THAT.
    assert!(
        restart_would_exceed_cap(0, 0),
        "cap 0 fails the very first mid-export DDL"
    );
    assert!(!restart_would_exceed_cap(0, 3), "first restart is the 1st");
    assert!(
        !restart_would_exceed_cap(2, 3),
        "the 3rd restart still fits"
    );
    assert!(
        restart_would_exceed_cap(3, 3),
        "the 4th would exceed a cap of 3"
    );
}

#[test]
fn restart_cap_holds_at_the_integer_ceiling() {
    // A bare `restart_count + 1` wraps to i32::MIN in release at the ceiling, so a corrupt control
    // row would report even a spent cap as unspent — an unbounded restart loop with no diagnostic.
    assert!(
        restart_would_exceed_cap(i32::MAX, i32::MAX),
        "no i32 cap can hold i32::MAX + 1"
    );
    assert!(restart_would_exceed_cap(i32::MAX, 0));
    // One below the ceiling still answers exactly: the successor is i32::MAX, which fits that cap.
    assert!(!restart_would_exceed_cap(i32::MAX - 1, i32::MAX));
}

/// `typed_reload_row!` names its output through `$crate::reload::`, so the expansion carries its own
/// path to `ReloadRow` instead of borrowing one from wherever the macro is invoked.
///
/// This module deliberately omits the file-level `use super::*`, so nothing named `ReloadRow` is in
/// scope here. That makes the test a *compile-time* guard: a bare `ReloadRow` in the macro body
/// stops resolving the moment the macro expands outside the module defining the struct — the one
/// failure the four production call sites, all inside `reload.rs` itself, can never surface.
mod expansion_site_needs_no_imports {
    /// Stands in for one `sqlx::query_file!` record: the field set the macro reads, with the bare
    /// `i64`s the real records hand over for the columns the macro converts into typed ids.
    struct Record {
        reload_id: i64,
        epoch: i64,
        source_schema: String,
        source_table: String,
        flavor: crate::reload::ReloadFlavor,
        status: crate::reload::ReloadStatus,
        chunk_no: i64,
        cursor_pk: Option<serde_json::Value>,
        first_lsn: Option<common::Lsn>,
        final_lsn: Option<common::Lsn>,
        schema_version: Option<i64>,
        restart_count: i32,
        lease_holder: Option<String>,
        error: Option<String>,
    }

    #[test]
    fn typed_reload_row_expands_where_reloadrow_is_unimported() {
        let record = Record {
            reload_id: 7,
            epoch: 3,
            source_schema: "public".to_string(),
            source_table: "orders".to_string(),
            flavor: crate::reload::ReloadFlavor::Resync,
            status: crate::reload::ReloadStatus::Exporting,
            chunk_no: 2,
            cursor_pk: None,
            first_lsn: Some(common::Lsn::new(16)),
            final_lsn: None,
            schema_version: Some(5),
            restart_count: 1,
            lease_holder: Some("worker-1".to_string()),
            error: None,
        };

        let row = typed_reload_row!(record);

        assert_eq!(row.reload_id, common::ReloadId(7));
        assert_eq!(row.epoch, common::EpochNo(3));
        assert_eq!(row.schema_version, Some(common::SchemaVersionNo(5)));
        assert_eq!(row.first_lsn, Some(common::Lsn::new(16)));
        assert_eq!(row.status, crate::reload::ReloadStatus::Exporting);
    }
}
