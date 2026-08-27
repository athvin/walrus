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
