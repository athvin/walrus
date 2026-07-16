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
