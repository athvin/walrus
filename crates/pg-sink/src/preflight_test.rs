use super::*;

#[test]
fn preflight_errors_map_to_exit_codes() {
    // A keyless table is its own terminal class + exit code.
    let e: common::Error = PreflightError::NoPrimaryKey {
        schema: "public".into(),
        table: "orders".into(),
    }
    .into();
    assert!(matches!(e, common::Error::KeylessTable { .. }));
    assert_eq!(e.exit_code(), common::ExitCode::KeylessTable);
    assert!(e.is_terminal());

    // Everything else is a terminal Preflight.
    for pe in [
        PreflightError::WalLevel {
            found: "replica".into(),
        },
        PreflightError::ServerTooOld { found: 130000 },
        PreflightError::NoHeadroom {
            kind: "wal_senders",
            used: 10,
            max: 10,
        },
        PreflightError::PublicationGap {
            pub_name: "walrus_pub".into(),
            schema: "walrus".into(),
            table: "heartbeat".into(),
        },
        PreflightError::NoReplicationPriv,
        PreflightError::ReloadSignalMissing {
            detail: "walrus.reload_signal table absent",
        },
    ] {
        let e: common::Error = pe.into();
        assert_eq!(e.exit_code(), common::ExitCode::Preflight);
        assert!(e.is_terminal());
    }
}

#[test]
fn sql_quoting_escapes() {
    assert_eq!(lit("wal_level"), "'wal_level'");
    assert_eq!(lit("a'b"), "'a''b'");
    assert_eq!(ident("walrus_pub"), "\"walrus_pub\"");
}

#[test]
fn gap_and_signal_errors_name_their_remediation() {
    // An operator reading the crash log must be able to copy-paste the fix (reload H11).
    let gap = PreflightError::PublicationGap {
        pub_name: "walrus_pub".into(),
        schema: "walrus".into(),
        table: "reload_signal".into(),
    };
    assert!(gap
        .to_string()
        .contains("ALTER PUBLICATION walrus_pub ADD TABLE walrus.reload_signal"));

    let missing = PreflightError::ReloadSignalMissing {
        detail: "walrus.reload_signal table absent",
    };
    assert!(missing
        .to_string()
        .contains("migrations/source/0003_reload_signal.sql"));
}
