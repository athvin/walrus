use super::*;
use common::FailureClass;
use common::sql::IdentError;

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
        PreflightError::PublicationCoverage(
            crate::source_catalog::PublicationCoverageIssue::DisabledOperations {
                publication: "walrus_pub".into(),
                disabled: "DELETE".into(),
            },
        ),
        PreflightError::NoReplicationPriv,
        PreflightError::ReloadSignalMissing {
            detail: "walrus.reload_signal table absent",
        },
        PreflightError::UnusableResult("no rows for `SELECT 1`".into()),
        PreflightError::Ident(IdentError::Empty),
    ] {
        let e: common::Error = pe.into();
        assert_eq!(e.exit_code(), common::ExitCode::Preflight);
        assert!(e.is_terminal());
    }
}

#[test]
fn quoting_doubles_delimiters_and_rejects_unusable_idents() {
    assert_eq!("wal_level".to_quoted_literal(), "'wal_level'");
    assert_eq!("a'b".to_quoted_literal(), "'a''b'");
    assert_eq!(ident("walrus_pub").unwrap().to_string(), "\"walrus_pub\"");
    assert_eq!(ident("a\"b").unwrap().to_string(), "\"a\"\"b\"");

    // A rejected name never reached the server, so it is its own class — and the rule it broke
    // stays typed in the chain rather than being re-read out of a message.
    let empty = ident("").unwrap_err();
    assert!(matches!(&empty, PreflightError::Ident(IdentError::Empty)));
    let cause = std::error::Error::source(&empty).expect("ident keeps the rule it broke");
    assert!(cause.to_string().contains("must not be empty"));
    assert!(matches!(
        ident("a\0b").unwrap_err(),
        PreflightError::Ident(IdentError::InteriorNul(name)) if name == "a\0b"
    ));
}

#[test]
fn unified_export_requires_a_real_primary_key() {
    for identity in [
        ReplicaIdentity::Default,
        ReplicaIdentity::Full,
        ReplicaIdentity::Index,
    ] {
        assert!(identity_is_usable(identity, true));
        assert!(!identity_is_usable(identity, false));
    }
    assert!(!identity_is_usable(ReplicaIdentity::Nothing, true));
    assert!(!identity_is_usable(ReplicaIdentity::Nothing, false));
}

#[test]
fn gap_and_signal_errors_name_their_remediation() {
    // An operator reading the crash log must be able to copy-paste the fix (reload H11).
    let gap = PreflightError::PublicationGap {
        pub_name: "walrus_pub".into(),
        schema: "walrus".into(),
        table: "reload_signal".into(),
    };
    assert!(
        gap.to_string()
            .contains("ALTER PUBLICATION walrus_pub ADD TABLE walrus.reload_signal")
    );

    let missing = PreflightError::ReloadSignalMissing {
        detail: "walrus.reload_signal table absent",
    };
    assert!(
        missing
            .to_string()
            .contains("migrations/source/0003_reload_signal.sql")
    );

    let missing = PreflightError::ReloadEventMissing {
        detail: "walrus.reload_event table absent",
    };
    assert!(
        missing
            .to_string()
            .contains("migrations/source/0004_reload_event.sql")
    );
}
