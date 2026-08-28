use super::*;
use common::FailureClass;
use std::error::Error as _;

fn duck_error() -> LoaderError {
    LoaderError::Duck {
        op: "append s3://bucket/f.parquet → orders_raw".to_string(),
        source: duckdb::Error::InvalidColumnName("_walrus_lsn".to_string()),
    }
}

#[test]
fn duck_preserves_the_engine_error_as_source() {
    let error = duck_error();
    assert_eq!(
        error.to_string(),
        "DuckDB: append s3://bucket/f.parquet → orders_raw"
    );
    assert!(error.source().is_some());

    let mut messages = Vec::new();
    let mut cause = error.source();
    while let Some(source) = cause {
        messages.push(source.to_string());
        cause = source.source();
    }
    assert!(
        messages
            .iter()
            .any(|message| message.contains("_walrus_lsn"))
    );
}

/// The log-site contract behind `error = ?e` in `main` and `supervisor`: `Display` names the
/// operation only, so `%e` would drop the engine failure entirely, while `Debug` carries it.
#[test]
fn debug_surfaces_the_cause_that_display_omits() {
    let error = duck_error();

    assert!(!format!("{error}").contains("_walrus_lsn"));
    assert!(format!("{error:?}").contains("_walrus_lsn"));
}

#[test]
fn duck_still_exits_internal() {
    assert_eq!(duck_error().exit_code(), common::ExitCode::Internal);
}

#[test]
fn registry_decode_preserves_source_and_exits_internal() {
    let source = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
    let error = LoaderError::RegistryDecode {
        table: "public.orders".to_string(),
        version: 7,
        source,
    };

    assert!(error.source().is_some());
    assert_eq!(error.exit_code(), common::ExitCode::Internal);
}

#[test]
fn lsn_parse_keeps_the_offending_input_in_the_chain() {
    let source = "zz/1".parse::<common::Lsn>().unwrap_err();
    let error = LoaderError::LsnParse {
        field: "max commit_lsn",
        source,
    };

    let cause = error.source().expect("LSN parse source");
    assert!(cause.to_string().contains("zz/1"));
    assert_eq!(error.exit_code(), common::ExitCode::Internal);
}

#[test]
fn control_txn_failure_preserves_source_and_exits_control_db() {
    let error = LoaderError::ControlTxn {
        op: "begin advance+delete txn",
        source: sqlx::Error::PoolClosed,
    };

    assert!(error.source().is_some());
    assert_eq!(error.exit_code(), common::ExitCode::ControlDb);
}

#[test]
fn health_failure_preserves_source_and_exits_internal() {
    let error = LoaderError::Health {
        op: "bind",
        source: Box::new(std::io::Error::other("address unavailable")),
    };

    assert!(error.source().is_some());
    assert_eq!(error.exit_code(), common::ExitCode::Internal);
}
