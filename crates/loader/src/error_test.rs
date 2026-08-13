use super::*;
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
    assert!(messages
        .iter()
        .any(|message| message.contains("_walrus_lsn")));
}

#[test]
fn duck_still_exits_internal() {
    assert_eq!(duck_error().exit_code(), common::ExitCode::Internal);
}
