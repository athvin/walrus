use super::*;

fn a_duck_error() -> duckdb::Error {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch("SELECT no_such_function();")
        .unwrap_err()
}

#[test]
fn duck_preserves_the_typed_error_and_operation() {
    let source = a_duck_error();
    let expected_source = source.to_string();
    let got = Err::<(), duckdb::Error>(source)
        .duck("configure S3")
        .unwrap_err();

    assert_eq!(got.to_string(), "DuckDB: configure S3");
    assert_eq!(
        std::error::Error::source(&got).unwrap().to_string(),
        expected_source
    );
}

#[test]
fn duck_with_preserves_the_typed_error_and_formatted_operation() {
    let source = a_duck_error();
    let expected_source = source.to_string();
    let got = Err::<(), duckdb::Error>(source)
        .duck_with(|| format!("prune {}_raw", "orders"))
        .unwrap_err();

    assert_eq!(got.to_string(), "DuckDB: prune orders_raw");
    assert_eq!(
        std::error::Error::source(&got).unwrap().to_string(),
        expected_source
    );
}

#[test]
fn duck_with_does_not_format_on_success() {
    let mut called = false;
    let ok: Result<u8, duckdb::Error> = Ok(7);
    let value = ok
        .duck_with(|| {
            called = true;
            String::from("never rendered")
        })
        .unwrap();

    assert_eq!(value, 7);
    assert!(!called, "the operation closure must not run on the Ok path");
}
