//! DuckDB read-back conformance harness (Tier-1).
//!
//! The keystone of Phase 2b: write a `RecordBatch` to Parquet, then read it back through in-process
//! DuckDB and assert **both** the inferred DuckDB `typeof(col)` *and* the value — the only proof
//! that the byte we wrote is the DuckDB type we intended (DuckDB ignores arrow-rs's `ARROW:schema`
//! metadata and reads native Parquet logical types; §2.1). Gated behind `--features conformance`
//! so the bundled DuckDB compile stays out of the default build. Every Tier-2/3 PR (2.12–2.16)
//! appends one `#[test]` here.
#![cfg(feature = "conformance")]

use common::{Kind, Op, PgColumn, PgRelation, ReplicaIdentity, SinkMeta, TupleValue, UtcTimestamp};
use duckdb::Connection;
use pg_to_arrow::batch::BatchBuilder;
use pg_to_arrow::{oids, write_parquet_bytes};
use std::io::Write;

fn meta() -> SinkMeta {
    SinkMeta {
        op: Op::Insert,
        lsn: "0/10".parse().unwrap(),
        commit_lsn: "0/20".parse().unwrap(),
        commit_ts: UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00Z").unwrap(),
        xid: 1,
        epoch: 7,
        batch_id: "b1".to_string(),
        schema_version: 1,
        source_schema: "public".to_string(),
        source_table: "t".to_string(),
        kind: Kind::Stream,
        unchanged_toast: vec![],
        sink_instance: "walrus-pg-sink-0".to_string(),
        sink_processed_at: UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00Z").unwrap(),
    }
}

/// One-column ("c") RecordBatch of a given pg type from one text value.
fn one_col_batch(oid: u32, typmod: i32, value: &str) -> arrow::array::RecordBatch {
    let rel = PgRelation {
        oid: 1,
        schema: "public".to_string(),
        name: "t".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![PgColumn {
            name: "c".to_string(),
            type_oid: oid,
            type_modifier: typmod,
            is_key: false,
        }],
    };
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(&[TupleValue::Text(value.to_string())], &meta())
        .unwrap();
    b.finish().unwrap()
}

/// Write `bytes` to a temp `.parquet`, then run `sql` (with `{p}` = `read_parquet('<path>')`) in
/// in-process DuckDB. Returns each row as (duckdb_typeof, value-as-varchar).
fn read_parquet_rows(bytes: &[u8], sql: &str) -> Vec<(String, String)> {
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.write_all(bytes).unwrap();
    tmp.flush().unwrap();
    let path = tmp.path().to_string_lossy().replace('\'', "''");
    let full = sql.replace("{p}", &format!("read_parquet('{path}')"));

    let conn = Connection::open_in_memory().unwrap();
    let mut stmt = conn.prepare(&full).unwrap();
    let rows = stmt
        .query_map([], |row| {
            let t: String = row.get(0)?;
            let v: Option<String> = row.get(1)?;
            Ok((t, v.unwrap_or_else(|| "NULL".to_string())))
        })
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

/// `typeof(c)` and `CAST(c AS VARCHAR)` for the single `c` column of a one-row batch.
fn typeof_and_value(oid: u32, typmod: i32, value: &str) -> (String, String) {
    let bytes = write_parquet_bytes(&one_col_batch(oid, typmod, value)).unwrap();
    let rows = read_parquet_rows(&bytes, "SELECT typeof(c), CAST(c AS VARCHAR) FROM {p}");
    assert_eq!(rows.len(), 1, "expected one row");
    rows.into_iter().next().unwrap()
}

#[test]
fn smallint_reads_back_as_int16_smallint() {
    let (t, v) = typeof_and_value(oids::INT2, -1, "7");
    assert_eq!(t, "SMALLINT");
    assert_eq!(v, "7");
}

#[test]
fn bool_int_float_roundtrip_type_and_value() {
    assert_eq!(
        typeof_and_value(oids::BOOL, -1, "t"),
        ("BOOLEAN".into(), "true".into())
    );
    assert_eq!(
        typeof_and_value(oids::INT4, -1, "42"),
        ("INTEGER".into(), "42".into())
    );
    assert_eq!(
        typeof_and_value(oids::INT8, -1, "9000000000"),
        ("BIGINT".into(), "9000000000".into())
    );
    assert_eq!(
        typeof_and_value(oids::FLOAT4, -1, "1.5"),
        ("FLOAT".into(), "1.5".into())
    );
    assert_eq!(
        typeof_and_value(oids::FLOAT8, -1, "2.25"),
        ("DOUBLE".into(), "2.25".into())
    );
}

#[test]
fn decimal_10_2_reads_back_as_decimal_10_2() {
    let (t, v) = typeof_and_value(oids::NUMERIC, 655366, "19.99"); // numeric(10,2)
    assert_eq!(t, "DECIMAL(10,2)");
    assert_eq!(v, "19.99");
}

#[test]
fn timestamptz_is_adjusted_to_utc() {
    let (t, v) = typeof_and_value(oids::TIMESTAMPTZ, -1, "2024-01-02 03:04:05+00");
    assert_eq!(t, "TIMESTAMP WITH TIME ZONE");
    assert!(v.contains("2024-01-02"), "value was {v}");
}

#[test]
fn timestamp_is_not_adjusted() {
    let (t, v) = typeof_and_value(oids::TIMESTAMP, -1, "2024-01-02 03:04:05.678901");
    assert_eq!(t, "TIMESTAMP");
    assert!(v.contains("2024-01-02 03:04:05.678901"), "value was {v}");
}

#[test]
fn time_is_micros() {
    let (t, v) = typeof_and_value(oids::TIME, -1, "03:04:05.678901");
    assert_eq!(t, "TIME");
    assert!(v.contains("03:04:05.678901"), "value was {v}");
}

#[test]
fn date_reads_back_as_date() {
    let (t, v) = typeof_and_value(oids::DATE, -1, "2024-01-02");
    assert_eq!(t, "DATE");
    assert_eq!(v, "2024-01-02");
}

#[test]
fn bytea_reads_back_as_blob() {
    let (t, _v) = typeof_and_value(oids::BYTEA, -1, "\\x6869"); // "hi"
    assert_eq!(t, "BLOB");
}

#[test]
fn json_is_verbatim() {
    let (t, v) = typeof_and_value(oids::JSON, -1, "{\"a\": 1}");
    assert_eq!(t, "VARCHAR");
    assert_eq!(v, "{\"a\": 1}");
}
