use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, is_key: bool| PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

/// Write a local `(id, status, walrus_pg_sink_meta)` Parquet whose rows carry `commit_lsn = placeholder`
/// — mimicking a speculative spill written before its txn's commit LSN was known.
fn write_local_fixture(dir: &Path, name: &str, ids: (i64, i64), placeholder: &str) -> String {
    let path = dir.join(name);
    let uri = path.to_string_lossy().replace('\'', "''");
    let w = duckdb::Connection::open_in_memory().unwrap();
    let meta = |lsn: &str| {
        format!(
            "{{\"op\":\"Insert\",\"commit_lsn\":\"{placeholder}\",\"lsn\":\"{lsn}\",\
                  \"sink_processed_at\":\"2026-07-08T12:00:0{lsn}Z\"}}"
        )
    };
    w.execute_batch(&format!(
        "CREATE TABLE fixture (id BIGINT, status VARCHAR, walrus_pg_sink_meta VARCHAR); \
             INSERT INTO fixture VALUES \
               ({}, 'a', '{}'), ({}, 'b', '{}'); \
             COPY fixture TO '{uri}' (FORMAT PARQUET);",
        ids.0,
        meta("1"),
        ids.1,
        meta("2"),
    ))
    .unwrap();
    uri
}

fn commit_lsns(db: &TableDb, ids: (i64, i64)) -> Vec<String> {
    let mut stmt = db
        .conn
        .prepare("SELECT \"_walrus_commit_lsn\" FROM orders_raw WHERE id IN (?, ?) ORDER BY id")
        .unwrap();
    stmt.query_map([ids.0, ids.1], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// PR 4.3 fix: a `spill` file's per-row `commit_lsn` placeholder is overridden by the file's `lsn_end`
/// (the real commit LSN), while a non-spill file appends the per-row value verbatim.
#[test]
fn spill_override_stamps_lsn_end_but_verbatim_otherwise() {
    let dir = std::env::temp_dir().join("walrus-loader-spill-override");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = TableDb::open(&dir.join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), 1).unwrap();
    // Local Parquet + JSON extraction need the json extension (no S3 here → configure_s3 is not called).
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();

    // A spill file: rows carry the placeholder `0000000000000064`, but the file committed at `…00C8`.
    let placeholder = "0000000000000064";
    let lsn_end = "00000000000000C8";
    let spill = write_local_fixture(&dir, "spill.parquet", (1, 2), placeholder);
    let n = db
        .append_parquet("orders", &spill, 1, Some(lsn_end))
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        commit_lsns(&db, (1, 2)),
        vec![lsn_end, lsn_end],
        "a spill file's rows are stamped with the file's lsn_end, not the placeholder"
    );

    // A non-spill (verbatim) file: the per-row placeholder is preserved.
    let batch = write_local_fixture(&dir, "batch.parquet", (3, 4), placeholder);
    let n = db.append_parquet("orders", &batch, 1, None).unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        commit_lsns(&db, (3, 4)),
        vec![placeholder, placeholder],
        "a non-spill file keeps its verbatim per-row commit_lsn"
    );

    // PR 5.8: both files are schema_version 1 → the column list is DESCRIBEd once, cached, reused.
    assert_eq!(
        db.cached_schema_versions(),
        1,
        "two v1 files → one cached introspection, not per-file"
    );
}
