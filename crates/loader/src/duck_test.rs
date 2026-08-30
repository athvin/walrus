use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};

fn requires_send<T: Send>() {}

/// The exact closure and result bounds imposed by `tokio::task::spawn_blocking`. This is
/// intentionally instantiated only with an owned legal closure: current DuckDB work borrows a
/// `TableDb`, whose negative shared-reference bounds are guarded by `TableDb`'s compile-fail docs.
fn requires_spawn_blocking_bounds<F, R>(_f: F)
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
}

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

/// `S3Access` derives `Debug` and is carried by every worker, so a stray `?s3` in a diagnostic
/// would ship the bucket credential to the log aggregator. The endpoint, region and key id stay
/// legible — they are identifiers an operator needs; only the secret half is withheld.
#[test]
fn debug_renders_the_bucket_secret_but_not_its_neighbours() {
    let access = S3Access {
        endpoint: "minio:9000".to_string(),
        region: "eu-west-2".to_string(),
        access_key_id: "AKIAEXAMPLE".to_string(),
        secret_access_key: "wJalrXUtnFEMI".into(),
        use_ssl: false,
    };

    let rendered = format!("{access:?}");

    assert!(!rendered.contains("wJalrXUtnFEMI"), "{rendered}");
    assert!(rendered.contains("AKIAEXAMPLE"), "{rendered}");
    assert!(rendered.contains(common::REDACTED), "{rendered}");
}

#[test]
fn in_txn_commits_on_ok() {
    let db = TableDb::open(":memory:").unwrap();

    db.in_txn("probe", |conn| {
        conn.execute_batch("CREATE TABLE txn_probe (id INTEGER); INSERT INTO txn_probe VALUES (1);")
            .duck("commit probe body")
    })
    .unwrap();

    let rows: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM txn_probe", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1, "an Ok transaction body is committed");
}

#[test]
fn in_txn_rolls_back_on_err() {
    let db = TableDb::open(":memory:").unwrap();
    db.conn()
        .execute_batch("CREATE TABLE txn_probe (id INTEGER);")
        .unwrap();

    let error = db
        .in_txn("probe", |conn| {
            conn.execute_batch("INSERT INTO txn_probe VALUES (1);")
                .duck("rollback probe body")?;
            Err::<(), LoaderError>(LoaderError::Quarantine {
                table: "public.orders".into(),
                reason: "identity sentinel".into(),
            })
        })
        .unwrap_err();

    match error {
        LoaderError::Quarantine { table, reason } => {
            assert_eq!(table, "public.orders");
            assert_eq!(reason, "identity sentinel");
        }
        other => panic!("transaction changed the body error: {other:?}"),
    }
    let rows: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM txn_probe", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0, "an Err transaction body is rolled back");

    db.in_txn("following", |conn| {
        conn.execute_batch("INSERT INTO txn_probe VALUES (2);")
            .duck("following transaction body")
    })
    .unwrap();
    let rows: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM txn_probe", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1, "rollback leaves the connection transaction-ready");
}

#[test]
fn in_txn_accepts_a_move_consuming_body() {
    let db = TableDb::open(":memory:").unwrap();
    let sql = String::from("CREATE TABLE t_once (a INTEGER);");

    db.in_txn("once", move |conn| {
        conn.execute_batch(&sql).duck("once body")?;
        drop(sql);
        Ok(())
    })
    .unwrap();
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

/// PR 12.5: `TableDb` is `Send`. The apply worker moves one into `TableCtx` and then into a
/// `spawn_local` future; this test exercises the bound by moving the database across an OS thread.
#[test]
fn table_db_moves_across_a_thread() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(7))
        .unwrap();

    let handle = std::thread::spawn(move || db.schema_version().unwrap());
    let version = handle.join().unwrap();
    assert_eq!(
        version,
        common::SchemaVersionNo(7),
        "the schema version survives the thread move"
    );
}

/// Bootstrap reads the watermark BEFORE the seeding `ensure_tables*`, so "no watermark yet" must be
/// a value and not an error the caller has to paper over with a fallback version — a papered-over
/// read failure would answer with the registry version the pending-rebuild branch exists to avoid.
#[test]
fn stored_schema_version_reports_absence_without_an_error() {
    let db = TableDb::open(":memory:").unwrap();
    assert_eq!(
        db.stored_schema_version().unwrap(),
        None,
        "a brand-new file has no _walrus_meta to read"
    );

    db.ensure_tables(&orders(), common::SchemaVersionNo(4))
        .unwrap();
    assert_eq!(
        db.stored_schema_version().unwrap(),
        Some(common::SchemaVersionNo(4)),
        "the seeded watermark reads back"
    );
}

#[test]
fn owned_duckdb_handles_meet_the_blocking_pool_send_bound() {
    requires_send::<duckdb::Connection>();
    requires_send::<TableDb>();
    requires_spawn_blocking_bounds(|| 1_i64);
}

/// PR 4.3 fix: a `spill` file's per-row `commit_lsn` placeholder is overridden by the file's `lsn_end`
/// (the real commit LSN), while a non-spill file appends the per-row value verbatim.
#[test]
fn spill_override_stamps_lsn_end_but_verbatim_otherwise() {
    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), common::SchemaVersionNo(1))
        .unwrap();
    // Local Parquet + JSON extraction need the json extension (no S3 here → configure_s3 is not called).
    db.conn.execute_batch("INSTALL json; LOAD json;").unwrap();

    // A spill file: rows carry the placeholder `0000000000000064`, but the file committed at `…00C8`.
    let placeholder = "0000000000000064";
    let lsn_end = "00000000000000C8";
    let spill = write_local_fixture(dir.path(), "spill.parquet", (1, 2), placeholder);
    let n = db
        .append_parquet("orders", &spill, common::SchemaVersionNo(1), Some(lsn_end))
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        commit_lsns(&db, (1, 2)),
        vec![lsn_end, lsn_end],
        "a spill file's rows are stamped with the file's lsn_end, not the placeholder"
    );

    // A non-spill (verbatim) file: the per-row placeholder is preserved.
    let batch = write_local_fixture(dir.path(), "batch.parquet", (3, 4), placeholder);
    let n = db
        .append_parquet("orders", &batch, common::SchemaVersionNo(1), None)
        .unwrap();
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

#[test]
fn parquet_column_cache_hit_then_mutation_capable_miss() {
    assert!(
        include_str!("duck.rs")
            .contains("let cached = { self.parquet_cols.borrow().get(&schema_version).cloned() };")
    );

    let dir = tempfile::tempdir().unwrap();
    let db = TableDb::open(dir.path().join("cache.duckdb")).unwrap();
    let parquet = write_local_fixture(dir.path(), "columns.parquet", (1, 2), "0");

    let initial = db
        .columns_for(&parquet, common::SchemaVersionNo(1))
        .unwrap();
    let hit = db
        .columns_for("not-read-on-cache-hit.parquet", common::SchemaVersionNo(1))
        .unwrap();
    assert!(Arc::ptr_eq(&initial, &hit), "cache hit reuses the same Arc");

    let miss = db
        .columns_for(&parquet, common::SchemaVersionNo(2))
        .unwrap();
    assert_eq!(&*miss, &*initial);
    assert_eq!(
        db.cached_schema_versions(),
        2,
        "the miss can mutably insert after the hit borrow ends"
    );
}
