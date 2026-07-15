//! Destructive DDL apply (loader §5.7, PR 3.9) — where mirror and raw **diverge**. Three hermetic tests
//! (in-memory / temp-file `TableDb`) prove the per-taxonomy behaviour; the `#[ignore]` compose test
//! proves a lossy-cast failure quarantines the table and degrades `/ready`.
//!
//!   cargo test -p loader --test ddl_destructive              # hermetic
//!   cargo test -p loader --test ddl_destructive -- --ignored # + compose (quarantine)

use common::{PgColumn, PgRelation, ReplicaIdentity};
use loader::ddl::{apply_destructive, retire_file, DestructiveChange};
use loader::duck::{S3Access, TableDb};
use loader::error::LoaderError;
use loader::health::LoaderState;
use loader::phase_a::{run_phase_a, TableCtx};
use loader::phase_b::run_phase_b;
use std::time::Duration;

fn col(name: &str, oid: u32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    }
}

fn rel(name: &str, columns: Vec<PgColumn>) -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: name.into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

fn mem(rel: &PgRelation) -> TableDb {
    let db = TableDb::open(std::path::Path::new(":memory:")).unwrap();
    db.ensure_tables(rel, 1).unwrap();
    db
}

fn columns_of(conn: &duckdb::Connection, name: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
        )
        .unwrap();
    let rows = stmt.query_map([name], |r| r.get::<_, String>(0)).unwrap();
    rows.map(Result::unwrap).collect()
}

fn data_type_of(conn: &duckdb::Connection, table: &str, column: &str) -> String {
    conn.query_row(
        "SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
        [table, column],
        |r| r.get::<_, String>(0),
    )
    .unwrap()
}

fn table_exists(conn: &duckdb::Connection, name: &str) -> bool {
    let n: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
            |r| r.get(0),
        )
        .unwrap();
    n > 0
}

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("walrus-loader-ddld-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

// ---- DROP COLUMN: physical on the mirror, retained-nullable on raw. ----
#[test]
fn drop_column_physical_on_mirror_retained_nullable_on_raw() {
    let db = mem(&rel(
        "orders",
        vec![col("id", 23, true), col("x", 25, false)],
    ));
    apply_destructive(
        db.conn(),
        "orders",
        &[DestructiveChange::DropColumn { name: "x".into() }],
    )
    .unwrap();

    assert!(
        !columns_of(db.conn(), "orders").iter().any(|c| c == "x"),
        "mirror physically drops the column"
    );
    assert!(
        columns_of(db.conn(), "orders_raw").iter().any(|c| c == "x"),
        "raw RETAINS the column (verbatim history)"
    );
    // The retained raw column is nullable — a post-drop verbatim row omitting it reads NULL.
    db.conn()
        .execute(
            "INSERT INTO orders_raw (id, walrus_pg_sink_meta, \"_walrus_op\", \
             \"_walrus_commit_lsn\", \"_walrus_lsn\", \"_walrus_sink_processed_at\") \
             VALUES (1, '{}', 'i', 'A', 'B', 'C')",
            [],
        )
        .unwrap();
    let x: Option<String> = db
        .conn()
        .query_row("SELECT x FROM orders_raw WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(x, None, "post-drop file NULL-fills the retained raw column");
    // The user view refreshed — no dropped column.
    assert!(!columns_of(db.conn(), "orders_current")
        .iter()
        .any(|c| c == "x"));
}

// ---- Lossy ALTER TYPE: raw widens to VARCHAR (history preserved), never re-cast. ----
#[test]
fn lossy_type_change_widens_raw_to_varchar_without_recasting() {
    // Mirror value fits the narrowed type (42 → SMALLINT); raw holds a BIG value (99999) that would
    // OVERFLOW a smallint cast — proving raw is widened to VARCHAR and never re-cast.
    let db = mem(&rel(
        "orders",
        vec![col("id", 23, true), col("n", 23, false)],
    )); // n int4
    db.conn()
        .execute("INSERT INTO orders (id, n) VALUES (1, 42)", [])
        .unwrap();
    db.conn()
        .execute(
            "INSERT INTO orders_raw (id, n, walrus_pg_sink_meta, \"_walrus_op\", \
             \"_walrus_commit_lsn\", \"_walrus_lsn\", \"_walrus_sink_processed_at\") \
             VALUES (1, 99999, '{}', 'i', 'A', 'B', 'C')",
            [],
        )
        .unwrap();

    apply_destructive(
        db.conn(),
        "orders",
        &[DestructiveChange::LossyType {
            name: "n".into(),
            new: col("n", 21, false), // int4 → int2 (narrowing / lossy)
        }],
    )
    .unwrap();

    // Raw widened to VARCHAR; the big value survives as text (never re-cast → no overflow).
    assert_eq!(data_type_of(db.conn(), "orders_raw", "n"), "VARCHAR");
    let raw_n: String = db
        .conn()
        .query_row("SELECT n FROM orders_raw WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(raw_n, "99999", "raw value preserved as text, not re-cast");

    // Mirror cast succeeded in place (42 fits SMALLINT).
    assert_eq!(data_type_of(db.conn(), "orders", "n"), "SMALLINT");
    let m_n: i16 = db
        .conn()
        .query_row("SELECT n FROM orders WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(m_n, 42);
}

// ---- DROP TABLE: retire both DuckDB tables + the .duckdb file (idempotent). ----
#[test]
fn drop_table_retires_both_tables_and_the_file() {
    let dir = tmpdir("drop");
    let path = dir.join("orders.duckdb");
    {
        let db = TableDb::open(&path).unwrap();
        db.ensure_tables(
            &rel(
                "orders",
                vec![col("id", 23, true), col("status", 25, false)],
            ),
            1,
        )
        .unwrap();
        apply_destructive(
            db.conn(),
            "orders",
            &[DestructiveChange::DropTable {
                name: "orders".into(),
            }],
        )
        .unwrap();
        assert!(!table_exists(db.conn(), "orders"), "mirror retired");
        assert!(!table_exists(db.conn(), "orders_raw"), "CDC log retired");
    } // drop the connection → release the DuckDB file lock

    assert!(path.exists(), "file present until explicitly retired");
    retire_file(&path).unwrap();
    assert!(!path.exists(), "the .duckdb file is retired");
    retire_file(&path).unwrap(); // idempotent — a crash-mid-retire re-run is a no-op
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- Compose: a lossy cast that fails quarantines the table and degrades /ready. ----

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

fn s3() -> S3Access {
    S3Access {
        endpoint: "localhost:9000".into(),
        region: "us-east-1".into(),
        access_key_id: "minioadmin".into(),
        secret_access_key: "minioadmin".into(),
        use_ssl: false,
    }
}

fn meta(commit_hex: &str, l: u64) -> String {
    format!(
        "{{\"op\":\"i\",\"commit_lsn\":\"{commit_hex}\",\"lsn\":\"{:016X}\",\"sink_processed_at\":\"2026-07-07T12:00:{:02}Z\"}}",
        l, l % 60
    )
}

/// Write an (id, n, walrus_pg_sink_meta) Parquet fixture to S3.
fn write_fixture(epoch: i64, tag: &str, id: i64, n: i64, commit_hex: &str, l: u64) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    let a = s3();
    w.execute_batch(&format!(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='{}'; SET s3_endpoint='{}'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='{}'; SET s3_secret_access_key='{}';",
        a.region, a.endpoint, a.access_key_id, a.secret_access_key
    ))
    .unwrap();
    w.execute_batch("CREATE TABLE fixture (id INTEGER, n INTEGER, walrus_pg_sink_meta VARCHAR);")
        .unwrap();
    w.execute(
        "INSERT INTO fixture VALUES (?, ?, ?)",
        duckdb::params![id, n, meta(commit_hex, l)],
    )
    .unwrap();
    let uri = format!("s3://walrus/{epoch}/public/orders/{tag}-{epoch}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn lossy_cast_failure_quarantines_the_table_and_alerts() {
    let _g = LOCK.lock().await;
    let epoch = 3_900_001;
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    for tbl in [
        "file_manifest",
        "loader_checkpoint",
        "schema_registry",
        "replication_state",
    ] {
        let _ = sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(&pool)
            .await;
    }
    control::insert_epoch(
        &pool,
        &control::ReplicationState {
            epoch,
            slot_name: "walrus_slot".into(),
            created_lsn: "0/0".parse().unwrap(),
            status: "streaming".into(),
        },
    )
    .await
    .unwrap();
    control::ensure_checkpoint(&pool, epoch, "public", "orders")
        .await
        .unwrap();

    // v1: n is int4. Seed a v1 file with a value (99999) that will NOT fit the later int2 narrowing.
    let orders_v1 = rel("orders", vec![col("id", 23, true), col("n", 23, false)]);
    control::upsert_registry(
        &pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            schema_version: 1,
            descriptors: Vec::new(),
            columns: serde_json::to_value(&orders_v1).unwrap(),
        },
    )
    .await
    .unwrap();
    let v1 = write_fixture(epoch, "v1", 1, 99999, "0000000000000064", 1);
    control::insert_ready(
        &pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: v1,
            kind: "stream".into(),
            row_count: 1,
            lsn_start: "0/64".parse().unwrap(),
            lsn_end: "0/64".parse().unwrap(),
            schema_version: 1,
            reload_id: None,
        },
    )
    .await
    .unwrap();

    let dir = tmpdir(&epoch.to_string());
    let db = TableDb::open(&dir.join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders_v1, 1).unwrap();
    db.configure_s3(&s3()).unwrap();
    let state = LoaderState::new();
    let ctx = TableCtx {
        pool,
        epoch,
        schema: "public".into(),
        table: "orders".into(),
        rel: orders_v1,
        db,
        state: state.clone(),
        max_files: 100,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
    };

    // Process v1 fully so the mirror holds n=99999 BEFORE the lossy DDL reconcile runs.
    run_phase_a(&ctx).await.unwrap();
    run_phase_b(&ctx).await.unwrap();
    state.mark_ready();
    assert!(ctx.state.is_ready(), "ready after bootstrap+first cycle");

    // v2: n narrows to int2 (lossy). A v2 file triggers the reconcile before it is appended.
    let orders_v2 = rel("orders", vec![col("id", 23, true), col("n", 21, false)]);
    control::upsert_registry(
        &ctx.pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            schema_version: 2,
            descriptors: Vec::new(),
            columns: serde_json::to_value(&orders_v2).unwrap(),
        },
    )
    .await
    .unwrap();
    let v2 = write_fixture(epoch, "v2", 2, 5, "00000000000000C8", 10);
    control::insert_ready(
        &ctx.pool,
        &control::NewManifestFile {
            epoch,
            source_schema: "public".into(),
            source_table: "orders".into(),
            s3_uri: v2,
            kind: "stream".into(),
            row_count: 1,
            lsn_start: "0/C8".parse().unwrap(),
            lsn_end: "0/C8".parse().unwrap(),
            schema_version: 2,
            reload_id: None,
        },
    )
    .await
    .unwrap();

    // The lossy int4→int2 cast on the mirror (which holds 99999) overflows → quarantine.
    let result = run_phase_a(&ctx).await;
    assert!(
        matches!(result, Err(LoaderError::Quarantine { .. })),
        "a failed lossy cast quarantines the table: {result:?}"
    );
    assert!(
        ctx.state.is_quarantined(),
        "quarantine latched on the state"
    );
    assert!(!ctx.state.is_ready(), "/ready degrades on quarantine");

    // raw was widened to VARCHAR (history preserved); the mirror value was NOT destroyed.
    let raw_type: String = ctx
        .db
        .conn()
        .query_row(
            "SELECT data_type FROM information_schema.columns WHERE table_name='orders_raw' AND column_name='n'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(raw_type, "VARCHAR", "raw widened to VARCHAR, not re-cast");

    let _ = std::fs::remove_dir_all(&dir);
}
