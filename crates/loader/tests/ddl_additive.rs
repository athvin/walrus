#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Additive/lossless DDL apply (loader §5.7, architecture per-change-type). The four hermetic tests
//! (`Connection::open_in_memory()` via `TableDb`) prove the schema-DIFF + DuckDB `ALTER`s per taxonomy
//! row; the `#[ignore]` compose test proves both tables evolve at the correct LSN relative to data.
//!
//!   cargo test -p loader --test ddl_additive              # hermetic
//!   cargo test -p loader --test ddl_additive -- --ignored # + compose

use common::{PgColumn, PgRelation, ReplicaIdentity};
use loader::ddl::{apply_additive, diff_additive, AdditiveChange, CommentTarget, SchemaVersion};
use loader::duck::{S3Access, TableDb};
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

fn sv(version: i64, relation: PgRelation) -> SchemaVersion {
    SchemaVersion { version, relation }
}

fn mem(rel: &PgRelation) -> TableDb {
    let db = TableDb::open(std::path::Path::new(":memory:")).unwrap();
    db.ensure_tables(rel, 1).unwrap();
    db
}

/// Column names of a table/view, in ordinal order.
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

// ---- ADD COLUMN: mirror gets it; raw gets it NULLABLE; pre-change rows read NULL. ----
#[test]
fn add_column_mirror_and_raw_nullable_old_rows_null() {
    let db = mem(&rel(
        "orders",
        vec![col("id", 23, true), col("status", 25, false)],
    ));
    let conn = db.conn();
    conn.execute("INSERT INTO orders (id, status) VALUES (1, 'a')", [])
        .unwrap(); // a pre-change mirror row

    apply_additive(
        conn,
        "orders",
        &[AdditiveChange::AddColumn(col("note", 25, false))],
    )
    .unwrap();

    // Mirror: the pre-change row reads NULL for the new column.
    let note: Option<String> = conn
        .query_row("SELECT note FROM orders WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        note, None,
        "pre-change mirror row reads NULL for the added column"
    );

    // Raw: the column is added NULLABLE — a verbatim row omitting it stays valid.
    conn.execute(
        "INSERT INTO orders_raw (id, status, walrus_pg_sink_meta, \"_walrus_op\", \
         \"_walrus_commit_lsn\", \"_walrus_lsn\", \"_walrus_sink_processed_at\") \
         VALUES (2, 'b', '{}', 'i', 'A', 'B', 'C')",
        [],
    )
    .unwrap();
    let raw_note: Option<String> = conn
        .query_row("SELECT note FROM orders_raw WHERE id = 2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        raw_note, None,
        "raw ADD COLUMN is nullable — old verbatim rows read NULL"
    );

    // The user view is refreshed to include the new column, still hiding the guard columns.
    let view = columns_of(conn, "orders_current");
    assert!(view.iter().any(|c| c == "note"), "view refreshed: {view:?}");
    assert!(
        !view.iter().any(|c| c.starts_with("_applied")),
        "guard cols stay hidden"
    );
}

// ---- RENAME COLUMN resolved by ATTNUM (position), never name — the RENAME-then-ADD trap. ----
#[test]
fn rename_column_tracked_by_attnum_not_name() {
    // v1: (id, a). v2: (id, b, a) — position 1 RENAMED a→b, and a NEW column also named `a` appended.
    // Name-matching would read "a unchanged, b added" and corrupt the mapping; position-matching does not.
    let old = sv(
        1,
        rel("orders", vec![col("id", 23, true), col("a", 25, false)]),
    );
    let new = sv(
        2,
        rel(
            "orders",
            vec![
                col("id", 23, true),
                col("b", 25, false),
                col("a", 25, false),
            ],
        ),
    );
    let changes = diff_additive(&old, &new).unwrap();
    assert_eq!(
        changes.len(),
        2,
        "a rename + an add, not an add of `b` alone"
    );
    assert!(
        matches!(&changes[0], AdditiveChange::RenameColumn { position, from, to } if *position == 1 && from == "a" && to == "b"),
        "position 1 renamed a→b"
    );
    assert!(
        matches!(&changes[1], AdditiveChange::AddColumn(c) if c.name == "a"),
        "the freshly-added `a` is a NEW column, not the old one"
    );

    // Applying it: both tables end (id, b, a) — exactly one `a`, plus `b`.
    let db = mem(&old.relation);
    apply_additive(db.conn(), "orders", &changes).unwrap();
    for t in ["orders", "orders_raw"] {
        let cols = columns_of(db.conn(), t);
        assert!(cols.iter().any(|c| c == "b"), "{t} renamed a→b: {cols:?}");
        assert_eq!(
            cols.iter().filter(|c| *c == "a").count(),
            1,
            "{t} has exactly one `a`"
        );
    }
}

// ---- A lossless/widening ALTER COLUMN TYPE casts in place on BOTH tables. ----
#[test]
fn widening_type_change_casts_in_place_both_tables() {
    let old = sv(
        1,
        rel("orders", vec![col("id", 23, true), col("n", 23, false)]),
    ); // n int4
    let new = sv(
        2,
        rel("orders", vec![col("id", 23, true), col("n", 20, false)]),
    ); // n int8
    let changes = diff_additive(&old, &new).unwrap();
    assert!(
        matches!(&changes[0], AdditiveChange::WidenColumn { name, .. } if name == "n"),
        "int4→int8 is a lossless widen"
    );

    let db = mem(&old.relation);
    db.conn()
        .execute("INSERT INTO orders (id, n) VALUES (1, 5)", [])
        .unwrap();
    apply_additive(db.conn(), "orders", &changes).unwrap();

    // A value beyond int32 now fits — the column is genuinely BIGINT on both tables.
    db.conn()
        .execute("INSERT INTO orders (id, n) VALUES (2, 5000000000)", [])
        .unwrap();
    let n2: i64 = db
        .conn()
        .query_row("SELECT n FROM orders WHERE id = 2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n2, 5_000_000_000);
    assert_eq!(data_type_of(db.conn(), "orders", "n"), "BIGINT");
    assert_eq!(data_type_of(db.conn(), "orders_raw", "n"), "BIGINT");
}

// ---- COMMENT is mirrored onto <table> ONLY (never <table>_raw) and does not gate data. ----
#[test]
fn comment_mirrored_onto_mirror_only_not_raw() {
    let db = mem(&rel(
        "orders",
        vec![col("id", 23, true), col("status", 25, false)],
    ));
    apply_additive(
        db.conn(),
        "orders",
        &[
            AdditiveChange::Comment {
                target: CommentTarget::Table,
                text: Some("customer orders".into()),
            },
            AdditiveChange::Comment {
                target: CommentTarget::Column("status".into()),
                text: Some("fulfilment state".into()),
            },
        ],
    )
    .unwrap();

    let table_comment: Option<String> = db
        .conn()
        .query_row(
            "SELECT comment FROM duckdb_tables() WHERE table_name = 'orders'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(table_comment.as_deref(), Some("customer orders"));
    let col_comment: Option<String> = db
        .conn()
        .query_row(
            "SELECT comment FROM duckdb_columns() WHERE table_name = 'orders' AND column_name = 'status'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(col_comment.as_deref(), Some("fulfilment state"));

    // The CDC log is untouched — COMMENT is mirror-only metadata.
    let raw_comment: Option<String> = db
        .conn()
        .query_row(
            "SELECT comment FROM duckdb_tables() WHERE table_name = 'orders_raw'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(raw_comment, None, "COMMENT never touches <table>_raw");
}

// ---- Destructive / lossy changes are deferred to PR 3.9 — an explicit error, never a silent cast. ----
#[test]
fn destructive_and_lossy_changes_are_errors_deferred_to_3_9() {
    // A dropped column (fewer columns) → error.
    let drop = diff_additive(
        &sv(1, rel("t", vec![col("id", 23, true), col("x", 25, false)])),
        &sv(2, rel("t", vec![col("id", 23, true)])),
    );
    assert!(drop.is_err(), "a DROP COLUMN is destructive → PR 3.9");

    // A narrowing cast (int8→int4) → error.
    let narrow = diff_additive(
        &sv(1, rel("t", vec![col("id", 23, true), col("n", 20, false)])),
        &sv(2, rel("t", vec![col("id", 23, true), col("n", 23, false)])),
    );
    assert!(narrow.is_err(), "a narrowing type change is lossy → PR 3.9");
}

// ---- Compose: both tables evolve at the correct LSN relative to data (the homogeneous-file rule). ----

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

fn orders_v1() -> PgRelation {
    rel(
        "orders",
        vec![col("id", 23, true), col("status", 25, false)],
    )
}

fn orders_v2() -> PgRelation {
    rel(
        "orders",
        vec![
            col("id", 23, true),
            col("status", 25, false),
            col("note", 25, false),
        ],
    )
}

fn tmpdir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("walrus-loader-ddl-{name}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn meta(op: &str, commit_hex: &str, l: u64) -> String {
    format!(
        "{{\"op\":\"{op}\",\"commit_lsn\":\"{commit_hex}\",\"lsn\":\"{:016X}\",\"sink_processed_at\":\"2026-07-07T12:00:{:02}Z\"}}",
        l, l % 60
    )
}

/// Write a homogeneous Parquet fixture to S3. `with_note` = the v2 shape (adds the `note` column).
fn write_fixture(
    epoch: i64,
    tag: &str,
    with_note: bool,
    rows: &[(i64, &str, &str, &str, u64)],
) -> String {
    let w = duckdb::Connection::open_in_memory().unwrap();
    let a = s3();
    w.execute_batch(&format!(
        "INSTALL httpfs; LOAD httpfs; SET s3_region='{}'; SET s3_endpoint='{}'; \
         SET s3_url_style='path'; SET s3_use_ssl=false; \
         SET s3_access_key_id='{}'; SET s3_secret_access_key='{}';",
        a.region, a.endpoint, a.access_key_id, a.secret_access_key
    ))
    .unwrap();
    if with_note {
        w.execute_batch(
            "CREATE TABLE fixture (id INTEGER, status VARCHAR, note VARCHAR, walrus_pg_sink_meta VARCHAR);",
        )
        .unwrap();
        for (id, status, note, op, l) in rows {
            w.execute(
                "INSERT INTO fixture VALUES (?, ?, ?, ?)",
                duckdb::params![id, status, note, meta(op, "00000000000000C8", *l)],
            )
            .unwrap();
        }
    } else {
        w.execute_batch(
            "CREATE TABLE fixture (id INTEGER, status VARCHAR, walrus_pg_sink_meta VARCHAR);",
        )
        .unwrap();
        for (id, status, _note, op, l) in rows {
            w.execute(
                "INSERT INTO fixture VALUES (?, ?, ?)",
                duckdb::params![id, status, meta(op, "0000000000000064", *l)],
            )
            .unwrap();
        }
    }
    let uri = format!("s3://walrus/{epoch}/public/orders/{tag}-{epoch}.parquet");
    w.execute_batch(&format!("COPY fixture TO '{uri}' (FORMAT PARQUET);"))
        .unwrap();
    uri
}

fn mirror3(ctx: &TableCtx) -> Vec<(i64, String, Option<String>)> {
    let conn = ctx.db.conn();
    let mut stmt = conn
        .prepare("SELECT id, status, note FROM orders ORDER BY id")
        .unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap();
    rows.map(Result::unwrap).collect()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG + MinIO)"]
async fn both_tables_evolve_at_the_correct_lsn_relative_to_data() {
    let _g = LOCK.lock().await;
    let epoch = 3_800_001;
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

    // Register both shapes: v1 (id, status) then v2 (id, status, note).
    for (v, r) in [(1, orders_v1()), (2, orders_v2())] {
        control::upsert_registry(
            &pool,
            &control::RegistryRow {
                epoch,
                source_schema: "public".into(),
                source_table: "orders".into(),
                schema_version: v,
                descriptors: Vec::new(),
                columns: serde_json::to_value(&r).unwrap(),
            },
        )
        .await
        .unwrap();
    }

    // A v1 file (commit 0x64) BEFORE the DDL, then a v2 file (commit 0xC8) AFTER — the ADD COLUMN happens
    // between them. key 1 is v1-only; key 2 is updated in v2; key 3 is inserted in v2.
    let v1 = write_fixture(
        epoch,
        "v1",
        false,
        &[(1, "a1", "", "i", 1), (2, "b1", "", "i", 2)],
    );
    let v2 = write_fixture(
        epoch,
        "v2",
        true,
        &[(2, "b2", "N2", "u", 10), (3, "c2", "N3", "i", 11)],
    );
    for (uri, ver, lsn) in [(v1, 1, "0/64"), (v2, 2, "0/C8")] {
        control::insert_ready(
            &pool,
            &control::NewManifestFile {
                epoch,
                source_schema: "public".into(),
                source_table: "orders".into(),
                s3_uri: uri,
                kind: control::ManifestKind::Stream,
                row_count: 2,
                lsn_start: lsn.parse().unwrap(),
                lsn_end: lsn.parse().unwrap(),
                schema_version: ver,
                reload_id: None,
            },
        )
        .await
        .unwrap();
    }

    // The loader starts at the v1 shape (a fresh .duckdb); Phase A reconciles across the boundary.
    let dir = tmpdir(&epoch.to_string());
    let db = TableDb::open(&dir.join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders_v1(), 1).unwrap();
    db.configure_s3(&s3()).unwrap();
    let ctx = TableCtx {
        pool,
        epoch,
        schema: "public".into(),
        table: "orders".into(),
        rel: orders_v1(),
        db,
        state: LoaderState::new(),
        max_files: 100,
        poll_interval: Duration::from_secs(5),
        compaction_interval: Duration::from_secs(3600),
        retention_lsn_lag: 16 << 20,
        pause_logged: Default::default(),
        resync_ids: Default::default(),
    };

    run_phase_a(&ctx).await.unwrap();
    assert_eq!(
        ctx.db.schema_version().unwrap(),
        2,
        "Phase A reconciled to v2 before appending the v2 file (DDL applied at the boundary)"
    );
    run_phase_b(&ctx).await.unwrap();

    assert_eq!(
        mirror3(&ctx),
        vec![
            (1, "a1".to_string(), None), // v1-only → the added column reads NULL
            (2, "b2".to_string(), Some("N2".to_string())), // v1 then v2 update carries the new column
            (3, "c2".to_string(), Some("N3".to_string())), // v2 insert
        ],
        "both tables evolved at the boundary: old rows NULL, post-DDL rows carry `note`"
    );
    // The user view exposes the new column and still hides the guard columns.
    let view = columns_of(ctx.db.conn(), "orders_current");
    assert!(
        view.iter().any(|c| c == "note") && !view.iter().any(|c| c.starts_with("_applied")),
        "view evolved with the mirror, guard cols still hidden: {view:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
