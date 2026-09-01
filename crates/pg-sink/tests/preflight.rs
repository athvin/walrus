#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Source-side preflight against the compose stack (`#[ignore]` — needs source + control PG).
//!
//! After `docker compose up --wait`:
//!   cargo test -p pg-sink --test preflight -- --ignored
//!
//! The tests mutate shared source-pg state (a keyless table, the walrus schema), so they hold a
//! process-wide async lock to run serially; each sets up its own preconditions under that lock.

use common::FailureClass;
use pg_sink::config::SinkConfig;
use pg_sink::preflight::{PkMode, PreflightError, SourcePreflight, connect_source};
use tokio_postgres::NoTls;

const SOURCE_MIGRATION: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_DDL_MIGRATION: &str = include_str!("../../../migrations/source/0002_ddl_triggers.sql");
const SOURCE_MIGRATION_0003: &str =
    include_str!("../../../migrations/source/0003_reload_signal.sql");

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}

fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

fn cfg_for(url: &str) -> SinkConfig {
    SinkConfig {
        source_db_url: url.into(),
        publication_name: "walrus_pub".to_string(),
        ..SinkConfig::default()
    }
}

/// A plain (non-replication) connection for setup DDL.
async fn plain(url: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("plain connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn good_source_passes_all_assertions() {
    let _guard = SOURCE_LOCK.lock().await;
    let setup = plain(&source_url()).await;
    setup.batch_execute(SOURCE_MIGRATION).await.unwrap(); // idempotent: ensure walrus tables
    setup.batch_execute(SOURCE_MIGRATION_0003).await.unwrap(); // …incl. the reload signal
    setup
        .batch_execute("DROP TABLE IF EXISTS public._walrus_pf_keyless") // defensive cleanup
        .await
        .unwrap();

    let cfg = cfg_for(&source_url());
    let client = connect_source(cfg.source_db_url.expose())
        .await
        .expect("replication connect to source-pg");
    let pf = SourcePreflight::new(&client, &cfg);

    let info = pf.assert_server_prereqs().await.expect("server prereqs");
    assert_eq!(info.wal_level, "logical");
    assert!(info.version_num >= 140_000, "PG {info:?} must be ≥14");

    pf.assert_reload_signal()
        .await
        .expect("reload signal table installed with its PK");

    pf.assert_publication_covers()
        .await
        .expect("publication covers ddl_audit + heartbeat + reload_signal");

    let report = pf
        .assert_tables_have_pk(PkMode::Strict)
        .await
        .expect("every published user table is keyed");
    assert!(report.quarantined.is_empty(), "no table should be keyless");
    assert!(
        !report.ok.is_empty(),
        "orders/customers/items are published"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (control PG runs wal_level=replica)"]
async fn wrong_wal_level_is_terminal() {
    let _guard = SOURCE_LOCK.lock().await;
    // control-pg runs with the default wal_level = replica, so the assertion is terminal.
    let cfg = cfg_for(&control_url());
    let client = connect_source(cfg.source_db_url.expose())
        .await
        .expect("replication connect to control-pg");
    let pf = SourcePreflight::new(&client, &cfg);

    let err = pf.assert_server_prereqs().await.unwrap_err();
    assert!(
        matches!(err, PreflightError::WalLevel { .. }),
        "expected WalLevel, got {err:?}"
    );
    let mapped = common::Error::from(err);
    assert!(mapped.is_terminal());
    assert_eq!(mapped.exit_code(), common::ExitCode::Preflight);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn keyless_table_is_terminal_in_strict_and_quarantined_in_lenient() {
    let _guard = SOURCE_LOCK.lock().await;
    let setup = plain(&source_url()).await;
    setup.batch_execute(SOURCE_MIGRATION).await.unwrap();
    // A published user table with no PK + REPLICA IDENTITY DEFAULT ('d') → keyless.
    setup
        .batch_execute(
            "DROP TABLE IF EXISTS public._walrus_pf_keyless; \
             CREATE TABLE public._walrus_pf_keyless (x int)",
        )
        .await
        .unwrap();

    let cfg = cfg_for(&source_url());
    let client = connect_source(cfg.source_db_url.expose()).await.unwrap();
    let pf = SourcePreflight::new(&client, &cfg);

    // Strict → terminal on the offender.
    let err = pf.assert_tables_have_pk(PkMode::Strict).await.unwrap_err();
    match &err {
        PreflightError::NoPrimaryKey { table, .. } => assert_eq!(table, "_walrus_pf_keyless"),
        other => panic!("expected NoPrimaryKey, got {other:?}"),
    }
    assert_eq!(
        common::Error::from(err).exit_code(),
        common::ExitCode::KeylessTable
    );

    // Lenient → quarantine + continue.
    let report = pf.assert_tables_have_pk(PkMode::Lenient).await.unwrap();
    assert!(
        report
            .quarantined
            .iter()
            .any(|t| t.table == "_walrus_pf_keyless"),
        "keyless table must be quarantined in lenient mode: {report:?}"
    );

    setup
        .batch_execute("DROP TABLE IF EXISTS public._walrus_pf_keyless")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn publication_missing_heartbeat_is_terminal() {
    let _guard = SOURCE_LOCK.lock().await;
    let setup = plain(&source_url()).await;
    // Remove the walrus internal tables so the FOR-ALL-TABLES publication no longer covers them.
    setup
        .batch_execute("DROP TABLE IF EXISTS walrus.heartbeat, walrus.ddl_audit")
        .await
        .unwrap();

    let cfg = cfg_for(&source_url());
    let client = connect_source(cfg.source_db_url.expose()).await.unwrap();
    let pf = SourcePreflight::new(&client, &cfg);

    let err = pf.assert_publication_covers().await.unwrap_err();
    match &err {
        PreflightError::PublicationGap { table, .. } => {
            assert!(table == "heartbeat" || table == "ddl_audit", "got {table}");
        }
        other => panic!("expected PublicationGap, got {other:?}"),
    }
    assert!(common::Error::from(err).is_terminal());

    // Restore the internal tables and the audit table's full shape for the other tests
    // (idempotent). The event triggers survive dropping the tables, so restoring only the
    // 0001 stub leaves them targeting columns that no longer exist.
    setup.batch_execute(SOURCE_MIGRATION).await.unwrap();
    setup.batch_execute(SOURCE_DDL_MIGRATION).await.unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn ddl_event_bindings_share_one_function_and_capture_commit_safe_snapshots() {
    let _guard = SOURCE_LOCK.lock().await;
    let setup = plain(&source_url()).await;
    setup.batch_execute(SOURCE_MIGRATION).await.unwrap();
    setup.batch_execute(SOURCE_DDL_MIGRATION).await.unwrap();
    setup
        .batch_execute("DROP TABLE IF EXISTS public._walrus_ddl_trigger_test")
        .await
        .unwrap();

    let bindings = setup
        .query(
            "SELECT evtname, evtevent, evtfoid::regprocedure::text \
             FROM pg_event_trigger \
             WHERE evtname IN ('walrus_intercept_ddl', 'walrus_intercept_drop') \
             ORDER BY evtname",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].get::<_, &str>(1), "ddl_command_end");
    assert_eq!(bindings[1].get::<_, &str>(1), "sql_drop");
    assert_eq!(bindings[0].get::<_, &str>(2), "walrus.intercept_ddl()");
    assert_eq!(bindings[1].get::<_, &str>(2), "walrus.intercept_ddl()");

    let before: i64 = setup
        .query_one("SELECT COALESCE(max(id), 0) FROM walrus.ddl_audit", &[])
        .await
        .unwrap()
        .get(0);
    setup
        .execute(
            "CREATE TABLE public._walrus_ddl_trigger_test (id int PRIMARY KEY, note text)",
            &[],
        )
        .await
        .unwrap();
    let created = setup
        .query_one(
            "SELECT c_event, c_tag, c_rel_oid::text, c_replica_identity, c_columns::text, c_ddl_text \
             FROM walrus.ddl_audit WHERE id > $1 AND c_table = '_walrus_ddl_trigger_test'",
            &[&before],
        )
        .await
        .unwrap();
    assert_eq!(created.get::<_, &str>(0), "ddl_command_end");
    assert_eq!(created.get::<_, &str>(1), "CREATE TABLE");
    assert!(!created.get::<_, &str>(2).is_empty());
    assert_eq!(created.get::<_, &str>(3), "d");
    let columns: serde_json::Value = serde_json::from_str(created.get::<_, &str>(4)).unwrap();
    assert_eq!(columns.as_array().unwrap().len(), 2);
    assert_eq!(columns[0]["name"], "id");
    assert_eq!(columns[0]["is_key"], true);
    assert!(
        created
            .get::<_, &str>(5)
            .contains("CREATE TABLE public._walrus_ddl_trigger_test")
    );

    let before_drop_column: i64 = setup
        .query_one("SELECT COALESCE(max(id), 0) FROM walrus.ddl_audit", &[])
        .await
        .unwrap()
        .get(0);
    setup
        .execute(
            "ALTER TABLE public._walrus_ddl_trigger_test DROP COLUMN note",
            &[],
        )
        .await
        .unwrap();
    let drop_column_rows = setup
        .query(
            "SELECT c_event, c_tag, c_columns::text FROM walrus.ddl_audit \
             WHERE id > $1 AND c_table = '_walrus_ddl_trigger_test' ORDER BY id",
            &[&before_drop_column],
        )
        .await
        .unwrap();
    assert_eq!(
        drop_column_rows.len(),
        1,
        "DROP COLUMN must not be duplicated by sql_drop + ddl_command_end"
    );
    assert_eq!(drop_column_rows[0].get::<_, &str>(0), "ddl_command_end");
    assert_eq!(drop_column_rows[0].get::<_, &str>(1), "ALTER TABLE");
    let columns: serde_json::Value =
        serde_json::from_str(drop_column_rows[0].get::<_, &str>(2)).unwrap();
    assert_eq!(columns.as_array().unwrap().len(), 1);

    let before_rollback: i64 = setup
        .query_one("SELECT COALESCE(max(id), 0) FROM walrus.ddl_audit", &[])
        .await
        .unwrap()
        .get(0);
    setup
        .batch_execute(
            "BEGIN; \
             ALTER TABLE public._walrus_ddl_trigger_test ADD COLUMN rolled_back text; \
             ROLLBACK",
        )
        .await
        .unwrap();
    let after_rollback: i64 = setup
        .query_one("SELECT COALESCE(max(id), 0) FROM walrus.ddl_audit", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        after_rollback, before_rollback,
        "audit INSERT rolls back with DDL"
    );
    let rolled_back_column_exists: bool = setup
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = '_walrus_ddl_trigger_test' \
               AND column_name = 'rolled_back')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!rolled_back_column_exists);

    let before_drop_table: i64 = setup
        .query_one("SELECT COALESCE(max(id), 0) FROM walrus.ddl_audit", &[])
        .await
        .unwrap()
        .get(0);
    setup
        .execute("DROP TABLE public._walrus_ddl_trigger_test", &[])
        .await
        .unwrap();
    let dropped = setup
        .query_one(
            "SELECT c_event, c_tag, c_columns::text, c_dropped::text, c_ddl_text \
             FROM walrus.ddl_audit WHERE id > $1 AND c_table = '_walrus_ddl_trigger_test'",
            &[&before_drop_table],
        )
        .await
        .unwrap();
    assert_eq!(dropped.get::<_, &str>(0), "sql_drop");
    assert_eq!(dropped.get::<_, &str>(1), "DROP TABLE");
    assert_eq!(dropped.get::<_, &str>(2), "[]");
    assert!(dropped.get::<_, &str>(3).contains("table"));
    assert!(
        dropped
            .get::<_, &str>(4)
            .contains("DROP TABLE public._walrus_ddl_trigger_test")
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn concurrent_table_ddl_waits_for_an_inflight_writer_and_captures_only_after_success() {
    let _guard = SOURCE_LOCK.lock().await;
    let writer = plain(&source_url()).await;
    let ddl = plain(&source_url()).await;
    writer.batch_execute(SOURCE_MIGRATION).await.unwrap();
    writer.batch_execute(SOURCE_DDL_MIGRATION).await.unwrap();
    writer
        .batch_execute(
            "DROP TABLE IF EXISTS public._walrus_ddl_lock_test; \
             CREATE TABLE public._walrus_ddl_lock_test (id int PRIMARY KEY)",
        )
        .await
        .unwrap();
    let audit_floor: i64 = writer
        .query_one("SELECT COALESCE(max(id), 0) FROM walrus.ddl_audit", &[])
        .await
        .unwrap()
        .get(0);

    writer
        .batch_execute("BEGIN; INSERT INTO public._walrus_ddl_lock_test (id) VALUES (1)")
        .await
        .unwrap();
    ddl.batch_execute("SET lock_timeout = '250ms'")
        .await
        .unwrap();
    let blocked = ddl
        .execute(
            "ALTER TABLE public._walrus_ddl_lock_test ADD COLUMN extra text",
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(
        blocked.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
        "ALTER must wait behind the writer's RowExclusive lock"
    );
    let premature_audits: i64 = writer
        .query_one(
            "SELECT count(*) FROM walrus.ddl_audit \
             WHERE id > $1 AND c_table = '_walrus_ddl_lock_test' AND c_tag = 'ALTER TABLE'",
            &[&audit_floor],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(premature_audits, 0, "a timed-out DDL has no audit event");

    writer.batch_execute("COMMIT").await.unwrap();
    ddl.batch_execute("SET lock_timeout = 0").await.unwrap();
    ddl.execute(
        "ALTER TABLE public._walrus_ddl_lock_test ADD COLUMN extra text",
        &[],
    )
    .await
    .unwrap();
    let committed_audits: i64 = writer
        .query_one(
            "SELECT count(*) FROM walrus.ddl_audit \
             WHERE id > $1 AND c_table = '_walrus_ddl_lock_test' AND c_tag = 'ALTER TABLE'",
            &[&audit_floor],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(committed_audits, 1);

    writer
        .batch_execute("DROP TABLE public._walrus_ddl_lock_test")
        .await
        .unwrap();
}
