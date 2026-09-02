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
const SOURCE_MIGRATION_0004: &str =
    include_str!("../../../migrations/source/0004_reload_event.sql");

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
    setup.batch_execute(SOURCE_DDL_MIGRATION).await.unwrap(); // …DDL tap + publication guard
    setup.batch_execute(SOURCE_MIGRATION_0003).await.unwrap(); // …incl. the reload signal
    setup.batch_execute(SOURCE_MIGRATION_0004).await.unwrap(); // …and the reload event log
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
    pf.assert_reload_event()
        .await
        .expect("reload event table installed with its PK");
    pf.assert_ddl_capture()
        .await
        .expect("DDL tap and publication/schema guards installed with complete tag coverage");

    pf.assert_publication_covers()
        .await
        .expect("publication covers ddl_audit + heartbeat + reload internals");

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
#[ignore = "requires docker compose up --wait (source PG)"]
async fn reload_events_are_request_namespaced_and_append_only() {
    let _guard = SOURCE_LOCK.lock().await;
    let setup = plain(&source_url()).await;
    setup.batch_execute(SOURCE_MIGRATION).await.unwrap();
    setup.batch_execute(SOURCE_MIGRATION_0004).await.unwrap();

    let first_event = uuid::Uuid::new_v4();
    let second_event = uuid::Uuid::new_v4();
    let first_request = uuid::Uuid::new_v4();
    let second_request = uuid::Uuid::new_v4();
    let reused_reload_id = 8_880_004_i64;
    for (event_id, request_id) in [(first_event, first_request), (second_event, second_request)] {
        let event_id = event_id.to_string();
        let request_id = request_id.to_string();
        setup
            .execute(
                "INSERT INTO walrus.reload_event
                   (event_id, request_id, reload_id, event_kind, scope,
                    source_schema, source_table, schema_version)
                 VALUES ($1::text::uuid, $2::text::uuid, $3, 'start_fence', 'table', 'public', 'orders', 1)",
                &[&event_id, &request_id, &reused_reload_id],
            )
            .await
            .unwrap();
    }

    let update = setup
        .execute(
            "UPDATE walrus.reload_event SET source_table = 'changed' WHERE event_id = $1::text::uuid",
            &[&first_event.to_string()],
        )
        .await
        .unwrap_err();
    assert_eq!(
        update.code(),
        Some(&tokio_postgres::error::SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    let delete = setup
        .execute(
            "DELETE FROM walrus.reload_event WHERE event_id = $1::text::uuid",
            &[&first_event.to_string()],
        )
        .await
        .unwrap_err();
    assert_eq!(
        delete.code(),
        Some(&tokio_postgres::error::SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
    );
    let truncate = setup
        .batch_execute("TRUNCATE walrus.reload_event")
        .await
        .unwrap_err();
    assert_eq!(
        truncate.code(),
        Some(&tokio_postgres::error::SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
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
async fn publication_must_emit_every_action_and_unrestricted_rows() {
    let _guard = SOURCE_LOCK.lock().await;
    let url = source_url();
    let setup = plain(&url).await;
    setup.batch_execute(SOURCE_MIGRATION).await.unwrap();
    setup.batch_execute(SOURCE_DDL_MIGRATION).await.unwrap();
    setup.batch_execute(SOURCE_MIGRATION_0003).await.unwrap();
    setup.batch_execute(SOURCE_MIGRATION_0004).await.unwrap();
    setup
        .batch_execute(
            "DROP PUBLICATION IF EXISTS walrus_pf_coverage;
             DROP TABLE IF EXISTS public._walrus_pf_coverage;
             CREATE TABLE public._walrus_pf_coverage (id int PRIMARY KEY, payload text);",
        )
        .await
        .unwrap();

    let required = "walrus.heartbeat, walrus.ddl_audit, walrus.reload_signal, walrus.reload_event";
    setup
        .batch_execute(&format!(
            "CREATE PUBLICATION walrus_pf_coverage
             FOR TABLE {required}, public._walrus_pf_coverage
             WITH (publish = 'insert, update, truncate')"
        ))
        .await
        .unwrap();
    let mut cfg = cfg_for(&url);
    cfg.publication_name = "walrus_pf_coverage".to_string();
    let client = connect_source(cfg.source_db_url.expose()).await.unwrap();
    let pf = SourcePreflight::new(&client, &cfg);
    let err = pf.assert_publication_covers().await.unwrap_err();
    assert!(
        matches!(
            &err,
            PreflightError::PublicationCoverage(
                pg_sink::source_catalog::PublicationCoverageIssue::DisabledOperations { .. }
            )
        ),
        "disabled DELETE must be terminal, got {err:?}"
    );

    setup
        .batch_execute(&format!(
            "DROP PUBLICATION walrus_pf_coverage;
             CREATE PUBLICATION walrus_pf_coverage
             FOR TABLE {required}, public._walrus_pf_coverage WHERE (id > 0)"
        ))
        .await
        .unwrap();
    let err = pf.assert_publication_covers().await.unwrap_err();
    assert!(
        matches!(
            &err,
            PreflightError::PublicationCoverage(
                pg_sink::source_catalog::PublicationCoverageIssue::RowFilter { .. }
            )
        ),
        "a row filter must be terminal, got {err:?}"
    );

    setup
        .batch_execute(&format!(
            "DROP PUBLICATION walrus_pf_coverage;
             CREATE PUBLICATION walrus_pf_coverage
             FOR TABLE {required}, public._walrus_pf_coverage (id, payload)"
        ))
        .await
        .unwrap();
    let err = pf.assert_publication_covers().await.unwrap_err();
    assert!(
        matches!(
            &err,
            PreflightError::PublicationCoverage(
                pg_sink::source_catalog::PublicationCoverageIssue::ColumnList { .. }
            )
        ),
        "even an explicit list of every current column must be terminal, got {err:?}"
    );

    setup
        .batch_execute(
            "DROP PUBLICATION walrus_pf_coverage;
             DROP TABLE public._walrus_pf_coverage;
             CREATE TABLE public._walrus_pf_coverage (id int PRIMARY KEY, payload text)
               PARTITION BY RANGE (id);
             CREATE TABLE public._walrus_pf_coverage_leaf
               PARTITION OF public._walrus_pf_coverage FOR VALUES FROM (0) TO (100);",
        )
        .await
        .unwrap();
    setup
        .batch_execute(&format!(
            "CREATE PUBLICATION walrus_pf_coverage
             FOR TABLE {required}, public._walrus_pf_coverage"
        ))
        .await
        .unwrap();
    let err = pf.assert_publication_covers().await.unwrap_err();
    assert!(
        matches!(
            &err,
            PreflightError::PublicationCoverage(
                pg_sink::source_catalog::PublicationCoverageIssue::TopologyDependent { .. }
            )
        ),
        "partition-derived membership must be rejected, got {err:?}"
    );

    setup
        .batch_execute(
            "DROP PUBLICATION walrus_pf_coverage;
             DROP TABLE public._walrus_pf_coverage CASCADE;",
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn publication_ddl_is_rejected_by_the_shared_pipeline_guard() {
    let _guard = SOURCE_LOCK.lock().await;
    let url = source_url();
    let setup = plain(&url).await;
    setup.batch_execute(SOURCE_DDL_MIGRATION).await.unwrap();
    setup
        .batch_execute(
            "DROP TABLE IF EXISTS public._walrus_pf_guard_create;
             DROP TABLE IF EXISTS public._walrus_pf_guard_root CASCADE;
             DROP TABLE IF EXISTS public._walrus_pf_guard_leaf;
             DROP SCHEMA IF EXISTS _walrus_pf_guard_schema CASCADE;
             DROP SCHEMA IF EXISTS _walrus_pf_guard_schema_v2 CASCADE;
             CREATE TABLE public._walrus_pf_guard_root (id int NOT NULL)
               PARTITION BY RANGE (id);
             CREATE TABLE public._walrus_pf_guard_leaf (id int NOT NULL);
             CREATE SCHEMA _walrus_pf_guard_schema;
             CREATE TABLE _walrus_pf_guard_schema.tracked (id int PRIMARY KEY);",
        )
        .await
        .unwrap();

    let holder = plain(&url).await;
    assert!(
        pg_sink::source_catalog::try_acquire_publication_ddl_guard(&holder)
            .await
            .unwrap(),
        "shared guard should be immediately available"
    );

    let ddl = plain(&url).await;
    ddl.batch_execute("SET statement_timeout = '2s'")
        .await
        .unwrap();
    let err = ddl
        .batch_execute(
            "ALTER PUBLICATION walrus_pub
             SET (publish = 'insert, update, delete, truncate')",
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
        "command-start trigger must reject rather than wait behind the shared advisory lock: {err}"
    );
    assert!(
        err.as_db_error()
            .is_some_and(|db| db.message().contains("publication DDL rejected")),
        "guard rejection should explain the operational remedy: {err}"
    );

    let err = ddl
        .batch_execute("CREATE TABLE public._walrus_pf_guard_create (id int PRIMARY KEY)")
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
        "CREATE TABLE must not add an un-baselined FOR ALL target while online: {err}"
    );
    let absent: bool = setup
        .query_one(
            "SELECT to_regclass('public._walrus_pf_guard_create') IS NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        absent,
        "rejected CREATE TABLE must leave no relation behind"
    );

    ddl.batch_execute("CREATE TEMP TABLE _walrus_pf_guard_temp (id int PRIMARY KEY)")
        .await
        .expect("an unpublished temporary table must not contend on the coverage guard");
    let temp_exists: bool = ddl
        .query_one(
            "SELECT to_regclass('pg_temp._walrus_pf_guard_temp') IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(temp_exists, "the allowed temporary CREATE must commit");

    let err = ddl
        .batch_execute(
            "ALTER TABLE public._walrus_pf_guard_root
             ATTACH PARTITION public._walrus_pf_guard_leaf FOR VALUES FROM (0) TO (100)",
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
        "partition topology must remain fixed while the coverage guard is held: {err}"
    );

    ddl.batch_execute("ALTER SCHEMA _walrus_pf_guard_schema OWNER TO CURRENT_USER")
        .await
        .expect("a metadata-only schema owner change must remain allowed online");
    let err = ddl
        .batch_execute("ALTER SCHEMA _walrus_pf_guard_schema RENAME TO _walrus_pf_guard_schema_v2")
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
        "renaming a schema with published tables must fail while the guard is held: {err}"
    );
    let schema_rename_rolled_back: bool = setup
        .query_one(
            "SELECT to_regnamespace('_walrus_pf_guard_schema') IS NOT NULL
                    AND to_regnamespace('_walrus_pf_guard_schema_v2') IS NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        schema_rename_rolled_back,
        "the rejected schema rename must roll back atomically"
    );

    drop(holder);
    setup
        .batch_execute(
            "DROP TABLE public._walrus_pf_guard_root CASCADE;
             DROP TABLE public._walrus_pf_guard_leaf;
             DROP SCHEMA _walrus_pf_guard_schema CASCADE;",
        )
        .await
        .unwrap();
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
            "CREATE TABLE public._walrus_ddl_trigger_test (
               id int NOT NULL,
               note text,
               included text,
               id_twice int GENERATED ALWAYS AS (id * 2) STORED,
               PRIMARY KEY (id) INCLUDE (included)
             )",
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
    assert_eq!(columns.as_array().unwrap().len(), 3);
    assert_eq!(columns[0]["name"], "id");
    assert_eq!(columns[0]["is_key"], true);
    assert!(
        columns
            .as_array()
            .unwrap()
            .iter()
            .all(|column| column["name"] != "id_twice"),
        "the DDL snapshot must match pgoutput and omit generated columns"
    );
    assert_eq!(
        columns[2]["is_key"], false,
        "an INCLUDE attribute is payload, not part of the replica identity"
    );
    let described = pg_sink::source_catalog::describe_source_relation(
        &setup,
        "public",
        "_walrus_ddl_trigger_test",
    )
    .await
    .unwrap();
    assert_eq!(
        described.columns.len(),
        3,
        "the live export shape must omit the physical generated attribute"
    );
    assert!(
        described
            .columns
            .iter()
            .all(|column| column.name != "id_twice"),
        "a generated column cannot appear in the catalog-derived export shape"
    );
    assert!(
        described
            .columns
            .iter()
            .find(|c| c.name == "id")
            .unwrap()
            .is_key
    );
    assert!(
        !described
            .columns
            .iter()
            .find(|c| c.name == "included")
            .unwrap()
            .is_key,
        "the live source-catalog shape must exclude INCLUDE attributes too"
    );
    let audit_shape = pg_sink::ddl::DdlEvent {
        source_audit_id: 0,
        capture_lsn: common::Lsn::ZERO,
        c_event: "ddl_command_end".to_string(),
        c_tag: "CREATE TABLE".to_string(),
        source_schema: "public".to_string(),
        source_table: "_walrus_ddl_trigger_test".to_string(),
        c_rel_oid: Some(created.get::<_, &str>(2).parse().unwrap()),
        c_replica_identity: Some(created.get::<_, &str>(3).parse().unwrap()),
        c_columns: Some(columns),
        c_dropped: None,
        c_ddl_text: None,
    }
    .relation_after(None)
    .unwrap()
    .unwrap();
    assert_eq!(
        audit_shape, described,
        "DDL registry and catalog export shapes must agree at one schema version"
    );
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
    assert_eq!(columns.as_array().unwrap().len(), 2);

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
