//! Source-side preflight against the compose stack (`#[ignore]` — needs source + control PG).
//!
//! After `docker compose up --wait`:
//!   cargo test -p pg-sink --test preflight -- --ignored
//!
//! The tests mutate shared source-pg state (a keyless table, the walrus schema), so they hold a
//! process-wide async lock to run serially; each sets up its own preconditions under that lock.

use pg_sink::config::SinkConfig;
use pg_sink::preflight::{connect_source, PkMode, PreflightError, SourcePreflight};
use tokio_postgres::NoTls;

const SOURCE_MIGRATION: &str = include_str!("../../../migrations/source/0001_publication.sql");

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
        source_db_url: url.to_string(),
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
    setup
        .batch_execute("DROP TABLE IF EXISTS public._walrus_pf_keyless") // defensive cleanup
        .await
        .unwrap();

    let cfg = cfg_for(&source_url());
    let client = connect_source(&cfg.source_db_url)
        .await
        .expect("replication connect to source-pg");
    let pf = SourcePreflight::new(&client, &cfg);

    let info = pf.assert_server_prereqs().await.expect("server prereqs");
    assert_eq!(info.wal_level, "logical");
    assert!(info.version_num >= 140_000, "PG {info:?} must be ≥14");

    pf.assert_publication_covers()
        .await
        .expect("publication covers ddl_audit + heartbeat");

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
    let client = connect_source(&cfg.source_db_url)
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
    let client = connect_source(&cfg.source_db_url).await.unwrap();
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
    let client = connect_source(&cfg.source_db_url).await.unwrap();
    let pf = SourcePreflight::new(&client, &cfg);

    let err = pf.assert_publication_covers().await.unwrap_err();
    match &err {
        PreflightError::PublicationGap { table, .. } => {
            assert!(table == "heartbeat" || table == "ddl_audit", "got {table}");
        }
        other => panic!("expected PublicationGap, got {other:?}"),
    }
    assert!(common::Error::from(err).is_terminal());

    // Restore the internal tables for the other tests (idempotent).
    setup.batch_execute(SOURCE_MIGRATION).await.unwrap();
}
