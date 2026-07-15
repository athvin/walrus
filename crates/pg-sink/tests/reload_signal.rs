//! Compose tests for the reload signal table (PR 6.2, `#[ignore]` — needs the compose source PG).
//!
//!   cargo test -p pg-sink --test reload_signal -- --ignored
//!
//! Three properties, one per test: a signal INSERT is decode-visible through the slot (the echo
//! PR 6.3 will wait on), the backfill's table source never contains the signal table (so no
//! snapshot file can ever exist for it), and preflight refuses a missing/unpublished signal table
//! loudly (reload H11) — or heals it under `manage_publication=true`.

use common::{Lsn, TupleValue};
use pg_sink::config::SinkConfig;
use pg_sink::consume::on_frame;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::preflight::{connect_source, PreflightError, SourcePreflight};
use pg_sink::replication::{ReplicationMessage, ReplicationStream};
use pg_sink::slot::verify_or_create_slot;
use pg_sink::snapshot::published_user_tables;
use std::time::Duration;
use tokio_postgres::NoTls;

const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}

async fn admin() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&source_url(), NoTls)
        .await
        .expect("connect to source");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn drop_slot(client: &tokio_postgres::Client, slot: &str) {
    let _ = client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

/// Drive the stream until a `Commit`, collecting every decoded message (live_decode's idiom).
async fn collect_until_commit(stream: &mut ReplicationStream) -> Vec<Message> {
    let mut ctx = StreamCtx::default();
    let mut msgs = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let frame = stream.next().await.expect("frame").expect("stream open");
            let is_commit = matches!(frame, ReplicationMessage::XLogData { .. });
            if let Some(msg) = on_frame(&mut ctx, frame).expect("decode") {
                let done = matches!(msg, Message::Commit { .. });
                msgs.push(msg);
                if done && is_commit {
                    break;
                }
            }
        }
    })
    .await
    .expect("a Commit within 10s");
    msgs
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn signal_insert_is_visible_in_decoded_stream() {
    let _guard = SOURCE_LOCK.lock().await;
    let slot = "walrus_reload_signal";
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    // Cleanup BEFORE the slot exists so the DELETE's WAL is never streamed (and: future pruning
    // DELETEs on this table flow through the slot too — PR 6.3's routing must ignore them).
    admin
        .execute(
            "DELETE FROM walrus.reload_signal WHERE reload_id = 990001",
            &[],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    // No explicit LSN in the INSERT — the volatile DEFAULT stamps wal_insert_lsn per row.
    admin
        .execute(
            "INSERT INTO walrus.reload_signal (reload_id, chunk_no) VALUES (990001, 1)",
            &[],
        )
        .await
        .unwrap();

    let msgs = collect_until_commit(&mut stream).await;

    // The Relation announces walrus.reload_signal with its 4 columns; find wal_insert_lsn's slot.
    let (rel_oid, lsn_col) = msgs
        .iter()
        .find_map(|m| match m {
            Message::Relation { relation, .. }
                if relation.schema == "walrus" && relation.name == "reload_signal" =>
            {
                Some((
                    relation.oid,
                    relation
                        .columns
                        .iter()
                        .position(|c| c.name == "wal_insert_lsn")
                        .expect("a wal_insert_lsn column"),
                ))
            }
            _ => None,
        })
        .expect("a Relation for walrus.reload_signal");

    let new = msgs
        .iter()
        .find_map(|m| match m {
            Message::Insert {
                relation_oid, new, ..
            } if *relation_oid == rel_oid => Some(new.clone()),
            _ => None,
        })
        .expect("the signal echo Insert");
    assert_eq!(
        new.len(),
        4,
        "reload_id, chunk_no, wal_insert_lsn, inserted_at"
    );
    match &new[lsn_col] {
        TupleValue::Text(s) => {
            s.parse::<Lsn>()
                .unwrap_or_else(|_| panic!("wal_insert_lsn is a pg_lsn, got {s:?}"));
        }
        other => panic!("wal_insert_lsn populated by the DEFAULT, got {other:?}"),
    }

    drop(stream);
    drop_slot(&admin, slot).await;
    admin
        .execute(
            "DELETE FROM walrus.reload_signal WHERE reload_id = 990001",
            &[],
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn backfill_never_copies_walrus_reload_signal() {
    let _guard = SOURCE_LOCK.lock().await;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();

    // Published — the precondition for the echo ever arriving…
    let published: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_publication_tables
             WHERE pubname = 'walrus_pub' AND schemaname = 'walrus'
               AND tablename = 'reload_signal'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(published, 1, "reload_signal is in the publication");

    // …but never a backfill target: `published_user_tables` is the EXACT list the snapshot walks
    // (its SQL excludes schemaname = 'walrus'), so exclusion here IS "no snapshot file, ever" —
    // the same inherited invariant snapshot_backfill.rs asserts for the whole walrus schema.
    let tables = published_user_tables(&admin, "walrus_pub").await.unwrap();
    assert!(
        !tables
            .iter()
            .any(|(s, t)| s == "walrus" && t == "reload_signal"),
        "the signal table must never be snapshotted/backfilled: {tables:?}"
    );
    assert!(
        tables.iter().any(|(s, t)| s == "public" && t == "orders"),
        "sanity: user tables still backfill"
    );
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn missing_signal_table_is_terminal_and_manage_publication_heals_the_gap() {
    let _guard = SOURCE_LOCK.lock().await;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();

    // (a) Table absent → terminal, and the error names the migration to apply.
    admin
        .batch_execute("DROP TABLE IF EXISTS walrus.reload_signal")
        .await
        .unwrap();
    let cfg = SinkConfig {
        source_db_url: source_url(),
        publication_name: "walrus_pub".to_string(),
        ..SinkConfig::default()
    };
    let client = connect_source(&cfg.source_db_url).await.unwrap();
    let pf = SourcePreflight::new(&client, &cfg);
    let err = pf.assert_reload_signal().await.unwrap_err();
    assert!(
        matches!(err, PreflightError::ReloadSignalMissing { .. }),
        "expected ReloadSignalMissing, got {err:?}"
    );
    assert!(err.to_string().contains("0003_reload_signal.sql"));
    assert!(common::Error::from(err).is_terminal());

    // (b) Table restored but missing from a TABLE-LIST publication → PublicationGap that names
    // the exact ALTER PUBLICATION fix (an unpublished signal table never errors at reload time —
    // the echo just silently never arrives, so preflight is the only honest failure mode).
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin
        .batch_execute(
            "DROP PUBLICATION IF EXISTS walrus_pf62;
             CREATE PUBLICATION walrus_pf62 FOR TABLE walrus.heartbeat, walrus.ddl_audit",
        )
        .await
        .unwrap();
    let cfg_gap = SinkConfig {
        source_db_url: source_url(),
        publication_name: "walrus_pf62".to_string(),
        ..SinkConfig::default()
    };
    let pf_gap = SourcePreflight::new(&client, &cfg_gap);
    let err = pf_gap.assert_publication_covers().await.unwrap_err();
    match &err {
        PreflightError::PublicationGap { table, .. } => assert_eq!(table, "reload_signal"),
        other => panic!("expected PublicationGap for reload_signal, got {other:?}"),
    }
    assert!(err
        .to_string()
        .contains("ALTER PUBLICATION walrus_pf62 ADD TABLE walrus.reload_signal"));

    // (c) The same gap under manage_publication=true self-heals via the existing auto-add path.
    let cfg_manage = SinkConfig {
        source_db_url: source_url(),
        publication_name: "walrus_pf62".to_string(),
        manage_publication: true,
        ..SinkConfig::default()
    };
    let pf_manage = SourcePreflight::new(&client, &cfg_manage);
    pf_manage
        .assert_publication_covers()
        .await
        .expect("manage_publication adds the signal table automatically");
    let added: i64 = admin
        .query_one(
            "SELECT count(*) FROM pg_publication_tables
             WHERE pubname = 'walrus_pf62' AND schemaname = 'walrus'
               AND tablename = 'reload_signal'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(added, 1, "auto-added to the table-list publication");

    admin
        .batch_execute("DROP PUBLICATION IF EXISTS walrus_pf62")
        .await
        .unwrap();
}
