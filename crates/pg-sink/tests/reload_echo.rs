//! Echo routing against compose (`#[ignore]` — needs source PG + control PG). A manual
//! `walrus.reload_signal` INSERT comes back through the slot, resolves a subscribed waiter with
//! the transaction's COMMIT LSN (the chunk watermark `L_i`), passes the embedded-LSN cross-check,
//! and never touches a batcher / Parquet file / manifest row. The registry/buffer semantics are
//! unit-tested in `src/reload_signal.rs`; this drives the REAL seams (`on_frame`,
//! `InternalTables`, `PendingSignals`, `BatchRouter`) against live WAL.
//!
//!   cargo test -p pg-sink --test reload_echo -- --ignored

use common::Lsn;
use pg_sink::batch::{BatchTriggers, SystemClock};
use pg_sink::consume::{on_frame, BatchRouter};
use pg_sink::heartbeat::InternalTables;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::relcache::RelationCache;
use pg_sink::reload_signal::{PendingSignal, PendingSignals, WatermarkWaiters};
use pg_sink::replication::{ReplicationMessage, ReplicationStream};
use pg_sink::slot::verify_or_create_slot;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn admin() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

async fn drop_slot(admin: &tokio_postgres::Client, slot: &str) {
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG)"]
async fn signal_insert_resolves_waiter_and_never_reaches_parquet() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_reload_echo";
    let reload_id = 990_042i64;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    // Cleanup BEFORE the slot exists so the DELETE's WAL never streams.
    admin
        .execute(
            "DELETE FROM walrus.reload_signal WHERE reload_id = $1",
            &[&reload_id],
        )
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    // The real consume-loop pieces this PR wires together.
    let waiters = Arc::new(WatermarkWaiters::default());
    let mut pending = PendingSignals::default();
    let mut internal = InternalTables::default();
    let cache = RelationCache::default();
    let mut router = BatchRouter::new(
        BatchTriggers {
            max_rows: 1, // seal-happy: ANY routed row would surface as a sealed batch instantly
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        990_042,
        "test".to_string(),
    );

    // Subscribe-then-insert: the registry holds the sender BEFORE the row exists.
    let rx = waiters.subscribe(reload_id, 1);
    admin
        .execute(
            "INSERT INTO walrus.reload_signal (reload_id, chunk_no) VALUES ($1, 1)",
            &[&reload_id],
        )
        .await
        .unwrap();

    // Mini consume loop mirroring src/consume.rs's routing arms for the messages involved.
    let mut ctx = StreamCtx::default();
    let mut sealed_count = 0usize;
    let commit_lsn = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Relation { relation, .. } => internal.note_relation(relation),
                Message::Insert {
                    relation_oid,
                    new,
                    xid,
                } if internal.is_reload_signal(*relation_oid) => {
                    let rel = internal.reload_signal_rel().expect("noted relation");
                    pending.push(PendingSignal::from_tuple(rel, new, *xid).expect("parses"));
                }
                Message::Commit { commit_lsn, .. } => {
                    sealed_count += router.route(&cache, &msg, frame_lsn, 1).unwrap().len();
                    pending.on_commit(*commit_lsn, &waiters);
                    return *commit_lsn;
                }
                other => {
                    sealed_count += router.route(&cache, other, frame_lsn, 1).unwrap().len();
                }
            }
        }
    })
    .await
    .expect("the signal txn commits within 15s");

    // The waiter resolved with the transaction's COMMIT LSN — and the cross-check held.
    let echo = rx.await.expect("waiter resolved at Commit");
    assert_eq!(echo.commit_lsn, commit_lsn, "the stamp is the commit LSN");
    assert!(
        echo.embedded_lsn < echo.commit_lsn,
        "embedded wal_insert_lsn ({}) must precede the commit LSN ({})",
        echo.embedded_lsn,
        echo.commit_lsn
    );
    assert_eq!(waiters.crosscheck_violations(), 0);

    // Never batched: the seal-happy router (max_rows=1) sealed nothing, and no manifest row for
    // the signal table exists anywhere in the control DB.
    assert_eq!(sealed_count, 0, "a signal echo must never reach a batcher");
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let staged: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM walrus.file_manifest WHERE source_table = 'reload_signal'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        staged, 0,
        "no Parquet/manifest row for the signal table, ever"
    );

    drop(stream);
    drop_slot(&admin, slot).await;
    admin
        .execute(
            "DELETE FROM walrus.reload_signal WHERE reload_id = $1",
            &[&reload_id],
        )
        .await
        .unwrap();
}
