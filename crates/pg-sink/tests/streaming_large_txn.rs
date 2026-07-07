//! Large-transaction streaming against compose (`#[ignore]` — needs source PG with
//! `logical_decoding_work_mem=64kB` + MinIO + control PG). An 8000-row txn arrives *before* its commit
//! as interleaved `Stream` blocks: the demux stages speculatively (no manifest), holds
//! `confirmed_flush` at the open txn's begin LSN, and only writes `ready` rows on `Stream Commit`. A
//! whole-txn `Stream Abort` leaves no `ready` row. The demux logic is unit-tested in `src/stream_txn.rs`.
//!
//!   cargo test -p pg-sink --test streaming_large_txn -- --ignored

use common::Lsn;
use pg_sink::batch::{BatchTriggers, SystemClock};
use pg_sink::checkpoint::DurabilityCheckpoint;
use pg_sink::consume::on_frame;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::relcache::RelationCache;
use pg_sink::replication::{ReplicationMessage, ReplicationStream};
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use pg_sink::stream_txn::StreamDemux;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_MIGRATION: &str = include_str!("../../../migrations/source/0001_publication.sql");

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}
fn control_url() -> String {
    std::env::var("WALRUS_CONTROL_DB_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5433/walrus_control".to_string()
    })
}

async fn source() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

fn minio() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(
        object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("walrus")
            .with_region("us-east-1")
            .with_endpoint("http://localhost:9000")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()
            .unwrap(),
    )
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

async fn ready_count(pool: &sqlx::PgPool, epoch: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1 AND status = 'ready'",
    )
    .bind(epoch)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn cleanup(pool: &sqlx::PgPool, admin: &tokio_postgres::Client, epoch: i64, slot: &str) {
    let uris: Vec<String> =
        sqlx::query_scalar("SELECT s3_uri FROM walrus.file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(pool)
            .await
            .unwrap();
    let store = minio();
    for uri in uris {
        if let Some(key) = uri.strip_prefix("s3://walrus/") {
            let _ = store.delete(&object_store::path::Path::from(key)).await;
        }
    }
    let _ = sqlx::query("DELETE FROM walrus.file_manifest WHERE epoch = $1")
        .bind(epoch)
        .execute(pool)
        .await;
    let _ = admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 800000 AND 809000",
            &[],
        )
        .await;
    drop_slot(admin, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn large_txn_single_ready_file_only_after_stream_commit() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_stream";
    let epoch = 2_300_001;
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 800000 AND 809000",
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
    let sink = ParquetSink::new(minio(), "walrus".to_string(), epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let mut demux = StreamDemux::new(
        // Small caps → the open txn spills speculatively (no manifest) many times before commit.
        BatchTriggers {
            max_rows: 500,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        1,
        "test".to_string(),
    );
    let mut checkpoint = DurabilityCheckpoint::new(resume.start_lsn());
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    // 8000 rows in ONE transaction → exceeds logical_decoding_work_mem=64kB → streams before commit.
    admin
        .batch_execute(
            "INSERT INTO public.orders (id, status)
             SELECT g, 'streamed' FROM generate_series(800000, 807999) g",
        )
        .await
        .unwrap();

    let mut streamed_changes = 0u64;
    let mut mid_checked = false;
    let mut commit_lsn: Option<Lsn> = None;
    tokio::time::timeout(Duration::from_secs(45), async {
        while commit_lsn.is_none() {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Relation { relation, .. } => {
                    cache.upsert_from_relation(relation.clone(), 1).unwrap();
                }
                Message::StreamStart { xid, first_segment } => {
                    demux.on_stream_start(*xid, *first_segment, frame_lsn);
                    checkpoint.set_open_txn_floor(demux.open_floor());
                }
                Message::StreamStop => demux.on_stream_stop(),
                m @ (Message::Insert { xid: Some(_), .. }
                | Message::Update { xid: Some(_), .. }
                | Message::Delete { xid: Some(_), .. }) => {
                    demux.on_change(&cache, m, &sink, frame_lsn).await.unwrap();
                    streamed_changes += 1;
                    // Mid-open-window: NOT committed yet — no ready rows, slot NOT advanced.
                    if !mid_checked && streamed_changes >= 1000 {
                        assert_eq!(
                            ready_count(&pool, epoch).await,
                            0,
                            "no ready row while the txn is open"
                        );
                        assert_eq!(
                            checkpoint.confirmed_flush(),
                            resume.start_lsn(),
                            "confirmed_flush held (not advanced) for the whole open window"
                        );
                        assert!(
                            demux.open_floor().is_some(),
                            "the txn is open → a floor exists"
                        );
                        mid_checked = true;
                    }
                }
                Message::StreamCommit {
                    xid,
                    commit_lsn: clsn,
                    ..
                } => {
                    let objs = demux.on_stream_commit(*xid, *clsn, &sink).await.unwrap();
                    for obj in &objs {
                        pg_sink::manifest::record_ready(&pool, epoch, obj)
                            .await
                            .unwrap();
                    }
                    checkpoint.set_open_txn_floor(demux.open_floor());
                    checkpoint.on_batch_durable(*clsn);
                    commit_lsn = Some(*clsn);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("the streamed txn commits within 45s");

    assert!(
        mid_checked,
        "the txn actually streamed (>=1000 changes before commit)"
    );
    let clsn = commit_lsn.unwrap();
    // Only AFTER Stream Commit do ready rows exist — all kind='stream', lsn_end = commit_lsn.
    let files: Vec<(String, String)> =
        sqlx::query_as("SELECT kind, lsn_end::text FROM walrus.file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        !files.is_empty(),
        "Stream Commit promoted the speculative files to ready"
    );
    assert!(
        files.iter().all(|(k, _)| k == "stream"),
        "streamed files are kind='stream'"
    );
    assert!(
        files.iter().all(|(_, e)| e.parse::<Lsn>().unwrap() == clsn),
        "every ready file's lsn_end is the commit LSN"
    );
    assert_eq!(
        checkpoint.confirmed_flush(),
        clsn,
        "confirmed_flush advances on commit"
    );

    cleanup(&pool, &admin, epoch, slot).await;
    drop(stream);
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (logical_decoding_work_mem=64kB)"]
async fn whole_txn_abort_writes_no_ready_row() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_stream_abort";
    let epoch = 2_300_002;
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id BETWEEN 800000 AND 809000",
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
    let sink = ParquetSink::new(minio(), "walrus".to_string(), epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    let mut demux = StreamDemux::new(
        BatchTriggers {
            max_rows: 500,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        1,
        "test".to_string(),
    );
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    // A big txn that ROLLS BACK: a live walsender streams the rows (proto §9a) then Stream Abort.
    admin
        .batch_execute(
            "BEGIN;
             INSERT INTO public.orders (id, status)
             SELECT g, 'aborted' FROM generate_series(800000, 807999) g;
             ROLLBACK;",
        )
        .await
        .unwrap();
    // A trailing committed change gives the loop a definite stop point after the abort.
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (809000, 'after')",
            &[],
        )
        .await
        .unwrap();

    let mut saw_abort = false;
    let mut done = false;
    tokio::time::timeout(Duration::from_secs(45), async {
        while !done {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Relation { relation, .. } => {
                    cache.upsert_from_relation(relation.clone(), 1).unwrap();
                }
                Message::StreamStart { xid, first_segment } => {
                    demux.on_stream_start(*xid, *first_segment, frame_lsn);
                }
                Message::StreamStop => demux.on_stream_stop(),
                m @ (Message::Insert { xid: Some(_), .. }
                | Message::Update { xid: Some(_), .. }
                | Message::Delete { xid: Some(_), .. }) => {
                    demux.on_change(&cache, m, &sink, frame_lsn).await.unwrap();
                }
                Message::StreamAbort { top_xid, sub_xid } => {
                    demux
                        .on_stream_abort(*top_xid, *sub_xid, &sink)
                        .await
                        .unwrap();
                    saw_abort = true;
                }
                // The trailing small (non-streamed) commit ends the loop.
                Message::Commit { .. } if saw_abort => done = true,
                _ => {}
            }
        }
    })
    .await
    .expect("the aborted txn streams + aborts within 45s");

    assert!(saw_abort, "a whole-txn Stream Abort was decoded");
    assert_eq!(
        ready_count(&pool, epoch).await,
        0,
        "an aborted streamed txn writes NO ready row"
    );

    cleanup(&pool, &admin, epoch, slot).await;
    drop(stream);
}
