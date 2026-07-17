#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! The durability checkpoint against compose (`#[ignore]` — needs source PG + MinIO + control PG). The
//! slot's `confirmed_flush_lsn` reaches a batch's `lsn_end` only *after* the S3 PUT + manifest commit;
//! and a crash between the PUT and the standby update re-streams the batch (at-least-once, no loss).
//! The two-distinct-LSN rule is unit-tested in `src/checkpoint.rs`.
//!
//!   cargo test -p pg-sink --test durability -- --ignored

use common::Lsn;
use pg_sink::batch::{BatchTriggers, SystemClock};
use pg_sink::checkpoint::DurabilityCheckpoint;
use pg_sink::consume::{flush_batch, on_frame, BatchRouter};
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::relcache::RelationCache;
use pg_sink::replication::{ReplicationMessage, ReplicationStream};
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use std::sync::Arc;
use std::time::Duration;
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

async fn source() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&source_url(), NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    c
}

async fn control_pool() -> sqlx::postgres::PgPool {
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    pool
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

async fn confirmed_flush(admin: &tokio_postgres::Client, slot: &str) -> Lsn {
    let row = admin
        .query_one(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap();
    let s: Option<String> = row.get(0);
    s.map(|x| x.parse().unwrap()).unwrap_or(Lsn::ZERO)
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + MinIO + control PG)"]
async fn slot_advances_only_after_s3_and_manifest_durable() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_durability";
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute("DELETE FROM public.orders WHERE id = 960001", &[])
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();

    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();
    let epoch = 2_260_001;
    let sink = ParquetSink::new(minio(), "walrus".to_string(), epoch);
    let pool = control_pool().await;
    let mut tx = pool.begin().await.unwrap();
    let mut checkpoint = DurabilityCheckpoint::new(resume.start_lsn());
    let mut router = BatchRouter::new(
        BatchTriggers {
            max_rows: 1,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
    );
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (960001, 'new')",
            &[],
        )
        .await
        .unwrap();

    let mut lsn_end: Option<Lsn> = None;
    let mut key = None;
    tokio::time::timeout(Duration::from_secs(15), async {
        while lsn_end.is_none() {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            if let Some(msg) = on_frame(&mut ctx, frame).unwrap() {
                match &msg {
                    Message::Relation { relation, .. } => {
                        cache.upsert_from_relation(relation.clone(), 1).unwrap();
                    }
                    other => {
                        for sealed in router.route(&cache, other, frame_lsn, 1).unwrap() {
                            let obj = flush_batch(&sink, &mut *tx, epoch, sealed).await.unwrap(); // (a)+(b)
                            checkpoint.on_batch_durable(obj.lsn_end); // (c)
                            checkpoint.send(&mut stream, false).await.unwrap();
                            lsn_end = Some(obj.lsn_end);
                            key = Some(obj.key);
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("a durable batch within 15s");

    let target = lsn_end.unwrap();
    // The server confirms the flush after a round-trip — poll until it reaches lsn_end.
    let mut reached = false;
    for _ in 0..40 {
        if confirmed_flush(&admin, slot).await >= target {
            reached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        reached,
        "confirmed_flush_lsn reached the batch's lsn_end only after S3 + manifest were durable"
    );

    tx.rollback().await.unwrap();
    if let Some(k) = key {
        let _ = minio().delete(&k).await;
    }
    drop(stream);
    drop_slot(&admin, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + MinIO + control PG)"]
async fn crash_between_put_and_standby_restreams_without_loss() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_durability_crash";
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute("DELETE FROM public.orders WHERE id = 960002", &[])
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();

    let epoch = 2_260_002;
    let sink = ParquetSink::new(minio(), "walrus".to_string(), epoch);
    let pool = control_pool().await;
    let mut cache = RelationCache::default();
    let mut router = BatchRouter::new(
        BatchTriggers {
            max_rows: 1,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
    );

    // --- Session 1: PUT + manifest, then "crash" BEFORE the standby update (no confirmed_flush advance).
    let mut key = None;
    {
        let mut stream =
            ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
                .await
                .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let mut ctx = StreamCtx::default();
        admin
            .execute(
                "INSERT INTO public.orders (id, status) VALUES (960002, 'new')",
                &[],
            )
            .await
            .unwrap();
        let mut flushed = false;
        tokio::time::timeout(Duration::from_secs(15), async {
            while !flushed {
                let frame = stream.next().await.unwrap().unwrap();
                let frame_lsn = match &frame {
                    ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                    ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
                };
                if let Some(msg) = on_frame(&mut ctx, frame).unwrap() {
                    match &msg {
                        Message::Relation { relation, .. } => {
                            cache.upsert_from_relation(relation.clone(), 1).unwrap();
                        }
                        other => {
                            for sealed in router.route(&cache, other, frame_lsn, 1).unwrap() {
                                let obj =
                                    flush_batch(&sink, &mut *tx, epoch, sealed).await.unwrap();
                                key = Some(obj.key); // PUT + manifest done...
                                flushed = true; // ...but we CRASH here — no checkpoint.send().
                            }
                        }
                    }
                }
            }
        })
        .await
        .expect("a PUT within 15s");
        tx.rollback().await.unwrap(); // the crash lost the uncommitted manifest too
                                      // stream dropped here = the connection dies (the "crash").
    }

    // confirmed_flush never advanced (we never sent a durable standby update).
    assert_eq!(
        confirmed_flush(&admin, slot).await,
        resume.start_lsn(),
        "confirmed_flush did not advance past the crash window"
    );

    // --- Session 2: reconnect from confirmed_flush → the insert re-streams (at-least-once).
    let resume2 = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream2 =
        ReplicationStream::start(&source_url(), slot, resume2.start_lsn(), "walrus_pub")
            .await
            .unwrap();
    let mut ctx2 = StreamCtx::default();
    let restreamed = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = stream2.next().await.unwrap().unwrap();
            if let Some(Message::Insert { relation_oid, .. }) = on_frame(&mut ctx2, frame).unwrap()
            {
                let _ = relation_oid;
                break true;
            }
        }
    })
    .await
    .expect("the insert re-streams within 15s");
    assert!(
        restreamed,
        "the batch re-streams after a crash before the checkpoint"
    );

    if let Some(k) = key {
        let _ = minio().delete(&k).await;
    }
    drop(stream2);
    drop_slot(&admin, slot).await;
}
