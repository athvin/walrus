#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Micro-batching against the live stream (`#[ignore]` — needs source PG on `trust`). A stream of
//! inserts forms and seals ≥ 1 batch. The fake-clock threshold logic is unit-tested inline in
//! `src/batch.rs`; this proves the routing seals a real batch end to end.
//!
//!   cargo test -p pg-sink --test batching -- --ignored

use pg_sink::batch::{BatchTriggers, SystemClock};
use pg_sink::consume::{on_frame, BatchRouter};
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::relcache::RelationCache;
use pg_sink::replication::{ReplicationMessage, ReplicationStream};
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

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG, trust auth)"]
async fn a_stream_of_inserts_forms_and_seals_a_batch() {
    let _guard = SOURCE_LOCK.lock().await;
    let slot = "walrus_batch";
    let admin = admin().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id >= 970000 AND id < 971000",
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

    // Three committed inserts. max_rows = 1 → each commit seals its own batch.
    for id in 970001i32..=970003 {
        admin
            .execute(
                "INSERT INTO public.orders (id, status) VALUES ($1, 'new')",
                &[&id],
            )
            .await
            .unwrap();
    }

    let triggers = BatchTriggers {
        max_rows: 1,
        max_bytes: u64::MAX,
        max_fill: Duration::from_secs(3600),
    };
    let mut router = BatchRouter::new(triggers, Arc::new(SystemClock), 1, "test".to_string());
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();
    let mut sealed_total = 0u64;

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = stream.next().await.expect("frame").expect("stream open");
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => common::Lsn::ZERO,
            };
            if let Some(msg) = on_frame(&mut ctx, frame).expect("decode") {
                match &msg {
                    Message::Relation { relation, .. } => {
                        cache.upsert_from_relation(relation.clone(), 1).unwrap();
                    }
                    other => {
                        for sealed in router.route(&cache, other, frame_lsn, 1).unwrap() {
                            assert_eq!(sealed.table, "orders");
                            assert!(sealed.row_count >= 1);
                            assert_eq!(sealed.record_batch.num_rows(), sealed.row_count as usize);
                            sealed_total += 1;
                        }
                    }
                }
            }
            if sealed_total >= 1 {
                break;
            }
        }
    })
    .await
    .expect("a sealed batch within 15s");

    assert!(sealed_total >= 1, "≥1 batch sealed from the insert stream");

    drop(stream);
    drop_slot(&admin, slot).await;
}
