//! Graceful SIGTERM drain against compose (`#[ignore]` — needs source PG + MinIO + control PG). A
//! committed-but-unflushed batch, on drain, is flushed to S3 + manifested, `confirmed_flush_lsn`
//! advances via a final standby update, `CopyDone` closes the connection, and the **slot is never
//! dropped** — so a restarted sink resumes from `confirmed_flush_lsn` with **no data loss** (the
//! boundary txn may re-stream; the loader de-duplicates: at-least-once → effectively-once). The drain
//! ordering itself is unit-tested in `src/batch.rs`.
//!
//!   cargo test -p pg-sink --test graceful_shutdown -- --ignored

use common::{Lsn, TupleValue};
use object_store::path::Path;
use object_store::ObjectStore;
use pg_sink::batch::{BatchTriggers, SystemClock};
use pg_sink::checkpoint::DurabilityCheckpoint;
use pg_sink::consume::{on_frame, BatchRouter};
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::relcache::RelationCache;
use pg_sink::replication::{ReplicationMessage, ReplicationStream};
use pg_sink::shutdown::{drain, DrainOutcome};
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
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

fn minio() -> Arc<dyn ObjectStore> {
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

async fn slot_exists(admin: &tokio_postgres::Client, slot: &str) -> bool {
    let row = admin
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap();
    row.get::<_, i64>(0) > 0
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

/// The `id` (first column) of an orders new-tuple, text format.
fn orders_id(new: &[TupleValue]) -> Option<i32> {
    match new.first()? {
        TupleValue::Text(s) => s.parse().ok(),
        _ => None,
    }
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + MinIO + control PG)"]
async fn sigterm_mid_stream_drains_commits_and_resumes() {
    let _g = SOURCE_LOCK.lock().await;
    let slot = "walrus_drain";
    let epoch = 2_280_001;
    let admin = source().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id IN (980001, 980002)",
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
    let mut checkpoint = DurabilityCheckpoint::new(resume.start_lsn());
    let sink = ParquetSink::new(minio(), "walrus".to_string(), epoch);
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    // Thresholds so high nothing auto-flushes: the batch is committed but stays in flight.
    let mut router = BatchRouter::new(
        BatchTriggers {
            max_rows: u64::MAX,
            max_bytes: u64::MAX,
            max_fill: Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
        epoch,
        "test".to_string(),
    );
    let mut cache = RelationCache::default();
    let mut ctx = StreamCtx::default();

    // A single committed txn — flush-eligible only via drain (below every threshold).
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (980001, 'new')",
            &[],
        )
        .await
        .unwrap();

    let mut orders_oid: Option<u32> = None;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            let frame_lsn = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => *wal_start,
                ReplicationMessage::Keepalive { .. } => Lsn::ZERO,
            };
            if let Some(msg) = on_frame(&mut ctx, frame).unwrap() {
                match &msg {
                    Message::Relation { relation, .. } => {
                        if relation.name == "orders" {
                            orders_oid = Some(relation.oid);
                        }
                        cache.upsert_from_relation(relation.clone(), 1).unwrap();
                    }
                    other => {
                        router.route(&cache, other, frame_lsn, 1).unwrap();
                        // Stop once the insert has committed but is still un-durable.
                        if matches!(other, Message::Commit { .. })
                            && router.undurable_floor().is_some()
                        {
                            break;
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("the committed-but-unflushed batch forms within 15s");
    assert!(
        router.undurable_floor().is_some(),
        "batch committed, not yet durable"
    );

    // --- SIGTERM drain: flush + manifest + final standby, CopyDone, slot left in place.
    let outcome = drain(
        &mut stream,
        &mut router,
        &sink,
        &mut checkpoint,
        &pool,
        epoch,
    )
    .await
    .unwrap();
    let DrainOutcome::Drained {
        confirmed_flush: drained_lsn,
    } = outcome
    else {
        panic!("expected the in-flight committed batch to drain, got {outcome:?}");
    };
    assert!(
        drained_lsn > resume.start_lsn(),
        "the drain advanced confirmed_flush past the resume point"
    );

    // The committed batch is now durable: a manifest row exists for this epoch.
    let manifested: i64 =
        sqlx::query_scalar("SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        manifested >= 1,
        "the drained batch was committed to the manifest"
    );

    // The slot is NEVER dropped on a graceful shutdown.
    assert!(
        slot_exists(&admin, slot).await,
        "the slot persists across shutdown"
    );

    // The server records the final feedback (round-trip); poll until confirmed_flush advances.
    let mut reached = false;
    for _ in 0..40 {
        if confirmed_flush(&admin, slot).await >= drained_lsn {
            reached = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        reached,
        "the final standby update advanced the server's confirmed_flush"
    );
    drop(stream);

    // --- Resume: a replacement pod streams from confirmed_flush and loses nothing. The boundary txn
    // may re-stream (at-least-once → the loader de-duplicates); the load-bearing property is that the
    // NEW change (980002) is delivered after resume — no data is lost across the graceful shutdown.
    let resume2 = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream2 =
        ReplicationStream::start(&source_url(), slot, resume2.start_lsn(), "walrus_pub")
            .await
            .unwrap();
    let mut ctx2 = StreamCtx::default();
    admin
        .execute(
            "INSERT INTO public.orders (id, status) VALUES (980002, 'new')",
            &[],
        )
        .await
        .unwrap();
    let saw_new = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = stream2.next().await.unwrap().unwrap();
            if let Some(Message::Insert {
                relation_oid, new, ..
            }) = on_frame(&mut ctx2, frame).unwrap()
            {
                if orders_oid == Some(relation_oid) && orders_id(&new) == Some(980002) {
                    return true;
                }
            }
        }
    })
    .await
    .expect("the resumed stream delivers the post-shutdown insert within 15s");
    assert!(
        saw_new,
        "the sink resumed from confirmed_flush and lost no data"
    );

    // --- Cleanup: delete the staged S3 object(s), manifest rows, slot, and test data.
    let uris: Vec<String> =
        sqlx::query_scalar("SELECT s3_uri FROM walrus.file_manifest WHERE epoch = $1")
            .bind(epoch)
            .fetch_all(&pool)
            .await
            .unwrap();
    let store = minio();
    for uri in uris {
        if let Some(key) = uri.strip_prefix("s3://walrus/") {
            let _ = store.delete(&Path::from(key)).await;
        }
    }
    sqlx::query("DELETE FROM walrus.file_manifest WHERE epoch = $1")
        .bind(epoch)
        .execute(&pool)
        .await
        .unwrap();
    admin
        .execute(
            "DELETE FROM public.orders WHERE id IN (980001, 980002)",
            &[],
        )
        .await
        .unwrap();
    drop(stream2);
    drop_slot(&admin, slot).await;
}
