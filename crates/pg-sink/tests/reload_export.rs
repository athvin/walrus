//! Chunk export engine against compose (`#[ignore]` — needs source PG + control PG + MinIO).
//! 2,500 seeded rows at `chunk_rows=1000` become exactly 3 `kind='reload'` files whose union is
//! the table exactly, every row stamped `commit_lsn = lsn =` its chunk's `L_i`; a crashed export
//! resumes from the cursor without re-exporting; a never-arriving echo fails the reload loudly
//! with the publication hint. The SQL/stamp shapes are unit-tested in `src/reload_export.rs`.
//!
//! Each test spins a mini echo-resolver: a real slot + `on_frame` + `PendingSignals` resolving
//! the shared `WatermarkWaiters` — the decode-loop half of echo-wait (PR 6.3), which the engine
//! blocks on.
//!
//!   cargo test -p pg-sink --test reload_export -- --ignored --test-threads=1

use bytes::Bytes;
use common::Lsn;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pg_sink::consume::on_frame;
use pg_sink::heartbeat::InternalTables;
use pg_sink::pgoutput::{Message, StreamCtx};
use pg_sink::reload_export::{ChunkExportConfig, ChunkExporter, ChunkOutcome};
use pg_sink::reload_signal::{PendingSignal, PendingSignals, WatermarkWaiters};
use pg_sink::replication::ReplicationStream;
use pg_sink::sink::ParquetSink;
use pg_sink::slot::verify_or_create_slot;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const TABLE: &str = "_walrus_re_orders";

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

fn store() -> Arc<dyn ObjectStore> {
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

/// Seed the target table with `n` rows and register its shape at the test epoch.
async fn seed(admin: &tokio_postgres::Client, pool: &sqlx::PgPool, epoch: i64, n: i64) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id int PRIMARY KEY, val text NOT NULL);
             INSERT INTO public.{TABLE} SELECT g, 'v' || g FROM generate_series(1, {n}) g;"
        ))
        .await
        .unwrap();
    let rel = pg_sink::snapshot::describe_source_relation(admin, "public", TABLE)
        .await
        .unwrap();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: TABLE.to_string(),
            schema_version: 1,
            descriptors: pg_to_arrow::descriptor::describe_relation(&rel),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

/// Control-side hygiene for a test epoch (safe to run before and after).
async fn scrub(pool: &sqlx::PgPool, epoch: i64) {
    for tbl in ["file_manifest", "table_reload", "schema_registry"] {
        sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(pool)
            .await
            .unwrap();
    }
}

/// The decode-loop half of echo-wait, minimally: resolve signal echoes against `waiters`, and
/// report the commit LSN of any transaction that carried a change on `watch_oid`'s table (the
/// overlap/no-stall probe). Runs until cancelled.
fn spawn_echo_resolver(
    slot: &'static str,
    waiters: Arc<WatermarkWaiters>,
    watch_table: &'static str,
    token: CancellationToken,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<Lsn>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        let admin = admin().await;
        drop_slot(&admin, slot).await;
        let resume = verify_or_create_slot(&admin, slot).await.unwrap();
        let mut stream =
            ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
                .await
                .unwrap();
        let mut ctx = StreamCtx::default();
        let mut internal = InternalTables::default();
        let mut pending = PendingSignals::default();
        let mut watch_oid: Option<u32> = None;
        let mut txn_touched_watch = false;
        loop {
            let frame = tokio::select! {
                _ = token.cancelled() => break,
                f = stream.next() => f.unwrap().unwrap(),
            };
            let Some(msg) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &msg {
                Message::Relation { relation, .. } => {
                    internal.note_relation(relation);
                    if relation.name == watch_table {
                        watch_oid = Some(relation.oid);
                    }
                }
                Message::Insert {
                    relation_oid,
                    new,
                    xid,
                } if internal.is_reload_signal(*relation_oid) => {
                    let rel = internal.reload_signal_rel().unwrap();
                    if let Some(sig) = PendingSignal::from_tuple(rel, new, *xid) {
                        pending.push(sig);
                    }
                }
                Message::Insert { relation_oid, .. } | Message::Update { relation_oid, .. }
                    if watch_oid == Some(*relation_oid) =>
                {
                    txn_touched_watch = true;
                }
                Message::Commit { commit_lsn, .. } => {
                    pending.on_commit(*commit_lsn, &waiters);
                    if std::mem::take(&mut txn_touched_watch) {
                        let _ = tx.send(*commit_lsn);
                    }
                }
                _ => {}
            }
        }
        drop(stream);
        drop_slot(&admin, slot).await;
    });
    (handle, rx)
}

fn export_cfg(epoch: i64, chunk_rows: u64, echo_timeout: Duration) -> ChunkExportConfig {
    ChunkExportConfig {
        chunk_rows,
        echo_timeout,
        instance: "walrus-sink-test".to_string(),
        epoch,
    }
}

/// Claim the single requested reload and return its row.
async fn request_and_claim(pool: &sqlx::PgPool, epoch: i64) -> control::ReloadRow {
    control::reload::request(
        pool,
        epoch,
        "public",
        TABLE,
        control::reload::ReloadFlavor::Reload,
    )
    .await
    .unwrap();
    let mut claimed = control::reload::claim_requested(pool, epoch, "walrus-sink-test", 60, 1)
        .await
        .unwrap();
    claimed.pop().unwrap()
}

/// Manifest rows for the test table's reload files, in claim order.
async fn reload_manifest_rows(pool: &sqlx::PgPool, epoch: i64) -> Vec<(String, i64, String, i64)> {
    sqlx::query_as::<_, (String, i64, String, i64)>(
        "SELECT s3_uri, row_count, lsn_end::text, reload_id
         FROM walrus.file_manifest
         WHERE epoch = $1 AND source_table = $2 AND kind = 'reload'
         ORDER BY lsn_end, id",
    )
    .bind(epoch)
    .bind(TABLE)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Read a reload chunk file back: (ids, every-row (commit_lsn, lsn) from the meta JSON).
async fn read_chunk_file(uri: &str) -> (Vec<i32>, Vec<(String, String)>) {
    let key = uri.strip_prefix("s3://walrus/").unwrap();
    let bytes: Bytes = store()
        .get(&Path::from(key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();
    let mut ids = Vec::new();
    let mut metas = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let id_col = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .clone();
        let meta_col = batch
            .column_by_name(pg_to_arrow::SINK_META_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .clone();
        for i in 0..batch.num_rows() {
            ids.push(id_col.value(i));
            let meta: serde_json::Value = serde_json::from_str(meta_col.value(i)).unwrap();
            metas.push((
                meta["commit_lsn"].as_str().unwrap().to_string(),
                meta["lsn"].as_str().unwrap().to_string(),
            ));
        }
    }
    (ids, metas)
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn chunks_cover_the_table_exactly_with_per_chunk_stamps() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = 650_001;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 2500).await;

    let waiters = Arc::new(WatermarkWaiters::default());
    let token = CancellationToken::new();
    let (resolver, mut watch_rx) =
        spawn_echo_resolver("walrus_re_cover", waiters.clone(), TABLE, token.clone());
    // Give the resolver's slot a moment to exist before the exporter signals through it.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let req = request_and_claim(&pool, epoch).await;
    let reload_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        ParquetSink::new(store(), "walrus".to_string(), epoch),
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();
    exporter.run().await.unwrap();

    // Exactly 3 chunk files: 1000 + 1000 + 500, strictly increasing lsn_end, all this reload's.
    let files = reload_manifest_rows(&pool, epoch).await;
    assert_eq!(files.len(), 3, "2500 rows at chunk_rows=1000 ⇒ 3 files");
    assert_eq!(
        files.iter().map(|f| f.1).collect::<Vec<_>>(),
        vec![1000, 1000, 500]
    );
    let ends: Vec<Lsn> = files.iter().map(|f| f.2.parse().unwrap()).collect();
    assert!(
        ends[0] < ends[1] && ends[1] < ends[2],
        "strictly increasing L_i"
    );
    assert!(files.iter().all(|f| f.3 == reload_id));

    // Cursor/freeze bookkeeping: chunk_no 3, cursor at the last PK, first_lsn = L_1, forever.
    let row = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.chunk_no, 3);
    assert_eq!(row.cursor_pk, Some(serde_json::json!(["2500"])));
    assert_eq!(row.first_lsn, Some(ends[0]), "first_lsn is chunk 1's L_1");
    assert_eq!(
        row.status,
        control::reload::ReloadStatus::Exporting,
        "export_complete is PR 6.9's"
    );

    // Read the 3 files back: the union is the table exactly, and EVERY row's meta carries
    // commit_lsn = lsn = its file's lsn_end (the stamp).
    let mut all_ids = Vec::new();
    for (i, (uri, _, _lsn_end, _)) in files.iter().enumerate() {
        let (ids, metas) = read_chunk_file(uri).await;
        for (commit_lsn, lsn) in &metas {
            assert_eq!(commit_lsn, lsn, "stamped commit_lsn = lsn");
            assert_eq!(
                commit_lsn.parse::<Lsn>().unwrap(),
                ends[i],
                "stamp == the file's lsn_end"
            );
        }
        all_ids.extend(ids);
    }
    let unique: BTreeSet<i32> = all_ids.iter().copied().collect();
    assert_eq!(all_ids.len(), 2500, "no duplicates across chunks");
    assert_eq!(unique.len(), 2500, "no misses");
    assert_eq!(*unique.first().unwrap(), 1);
    assert_eq!(*unique.last().unwrap(), 2500);
    assert_eq!(
        waiters.crosscheck_violations(),
        0,
        "embedded < L_i held on every chunk"
    );

    // Overlap math + the stream still flows: a write AFTER the chunks decodes promptly and its
    // commit LSN outranks every chunk stamp — the stream event would win Phase B's dedup.
    admin
        .execute(
            &format!("UPDATE public.{TABLE} SET val = 'overlap' WHERE id = 1"),
            &[],
        )
        .await
        .unwrap();
    let overlap_commit = tokio::time::timeout(Duration::from_secs(10), watch_rx.recv())
        .await
        .expect("the stream keeps decoding during/after the export")
        .unwrap();
    assert!(
        overlap_commit > ends[2],
        "a post-chunk stream event outranks every chunk stamp ({overlap_commit} > {})",
        ends[2]
    );

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn resume_from_cursor_never_reexports_completed_chunks() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = 650_002;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 2500).await;

    let waiters = Arc::new(WatermarkWaiters::default());
    let token = CancellationToken::new();
    let (resolver, _watch) =
        spawn_echo_resolver("walrus_re_resume", waiters.clone(), TABLE, token.clone());
    tokio::time::sleep(Duration::from_millis(500)).await;

    let req = request_and_claim(&pool, epoch).await;
    let reload_id = req.reload_id;
    let sink = ParquetSink::new(store(), "walrus".to_string(), epoch);

    // Chunk 1, then "crash" (drop the exporter).
    let mut first = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        sink.clone(),
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();
    let outcome = first.export_next_chunk().await.unwrap();
    assert!(matches!(outcome, ChunkOutcome::Exported { rows: 1000 }));
    drop(first);

    let mid = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mid.chunk_no, 1);
    let frozen_first_lsn = mid.first_lsn.expect("L_1 frozen at chunk 1");

    // Resume from the cursor: chunks 2..3 export; chunk 1 is NOT re-exported.
    let mut resumed = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters.clone(),
        sink,
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &mid,
    )
    .await
    .unwrap();
    resumed.run().await.unwrap();

    let files = reload_manifest_rows(&pool, epoch).await;
    assert_eq!(files.len(), 3, "1 pre-crash + 2 post-resume, never 4");
    let mut total = 0usize;
    let mut unique = BTreeSet::new();
    for (uri, _, _, _) in &files {
        let (ids, _) = read_chunk_file(uri).await;
        total += ids.len();
        unique.extend(ids);
    }
    assert_eq!(total, 2500, "no chunk re-exported (no duplicate rows)");
    assert_eq!(unique.len(), 2500, "and no gap either");

    let done = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.chunk_no, 3);
    assert_eq!(
        done.first_lsn,
        Some(frozen_first_lsn),
        "first_lsn never changes across a resume"
    );

    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn echo_timeout_fails_the_reload_with_publication_hint() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = 650_003;
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 10).await;

    // NO echo resolver runs — the production analogue of an unpublished signal table: the echo
    // simply never arrives. The timeout must turn that silence into a failed row naming the fix.
    let waiters = Arc::new(WatermarkWaiters::default());
    let req = request_and_claim(&pool, epoch).await;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters,
        ParquetSink::new(store(), "walrus".to_string(), epoch),
        export_cfg(epoch, 1000, Duration::from_secs(2)),
        &req,
    )
    .await
    .unwrap();
    let err = exporter.run().await.unwrap_err();
    assert!(err.to_string().contains("echo timeout"), "got: {err:#}");

    let row = control::reload::get(&pool, req.reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, control::reload::ReloadStatus::Failed);
    let reason = row.error.unwrap();
    assert!(
        reason.contains("publication") && reason.contains("0003_reload_signal"),
        "the failure names the fix: {reason}"
    );

    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}
