#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Chunk export engine against compose (`#[ignore]` — needs source PG + control PG + MinIO).
//! 2,500 seeded rows at `chunk_rows=1000` become exactly 3 `kind='reload'` files whose union is
//! the table exactly, every row stamped `commit_lsn = lsn = F`; a crashed snapshot is purged and
//! replaced by a fresh fenced successor; and a missing decoder cannot let an exporter query before
//! F is durable. The SQL/stamp shapes are unit-tested in `src/reload_export.rs`.
//!
//! Each test spins a mini fence resolver: a real slot commit-gates `reload_event`, durably records
//! baseline `F` / terminal `H`, and only then resolves the shared `FenceWaiters`.
//!
//!   cargo test -p pg-sink --test reload_export -- --ignored --test-threads=1

use bytes::Bytes;
use common::{EpochNo, Lsn, ReloadId, SchemaVersionNo};
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pg_sink::reload::{RestartDecision, handle_lost_snapshot_restart};
use pg_sink::reload_event::FenceWaiters;
use pg_sink::reload_export::{ChunkExportConfig, ChunkExporter, ChunkOutcome, RunOutcome};
use pg_sink::sink::ParquetSink;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[path = "support/reload_fence.rs"]
mod reload_fence_support;

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const SOURCE_0001: &str = include_str!("../../../migrations/source/0001_publication.sql");
const SOURCE_0003: &str = include_str!("../../../migrations/source/0003_reload_signal.sql");
const SOURCE_0004: &str = include_str!("../../../migrations/source/0004_reload_event.sql");
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

/// Seed the target table with `n` rows and register its shape at the test epoch.
async fn seed(admin: &tokio_postgres::Client, pool: &sqlx::PgPool, epoch: EpochNo, n: i64) {
    admin
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS public.{TABLE};
             CREATE TABLE public.{TABLE} (id int PRIMARY KEY, val text NOT NULL);
             INSERT INTO public.{TABLE} SELECT g, 'v' || g FROM generate_series(1, {n}) g;"
        ))
        .await
        .unwrap();
    let rel = pg_sink::source_catalog::describe_source_relation(admin, "public", TABLE)
        .await
        .unwrap();
    control::upsert_registry(
        pool,
        &control::RegistryRow {
            epoch,
            source_schema: "public".to_string(),
            source_table: TABLE.to_string(),
            schema_version: SchemaVersionNo(1),
            descriptors: pg_to_arrow::describe_relation(&rel).unwrap(),
            columns: serde_json::to_value(&rel).unwrap(),
        },
    )
    .await
    .unwrap();
}

/// Control-side hygiene for a test epoch (safe to run before and after).
async fn scrub(pool: &sqlx::PgPool, epoch: EpochNo) {
    for tbl in ["file_manifest", "table_reload", "schema_registry"] {
        sqlx::query(&format!("DELETE FROM walrus.{tbl} WHERE epoch = $1"))
            .bind(epoch)
            .execute(pool)
            .await
            .unwrap();
    }
}

fn export_cfg(epoch: EpochNo, chunk_rows: u64, echo_timeout: Duration) -> ChunkExportConfig {
    ChunkExportConfig {
        chunk_rows: std::num::NonZeroU64::new(chunk_rows).unwrap(),
        echo_timeout,
        instance: "walrus-sink-test".to_string(),
        epoch,
        publication_name: "walrus_pub".to_string(),
    }
}

/// Claim the single requested reload and return its row.
async fn request_and_claim(pool: &sqlx::PgPool, epoch: EpochNo) -> control::ReloadRow {
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
async fn reload_manifest_rows(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
) -> Vec<(String, i64, String, ReloadId)> {
    let rows = sqlx::query_as::<_, (String, i64, String, i64)>(
        "SELECT s3_uri, row_count, lsn_end::text, reload_id
         FROM walrus.file_manifest
         WHERE epoch = $1 AND source_table = $2 AND kind = 'reload'
         ORDER BY lsn_end, id",
    )
    .bind(epoch)
    .bind(TABLE)
    .fetch_all(pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(uri, count, lsn, reload_id)| (uri, count, lsn, ReloadId(reload_id)))
        .collect()
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
        // Two columns walked in lockstep, so `zip` carries the pairing instead of a row index. The
        // `Option`s are the arrays' null slots: unwrapping asserts what the sink guarantees (an
        // `id` and a stamp on every exported row), which `.value(i)` would have read past silently.
        for (id, meta) in id_col.iter().zip(meta_col.iter()) {
            ids.push(id.unwrap());
            let meta: serde_json::Value = serde_json::from_str(meta.unwrap()).unwrap();
            metas.push((
                meta["commit_lsn"].as_str().unwrap().to_string(),
                meta["lsn"].as_str().unwrap().to_string(),
            ));
        }
    }
    (ids, metas)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn chunks_cover_the_table_exactly_with_shared_baseline_stamp() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(650_001);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 2500).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_re_cover",
        pool.clone(),
        Arc::clone(&waiters),
        Some(TABLE),
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let mut watch_rx = resolver.watched_commit;
    let resolver = resolver.handle;

    let req = request_and_claim(&pool, epoch).await;
    let reload_id = req.reload_id;
    let mut exporter = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        ParquetSink::new(store(), "walrus", epoch),
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();

    // Chunk 1 first, then a concurrent write MID-EXPORT to a PK chunk 1 already covered: the
    // stream event's commit LSN must outrank the chunk stamp (so it wins Phase B's dedup), and
    // its prompt decode is the no-stall proof — both while the export is genuinely in flight.
    exporter.export_next_chunk().await.unwrap();
    admin
        .execute(
            &format!("UPDATE public.{TABLE} SET val = 'overlap' WHERE id = 1"),
            &[],
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), watch_rx.changed())
        .await
        .expect("the stream keeps decoding while the export is mid-flight")
        .expect("the resolver holds the sender until this test cancels it");
    // Copy the LSN out inside the borrowing statement: a `watch::Ref` is a read guard on the
    // channel, never held across an await (`clippy.toml`'s `await-holding-invalid-types`).
    let overlap_commit =
        (*watch_rx.borrow_and_update()).expect("a change is always a published overlap commit");
    let baseline_f = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap()
        .start_lsn
        .unwrap();
    assert!(
        overlap_commit > baseline_f,
        "the mid-export stream event ({overlap_commit}) outranks baseline F ({baseline_f}) — \
         it wins the loader's dedup for that PK"
    );

    let final_h = match exporter.run(false).await.unwrap() {
        RunOutcome::Drained { final_lsn } => final_lsn,
        RunOutcome::SchemaChanged { new_version } => {
            panic!("unexpected schema change to {new_version}")
        }
        RunOutcome::SnapshotLost => panic!("a freshly claimed exporter owns its snapshot"),
    };
    assert!(
        final_h > overlap_commit,
        "terminal H ({final_h}) follows the overlapping WAL commit ({overlap_commit})"
    );
    let markers = control::reload::read_markers(&pool, reload_id)
        .await
        .unwrap();
    assert_eq!(
        markers
            .iter()
            .map(|marker| (marker.kind, marker.lsn))
            .collect::<Vec<_>>(),
        vec![
            (control::ReloadMarkerKind::Baseline, baseline_f),
            (control::ReloadMarkerKind::End, final_h),
        ],
        "the decoder persisted F then H before resolving each waiter"
    );

    // Exactly 3 chunk files: 1000 + 1000 + 500, all sharing safe baseline F.
    let files = reload_manifest_rows(&pool, epoch).await;
    assert_eq!(files.len(), 3, "2500 rows at chunk_rows=1000 ⇒ 3 files");
    assert_eq!(
        files.iter().map(|f| f.1).collect::<Vec<_>>(),
        vec![1000, 1000, 500]
    );
    let ends: Vec<Lsn> = files.iter().map(|f| f.2.parse().unwrap()).collect();
    assert!(
        ends.iter().all(|end| *end == baseline_f),
        "all chunks use F"
    );
    assert!(files.iter().all(|f| f.3 == reload_id));

    // Cursor/freeze bookkeeping: chunk_no 3, cursor at the last PK, and one immutable F.
    let row = control::reload::get(&pool, reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.chunk_no, 3);
    assert_eq!(row.cursor_pk, Some(serde_json::json!(["2500"])));
    assert_eq!(row.start_lsn, Some(baseline_f), "start_lsn is baseline F");
    assert_eq!(
        row.first_lsn,
        Some(baseline_f),
        "legacy first_lsn aliases F"
    );
    assert_eq!(
        row.status,
        control::reload::ReloadStatus::Exporting,
        "the controller records export_complete after the exporter drains"
    );

    // Read the 3 files back: the union is the table exactly, and EVERY row's meta carries
    // commit_lsn = lsn = shared F, which is every file's lsn_end.
    let mut all_ids = Vec::new();
    // `ends` was parsed out of `files` above, so zipping pairs each file with its own stamp — the
    // row index that carried the pairing before was only ever a way to reach back into `ends`.
    for ((uri, _, _lsn_end, _), end) in files.iter().zip(&ends) {
        let (ids, metas) = read_chunk_file(uri).await;
        for (commit_lsn, lsn) in &metas {
            assert_eq!(commit_lsn, lsn, "stamped commit_lsn = lsn");
            assert_eq!(
                commit_lsn.parse::<Lsn>().unwrap(),
                *end,
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
    token.cancel();
    resolver.await.unwrap();
    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires docker compose up --wait (source + control PG + MinIO)"]
async fn adopted_cursor_is_purged_and_successor_reexports_one_snapshot() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(650_002);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 2500).await;

    let waiters = Arc::new(FenceWaiters::default());
    let token = CancellationToken::new();
    let resolver = reload_fence_support::spawn(
        source_url(),
        "walrus_re_resume",
        pool.clone(),
        Arc::clone(&waiters),
        None,
        token.clone(),
    );
    tokio::time::timeout(Duration::from_secs(10), resolver.ready)
        .await
        .expect("fence resolver starts")
        .expect("fence resolver ready sender remains live");
    let resolver = resolver.handle;

    let req = request_and_claim(&pool, epoch).await;
    let reload_id = req.reload_id;
    let sink = ParquetSink::new(store(), "walrus", epoch);

    // Chunk 1, then "crash" (drop the exporter).
    let mut first = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
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
    let baseline_f = mid.start_lsn.expect("F persisted before chunk 1");

    // Adoption cannot resurrect the connection-local repeatable-read snapshot, so it returns a
    // control-flow outcome without another source-table SELECT.
    let mut adopted = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        sink.clone(),
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &mid,
    )
    .await
    .unwrap();
    assert_eq!(adopted.run(true).await.unwrap(), RunOutcome::SnapshotLost);
    let successor_id = match handle_lost_snapshot_restart(&pool, &mid, 3).await.unwrap() {
        RestartDecision::Restarted(id) => id,
        RestartDecision::Capped => panic!("first lost-snapshot restart must be under the cap"),
    };
    assert_eq!(
        reload_manifest_rows(&pool, epoch).await.len(),
        0,
        "the predecessor's chunk is purged with its terminal transition"
    );

    // A lease-expired predecessor that wakes late cannot commit another chunk after the atomic
    // terminal+successor transaction. Its duplicate object is an orphan; no manifest gap forms.
    let mut stale = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        sink.clone(),
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &req,
    )
    .await
    .unwrap();
    let stale_err = stale.export_next_chunk().await.unwrap_err();
    assert!(
        format!("{stale_err:#}").contains("illegal reload transition"),
        "the failed predecessor rejects a late cursor commit: {stale_err:#}"
    );

    let successor = control::reload::get(&pool, successor_id)
        .await
        .unwrap()
        .unwrap();
    let mut restarted = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        Arc::clone(&waiters),
        sink,
        export_cfg(epoch, 1000, Duration::from_secs(20)),
        &successor,
    )
    .await
    .unwrap();
    assert!(matches!(
        restarted.run(false).await.unwrap(),
        RunOutcome::Drained { .. }
    ));

    let files = reload_manifest_rows(&pool, epoch).await;
    assert_eq!(files.len(), 3, "the successor exports 1000+1000+500");
    assert!(files.iter().all(|file| file.3 == successor_id));
    let mut total = 0usize;
    let mut unique = BTreeSet::new();
    for (uri, _, _, _) in &files {
        let (ids, _) = read_chunk_file(uri).await;
        total += ids.len();
        unique.extend(ids);
    }
    assert_eq!(total, 2500, "one consistent successor baseline");
    assert_eq!(unique.len(), 2500, "no gap in the restarted snapshot");

    let done = control::reload::get(&pool, successor_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.chunk_no, 3);
    assert!(
        matches!(done.start_lsn, Some(new_f) if new_f > baseline_f),
        "the successor must establish a fresh F"
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
async fn start_fence_timeout_leaves_the_reload_resumable() {
    let _g = SOURCE_LOCK.lock().await;
    let epoch = EpochNo(650_003);
    let admin = admin().await;
    admin.batch_execute(SOURCE_0001).await.unwrap();
    admin.batch_execute(SOURCE_0003).await.unwrap();
    admin.batch_execute(SOURCE_0004).await.unwrap();
    let pool = control::connect(&control_url()).await.unwrap();
    control::run_migrations(&pool).await.unwrap();
    scrub(&pool, epoch).await;
    seed(&admin, &pool, epoch, 10).await;

    // NO fence resolver runs. Establishing F fails before any table SELECT or file write; the row
    // remains exporting so lease adoption can retry the same deterministic source event safely.
    let waiters = Arc::new(FenceWaiters::default());
    let req = request_and_claim(&pool, epoch).await;
    let err = ChunkExporter::connect(
        &source_url(),
        pool.clone(),
        waiters,
        ParquetSink::new(store(), "walrus", epoch),
        export_cfg(epoch, 1000, Duration::from_secs(2)),
        &req,
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:#}").contains("decode echo timed out"),
        "got: {err:#}"
    );

    let row = control::reload::get(&pool, req.reload_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.status, control::reload::ReloadStatus::Exporting);
    assert_eq!(row.start_lsn, None, "no decoded F was persisted");
    assert_eq!(reload_manifest_rows(&pool, epoch).await.len(), 0);

    scrub(&pool, epoch).await;
    admin
        .batch_execute(&format!("DROP TABLE IF EXISTS public.{TABLE}"))
        .await
        .unwrap();
}
