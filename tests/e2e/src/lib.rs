//! The walrus end-to-end harness (`architecture.md` "Local harness"). It brings up **both binaries** —
//! `walrus-pg-sink` and `walrus-loader` — as child processes against the already-running compose stack
//! (source PG :5432, control PG :5433, MinIO :9000), drives the *source* database, and lets a test assert
//! the full two-hop contract: Parquet in MinIO → verbatim `<table>_raw` → the `<table>` mirror equals the
//! current source. Everything is `#[ignore]` and gated behind `--features it`, so a plain
//! `cargo build/test --workspace` compiles this crate with zero active tests and never needs docker.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};

const SOURCE_URL: &str = "postgres://postgres:postgres@localhost:5432/walrus";
const CONTROL_URL: &str = "postgres://postgres:postgres@localhost:5433/walrus_control";
const S3_ENDPOINT: &str = "http://localhost:9000";
const BUCKET: &str = "walrus";
const SLOT: &str = "walrus_e2e_slot";

/// A running walrus stack: the compose services (assumed up) plus a live `pg-sink` and `loader` spawned
/// as child processes. `Drop` kills both — a leaked sink holds the replication slot and blocks the next
/// run's bootstrap.
pub struct Harness {
    sink: Child,
    loader: Child,
    source: sqlx::PgPool,
    control: sqlx::PgPool,
    duckdb_dir: PathBuf,
    /// The sink's captured stdout+stderr (its `tracing` log) — scraped for spill events (PR 4.3).
    sink_log: PathBuf,
    /// The epoch the sink established (always 1 after the clean reset).
    pub epoch: i64,
}

impl Harness {
    /// Reset control + source to a clean slate, then bring up both binaries and block until each reports
    /// `/ready`. Fails fast if either bootstrap errors.
    pub async fn start() -> Result<Self> {
        let control = control::connect(CONTROL_URL)
            .await
            .context("connect control PG")?;
        // Fully reset control so `read_current_epoch` (MAX) yields a fresh epoch 1 — a leftover higher
        // epoch from another test would otherwise be resumed with no slot/registry behind it.
        sqlx::raw_sql(
            "DROP SCHEMA IF EXISTS walrus CASCADE; DROP TABLE IF EXISTS _sqlx_migrations;",
        )
        .execute(&control)
        .await
        .context("reset control schema")?;
        control::run_migrations(&control)
            .await
            .context("control migrations")?;

        let source = control::connect(SOURCE_URL)
            .await
            .context("connect source PG")?;
        // Idempotent source-side setup (walrus.heartbeat / ddl_audit + DDL triggers), a clean `orders`,
        // and a dropped leftover slot so the sink creates its own fresh one.
        sqlx::raw_sql(include_str!(
            "../../../migrations/source/0001_publication.sql"
        ))
        .execute(&source)
        .await
        .context("source 0001")?;
        sqlx::raw_sql(include_str!(
            "../../../migrations/source/0002_ddl_triggers.sql"
        ))
        .execute(&source)
        .await
        .context("source 0002")?;
        // The wide fidelity table (PR 4.2) — one column per mapped type family + a TOAST-able `big`. It
        // must exist BEFORE the sink bootstraps so the sink registers it and the loader owns it.
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS public.types_matrix ( \
                 id  int PRIMARY KEY, n numeric(10,4), j jsonb, u uuid, ts timestamptz, \
                 b bytea, iv interval, rng int4range, big text, s text); \
             ALTER TABLE public.types_matrix ALTER COLUMN big SET STORAGE EXTENDED;",
        )
        .execute(&source)
        .await
        .context("create types_matrix")?;
        sqlx::raw_sql(&format!(
            "TRUNCATE public.orders; TRUNCATE public.types_matrix; \
             SELECT pg_drop_replication_slot('{SLOT}') \
                FROM pg_replication_slots WHERE slot_name = '{SLOT}';"
        ))
        .execute(&source)
        .await
        .context("reset source tables + slot")?;

        let bins = target_dir();
        build_bins(&bins).await?;
        let duckdb_dir = std::env::temp_dir().join(format!("walrus-e2e-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&duckdb_dir);
        std::fs::create_dir_all(&duckdb_dir)?;
        let sink_log = duckdb_dir.join("sink.log");

        let sink = spawn_sink(&bins, &sink_log)?;
        wait_ready("http://127.0.0.1:8130", Duration::from_secs(45))
            .await
            .context("sink /ready")?;
        let loader = spawn_loader(&bins, &duckdb_dir)?;
        wait_ready("http://127.0.0.1:8131", Duration::from_secs(45))
            .await
            .context("loader /ready")?;

        Ok(Harness {
            sink,
            loader,
            source,
            control,
            duckdb_dir,
            sink_log,
            epoch: 1,
        })
    }

    /// The source Postgres pool — for tests that need multiple concurrent sessions (overlapping txns).
    pub fn source_pool(&self) -> &sqlx::PgPool {
        &self.source
    }

    /// How many speculative spills the sink has logged so far (PR 4.3) — the observable proof that the
    /// `max_inflight_bytes` ceiling fired and open-txn memory stayed bounded (a real `in_flight_bytes`
    /// metric endpoint lands in PR 4.10).
    pub fn sink_spill_count(&self) -> usize {
        std::fs::read_to_string(&self.sink_log)
            .map(|s| s.matches("spilled open-txn buffer").count())
            .unwrap_or(0)
    }

    /// Poll [`Harness::sink_spill_count`] until it reaches `min`, or the deadline elapses. Call this while
    /// the producing txn is STILL OPEN: holding the txn open is what lets the walsender read past
    /// `logical_decoding_work_mem` and stream it (a fast `BEGIN;…;COMMIT` can commit before it is decoded,
    /// so it decodes as a complete, non-streamed txn and never spills). Deterministic, not a fixed sleep.
    pub async fn await_spill(&self, min: usize, deadline: std::time::Duration) -> Result<usize> {
        let start = tokio::time::Instant::now();
        loop {
            let n = self.sink_spill_count();
            if n >= min {
                return Ok(n);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("sink spilled {n} < {min} within {deadline:?}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// The WAL bytes the replication slot is retaining (`restart_lsn` .. current) — bounded once a txn
    /// commits and is consumed.
    pub async fn slot_retained_bytes(&self) -> Result<i64> {
        Ok(sqlx::query_scalar(
            "SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint \
             FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(SLOT)
        .fetch_optional(&self.source)
        .await?
        .unwrap_or(0))
    }

    /// Run a SINGLE SQL statement on the SOURCE database; returns rows affected.
    pub async fn source_exec(&self, sql: &str) -> Result<u64> {
        Ok(sqlx::query(sql)
            .execute(&self.source)
            .await?
            .rows_affected())
    }

    /// Run a MULTI-statement SQL batch on the SOURCE (simple query protocol) — e.g. `BEGIN; …; COMMIT`
    /// with savepoints, which the extended protocol of [`Harness::source_exec`] rejects.
    pub async fn source_batch(&self, sql: &str) -> Result<()> {
        sqlx::raw_sql(sql).execute(&self.source).await?;
        Ok(())
    }

    /// List S3 object keys under `<epoch>/<schema>/<table>/`.
    pub async fn s3_list(&self, table: &str) -> Result<Vec<String>> {
        use object_store::{aws::AmazonS3Builder, ObjectStore};
        let store = AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_region("us-east-1")
            .with_endpoint(S3_ENDPOINT)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()?;
        // The Parquet objects sit one delimiter level under `<epoch>/public/<table>/`, so a single
        // `list_with_delimiter` returns them directly — no streaming.
        let prefix = object_store::path::Path::from(format!("{}/public/{}", self.epoch, table));
        let res = store.list_with_delimiter(Some(&prefix)).await?;
        Ok(res
            .objects
            .into_iter()
            .map(|o| o.location.to_string())
            .collect())
    }

    /// The source's current WAL insert position — captured **before** a change as the watermark target
    /// the loader's `transformed_lsn` must later cross.
    pub async fn source_wal_lsn(&self) -> Result<common::Lsn> {
        let s: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::text")
            .fetch_one(&self.source)
            .await?;
        s.parse()
            .map_err(|e| anyhow::anyhow!("parse wal lsn {s:?}: {e:?}"))
    }

    /// Poll `loader_checkpoint.transformed_lsn` for `table` until it passes `target` (every streamed
    /// change committed before `target` is now in the mirror) AND the queue is drained. Watermark-based,
    /// never a fixed sleep. `target` is a source LSN taken before the change, so only the streamed change
    /// — not the earlier snapshot — can cross it.
    pub async fn await_transformed_past(
        &self,
        table: &str,
        target: common::Lsn,
        deadline: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            let pending: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM walrus.file_manifest WHERE epoch = $1 AND source_table = $2",
            )
            .bind(self.epoch)
            .bind(table)
            .fetch_one(&self.control)
            .await?;
            let cp = control::read_checkpoint(&self.control, self.epoch, "public", table).await?;
            if let Some(cp) = cp {
                if pending == 0
                    && cp.transformed_lsn > target
                    && cp.transformed_lsn == cp.raw_appended_lsn
                {
                    return Ok(());
                }
            }
            if start.elapsed() > deadline {
                anyhow::bail!(
                    "transformed_lsn for {table} never passed {target} within {deadline:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Stop the loader so its `.duckdb` file lock is released, then query the file. DuckDB is single-writer;
    /// reading after the loader exits avoids fighting the lock.
    pub async fn stop_loader(&mut self) -> Result<()> {
        let _ = self.loader.start_kill();
        let _ = self.loader.wait().await;
        Ok(())
    }

    /// Open the loader's per-table `.duckdb` file read-only and collect the first column of each row as a
    /// string. Call after [`Harness::stop_loader`].
    pub fn duckdb_rows(&self, table: &str, sql: &str) -> Result<Vec<String>> {
        let path = self.duckdb_dir.join(format!("{table}.duckdb"));
        let conn = duckdb::Connection::open_with_flags(
            &path,
            duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?,
        )
        .with_context(|| format!("open {}", path.display()))?;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// A single integer scalar from the loader's `.duckdb` file (read-only).
    pub fn duckdb_scalar(&self, table: &str, sql: &str) -> Result<i64> {
        let path = self.duckdb_dir.join(format!("{table}.duckdb"));
        let conn = duckdb::Connection::open_with_flags(
            &path,
            duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?,
        )?;
        Ok(conn.query_row(sql, [], |r| r.get(0))?)
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Best-effort kill both bins — a leaked sink pins the slot and blocks the next bootstrap.
        let _ = self.sink.start_kill();
        let _ = self.loader.start_kill();
        let _ = std::fs::remove_dir_all(&self.duckdb_dir);
    }
}

/// The `target/<profile>/` directory holding the sibling binaries (next to this test binary).
fn target_dir() -> PathBuf {
    // .../target/<profile>/deps/<thisbin> → up two = target/<profile>/
    let mut p = std::env::current_exe().expect("current_exe");
    p.pop(); // deps
    p.pop(); // <profile>
    p
}

async fn build_bins(_target: &std::path::Path) -> Result<()> {
    let status = Command::new(env!("CARGO"))
        .args([
            "build",
            "-p",
            "pg-sink",
            "--bin",
            "walrus-pg-sink",
            "-p",
            "loader",
            "--bin",
            "walrus-loader",
        ])
        .status()
        .await
        .context("cargo build bins")?;
    anyhow::ensure!(status.success(), "cargo build of the bins failed");
    Ok(())
}

fn spawn_sink(bins: &std::path::Path, log: &std::path::Path) -> Result<Child> {
    // The sink's `tracing` fmt layer writes to STDOUT (its spill/durability events live there); config
    // errors + panics go to STDERR. Capture BOTH into `sink.log` (two handles onto one file) so
    // [`Harness::sink_spill_count`] can scrape the spill events AND a startup failure is still visible.
    let stdout = std::fs::File::create(log).context("create sink log")?;
    let stderr = stdout.try_clone().context("clone sink log handle")?;
    Command::new(bins.join("walrus-pg-sink"))
        .stdout(std::process::Stdio::from(stdout))
        .env("WALRUS_SOURCE_DB_URL", SOURCE_URL)
        .env("WALRUS_CONTROL_DB_URL", CONTROL_URL)
        .env("WALRUS_OBJECT_STORE__BUCKET", BUCKET)
        .env("WALRUS_OBJECT_STORE__ENDPOINT", S3_ENDPOINT)
        .env("WALRUS_OBJECT_STORE__REGION", "us-east-1")
        .env("WALRUS_INSTANCE", "e2e-sink")
        .env("WALRUS_SLOT_NAME", SLOT)
        .env("WALRUS_PUBLICATION_NAME", "walrus_pub")
        .env("WALRUS_MANAGE_PUBLICATION", "false")
        .env("WALRUS_MAX_FILL", "1s")
        .env("WALRUS_MAX_ROWS", "100000")
        // A LOW aggregate ceiling (64 KiB) so a few thousand rows in one open txn spill (PR 4.3) —
        // bounding memory — instead of buffering the whole txn. `max_bytes` (per-batch) must stay ≤ the
        // ceiling (the sink validates `max_inflight_bytes >= max_bytes`).
        .env("WALRUS_MAX_BYTES", "32768")
        .env("WALRUS_MAX_INFLIGHT_BYTES", "65536")
        .env("WALRUS_HEARTBEAT_IDLE_AFTER", "1s")
        .env("WALRUS_STARTUP_DEADLINE", "30s")
        .env("WALRUS_HEALTH_ADDR", "127.0.0.1:8130")
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .stderr(std::process::Stdio::from(stderr))
        .kill_on_drop(true)
        .spawn()
        .context("spawn walrus-pg-sink")
}

fn spawn_loader(bins: &std::path::Path, duckdb_dir: &std::path::Path) -> Result<Child> {
    Command::new(bins.join("walrus-loader"))
        .env("WALRUS_CONTROL_DB_URL", CONTROL_URL)
        .env("WALRUS_OBJECT_STORE__BUCKET", BUCKET)
        .env("WALRUS_OBJECT_STORE__ENDPOINT", S3_ENDPOINT)
        .env("WALRUS_OBJECT_STORE__REGION", "us-east-1")
        .env("WALRUS_INSTANCE", "e2e-loader")
        .env("WALRUS_DUCKDB_DIR", duckdb_dir.to_string_lossy().as_ref())
        .env("WALRUS_POLL_INTERVAL", "1s")
        .env("WALRUS_STARTUP_DEADLINE", "30s")
        .env("WALRUS_HEALTH_ADDR", "127.0.0.1:8131")
        .env("AWS_ACCESS_KEY_ID", "minioadmin")
        .env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .kill_on_drop(true)
        .spawn()
        .context("spawn walrus-loader")
}

/// Poll a `/ready` endpoint until it answers 200 or the deadline elapses.
async fn wait_ready(base: &str, deadline: Duration) -> Result<()> {
    let url = format!("{base}/ready");
    let start = Instant::now();
    loop {
        if let Ok(conn) = tokio::net::TcpStream::connect(base.trim_start_matches("http://")).await {
            drop(conn);
            // Minimal HTTP GET; treat "200" in the status line as ready.
            if http_get_ok(&url).await {
                return Ok(());
            }
        }
        if start.elapsed() > deadline {
            anyhow::bail!("{url} never became ready within {deadline:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// A dependency-free HTTP GET returning true iff the status line is `200`.
async fn http_get_ok(url: &str) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.trim_start_matches("http://");
    let (authority, path) = rest
        .split_once('/')
        .map(|(a, p)| (a, format!("/{p}")))
        .unwrap_or((rest, "/".into()));
    let Ok(mut stream) = tokio::net::TcpStream::connect(authority).await else {
        return false;
    };
    let req = format!("GET {path} HTTP/1.0\r\nHost: {authority}\r\n\r\n");
    if stream.write_all(req.as_bytes()).await.is_err() {
        return false;
    }
    let mut buf = Vec::new();
    if stream.read_to_end(&mut buf).await.is_err() {
        return false;
    }
    String::from_utf8_lossy(&buf).starts_with("HTTP/1.0 200")
        || String::from_utf8_lossy(&buf).starts_with("HTTP/1.1 200")
}
