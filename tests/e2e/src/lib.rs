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
/// The MinIO container name (`<compose project>-<service>-1`) — `docker pause`d to stall the sink's S3
/// durability in the WAL-runaway / keepalive chaos tests (PR 4.5).
const MINIO: &str = "walrus-minio-1";

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
    /// The `target/<profile>/` dir the binaries live in — kept so a crashed child can be respawned (PR 4.4).
    bins: PathBuf,
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
        sqlx::raw_sql(include_str!(
            "../../../migrations/source/0003_reload_signal.sql"
        ))
        .execute(&source)
        .await
        .context("source 0003")?;
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
            bins,
            epoch: 1,
        })
    }

    /// The source Postgres pool — for tests that need multiple concurrent sessions (overlapping txns).
    pub fn source_pool(&self) -> &sqlx::PgPool {
        &self.source
    }

    /// The control Postgres pool — for tests that read checkpoints / the manifest directly (PR 4.4).
    pub fn control_pool(&self) -> &sqlx::PgPool {
        &self.control
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

    /// **SIGKILL** the sink (PR 4.4) — the *ungraceful* crash path, NOT the SIGTERM graceful drain of PR
    /// 2.28. `tokio::process::Child::start_kill` sends `SIGKILL` (signal 9); `wait` reaps the zombie so the
    /// process is gone (and its walsender connection torn down) before we respawn.
    pub async fn kill_sink(&mut self) -> Result<()> {
        self.sink.start_kill().context("SIGKILL sink")?;
        let _ = self.sink.wait().await;
        Ok(())
    }

    /// Respawn the sink fresh and block until `/ready`. After a `SIGKILL` the source still marks the
    /// replication slot **active** until it notices the dropped connection, and the sink's resume path
    /// issues `START_REPLICATION` with no retry — so wait for the slot to go inactive first (what a real
    /// orchestrator's backoff-restart achieves), then spawn. Resume is from `confirmed_flush_lsn`.
    pub async fn restart_sink(&mut self) -> Result<()> {
        self.await_slot_inactive(Duration::from_secs(30)).await?;
        self.sink = spawn_sink(&self.bins, &self.sink_log)?;
        wait_ready("http://127.0.0.1:8130", Duration::from_secs(45))
            .await
            .context("sink /ready after restart")
    }

    /// **SIGKILL** the loader (PR 4.4) — ungraceful, distinct from the SIGTERM drain of PR 3.12. Process
    /// death releases the DuckDB file lock (the OS closes the fd) and leaves the lease row in place.
    pub async fn kill_loader(&mut self) -> Result<()> {
        self.loader.start_kill().context("SIGKILL loader")?;
        let _ = self.loader.wait().await;
        Ok(())
    }

    /// Respawn the loader fresh and block until `/ready`. It reuses `WALRUS_INSTANCE=e2e-loader`, so
    /// `acquire_lease` sees the lease as **already ours** and reclaims it immediately (no TTL wait); the
    /// DuckDB lock was freed on `SIGKILL`. Resume is from the two persisted watermarks.
    pub async fn restart_loader(&mut self) -> Result<()> {
        self.loader = spawn_loader(&self.bins, &self.duckdb_dir)?;
        wait_ready("http://127.0.0.1:8131", Duration::from_secs(45))
            .await
            .context("loader /ready after restart")
    }

    /// Poll the source until the replication slot is `active = false` (the dead walsender cleaned up), so a
    /// fresh sink can `START_REPLICATION` without hitting "replication slot is active".
    pub async fn await_slot_inactive(&self, deadline: Duration) -> Result<()> {
        let start = Instant::now();
        loop {
            let active: Option<bool> =
                sqlx::query_scalar("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
                    .bind(SLOT)
                    .fetch_optional(&self.source)
                    .await?;
            if active != Some(true) {
                return Ok(());
            }
            if start.elapsed() > deadline {
                anyhow::bail!("replication slot {SLOT} still active within {deadline:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Poll `loader_checkpoint.raw_appended_lsn` for `table` until it passes `target` — i.e. Phase A has
    /// appended the batch to `<table>_raw`, even if Phase B has not yet MERGEd it (the mid-MERGE window,
    /// where `transformed_lsn < raw_appended_lsn`). PR 4.4 uses this to crash the loader *after append,
    /// before/ during merge*.
    pub async fn await_raw_appended_past(
        &self,
        table: &str,
        target: common::Lsn,
        deadline: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        loop {
            if let Some(cp) =
                control::read_checkpoint(&self.control, self.epoch, "public", table).await?
            {
                if cp.raw_appended_lsn > target {
                    return Ok(());
                }
            }
            if start.elapsed() > deadline {
                anyhow::bail!(
                    "raw_appended_lsn for {table} never passed {target} within {deadline:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Assert the loader's DuckDB mirror `<table>_current` equals the current source `public.<table>`
    /// **row-by-row** (id + status), the effectively-once convergence check. Call after [`stop_loader`]
    /// (DuckDB is single-writer, so the mirror is read only once the loader has exited).
    pub async fn assert_mirror_equals_source(&self, table: &str) -> Result<()> {
        let src: Vec<(i32, Option<String>)> = sqlx::query_as(&format!(
            "SELECT id, status FROM public.{table} ORDER BY id"
        ))
        .fetch_all(&self.source)
        .await
        .context("read source rows")?;
        let mirror = self.duckdb_pairs(
            table,
            &format!("SELECT id, status FROM {table}_current ORDER BY id"),
        )?;
        anyhow::ensure!(
            src.len() == mirror.len(),
            "row count mismatch: source has {} rows, mirror has {}",
            src.len(),
            mirror.len()
        );
        for (s, m) in src.iter().zip(mirror.iter()) {
            anyhow::ensure!(s == m, "mirror row {m:?} != source row {s:?}");
        }
        Ok(())
    }

    /// Read `(id, status)` pairs from the loader's read-only `.duckdb` file. Call after [`stop_loader`].
    fn duckdb_pairs(&self, table: &str, sql: &str) -> Result<Vec<(i32, Option<String>)>> {
        let path = self.duckdb_dir.join(format!("{table}.duckdb"));
        let conn = duckdb::Connection::open_with_flags(
            &path,
            duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?,
        )
        .with_context(|| format!("open {}", path.display()))?;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, i32>(0)?, r.get::<_, Option<String>>(1)?))
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
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

    // ---- PR 4.5: slot-liveness chaos (S3 stall, slot status, heartbeat, health) ----------------

    /// Stall the sink's durability by pausing MinIO (`docker pause`) — every S3 PUT then hangs, so the
    /// sink cannot finish a durable flush: `confirmed_flush_lsn` freezes and the slot's `restart_lsn`
    /// is pinned, so retained WAL grows (the WAL-runaway). Pausing the **loader** would NOT do this —
    /// it doesn't own the slot; the sink advances `confirmed_flush` on its OWN S3 durability (§1.5/§1.9),
    /// so stalling S3 is the only thing that retains source WAL. The keepalive fix (PR #71) keeps the
    /// walsender connected throughout.
    pub async fn stall_s3(&self) -> Result<()> {
        docker(&["pause", MINIO]).await
    }

    /// Resume S3 (`docker unpause` MinIO) — the stalled PUT completes and the sink drains the backlog.
    pub async fn unstall_s3(&self) -> Result<()> {
        docker(&["unpause", MINIO]).await
    }

    /// The slot's `confirmed_flush_lsn` — the durable, slot-advancing LSN (moves only after S3 + manifest
    /// durability, or an idle heartbeat commit; never on a stalled flush).
    pub async fn slot_confirmed_flush(&self) -> Result<common::Lsn> {
        let s: Option<String> = sqlx::query_scalar(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(SLOT)
        .fetch_optional(&self.source)
        .await?;
        s.context("replication slot not found")?
            .parse()
            .map_err(|e| anyhow::anyhow!("parse confirmed_flush_lsn: {e:?}"))
    }

    /// The slot's `restart_lsn` — the oldest WAL the slot still needs. Follows `confirmed_flush` once a
    /// beat or durable flush advances the latter; a stuck `restart_lsn` is what retained WAL measures.
    pub async fn slot_restart_lsn(&self) -> Result<common::Lsn> {
        let s: Option<String> = sqlx::query_scalar(
            "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(SLOT)
        .fetch_optional(&self.source)
        .await?;
        s.context("replication slot not found")?
            .parse()
            .map_err(|e| anyhow::anyhow!("parse restart_lsn: {e:?}"))
    }

    /// Whether a walsender is attached to the slot (`active = true`) — proof the connection is live. A
    /// severed walsender (e.g. `wal_sender_timeout` with no keepalive) flips this to `false`.
    pub async fn slot_active(&self) -> Result<bool> {
        let active: Option<bool> =
            sqlx::query_scalar("SELECT active FROM pg_replication_slots WHERE slot_name = $1")
                .bind(SLOT)
                .fetch_optional(&self.source)
                .await?;
        Ok(active == Some(true))
    }

    /// Poll `slot_retained_bytes` until it exceeds `threshold` (the retained-WAL alert condition trips),
    /// or the deadline elapses. Watermark-based, not a fixed sleep.
    pub async fn await_retained_bytes_over(
        &self,
        threshold: i64,
        deadline: Duration,
    ) -> Result<i64> {
        let start = Instant::now();
        loop {
            let n = self.slot_retained_bytes().await?;
            if n > threshold {
                return Ok(n);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("retained WAL {n} never exceeded {threshold} within {deadline:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Poll the slot's `confirmed_flush_lsn` until it passes `target`, or the deadline elapses.
    pub async fn await_confirmed_flush_past(
        &self,
        target: common::Lsn,
        deadline: Duration,
    ) -> Result<common::Lsn> {
        let start = Instant::now();
        loop {
            let cf = self.slot_confirmed_flush().await?;
            if cf > target {
                return Ok(cf);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("confirmed_flush {cf} never passed {target} within {deadline:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// How many times the sink logged `needle` (log-scrape, like [`Harness::sink_spill_count`]).
    fn grep_sink_log(&self, needle: &str) -> usize {
        std::fs::read_to_string(&self.sink_log)
            .map(|s| s.matches(needle).count())
            .unwrap_or(0)
    }

    /// How many idle heartbeats the sink has FIRED (idle publication → wrote `walrus.heartbeat`).
    pub fn heartbeat_beats(&self) -> usize {
        self.grep_sink_log("fired idle heartbeat")
    }

    /// How many heartbeat round-trips the sink has OBSERVED (a `beat_seq` returned through the stream —
    /// the slot-consume liveness signal that feeds the `/ready` `degraded` field).
    pub fn heartbeat_roundtrips(&self) -> usize {
        self.grep_sink_log("heartbeat round-trip observed")
    }

    /// Whether the sink log contains `needle` — e.g. a reconnect/sever error the keepalive path must
    /// prevent (`"source closed the replication connection"`).
    pub fn sink_log_contains(&self, needle: &str) -> bool {
        self.grep_sink_log(needle) > 0
    }

    /// Poll [`Harness::heartbeat_roundtrips`] until it reaches `min`, or the deadline elapses.
    pub async fn await_heartbeat_roundtrip(&self, min: usize, deadline: Duration) -> Result<usize> {
        let start = Instant::now();
        loop {
            let n = self.heartbeat_roundtrips();
            if n >= min {
                return Ok(n);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("heartbeat round-trips {n} < {min} within {deadline:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// GET the sink's `/ready`, returning `(ready, degraded)`. Per `walrus-pg-sink.md` §4.3, `degraded`
    /// is a FIELD, never a readiness gate — a catching-up sink is `degraded` yet still `ready` (HTTP 200).
    /// `ready` is the HTTP-200 status (equals the body's `ready`); `degraded` is the body's field.
    pub async fn sink_ready(&self) -> Result<(bool, bool)> {
        let (ok, body) = http_get("http://127.0.0.1:8130/ready").await?;
        let v: serde_json::Value =
            serde_json::from_str(body.trim()).context("parse /ready JSON body")?;
        Ok((ok, v["degraded"].as_bool().unwrap_or(false)))
    }

    /// Whether the sink child is still running (has not exited) — proof the walsender did not sever it
    /// (a severed replication connection makes the sink's `next()` error and the process exit).
    pub fn sink_running(&mut self) -> bool {
        matches!(self.sink.try_wait(), Ok(None))
    }

    // ---- PR 4.6: total-restart (epoch bump on slot loss) ----------------------------------------

    /// The current (highest) epoch in `replication_state`, or 1 if none yet — bumps on a total-restart.
    pub async fn current_epoch(&self) -> Result<i64> {
        Ok(control::read_current_epoch(&self.control)
            .await?
            .map(|s| s.epoch)
            .unwrap_or(1))
    }

    /// Re-read the current epoch into `self.epoch` after a total-restart, so the epoch-namespaced reads
    /// (`s3_list`, `await_transformed_past`, …) target the NEW generation.
    pub async fn refresh_epoch(&mut self) -> Result<i64> {
        self.epoch = self.current_epoch().await?;
        Ok(self.epoch)
    }

    /// Poll `current_epoch` until it exceeds `from`, or the deadline elapses (a total-restart bumped it).
    pub async fn await_epoch_past(&self, from: i64, deadline: Duration) -> Result<i64> {
        let start = Instant::now();
        loop {
            let e = self.current_epoch().await?;
            if e > from {
                return Ok(e);
            }
            if start.elapsed() > deadline {
                anyhow::bail!("epoch never advanced past {from} within {deadline:?} (still {e})");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// DROP the source replication slot — the total-restart trigger: its WAL history is gone, so on
    /// restart the sink classifies the slot `Absent` and bumps the epoch. A slot cannot be dropped while a
    /// walsender is attached, so terminate any attached one and wait for inactivity first.
    pub async fn drop_slot(&self) -> Result<()> {
        sqlx::raw_sql(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{SLOT}' AND active_pid IS NOT NULL;"
        ))
        .execute(&self.source)
        .await?;
        self.await_slot_inactive(Duration::from_secs(30)).await?;
        sqlx::raw_sql(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') \
             FROM pg_replication_slots WHERE slot_name = '{SLOT}';"
        ))
        .execute(&self.source)
        .await?;
        Ok(())
    }

    /// Terminate the sink's walsender backend WITHOUT dropping the slot — a transient disconnect (a
    /// network blip). The slot survives, so a restart must RESUME from `confirmed_flush` and NOT bump the
    /// epoch (the false-positive guard, §1.8).
    pub async fn terminate_walsender(&self) -> Result<()> {
        sqlx::raw_sql(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
             WHERE slot_name = '{SLOT}' AND active_pid IS NOT NULL;"
        ))
        .execute(&self.source)
        .await?;
        Ok(())
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Best-effort kill both bins — a leaked sink pins the slot and blocks the next bootstrap.
        let _ = self.sink.start_kill();
        let _ = self.loader.start_kill();
        // Undo an S3 stall a panicking test may have left in place, or the next run's sink bootstrap
        // hangs on a paused MinIO.
        // Quiet: `unpause` errors harmlessly when the container is not paused (the common case).
        let _ = std::process::Command::new("docker")
            .args(["unpause", MINIO])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
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

/// A dependency-free HTTP GET returning `(status_is_200, body)` — used to read the `/ready` JSON body
/// (`{ready, degraded}`), which [`http_get_ok`] discards.
async fn http_get(url: &str) -> Result<(bool, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.trim_start_matches("http://");
    let (authority, path) = rest
        .split_once('/')
        .map(|(a, p)| (a, format!("/{p}")))
        .unwrap_or((rest, "/".into()));
    let mut stream = tokio::net::TcpStream::connect(authority)
        .await
        .context("connect for GET")?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: {authority}\r\n\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .context("write GET")?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .context("read GET response")?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let ok = text.starts_with("HTTP/1.0 200") || text.starts_with("HTTP/1.1 200");
    // The body follows the blank line after the headers.
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Ok((ok, body))
}

/// Run a `docker` subcommand (e.g. `pause`/`unpause` the MinIO container) and require success.
async fn docker(args: &[&str]) -> Result<()> {
    let status = Command::new("docker")
        .args(args)
        .status()
        .await
        .with_context(|| format!("run `docker {}`", args.join(" ")))?;
    anyhow::ensure!(status.success(), "`docker {}` failed", args.join(" "));
    Ok(())
}
