//! The chunk export engine (reload H1/H2, §5 step 3, PR 6.5).
//!
//! Per PK-ordered chunk: INSERT a watermark signal row → await its echo ⇒ `L_i` (PR 6.3) → SELECT
//! the chunk on this exporter's own SQL connection → write Parquet with **every row stamped
//! `commit_lsn = lsn = L_i`** → manifest row `kind='reload'`, the `reload_id`,
//! `lsn_start = lsn_end = L_i` → advance the cursor. No stream pause, no chunk buffer, no high
//! watermark. Chunks are short autocommit single statements — deliberately no long
//! `REPEATABLE READ` pinning xmin — and a crash resumes from the cursor, not row zero.
//!
//! **Why the early stamp converges (the whole proof):** every chunk row was committed by some
//! transaction at commit LSN `C`, and the chunk read happens after `L_i` was observed in-stream.
//! If `C ≤ L_i`, the chunk's copy (stamped `L_i ≥ C`) wins the loader's `(commit_lsn, lsn)` dedup
//! over the stream event at `C` — same data, so nothing is lost. If `C > L_i`, the stream event
//! outranks the chunk copy and wins. Either way the mirror converges; over-inclusion is free and
//! under-inclusion is bounded by the echo round-trip (`notes/commit-visibility-race.md`).
//!
//! **Cursor-vs-manifest ordering:** the manifest `insert_ready` and the cursor advance share ONE
//! control-pg transaction. A crash between "file durable in S3" and that commit re-exports one
//! chunk — a duplicate the dedup algebra eats. The reverse order (cursor first) would build a gap
//! nothing can heal. Duplicates are safe; gaps are not.
//!
//! For very large tables, a future CTID-range fan-out (deferred goal §3) would parallelise
//! *within* a chunk — the composition point is `export_next_chunk`'s SELECT; nothing else changes.

use crate::reload_signal::WatermarkWaiters;
use crate::sink::{FileKind, ParquetSink};
use anyhow::Context;
use common::{Kind, Lsn, Op, PgRelation, SinkMeta, TupleValue, UtcTimestamp};
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

/// What one chunk did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkOutcome {
    /// A full chunk exported; more rows may remain.
    Exported { rows: u64 },
    /// The table is drained: this chunk came back short (possibly empty). A short-but-non-empty
    /// chunk still produced a file; an empty one produced nothing. `final_lsn` is this drain
    /// probe's watermark `H`, always available since every probe echoes (and thus sets the
    /// watermark) before it can report drained.
    Drained { rows: u64, final_lsn: Lsn },
}

/// How a whole [`ChunkExporter::run`] ended (PR 6.8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The table drained at this attempt's frozen schema — the export is done. `final_lsn` is `H`:
    /// the drain probe's watermark, `>=` every chunk's `L_i` and `>= first_lsn` (LSNs are monotonic
    /// in the stream). The controller flips `export_complete(H)` (PR 6.9); the loader then flips
    /// `complete` once `transformed_lsn >= H`.
    Drained { final_lsn: Lsn },
    /// DDL bumped the table's structural `schema_version` past the frozen one between chunks: this
    /// attempt is invalid and the controller must restart it at `new_version` (reload H9).
    SchemaChanged { new_version: i64 },
}

/// Has the table's structural `schema_version` moved past the reload's `frozen` version? Returns
/// the new version if so, else `None`. Deliberately compares the REGISTRY's version — which bumps
/// only on structural DDL (a decoded Relation message, PR 2.33), so metadata-only DDL (`COMMENT
/// ON`) never trips it — and never restarts backwards (`latest < frozen` is a stale read). Pure so
/// the restart trigger unit-tests without a database.
fn version_changed(frozen: i64, latest: Option<i64>) -> Option<i64> {
    match latest {
        Some(v) if v > frozen => Some(v),
        _ => None,
    }
}

/// Everything the exporter needs beyond the reload row itself.
#[derive(Clone)]
pub struct ChunkExportConfig {
    pub chunk_rows: u64,
    pub echo_timeout: Duration,
    pub instance: String,
    pub epoch: i64,
}

/// One table's chunked export (reload §5.3). Owns a side SQL connection; talks to the consume
/// loop only through [`WatermarkWaiters`]; never touches the replication connection.
pub struct ChunkExporter {
    client: tokio_postgres::Client,
    waiters: Arc<WatermarkWaiters>,
    pool: sqlx::PgPool,
    sink: ParquetSink,
    cfg: ChunkExportConfig,
    /// The table shape at the reload's (single) schema version — from the REGISTRY, so files
    /// always match the descriptors their stamped version points at.
    rel: PgRelation,
    /// PK columns in PK-INDEX order (pg_index.indkey position) — the pagination total order.
    pk_cols: Vec<String>,
    schema_version: i64,
    reload_id: i64,
    /// Last COMPLETED chunk (from `table_reload`; 0 = fresh start).
    chunk_no: i64,
    /// Last exported PK bound as a JSON array of text values in PK-column order; `None` = start.
    cursor: Option<serde_json::Value>,
    /// Chunk 1's `L_1` once frozen (mirrors `table_reload.first_lsn`).
    first_lsn: Option<Lsn>,
}

impl ChunkExporter {
    /// Dial the side connection and resolve the export's fixed shape: the relation from the source
    /// catalog and the schema version from the registry (frozen on the reload row when resuming —
    /// every attempt is single-schema by construction; PR 6.8 enforces it across DDL).
    pub async fn connect(
        source_db_url: &str,
        pool: sqlx::PgPool,
        waiters: Arc<WatermarkWaiters>,
        sink: ParquetSink,
        cfg: ChunkExportConfig,
        req: &control::ReloadRow,
    ) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(source_db_url, NoTls)
            .await
            .context("open chunk-export SQL connection")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "chunk-export SQL connection closed");
            }
        });
        // A resumed attempt exports at its FROZEN version; a fresh one at the registry's latest.
        let schema_version = match req.schema_version {
            Some(v) => v,
            None => control::read_latest_version(
                &pool,
                req.epoch,
                &req.source_schema,
                &req.source_table,
            )
            .await
            .context("read registry version for reload")?
            .with_context(|| {
                format!(
                    "{}.{} has no schema_registry entry — is the sink streaming it?",
                    req.source_schema, req.source_table
                )
            })?,
        };
        // The export shape comes from the REGISTRY at that version — never the live catalog — so
        // every chunk file's columns match the descriptor set the loader will fetch for its
        // stamped schema_version. (A live `describe` can be ahead of the registry: DDL bumps the
        // registry only when the next Relation message decodes, and e.g. `ADD COLUMN … DEFAULT`
        // backfills without any DML. Files carrying a shape their version doesn't describe would
        // silently break Phase B's column plan.)
        let registry_row = control::read_registry(
            &pool,
            req.epoch,
            &req.source_schema,
            &req.source_table,
            schema_version,
        )
        .await
        .context("read registry row for reload shape")?
        .with_context(|| {
            format!(
                "{}.{} has no schema_registry row at version {schema_version}",
                req.source_schema, req.source_table
            )
        })?;
        let rel: PgRelation = serde_json::from_value(registry_row.columns)
            .context("registry columns snapshot is not a PgRelation")?;
        // Pagination order comes from the PRIMARY KEY INDEX (pg_index.indkey position) — not the
        // relation's attnum order, and never the PK∪replica-identity union — so the row-comparison
        // WHERE and the ORDER BY are served by the PK btree instead of a per-chunk top-N sort.
        let pk_cols = pk_columns_in_index_order(&client, &req.source_schema, &req.source_table)
            .await
            .context("read PK index column order")?;
        let registry_keys: std::collections::BTreeSet<&str> =
            rel.key_columns().into_iter().collect();
        let live_keys: std::collections::BTreeSet<&str> =
            pk_cols.iter().map(|c| c.as_str()).collect();
        if registry_keys != live_keys {
            // The live PK drifted from the registered shape (a between-attempts DDL): stop
            // without failing the row — PR 6.8's restart-on-DDL is the mechanism that reissues
            // the attempt at the new schema; until then the loud error is the breadcrumb.
            anyhow::bail!(
                "reload {}: live PK {live_keys:?} != registered key set {registry_keys:?} at                  version {schema_version} — schema drifted; restart-on-DDL (PR 6.8) reissues",
                req.reload_id
            );
        }
        Ok(ChunkExporter {
            client,
            waiters,
            pool,
            sink,
            cfg,
            rel,
            pk_cols,
            schema_version,
            reload_id: req.reload_id,
            chunk_no: req.chunk_no,
            cursor: req.cursor_pk.clone(),
            first_lsn: req.first_lsn,
        })
    }

    /// Fresh start or cursor resume (H7): loop `export_next_chunk` until a short chunk says
    /// drained. The row then simply stays `exporting`, fully drained, cursor at end — PR 6.9
    /// gives it its `export_complete` ending and the final watermark `H`.
    ///
    /// Before each chunk, re-check the table's structural version (PR 6.8 / H9): a DDL that bumped
    /// it past this attempt's frozen version returns [`RunOutcome::SchemaChanged`] so the
    /// controller restarts the attempt at the new shape. Every attempt is single-schema by
    /// construction; the loader therefore never reconciles a version change *inside* a rebuild.
    ///
    /// The tradeoff (H9): restart-on-DDL trades *wasted export work* (bounded by
    /// `reload_max_restarts`, counted by `walrus_reload_restarts_total`) for the loader never
    /// facing a half-populated table at a version boundary. Per-chunk version *tolerance* — letting
    /// chunks straddle versions and reconciling in the rebuild — was rejected: its failure mode is
    /// silent mis-reconciliation, not visible waste. Revisit only if restart churn on DDL-heavy
    /// tables becomes a *measured* problem (`single-table-reload.md` H9).
    pub async fn run(&mut self) -> anyhow::Result<RunOutcome> {
        loop {
            if let Some(new_version) = self.check_schema_still_current().await? {
                tracing::info!(
                    reload_id = self.reload_id,
                    frozen = self.schema_version,
                    new_version,
                    "reload interrupted: DDL bumped schema_version between chunks — restarting (H9)"
                );
                return Ok(RunOutcome::SchemaChanged { new_version });
            }
            match self.export_next_chunk().await? {
                ChunkOutcome::Exported { rows } => {
                    tracing::info!(
                        reload_id = self.reload_id,
                        chunk_no = self.chunk_no,
                        rows,
                        "reload chunk exported"
                    );
                }
                ChunkOutcome::Drained { rows, final_lsn } => {
                    // H = the last probe's watermark, carried by the drain outcome itself (every
                    // probe echoes and sets the watermark before it can report drained).
                    tracing::info!(
                        reload_id = self.reload_id,
                        chunk_no = self.chunk_no,
                        rows,
                        final_lsn = %final_lsn,
                        "reload export drained (controller flips export_complete)"
                    );
                    return Ok(RunOutcome::Drained { final_lsn });
                }
            }
        }
    }

    /// The per-chunk staleness check (PR 6.8): is the table still at this attempt's frozen
    /// `schema_version`? Reads the REGISTRY's latest version (control-pg, a cheap indexed MAX) —
    /// the sink's own structural-version source of truth, bumped only when a Relation message
    /// decodes — never a per-chunk catalog query against the source. Returns the new version if a
    /// structural bump landed, else `None`.
    ///
    /// A window remains between this check and the chunk's own SELECT where DDL can still slip in;
    /// that chunk exports at the old shape, but the NEXT chunk's check catches the bump and the
    /// restart throws that file away with the rest — harmless only because the restart's purge is
    /// total (H9).
    async fn check_schema_still_current(&self) -> anyhow::Result<Option<i64>> {
        let latest = control::read_latest_version(
            &self.pool,
            self.cfg.epoch,
            &self.rel.schema,
            &self.rel.name,
        )
        .await
        .context("read registry latest version for reload staleness check")?;
        Ok(version_changed(self.schema_version, latest))
    }

    /// Signal chunk `chunk_no` and wait for its echo, retrying the full
    /// subscribe → re-signal → await cycle up to [`ECHO_ATTEMPTS`] times.
    ///
    /// One timeout is NOT proof of the H11 misconfiguration — a badly lagged slot (a huge
    /// transaction ahead of the echo in the WAL) delays echoes too, and terminally failing a
    /// reload for lag would also purge its already-exported chunks. Retries ride out lag;
    /// persistent silence then fails loudly, naming both candidate causes. Each retry re-signals
    /// via DELETE + INSERT in one implicit transaction (one simple-query batch = one commit = one
    /// FRESH echo — an `ON CONFLICT DO NOTHING` would echo nothing); the same statement shape
    /// serves a crash-redone chunk. The DELETE also rides the slot; PR 6.3's routing ignores
    /// non-insert signal ops by design.
    async fn await_echo(&mut self, chunk_no: i64) -> anyhow::Result<crate::reload_signal::Echo> {
        const ECHO_ATTEMPTS: u32 = 3;
        for attempt in 1..=ECHO_ATTEMPTS {
            // Subscribe-then-insert (PR 6.3): the waiter must exist before the echo can arrive.
            let rx = self.waiters.subscribe(self.reload_id, chunk_no);
            let signalled_at = std::time::Instant::now();
            self.client
                .batch_execute(&format!(
                    "DELETE FROM walrus.reload_signal WHERE reload_id = {r} AND chunk_no = {c}; \
                     INSERT INTO walrus.reload_signal (reload_id, chunk_no) VALUES ({r}, {c});",
                    r = self.reload_id,
                    c = chunk_no,
                ))
                .await
                .context("insert reload watermark signal")?;
            match tokio::time::timeout(self.cfg.echo_timeout, rx).await {
                Ok(Ok(echo)) => {
                    // The echo round-trip: signal INSERT → decoded-commit echo (PR 6.11). Its p99
                    // bounds reload throughput and tracks end-to-end decode latency.
                    common::metrics::record_reload_echo_wait(signalled_at.elapsed().as_secs_f64());
                    return Ok(echo);
                }
                Ok(Err(_)) => {
                    anyhow::bail!("echo waiter superseded (a newer subscriber replaced it)")
                }
                Err(_) => tracing::warn!(
                    reload_id = self.reload_id,
                    chunk_no,
                    attempt,
                    timeout = ?self.cfg.echo_timeout,
                    "no echo within the timeout; re-signalling"
                ),
            }
        }
        // H11's silent failure, made loud — after enough patience that plain decode lag has had
        // its chance. The fail() purges this reload's staged chunks (6.1); a later re-request
        // re-exports them.
        let reason = format!(
            "no echo after {ECHO_ATTEMPTS} attempts × {:?} on chunk {chunk_no} — either \
             walrus.reload_signal is not in the publication \
             (migrations/source/0003_reload_signal.sql) or the replication stream is severely \
             lagged",
            self.cfg.echo_timeout
        );
        let mut conn = self.pool.acquire().await?;
        control::reload::fail(&mut conn, self.reload_id, &reason).await?;
        common::metrics::record_reload_failed(&format!("{}.{}", self.rel.schema, self.rel.name));
        anyhow::bail!("reload {} failed: {reason}", self.reload_id);
    }

    /// One chunk: subscribe → signal → echo ⇒ `L_n` → SELECT the next PK slice → stamped Parquet
    /// → one control-pg txn { manifest row + cursor advance }. Returns the outcome; a chunk
    /// shorter than `chunk_rows` means the table is drained.
    pub async fn export_next_chunk(&mut self) -> anyhow::Result<ChunkOutcome> {
        let chunk_no = self.chunk_no + 1;
        let echo = self.await_echo(chunk_no).await?;
        // The probe's watermark — carried into every `ChunkOutcome`'s `final_lsn` below so even an
        // empty drain (no file, no cursor advance) reports a valid `H` (>= first_lsn and every
        // chunk `L_i`, LSNs being monotonic). PR 6.9.
        let watermark = echo.commit_lsn;

        // The chunk read: one short autocommit statement, strictly after the echo was observed.
        let rows = self
            .client
            .query(&self.chunk_sql(), &[])
            .await
            .context("reload chunk SELECT")?;
        if rows.is_empty() {
            // Nothing at all past the cursor — drained with no file (the signal row for this
            // empty probe is harmless; its echo resolved above).
            return Ok(ChunkOutcome::Drained {
                rows: 0,
                final_lsn: watermark,
            });
        }

        // Stamp + write: every row `commit_lsn = lsn = L_i` (see the module doc for the proof).
        let cached = crate::relcache::RelationCache::default()
            .upsert_from_relation(self.rel.clone(), self.schema_version)
            .context("build Arrow schema for reload chunk")?;
        let mut batcher = crate::batch::TableBatcher::new(
            cached,
            crate::batch::BatchTriggers {
                max_rows: u64::MAX, // one file per chunk; chunk_rows bounds the SELECT
                max_bytes: u64::MAX,
                max_fill: Duration::from_secs(3600),
            },
            Arc::new(crate::batch::SystemClock),
        )
        .context("create reload chunk batcher")?;
        for row in &rows {
            batcher.push(self.chunk_meta(watermark), &row_to_tuple(row, &self.rel));
        }
        batcher
            .on_commit(watermark, UtcTimestamp::now())
            .context("promote reload chunk rows at L_i")?;
        let sealed = batcher.seal().context("seal reload chunk")?;
        let obj = self
            .sink
            .put_with_kind(sealed, FileKind::Reload)
            .await
            .context("PUT reload chunk Parquet")?;

        // The cursor comes from the LAST ROW of the chunk just written — never a separate MAX()
        // query (racy). Values stay in their text output form (precision-safe for bigint PKs).
        let last = &rows[rows.len() - 1];
        let cursor = cursor_from_row(&self.rel, &self.pk_cols, last)
            .context("build reload cursor from last chunk row")?;

        // ONE control-pg transaction: manifest row + cursor advance (see the module doc).
        let mut tx = self.pool.begin().await.context("begin chunk commit txn")?;
        crate::manifest::record_ready_with_reload(
            &mut *tx,
            self.cfg.epoch,
            &obj,
            Some(self.reload_id),
        )
        .await
        .context("record reload chunk manifest row")?;
        control::reload::advance_cursor(
            &mut *tx,
            self.reload_id,
            chunk_no,
            &cursor,
            watermark,
            self.schema_version,
        )
        .await
        .context("advance reload cursor")?;
        tx.commit().await.context("commit chunk manifest+cursor")?;

        self.chunk_no = chunk_no;
        self.cursor = Some(cursor);
        if self.first_lsn.is_none() {
            self.first_lsn = Some(watermark);
        }

        let n = rows.len() as u64;
        // One chunk file exported (PR 6.11): bump the per-table chunk + row counters.
        common::metrics::record_reload_chunk(&format!("{}.{}", self.rel.schema, self.rel.name), n);
        if n < self.cfg.chunk_rows {
            Ok(ChunkOutcome::Drained {
                rows: n,
                final_lsn: watermark,
            })
        } else {
            Ok(ChunkOutcome::Exported { rows: n })
        }
    }

    /// `SELECT "c1"::text, … FROM t [WHERE (pk…) > (cursor…)] ORDER BY pk… LIMIT n` — keyset
    /// pagination via row comparison over the PK-INDEX column order: index-friendly and
    /// composite-safe (never OFFSET).
    fn chunk_sql(&self) -> String {
        continuation_sql(
            &self.rel,
            &self.pk_cols,
            self.cursor.as_ref(),
            self.cfg.chunk_rows,
        )
    }

    fn chunk_meta(&self, watermark: Lsn) -> SinkMeta {
        SinkMeta {
            op: Op::Insert,
            // The stamp: chunk rows carry the chunk's low watermark as BOTH LSNs, so any
            // overlapping stream event (commit LSN > L_i) wins the loader's dedup.
            lsn: watermark,
            commit_lsn: Lsn::ZERO, // patched to L_i by the batcher's on_commit
            commit_ts: UtcTimestamp::now(),
            xid: 0,
            epoch: self.cfg.epoch,
            batch_id: String::new(),
            schema_version: self.schema_version,
            source_schema: self.rel.schema.clone(),
            source_table: self.rel.name.clone(),
            kind: Kind::Reload,
            unchanged_toast: vec![],
            sink_instance: self.cfg.instance.clone(),
            sink_processed_at: UtcTimestamp::now(),
        }
    }
}

/// The PRIMARY KEY's columns in INDEX order (`pg_index.indkey` position) — the order the PK
/// btree can actually serve for keyset pagination. Deliberately PK-only: the relation shape's
/// `is_key` union (PK ∪ replica identity) matches no single index.
async fn pk_columns_in_index_order(
    client: &tokio_postgres::Client,
    schema: &str,
    table: &str,
) -> anyhow::Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT a.attname
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
             WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
             ORDER BY k.ord",
            &[&schema, &table],
        )
        .await
        .context("read pg_index PK column order")?;
    anyhow::ensure!(!rows.is_empty(), "{schema}.{table} has no primary key");
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// The keyset-pagination SELECT. The cursor is a JSON array of text values in PK-column order;
/// its literals are left untyped (`'…'`) so Postgres coerces them to the PK column types in the
/// row comparison — no per-type casting table needed. PK columns and their order come from the
/// relation shape — never hardcoded.
///
/// The key columns are TABLE-QUALIFIED (`_src."id"`) everywhere: the SELECT list's `::text` casts
/// keep the original output names, and a bare `ORDER BY "id"` would bind to that TEXT output
/// column (Postgres resolves output names first) — text-ordered pages with int-compared
/// continuation silently skip and truncate. The qualifier pins both the WHERE and the ORDER BY to
/// the native-typed table column.
fn continuation_sql(
    rel: &PgRelation,
    pk_cols: &[String],
    cursor: Option<&serde_json::Value>,
    limit: u64,
) -> String {
    let cols: Vec<String> = rel
        .columns
        .iter()
        .map(|c| format!("\"{}\"::text", c.name))
        .collect();
    let key_cols: Vec<String> = pk_cols.iter().map(|c| format!("_src.\"{c}\"")).collect();
    let mut sql = format!(
        "SELECT {} FROM \"{}\".\"{}\" AS _src",
        cols.join(", "),
        rel.schema,
        rel.name
    );
    if let Some(serde_json::Value::Array(values)) = cursor {
        let literals: Vec<String> = values
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => sql_lit(s),
                other => sql_lit(&other.to_string()),
            })
            .collect();
        sql.push_str(&format!(
            " WHERE ({}) > ({})",
            key_cols.join(", "),
            literals.join(", ")
        ));
    }
    sql.push_str(&format!(" ORDER BY {} LIMIT {limit}", key_cols.join(", ")));
    sql
}

/// The last row's PK values, in PK-INDEX order, as their text output form.
fn cursor_from_row(
    rel: &PgRelation,
    pk_cols: &[String],
    row: &tokio_postgres::Row,
) -> anyhow::Result<serde_json::Value> {
    let values: Vec<serde_json::Value> = pk_cols
        .iter()
        .map(|key| {
            let idx = rel
                .columns
                .iter()
                .position(|c| &c.name == key)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "PK column {key:?} not found in relation {}.{}",
                        rel.schema,
                        rel.name
                    )
                })?;
            Ok(match row.get::<_, Option<String>>(idx) {
                Some(s) => serde_json::Value::String(s),
                None => serde_json::Value::Null, // PK columns are NOT NULL; defensive only
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(serde_json::Value::Array(values))
}

fn row_to_tuple(row: &tokio_postgres::Row, rel: &PgRelation) -> Vec<TupleValue> {
    (0..rel.columns.len())
        .map(|i| match row.get::<_, Option<String>>(i) {
            Some(s) => TupleValue::Text(s),
            None => TupleValue::Null,
        })
        .collect()
}

/// A SQL string literal (single-quoted, quotes doubled).
fn sql_lit(s: &str) -> String {
    format!("'{}'", common::sql::sql_literal(s))
}

#[cfg(test)]
#[path = "reload_export_test.rs"]
mod tests;
