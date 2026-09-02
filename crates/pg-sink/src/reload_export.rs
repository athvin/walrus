//! The chunk export engine (reload H1/H2, §5 step 3).
//!
//! Each attempt first appends a start event and waits for its decoded commit `F`. It then walks the
//! table in PK order inside one read-only `REPEATABLE READ` transaction, writing every baseline row
//! with `commit_lsn = lsn = F`. After committing that source snapshot it appends an end event `H`;
//! decode force-flushes this table's WAL and persists the end marker before resolving the exporter.
//! A PostgreSQL snapshot is connection-local: an adopted attempt without durable H is superseded by
//! a fresh attempt/F rather than resuming its cursor against a different snapshot.
//!
//! **Why the early stamp converges (the whole proof):** one consistent snapshot `S`, with
//! `F < S < H`, supplies a complete row anchor. WAL changes in `(F, H]` outrank the F-stamped
//! baseline, including old-key deletes/new-key upserts and unchanged-TOAST updates. Transactions
//! after `H` stay queued for the normal live path after the atomic shadow cutover. TRUNCATE is a
//! table-level WAL boundary, so the overlay cannot leave ghosts.
//!
//! **Cursor-vs-manifest ordering:** the manifest `insert_ready` and the cursor advance share ONE
//! control-pg transaction. A crash between "file durable in S3" and that commit re-exports one
//! chunk — a duplicate the dedup algebra eats. The reverse order (cursor first) would build a gap
//! nothing can heal. Duplicates are safe; gaps are not.
//!
//! For very large tables, a future CTID-range fan-out (deferred goal §3) would parallelise
//! *within* a chunk — the composition point is `export_next_chunk`'s SELECT; nothing else changes.

use crate::reload_event::{FenceEmission, FencePhase, FenceWaiters};
use crate::sink::{FileKind, ParquetSink};
use anyhow::Context;
use common::sql::{SqlIdent, SqlStrExt};
use common::{
    EpochNo, Kind, Lsn, Op, PgRelation, ReloadId, SchemaVersionNo, SinkMeta, TupleValue,
    UtcTimestamp,
};
use std::fmt::Write as _;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;
use tracing::Instrument as _;
use uuid::Uuid;

/// Bound how long one queued schema-fence lock may wait behind structural DDL before it retries.
const FENCE_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// What one chunk did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkOutcome {
    /// A full chunk exported; more rows may remain.
    Exported { rows: u64 },
    /// The table is drained: this chunk came back short (possibly empty). A short-but-non-empty
    /// chunk still produced a file; an empty one produced nothing. Lifecycle progress comes from
    /// explicit baseline/end markers, never from the presence of this file.
    Drained { rows: u64 },
}

/// How a whole [`ChunkExporter::run`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The table drained at this attempt's frozen schema — the export is done. `final_lsn` is `H`:
    /// the explicit end-event commit, `>=` the shared baseline `F`. The controller flips
    /// `export_complete(H)`; the loader then flips
    /// `complete` once `transformed_lsn >= H`.
    Drained { final_lsn: Lsn },
    /// DDL bumped the table's structural `schema_version` past the frozen one between chunks: this
    /// attempt is invalid and the controller must restart it at `new_version` (reload H9).
    SchemaChanged { new_version: SchemaVersionNo },
    /// This exporter was adopted after losing ownership of its connection-local source snapshot.
    /// Even a zero-chunk cursor cannot safely reuse the attempt: the expired prior exporter may
    /// still emit H from an older snapshot. The controller atomically fails/purges this attempt and
    /// creates a fresh lease-carrying successor with a fresh F.
    SnapshotLost,
}

/// A structural change discovered while constructing an exporter or establishing its initial
/// fence. This is a control-flow outcome for the controller's bounded DDL restart loop, not a
/// terminal exporter failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("source shape changed before reload export started (new schema version {new_version})")]
pub struct ConnectSchemaChanged {
    /// Registry version that supersedes the attempt's frozen version.
    pub new_version: SchemaVersionNo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndFenceOutcome {
    Durable(Lsn),
    SchemaChanged(SchemaVersionNo),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotExportOutcome {
    Drained { rows: u64 },
    SchemaChanged { new_version: SchemaVersionNo },
}

/// Has the table's structural `schema_version` moved past the reload's `frozen` version? Returns
/// the new version if so, else `None`. Deliberately compares the REGISTRY's version — which bumps
/// only on structural DDL (a decoded Relation message), so metadata-only DDL (`COMMENT
/// ON`) never trips it — and never restarts backwards (`latest < frozen` is a stale read). Pure so
/// the restart trigger unit-tests without a database.
fn version_changed(
    frozen: SchemaVersionNo,
    latest: Option<SchemaVersionNo>,
) -> Option<SchemaVersionNo> {
    match latest {
        Some(v) if v > frozen => Some(v),
        _ => None,
    }
}

/// Everything the exporter needs beyond the reload row itself.
#[derive(Clone, Debug)]
pub struct ChunkExportConfig {
    /// Rows per chunk SELECT. Non-zero, or the export would make no progress.
    pub chunk_rows: NonZeroU64,
    /// How long a start/end fence waits to return through the decode loop.
    pub echo_timeout: Duration,
    /// This pod's identity, written as the reload's `lease_holder`.
    pub instance: String,
    /// The generation the exported chunks are stamped with.
    pub epoch: EpochNo,
    /// The logical publication whose complete target coverage both fences revalidate.
    pub publication_name: String,
}

/// One table's chunked export (reload §5.3). Owns a side SQL connection; talks to the consume
/// loop only through [`FenceWaiters`]; never touches the replication connection.
#[derive(Debug)]
pub struct ChunkExporter {
    client: tokio_postgres::Client,
    fence_waiters: Arc<FenceWaiters>,
    pool: sqlx::PgPool,
    sink: ParquetSink,
    cfg: ChunkExportConfig,
    /// The table shape at the reload's (single) schema version — from the REGISTRY, so files
    /// always match the descriptors their stamped version points at.
    rel: PgRelation,
    /// The `table` metric label (`"<schema>.<table>"`) for this export, precomputed from `rel`.
    series: String,
    /// PK columns in PK-INDEX order (pg_index.indkey position) — the pagination total order.
    pk_cols: Vec<String>,
    schema_version: SchemaVersionNo,
    reload_id: ReloadId,
    /// Last COMPLETED chunk (from `table_reload`; 0 = fresh start).
    chunk_no: i64,
    /// Last exported PK bound as a JSON array of text values in PK-column order; `None` = start.
    cursor: Option<serde_json::Value>,
    /// One safe lower fence shared by every chunk in the consistent baseline snapshot.
    start_lsn: Lsn,
    /// Stable source request correlation; synthesized deterministically for legacy callers.
    request_id: Uuid,
    /// Whether this connection currently owns the one repeatable-read dump transaction. Public
    /// one-chunk test/diagnostic calls share this state with [`Self::run`], so no caller can
    /// accidentally build one attempt from multiple source snapshots.
    snapshot_open: bool,
}

impl ChunkExporter {
    /// Dial the side connection and resolve the export's fixed shape: the relation from the source
    /// catalog and the schema version from the registry (frozen on the reload row when resuming —
    /// every attempt is single-schema by construction and stays so across DDL).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if the source SQL connection, relation description, registry lookup,
    /// frozen schema resolution, or primary-key discovery fails.
    pub async fn connect(
        source_db_url: &str,
        pool: sqlx::PgPool,
        fence_waiters: Arc<FenceWaiters>,
        sink: ParquetSink,
        cfg: ChunkExportConfig,
        req: &control::ReloadRow,
    ) -> anyhow::Result<Self> {
        let (mut client, connection) = tokio_postgres::connect(source_db_url, NoTls)
            .await
            .context("open chunk-export SQL connection")?;
        // `tokio::spawn` starts its task with an EMPTY span stack — the caller's span is
        // thread-local to whoever polls it, not something a spawn inherits — so this driver's one
        // warning would name no reload at all. `in_current_span` copies the exporter task's span
        // (`reload::spawn_exporter`) onto the new task, which is what makes a dropped side
        // connection attributable to the export it was dialled for.
        let driver = async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "chunk-export SQL connection closed");
            }
        };
        tokio::spawn(driver.in_current_span());
        // The registry chain (control-pg) and the PK-index read (the SOURCE catalog) hit different
        // servers and neither consumes the other's output, so this dial-up costs the slower of the
        // two instead of their sum — `main::establish_stream`'s argument, paid on every attempt and
        // again on every DDL restart. Both branches are terminal-on-error and fail fast: whichever
        // resolves first drops the other, and either way the exporter never connects.
        let ((rel, schema_version), (live_relation, pk_cols)) = tokio::try_join!(
            async {
                // A resumed attempt exports at its FROZEN version; a fresh one at the
                // registry's latest.
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
                // The export shape comes from the REGISTRY at that version — never the live
                // catalog — so every chunk file's columns match the descriptor set the loader
                // will fetch for its stamped schema_version. (A live `describe` can be ahead of
                // the registry: DDL bumps the registry only when the next Relation message
                // decodes, and e.g. `ADD COLUMN … DEFAULT` materializes values without any DML. Files
                // carrying a shape their version doesn't describe would silently break Phase B's
                // column plan.)
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
                anyhow::Ok((rel, schema_version))
            },
            // Pagination order comes from the PRIMARY KEY INDEX (pg_index.indkey position) — not
            // the relation's attnum order, and never the PK∪replica-identity union — so the
            // row-comparison WHERE and the ORDER BY are served by the PK btree instead of a
            // per-chunk top-N sort.
            async {
                tokio::try_join!(
                    async {
                        crate::source_catalog::describe_source_relation(
                            &client,
                            &req.source_schema,
                            &req.source_table,
                        )
                        .await
                        .context("describe live relation for reload")
                    },
                    async {
                        pk_columns_in_index_order(&client, &req.source_schema, &req.source_table)
                            .await
                            .context("read PK index column order")
                    },
                )
            },
        )?;
        if live_relation != rel {
            return Err(ConnectSchemaChanged {
                new_version: await_schema_change(
                    &pool,
                    req.epoch,
                    &req.source_schema,
                    &req.source_table,
                    schema_version,
                    req.reload_id,
                    cfg.echo_timeout,
                )
                .await?,
            }
            .into());
        }
        validate_export_keys(&rel, &pk_cols)
            .with_context(|| format!("validate reload {} export keys", req.reload_id))?;
        let request_id = req
            .source_request_id
            .or(req.parent_request_id)
            .with_context(|| {
                format!(
                    "reload {} has no durable source-fence request namespace",
                    req.reload_id
                )
            })?;
        let start_lsn = match req.start_lsn {
            Some(lsn) => lsn,
            None => {
                establish_start_fence(
                    &mut client,
                    &pool,
                    &fence_waiters,
                    &cfg.publication_name,
                    request_id,
                    req,
                    &rel,
                    schema_version,
                    cfg.echo_timeout,
                )
                .await?
            }
        };
        Ok(ChunkExporter {
            client,
            fence_waiters,
            pool,
            sink,
            cfg,
            series: format!("{}.{}", rel.schema, rel.name),
            rel,
            pk_cols,
            schema_version,
            reload_id: req.reload_id,
            chunk_no: req.chunk_no,
            cursor: req.cursor_pk.clone(),
            start_lsn,
            request_id,
            snapshot_open: false,
        })
    }

    /// Walk one fresh, connection-owned snapshot until a short chunk says drained. The row then
    /// stays `exporting`, fully drained with its cursor at the end, until the controller records
    /// `export_complete` and the final watermark `H`. An adopted call may recover durable H, but
    /// otherwise returns [`RunOutcome::SnapshotLost`] before opening or scanning a new snapshot.
    ///
    /// Before each chunk, re-check the table's structural version (H9): a DDL that bumped
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
    ///
    /// ## Cancel safety
    ///
    /// **Not cancel-safe within a chunk; safe to drop between them.** The controller deliberately
    /// races this future in [`lease_guarded_export`](crate::reload::lease_guarded_export), so a drop
    /// mid-[`Self::export_next_chunk`] is a normal shutdown/lost-lease outcome rather than a bug: the
    /// chunk's manifest row and its cursor advance share ONE control-pg transaction, so an
    /// uncommitted chunk simply never happened and the row stays `exporting`; adoption terminally
    /// supersedes/purges that attempt and starts a fresh fenced successor. A drop between that
    /// chunk's S3 PUT and the commit orphans the object exactly as
    /// [`crate::consume::flush_batch_kind`] does, and the successor regenerates it.
    /// What a drop can never recover is partial progress *inside* one chunk — those rows live in this
    /// future's batcher — which is why the cursor only ever moves at a committed chunk boundary.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if schema checks, chunk export, echo handling, control transitions,
    /// or final export completion fails.
    pub async fn run(&mut self, lost_snapshot_ownership: bool) -> anyhow::Result<RunOutcome> {
        // H is the irreversible upper boundary for this attempt. Decode persists it only after all
        // target WAL through H is durable, but the exporter can crash in the tiny window before its
        // controller flips `exporting -> export_complete`. An adopter must finish from that durable
        // fact: querying the source again could copy a row committed *after* H and stamp it at F,
        // incorrectly pulling future state into the H rebuild.
        //
        // Validate the whole persisted fence identity before trusting H, then repeat the same
        // post-H registry check used by the normal drain path. Neither operation reads table data.
        if let Some(final_lsn) = self.recover_durable_end().await? {
            if let Some(new_version) = self.check_schema_still_current().await? {
                return Ok(RunOutcome::SchemaChanged { new_version });
            }
            tracing::info!(
                reload_id = %self.reload_id,
                final_lsn = %final_lsn,
                "reload recovered durable end marker (controller flips export_complete)"
            );
            return Ok(RunOutcome::Drained { final_lsn });
        }

        // A PostgreSQL snapshot belongs to one backend transaction; neither the PK cursor nor the
        // lease can resurrect it after adoption. This applies even at chunk zero: the expired old
        // exporter could still finish an empty/short snapshot and emit H while this adopter starts
        // a later one. Durable H above is the only safe same-attempt recovery fact.
        if lost_snapshot_ownership {
            return Ok(RunOutcome::SnapshotLost);
        }

        if !self.snapshot_open {
            self.begin_dump_snapshot().await?;
        }
        let snapshot_outcome = match self.export_snapshot().await {
            Ok(outcome) => outcome,
            Err(export_error) => {
                if let Err(rollback_error) = self.rollback_dump_snapshot().await {
                    return Err(export_error.context(format!(
                        "rolling back the failed reload snapshot also failed: {rollback_error:#}"
                    )));
                }
                return Err(export_error);
            }
        };
        match snapshot_outcome {
            SnapshotExportOutcome::SchemaChanged { new_version } => {
                self.rollback_dump_snapshot().await?;
                return Ok(RunOutcome::SchemaChanged { new_version });
            }
            SnapshotExportOutcome::Drained { rows } => {
                // H must follow the snapshot commit. Releasing the source transaction first also
                // lets queued DDL win its table lock; H's live-shape fence will then reject this
                // attempt instead of publishing a boundary for an obsolete shape.
                self.client
                    .batch_execute("COMMIT")
                    .await
                    .context("commit repeatable-read reload snapshot before H")?;
                self.snapshot_open = false;
                let final_lsn = match self.establish_end_fence().await? {
                    EndFenceOutcome::Durable(lsn) => lsn,
                    EndFenceOutcome::SchemaChanged(new_version) => {
                        return Ok(RunOutcome::SchemaChanged { new_version });
                    }
                };
                // The catalog shape check inside H catches ordinary column/OID changes. A
                // topology-only ALTER TABLE (for example ATTACH/DETACH PARTITION) can leave
                // that shape byte-for-byte identical while changing which rows a published
                // root contains. Its ddl_audit row precedes H in the same WAL stream, so once
                // H's echo resolves the committed registry version is authoritative. Reject
                // the attempt if that boundary crossed any structural DDL.
                if let Some(new_version) = self.check_schema_still_current().await? {
                    return Ok(RunOutcome::SchemaChanged { new_version });
                }
                tracing::info!(
                    reload_id = %self.reload_id,
                    chunk_no = self.chunk_no,
                    rows,
                    final_lsn = %final_lsn,
                    "reload export drained (controller flips export_complete)"
                );
                return Ok(RunOutcome::Drained { final_lsn });
            }
        }
    }

    /// Walk all keyset chunks under the source transaction opened by
    /// [`Self::begin_dump_snapshot`]. This method never commits H; its caller owns rollback/commit.
    async fn export_snapshot(&mut self) -> anyhow::Result<SnapshotExportOutcome> {
        loop {
            if let Some(new_version) = self.check_schema_still_current().await? {
                tracing::info!(
                    reload_id = %self.reload_id,
                    frozen = %self.schema_version,
                    new_version = %new_version,
                    "reload interrupted: DDL bumped schema_version between chunks — restarting (H9)"
                );
                return Ok(SnapshotExportOutcome::SchemaChanged { new_version });
            }
            match self.export_next_chunk().await? {
                ChunkOutcome::Exported { rows } => {
                    tracing::info!(
                        reload_id = %self.reload_id,
                        chunk_no = self.chunk_no,
                        rows,
                        "reload chunk exported"
                    );
                }
                ChunkOutcome::Drained { rows } => {
                    if let Some(new_version) = self.check_schema_still_current().await? {
                        return Ok(SnapshotExportOutcome::SchemaChanged { new_version });
                    }
                    return Ok(SnapshotExportOutcome::Drained { rows });
                }
            }
        }
    }

    /// Open the one consistent source snapshot used by every chunk in this attempt. Turning row
    /// security off is deliberate: for a role subject to an RLS policy PostgreSQL errors instead
    /// of silently exporting a policy-filtered partial table.
    async fn begin_dump_snapshot(&mut self) -> anyhow::Result<()> {
        let begin = self
            .client
            .batch_execute(
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; \
                 SET LOCAL row_security = off",
            )
            .await;
        if let Err(begin_error) = begin {
            let rollback_error = self.client.batch_execute("ROLLBACK").await.err();
            let error = anyhow::Error::new(begin_error)
                .context("begin read-only repeatable-read reload snapshot with RLS disabled");
            if let Some(rollback_error) = rollback_error {
                return Err(error.context(format!(
                    "rolling back the failed snapshot setup also failed: {rollback_error}"
                )));
            }
            return Err(error);
        }
        self.snapshot_open = true;
        Ok(())
    }

    async fn rollback_dump_snapshot(&mut self) -> anyhow::Result<()> {
        self.client
            .batch_execute("ROLLBACK")
            .await
            .context("roll back repeatable-read reload snapshot")?;
        self.snapshot_open = false;
        Ok(())
    }

    /// Recover the crash seam after decode persisted H but before the controller recorded
    /// `export_complete(H)`. An end marker is trusted only when the durable reload row, F marker,
    /// and H marker all name this exact attempt, table, schema version, and request namespace.
    async fn recover_durable_end(&self) -> anyhow::Result<Option<Lsn>> {
        let markers = control::reload::read_markers(&self.pool, self.reload_id)
            .await
            .context("read reload markers for end-fence recovery")?;
        if !markers
            .iter()
            .any(|marker| marker.kind == control::ReloadMarkerKind::End)
        {
            return Ok(None);
        }

        let reload = control::reload::get(&self.pool, self.reload_id)
            .await
            .context("read reload row for end-fence recovery")?
            .with_context(|| {
                format!(
                    "reload {} disappeared while recovering its durable end marker",
                    self.reload_id
                )
            })?;
        validate_durable_end(
            &reload,
            &markers,
            self.cfg.epoch,
            &self.rel,
            self.schema_version,
            self.start_lsn,
            self.request_id,
        )
    }

    /// The per-chunk staleness check: is the table still at this attempt's frozen
    /// `schema_version`? Reads the REGISTRY's latest version (control-pg, a cheap indexed MAX) —
    /// the sink's own structural-version source of truth, bumped only when a Relation message
    /// decodes — never a per-chunk catalog query against the source. Returns the new version if a
    /// structural bump landed, else `None`.
    ///
    /// A window remains between this check and the chunk's own SELECT where DDL can still slip in;
    /// that chunk exports at the old shape, but the NEXT chunk's check catches the bump and the
    /// restart throws that file away with the rest — harmless only because the restart's purge is
    /// total (H9).
    async fn check_schema_still_current(&self) -> anyhow::Result<Option<SchemaVersionNo>> {
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

    /// Emit the terminal source event and return only after decode has force-flushed the target and
    /// persisted its durable end marker.
    async fn establish_end_fence(&mut self) -> anyhow::Result<EndFenceOutcome> {
        match crate::reload_event::emit_fence(
            &mut self.client,
            &self.fence_waiters,
            &self.cfg.publication_name,
            self.request_id,
            self.reload_id,
            FencePhase::End,
            &self.rel.schema,
            &self.rel.name,
            &self.rel,
            self.schema_version,
            FENCE_LOCK_TIMEOUT,
            self.cfg.echo_timeout,
        )
        .await?
        {
            FenceEmission::Observed(echo) => {
                // Decode resolves an end fence only after the target flush. Recording again here is
                // an idempotent crash seam and keeps this exporter usable with a rolling decoder.
                control::reload::record_end_marker(
                    &self.pool,
                    self.reload_id,
                    echo.commit_lsn,
                    control::ReloadFenceIdentity {
                        request_id: Some(self.request_id),
                        source_schema: &self.rel.schema,
                        source_table: &self.rel.name,
                        schema_version: self.schema_version,
                    },
                )
                .await
                .context("record durable reload end marker")?;
                Ok(EndFenceOutcome::Durable(echo.commit_lsn))
            }
            FenceEmission::AlreadyExists => {
                let lsn = wait_for_marker(
                    &self.pool,
                    self.reload_id,
                    control::ReloadMarkerKind::End,
                    self.cfg.echo_timeout,
                )
                .await?;
                Ok(EndFenceOutcome::Durable(lsn))
            }
            FenceEmission::SchemaChanged => Ok(EndFenceOutcome::SchemaChanged(
                await_schema_change(
                    &self.pool,
                    self.cfg.epoch,
                    &self.rel.schema,
                    &self.rel.name,
                    self.schema_version,
                    self.reload_id,
                    self.cfg.echo_timeout,
                )
                .await?,
            )),
        }
    }

    /// One chunk: ensure the attempt's single repeatable-read transaction is open, SELECT the next
    /// PK slice, write Parquet stamped at the fixed `F`, then commit one control-pg transaction
    /// containing the manifest row and cursor advance. The source transaction remains open for the
    /// next call or [`Self::run`]. A chunk shorter than `chunk_rows` means the table is drained.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] when the source slice cannot be read,
    /// Arrow/Parquet/S3 export fails, or the manifest-plus-cursor control transaction cannot commit.
    pub async fn export_next_chunk(&mut self) -> anyhow::Result<ChunkOutcome> {
        // Keep the public one-chunk seam honest: integration tests and diagnostics may step an
        // exporter manually, but every such step must still belong to the exact snapshot that a
        // later `run()` continues. Production `run()` has already opened it, making this a no-op.
        if !self.snapshot_open {
            self.begin_dump_snapshot().await?;
        }
        let chunk_no = self.chunk_no + 1;
        let watermark = self.start_lsn;

        // The chunk read stays inside this attempt's one repeatable-read snapshot, established
        // strictly after the start-fence echo was observed.
        let chunk_sql = self.chunk_sql()?;
        let rows = self
            .client
            .query(&chunk_sql, &[])
            .await
            .context("reload chunk SELECT")?;
        // The chunk's last row is its next cursor (built once the file is written, below), and its
        // absence is the drain: nothing at all past the cursor, so no file (the signal row for this
        // empty probe is harmless; its echo resolved above).
        let Some(last) = rows.last() else {
            return Ok(ChunkOutcome::Drained { rows: 0 });
        };

        // Stamp + write: every row `commit_lsn = lsn = F` (see the module doc for the proof).
        let cached = crate::relcache::RelationCache::default()
            .upsert_from_relation(self.rel.clone(), self.schema_version)
            .context("build Arrow schema for reload chunk")?;
        let mut batcher = crate::batch::TableBatcher::new(
            cached,
            crate::batch::BatchTriggers {
                max_rows: NonZeroU64::MAX, // one file per chunk; chunk_rows bounds the SELECT
                max_bytes: NonZeroU64::MAX,
                max_fill: Duration::from_secs(3600),
            },
            Arc::new(crate::batch::SystemClock),
        )
        .context("create reload chunk batcher")?;
        // One tuple buffer for the whole chunk: `push` copies the row into the batcher, so refilling
        // this scratch keeps its capacity instead of allocating (and dropping) a `Vec` per row.
        let mut tuple: Vec<TupleValue> = Vec::with_capacity(self.rel.columns.len());
        for row in &rows {
            read_row_into(row, &self.rel, &mut tuple);
            batcher.push(self.chunk_meta(watermark), &tuple);
        }
        batcher
            .on_commit(watermark, UtcTimestamp::now())
            .context("promote reload chunk rows at the baseline fence")?;
        let sealed = batcher.seal().context("seal reload chunk")?;
        let obj = self
            .sink
            .put_with_kind(sealed, FileKind::Reload)
            .await
            .context("PUT reload chunk Parquet")?;

        // The cursor comes from the LAST ROW of the chunk just written — never a separate MAX()
        // query (racy). Values stay in their text output form (precision-safe for bigint PKs).
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
        let n = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        // One chunk file exported: bump the per-table chunk + row counters.
        common::metrics::record_reload_chunk(&self.series, n);
        if n < self.cfg.chunk_rows.get() {
            Ok(ChunkOutcome::Drained { rows: n })
        } else {
            Ok(ChunkOutcome::Exported { rows: n })
        }
    }

    /// `SELECT "c1"::text, … FROM t [WHERE (pk…) > (cursor…)] ORDER BY pk… LIMIT n` — keyset
    /// pagination via row comparison over the PK-INDEX column order: index-friendly and
    /// composite-safe (never OFFSET).
    fn chunk_sql(&self) -> anyhow::Result<String> {
        continuation_sql(
            &self.rel,
            &self.pk_cols,
            self.cursor.as_ref(),
            self.cfg.chunk_rows.get(),
        )
    }

    fn chunk_meta(&self, watermark: Lsn) -> SinkMeta {
        SinkMeta {
            op: Op::Insert,
            // The stamp: every chunk row carries the attempt's lower fence as BOTH LSNs, so any
            // overlapping stream event (commit LSN > F) wins the loader's dedup.
            lsn: watermark,
            commit_lsn: Lsn::ZERO, // patched to F by the batcher's on_commit
            commit_ts: UtcTimestamp::now(),
            xid: 0,
            epoch: self.cfg.epoch,
            batch_id: String::new(),
            schema_version: self.schema_version,
            source_schema: self.rel.schema.clone(),
            source_table: self.rel.name.clone(),
            kind: Kind::Reload,
            unchanged_toast: Box::default(),
            sink_instance: self.cfg.instance.clone(),
            sink_processed_at: UtcTimestamp::now(),
        }
    }
}

/// Validate that a persisted H belongs to this exact exporter attempt. Kept pure so corruption and
/// identity mismatches are testable without a live control database.
fn validate_durable_end(
    reload: &control::ReloadRow,
    markers: &[control::ReloadMarkerRow],
    expected_epoch: EpochNo,
    expected_relation: &PgRelation,
    expected_schema_version: SchemaVersionNo,
    expected_start_lsn: Lsn,
    expected_request_id: Uuid,
) -> anyhow::Result<Option<Lsn>> {
    let Some(end) = markers
        .iter()
        .find(|marker| marker.kind == control::ReloadMarkerKind::End)
    else {
        return Ok(None);
    };
    let Some(baseline) = markers
        .iter()
        .find(|marker| marker.kind == control::ReloadMarkerKind::Baseline)
    else {
        anyhow::bail!(
            "reload {} has an end marker without its durable baseline marker",
            reload.reload_id
        );
    };
    let end_count = markers
        .iter()
        .filter(|marker| marker.kind == control::ReloadMarkerKind::End)
        .count();
    let baseline_count = markers
        .iter()
        .filter(|marker| marker.kind == control::ReloadMarkerKind::Baseline)
        .count();

    anyhow::ensure!(
        end_count == 1 && baseline_count == 1 && markers.len() == 2,
        "reload {} has an incomplete or duplicate durable F/H marker set",
        reload.reload_id
    );
    anyhow::ensure!(
        reload.status == control::ReloadStatus::Exporting,
        "reload {} durable H recovery requires exporting status, found {:?}",
        reload.reload_id,
        reload.status
    );
    anyhow::ensure!(
        reload.epoch == expected_epoch
            && reload.source_schema == expected_relation.schema
            && reload.source_table == expected_relation.name,
        "reload {} durable H target identity disagrees with the exporter",
        reload.reload_id
    );
    anyhow::ensure!(
        reload.schema_version == Some(expected_schema_version)
            && reload.start_lsn == Some(expected_start_lsn)
            && reload.final_lsn.is_none(),
        "reload {} durable H boundaries disagree with the frozen exporter attempt",
        reload.reload_id
    );
    let Some(durable_request_id) = reload.source_request_id.or(reload.parent_request_id) else {
        anyhow::bail!(
            "reload {} durable H has no source-fence request namespace",
            reload.reload_id
        );
    };
    anyhow::ensure!(
        durable_request_id == expected_request_id,
        "reload {} durable H request identity disagrees with the exporter",
        reload.reload_id
    );
    anyhow::ensure!(
        baseline.reload_id == reload.reload_id
            && baseline.kind == control::ReloadMarkerKind::Baseline
            && baseline.lsn == expected_start_lsn
            && baseline.schema_version == expected_schema_version,
        "reload {} durable baseline marker disagrees with frozen F/schema",
        reload.reload_id
    );
    anyhow::ensure!(
        end.reload_id == reload.reload_id
            && end.kind == control::ReloadMarkerKind::End
            && end.schema_version == expected_schema_version
            && end.lsn >= expected_start_lsn,
        "reload {} durable end marker disagrees with frozen F/schema",
        reload.reload_id
    );
    Ok(Some(end.lsn))
}

async fn establish_start_fence(
    client: &mut tokio_postgres::Client,
    pool: &sqlx::PgPool,
    waiters: &FenceWaiters,
    publication: &str,
    request_id: Uuid,
    req: &control::ReloadRow,
    expected_relation: &PgRelation,
    schema_version: SchemaVersionNo,
    echo_timeout: Duration,
) -> anyhow::Result<Lsn> {
    match crate::reload_event::emit_fence(
        client,
        waiters,
        publication,
        request_id,
        req.reload_id,
        FencePhase::Start,
        &req.source_schema,
        &req.source_table,
        expected_relation,
        schema_version,
        FENCE_LOCK_TIMEOUT,
        echo_timeout,
    )
    .await?
    {
        FenceEmission::Observed(echo) => {
            control::reload::record_start_fence(
                pool,
                req.reload_id,
                echo.commit_lsn,
                control::ReloadFenceIdentity {
                    request_id: Some(request_id),
                    source_schema: &req.source_schema,
                    source_table: &req.source_table,
                    schema_version,
                },
            )
            .await
            .context("record safe reload start fence")?;
            Ok(echo.commit_lsn)
        }
        FenceEmission::AlreadyExists => {
            wait_for_marker(
                pool,
                req.reload_id,
                control::ReloadMarkerKind::Baseline,
                echo_timeout,
            )
            .await
        }
        FenceEmission::SchemaChanged => Err(ConnectSchemaChanged {
            new_version: await_schema_change(
                pool,
                req.epoch,
                &req.source_schema,
                &req.source_table,
                schema_version,
                req.reload_id,
                echo_timeout,
            )
            .await?,
        }
        .into()),
    }
}

async fn await_schema_change(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    frozen: SchemaVersionNo,
    reload_id: ReloadId,
    timeout: Duration,
) -> anyhow::Result<SchemaVersionNo> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let latest = control::read_latest_version(pool, epoch, schema, table)
            .await
            .context("read registry latest version after live shape change")?;
        if let Some(version) = version_changed(frozen, latest) {
            return Ok(version);
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "live source shape changed for reload {reload_id}, but schema_registry did not advance within {timeout:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn validate_export_keys(rel: &PgRelation, pk_cols: &[String]) -> anyhow::Result<()> {
    anyhow::ensure!(
        !rel.to_key_columns().is_empty(),
        "{}.{} has no replica-identity key",
        rel.schema,
        rel.name
    );
    for pk in pk_cols {
        anyhow::ensure!(
            rel.columns.iter().any(|column| &column.name == pk),
            "PRIMARY KEY column {pk:?} is absent from frozen relation {}.{}",
            rel.schema,
            rel.name
        );
    }
    Ok(())
}

async fn wait_for_marker(
    pool: &sqlx::PgPool,
    reload_id: ReloadId,
    kind: control::ReloadMarkerKind,
    timeout: Duration,
) -> anyhow::Result<Lsn> {
    tokio::time::timeout(timeout, async {
        loop {
            if let Some(marker) = control::reload::read_markers(pool, reload_id)
                .await
                .context("read reload boundary markers")?
                .into_iter()
                .find(|marker| marker.kind == kind)
            {
                return anyhow::Ok(marker.lsn);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .with_context(|| format!("timed out waiting for {kind:?} marker on reload {reload_id}"))?
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
               AND k.ord <= i.indnkeyatts
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
) -> anyhow::Result<String> {
    let cols: Vec<String> = rel
        .columns
        .iter()
        .map(|column| {
            SqlIdent::new(&column.name)
                .map(|ident| format!("{ident}::text"))
                .context("validate reload SELECT column")
        })
        .collect::<anyhow::Result<_>>()?;
    let key_cols: Vec<String> = pk_cols
        .iter()
        .map(|column| {
            SqlIdent::new(column)
                .map(|ident| format!("_src.{ident}"))
                .context("validate reload PRIMARY KEY column")
        })
        .collect::<anyhow::Result<_>>()?;
    let schema = SqlIdent::new(&rel.schema).context("validate reload SELECT schema")?;
    let table = SqlIdent::new(&rel.name).context("validate reload SELECT table")?;
    let mut sql = format!(
        "SELECT {} FROM {}.{} AS _src",
        cols.join(", "),
        schema,
        table
    );
    if let Some(serde_json::Value::Array(values)) = cursor {
        let literals: Vec<String> = values
            .iter()
            .map(|v| match v {
                serde_json::Value::String(s) => s.to_quoted_literal(),
                other => other.to_string().to_quoted_literal(),
            })
            .collect();
        let _write_result = write!(
            &mut sql,
            " WHERE ({}) > ({})",
            key_cols.join(", "),
            literals.join(", ")
        );
    }
    let _write_result = write!(&mut sql, " ORDER BY {} LIMIT {limit}", key_cols.join(", "));
    Ok(sql)
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

/// Refill `out` with one row's text values. Takes the buffer rather than returning a fresh `Vec` so
/// the chunk loop reuses one allocation across every row it exports.
fn read_row_into(row: &tokio_postgres::Row, rel: &PgRelation, out: &mut Vec<TupleValue>) {
    out.clear();
    out.extend(
        (0..rel.columns.len()).map(|i| match row.get::<_, Option<String>>(i) {
            Some(s) => TupleValue::Text(s),
            None => TupleValue::Null,
        }),
    );
}

#[cfg(test)]
#[path = "reload_export_test.rs"]
mod tests;
