//! Phase A — the loader's ingest half (loader §4). **Claim** the next `ready` manifest files in
//! `(lsn_end, id)` order, **append every row verbatim** into `<table>_raw` with DuckDB, then — in **one
//! control-DB transaction** — advance `raw_appended_lsn = max(claimed lsn_end)` **and** delete the
//! claimed queue rows. No transform: this ends with a faithful, idempotent CDC log.
//!
//! **Two guards, both load-bearing (§4 crash-window):** (1) the queue *deletion* is what advances the
//! frontier (not the watermark alone); (2) the per-file DuckDB ingest ledger commits atomically with
//! each raw append and absorbs a replay. DuckDB and control Postgres cannot share a transaction, so
//! the ordering is strict: the DuckDB append + marker **commit first**, then the Postgres
//! advance+delete txn. A crash between them re-claims the still-`ready` file, finds its marker, and
//! returns zero without rebuilding a per-row index.

use crate::duck::{BeginReload, ReloadBuild, TableDb};
use crate::error::LoaderError;
use crate::health::LoaderState;
use common::{EpochNo, Kind, Lsn, PgRelation, ReloadId, SchemaVersionNo};
use futures_util::StreamExt as _;
use sha2::{Digest as _, Sha256};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt as _;

/// Everything one owned table's apply worker needs — **owned** (one [`TableDb`]/DuckDB connection per
/// table, never shared), so it can move into a `spawn_local`'d [`crate::apply_loop::apply_loop`].
#[derive(Debug)]
pub struct TableCtx {
    /// Control-Postgres handle. Cloning a pool shares the same connections, so every worker's copy
    /// draws on one set.
    pub pool: sqlx::PgPool,
    /// The generation this worker was started for. Compared against `epoch_rx` to detect a bump.
    pub epoch: EpochNo,
    /// Latest global control-plane epoch, broadcast by the loader's one shared poller.
    pub epoch_rx: tokio::sync::watch::Receiver<EpochNo>,
    /// Identity of the table-ownership lease held by this worker.
    pub owner_pod: String,
    /// Monotonic ownership token copied at bootstrap and rechecked for every publication action.
    pub fencing_token: i64,
    /// Staging object store used to download and fingerprint immutable manifest objects.
    pub store: Arc<dyn object_store::ObjectStore>,
    /// Bucket expected in every manifest `s3://` URI; the configured client is scoped to it.
    pub staging_bucket: String,
    /// Source schema of the table this worker owns.
    pub schema: String,
    /// Source table this worker owns.
    pub table: String,
    /// The `table` metric label (`"<schema>.<table>"`), precomputed at construction. Cardinality is
    /// bounded by the tables this pod owns, and the value is constant per worker.
    pub series: String,
    /// The table shape — the transform (Phase B) renders its SQL from this.
    pub rel: PgRelation,
    /// This table's transient DuckDB connection attached to its isolated DuckLake schema.
    /// `app::pipeline` joins every worker and drops these connections before it releases the shared
    /// catalog advisory-lock session and then the control leases.
    pub db: TableDb,
    /// The process-wide probe state. Shared, not owned: one table's quarantine degrades the pod.
    pub state: Arc<LoaderState>,
    /// Files claimed per cycle.
    pub max_files: NonZeroI64,
    /// Fresh full-table snapshots permitted after an immutable object fails verification.
    pub max_integrity_resnapshots: u32,
    /// The apply-loop poll cadence.
    pub poll_interval: Duration,
    /// The compaction cadence — full-rebuild + prune, on this worker thread after an apply cycle.
    pub compaction_interval: Duration,
    /// Raw retention as an LSN-byte lag behind `transformed_lsn` (the prune floor).
    pub retention_lsn_lag: u64,
    /// The reload_id whose claim pause was already logged — a paused table says *why* it
    /// is idle once per pause, not once per poll. Per-table by construction (one [`TableCtx`] per
    /// worker, with `!Sync` interior state), so this needs interior mutability behind
    /// `run_phase_a(&ctx)`, not synchronisation. `Option<ReloadId>` is `Copy`, so `Cell` has no borrow
    /// flag or runtime panic path.
    pub pause_logged: Cell<Option<ReloadId>>,
}

/// The once-per-pause transition: `Some(reload_id)` exactly when a NEW pause begins (a different
/// reload than last logged, or the first). A lifted pause (no live rebuild) clears the latch so
/// the next reload logs again.
pub(crate) fn pause_began(
    logged: &Cell<Option<ReloadId>>,
    live: Option<ReloadId>,
) -> Option<ReloadId> {
    match (logged.get(), live) {
        (prev, Some(id)) if prev != Some(id) => {
            logged.set(Some(id));
            Some(id)
        }
        (_, None) => {
            logged.set(None);
            None
        }
        _ => None,
    }
}

/// Read the two independent inputs to the Phase-A backlog gauge concurrently.
///
/// Neither indexed control-plane read consumes the other's output or runs inside a transaction, so
/// concurrency changes their latency from the sum of two round trips to the maximum while retaining
/// two SQL queries. This uses one task rather than spawning, which remains valid on the loader's
/// `LocalSet`.
///
/// Each read may hold one of `control::connect`'s five default pool connections. With many owned
/// tables that can queue, but it cannot deadlock here: neither future holds an open transaction while
/// waiting to acquire another connection.
async fn read_lag_inputs(ctx: &TableCtx) -> Result<(Option<Lsn>, Lsn), LoaderError> {
    let (max_ready, checkpoint) = tokio::try_join!(
        control::max_ready_lsn_end(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table),
        control::read_checkpoint(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table),
    )?;
    Ok((
        max_ready,
        checkpoint.map_or(Lsn::ZERO, |cp| cp.raw_appended_lsn),
    ))
}

/// One Phase-A pass. Returns the max `lsn_end` appended, or `None` if the queue was empty.
///
/// # Errors
///
/// Returns [`LoaderError::Control`] or [`LoaderError::ControlTxn`] for control-plane reads and the
/// advance/delete transaction, [`LoaderError::Duck`] for local append/reconcile failures,
/// [`LoaderError::RegistryDecode`] for an invalid stored shape, [`LoaderError::Quarantine`] for an
/// unsafe DDL cast, or [`LoaderError::Internal`] for an inconsistent reload manifest.
pub async fn run_phase_a(ctx: &TableCtx) -> Result<Option<Lsn>, LoaderError> {
    // A crash can land after control Postgres fenced a corrupt publication but before the local
    // hidden generation was removed. Clean only that exact failed/building identity before the
    // durable recovery state below decides whether this table may continue.
    discard_failed_reload_build(ctx).await?;
    enforce_integrity_recovery_state(ctx).await?;

    // Observability: set the Phase-A backlog gauge every poll (0 when caught up) —
    // `max(lsn_end over ready files) − raw_appended_lsn`. Both operands are cheap indexed control-DB
    // reads; doing this before the claim means idle polls report a truthful 0.
    let (max_ready, raw_appended) = read_lag_inputs(ctx).await?;
    common::metrics::set_raw_append_lag(&ctx.series, raw_append_lag_bytes(max_ready, raw_appended));

    // A source-driven attempt is discoverable from its durable marker rows even when the table
    // exported zero rows and therefore produced no reload manifest. Start (or resume) its hidden
    // generation before looking at the file queue; this also purges the pre-fence backlog so a
    // large backlog cannot keep the first reload chunk outside every bounded claim forever.
    let mut reload_build = prepare_ready_reload(ctx).await?;

    // 1. Claim in (lsn_end, id) order — NEVER `lsn_end > raw_appended_lsn` (that skips equal-lsn_end
    //    snapshot files forever).
    let claimed = if let Some(build) = &reload_build {
        if build.phase == crate::duck::ReloadPhase::Published {
            Vec::new()
        } else {
            let publication = publication_for_build(ctx, build).await?;
            control::reload::claim_publication_ready(
                &ctx.pool,
                &publication,
                &ctx.owner_pod,
                ctx.fencing_token,
                ctx.max_files.get(),
            )
            .await?
        }
    } else {
        control::claim_ready(
            &ctx.pool,
            ctx.epoch,
            &ctx.schema,
            &ctx.table,
            ctx.max_files.get(),
        )
        .await?
    };

    // Close the readiness/claim check-use seam. If the first read saw `exporting` but
    // `complete_export` became visible to the claim and lifted its pause, the newer read must
    // create the hidden generation before any claimed (F,H] WAL is routed. This second read is
    // needed even when `claimed` is empty: a zero-row dump still has marker-only work to publish.
    // A completion after this read is safe: a claim that saw the existing attempt as requested or
    // exporting returned no rows, while files claimed before a brand-new request all precede its F.
    if reload_build.is_none() {
        reload_build = prepare_ready_reload(ctx).await?;
    }

    if claimed.is_empty() {
        // Distinguish IDLE from PAUSED: a live reload attempt withholds this
        // table's claims (reload §2 — claiming would retire post-`W` files the rebuild must
        // replay). Only probe when a backlog exists, and log the reason once per pause.
        if max_ready.is_some() {
            let live = control::reload::active_rebuilds(&ctx.pool, ctx.epoch)
                .await?
                .into_iter()
                .find(|r| r.source_schema == ctx.schema && r.source_table == ctx.table)
                .map(|r| r.reload_id);
            if let Some(reload_id) = pause_began(&ctx.pause_logged, live) {
                tracing::info!(
                    table = %format_args!("{}.{}", ctx.schema, ctx.table),
                    reload_id = %reload_id,
                    reason = "rebuild-in-flight",
                    "claims paused: ready rows accumulate (frontier frozen at W) until export_complete"
                );
            }
        } else {
            pause_began(&ctx.pause_logged, None); // caught up — clear the latch
        }
        // An upgraded database keeps its legacy row-level replay PK until the control queue is
        // empty. Only then can we prove there is no old-version crash-window append lacking a file
        // marker. New files arriving after this observation have not been appended and are safe.
        if reload_build.is_none()
            && max_ready.is_none()
            && ctx.db.migrate_legacy_replay_fence(&ctx.table)?
        {
            tracing::info!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                "migrated raw replay fence from a per-row primary key to the file ingest ledger"
            );
        }
        return Ok(None);
    }
    pause_began(&ctx.pause_logged, None); // claiming again — any pause has lifted

    // 2. Append each file verbatim to <table>_raw (DuckDB auto-commits each statement). Idempotent.
    //    Files are claimed in (lsn_end, id) = commit order, and the sink cuts a fresh homogeneous file at
    //    every structural change, so schema_version is monotonic across `claimed`. Before appending a
    //    file at a NEWER version, reconcile both tables UP TO it — so `<table>_raw` always has
    //    exactly the file's columns and the verbatim `SELECT *` append lines up; already-appended older
    //    rows read NULL for the freshly-added column (additive superset).
    // A pending reload will publish a replacement mirror at the new schema
    // and `delete_superseded` every non-reload file at `lsn_end <= F`. Such a file must NOT
    // reconcile here: a lossy cast would re-quarantine the loader on every restart BEFORE it ever
    // reaches the reload chunk file that clears the quarantine (the claim order puts the low-`lsn_end`
    // blocker first). Compute the floor once; skip superseded version-crossing files in the loop.
    let supersede_floor =
        control::reload::reload_supersede_floor(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
            .await?;

    let mut max_lsn = reload_build
        .as_ref()
        .map_or(raw_appended, |build| build.raw_appended_lsn);
    let mut ids = Vec::with_capacity(claimed.len());
    let mut appended = 0u64;
    for unit in manifest_units(&claimed) {
        let first = unit
            .first()
            .copied()
            .ok_or_else(|| LoaderError::ManifestInvariant {
                message: "claim produced an empty manifest unit".to_string(),
            })?;
        // H is a unit boundary. Protocol-v2 group children share lsn_end, and no part of a group is
        // routed or retired when the entire unit belongs after the active publication barrier.
        if reload_build
            .as_ref()
            .is_some_and(|build| first.lsn_end > build.final_lsn)
        {
            break;
        }

        let mut route = None;
        for f in &unit {
            if let Some(build) = &reload_build
                && f.kind == control::ManifestKind::Reload
                && f.reload_id == Some(build.reload_id)
            {
                validate_reload_manifest_at_f(f, build)?;
            }
            let file_route = if f.kind == control::ManifestKind::Reload {
                let file_route = route_reload_file(ctx, f).await?;
                reload_build = ctx.db.reload_build()?;
                file_route
            } else if let Some(build) = &reload_build {
                if f.lsn_end <= build.start_lsn {
                    FileRoute::Retire
                } else {
                    FileRoute::Shadow(build.shadow_table.clone())
                }
            } else {
                FileRoute::Live
            };
            if route
                .as_ref()
                .is_some_and(|expected| expected != &file_route)
            {
                return Err(LoaderError::ManifestInvariant {
                    message: format!(
                        "stream group {:?} would split across loader destinations",
                        first.stream_group_id
                    ),
                });
            }
            route = Some(file_route);
        }
        let route = route.ok_or_else(|| LoaderError::ManifestInvariant {
            message: "manifest unit has no route".to_string(),
        })?;

        if route == FileRoute::Retire {
            ids.extend(unit.iter().map(|file| file.id));
            continue;
        }
        let current_schema = ctx.db.schema_version()?;
        if route == FileRoute::Live
            && unit.iter().any(|f| {
                f.kind != control::ManifestKind::Reload
                    && f.schema_version > current_schema
                    && supersede_floor.is_some_and(|floor| f.lsn_end <= floor)
            })
        {
            continue;
        }
        if let FileRoute::Shadow(_) = &route {
            let build = reload_build
                .as_ref()
                .ok_or_else(|| LoaderError::ManifestInvariant {
                    message: "shadow-routed manifest unit has no reload build".to_string(),
                })?;
            if unit
                .iter()
                .any(|file| file.schema_version != build.schema_version)
            {
                return Err(LoaderError::ManifestInvariant {
                    message: format!(
                        "stream group {:?} crosses frozen reload {} schema {}; shadow schema evolution inside [F,H] is not yet supported",
                        first.stream_group_id, build.reload_id, build.schema_version
                    ),
                });
            }
        }

        if route == FileRoute::Live {
            let target = unit
                .iter()
                .map(|file| file.schema_version)
                .max()
                .ok_or_else(|| LoaderError::ManifestInvariant {
                    message: "manifest unit has no schema version".to_string(),
                })?;
            if target > ctx.db.schema_version()?
                && let Err(error) = crate::ddl::reconcile_to_version(
                    &ctx.db,
                    &ctx.pool,
                    ctx.epoch,
                    &ctx.schema,
                    &ctx.table,
                    target,
                )
                .await
            {
                if matches!(error, LoaderError::Quarantine { .. }) {
                    ctx.state.quarantine_table(&ctx.schema, &ctx.table);
                    tracing::error!(
                        table = %format_args!("{}.{}", ctx.schema, ctx.table),
                        "QUARANTINE: schema change could not be applied before atomic stream-group append"
                    );
                }
                return Err(error);
            }
        }

        let destination = match &route {
            FileRoute::Live => ctx.table.as_str(),
            FileRoute::Shadow(table) => table.as_str(),
            FileRoute::Retire => {
                return Err(LoaderError::ManifestInvariant {
                    message: "retired manifest unit reached append preparation".to_string(),
                });
            }
        };
        let overrides: Vec<Option<String>> = unit
            .iter()
            .map(|file| {
                (file.kind == control::ManifestKind::Spill).then(|| file.lsn_end.to_string())
            })
            .collect();
        let probes: Vec<crate::duck::ManifestAppend<'_>> = unit
            .iter()
            .zip(&overrides)
            .map(|(file, lsn_override)| crate::duck::ManifestAppend {
                manifest_id: file.id,
                original_uri: &file.s3_uri,
                verified_uri: None,
                object_size: file.object_size,
                sha256: &file.sha256,
                stream_group_id: file.stream_group_id.map(|id| id.0),
                schema_version: file.schema_version,
                commit_lsn_override: lsn_override.as_deref(),
                expectation: Some(manifest_expectation(file, &ctx.rel)),
            })
            .collect();
        let mut states = Vec::with_capacity(probes.len());
        for (file, probe) in unit.iter().zip(&probes) {
            match ctx.db.ingest_receipt_state(probe) {
                Ok(state) => states.push(state),
                Err(error @ LoaderError::ManifestInvariant { .. }) => {
                    recover_object_integrity_failure(ctx, file, reload_build.as_ref(), &error)
                        .await?;
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        }
        let verified = if states
            .iter()
            .all(|state| *state == crate::duck::IngestReceiptState::Ingested)
        {
            Vec::new()
        } else if states
            .iter()
            .all(|state| *state == crate::duck::IngestReceiptState::Missing)
        {
            let mut downloads = Vec::with_capacity(unit.len());
            for file in &unit {
                match download_verified(ctx, file).await {
                    Ok(object) => downloads.push(object),
                    Err(error @ LoaderError::ObjectIntegrity { .. }) => {
                        recover_object_integrity_failure(ctx, file, reload_build.as_ref(), &error)
                            .await?;
                        // Nothing in this claim is retired. Earlier unit appends are protected by
                        // their Duck receipts and safely replay after the replacement snapshot.
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
            }
            downloads
        } else {
            let error = LoaderError::ManifestInvariant {
                message: format!(
                    "stream group {:?} has a partial durable ingest receipt",
                    first.stream_group_id
                ),
            };
            recover_object_integrity_failure(ctx, first, reload_build.as_ref(), &error).await?;
            return Ok(None);
        };
        let appends: Vec<crate::duck::ManifestAppend<'_>> = unit
            .iter()
            .zip(&overrides)
            .enumerate()
            .map(|(index, (file, lsn_override))| {
                let verified_uri = verified
                    .get(index)
                    .map(|object| object.uri(&file.s3_uri))
                    .transpose()?;
                Ok(crate::duck::ManifestAppend {
                    manifest_id: file.id,
                    original_uri: &file.s3_uri,
                    verified_uri,
                    object_size: file.object_size,
                    sha256: &file.sha256,
                    stream_group_id: file.stream_group_id.map(|id| id.0),
                    schema_version: file.schema_version,
                    commit_lsn_override: lsn_override.as_deref(),
                    expectation: Some(manifest_expectation(file, &ctx.rel)),
                })
            })
            .collect::<Result<_, LoaderError>>()?;
        match ctx.db.append_manifest_unit(destination, &appends) {
            Ok(rows) => appended = appended.saturating_add(rows),
            Err(error @ LoaderError::ObjectIntegrity { .. }) => {
                let failed = match &error {
                    LoaderError::ObjectIntegrity { uri, .. } => unit
                        .iter()
                        .copied()
                        .find(|file| file.s3_uri == *uri)
                        .unwrap_or(first),
                    _ => first,
                };
                recover_object_integrity_failure(ctx, failed, reload_build.as_ref(), &error)
                    .await?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        max_lsn = unit.iter().fold(max_lsn, |max, file| max.max(file.lsn_end));
        ids.extend(unit.iter().map(|file| file.id));
    }

    // 3. ONE control-DB txn: advance the watermark to the batch max AND delete the claimed queue rows.
    //    (The append is already durable in DuckDB — step 2 committed.)
    if ids.is_empty() {
        return Ok(None);
    }
    let publication = match &reload_build {
        Some(build) => {
            ctx.db
                .advance_reload_raw(build.reload_id, build.publication_nonce, max_lsn)?;
            Some(publication_for_build(ctx, build).await?)
        }
        None => None,
    };
    let mut tx = ctx
        .pool
        .begin()
        .await
        .map_err(|source| LoaderError::ControlTxn {
            op: "begin advance+delete txn",
            source,
        })?;
    if let Some(publication) = &publication {
        control::reload::lock_publication(&mut *tx, publication, &ctx.owner_pod, ctx.fencing_token)
            .await?;
    } else {
        control::advance_raw_appended(&mut *tx, ctx.epoch, &ctx.schema, &ctx.table, max_lsn)
            .await?;
    }
    let deleted = control::delete_claimed(&mut *tx, &ids).await?;
    if deleted != ids.len() as u64 {
        return Err(LoaderError::Internal(format!(
            "claimed manifest retirement removed {deleted} rows, expected {}",
            ids.len()
        )));
    }
    tx.commit()
        .await
        .map_err(|source| LoaderError::ControlTxn {
            op: "commit advance+delete txn",
            source,
        })?;

    // Migration is intentionally deferred until a fresh control-plane read proves the queue is
    // empty. A prior release may have appended more files than this process's current `max_files`,
    // then crashed before deleting them; migrating after merely one batch would duplicate the rest.
    if reload_build.is_none()
        && ctx.db.has_legacy_replay_fence()
        && control::max_ready_lsn_end(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table)
            .await?
            .is_none()
        && ctx.db.migrate_legacy_replay_fence(&ctx.table)?
    {
        tracing::info!(
            table = %format_args!("{}.{}", ctx.schema, ctx.table),
            "migrated raw replay fence from a per-row primary key to the file ingest ledger"
        );
    }

    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        files = ids.len(),
        rows = appended,
        raw_appended = %max_lsn,
        "Phase A: appended to <table>_raw, watermark advanced, queue drained"
    );
    Ok(Some(max_lsn))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FileRoute {
    Live,
    Shadow(String),
    Retire,
}

#[derive(Debug)]
struct VerifiedObject {
    temp: tempfile::NamedTempFile,
}

impl VerifiedObject {
    fn uri(&self, original_uri: &str) -> Result<&str, LoaderError> {
        self.temp
            .path()
            .to_str()
            .ok_or_else(|| LoaderError::ManifestInvariant {
                message: format!("temporary path for {original_uri} is not UTF-8"),
            })
    }
}

fn manifest_units(rows: &[control::ManifestRow]) -> Vec<Vec<&control::ManifestRow>> {
    let mut units = Vec::<Vec<&control::ManifestRow>>::new();
    let mut groups = BTreeMap::<control::ManifestGroupId, usize>::new();
    for row in rows {
        if let Some(group_id) = row.stream_group_id {
            let index = match groups.get(&group_id).copied() {
                Some(index) => index,
                None => {
                    let index = units.len();
                    units.push(Vec::new());
                    groups.insert(group_id, index);
                    index
                }
            };
            units[index].push(row);
        } else {
            units.push(vec![row]);
        }
    }
    units
}

fn manifest_expectation<'a>(
    file: &'a control::ManifestRow,
    relation: &'a PgRelation,
) -> crate::duck::ManifestExpectation<'a> {
    let kind = match file.kind {
        control::ManifestKind::Snapshot => Kind::Snapshot,
        control::ManifestKind::Reload => Kind::Reload,
        control::ManifestKind::Stream | control::ManifestKind::Spill => Kind::Stream,
    };
    crate::duck::ManifestExpectation {
        row_count: file.row_count,
        epoch: file.epoch,
        source_schema: &file.source_schema,
        source_table: &file.source_table,
        source_columns: &relation.columns,
        schema_version: file.schema_version,
        kind,
        lsn_start: file.lsn_start,
        lsn_end: file.lsn_end,
        speculative_commit_lsn: file.kind == control::ManifestKind::Spill,
    }
}

async fn download_verified(
    ctx: &TableCtx,
    manifest: &control::ManifestRow,
) -> Result<VerifiedObject, LoaderError> {
    let rest =
        manifest
            .s3_uri
            .strip_prefix("s3://")
            .ok_or_else(|| LoaderError::ManifestInvariant {
                message: format!("manifest {} has a non-S3 URI", manifest.id),
            })?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| LoaderError::ManifestInvariant {
            message: format!("manifest {} has an S3 URI with no object key", manifest.id),
        })?;
    if bucket != ctx.staging_bucket || key.is_empty() {
        return Err(LoaderError::ManifestInvariant {
            message: format!(
                "manifest {} URI bucket/key does not match configured staging bucket",
                manifest.id
            ),
        });
    }
    let object_path = object_store::path::Path::from(key);
    let result = match ctx.store.get(&object_path).await {
        Ok(result) => result,
        Err(source) => return Err(classify_manifest_get_error(&manifest.s3_uri, source)),
    };
    let temp = tempfile::NamedTempFile::new().map_err(|source| LoaderError::File {
        op: "create verified manifest temp file",
        path: std::env::temp_dir().display().to_string(),
        source,
    })?;
    let path = temp.path().to_path_buf();
    let mut output = tokio::fs::File::create(&path)
        .await
        .map_err(|source| LoaderError::File {
            op: "open verified manifest temp file",
            path: path.display().to_string(),
            source,
        })?;
    let mut stream = result.into_stream();
    let mut hasher = Sha256::new();
    let mut size = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|source| {
            classify_manifest_object_error(&manifest.s3_uri, "stream manifest object", source)
        })?;
        size = size
            .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| LoaderError::ObjectIntegrity {
                uri: manifest.s3_uri.clone(),
                reason: "downloaded byte count overflowed u64".to_string(),
            })?;
        hasher.update(&chunk);
        output
            .write_all(&chunk)
            .await
            .map_err(|source| LoaderError::File {
                op: "write verified manifest temp file",
                path: path.display().to_string(),
                source,
            })?;
    }
    output.flush().await.map_err(|source| LoaderError::File {
        op: "flush verified manifest temp file",
        path: path.display().to_string(),
        source,
    })?;
    drop(output);
    let actual_size = i64::try_from(size).map_err(|_| LoaderError::ObjectIntegrity {
        uri: manifest.s3_uri.clone(),
        reason: "downloaded object is larger than bigint".to_string(),
    })?;
    let actual_sha = hasher.finalize();
    validate_object_fingerprint(
        &manifest.s3_uri,
        manifest.object_size,
        &manifest.sha256,
        actual_size,
        actual_sha.as_slice(),
    )?;
    Ok(VerifiedObject { temp })
}

fn classify_manifest_get_error(uri: &str, source: object_store::Error) -> LoaderError {
    classify_manifest_object_error(uri, "download manifest object", source)
}

fn classify_manifest_object_error(
    uri: &str,
    op: &'static str,
    source: object_store::Error,
) -> LoaderError {
    match source {
        object_store::Error::NotFound { .. } => LoaderError::ObjectIntegrity {
            uri: uri.to_string(),
            reason: "durable manifest object is missing".to_string(),
        },
        source => LoaderError::ObjectStore {
            op,
            source: Box::new(source),
        },
    }
}

async fn enforce_integrity_recovery_state(ctx: &TableCtx) -> Result<(), LoaderError> {
    let Some(recovery) =
        control::read_integrity_recovery(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?
    else {
        return Ok(());
    };
    match recovery.status {
        control::IntegrityRecoveryStatus::Retrying => {
            ctx.state.quarantine_table(&ctx.schema, &ctx.table);
            tracing::debug!(
                table = %ctx.series,
                attempt = recovery.attempt_count,
                recovery_reload_id = ?recovery.recovery_reload_id,
                "table readiness remains degraded while an integrity replacement is pending"
            );
            Ok(())
        }
        control::IntegrityRecoveryStatus::Quarantined => {
            ctx.state.quarantine_table(&ctx.schema, &ctx.table);
            Err(LoaderError::Quarantine {
                table: ctx.series.clone(),
                reason: recovery.last_error,
            })
        }
        control::IntegrityRecoveryStatus::Recovered => {
            ctx.state.clear_table_quarantine(&ctx.schema, &ctx.table);
            Ok(())
        }
    }
}

async fn discard_failed_reload_build(ctx: &TableCtx) -> Result<(), LoaderError> {
    let Some(build) = ctx.db.reload_build()? else {
        return Ok(());
    };
    if build.phase != crate::duck::ReloadPhase::Building {
        return Ok(());
    }
    let Some(reload) = control::reload::get(&ctx.pool, build.reload_id).await? else {
        return Ok(());
    };
    if reload.epoch != ctx.epoch
        || reload.source_schema != ctx.schema
        || reload.source_table != ctx.table
    {
        return Err(LoaderError::Internal(format!(
            "local reload {} belongs to {}.{}, but its control row belongs to epoch {} {}.{}",
            build.reload_id,
            ctx.schema,
            ctx.table,
            reload.epoch,
            reload.source_schema,
            reload.source_table
        )));
    }
    if reload.status != control::ReloadStatus::Failed {
        return Ok(());
    }
    if !ctx
        .db
        .abandon_reload_build(build.reload_id, build.publication_nonce)?
    {
        return Err(LoaderError::Internal(format!(
            "failed reload {} changed while cleaning its stale local shadow",
            build.reload_id
        )));
    }
    tracing::warn!(
        table = %ctx.series,
        reload_id = %build.reload_id,
        "removed a stale hidden generation whose control publication is failed"
    );
    Ok(())
}

async fn recover_object_integrity_failure(
    ctx: &TableCtx,
    manifest: &control::ManifestRow,
    build: Option<&ReloadBuild>,
    error: &LoaderError,
) -> Result<(), LoaderError> {
    let reason = error.to_string();
    let publication = build.map(|build| control::IntegrityPublicationFence {
        reload_id: build.reload_id,
        publication_nonce: build.publication_nonce,
    });
    let outcome = control::handle_integrity_failure(
        &ctx.pool,
        &control::IntegrityFailure {
            epoch: ctx.epoch,
            source_schema: &ctx.schema,
            source_table: &ctx.table,
            manifest_id: manifest.id,
            reason: &reason,
            owner_pod: &ctx.owner_pod,
            fencing_token: ctx.fencing_token,
            publication,
            max_resnapshots: ctx.max_integrity_resnapshots,
        },
    )
    .await?;

    if let Some(build) = build
        && !ctx
            .db
            .abandon_reload_build(build.reload_id, build.publication_nonce)?
    {
        return Err(LoaderError::Internal(format!(
            "integrity handler fenced reload {}, but its local building receipt changed before cleanup",
            build.reload_id
        )));
    }
    ctx.state.quarantine_table(&ctx.schema, &ctx.table);
    match outcome {
        control::IntegrityFailureOutcome::RecoveryScheduled { reload_id, attempt } => {
            tracing::error!(
                table = %ctx.series,
                manifest_id = %manifest.id,
                recovery_reload_id = %reload_id,
                attempt,
                reason,
                "staged object failed verification; scheduled a bounded replacement snapshot"
            );
            Ok(())
        }
        control::IntegrityFailureOutcome::Quarantined { attempt } => {
            tracing::error!(
                table = %ctx.series,
                manifest_id = %manifest.id,
                attempt,
                reason,
                "staged object failed verification; integrity replacement budget exhausted"
            );
            Err(LoaderError::Quarantine {
                table: ctx.series.clone(),
                reason,
            })
        }
    }
}

fn validate_object_fingerprint(
    uri: &str,
    expected_size: i64,
    expected_sha: &[u8],
    actual_size: i64,
    actual_sha: &[u8],
) -> Result<(), LoaderError> {
    if actual_size == expected_size && actual_sha == expected_sha {
        return Ok(());
    }
    Err(LoaderError::ObjectIntegrity {
        uri: uri.to_string(),
        reason: format!(
            "expected {expected_size} bytes / {}, got {actual_size} bytes / {}",
            hex::encode(expected_sha),
            hex::encode(actual_sha)
        ),
    })
}

fn validate_reload_manifest_at_f(
    file: &control::ManifestRow,
    build: &ReloadBuild,
) -> Result<(), LoaderError> {
    if file.reload_id != Some(build.reload_id)
        || file.lsn_start != build.start_lsn
        || file.lsn_end != build.start_lsn
    {
        return Err(LoaderError::Internal(format!(
            "reload manifest {} identity/boundaries ({:?}, [{}, {}]) do not equal active reload {} at frozen F {}",
            file.id, file.reload_id, file.lsn_start, file.lsn_end, build.reload_id, build.start_lsn
        )));
    }
    Ok(())
}

/// Discover a marker-delimited, source-driven rebuild without depending on a data file.
async fn prepare_ready_reload(ctx: &TableCtx) -> Result<Option<ReloadBuild>, LoaderError> {
    discard_failed_reload_build(ctx).await?;
    let existing = ctx.db.reload_build()?;
    if let Some(build) = &existing
        && build.phase == crate::duck::ReloadPhase::Published
    {
        let receipt = control::reload::read_publication(&ctx.pool, build.reload_id)
            .await?
            .ok_or_else(|| {
                LoaderError::Internal(format!(
                    "Duck has published reload {} but control has no publication receipt",
                    build.reload_id
                ))
            })?;
        validate_build_publication(ctx, build, &receipt)?;
        if receipt.status == control::ReloadStatus::Complete {
            ctx.db
                .clear_reload_publication(build.reload_id, build.publication_nonce)?;
            return Ok(None);
        }
    }

    let Some(publication) = control::reload::claim_publication(
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?
    else {
        if let Some(build) = existing {
            return Err(LoaderError::Internal(format!(
                "Duck reload {} has no export_complete/publishing control owner",
                build.reload_id
            )));
        }
        return Ok(None);
    };

    if let Some(build) = existing {
        validate_build_publication(ctx, &build, &publication)?;
        return Ok(Some(build));
    }

    let plan = plan_at_version(ctx, publication.schema_version).await?;
    let BeginReload::Ready(build) = ctx.db.begin_reload_shadow(
        &plan,
        publication.schema_version,
        publication.reload_id,
        publication.start_lsn,
        publication.final_lsn,
        publication.publication_nonce,
    )?
    else {
        return Ok(None);
    };
    let purged = control::delete_publication_superseded(
        &ctx.pool,
        &publication,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        reload_id = %publication.reload_id,
        schema_version = %publication.schema_version,
        start_lsn = %publication.start_lsn,
        final_lsn = %publication.final_lsn,
        purged,
        "fenced reload publication started in a hidden generation"
    );
    Ok(Some(*build))
}

pub(crate) fn validate_build_publication(
    ctx: &TableCtx,
    build: &ReloadBuild,
    publication: &control::ReloadPublication,
) -> Result<(), LoaderError> {
    if publication.epoch != ctx.epoch
        || publication.source_schema != ctx.schema
        || publication.source_table != ctx.table
        || publication.reload_id != build.reload_id
        || publication.start_lsn != build.start_lsn
        || publication.final_lsn != build.final_lsn
        || publication.schema_version != build.schema_version
        || publication.publication_nonce != build.publication_nonce
    {
        return Err(LoaderError::Internal(format!(
            "Duck reload {} does not match its durable control publication receipt",
            build.reload_id
        )));
    }
    Ok(())
}

pub(crate) async fn publication_for_build(
    ctx: &TableCtx,
    build: &ReloadBuild,
) -> Result<control::ReloadPublication, LoaderError> {
    let publication = control::reload::claim_publication(
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?
    .ok_or_else(|| {
        LoaderError::Internal(format!(
            "reload {} lost its publishing control receipt",
            build.reload_id
        ))
    })?;
    validate_build_publication(ctx, build, &publication)?;
    Ok(publication)
}

/// Route one claimed `kind='reload'` file to the live generation, the matching hidden generation,
/// or retirement when it belongs to a stale attempt.
async fn route_reload_file(
    ctx: &TableCtx,
    f: &control::ManifestRow,
) -> Result<FileRoute, LoaderError> {
    let file_reload_id = f.reload_id.ok_or_else(|| {
        LoaderError::Internal(format!(
            "manifest row {} is kind='reload' but carries no reload_id",
            f.id
        ))
    })?;
    if let Some(build) = ctx.db.reload_build()? {
        if file_reload_id < build.reload_id {
            return Ok(FileRoute::Retire);
        }
        if file_reload_id == build.reload_id {
            return Ok(FileRoute::Shadow(build.shadow_table));
        }
    }
    // `None` = the latch was never set, so this file can only be a NEW attempt: fall through to
    // the "greater" arm below rather than compare it against a stand-in id.
    if let Some(recorded) = ctx.db.recorded_reload_id()? {
        if file_reload_id < recorded {
            return Ok(FileRoute::Retire); // a superseded attempt whose purge raced the claim (H9)
        }
        if file_reload_id == recorded {
            return Ok(FileRoute::Retire); // a crash-window chunk discovered after publication
        }
    }

    // The ordinary claim can observe `export_complete` in the narrow interval before the second
    // preparation read. Claim publication here too; no shadow may be created from a file alone.
    let publication = control::reload::claim_publication(
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?
    .ok_or_else(|| {
        LoaderError::Internal(format!(
            "reload manifest {} has no fenced publication attempt",
            f.id
        ))
    })?;
    if publication.reload_id != file_reload_id {
        return if file_reload_id < publication.reload_id {
            Ok(FileRoute::Retire)
        } else {
            Err(LoaderError::Internal(format!(
                "reload manifest {} belongs to future attempt {file_reload_id} while {} is publishing",
                f.id, publication.reload_id
            )))
        };
    }
    if f.schema_version != publication.schema_version {
        return Err(LoaderError::Internal(format!(
            "reload {file_reload_id} chunk schema {} differs from frozen schema {}",
            f.schema_version, publication.schema_version
        )));
    }
    if f.lsn_end > publication.final_lsn {
        return Err(LoaderError::Internal(format!(
            "reload {file_reload_id} chunk {} lies beyond end marker {}",
            f.id, publication.final_lsn
        )));
    }

    // Build both replacement tables under a hidden deterministic name. The public/live generation
    // remains untouched until Phase B reaches the explicit H barrier.
    let plan = plan_at_version(ctx, publication.schema_version).await?;
    let BeginReload::Ready(build) = ctx.db.begin_reload_shadow(
        &plan,
        publication.schema_version,
        file_reload_id,
        publication.start_lsn,
        publication.final_lsn,
        publication.publication_nonce,
    )?
    else {
        return Ok(FileRoute::Retire);
    };
    // Purge superseded pending rows: every non-reload file at lsn_end <= the start fence describes a
    // commit the consistent baseline re-covers; applying it after replacement would only churn.
    // Post-F stream files survive and apply after the baseline chunks in (lsn_end, id) order.
    let purged = control::delete_publication_superseded(
        &ctx.pool,
        &publication,
        &ctx.owner_pod,
        ctx.fencing_token,
    )
    .await?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        reload_id = %file_reload_id,
        schema_version = %publication.schema_version,
        start_lsn = %publication.start_lsn,
        final_lsn = %publication.final_lsn,
        purged,
        "reload reconciliation started in a hidden generation"
    );
    Ok(FileRoute::Shadow(build.shadow_table))
}

/// The registry shape at `version` as a [`crate::plan::TablePlan`] (the Tier-2 emit/recombine
/// path), falling back to the bootstrap relation's scalar shape for hermetic
/// single-version setups — `phase_b::current_transform`'s exact precedent.
async fn plan_at_version(
    ctx: &TableCtx,
    version: SchemaVersionNo,
) -> Result<crate::plan::TablePlan, LoaderError> {
    match control::read_registry(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, version).await? {
        Some(r) => {
            // Label built inside the closure (`current_transform`'s precedent): only a decode
            // failure pays for it.
            let rel: PgRelation = serde_json::from_value(r.columns).map_err(|source| {
                LoaderError::RegistryDecode {
                    table: format!("{}.{}", ctx.schema, ctx.table),
                    version: version.0,
                    source,
                }
            })?;
            Ok(crate::plan::TablePlan::from_registry(&rel, &r.descriptors))
        }
        None => Ok(crate::plan::TablePlan::tier1(&ctx.rel)),
    }
}

/// The raw-append backlog in LSN-bytes: how far the newest ready file's commit LSN leads the Phase-A
/// frontier. An empty queue (`None`) is 0; a frontier already at/after the head is 0. This is the
/// value of `walrus_loader_raw_append_lag_bytes`.
///
/// `max` bounds the head **up to** the frontier rather than branching on the same comparison twice:
/// the lower bound is what makes `-` reachable only inside its defined ordered domain, and the
/// clamped-equal case is exactly the 0 a caught-up (or momentarily stale) read should report.
fn raw_append_lag_bytes(max_ready_lsn_end: Option<Lsn>, raw_appended: Lsn) -> u64 {
    max_ready_lsn_end.map_or(0, |head| head.max(raw_appended) - raw_appended)
}

#[cfg(test)]
#[path = "phase_a_test.rs"]
mod tests;
