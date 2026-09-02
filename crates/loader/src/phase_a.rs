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
use common::{EpochNo, Lsn, PgRelation, ReloadId, SchemaVersionNo};
use std::cell::Cell;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::time::Duration;

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
    let claimed = control::claim_ready(
        &ctx.pool,
        ctx.epoch,
        &ctx.schema,
        &ctx.table,
        ctx.max_files.get(),
    )
    .await?;

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
        if max_ready.is_none() && ctx.db.migrate_legacy_replay_fence(&ctx.table)? {
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

    let mut max_lsn = raw_appended;
    let mut ids = Vec::with_capacity(claimed.len());
    let mut appended = 0u64;
    for f in &claimed {
        // Validate this attempt's baseline identity before the generic H barrier. A malformed
        // same-attempt reload file above H is not "future WAL": allowing it to hide behind the
        // break would let Phase B publish an incomplete shadow while the bad baseline remained
        // queued. A genuinely newer attempt has a different reload_id and stays queued normally.
        if let Some(build) = &reload_build
            && f.kind == control::ManifestKind::Reload
            && f.reload_id == Some(build.reload_id)
        {
            validate_reload_manifest_at_f(f, build)?;
        }

        // H is a real, row-independent cut line. Leave later WAL in the ordered ready queue until
        // Phase B has transformed and atomically published this shadow. That prevents canonical
        // schema reconciliation or post-H writes from mutating either generation mid-cutover.
        if reload_build
            .as_ref()
            .is_some_and(|build| f.lsn_end > build.final_lsn)
        {
            break;
        }

        let route = if f.kind == control::ManifestKind::Reload {
            let route = route_reload_file(ctx, f).await?;
            reload_build = ctx.db.reload_build()?;
            route
        } else if let Some(build) = &reload_build {
            // The purge ran before this claim, but rows that were already returned in `claimed`
            // remain in this in-memory vector. Retire those pre-fence rows without appending them.
            if f.lsn_end <= build.start_lsn {
                ids.push(f.id);
                continue;
            }
            FileRoute::Shadow(build.shadow_table.clone())
        } else {
            FileRoute::Live
        };

        if route == FileRoute::Retire {
            tracing::debug!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                manifest_id = f.id.0,
                stale_reload_id = ?f.reload_id,
                "stale reload file retired unapplied (latest-id wins)"
            );
            ids.push(f.id);
            continue;
        }
        // Skip a version-crossing non-reload file a pending rebuild will supersede: leave
        // it `ready` (do NOT append, advance the frontier, or delete it) so the rebuild's
        // `delete_superseded` purges it, and so the loop reaches the reload chunk file that clears
        // the quarantine. Same-version files still apply normally and drop through the rebuild's
        // clear as the "wasted but harmless" pre-`W` backlog.
        if route == FileRoute::Live
            && f.kind != control::ManifestKind::Reload
            && f.schema_version > ctx.db.schema_version()?
            && supersede_floor.is_some_and(|floor| f.lsn_end <= floor)
        {
            tracing::warn!(
                table = %format_args!("{}.{}", ctx.schema, ctx.table),
                manifest_id = f.id.0,
                lsn_end = %f.lsn_end,
                schema_version = %f.schema_version,
                "skipping a version-crossing file superseded by a pending reload rebuild (quarantine-recovery in flight)"
            );
            continue;
        }
        if let FileRoute::Shadow(_) = &route
            && reload_build
                .as_ref()
                .is_none_or(|build| f.schema_version != build.schema_version)
        {
            return Err(LoaderError::Internal(format!(
                "manifest {} at schema version {} crossed frozen reload {} shape",
                f.id,
                f.schema_version,
                reload_build.as_ref().map_or_else(
                    || "<missing>".to_string(),
                    |build| build.reload_id.to_string()
                )
            )));
        }
        if f.kind == control::ManifestKind::Reload
            && let FileRoute::Shadow(_) = &route
        {
            let build = reload_build.as_ref().ok_or_else(|| {
                LoaderError::Internal(format!(
                    "reload manifest {} was routed to a shadow with no active build",
                    f.id
                ))
            })?;
            validate_reload_manifest_at_f(f, build)?;
        }
        if route == FileRoute::Live
            && f.schema_version > ctx.db.schema_version()?
            && let Err(e) = crate::ddl::reconcile_to_version(
                &ctx.db,
                &ctx.pool,
                ctx.epoch,
                &ctx.schema,
                &ctx.table,
                f.schema_version,
            )
            .await
        {
            // A lossy DDL cast that fails is a QUARANTINE: latch the state so `/ready`
            // degrades, fire a loud error-level alert, and stop — never a silent continue. The alert
            // names the table and the latch, not the error: this worker's `Err` drains the loader, so
            // `main` logs the failure itself (reason included) exactly once on the way out.
            if matches!(e, LoaderError::Quarantine { .. }) {
                ctx.state.quarantine();
                tracing::error!(
                    table = %format_args!("{}.{}", ctx.schema, ctx.table),
                    "QUARANTINE: lossy schema change could not be applied — /ready degraded, processing stopped"
                );
            }
            return Err(e);
        }
        // A `spill` file is one streamed txn written before its commit LSN was known, so its per-row
        // `commit_lsn` is a placeholder; `lsn_end` (corrected on `Stream Commit`) is the real commit LSN
        // for every row. Stamp it so the transform's commit-LSN window can't drop a neighbour txn that
        // committed inside the spill's placeholder range (architecture.md §1.6). Other kinds append verbatim.
        let commit_lsn_override =
            (f.kind == control::ManifestKind::Spill).then(|| f.lsn_end.to_string());
        let destination = match &route {
            FileRoute::Live => ctx.table.as_str(),
            FileRoute::Shadow(table) => table.as_str(),
            FileRoute::Retire => unreachable!("retired files continued above"),
        };
        appended += ctx.db.append_parquet(
            destination,
            f.id,
            &f.s3_uri,
            f.schema_version,
            commit_lsn_override.as_deref(),
        )?;
        max_lsn = max_lsn.max(f.lsn_end);
        ids.push(f.id);
    }

    // 3. ONE control-DB txn: advance the watermark to the batch max AND delete the claimed queue rows.
    //    (The append is already durable in DuckDB — step 2 committed.)
    if ids.is_empty() {
        return Ok(None);
    }
    let mut tx = ctx
        .pool
        .begin()
        .await
        .map_err(|source| LoaderError::ControlTxn {
            op: "begin advance+delete txn",
            source,
        })?;
    control::advance_raw_appended(&mut *tx, ctx.epoch, &ctx.schema, &ctx.table, max_lsn).await?;
    control::delete_claimed(&mut *tx, &ids).await?;
    tx.commit()
        .await
        .map_err(|source| LoaderError::ControlTxn {
            op: "commit advance+delete txn",
            source,
        })?;

    // Migration is intentionally deferred until a fresh control-plane read proves the queue is
    // empty. A prior release may have appended more files than this process's current `max_files`,
    // then crashed before deleting them; migrating after merely one batch would duplicate the rest.
    if ctx.db.has_legacy_replay_fence()
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
    let existing = ctx.db.reload_build()?;
    let ready =
        control::reload::ready_rebuild(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table).await?;
    let Some(row) = ready else {
        let Some(build) = existing else {
            return Ok(None);
        };
        let row = control::reload::get(&ctx.pool, build.reload_id).await?;
        let still_ready = if let Some(row) = &row
            && row.status == control::ReloadStatus::ExportComplete
        {
            let (start_lsn, schema_version, final_lsn) = rebuild_boundaries(ctx, row).await?;
            start_lsn == build.start_lsn
                && schema_version == build.schema_version
                && final_lsn == build.final_lsn
        } else {
            false
        };
        if still_ready {
            return Ok(Some(build));
        }
        return Err(LoaderError::Internal(format!(
            "unpublished reload {} is no longer export_complete; a newer marker-ready attempt is required to supersede it",
            build.reload_id
        )));
    };
    let (start_lsn, schema_version, final_lsn) = rebuild_boundaries(ctx, &row).await?;
    let plan = plan_at_version(ctx, schema_version).await?;
    let BeginReload::Ready(build) =
        ctx.db
            .begin_reload_shadow(&plan, schema_version, row.reload_id, start_lsn, final_lsn)?
    else {
        return Ok(None);
    };
    let purged =
        control::delete_superseded(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, start_lsn)
            .await?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        reload_id = %row.reload_id,
        schema_version = %schema_version,
        start_lsn = %start_lsn,
        final_lsn = %final_lsn,
        purged,
        "reload reconciliation started in a hidden generation"
    );
    Ok(Some(build))
}

async fn rebuild_boundaries(
    ctx: &TableCtx,
    row: &control::ReloadRow,
) -> Result<(Lsn, SchemaVersionNo, Lsn), LoaderError> {
    let schema_version = row.schema_version.ok_or_else(|| {
        LoaderError::Internal(format!(
            "reload {} has no frozen schema version",
            row.reload_id
        ))
    })?;
    let final_lsn = row.final_lsn.ok_or_else(|| {
        LoaderError::Internal(format!("reload {} has no end boundary", row.reload_id))
    })?;
    let start_lsn = row.start_lsn.ok_or_else(|| {
        LoaderError::Internal(format!(
            "reload {} has no durable start fence",
            row.reload_id
        ))
    })?;
    if final_lsn < start_lsn {
        return Err(LoaderError::Internal(format!(
            "reload {} has inverted boundaries: H {final_lsn} precedes F {start_lsn}",
            row.reload_id
        )));
    }

    let markers = control::reload::read_markers(&ctx.pool, row.reload_id).await?;
    let baseline = markers
        .iter()
        .find(|marker| marker.kind == control::ReloadMarkerKind::Baseline);
    let end = markers
        .iter()
        .find(|marker| marker.kind == control::ReloadMarkerKind::End);
    let valid = baseline
        .is_some_and(|marker| marker.lsn == start_lsn && marker.schema_version == schema_version)
        && end.is_some_and(|marker| {
            marker.lsn == final_lsn && marker.schema_version == schema_version
        })
        && markers.len() == 2;
    if !valid {
        return Err(LoaderError::Internal(format!(
            "reload {} boundaries do not match its durable baseline/end markers",
            row.reload_id
        )));
    }
    Ok((start_lsn, schema_version, final_lsn))
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

    // Greater (or unlatched): the first file of a NEW attempt. Both persisted flavors use the same
    // hidden-generation reconciliation; `resync` is retained only as a compatibility spelling.
    let row = control::reload::get(&ctx.pool, file_reload_id)
        .await?
        .ok_or_else(|| {
            LoaderError::Internal(format!(
                "reload {file_reload_id} has chunk files but no table_reload row"
            ))
        })?;
    let (start_lsn, schema_version, final_lsn) = rebuild_boundaries(ctx, &row).await?;
    if f.schema_version != schema_version {
        return Err(LoaderError::Internal(format!(
            "reload {file_reload_id} chunk schema {} differs from frozen schema {schema_version}",
            f.schema_version
        )));
    }
    if f.lsn_end > final_lsn {
        return Err(LoaderError::Internal(format!(
            "reload {file_reload_id} chunk {} lies beyond end marker {final_lsn}",
            f.id
        )));
    }

    // Build both replacement tables under a hidden deterministic name. The public/live generation
    // remains untouched until Phase B reaches the explicit H barrier.
    let plan = plan_at_version(ctx, schema_version).await?;
    let BeginReload::Ready(build) =
        ctx.db
            .begin_reload_shadow(&plan, schema_version, file_reload_id, start_lsn, final_lsn)?
    else {
        return Ok(FileRoute::Retire);
    };
    // Purge superseded pending rows: every non-reload file at lsn_end <= the start fence describes a
    // commit the consistent baseline re-covers; applying it after replacement would only churn.
    // Post-F stream files survive and apply after the baseline chunks in (lsn_end, id) order.
    let purged =
        control::delete_superseded(&ctx.pool, ctx.epoch, &ctx.schema, &ctx.table, start_lsn)
            .await?;
    tracing::info!(
        table = %format_args!("{}.{}", ctx.schema, ctx.table),
        reload_id = %file_reload_id,
        schema_version = %schema_version,
        start_lsn = %start_lsn,
        final_lsn = %final_lsn,
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
