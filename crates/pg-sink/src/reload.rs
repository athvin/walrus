//! The reload controller: pickup, preflight, lease, concurrency cap (reload H6/H7/H11, PR 6.4).
//!
//! The export is a **sink-owned side task the replication loop never waits for** (H6): if the
//! stream stalled for one table's export, the single slot would lag for *every* table — the exact
//! failure the reload design exists to avoid. So the controller lives entirely off the decode
//! path: its own control-pg pool, its own source SQL connection for catalog preflight, and the
//! only shared state is the `Arc<WatermarkWaiters>` the decode loop resolves (PR 6.3).
//!
//! Each tick (the heartbeat cadence): claim up to *free-permit-count* `requested` rows
//! (`control::reload::claim_requested` — the DB guards double-claims), preflight each (fail fast
//! at request time, not mid-export — H11), and spawn an exporter per survivor under a
//! `tokio::sync::Semaphore` sized `max_concurrent_reloads` — "reload N tables" drains a queue
//! politely. Exporters renew their lease at TTL/3 for as long as they run; a lost lease cancels
//! the exporter. The exporter body is PR 6.5's chunk
//! engine (`crate::reload_export`), driven under the lease guard below.
//!
//! The lease is liveness today and the future fence: under loader sharding (deferred goal §2),
//! `lease_holder` plus the `table_ownership` fencing-token pattern is how a stale sink would be
//! kept from double-exporting. Noted, deliberately not built (`replicas=1`).

use crate::reload_signal::WatermarkWaiters;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// A preflight either genuinely REJECTS the request (typed, terminal, operator-facing) or fails
/// for INFRA reasons (dead connection, timeout) — in which case the claim is released and retried
/// next tick. Conflating the two would let an idle-connection kill terminally fail a valid
/// request with a false "not in the publication" reason.
#[derive(Debug)]
pub enum PreflightOutcome {
    Rejected(PreflightRejection),
    Infra(anyhow::Error),
}

/// H11's fail-fast request validation: why a request never becomes an export. The reason lands in
/// `table_reload.error` verbatim, so the operator reads it off the row.
#[derive(Debug, thiserror::Error)]
pub enum PreflightRejection {
    #[error("table {0}.{1} is not in the publication")]
    NotPublished(String, String),
    #[error("table {0}.{1} has no primary key")]
    NoPrimaryKey(String, String),
}

/// Why a lease-guarded exporter ended (see [`lease_guarded_export`]).
#[derive(Debug)]
pub enum ExporterEnd {
    /// Shutdown: the row is deliberately left `exporting` with its lease running out — an expired
    /// lease on a non-terminal row is exactly what PR 6.9's startup scan adopts and resumes.
    Cancelled,
    /// A renewal found we no longer hold the lease (expired + adopted, or superseded). The export
    /// stops immediately; whoever holds the lease now owns the row.
    LostLease,
    /// The export future itself finished (PR 6.5 gives it real completion semantics).
    Finished(anyhow::Result<()>),
}

/// Drive `export` while renewing its lease every `renew_every`; the first failed renewal cancels
/// the export. Pure orchestration — the lease action and the export are injected, so the
/// cancel-on-lost-lease contract is unit-tested without a database.
pub async fn lease_guarded_export<R, RFut, E>(
    token: CancellationToken,
    renew_every: Duration,
    mut renew: R,
    export: E,
) -> ExporterEnd
where
    R: FnMut() -> RFut,
    RFut: std::future::Future<Output = anyhow::Result<bool>>,
    E: std::future::Future<Output = anyhow::Result<()>>,
{
    tokio::pin!(export);
    let mut renew_tick = tokio::time::interval(renew_every);
    renew_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    renew_tick.tick().await; // the immediate first tick — the claim just set the lease
    loop {
        tokio::select! {
            _ = token.cancelled() => return ExporterEnd::Cancelled,
            res = &mut export => return ExporterEnd::Finished(res),
            _ = renew_tick.tick() => {
                match renew().await {
                    Ok(true) => {}
                    Ok(false) => return ExporterEnd::LostLease,
                    // A transient renewal error is NOT a lost lease: keep exporting — the lease
                    // expiry is the real deadline, and the next tick retries.
                    Err(e) => tracing::warn!(error = %e, "reload lease renewal errored; retrying"),
                }
            }
        }
    }
}

/// The outcome of a mid-export DDL restart (PR 6.8 / H9).
#[derive(Debug)]
pub enum RestartDecision {
    /// A fresh successor at the new schema; keep exporting under this `reload_id`.
    Restarted(i64),
    /// `reload_max_restarts` is spent — the reload is now `failed`; stop.
    Capped,
}

/// Run [`control::reload::restart_for_ddl`] and emit the matching metric (PR 6.8). Split from the
/// export loop so a compose test drives this exact path — metric increment included — without
/// standing up the whole controller.
pub async fn handle_ddl_restart(
    pool: &sqlx::PgPool,
    old: &control::ReloadRow,
    new_version: i64,
    max_restarts: i32,
) -> anyhow::Result<RestartDecision> {
    let table = format!("{}.{}", old.source_schema, old.source_table);
    let mut conn = pool.acquire().await?;
    match control::reload::restart_for_ddl(&mut conn, old, new_version, max_restarts).await? {
        Some(new_id) => {
            common::metrics::record_reload_restart(&table);
            tracing::info!(
                old_reload_id = old.reload_id,
                new_reload_id = new_id,
                new_version,
                restart_count = old.restart_count + 1,
                "reload restarted at the new schema (DDL landed between chunks)"
            );
            Ok(RestartDecision::Restarted(new_id))
        }
        None => {
            common::metrics::record_reload_restart_cap_exhausted();
            common::metrics::record_reload_failed(&table);
            tracing::error!(
                reload_id = old.reload_id,
                new_version,
                max_restarts,
                "reload restart cap exhausted — attempt failed (visible waste, not silent corruption)"
            );
            Ok(RestartDecision::Capped)
        }
    }
}

/// The connections + config an exporter needs, bundled so the restart loop takes few args.
struct ExportDeps {
    source_db_url: String,
    pool: sqlx::PgPool,
    waiters: Arc<WatermarkWaiters>,
    sink: crate::sink::ParquetSink,
    export_cfg: crate::reload_export::ChunkExportConfig,
}

/// The exporter body under DDL-restart (PR 6.8 / H9): export until drained; on a mid-export
/// structural bump, fail-and-reissue via [`handle_ddl_restart`] and resume from chunk zero at the
/// new schema under the successor `reload_id` — or stop at the cap (the row is already `failed`).
/// `current_reload_id` is shared with the lease-renewal closure: repointing it to the successor
/// BEFORE the next await keeps renewal following the lease onto the new row (which
/// `restart_for_ddl` carried the lease onto), so a renewal tick never fails against the terminal
/// predecessor.
async fn export_with_ddl_restarts(
    deps: ExportDeps,
    mut req: control::ReloadRow,
    max_restarts: i32,
    current_reload_id: Arc<AtomicI64>,
) -> anyhow::Result<()> {
    use crate::reload_export::{ChunkExporter, RunOutcome};
    let pool = deps.pool;
    loop {
        let mut exporter = ChunkExporter::connect(
            &deps.source_db_url,
            pool.clone(),
            deps.waiters.clone(),
            deps.sink.clone(),
            deps.export_cfg.clone(),
            &req,
        )
        .await?;
        match exporter.run().await? {
            RunOutcome::Drained { final_lsn } => {
                // The sink's last act (PR 6.9 / H10): flip export_complete carrying H. The LOADER
                // then flips `complete` once transformed_lsn >= H — the sink never writes `complete`.
                control::reload::complete_export(&pool, req.reload_id, final_lsn).await?;
                tracing::info!(
                    reload_id = req.reload_id,
                    final_lsn = %final_lsn,
                    "reload export_complete (loader flips complete once transformed_lsn >= H)"
                );
                return Ok(());
            }
            RunOutcome::SchemaChanged { new_version } => {
                match handle_ddl_restart(&pool, &req, new_version, max_restarts).await? {
                    RestartDecision::Restarted(new_id) => {
                        current_reload_id.store(new_id, Ordering::SeqCst);
                        req = control::reload::get(&pool, new_id).await?.ok_or_else(|| {
                            anyhow::anyhow!("successor reload {new_id} vanished after restart")
                        })?;
                    }
                    RestartDecision::Capped => return Ok(()),
                }
            }
        }
    }
}

/// Everything the controller needs, cut from `SinkConfig` + bootstrap state.
#[derive(Clone)]
pub struct ReloadControllerConfig {
    /// Poll cadence — the heartbeat cadence (`heartbeat_idle_after`), per the task's contract.
    pub poll_interval: Duration,
    pub max_concurrent_reloads: usize,
    pub lease_ttl: Duration,
    /// `lease_holder` — the same identity the heartbeat/ownership machinery uses (never a second one).
    pub instance: String,
    pub publication_name: String,
    pub epoch: i64,
    /// Rows per chunk SELECT (PR 6.5).
    pub chunk_rows: u64,
    /// How long a chunk waits for its watermark echo before failing loudly (PR 6.5 / H11).
    pub echo_timeout: Duration,
    /// How many DDL-restarts a reload may consume before it fails (PR 6.8 / H9).
    pub reload_max_restarts: i32,
}

/// Sink-owned reload orchestration (H6). Never on the replication loop's path — it holds a
/// cloned handle of the control-pg pool and dials its OWN source connections; the only shared
/// state is the waiter registry.
pub struct ReloadController {
    pool: sqlx::PgPool,
    /// Catalog preflight dials a FRESH ordinary source connection per non-empty tick: reloads are
    /// rare operator events, and a held-forever idle client is exactly what proxies/failovers
    /// silently kill — a dead connection must never masquerade as a preflight rejection.
    source_db_url: String,
    /// Exporters subscribe here before signalling; the decode loop resolves (PR 6.3).
    waiters: Arc<WatermarkWaiters>,
    /// Each exporter clones a handle: chunk Parquet lands in the same epoch-prefixed layout.
    sink: crate::sink::ParquetSink,
    cfg: ReloadControllerConfig,
    semaphore: Arc<Semaphore>,
    token: CancellationToken,
}

impl ReloadController {
    /// Spawn the controller task next to the heartbeat. Failures inside the task are logged and
    /// retried next tick — the controller can degrade, never take the sink down.
    pub fn spawn(
        pool: sqlx::PgPool,
        source_db_url: &str,
        waiters: Arc<WatermarkWaiters>,
        sink: crate::sink::ParquetSink,
        cfg: ReloadControllerConfig,
        token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let controller = ReloadController {
            pool,
            source_db_url: source_db_url.to_string(),
            waiters,
            sink,
            semaphore: Arc::new(Semaphore::new(cfg.max_concurrent_reloads)),
            token: token.clone(),
            cfg,
        };
        tokio::spawn(async move {
            // Startup crash-recovery (PR 6.9): adopt + resume our own / orphaned exporting reloads
            // ONCE, before the tick loop, unless we're already shutting down.
            if !token.is_cancelled() {
                controller.adopt_and_resume().await;
            }
            let mut tick = tokio::time::interval(controller.cfg.poll_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        // Graceful shutdown: exporters see the same token and end Cancelled —
                        // their rows stay `exporting` for PR 6.9's startup scan to resume.
                        tracing::info!("reload controller cancelled");
                        return;
                    }
                    _ = tick.tick() => {
                        // The tick itself races the token too: a wedged claim/preflight must
                        // never block `handle.await` in the shutdown path. A mid-claim drop can
                        // leave rows `exporting` with a dying lease — expiry + PR 6.9's adoption
                        // is the designed net for exactly that.
                        tokio::select! {
                            _ = token.cancelled() => {
                                tracing::info!("reload controller cancelled mid-tick");
                                return;
                            }
                            res = controller.tick() => {
                                if let Err(e) = res {
                                    tracing::warn!(error = %e, "reload controller tick failed; retrying next tick");
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    /// One tick: claim ≤ free-permit `requested` rows, preflight each, spawn exporters for the
    /// survivors. Claiming only what can run keeps queued requests in `requested`, where another
    /// (future) sink instance — or the next tick — can pick them up.
    ///
    /// Error discipline after the claim: **never `?` inside the per-row loop.** The claim already
    /// flipped every row to `exporting`, so an early return would orphan the siblings — claimed,
    /// leased, but with no exporter and no way back (`claim_requested` only sees `requested`).
    /// A typed rejection `fail`s its row; an INFRA error (dead source connection, control-pg
    /// blip) `release_claim`s it back to `requested` for the next tick — an infra failure must
    /// never be recorded as a terminal, operator-misleading preflight rejection.
    async fn tick(&self) -> anyhow::Result<()> {
        // Surface genuinely stuck exports every tick (PR 6.9) — independent of free permits, and
        // best-effort so a transient control-pg blip on this read never skips the claim below.
        if let Err(e) = self.warn_stuck().await {
            tracing::debug!(error = %e, "stuck-reload scan failed this tick");
        }
        let free = self.semaphore.available_permits();
        if free == 0 {
            return Ok(());
        }
        // A single guarded UPDATE: if THIS errors, nothing was claimed — safe to propagate.
        let claimed = control::reload::claim_requested(
            &self.pool,
            self.cfg.epoch,
            &self.cfg.instance,
            self.cfg.lease_ttl.as_secs() as i64,
            free as i64,
        )
        .await?;
        if claimed.is_empty() {
            return Ok(());
        }
        // Fresh preflight connection per non-empty tick (see the field doc). If the SOURCE is
        // unreachable, no preflight can be trusted: release every claim and retry next tick.
        let source = match crate::preflight::connect_source(&self.source_db_url).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    claims = claimed.len(),
                    "preflight source connection failed; releasing claims to retry next tick"
                );
                for req in &claimed {
                    self.release_row(req).await;
                }
                return Ok(());
            }
        };
        for req in claimed {
            match self.preflight(&source, &req).await {
                Ok(()) => {}
                Err(PreflightOutcome::Rejected(rejection)) => {
                    tracing::warn!(
                        reload_id = req.reload_id,
                        source_table = %format_args!("{}.{}", req.source_schema, req.source_table),
                        reason = %rejection,
                        "reload request rejected at preflight"
                    );
                    common::metrics::record_reload_failed(&format!(
                        "{}.{}",
                        req.source_schema, req.source_table
                    ));
                    if let Err(e) = self.fail_row(req.reload_id, &rejection.to_string()).await {
                        tracing::error!(
                            reload_id = req.reload_id,
                            error = %e,
                            "could not record the rejection; releasing the claim instead"
                        );
                        self.release_row(&req).await;
                    }
                    continue;
                }
                Err(PreflightOutcome::Infra(e)) => {
                    tracing::warn!(
                        reload_id = req.reload_id,
                        error = %e,
                        "preflight infra error (NOT a rejection); releasing the claim to retry"
                    );
                    self.release_row(&req).await;
                    continue;
                }
            }
            // The permit is held INSIDE the spawned task — dropping it on task exit frees the slot.
            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    // All permits raced away within this tick (can't happen while this controller
                    // is the only claimant; harmless if it ever does): leave the row `exporting`
                    // with its lease — expiry + PR 6.9's adoption recover it.
                    tracing::warn!(reload_id = req.reload_id, "no free permit after claim");
                    continue;
                }
            };
            tracing::info!(
                reload_id = req.reload_id,
                source_table = %format_args!("{}.{}", req.source_schema, req.source_table),
                flavor = req.flavor.as_str(),
                "reload claimed → exporting; exporter scheduled"
            );
            self.spawn_exporter(req, permit);
        }
        Ok(())
    }

    /// Spawn the lease-guarded exporter for a claimed or adopted reload, holding `permit` inside the
    /// task so the slot frees on exit. Shared by ordinary pickup ([`tick`]) and crash-recovery
    /// ([`adopt_and_resume`]) — both hand it a row already `exporting` with a fresh lease.
    fn spawn_exporter(&self, req: control::ReloadRow, permit: tokio::sync::OwnedSemaphorePermit) {
        let pool = self.pool.clone();
        let holder = self.cfg.instance.clone();
        let ttl = self.cfg.lease_ttl;
        let max_restarts = self.cfg.reload_max_restarts;
        let child = self.token.child_token();
        let export_cfg = crate::reload_export::ChunkExportConfig {
            chunk_rows: self.cfg.chunk_rows,
            echo_timeout: self.cfg.echo_timeout,
            instance: self.cfg.instance.clone(),
            epoch: self.cfg.epoch,
        };
        let source_db_url = self.source_db_url.clone();
        let waiters = self.waiters.clone();
        let sink = self.sink.clone();
        // The reload-active gauge (PR 6.11): +1 for this exporter task's flavor now, -1 when it
        // ends (any exit path). The flavor is stable across DDL-restarts, so one task = one count.
        let flavor = req.flavor.as_str();
        common::metrics::inc_reload_active(flavor);
        tokio::spawn(async move {
            let _permit = permit;
            // The lease-renewal target: the export loop repoints this on every DDL-restart, so
            // renewal follows the lease onto each successor row (PR 6.8).
            let current_reload_id = Arc::new(AtomicI64::new(req.reload_id));
            let renew_pool = pool.clone();
            let renew_id = current_reload_id.clone();
            // The chunk engine (PR 6.5) under DDL-restart (PR 6.8): dial the side connection,
            // resume from the cursor, export until drained — restarting at the new schema if DDL
            // bumps the version mid-export, then flipping export_complete (PR 6.9). Echo timeout
            // fails the row inside; any other error leaves it `exporting` for lease-expiry + PR
            // 6.9's adoption (infra errors are retried, never terminally mis-recorded).
            let export = export_with_ddl_restarts(
                ExportDeps {
                    source_db_url,
                    pool,
                    waiters,
                    sink,
                    export_cfg,
                },
                req,
                max_restarts,
                current_reload_id.clone(),
            );
            let end = lease_guarded_export(
                child,
                ttl / 3,
                move || {
                    let pool = renew_pool.clone();
                    let holder = holder.clone();
                    let reload_id = renew_id.load(Ordering::SeqCst);
                    async move {
                        Ok(control::reload::renew_lease(
                            &pool,
                            reload_id,
                            &holder,
                            ttl.as_secs() as i64,
                        )
                        .await?)
                    }
                },
                export,
            )
            .await;
            let reload_id = current_reload_id.load(Ordering::SeqCst);
            match &end {
                ExporterEnd::Cancelled => tracing::info!(
                    reload_id,
                    "exporter cancelled (shutdown); row left for startup-scan resume (PR 6.9)"
                ),
                ExporterEnd::LostLease => tracing::warn!(
                    reload_id,
                    "exporter lost its lease; stopping (another holder owns the row now)"
                ),
                ExporterEnd::Finished(res) => match res {
                    Ok(()) => tracing::info!(reload_id, "export finished"),
                    Err(e) => tracing::error!(reload_id, error = %e, "export failed"),
                },
            }
            common::metrics::dec_reload_active(flavor); // balances the inc above (PR 6.11)
        });
    }

    /// Startup crash-recovery (PR 6.9 / H7): adopt this sink's own / orphaned `exporting` reloads
    /// (re-acquiring each lease in a race-safe guarded UPDATE) and resume them from the chunk cursor
    /// — NOT from WAL redelivery, which is long gone. Runs ONCE before the tick loop: `adopt_resumable`'s
    /// `lease_holder = me` clause is only safe before any exporter of ours is live (afterwards a live
    /// row would be re-adopted into a duplicate). Bounded by the free permits, so it never oversubscribes.
    async fn adopt_and_resume(&self) {
        let free = self.semaphore.available_permits();
        if free == 0 {
            return;
        }
        let adopted = match control::reload::adopt_resumable(
            &self.pool,
            self.cfg.epoch,
            &self.cfg.instance,
            self.cfg.lease_ttl.as_secs() as i64,
            free as i64,
        )
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "startup reload-adoption scan failed; requested reloads still pick up per tick");
                return;
            }
        };
        for req in adopted {
            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    tracing::warn!(
                        reload_id = req.reload_id,
                        "no free permit to resume an adopted reload; leaving it (lease re-acquired)"
                    );
                    continue;
                }
            };
            tracing::info!(
                reload_id = req.reload_id,
                source_table = %format_args!("{}.{}", req.source_schema, req.source_table),
                status = req.status.as_str(),
                cursor_chunk = req.chunk_no,
                "adopting reload (crash recovery); resuming from the cursor"
            );
            self.spawn_exporter(req, permit);
        }
    }

    /// Per-tick surfacing (PR 6.9/6.11): genuinely stuck exports — `exporting`, lease expired,
    /// nobody renewing — are warned per row AND counted into the `walrus_reload_lease_stale` gauge
    /// the stuck-lease alert reads (a gauge, so the alert never queries control-pg).
    async fn warn_stuck(&self) -> anyhow::Result<()> {
        let stuck = control::reload::stuck_exporting(&self.pool, self.cfg.epoch).await?;
        common::metrics::set_reload_lease_stale(stuck.len() as u64);
        for (reload_id, holder) in stuck {
            tracing::warn!(
                reload_id,
                lease_holder = ?holder,
                "reload stuck: exporting with an expired, unadopted lease (no live exporter renewing it)"
            );
        }
        Ok(())
    }

    /// Record a typed rejection on the row (its reason IS the operator UX).
    async fn fail_row(&self, reload_id: i64, reason: &str) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        control::reload::fail(&mut conn, reload_id, reason).await?;
        Ok(())
    }

    /// Un-claim a row after an infra failure: back to `requested` for the next tick. If even the
    /// release fails, the row stays `exporting` with a dying lease — expiry + PR 6.9's adoption
    /// is the recovery net, and the error log is the operator breadcrumb.
    async fn release_row(&self, req: &control::ReloadRow) {
        match control::reload::release_claim(&self.pool, req.reload_id, &self.cfg.instance).await {
            Ok(true) => tracing::info!(
                reload_id = req.reload_id,
                "claim released → requested (retried next tick)"
            ),
            Ok(false) => tracing::warn!(
                reload_id = req.reload_id,
                "claim no longer ours to release; leaving it"
            ),
            Err(e) => tracing::error!(
                reload_id = req.reload_id,
                error = %e,
                "release failed; row stays exporting — lease expiry + PR 6.9 adoption recover it"
            ),
        }
    }

    /// H11, fail-fast: target in the publication, target has a PK, flavor implementable. Runs
    /// BEFORE a single signal row or chunk is spent on a doomed reload. A catalog query error is
    /// an [`PreflightOutcome::Infra`] failure, NEVER a rejection — a dead connection must not
    /// terminally fail a valid request with a false reason.
    async fn preflight(
        &self,
        source: &tokio_postgres::Client,
        req: &control::ReloadRow,
    ) -> Result<(), PreflightOutcome> {
        // Both flavors preflight identically (PR 6.10): in the publication + has a PK. `resync`
        // needs no special guard — it merges chunks over the live mirror on the loader side, no
        // pause and no rebuild; only the semantics differ, not the export.
        let published = source
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_publication_tables
                                WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)",
                &[
                    &self.cfg.publication_name,
                    &req.source_schema,
                    &req.source_table,
                ],
            )
            .await
            .map(|row| row.get::<_, bool>(0))
            .map_err(|e| PreflightOutcome::Infra(anyhow::anyhow!(e)))?;
        if !published {
            return Err(PreflightOutcome::Rejected(
                PreflightRejection::NotPublished(
                    req.source_schema.clone(),
                    req.source_table.clone(),
                ),
            ));
        }
        let has_pk = source
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_index i
                                JOIN pg_class c ON c.oid = i.indrelid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary)",
                &[&req.source_schema, &req.source_table],
            )
            .await
            .map(|row| row.get::<_, bool>(0))
            .map_err(|e| PreflightOutcome::Infra(anyhow::anyhow!(e)))?;
        if !has_pk {
            return Err(PreflightOutcome::Rejected(
                PreflightRejection::NoPrimaryKey(
                    req.source_schema.clone(),
                    req.source_table.clone(),
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "reload_test.rs"]
mod tests;
