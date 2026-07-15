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
//! the exporter. The exporter body itself is PR 6.5's chunk engine — here it parks, so the
//! scheduling is observable.
//!
//! The lease is liveness today and the future fence: under loader sharding (deferred goal §2),
//! `lease_holder` plus the `table_ownership` fencing-token pattern is how a stale sink would be
//! kept from double-exporting. Noted, deliberately not built (`replicas=1`).

use crate::reload_signal::WatermarkWaiters;
use control::reload::ReloadFlavor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// H11's fail-fast request validation: why a request never becomes an export. The reason lands in
/// `table_reload.error` verbatim, so the operator reads it off the row.
#[derive(Debug, thiserror::Error)]
pub enum PreflightRejection {
    #[error("table {0}.{1} is not in the publication")]
    NotPublished(String, String),
    #[error("table {0}.{1} has no primary key")]
    NoPrimaryKey(String, String),
    #[error("flavor 'resync' lands in PR 6.10")]
    ResyncNotYetImplemented,
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
}

/// Sink-owned reload orchestration (H6). Never on the replication loop's path.
pub struct ReloadController {
    pool: sqlx::PgPool,
    /// Catalog preflight runs over this ordinary source SQL connection (the heartbeat shape).
    source: tokio_postgres::Client,
    #[allow(dead_code)] // handed to exporter tasks in PR 6.5 (subscribe-then-insert)
    waiters: Arc<WatermarkWaiters>,
    cfg: ReloadControllerConfig,
    semaphore: Arc<Semaphore>,
    token: CancellationToken,
}

impl ReloadController {
    /// Connect the side SQL connection and spawn the controller task next to the heartbeat.
    /// Failures inside the task are logged and retried next tick — the controller can degrade,
    /// never take the sink down.
    pub async fn spawn(
        pool: sqlx::PgPool,
        source_db_url: &str,
        waiters: Arc<WatermarkWaiters>,
        cfg: ReloadControllerConfig,
        token: CancellationToken,
    ) -> anyhow::Result<tokio::task::JoinHandle<()>> {
        let (source, connection) = tokio_postgres::connect(source_db_url, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!("reload controller source connection: {e}"))?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(error = %e, "reload controller source connection closed");
            }
        });
        let controller = ReloadController {
            pool,
            source,
            waiters,
            semaphore: Arc::new(Semaphore::new(cfg.max_concurrent_reloads)),
            token: token.clone(),
            cfg,
        };
        Ok(tokio::spawn(async move {
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
                        if let Err(e) = controller.tick().await {
                            tracing::warn!(error = %e, "reload controller tick failed; retrying next tick");
                        }
                    }
                }
            }
        }))
    }

    /// One tick: claim ≤ free-permit `requested` rows, preflight each, spawn exporters for the
    /// survivors. Claiming only what can run keeps queued requests in `requested`, where another
    /// (future) sink instance — or the next tick — can pick them up.
    async fn tick(&self) -> anyhow::Result<()> {
        let free = self.semaphore.available_permits();
        if free == 0 {
            return Ok(());
        }
        let claimed = control::reload::claim_requested(
            &self.pool,
            self.cfg.epoch,
            &self.cfg.instance,
            self.cfg.lease_ttl.as_secs() as i64,
            free as i64,
        )
        .await?;
        for req in claimed {
            if let Err(rejection) = self.preflight(&req).await {
                tracing::warn!(
                    reload_id = req.reload_id,
                    source_table = %format_args!("{}.{}", req.source_schema, req.source_table),
                    reason = %rejection,
                    "reload request rejected at preflight"
                );
                let mut conn = self.pool.acquire().await?;
                control::reload::fail(&mut conn, req.reload_id, &rejection.to_string()).await?;
                continue;
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
            let pool = self.pool.clone();
            let holder = self.cfg.instance.clone();
            let ttl = self.cfg.lease_ttl;
            let child = self.token.child_token();
            tracing::info!(
                reload_id = req.reload_id,
                source_table = %format_args!("{}.{}", req.source_schema, req.source_table),
                flavor = req.flavor.as_str(),
                "reload claimed → exporting; exporter scheduled"
            );
            tokio::spawn(async move {
                let _permit = permit;
                let reload_id = req.reload_id;
                let renew_pool = pool.clone();
                let end = lease_guarded_export(
                    child,
                    ttl / 3,
                    move || {
                        let pool = renew_pool.clone();
                        let holder = holder.clone();
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
                    export_table(req),
                )
                .await;
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
            });
        }
        Ok(())
    }

    /// H11, fail-fast: target in the publication, target has a PK, flavor implementable. Runs
    /// BEFORE a single signal row or chunk is spent on a doomed reload.
    async fn preflight(&self, req: &control::ReloadRow) -> Result<(), PreflightRejection> {
        if req.flavor == ReloadFlavor::Resync {
            return Err(PreflightRejection::ResyncNotYetImplemented);
        }
        let published = self
            .source
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
            .unwrap_or(false);
        if !published {
            return Err(PreflightRejection::NotPublished(
                req.source_schema.clone(),
                req.source_table.clone(),
            ));
        }
        let has_pk = self
            .source
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_index i
                                JOIN pg_class c ON c.oid = i.indrelid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary)",
                &[&req.source_schema, &req.source_table],
            )
            .await
            .map(|row| row.get::<_, bool>(0))
            .unwrap_or(false);
        if !has_pk {
            return Err(PreflightRejection::NoPrimaryKey(
                req.source_schema.clone(),
                req.source_table.clone(),
            ));
        }
        Ok(())
    }
}

/// PR 6.5 replaces this stub with the chunk engine (watermark INSERT → echo await → chunk SELECT
/// → stamped Parquet → cursor advance). Here it parks until cancelled, holding its permit, so the
/// semaphore's scheduling — and the lease renewal around it — are observable.
async fn export_table(req: control::ReloadRow) -> anyhow::Result<()> {
    tracing::info!(
        reload_id = req.reload_id,
        "export stub parked (chunk engine lands in PR 6.5)"
    );
    std::future::pending::<()>().await;
    unreachable!("pending() never resolves")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn preflight_rejections_read_as_operator_reasons() {
        // These strings land verbatim in table_reload.error — they ARE the operator UX.
        assert_eq!(
            PreflightRejection::NotPublished("public".into(), "ghost".into()).to_string(),
            "table public.ghost is not in the publication"
        );
        assert_eq!(
            PreflightRejection::NoPrimaryKey("public".into(), "keyless".into()).to_string(),
            "table public.keyless has no primary key"
        );
        assert!(PreflightRejection::ResyncNotYetImplemented
            .to_string()
            .contains("PR 6.10"));
    }

    #[tokio::test(start_paused = true)]
    async fn lost_lease_cancels_the_exporter() {
        let token = CancellationToken::new();
        // First renewal succeeds, second reports the lease gone.
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = calls.clone();
        let end = lease_guarded_export(
            token,
            Duration::from_secs(20),
            move || {
                let n = calls_in.fetch_add(1, Ordering::SeqCst);
                async move { Ok(n == 0) }
            },
            async {
                std::future::pending::<()>().await;
                unreachable!()
            },
        )
        .await;
        assert!(matches!(end, ExporterEnd::LostLease), "got {end:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_renewal_errors_do_not_cancel() {
        let token = CancellationToken::new();
        let cancel = token.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_in = calls.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(70)).await;
            cancel.cancel();
        });
        let end = lease_guarded_export(
            token,
            Duration::from_secs(20),
            move || {
                calls_in.fetch_add(1, Ordering::SeqCst);
                async move { Err(anyhow::anyhow!("control-pg blinked")) }
            },
            async {
                std::future::pending::<()>().await;
                unreachable!()
            },
        )
        .await;
        // Errors are retried (the lease expiry is the real deadline); only cancellation ends it.
        assert!(matches!(end, ExporterEnd::Cancelled), "got {end:?}");
        assert!(calls.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn cap_of_two_schedules_third_request_only_after_a_permit_frees() {
        // The scheduling shape `tick` relies on: permits live inside the exporter tasks, so a
        // third export starts only when one of the first two releases its permit.
        let semaphore = Arc::new(Semaphore::new(2));
        let started: Vec<Arc<AtomicUsize>> =
            (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let mut releases = Vec::new();
        let mut handles = Vec::new();
        for flag in &started {
            let sem = semaphore.clone();
            let flag = flag.clone();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            releases.push(Some(tx));
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.unwrap();
                flag.store(1, Ordering::SeqCst);
                let _ = rx.await; // park like the PR 6.5 stub, holding the permit
            }));
        }

        // Two acquire; the third is parked on the semaphore.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let started_count = || {
            started
                .iter()
                .filter(|f| f.load(Ordering::SeqCst) == 1)
                .count()
        };
        assert_eq!(started_count(), 2, "cap of two holds; the third waits");

        // Free exactly ONE permit (release a task that actually started): the third now runs.
        let running_idx = started
            .iter()
            .position(|f| f.load(Ordering::SeqCst) == 1)
            .unwrap();
        releases[running_idx].take().unwrap().send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(started_count(), 3, "the third started once a permit freed");

        for tx in releases.into_iter().flatten() {
            let _ = tx.send(());
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(semaphore.available_permits(), 2, "permits returned on exit");
    }
}
