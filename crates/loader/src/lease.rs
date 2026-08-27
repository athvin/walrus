//! The ownership lease (loader §8.1) — the **first** fence, acquired before DuckDB's file lock. Wraps
//! [`control::table_ownership`]: a live owner → terminal [`LoaderError::LeaseContended`]; an expired
//! lease → reclaim. Renewal runs on a background task well under the TTL so a busy apply loop can never
//! let the lease lapse and admit a phantom second writer.

use crate::error::LoaderError;
use common::EpochNo;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Floor for the renew cadence: never tick sub-second, however small the admitted TTL.
pub(crate) const MIN_RENEW_INTERVAL: Duration = Duration::from_secs(1);

/// Return one third of `ttl`, bounded into `[MIN_RENEW_INTERVAL, ttl]`.
///
/// The upper bound is a correctness fence: renewal at or after expiry could admit a second writer.
/// [`crate::config::LoaderConfig::validate`] establishes `MIN_RENEW_INTERVAL <= ttl`, which is the
/// `clamp` precondition, before the renewer is spawned.
fn renew_interval(ttl: Duration) -> Duration {
    debug_assert!(
        ttl >= MIN_RENEW_INTERVAL,
        "config must reject lease_ttl < 3s (clamp precondition)"
    );
    (ttl / 3).clamp(MIN_RENEW_INTERVAL, ttl)
}

/// Convert a lease TTL to the control plane's signed seconds, saturating oversized durations.
fn ttl_secs(ttl: Duration) -> i64 {
    i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX)
}

/// Acquire (or reclaim) the lease for one table. `Ok` only when the lease is free or already ours;
/// a live owner is terminal.
///
/// # Errors
///
/// Returns [`LoaderError::Control`] if the lease query fails, or
/// [`LoaderError::LeaseContended`] when another live pod owns the table.
pub async fn acquire(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    self_pod: &str,
    ttl: Duration,
) -> Result<control::Lease, LoaderError> {
    control::acquire_lease(pool, epoch, schema, table, self_pod, ttl_secs(ttl))
        .await?
        .ok_or_else(|| LoaderError::LeaseContended {
            table: format!("{schema}.{table}"),
            owner: "another live pod".to_string(),
        })
}

/// Renew every owned table's lease every `ttl/3`, off the apply-loop thread, until cancelled.
#[must_use]
pub fn spawn_renewer(
    pool: sqlx::PgPool,
    epoch: EpochNo,
    keys: Vec<(String, String)>,
    self_pod: String,
    ttl: Duration,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(renew_interval(ttl));
        loop {
            tokio::select! {
                biased;
                _ = token.cancelled() => return,
                _ = tick.tick() => {
                    for (schema, table) in &keys {
                        match control::renew_lease(&pool, epoch, schema, table, &self_pod, ttl_secs(ttl)).await {
                            Ok(true) => {}
                            Ok(false) => tracing::error!(table = %format_args!("{schema}.{table}"), "lease lost — no longer owner"),
                            Err(e) => tracing::warn!(error = %e, "lease renew failed (will retry)"),
                        }
                    }
                }
            }
        }
    })
}

/// Release every owned table's lease on graceful shutdown (best-effort).
pub async fn release_all(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    keys: &[(String, String)],
    self_pod: &str,
) {
    for (schema, table) in keys {
        if let Err(e) = control::release_lease(pool, epoch, schema, table, self_pod).await {
            tracing::warn!(error = %e, "lease release failed");
        }
    }
}

#[cfg(test)]
#[path = "lease_test.rs"]
mod tests;
