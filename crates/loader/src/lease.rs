//! The ownership lease (loader §8.1) — the **first** fence, acquired before DuckDB's file lock. Wraps
//! [`control::table_ownership`]: a live owner → terminal [`LoaderError::LeaseContended`]; an expired
//! lease → reclaim. Renewal runs on a background task well under the TTL so a busy apply loop can never
//! let the lease lapse and admit a phantom second writer.

use crate::error::LoaderError;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Acquire (or reclaim) the lease for one table. `Ok` only when the lease is free or already ours;
/// a live owner is terminal.
pub async fn acquire(
    pool: &sqlx::PgPool,
    epoch: i64,
    schema: &str,
    table: &str,
    self_pod: &str,
    ttl: Duration,
) -> Result<control::Lease, LoaderError> {
    match control::acquire_lease(pool, epoch, schema, table, self_pod, ttl.as_secs() as i64).await?
    {
        Some(lease) => Ok(lease),
        None => Err(LoaderError::LeaseContended {
            table: format!("{schema}.{table}"),
            owner: "another live pod".to_string(),
        }),
    }
}

/// Renew every owned table's lease every `ttl/3`, off the apply-loop thread, until cancelled.
pub fn spawn_renewer(
    pool: sqlx::PgPool,
    epoch: i64,
    keys: Vec<(String, String)>,
    self_pod: String,
    ttl: Duration,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval((ttl / 3).max(Duration::from_secs(1)));
        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                _ = tick.tick() => {
                    for (schema, table) in &keys {
                        match control::renew_lease(&pool, epoch, schema, table, &self_pod, ttl.as_secs() as i64).await {
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
    epoch: i64,
    keys: &[(String, String)],
    self_pod: &str,
) {
    for (schema, table) in keys {
        if let Err(e) = control::release_lease(pool, epoch, schema, table, self_pod).await {
            tracing::warn!(error = %e, "lease release failed");
        }
    }
}
