//! `table_ownership` — the loader's cooperative single-writer lease (loader §8.1, PR 3.1).
//!
//! The FIRST fence: a control-plane row per owned `(epoch, schema, table)` with a monotonic
//! `fencing_token`, acquired **before** the loader takes DuckDB's read-write file lock (the second
//! fence). A live owner keeps `lease_expiry` in the future by renewing well under the TTL; a dead PID's
//! lease simply expires and is reclaimable. All time comparisons happen in SQL (`now()`), so the Rust
//! side never needs a timestamp type.

use crate::ControlError;
use sqlx::PgExecutor;

/// A held lease. The `fencing_token` bumps only when ownership changes hands (dormant at `replicas=1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub fencing_token: i64,
    pub owner_pod: String,
}

/// Conditionally acquire (or renew) the lease for `ttl_secs`: succeeds iff the lease is **free**
/// (expired) or **already ours**. Returns `Ok(None)` when a *live* owner holds it — the caller maps
/// that to the terminal [`common::ExitCode::LeaseContended`]. On a change of owner the token bumps by 1.
pub async fn acquire_lease(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
    self_pod: &str,
    ttl_secs: i64,
) -> Result<Option<Lease>, ControlError> {
    let row = sqlx::query_as::<_, (i64, String)>(
        r#"
        INSERT INTO walrus.table_ownership
            (epoch, source_schema, source_table, owner_pod, fencing_token, lease_expiry, updated_at)
        VALUES ($1, $2, $3, $4, 1, now() + make_interval(secs => $5), now())
        ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
        SET owner_pod = EXCLUDED.owner_pod,
            fencing_token = walrus.table_ownership.fencing_token
                + (CASE WHEN walrus.table_ownership.owner_pod <> EXCLUDED.owner_pod THEN 1 ELSE 0 END),
            lease_expiry = EXCLUDED.lease_expiry,
            updated_at = now()
        WHERE walrus.table_ownership.lease_expiry < now()
           OR walrus.table_ownership.owner_pod = EXCLUDED.owner_pod
        RETURNING fencing_token, owner_pod
        "#,
    )
    .bind(epoch)
    .bind(schema)
    .bind(table)
    .bind(self_pod)
    .bind(ttl_secs as f64)
    .fetch_optional(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(row.map(|(fencing_token, owner_pod)| Lease {
        fencing_token,
        owner_pod,
    }))
}

/// Renew our lease (extend `lease_expiry`), off the apply-loop thread and well under the TTL. Fails to
/// affect any row if we are no longer the owner (a phantom writer must not renew).
pub async fn renew_lease(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
    self_pod: &str,
    ttl_secs: i64,
) -> Result<bool, ControlError> {
    let done = sqlx::query(
        r#"
        UPDATE walrus.table_ownership
        SET lease_expiry = now() + make_interval(secs => $5), updated_at = now()
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND owner_pod = $4
        "#,
    )
    .bind(epoch)
    .bind(schema)
    .bind(table)
    .bind(self_pod)
    .bind(ttl_secs as f64)
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(done.rows_affected() > 0)
}

/// Release our lease on graceful shutdown (expire it immediately) so a replacement pod need not wait
/// out the TTL. A no-op if we no longer own it.
pub async fn release_lease(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
    self_pod: &str,
) -> Result<(), ControlError> {
    sqlx::query(
        r#"
        UPDATE walrus.table_ownership
        SET lease_expiry = now() - make_interval(secs => 1), updated_at = now()
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND owner_pod = $4
        "#,
    )
    .bind(epoch)
    .bind(schema)
    .bind(table)
    .bind(self_pod)
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(())
}
