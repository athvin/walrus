//! `replication_state` models: the epoch generation that namespaces all control-plane state.

use crate::ControlError;
use common::Lsn;
use sqlx::PgExecutor;

/// One row per slot lifetime; a new slot = a new epoch (architecture §1.8). The `epoch` namespaces
/// **all** other state (manifest, checkpoints, registry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationState {
    pub epoch: i64,
    pub slot_name: String,
    /// The consistent snapshot LSN at slot creation.
    pub created_lsn: Lsn,
    /// `bootstrapping` | `streaming` | `total_restart`.
    pub status: String,
}

/// The highest-epoch (current) generation, if bootstrap has run.
pub async fn read_current_epoch(
    ex: impl PgExecutor<'_>,
) -> Result<Option<ReplicationState>, ControlError> {
    sqlx::query_as!(
        ReplicationState,
        r#"
        SELECT epoch, slot_name, created_lsn AS "created_lsn: Lsn", status
        FROM walrus.replication_state
        ORDER BY epoch DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(ex)
    .await
    .map_err(ControlError::from_sqlx)
}

/// Insert a new generation row (a new slot). Epoch bump / total-restart lands in PR 4.6.
pub async fn insert_epoch(
    ex: impl PgExecutor<'_>,
    s: &ReplicationState,
) -> Result<(), ControlError> {
    sqlx::query!(
        r#"
        INSERT INTO walrus.replication_state (epoch, slot_name, created_lsn, status)
        VALUES ($1, $2, $3, $4)
        "#,
        s.epoch,
        s.slot_name,
        s.created_lsn as Lsn,
        s.status,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(())
}
