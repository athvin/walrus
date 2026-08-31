//! `replication_state` models: the epoch generation that namespaces all control-plane state.

use crate::{ControlError, parse::ParseEnumError};
use common::string_enum;
use common::{EpochNo, Lsn};
use sqlx::PgExecutor;

string_enum! {
    /// Where a generation stands — the canonical enum for the `status` text column.
    ///
    /// Alone among the control plane's text-column enums this one has **no** SQL `CHECK` behind it:
    /// the vocabulary lives only in the migration's trailing comment, so this variant table is the
    /// only thing standing between a typo and a generation row nothing can classify. Every
    /// generation the sink opens is born `Streaming` — first bootstrap and total-restart alike take
    /// that one path (§1.8) — so `Bootstrapping` and `TotalRestart` are names the contract reserves
    /// and no production writer has claimed yet.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReplicationStatus {
        error = ParseEnumError;
        column = "replication_state.status";
        Bootstrapping => "bootstrapping",
        Streaming => "streaming",
        TotalRestart => "total_restart",
    }
}

/// One row per slot lifetime; a new slot = a new epoch (architecture §1.8). The `epoch` namespaces
/// **all** other state (manifest, checkpoints, registry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationState {
    /// The generation counter itself — the value every other control-plane table is keyed by.
    pub epoch: EpochNo,
    /// The replication slot whose lifetime *is* this generation.
    pub slot_name: String,
    /// The consistent snapshot LSN at slot creation.
    pub created_lsn: Lsn,
    /// Where the generation stands; see [`ReplicationStatus`].
    pub status: ReplicationStatus,
}

/// The highest-epoch (current) generation, if bootstrap has run.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the current generation cannot be queried, or
/// [`ControlError::Decode`] if the stored status is outside the enum's set.
pub async fn read_current_epoch(
    ex: impl PgExecutor<'_>,
) -> Result<Option<ReplicationState>, ControlError> {
    // The `status` text column decodes to `String` here, then parses into the typed enum — the same
    // shape as `manifest::claim_ready`, and for the same two reasons: a value outside the known set
    // is a data-integrity bug (the sink only ever writes `as_str()`) that belongs in the terminal
    // `Decode`, and the SQL text is unchanged, so the committed `.sqlx` offline cache stays valid
    // without a regenerate.
    let current = sqlx::query_file!("sql/postgres/queries/read_current_epoch.sql")
        .fetch_optional(ex)
        .await?;
    let Some(current) = current else {
        return Ok(None);
    };
    let state = ReplicationState {
        epoch: current.epoch.into(),
        slot_name: current.slot_name,
        created_lsn: current.created_lsn,
        status: current.status.parse()?,
    };
    Ok(Some(state))
}

/// Insert a new generation row for a newly created slot during bootstrap or total restart.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] for an insert failure, or [`ControlError::CheckViolation`] if
/// the proposed generation violates a database invariant.
pub async fn insert_epoch(
    ex: impl PgExecutor<'_>,
    s: &ReplicationState,
) -> Result<(), ControlError> {
    sqlx::query_file!(
        "sql/postgres/queries/insert_epoch.sql",
        s.epoch.0,
        s.slot_name,
        s.created_lsn as Lsn,
        s.status.as_str(),
    )
    .execute(ex)
    .await?;
    Ok(())
}

/// Open a new generation (§1.8 total-restart): insert `MAX(epoch) + 1` with the given slot + snapshot
/// `created_lsn`, returning the **new epoch**. Atomic (the `SELECT MAX … RETURNING` is one statement),
/// and monotonic by construction — the new epoch strictly exceeds every prior one, so it cleanly
/// re-namespaces all state (S3 prefix, manifest, checkpoints, registry). On an empty table it yields
/// `1`, so first-bootstrap and total-restart share this one path; the caller distinguishes them (a prior
/// epoch present ⇒ a *loud* total-restart) only to decide whether to alert.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the atomic generation insert fails, or
/// [`ControlError::CheckViolation`] if the new row violates a control-plane invariant.
pub async fn bump_epoch(
    ex: impl PgExecutor<'_>,
    slot_name: &str,
    created_lsn: Lsn,
    status: ReplicationStatus,
) -> Result<EpochNo, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/bump_epoch.sql",
        slot_name,
        created_lsn as Lsn,
        status.as_str(),
    )
    .fetch_one(ex)
    .await?;
    Ok(EpochNo(rec.epoch))
}

#[cfg(test)]
#[path = "replication_state_test.rs"]
mod tests;
