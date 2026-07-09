//! `LoaderError` — every terminal bootstrap failure, each mapped to a distinct [`common::ExitCode`] so
//! a broken deploy is greppable in `kubectl logs` (the "context in the loop, exit code at `main`"
//! idiom). Transient failures are retried to a deadline *before* becoming one of these.

use crate::config::ConfigError;

#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Control(#[from] control::ControlError),
    #[error("DuckDB: {0}")]
    Duck(String),
    #[error("object store: {0}")]
    ObjectStore(String),
    /// A *live* owner already holds the lease — a second writer must NOT proceed.
    #[error("lease for {table} is held by a live owner ({owner})")]
    LeaseContended { table: String, owner: String },
    /// `transformed_lsn > raw_appended_lsn` — the checkpoint is corrupt (should be impossible: the DB
    /// enforces `CHECK (transformed_lsn <= raw_appended_lsn)`), so this is terminal.
    #[error("corrupt checkpoint for {table}: transformed_lsn > raw_appended_lsn")]
    CorruptCheckpoint { table: String },
    /// A lossy/incompatible `ALTER COLUMN TYPE` failed the in-place mirror cast (PR 3.9). The table is
    /// quarantined and processing STOPS — an accepted, alerting v1 outcome (never silent data loss).
    #[error("table {table} quarantined: {reason}")]
    Quarantine { table: String, reason: String },
    /// The control plane opened a NEW generation (§1.8 total-restart) while this loader was running the
    /// old one. Exit **loudly** so the orchestrator restarts us into a rebuild under the new epoch —
    /// never rebuild a running generation in place.
    #[error("epoch bumped {from} → {to}: control-plane opened a new generation (total-restart) — restarting to rebuild")]
    EpochBumped { from: i64, to: i64 },
    #[error("{0}")]
    Internal(String),
}

impl LoaderError {
    /// The classified terminal error `main` surfaces as an exit code.
    pub fn as_common(&self) -> common::Error {
        match self {
            LoaderError::Config(e) => common::Error::Config(e.0.clone()),
            LoaderError::Control(e) => common::Error::ControlDb(e.to_string()),
            LoaderError::Duck(m) => common::Error::Internal(format!("duckdb: {m}")),
            LoaderError::ObjectStore(m) => common::Error::ObjectStore(m.clone()),
            LoaderError::LeaseContended { table, owner } => {
                common::Error::LeaseContended(format!("{table} held by {owner}"))
            }
            LoaderError::CorruptCheckpoint { table } => {
                common::Error::Internal(format!("corrupt checkpoint for {table}"))
            }
            LoaderError::Quarantine { table, reason } => {
                common::Error::Quarantine(format!("{table}: {reason}"))
            }
            LoaderError::EpochBumped { from, to } => {
                common::Error::Internal(format!("epoch bumped {from} → {to} (total-restart)"))
            }
            LoaderError::Internal(m) => common::Error::Internal(m.clone()),
        }
    }

    pub fn exit_code(&self) -> common::ExitCode {
        self.as_common().exit_code()
    }
}
