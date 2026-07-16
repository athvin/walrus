//! Error taxonomy for walrus services, with a terminal-vs-transient classifier and stable
//! process exit codes.
//!
//! Both services run an ordered, fail-fast bootstrap: on Kubernetes a non-zero exit becomes
//! `CrashLoopBackOff`, so a broken deploy must be *loud and immediate*. This module gives that
//! vocabulary — [`Error`] models each precondition failure as data, [`Error::is_terminal`]
//! decides whether retrying under the startup deadline could ever help, and [`ExitCode`] gives
//! each terminal class a distinct, greppable process exit status.

use thiserror::Error;

/// Library-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Every way a walrus service can fail a precondition or an operation.
///
/// **Invariant:** whether a variant is terminal or transient is decided by
/// [`Error::is_terminal`] — a method matched exhaustively over the variants — *never* by
/// inspecting the `Display` message string. Classification is modelled as data, not guessed.
#[derive(Debug, Error)]
pub enum Error {
    /// Misconfiguration — ConfigMap/env failed schema or bounds validation. Always terminal.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// Control Postgres could not be reached. May be transient during a rollout.
    #[error("control database unavailable: {0}")]
    ControlDb(String),

    /// Object store (S3/MinIO) unreachable or the canary head/put/get failed. May be transient.
    #[error("object store unavailable: {0}")]
    ObjectStore(String),

    /// Source Postgres could not be reached (a replication-capable connect failed for a reason that
    /// retrying might fix — the server is still coming up). May be transient. A *privilege* or
    /// server-config mismatch is a terminal [`Error::Preflight`] instead.
    #[error("source database unavailable: {0}")]
    SourceDb(String),

    /// Source-server prerequisite mismatch (`wal_level`, version, slot/wal_sender headroom,
    /// missing publication/slot). Terminal.
    #[error("source preflight failed: {0}")]
    Preflight(String),

    /// A published table has no usable replica identity, in strict mode. Terminal.
    #[error("table {table} has no usable key (strict mode)")]
    KeylessTable { table: String },

    /// Another loader holds the table-ownership lease. Terminal for this pod.
    #[error("table ownership lease contended: {0}")]
    LeaseContended(String),

    /// A lossy/incompatible schema change could not be applied without destroying data — the table is
    /// quarantined and processing stops (an accepted, alerting v1 outcome). Terminal.
    #[error("table quarantined: {0}")]
    Quarantine(String),

    /// Anything not otherwise classified. Terminal.
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// True when retrying under the startup deadline can never help — die now, non-zero.
    ///
    /// The `match` has **no `_ =>` arm on purpose**: adding a future variant is a compile error
    /// until it is explicitly classified here. That is the whole point of modelling the property
    /// as data rather than a comment.
    pub fn is_terminal(&self) -> bool {
        match self {
            // Misconfiguration / unrecoverable preconditions — no retry can fix these.
            Error::Config(_)
            | Error::Preflight(_)
            | Error::KeylessTable { .. }
            | Error::LeaseContended(_)
            | Error::Quarantine(_)
            | Error::Internal(_) => true,
            // Dependencies that may simply be "still coming up" during a rollout.
            Error::ControlDb(_) | Error::ObjectStore(_) | Error::SourceDb(_) => false,
        }
    }

    /// The complement of [`Error::is_terminal`] — a dependency that may still be coming up, so the
    /// bootstrap retries it with backoff up to the startup deadline.
    pub fn is_transient(&self) -> bool {
        !self.is_terminal()
    }

    /// The distinct process exit code for this failure (greppable in `kubectl logs`).
    pub fn exit_code(&self) -> ExitCode {
        match self {
            Error::Config(_) => ExitCode::Config,
            Error::ControlDb(_) => ExitCode::ControlDb,
            Error::ObjectStore(_) => ExitCode::ObjectStore,
            Error::SourceDb(_) => ExitCode::SourceDb,
            Error::Preflight(_) => ExitCode::Preflight,
            Error::KeylessTable { .. } => ExitCode::KeylessTable,
            Error::LeaseContended(_) => ExitCode::LeaseContended,
            Error::Quarantine(_) => ExitCode::Quarantine,
            Error::Internal(_) => ExitCode::Internal,
        }
    }
}

/// Stable, distinct exit statuses. The numbers are a **public contract** — runbooks and alerts
/// grep them — so never renumber an existing code, only append new ones. Kept small (< 125) to
/// stay clear of shell-reserved statuses and to fit `std::process::ExitCode`'s `u8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitCode {
    Success = 0,
    Config = 10,
    ControlDb = 11,
    ObjectStore = 12,
    Preflight = 13,
    KeylessTable = 14,
    LeaseContended = 15,
    SourceDb = 16,
    Quarantine = 17,
    Internal = 70,
}

impl From<ExitCode> for std::process::ExitCode {
    fn from(code: ExitCode) -> Self {
        // Every variant is < 125, so the i32 repr fits a u8 without truncation.
        std::process::ExitCode::from(code as u8)
    }
}

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;
