//! Error taxonomy for walrus services, with a terminal-vs-transient classifier and stable
//! process exit codes.
//!
//! Both services run an ordered, fail-fast bootstrap: on Kubernetes a non-zero exit becomes
//! `CrashLoopBackOff`, so a broken deploy must be *loud and immediate*. This module gives that
//! vocabulary — [`enum@Error`] models each precondition failure as data, [`Error::is_terminal`]
//! decides whether retrying under the startup deadline could ever help, and [`ExitCode`] gives
//! each terminal class a distinct, greppable process exit status.

use crate::FailureClass;
use thiserror::Error;

/// Library-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// Every way a walrus service can fail a precondition or an operation.
///
/// **Invariant:** whether a variant is terminal or transient is decided by
/// [`Error::is_terminal`] — a method matched exhaustively over the variants — *never* by
/// inspecting the `Display` message string. Classification is modelled as data, not guessed.
///
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, Error)]
#[non_exhaustive]
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

/// Move-cost budget for the error value propagated through every service hot path.
///
/// Measured with `size_of::<Error>()` on PR 9.7. If this trips, shrink or box the growing variant
/// in the Phase 11 layout work, or raise the measured budget deliberately in review.
const ERROR_MAX_BYTES: usize = 32;
const _: () = assert!(size_of::<Error>() <= ERROR_MAX_BYTES);

impl FailureClass for Error {
    /// True when retrying under the startup deadline can never help — die now, non-zero.
    ///
    /// The `match` has **no `_ =>` arm on purpose**: adding a future variant is a compile error
    /// until it is explicitly classified here. That is the whole point of modelling the property
    /// as data rather than a comment.
    fn is_terminal(&self) -> bool {
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

    /// OVERRIDE of the default: the distinct process exit code for this failure (greppable in
    /// `kubectl logs`). The exhaustive match has no `_` arm because the numbers are a runbook
    /// contract.
    fn exit_code(&self) -> ExitCode {
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
///
/// This taxonomy is still growing; new codes must remain additive for downstream crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
#[non_exhaustive]
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

/// An `i32` that is not one of [`ExitCode`]'s documented statuses.
///
/// This is concrete rather than a string so callers can recover the offending number.
#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
#[error("unknown walrus exit code {0}")]
pub struct UnknownExitCode(pub i32);

/// The inverse of the documented `#[repr(i32)]` contract.
impl TryFrom<i32> for ExitCode {
    type Error = UnknownExitCode;

    fn try_from(code: i32) -> std::result::Result<Self, Self::Error> {
        match code {
            0 => Ok(Self::Success),
            10 => Ok(Self::Config),
            11 => Ok(Self::ControlDb),
            12 => Ok(Self::ObjectStore),
            13 => Ok(Self::Preflight),
            14 => Ok(Self::KeylessTable),
            15 => Ok(Self::LeaseContended),
            16 => Ok(Self::SourceDb),
            17 => Ok(Self::Quarantine),
            70 => Ok(Self::Internal),
            other => Err(UnknownExitCode(other)),
        }
    }
}

/// Stable fallback if a future exit-code discriminant no longer fits the process API's byte.
const INTERNAL_EXIT_BYTE: u8 = 70;

impl From<ExitCode> for std::process::ExitCode {
    fn from(code: ExitCode) -> Self {
        // Reading a repr(i32) discriminant has no From/TryFrom equivalent. The narrowing is checked;
        // `error_test.rs` exhaustively proves that the fallback is unreachable for today's variants.
        #[allow(
            clippy::cast_possible_truncation,
            reason = "read the enum's explicit repr(i32) discriminant before checked narrowing"
        )]
        let raw = code as i32;
        std::process::ExitCode::from(u8::try_from(raw).unwrap_or(INTERNAL_EXIT_BYTE))
    }
}

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;
