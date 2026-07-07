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
            | Error::Internal(_) => true,
            // Dependencies that may simply be "still coming up" during a rollout.
            Error::ControlDb(_) | Error::ObjectStore(_) => false,
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
            Error::Preflight(_) => ExitCode::Preflight,
            Error::KeylessTable { .. } => ExitCode::KeylessTable,
            Error::LeaseContended(_) => ExitCode::LeaseContended,
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
    Internal = 70,
}

impl From<ExitCode> for std::process::ExitCode {
    fn from(code: ExitCode) -> Self {
        // Every variant is < 125, so the i32 repr fits a u8 without truncation.
        std::process::ExitCode::from(code as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant, paired with its DoD-mandated terminal classification.
    fn one_of_each() -> Vec<(Error, bool)> {
        vec![
            (Error::Config("bad bound".into()), true),
            (Error::ControlDb("connection refused".into()), false),
            (Error::ObjectStore("503 from MinIO".into()), false),
            (Error::Preflight("wal_level=replica".into()), true),
            (
                Error::KeylessTable {
                    table: "public.orders".into(),
                },
                true,
            ),
            (Error::LeaseContended("held by loader-0".into()), true),
            (Error::Internal("unreachable".into()), true),
        ]
    }

    #[test]
    fn config_is_terminal_control_db_is_transient() {
        assert!(Error::Config("x".into()).is_terminal());
        assert!(!Error::ControlDb("x".into()).is_terminal());
        assert!(Error::ControlDb("x".into()).is_transient());

        // The full classification contract, exactly as the DoD states it.
        for (err, terminal) in one_of_each() {
            assert_eq!(
                err.is_terminal(),
                terminal,
                "{err:?} classified wrong (expected terminal={terminal})",
            );
            assert_eq!(err.is_transient(), !terminal, "transient is the complement");
        }
    }

    #[test]
    fn each_terminal_variant_maps_to_a_distinct_exit_code() {
        let terminal_codes: Vec<i32> = one_of_each()
            .into_iter()
            .filter(|(_, terminal)| *terminal)
            .map(|(err, _)| err.exit_code() as i32)
            .collect();

        assert!(
            terminal_codes.iter().all(|&c| c != 0),
            "no terminal failure may exit 0 (only Success is 0)",
        );

        let mut distinct = terminal_codes.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            terminal_codes.len(),
            "terminal exit codes must be distinct: {terminal_codes:?}",
        );
    }

    #[test]
    fn display_states_precondition_and_observed_value() {
        // Preflight names the precondition class AND the observed value.
        let e = Error::Preflight("wal_level=replica".into());
        let s = e.to_string();
        assert!(
            s.contains("source preflight failed"),
            "names precondition: {s}"
        );
        assert!(s.contains("wal_level=replica"), "names observed value: {s}");

        // Keyless table names the offending table so the log is actionable.
        let k = Error::KeylessTable {
            table: "public.orders".into(),
        };
        let ks = k.to_string();
        assert!(ks.contains("no usable key"), "names precondition: {ks}");
        assert!(ks.contains("public.orders"), "names observed value: {ks}");
    }

    #[test]
    fn exit_code_zero_is_success_only() {
        assert_eq!(ExitCode::Success as i32, 0);

        // No Error variant — terminal or transient — maps to the success code.
        for (err, _) in one_of_each() {
            assert_ne!(err.exit_code() as i32, 0, "{err:?} must not map to Success");
        }

        // The seam bins use in `main` exists and compiles for a real code.
        let _process_code: std::process::ExitCode = ExitCode::Success.into();
    }
}
