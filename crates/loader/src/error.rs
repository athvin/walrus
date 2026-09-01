//! [`LoaderError`] — every terminal bootstrap failure, each mapped to a distinct [`common::ExitCode`]
//! so a broken deploy is greppable in `kubectl logs` (the "context in the loop, exit code at `main`"
//! idiom). Transient failures are retried to a deadline *before* becoming one of these.

use crate::config::ConfigError;
use common::{EpochNo, ExitCode, FailureClass};

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LoaderError {
    /// Not `transparent`: [`ConfigError`]'s variants name the offending knob, and this is where the
    /// "which configuration" framing belongs.
    #[error("invalid loader configuration: {0}")]
    Config(#[from] ConfigError),
    /// A control-plane call failed. `transparent` because [`control::ControlError`] already names
    /// the operation, and it is the one variant here that can be transient.
    #[error(transparent)]
    Control(#[from] control::ControlError),
    /// A DuckDB engine call failed. `op` names the operation while `source` keeps the typed engine
    /// failure available to error-chain walkers.
    #[error("DuckDB: {op}")]
    Duck {
        op: String,
        #[source]
        source: duckdb::Error,
    },
    /// The dedicated DuckLake PostgreSQL catalog failed or its advisory-lock session was lost.
    #[error("ducklake catalog: {op}")]
    Catalog {
        op: &'static str,
        #[source]
        source: sqlx::Error,
    },
    /// An object-store call failed. `op` names the call while `source` keeps the store's own typed
    /// failure — its path, its status, its nested transport cause — reachable by
    /// [`source()`](std::error::Error::source)/`downcast_ref` instead of collapsed into a sentence.
    /// Boxed like [`LoaderError::Health`]: `Result<_, LoaderError>` is threaded through the whole
    /// loader, so the store's wide error enum stays behind a pointer.
    ///
    /// Unlike [`LoaderError::Duck`], `Display` still inlines the cause: this message already read
    /// `object store: <op>: <store error>` before the store failure was typed, and typing it must
    /// not shorten what an operator sees.
    #[error("object store: {op}: {source}")]
    ObjectStore {
        op: &'static str,
        #[source]
        source: Box<object_store::Error>,
    },
    /// A *live* owner already holds the lease — a second writer must NOT proceed.
    #[error("lease for {table} is held by a live owner ({owner})")]
    LeaseContended { table: String, owner: String },
    /// `transformed_lsn > raw_appended_lsn` — the checkpoint is corrupt (should be impossible: the DB
    /// enforces `CHECK (transformed_lsn <= raw_appended_lsn)`), so this is terminal.
    #[error("corrupt checkpoint for {table}: transformed_lsn > raw_appended_lsn")]
    CorruptCheckpoint { table: String },
    /// A lossy/incompatible `ALTER COLUMN TYPE` failed the in-place mirror cast. The table is
    /// quarantined and processing STOPS — an accepted, alerting v1 outcome (never silent data loss).
    #[error("table {table} quarantined: {reason}")]
    Quarantine { table: String, reason: String },
    /// The control plane opened a NEW generation (§1.8 total-restart) while this loader was running the
    /// old one. Exit **loudly** so the orchestrator restarts us into a rebuild under the new epoch —
    /// never rebuild a running generation in place.
    #[error(
        "epoch bumped {from} → {to}: control-plane opened a new generation (total-restart) — restarting to rebuild"
    )]
    EpochBumped { from: EpochNo, to: EpochNo },
    /// A schema-registry column snapshot did not decode into the relation shape the sink wrote.
    #[error("decode registry columns for {table} v{version}")]
    RegistryDecode {
        table: String,
        version: i64,
        #[source]
        source: serde_json::Error,
    },
    /// A Parquet column name is not a name that can be quoted as a SQL identifier, so the append's
    /// explicit column list could not be built. `source` keeps *which* rule it broke, so a caller
    /// can branch on it instead of matching on the message. `Display` inlines the cause: the text
    /// was already `parquet column name from <uri>: <rule>` while this was a
    /// [`LoaderError::Internal`] string.
    #[error("parquet column name from {uri}: {source}")]
    Ident {
        uri: String,
        #[source]
        source: common::sql::IdentError,
    },
    /// A stored watermark string failed to parse as a Postgres LSN.
    #[error("parse {field} as an LSN")]
    LsnParse {
        field: &'static str,
        #[source]
        source: common::lsn::LsnParseError,
    },
    /// A control-DB transaction could not be begun or committed.
    #[error("control transaction: {op}")]
    ControlTxn {
        op: &'static str,
        #[source]
        source: sqlx::Error,
    },
    /// The health/metrics server failed to bind, join, or serve.
    #[error("health server: {op}")]
    Health {
        op: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A local `.duckdb` file operation failed. `op` names it and `path` locates it, while `source`
    /// keeps the OS error — so "permission denied" and "read-only file system", one sentence apart
    /// to a log reader but two different operator actions, stay distinguishable by
    /// [`std::io::ErrorKind`]. `Display` inlines the cause for the same reason
    /// [`LoaderError::ObjectStore`] does: the text was already `retire <path>: <os error>` while
    /// this was a [`LoaderError::Internal`] string.
    #[error("{op} {path}: {source}")]
    File {
        op: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// An asserted invariant does not hold and no more specific typed cause exists.
    #[error("{0}")]
    Internal(String),
}

/// The classified terminal error `main` surfaces as an exit code.
///
/// Takes `&LoaderError` because the caller keeps its error for logging; the standard blanket impl
/// then also provides `Into<common::Error>` for `&LoaderError`.
impl From<&LoaderError> for common::Error {
    fn from(e: &LoaderError) -> Self {
        match e {
            LoaderError::Config(e) => common::Error::Config(e.to_string()),
            LoaderError::Control(e) => common::Error::ControlDb(e.to_string()),
            LoaderError::Duck { op, source } => {
                common::Error::Internal(format!("duckdb: {op}: {source}"))
            }
            LoaderError::Catalog { op, source } => {
                common::Error::ControlDb(format!("ducklake catalog {op}: {source}"))
            }
            LoaderError::ObjectStore { op, source } => {
                common::Error::ObjectStore(format!("{op}: {source}"))
            }
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
            LoaderError::RegistryDecode {
                table,
                version,
                source,
            } => common::Error::Internal(format!(
                "decode registry columns for {table} v{version}: {source}"
            )),
            LoaderError::Ident { uri, source } => {
                common::Error::Internal(format!("parquet column name from {uri}: {source}"))
            }
            LoaderError::LsnParse { field, source } => {
                common::Error::Internal(format!("parse {field} as an LSN: {source}"))
            }
            // Deliberate remap: a control-pg failure is ExitCode::ControlDb (11), not Internal (70).
            LoaderError::ControlTxn { op, source } => {
                common::Error::ControlDb(format!("control transaction {op}: {source}"))
            }
            LoaderError::Health { op, source } => {
                common::Error::Internal(format!("health server {op}: {source}"))
            }
            LoaderError::File { op, path, source } => {
                common::Error::Internal(format!("{op} {path}: {source}"))
            }
            LoaderError::Internal(m) => common::Error::Internal(m.clone()),
        }
    }
}

impl FailureClass for LoaderError {
    /// Exhaustive, no `_` arm. Only a wrapped [`control::ControlError`] can be transient; every
    /// other variant is a terminal bootstrap failure by construction.
    fn is_terminal(&self) -> bool {
        match self {
            LoaderError::Control(e) => e.is_terminal(),
            LoaderError::Config(_)
            | LoaderError::Duck { .. }
            | LoaderError::Catalog { .. }
            | LoaderError::ObjectStore { .. }
            | LoaderError::LeaseContended { .. }
            | LoaderError::CorruptCheckpoint { .. }
            | LoaderError::Quarantine { .. }
            | LoaderError::EpochBumped { .. }
            | LoaderError::RegistryDecode { .. }
            | LoaderError::Ident { .. }
            | LoaderError::LsnParse { .. }
            | LoaderError::ControlTxn { .. }
            | LoaderError::Health { .. }
            | LoaderError::File { .. }
            | LoaderError::Internal(_) => true,
        }
    }

    /// OVERRIDE of the default: preserve the existing per-variant codes by routing through the
    /// exhaustive `From<&LoaderError>` mapping.
    fn exit_code(&self) -> ExitCode {
        common::Error::from(self).exit_code()
    }
}

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;
