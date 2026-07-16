//! Control-DB connection pool and migration runner.

use sqlx::postgres::{PgPool, PgPoolOptions};

/// Errors from the control-DB entrypoint, classified terminal-vs-transient like [`common::Error`].
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// Could not connect to / query the control Postgres. May be transient during a rollout.
    #[error("control database unavailable: {0}")]
    Connect(#[source] sqlx::Error),

    /// A migration failed to apply (bad SQL, checksum mismatch, …). Terminal — retrying won't help.
    #[error("control-plane migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// A DB CHECK constraint was violated (e.g. `transformed_lsn > raw_appended_lsn`). Terminal —
    /// it means a programming bug, never a transient condition.
    #[error("control-plane invariant violated (check constraint): {0}")]
    CheckViolation(String),

    /// A reload was requested for a table that already has a live (non-terminal) one — the
    /// `table_reload_one_live` partial unique index fired (PR 6.1). Terminal for THIS request:
    /// retrying is pointless until the live reload reaches `complete`/`failed`.
    #[error("a reload is already in progress for {schema}.{table}")]
    ReloadInProgress { schema: String, table: String },

    /// A reload transition's guarded UPDATE matched zero rows — the row was not in the expected
    /// state (an illegal jump, a lost race, or a stale caller). Terminal: it means a bug or a
    /// superseded actor, never a cold dependency.
    #[error("illegal reload transition: reload {reload_id} is not in status {expected}")]
    ReloadTransition {
        reload_id: i64,
        expected: &'static str,
    },
}

impl ControlError {
    /// True when retrying can never help — a broken migration or a violated invariant is a bug, not
    /// a cold dependency.
    pub fn is_terminal(&self) -> bool {
        match self {
            ControlError::Migrate(_)
            | ControlError::CheckViolation(_)
            | ControlError::ReloadInProgress { .. }
            | ControlError::ReloadTransition { .. } => true,
            ControlError::Connect(_) => false,
        }
    }

    /// The complement of [`ControlError::is_terminal`] — a dependency that may still be coming up.
    pub fn is_transient(&self) -> bool {
        !self.is_terminal()
    }

    /// Classify a `sqlx::Error`: a CHECK violation (SQLSTATE `23514`) becomes the terminal
    /// [`ControlError::CheckViolation`]; everything else is a (possibly transient) [`Connect`].
    pub(crate) fn from_sqlx(e: sqlx::Error) -> Self {
        if let sqlx::Error::Database(db) = &e {
            if db.code().as_deref() == Some("23514") {
                return ControlError::CheckViolation(db.message().to_string());
            }
        }
        ControlError::Connect(e)
    }
}

/// The default control-pool ceiling. Bounds-checked, config-driven sizing arrives with the bin
/// bootstraps (PR 3.1); a small pool is right for the low-volume control traffic until then.
const DEFAULT_MAX_CONNECTIONS: u32 = 5;

/// Connect to the control Postgres, returning a ready connection pool.
pub async fn connect(dsn: &str) -> Result<PgPool, ControlError> {
    PgPoolOptions::new()
        .max_connections(DEFAULT_MAX_CONNECTIONS)
        .connect(dsn)
        .await
        .map_err(ControlError::Connect)
}

/// Apply every migration in `migrations/control/` idempotently — sqlx records applied versions in
/// `_sqlx_migrations`, so a second run is a no-op. The path is relative to this crate's `Cargo.toml`.
pub async fn run_migrations(pool: &PgPool) -> Result<(), ControlError> {
    sqlx::migrate!("../../migrations/control").run(pool).await?;
    Ok(())
}

#[cfg(test)]
#[path = "db_test.rs"]
mod tests;
