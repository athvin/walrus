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
}

impl ControlError {
    /// True when retrying can never help — a broken migration or a violated invariant is a bug, not
    /// a cold dependency.
    pub fn is_terminal(&self) -> bool {
        match self {
            ControlError::Migrate(_) | ControlError::CheckViolation(_) => true,
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
mod tests {
    use super::*;

    #[test]
    fn connect_errors_are_transient_migrations_are_terminal() {
        // A cold/unreachable control DB is transient — bootstrap retries it to the deadline.
        let connect = ControlError::Connect(sqlx::Error::PoolClosed);
        assert!(connect.is_transient());
        assert!(!connect.is_terminal());

        // A broken migration is a deploy bug — terminal, no retry.
        let migrate = ControlError::Migrate(sqlx::migrate::MigrateError::VersionMissing(1));
        assert!(migrate.is_terminal());
        assert!(!migrate.is_transient());

        // A violated invariant (CHECK constraint) is a programming bug — terminal.
        let check = ControlError::CheckViolation("transformed_lsn > raw_appended_lsn".to_string());
        assert!(check.is_terminal());
        assert!(!check.is_transient());
    }
}
