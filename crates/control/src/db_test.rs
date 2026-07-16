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
