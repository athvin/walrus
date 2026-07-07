//! `loader_checkpoint` models: the two per-table commit-LSN watermarks.
//!
//! The loader tracks progress with **two watermarks per `(epoch, schema, table)`**, both
//! **commit-LSN valued** (never max-row LSN): `raw_appended_lsn` (Phase A — the `<table>_raw` CDC
//! log is durable up to here) and `transformed_lsn` (Phase B — the `<table>` mirror is derived up
//! to here). They advance **independently** — Phase A in the loader's control txn alongside the
//! manifest delete (PR 3.2), Phase B on its own commit (PR 3.4) — and the mirror is never ahead of
//! the log, an invariant the DB enforces via `CHECK (transformed_lsn <= raw_appended_lsn)`.

use crate::ControlError;
use common::Lsn;
use sqlx::PgExecutor;

/// Per-table, per-epoch progress. **Invariant (DB-enforced):** `transformed_lsn <= raw_appended_lsn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    /// Phase A frontier — the CDC log is durable up to this commit LSN.
    pub raw_appended_lsn: Lsn,
    /// Phase B frontier — the mirror is derived up to this commit LSN (`<= raw_appended_lsn`).
    pub transformed_lsn: Lsn,
}

/// Read the checkpoint for a table, if one exists yet.
pub async fn read_checkpoint(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
) -> Result<Option<Checkpoint>, ControlError> {
    sqlx::query_as!(
        Checkpoint,
        r#"
        SELECT epoch, source_schema, source_table,
               raw_appended_lsn AS "raw_appended_lsn: Lsn",
               transformed_lsn AS "transformed_lsn: Lsn"
        FROM walrus.loader_checkpoint
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
        "#,
        epoch,
        schema,
        table,
    )
    .fetch_optional(ex)
    .await
    .map_err(ControlError::from_sqlx)
}

/// Create the row at `(0/0, 0/0)` if missing; a no-op if present. Called once at loader bootstrap
/// (PR 3.1), kept separate from `advance_*` so a fresh table starts at zero without a spurious
/// "advance to zero".
pub async fn ensure_checkpoint(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
) -> Result<(), ControlError> {
    sqlx::query!(
        r#"
        INSERT INTO walrus.loader_checkpoint
            (epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn)
        VALUES ($1, $2, $3, '0/0'::pg_lsn, '0/0'::pg_lsn)
        ON CONFLICT (epoch, source_schema, source_table) DO NOTHING
        "#,
        epoch,
        schema,
        table,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(())
}

/// Phase A: advance `raw_appended_lsn` (UPSERT). The caller passes the executor so this can share
/// the control-DB transaction that also deletes claimed manifest rows (PR 3.2). `GREATEST` makes
/// the advance **monotonic** — a re-run after a crash never moves the frontier backward.
pub async fn advance_raw_appended(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
    lsn: Lsn,
) -> Result<(), ControlError> {
    sqlx::query!(
        r#"
        INSERT INTO walrus.loader_checkpoint
            (epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn)
        VALUES ($1, $2, $3, $4, '0/0'::pg_lsn)
        ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
            SET raw_appended_lsn =
                    GREATEST(walrus.loader_checkpoint.raw_appended_lsn, EXCLUDED.raw_appended_lsn),
                updated_at = now()
        "#,
        epoch,
        schema,
        table,
        lsn as Lsn,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(())
}

/// Phase B: advance `transformed_lsn` (UPSERT), monotonically. The `CHECK` guards it — advancing
/// above the current `raw_appended_lsn` fails as a terminal [`ControlError::CheckViolation`]. (The
/// INSERT fallback seeds `raw_appended_lsn` equal so the CHECK holds; in practice
/// `ensure_checkpoint` has already created the row.)
pub async fn advance_transformed(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
    lsn: Lsn,
) -> Result<(), ControlError> {
    sqlx::query!(
        r#"
        INSERT INTO walrus.loader_checkpoint
            (epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn)
        VALUES ($1, $2, $3, $4, $4)
        ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
            SET transformed_lsn =
                    GREATEST(walrus.loader_checkpoint.transformed_lsn, EXCLUDED.transformed_lsn),
                updated_at = now()
        "#,
        epoch,
        schema,
        table,
        lsn as Lsn,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(())
}
