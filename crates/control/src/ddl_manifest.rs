//! `ddl_manifest` model: one row per schema-change event, in commit-LSN order.
//!
//! **History, never a queue** — never pruned. Because the source's `ddl_audit` INSERTs ride the
//! same replication slot as DML, each event's `c_lsn` (commit LSN) is directly comparable to
//! `file_manifest.lsn_end` and the checkpoints. The loader crosses a `schema_version` boundary by
//! applying the pending DDL whose `c_lsn` it is about to pass (PR 3.8/3.9) — there is no separate
//! ordering.

use crate::ControlError;
use common::Lsn;
use sqlx::PgExecutor;

/// A decoded schema-change event. (`c_columns` / `c_dropped` gain typed fields in PRs 3.8/3.9; they
/// are stored now but not read back here.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlRow {
    /// Assigned by the DB on insert; ignored by [`insert_ddl`].
    pub id: i64,
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    /// Commit LSN of the DDL — orders it relative to DML.
    pub c_lsn: Lsn,
    /// `ddl_command_end` | `sql_drop`.
    pub c_event: String,
    /// `CREATE TABLE` | `ALTER TABLE` | `DROP TABLE` | `COMMENT` | …
    pub c_tag: String,
    /// The `schema_version` this DDL produces.
    pub schema_version: i64,
}

/// Record a decoded schema-change event (sink, PR 2.33). Returns the assigned `id`.
pub async fn insert_ddl(ex: impl PgExecutor<'_>, row: &DdlRow) -> Result<i64, ControlError> {
    let rec = sqlx::query!(
        r#"
        INSERT INTO walrus.ddl_manifest
            (epoch, source_schema, source_table, c_lsn, c_event, c_tag, schema_version)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        "#,
        row.epoch,
        row.source_schema,
        row.source_table,
        row.c_lsn as Lsn,
        row.c_event,
        row.c_tag,
        row.schema_version,
    )
    .fetch_one(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(rec.id)
}

/// DDL the loader must apply before transforming past `after_lsn`, in `c_lsn` order (`id` breaks
/// ties) — the events with `c_lsn > after_lsn`.
pub async fn read_pending_ddl(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
    after_lsn: Lsn,
) -> Result<Vec<DdlRow>, ControlError> {
    sqlx::query_as!(
        DdlRow,
        r#"
        SELECT id, epoch, source_schema, source_table,
               c_lsn AS "c_lsn: Lsn", c_event, c_tag, schema_version
        FROM walrus.ddl_manifest
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND c_lsn > $4
        ORDER BY c_lsn, id
        "#,
        epoch,
        schema,
        table,
        after_lsn as Lsn,
    )
    .fetch_all(ex)
    .await
    .map_err(ControlError::from_sqlx)
}
