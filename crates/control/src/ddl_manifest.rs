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

/// Record a decoded schema-change event (sink, PR 2.33). `c_rel_oid` + `c_columns` are the structured
/// schema-diff payload (the source's post-change column snapshot) the loader applies in PR 3.8/3.9 —
/// schema-DIFF, not a replay of the DDL text. Returns the assigned `id`.
pub async fn insert_ddl(
    ex: impl PgExecutor<'_>,
    row: &DdlRow,
    c_rel_oid: Option<u32>,
    c_columns: Option<&serde_json::Value>,
) -> Result<i64, ControlError> {
    let c_rel_oid = c_rel_oid.map(sqlx::postgres::types::Oid);
    let rec = sqlx::query_file!(
        "sql/postgres/queries/insert_ddl.sql",
        row.epoch,
        row.source_schema,
        row.source_table,
        row.c_lsn as Lsn,
        row.c_event,
        row.c_tag,
        row.schema_version,
        c_rel_oid,
        c_columns,
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
    sqlx::query_file_as!(
        DdlRow,
        "sql/postgres/queries/read_pending_ddl.sql",
        epoch,
        schema,
        table,
        after_lsn as Lsn,
    )
    .fetch_all(ex)
    .await
    .map_err(ControlError::from_sqlx)
}
