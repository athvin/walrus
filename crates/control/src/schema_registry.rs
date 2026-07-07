//! `schema_registry` model: the versioned per-column type-mapping descriptors.
//!
//! **History, never a queue** — never pruned. The sink writes one row per structural
//! `schema_version` (a `Vec<TypeDescriptor>` from `common`, PR 1.2, plus a snapshot of the
//! resulting column set); the loader reads it back to rebuild the exact source types for a given
//! file's `schema_version`. A `DELETE` here would make old-version Parquet files un-reconstructable.

use crate::ControlError;
use common::TypeDescriptor;
use sqlx::types::Json;
use sqlx::{PgExecutor, Row};

/// One `schema_version` of a table's type mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRow {
    pub epoch: i64,
    pub source_schema: String,
    pub source_table: String,
    pub schema_version: i64,
    /// The per-column descriptors (stored as `jsonb`).
    pub descriptors: Vec<TypeDescriptor>,
    /// The resulting column-set snapshot (name/type/attnum/nullability/comment) — a `serde_json`
    /// value for now; PRs 3.8/3.9 give it a typed shape.
    pub columns: serde_json::Value,
}

/// Write the descriptor set for one `schema_version`, idempotently: a repeated write of the same
/// version updates in place rather than duplicating (the `(epoch, schema, table, version)` PK).
pub async fn upsert_registry(
    ex: impl PgExecutor<'_>,
    row: &RegistryRow,
) -> Result<(), ControlError> {
    sqlx::query!(
        r#"
        INSERT INTO walrus.schema_registry
            (epoch, source_schema, source_table, schema_version, descriptors, columns)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (epoch, source_schema, source_table, schema_version) DO UPDATE
            SET descriptors = EXCLUDED.descriptors, columns = EXCLUDED.columns
        "#,
        row.epoch,
        row.source_schema,
        row.source_table,
        row.schema_version,
        Json(&row.descriptors) as _,
        row.columns,
    )
    .execute(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(())
}

/// Read the descriptors for an **exact** `schema_version` — the loader rebuilds types from this.
pub async fn read_registry(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
    version: i64,
) -> Result<Option<RegistryRow>, ControlError> {
    let rec = sqlx::query!(
        r#"
        SELECT epoch, source_schema, source_table, schema_version,
               descriptors AS "descriptors: Json<Vec<TypeDescriptor>>",
               columns AS "columns: serde_json::Value"
        FROM walrus.schema_registry
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND schema_version = $4
        "#,
        epoch,
        schema,
        table,
        version,
    )
    .fetch_optional(ex)
    .await
    .map_err(ControlError::from_sqlx)?;

    Ok(rec.map(|r| RegistryRow {
        epoch: r.epoch,
        source_schema: r.source_schema,
        source_table: r.source_table,
        schema_version: r.schema_version,
        descriptors: r.descriptors.0,
        columns: r.columns,
    }))
}

/// The current (max) `schema_version` for a table, or `None` if it has no registry rows yet.
pub async fn read_latest_version(
    ex: impl PgExecutor<'_>,
    epoch: i64,
    schema: &str,
    table: &str,
) -> Result<Option<i64>, ControlError> {
    let rec = sqlx::query!(
        r#"
        SELECT MAX(schema_version) AS "max_version"
        FROM walrus.schema_registry
        WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
        "#,
        epoch,
        schema,
        table,
    )
    .fetch_one(ex)
    .await
    .map_err(ControlError::from_sqlx)?;
    Ok(rec.max_version)
}

/// The **latest** registry row for every `(source_schema, source_table)` under `epoch` — what the
/// sink hydrates its relation cache from at bootstrap (step 7). A runtime query (not `query!`) so it
/// needs no offline cache entry; the `jsonb` columns decode via `Json<_>` / `serde_json::Value`.
pub async fn read_all_latest_registry(
    ex: impl PgExecutor<'_>,
    epoch: i64,
) -> Result<Vec<RegistryRow>, ControlError> {
    let rows = sqlx::query(
        r#"
        SELECT r.epoch, r.source_schema, r.source_table, r.schema_version,
               r.descriptors, r.columns
        FROM walrus.schema_registry r
        JOIN (
            SELECT source_schema, source_table, MAX(schema_version) AS max_v
            FROM walrus.schema_registry
            WHERE epoch = $1
            GROUP BY source_schema, source_table
        ) latest
          ON r.source_schema = latest.source_schema
         AND r.source_table = latest.source_table
         AND r.schema_version = latest.max_v
        WHERE r.epoch = $1
        "#,
    )
    .bind(epoch)
    .fetch_all(ex)
    .await
    .map_err(ControlError::from_sqlx)?;

    rows.into_iter()
        .map(|row| {
            Ok(RegistryRow {
                epoch: row.try_get("epoch").map_err(ControlError::from_sqlx)?,
                source_schema: row
                    .try_get("source_schema")
                    .map_err(ControlError::from_sqlx)?,
                source_table: row
                    .try_get("source_table")
                    .map_err(ControlError::from_sqlx)?,
                schema_version: row
                    .try_get("schema_version")
                    .map_err(ControlError::from_sqlx)?,
                descriptors: row
                    .try_get::<Json<Vec<TypeDescriptor>>, _>("descriptors")
                    .map_err(ControlError::from_sqlx)?
                    .0,
                columns: row.try_get("columns").map_err(ControlError::from_sqlx)?,
            })
        })
        .collect()
}
