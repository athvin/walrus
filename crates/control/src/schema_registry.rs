//! `schema_registry` model: the versioned per-column type-mapping descriptors.
//!
//! **History, never a queue** — never pruned. The sink writes one row per structural
//! `schema_version` (a `Vec` of [`TypeDescriptor`]s from `common`, plus a snapshot of the
//! resulting column set); the loader reads it back to rebuild the exact source types for a given
//! file's `schema_version`. A `DELETE` here would make old-version Parquet files un-reconstructable.

use crate::ControlError;
use common::{EpochNo, SchemaVersionNo, TypeDescriptor};
use sqlx::types::Json;
use sqlx::{PgExecutor, Row};

/// One `schema_version` of a table's type mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRow {
    /// Generation this mapping was registered under.
    pub epoch: EpochNo,
    /// Schema of the table this version describes.
    pub source_schema: String,
    /// Table this version describes.
    pub source_table: String,
    /// The structural version these descriptors describe. Manifest rows name this value, which is
    /// how a Parquet file finds the mapping it was written against.
    pub schema_version: SchemaVersionNo,
    /// The per-column descriptors (stored as `jsonb`).
    pub descriptors: Vec<TypeDescriptor>,
    /// The resulting column-set snapshot (name/type/attnum/nullability/comment), preserved as JSON
    /// so it matches the source event payload exactly.
    pub columns: serde_json::Value,
}

/// Write the descriptor set for one `schema_version`, idempotently: a repeated write of the same
/// version updates in place rather than duplicating (the `(epoch, schema, table, version)` PK).
///
/// # Errors
///
/// Returns [`ControlError::Connect`] when the upsert fails, or [`ControlError::CheckViolation`] if
/// the registry row violates a database invariant.
pub async fn upsert_registry(
    ex: impl PgExecutor<'_>,
    row: &RegistryRow,
) -> Result<(), ControlError> {
    sqlx::query_file!(
        "sql/postgres/queries/upsert_registry.sql",
        row.epoch.0,
        row.source_schema,
        row.source_table,
        row.schema_version.0,
        Json(&row.descriptors) as _,
        row.columns,
    )
    .execute(ex)
    .await?;
    Ok(())
}

/// Read the descriptors for an **exact** `schema_version` — the loader rebuilds types from this.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the registry row cannot be queried or decoded.
pub async fn read_registry(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    version: SchemaVersionNo,
) -> Result<Option<RegistryRow>, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/read_registry.sql",
        epoch.0,
        schema,
        table,
        version.0,
    )
    .fetch_optional(ex)
    .await?;

    Ok(rec.map(|r| RegistryRow {
        epoch: r.epoch.into(),
        source_schema: r.source_schema,
        source_table: r.source_table,
        schema_version: r.schema_version.into(),
        descriptors: r.descriptors.0,
        columns: r.columns,
    }))
}

/// The current (max) `schema_version` for a table, or `None` if it has no registry rows yet.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the version query cannot reach or read control Postgres.
pub async fn read_latest_version(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
    schema: &str,
    table: &str,
) -> Result<Option<SchemaVersionNo>, ControlError> {
    let rec = sqlx::query_file!(
        "sql/postgres/queries/read_latest_version.sql",
        epoch.0,
        schema,
        table,
    )
    .fetch_one(ex)
    .await?;
    Ok(rec.max_version.map(Into::into))
}

/// The **latest** registry row for every `(source_schema, source_table)` under `epoch` — what the
/// sink hydrates its relation cache from at bootstrap (step 7). A runtime query (not `query!`) so it
/// needs no offline cache entry; the `jsonb` columns decode via `Json<_>` / `serde_json::Value`.
///
/// # Errors
///
/// Returns [`ControlError::Connect`] if the latest-row query fails or any typed column cannot be
/// decoded from control Postgres.
pub async fn read_all_latest_registry(
    ex: impl PgExecutor<'_>,
    epoch: EpochNo,
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
    .await?;

    rows.into_iter()
        .map(|row| {
            Ok(RegistryRow {
                epoch: row.try_get::<i64, _>("epoch")?.into(),
                source_schema: row.try_get("source_schema")?,
                source_table: row.try_get("source_table")?,
                schema_version: row.try_get::<i64, _>("schema_version")?.into(),
                descriptors: row
                    .try_get::<Json<Vec<TypeDescriptor>>, _>("descriptors")?
                    .0,
                columns: row.try_get("columns")?,
            })
        })
        .collect()
}
