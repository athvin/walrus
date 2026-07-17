//! DDL capture — the sink's consume side of the source's event-trigger tap (§3, PR 2.33).
//!
//! Postgres logical decoding never emits DDL, so the source's `ddl_command_end`/`sql_drop` triggers
//! (`migrations/source/0002`) INSERT into the **published** `walrus.ddl_audit` table, which rides the
//! *same* replication slot as DML **in commit order**. The sink recognises that relation's INSERTs and,
//! per event: writes a `ddl_manifest` row stamped with the DDL's `c_lsn`, bumps the affected table's
//! **structural** `schema_version` (structural events only), and signals the batcher to **cut a fresh
//! Parquet file** — so every file carries exactly one `schema_version` (the homogeneous-file rule).
//!
//! **Schema-DIFF, not DDL-text replay.** We act on the structured `c_columns` snapshot (the source read
//! the *already-changed* catalog post-execution), never by re-executing `c_ddl_text`. A `COMMENT ON` is
//! recorded but is **metadata-only** — it neither bumps the structural version nor cuts a file.
//!
//! `walrus.ddl_audit`/`walrus.heartbeat` are internal ([`crate::heartbeat::InternalTables`]) — consumed
//! for control, **never** materialised as `<table>`/`<table>_raw`. Event triggers are not exhaustive
//! (globals fire nothing; `TRUNCATE` is a native pgoutput message) — the Relation-message drift backstop
//! (TODO: full handling is the loader's, PR 3.8/3.9) covers the rest.

use common::{Lsn, PgRelation, TupleValue};
use std::collections::HashMap;

/// A decoded `walrus.ddl_audit` INSERT — the sink's only signal that the schema changed.
#[derive(Debug, Clone)]
pub struct DdlEvent {
    /// `pg_current_wal_lsn()` at capture — orders the DDL against data.
    pub c_lsn: Lsn,
    /// `ddl_command_end` | `sql_drop`.
    pub c_event: String,
    /// `ALTER TABLE` | `CREATE TABLE` | `DROP TABLE` | `COMMENT` | …
    pub c_tag: String,
    pub source_schema: String,
    pub source_table: String,
    /// The structured post-change column set (the schema-diff input); `None` for pure drops.
    pub c_columns: Option<serde_json::Value>,
}

impl DdlEvent {
    /// Extract from a decoded `ddl_audit` tuple by column name (text/pgoutput format).
    pub fn from_tuple(rel: &PgRelation, values: &[TupleValue]) -> Result<Self, DdlError> {
        let text = |name: &str| -> Option<String> {
            let idx = rel.columns.iter().position(|c| c.name == name)?;
            match values.get(idx)? {
                TupleValue::Text(s) => Some(s.clone()),
                _ => None,
            }
        };
        let c_lsn = text("c_lsn")
            .ok_or(DdlError::MissingColumn("c_lsn"))?
            .parse()
            .map_err(|_| DdlError::MissingColumn("c_lsn"))?;
        let c_columns = text("c_columns")
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(DdlError::Json)?;
        Ok(DdlEvent {
            c_lsn,
            c_event: text("c_event").unwrap_or_default(),
            c_tag: text("c_tag").unwrap_or_default(),
            source_schema: text("c_schema").unwrap_or_default(),
            source_table: text("c_table").unwrap_or_default(),
            c_columns,
        })
    }

    /// Structural (gates data + cuts a file) vs metadata-only. A `COMMENT` mirrors documentation but
    /// never changes the row shape, so it must NOT bump the structural version or cut a file.
    pub fn is_structural(&self) -> bool {
        !self.c_tag.eq_ignore_ascii_case("COMMENT")
    }
}

/// Consumes decoded `ddl_audit` events: writes the `ddl_manifest` history and tracks each table's
/// current **structural** `schema_version` (starts at 1; every structural DDL bumps it by one).
pub struct DdlConsumer {
    epoch: i64,
    versions: HashMap<(String, String), i64>,
}

impl DdlConsumer {
    pub fn new(epoch: i64) -> Self {
        DdlConsumer {
            epoch,
            versions: HashMap::new(),
        }
    }

    /// The current structural version for a table (1 until its first structural DDL).
    pub fn version_of(&self, schema: &str, table: &str) -> i64 {
        *self
            .versions
            .get(&(schema.to_string(), table.to_string()))
            .unwrap_or(&1)
    }

    /// **(1)** write a `ddl_manifest` row stamped with `c_lsn`; **(2)** for a *structural* event, bump the
    /// table's `schema_version`. Returns `Some(new_version)` iff structural (the caller cuts a fresh
    /// file), `None` for metadata-only.
    pub async fn consume(
        &mut self,
        ex: impl sqlx::PgExecutor<'_>,
        ev: &DdlEvent,
    ) -> Result<Option<i64>, DdlError> {
        let key = (ev.source_schema.clone(), ev.source_table.clone());
        let structural = ev.is_structural();
        let version = if structural {
            let v = self.versions.entry(key).or_insert(1);
            *v += 1;
            *v
        } else {
            *self.versions.get(&key).unwrap_or(&1)
        };
        let row = control::DdlRow {
            id: 0,
            epoch: self.epoch,
            source_schema: ev.source_schema.clone(),
            source_table: ev.source_table.clone(),
            c_lsn: ev.c_lsn,
            c_event: ev.c_event.clone(),
            c_tag: ev.c_tag.clone(),
            schema_version: version,
        };
        control::insert_ddl(ex, &row, None, ev.c_columns.as_ref()).await?;
        Ok(structural.then_some(version))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DdlError {
    #[error("ddl_audit tuple missing/invalid column: {0}")]
    MissingColumn(&'static str),
    #[error("parse c_columns json: {0}")]
    Json(#[source] serde_json::Error),
    #[error(transparent)]
    Control(#[from] control::ControlError),
}

#[cfg(test)]
#[path = "ddl_test.rs"]
mod tests;
