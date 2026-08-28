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

use common::{DdlId, EpochNo, Lsn, PgRelation, SchemaVersionNo, TupleValue};
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
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::MissingColumn`] when the required LSN is absent or invalid, and
    /// [`DdlError::Json`] when the optional structured column snapshot is malformed.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub fn from_tuple(rel: &PgRelation, values: &[TupleValue]) -> Result<Self, DdlError> {
        let text = |name: &str| -> Option<String> {
            let idx = rel.columns.iter().position(|c| c.name == name)?;
            // Every `ddl_audit` column arrives as text; the other images are listed rather than
            // absorbed by a wildcard, so a new TupleValue variant is decided here, not defaulted.
            match values.get(idx)? {
                TupleValue::Text(s) => Some(s.clone()),
                TupleValue::Null | TupleValue::UnchangedToast | TupleValue::Binary(_) => None,
            }
        };
        let c_lsn = text("c_lsn")
            .ok_or(DdlError::MissingColumn("c_lsn"))?
            .parse()
            .map_err(|_| DdlError::MissingColumn("c_lsn"))?;
        let c_columns = text("c_columns")
            .filter(|s| !s.is_empty())
            .map(|s| serde_json::from_str(&s))
            .transpose()?;
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
    #[must_use]
    pub fn is_structural(&self) -> bool {
        !self.c_tag.eq_ignore_ascii_case("COMMENT")
    }
}

/// Consumes decoded `ddl_audit` events: writes the `ddl_manifest` history and tracks each table's
/// current **structural** `schema_version` (starts at 1; every structural DDL bumps it by one).
#[derive(Debug)]
pub struct DdlConsumer {
    epoch: EpochNo,
    versions: HashMap<(String, String), SchemaVersionNo>,
}

impl DdlConsumer {
    #[must_use]
    pub fn new(epoch: EpochNo) -> Self {
        DdlConsumer {
            epoch,
            versions: HashMap::new(),
        }
    }

    /// The current structural version for a table (1 until its first structural DDL).
    #[must_use]
    pub fn version_of(&self, schema: &str, table: &str) -> SchemaVersionNo {
        // Compared borrowed, not looked up: `HashMap<(String, String), _>::get` can only be handed
        // an OWNED key — `Borrow` has no `(&str, &str)` form to reach a `(String, String)` — so the
        // keyed spelling has to allocate both halves of the name on every `Relation` message just
        // to copy a version out. This map holds one entry per table that has taken a structural
        // DDL, so scanning it borrowed is the cheaper read; [`crate::relcache::RelationCache`]
        // resolves a name over the same table set the same way.
        self.versions
            .iter()
            .find(|((s, t), _)| s.as_str() == schema && t.as_str() == table)
            .map_or(SchemaVersionNo(1), |(_, version)| *version)
    }

    /// Record one structural DDL against a table: its version + 1, or 2 when this is the first
    /// structural change it has seen.
    fn bump(&mut self, schema: &str, table: &str) -> SchemaVersionNo {
        // The bump edits its entry through the BORROWED name (see [`Self::version_of`]); only a
        // table's first structural DDL owns copies of it, for the key it inserts.
        let current = self
            .versions
            .iter_mut()
            .find(|((s, t), _)| s.as_str() == schema && t.as_str() == table);
        if let Some((_, version)) = current {
            version.0 += 1;
            return *version;
        }
        let version = SchemaVersionNo(2);
        self.versions.insert((schema.to_string(), table.to_string()), version);
        version
    }

    /// **(1)** write a `ddl_manifest` row stamped with `c_lsn`; **(2)** for a *structural* event, bump the
    /// table's `schema_version`. Returns `Some(new_version)` iff structural (the caller cuts a fresh
    /// file), `None` for metadata-only.
    ///
    /// # Errors
    ///
    /// Returns [`DdlError::Control`] if the DDL history row cannot be persisted in control Postgres.
    pub async fn consume(
        &mut self,
        ex: impl sqlx::PgExecutor<'_>,
        ev: &DdlEvent,
    ) -> Result<Option<SchemaVersionNo>, DdlError> {
        let structural = ev.is_structural();
        // Both arms read the version through the event's own borrowed names; the owned copies below
        // are the ones the `ddl_manifest` row keeps, so the bookkeeping adds none of its own.
        let version = if structural {
            self.bump(&ev.source_schema, &ev.source_table)
        } else {
            self.version_of(&ev.source_schema, &ev.source_table)
        };
        let row = control::DdlRow {
            id: DdlId(0), // ignored on insert; the DB assigns the bigserial
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

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DdlError {
    #[error("ddl_audit tuple missing/invalid column: {0}")]
    MissingColumn(&'static str),
    /// `#[from]` (which implies `#[source]`): a malformed column snapshot has exactly one meaning
    /// here, so `?` may carry the decode failure straight out of `from_tuple`.
    #[error("parse c_columns json: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Control(#[from] control::ControlError),
}

#[cfg(test)]
#[path = "ddl_test.rs"]
mod tests;
