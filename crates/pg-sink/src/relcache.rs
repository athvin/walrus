//! The **relation cache** — the bridge from decoded pgoutput messages to typed Arrow.
//!
//! Every pgoutput `Relation` describes a table's shape at a point in time; every later
//! `Insert`/`Update`/`Delete` references it by OID. This cache turns a `Relation` into a Tier-1 Arrow
//! schema (+ per-column [`TypeDescriptor`]s) via `pg-to-arrow` and stores it keyed by
//! **`(relation_oid, schema_version)`** — the version in the key is what makes a schema change
//! (PR 2.33) a *new* entry rather than a mutation, so in-flight batches at the old version still
//! resolve. At bootstrap the cache is **hydrated** from `schema_registry` so a restart is a resume.

use arrow::datatypes::SchemaRef;
use common::{PgRelation, TypeDescriptor};
use std::collections::HashMap;
use std::sync::Arc;

/// Everything the batching path (PR 2.23) needs for one relation at one `schema_version`, shared by
/// `Arc` so it is read without cloning per row.
#[derive(Debug)]
pub struct CachedRelation {
    pub relation: PgRelation,
    /// Built by `pg-to-arrow`: one field per source column + the trailing `walrus_pg_sink_meta` Utf8.
    pub arrow_schema: SchemaRef,
    /// Per source column, for the loader to rebuild the exact types (§2.6).
    pub descriptors: Vec<TypeDescriptor>,
    pub schema_version: i64,
}

/// The three walrus-internal source tables: control-plane, never registered or schematised as user
/// data. `reload_signal` (PR 6.3) is consumed for its echo — the chunk watermark — exactly as
/// `ddl_audit` is consumed for DDL events: never batched, never a Parquet file, never a manifest row.
pub fn is_internal_table(schema: &str, table: &str) -> bool {
    schema == "walrus" && (table == "ddl_audit" || table == "heartbeat" || table == "reload_signal")
}

#[derive(Debug, Default)]
pub struct RelationCache {
    by_key: HashMap<(u32, i64), Arc<CachedRelation>>,
}

impl RelationCache {
    pub fn get(&self, oid: u32, schema_version: i64) -> Option<Arc<CachedRelation>> {
        self.by_key.get(&(oid, schema_version)).cloned()
    }

    /// The cached shape for `oid` at its **highest** `schema_version` — used to stamp streamed changes
    /// after a DDL bump (PR 2.33), so a change always lands in the latest-shape file.
    pub fn latest_for(&self, oid: u32) -> Option<Arc<CachedRelation>> {
        self.by_key
            .iter()
            .filter(|((o, _), _)| *o == oid)
            .max_by_key(|((_, v), _)| *v)
            .map(|(_, r)| r.clone())
    }

    /// The OID of a cached `schema.table` (any version) — the DDL-capture cut (PR 2.33) needs it to find
    /// the affected table's batcher.
    pub fn oid_for(&self, schema: &str, table: &str) -> Option<u32> {
        self.by_key
            .values()
            .find(|r| r.relation.schema == schema && r.relation.name == table)
            .map(|r| r.relation.oid)
    }

    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Build the Arrow schema + descriptors from a decoded `Relation`, cache under
    /// `(oid, schema_version)`, and return the entry.
    pub fn upsert_from_relation(
        &mut self,
        relation: PgRelation,
        schema_version: i64,
    ) -> Result<Arc<CachedRelation>, RelationError> {
        let cached = build_cached(relation, schema_version)?;
        let key = (cached.relation.oid, schema_version);
        let entry = Arc::new(cached);
        self.by_key.insert(key, entry.clone());
        Ok(entry)
    }

    /// Rebuild cache entries at bootstrap from persisted `schema_registry` rows (step 7). Each row's
    /// `columns` snapshot is the serialized `PgRelation`; the Arrow schema is recomputed from it, and
    /// the stored descriptors are used verbatim.
    pub fn hydrate(&mut self, rows: Vec<control::RegistryRow>) -> Result<(), RelationError> {
        for row in rows {
            let relation: PgRelation = serde_json::from_value(row.columns).map_err(|e| {
                RelationError::Hydrate(format!(
                    "{}.{}: columns snapshot is not a PgRelation: {e}",
                    row.source_schema, row.source_table
                ))
            })?;
            let arrow_schema = build_arrow(&relation)?;
            let key = (relation.oid, row.schema_version);
            self.by_key.insert(
                key,
                Arc::new(CachedRelation {
                    arrow_schema,
                    descriptors: row.descriptors,
                    schema_version: row.schema_version,
                    relation,
                }),
            );
        }
        Ok(())
    }
}

fn build_arrow(relation: &PgRelation) -> Result<SchemaRef, RelationError> {
    pg_to_arrow::build_schema(relation)
        .map(Arc::new)
        .map_err(|source| RelationError::Schema {
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            source,
        })
}

fn build_cached(
    relation: PgRelation,
    schema_version: i64,
) -> Result<CachedRelation, RelationError> {
    let arrow_schema = build_arrow(&relation)?;
    let descriptors = pg_to_arrow::descriptor::describe_relation(&relation);
    Ok(CachedRelation {
        arrow_schema,
        descriptors,
        schema_version,
        relation,
    })
}

#[derive(Debug, thiserror::Error)]
pub enum RelationError {
    #[error("build Arrow schema for {schema}.{table}: {source}")]
    Schema {
        schema: String,
        table: String,
        #[source]
        source: pg_to_arrow::Error,
    },
    #[error("hydrate from schema_registry: {0}")]
    Hydrate(String),
}

#[cfg(test)]
#[path = "relcache_test.rs"]
mod tests;
