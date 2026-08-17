//! The **relation cache** — the bridge from decoded pgoutput messages to typed Arrow.
//!
//! Every pgoutput `Relation` describes a table's shape at a point in time; every later
//! `Insert`/`Update`/`Delete` references it by OID. This cache turns a `Relation` into a Tier-1 Arrow
//! schema (+ per-column [`TypeDescriptor`]s) via `pg-to-arrow` and stores it keyed by
//! **`(relation_oid, schema_version)`** — the version in the key is what makes a schema change
//! (PR 2.33) a *new* entry rather than a mutation, so in-flight batches at the old version still
//! resolve. At bootstrap the cache is **hydrated** from `schema_registry` so a restart is a resume.

use arrow::datatypes::SchemaRef;
use common::{PgRelation, SchemaVersionNo, TypeDescriptor};
use std::collections::{HashMap, hash_map};
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
    pub schema_version: SchemaVersionNo,
}

/// The three walrus-internal source tables: control-plane, never registered or schematised as user
/// data. `reload_signal` (PR 6.3) is consumed for its echo — the chunk watermark — exactly as
/// `ddl_audit` is consumed for DDL events: never batched, never a Parquet file, never a manifest row.
#[must_use]
pub fn is_internal_table(schema: &str, table: &str) -> bool {
    schema == "walrus" && (table == "ddl_audit" || table == "heartbeat" || table == "reload_signal")
}

#[derive(Debug, Default)]
pub struct RelationCache {
    by_key: HashMap<(u32, SchemaVersionNo), Arc<CachedRelation>>,
}

impl RelationCache {
    #[must_use]
    pub fn get(&self, oid: u32, schema_version: SchemaVersionNo) -> Option<Arc<CachedRelation>> {
        self.by_key.get(&(oid, schema_version)).cloned()
    }

    /// The cached shape for `oid` at its **highest** `schema_version` — used to stamp streamed changes
    /// after a DDL bump (PR 2.33), so a change always lands in the latest-shape file.
    #[must_use]
    pub fn latest_for(&self, oid: u32) -> Option<Arc<CachedRelation>> {
        self.iter()
            .filter(|cached| cached.relation.oid == oid)
            .max_by_key(|cached| cached.schema_version)
            .cloned()
    }

    /// The OID of a cached `schema.table` (any version) — the DDL-capture cut (PR 2.33) needs it to find
    /// the affected table's batcher.
    #[must_use]
    pub fn oid_for(&self, schema: &str, table: &str) -> Option<u32> {
        self.iter()
            .find(|cached| cached.relation.schema == schema && cached.relation.name == table)
            .map(|cached| cached.relation.oid)
    }

    /// The cached relations, in unspecified order. The map key is a projection of each value, so
    /// iteration yields values directly.
    #[must_use]
    pub fn iter(&self) -> hash_map::Values<'_, (u32, SchemaVersionNo), Arc<CachedRelation>> {
        <&Self as IntoIterator>::into_iter(self)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Build the Arrow schema + descriptors from a decoded `Relation`, cache under
    /// `(oid, schema_version)`, and return the entry.
    ///
    /// # Errors
    ///
    /// Returns [`RelationError::Schema`] when the relation contains an unsupported or invalid Arrow
    /// mapping.
    pub fn upsert_from_relation(
        &mut self,
        relation: PgRelation,
        schema_version: SchemaVersionNo,
    ) -> Result<Arc<CachedRelation>, RelationError> {
        let cached = build_cached(relation, schema_version)?;
        let key = (cached.relation.oid, schema_version);
        let entry = Arc::new(cached);
        self.by_key.insert(key, Arc::clone(&entry));
        Ok(entry)
    }

    /// Rebuild cache entries at bootstrap from persisted `schema_registry` rows (step 7). Each row's
    /// `columns` snapshot is the serialized `PgRelation`; the Arrow schema is recomputed from it, and
    /// the stored descriptors are used verbatim.
    ///
    /// # Errors
    ///
    /// Returns [`RelationError::Hydrate`] when a persisted snapshot is not a `PgRelation`, or
    /// [`RelationError::Schema`] when its shape cannot be mapped to Arrow.
    pub fn hydrate(&mut self, rows: Vec<control::RegistryRow>) -> Result<(), RelationError> {
        let decoded = rows
            .into_iter()
            .map(|row| {
                let relation: PgRelation = serde_json::from_value(row.columns).map_err(|e| {
                    RelationError::Hydrate(format!(
                        "{}.{}: columns snapshot is not a PgRelation: {e}",
                        row.source_schema, row.source_table
                    ))
                })?;
                let arrow_schema = build_arrow(&relation)?;
                Ok(CachedRelation {
                    arrow_schema,
                    descriptors: row.descriptors,
                    schema_version: row.schema_version,
                    relation,
                })
            })
            .collect::<Result<Vec<_>, RelationError>>()?;
        self.extend(decoded);
        Ok(())
    }
}

impl FromIterator<CachedRelation> for RelationCache {
    fn from_iter<I: IntoIterator<Item = CachedRelation>>(iter: I) -> Self {
        let mut cache = RelationCache::default();
        cache.extend(iter);
        cache
    }
}

impl Extend<CachedRelation> for RelationCache {
    fn extend<I: IntoIterator<Item = CachedRelation>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        self.by_key.reserve(iter.size_hint().0);
        for cached in iter {
            let key = (cached.relation.oid, cached.schema_version);
            self.by_key.insert(key, Arc::new(cached));
        }
    }
}

impl IntoIterator for RelationCache {
    type Item = Arc<CachedRelation>;
    type IntoIter = hash_map::IntoValues<(u32, SchemaVersionNo), Arc<CachedRelation>>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_key.into_values()
    }
}

impl<'a> IntoIterator for &'a RelationCache {
    type Item = &'a Arc<CachedRelation>;
    type IntoIter = hash_map::Values<'a, (u32, SchemaVersionNo), Arc<CachedRelation>>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_key.values()
    }
}

impl<'a> IntoIterator for &'a mut RelationCache {
    type Item = &'a mut Arc<CachedRelation>;
    type IntoIter = hash_map::ValuesMut<'a, (u32, SchemaVersionNo), Arc<CachedRelation>>;

    fn into_iter(self) -> Self::IntoIter {
        self.by_key.values_mut()
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
    schema_version: SchemaVersionNo,
) -> Result<CachedRelation, RelationError> {
    let arrow_schema = build_arrow(&relation)?;
    let descriptors = pg_to_arrow::descriptor::describe_relation(&relation).map_err(|source| {
        RelationError::Schema {
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            source,
        }
    })?;
    Ok(CachedRelation {
        arrow_schema,
        descriptors,
        schema_version,
        relation,
    })
}

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
