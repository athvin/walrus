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

/// The two walrus-internal source tables: control-plane, never registered or schematised as user data.
pub fn is_internal_table(schema: &str, table: &str) -> bool {
    schema == "walrus" && (table == "ddl_audit" || table == "heartbeat")
}

#[derive(Debug, Default)]
pub struct RelationCache {
    by_key: HashMap<(u32, i64), Arc<CachedRelation>>,
}

impl RelationCache {
    pub fn get(&self, oid: u32, schema_version: i64) -> Option<Arc<CachedRelation>> {
        self.by_key.get(&(oid, schema_version)).cloned()
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
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use common::{PgColumn, ReplicaIdentity};
    use pg_to_arrow::oids;

    fn orders() -> PgRelation {
        let col = |name: &str, oid: u32, typmod: i32, is_key: bool| PgColumn {
            name: name.to_string(),
            type_oid: oid,
            type_modifier: typmod,
            is_key,
        };
        PgRelation {
            oid: 16397,
            schema: "public".to_string(),
            name: "orders".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                col("id", oids::INT4, -1, true),
                col("amount", oids::NUMERIC, 655366, false), // numeric(10,2)
                col("created_at", oids::TIMESTAMPTZ, -1, false),
                col("note", oids::TEXT, -1, false),
            ],
        }
    }

    #[test]
    fn caches_arrow_schema_and_descriptors_by_versioned_key() {
        let mut cache = RelationCache::default();
        let entry = cache.upsert_from_relation(orders(), 1).unwrap();
        // Tier-1 schema (PR 2.9): 4 data cols + the trailing meta col.
        assert_eq!(entry.arrow_schema.fields().len(), 5);
        assert_eq!(entry.arrow_schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(
            entry.arrow_schema.field(4).name(),
            pg_to_arrow::SINK_META_COLUMN
        );
        assert_eq!(entry.descriptors.len(), 4);
        // Keyed by (oid, version): a lookup at a different version misses.
        assert!(cache.get(16397, 1).is_some());
        assert!(cache.get(16397, 2).is_none());
    }

    #[test]
    fn hydrate_round_trips_through_a_registry_row() {
        // Simulate what on_relation persists, then hydrate a fresh cache from it.
        let relation = orders();
        let descriptors = pg_to_arrow::descriptor::describe_relation(&relation);
        let row = control::RegistryRow {
            epoch: 1,
            source_schema: "public".to_string(),
            source_table: "orders".to_string(),
            schema_version: 3,
            descriptors: descriptors.clone(),
            columns: serde_json::to_value(&relation).unwrap(),
        };
        let mut cache = RelationCache::default();
        cache.hydrate(vec![row]).unwrap();
        let entry = cache.get(16397, 3).expect("hydrated entry");
        assert_eq!(entry.relation, relation);
        assert_eq!(entry.descriptors, descriptors);
        assert_eq!(entry.arrow_schema.fields().len(), 5);
    }

    #[test]
    fn internal_tables_are_recognised() {
        assert!(is_internal_table("walrus", "heartbeat"));
        assert!(is_internal_table("walrus", "ddl_audit"));
        assert!(!is_internal_table("public", "orders"));
        assert!(!is_internal_table("walrus", "something_else"));
    }
}
