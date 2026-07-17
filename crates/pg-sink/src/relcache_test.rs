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
    assert!(is_internal_table("walrus", "reload_signal"));
    assert!(!is_internal_table("public", "orders"));
    assert!(
        !is_internal_table("public", "reload_signal"),
        "schema-scoped"
    );
    assert!(!is_internal_table("walrus", "something_else"));
}
