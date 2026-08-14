use super::*;
use arrow::datatypes::DataType;
use common::{PgColumn, ReplicaIdentity, SchemaVersionNo};
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
    let entry = cache
        .upsert_from_relation(orders(), SchemaVersionNo(1))
        .unwrap();
    // Tier-1 schema (PR 2.9): 4 data cols + the trailing meta col.
    assert_eq!(entry.arrow_schema.fields().len(), 5);
    assert_eq!(entry.arrow_schema.field(0).data_type(), &DataType::Int32);
    assert_eq!(
        entry.arrow_schema.field(4).name(),
        pg_to_arrow::SINK_META_COLUMN
    );
    assert_eq!(entry.descriptors.len(), 4);
    // Keyed by (oid, version): a lookup at a different version misses.
    assert!(cache.get(16397, SchemaVersionNo(1)).is_some());
    assert!(cache.get(16397, SchemaVersionNo(2)).is_none());
}

#[test]
fn hydrate_round_trips_through_a_registry_row() {
    // Simulate what on_relation persists, then hydrate a fresh cache from it.
    let relation = orders();
    let descriptors = pg_to_arrow::descriptor::describe_relation(&relation).unwrap();
    let row = control::RegistryRow {
        epoch: common::EpochNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: SchemaVersionNo(3),
        descriptors: descriptors.clone(),
        columns: serde_json::to_value(&relation).unwrap(),
    };
    let mut cache = RelationCache::default();
    cache.hydrate(vec![row]).unwrap();
    let entry = cache
        .get(16397, SchemaVersionNo(3))
        .expect("hydrated entry");
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

#[test]
fn collects_and_extends_like_a_collection() {
    let first = build_cached(orders(), SchemaVersionNo(1)).unwrap();
    let second = build_cached(orders(), SchemaVersionNo(2)).unwrap();
    let cache: RelationCache = [first, second].into_iter().collect();

    assert_eq!(cache.len(), 2);
    assert!(cache.get(16397, SchemaVersionNo(1)).is_some());
    assert!(cache.get(16397, SchemaVersionNo(2)).is_some());

    let mut grown = RelationCache::default();
    grown.extend([build_cached(orders(), SchemaVersionNo(3)).unwrap()]);
    assert_eq!(grown.len(), 1);
    assert!(grown.get(16397, SchemaVersionNo(3)).is_some());
}

#[test]
fn iterates_by_ref_by_mut_and_by_value() {
    let cache: RelationCache = [build_cached(orders(), SchemaVersionNo(7)).unwrap()]
        .into_iter()
        .collect();

    assert_eq!(cache.iter().count(), 1);
    assert_eq!((&cache).into_iter().count(), 1);

    let mut cache = cache;
    for relation in &mut cache {
        let before = Arc::strong_count(relation);
        let clone = Arc::clone(relation);
        assert_eq!(Arc::strong_count(relation), before + 1);
        drop(clone);
    }

    let versions: Vec<SchemaVersionNo> = cache
        .into_iter()
        .map(|relation| relation.schema_version)
        .collect();
    assert_eq!(versions, vec![SchemaVersionNo(7)]);
}

#[test]
fn hydrate_message_is_unchanged_on_a_malformed_snapshot() {
    let relation = orders();
    let good = control::RegistryRow {
        epoch: common::EpochNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: SchemaVersionNo(1),
        descriptors: pg_to_arrow::descriptor::describe_relation(&relation).unwrap(),
        columns: serde_json::to_value(relation).unwrap(),
    };
    let malformed = serde_json::json!({"not": "a PgRelation"});
    let source = serde_json::from_value::<PgRelation>(malformed.clone()).unwrap_err();
    let bad = control::RegistryRow {
        epoch: common::EpochNo(1),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        schema_version: SchemaVersionNo(2),
        descriptors: vec![],
        columns: malformed,
    };
    let mut cache = RelationCache::default();

    let error = cache.hydrate(vec![good, bad]).unwrap_err();

    assert_eq!(
        error.to_string(),
        format!(
            "hydrate from schema_registry: public.orders: columns snapshot is not a PgRelation: {source}"
        )
    );
    assert!(
        cache.is_empty(),
        "hydration must not partially update the cache"
    );
}
