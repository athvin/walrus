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

fn table_at(oid: u32, name: &str) -> PgRelation {
    let mut relation = orders();
    relation.oid = oid;
    relation.name = name.to_string();
    relation
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

#[test]
fn latest_for_picks_the_highest_version_per_oid_across_interleaved_tables() {
    let mut cache = RelationCache::default();
    for (oid, name, version) in [
        (16397, "orders", 3),
        (16400, "customers", 1),
        (16397, "orders", 1),
        (16401, "products", 7),
        (16397, "orders", 2),
        (16400, "customers", 5),
    ] {
        cache
            .upsert_from_relation(table_at(oid, name), SchemaVersionNo(version))
            .unwrap();
    }

    assert_eq!(
        cache.latest_for(16397).unwrap().schema_version,
        SchemaVersionNo(3)
    );
    assert_eq!(
        cache.latest_for(16400).unwrap().schema_version,
        SchemaVersionNo(5)
    );
    assert_eq!(
        cache.latest_for(16401).unwrap().schema_version,
        SchemaVersionNo(7)
    );
    assert!(cache.latest_for(9999).is_none());
}

#[test]
fn latest_for_respects_neighbour_and_full_integer_range_edges() {
    let mut cache = RelationCache::default();
    for (oid, name, version) in [
        (42, "lower", i64::MIN),
        (43, "neighbour", i64::MAX),
        (u32::MAX, "max_oid", i64::MIN),
        (u32::MAX, "max_oid", i64::MAX),
    ] {
        cache
            .upsert_from_relation(table_at(oid, name), SchemaVersionNo(version))
            .unwrap();
    }

    assert_eq!(
        cache.latest_for(42).unwrap().schema_version,
        SchemaVersionNo(i64::MIN)
    );
    assert_eq!(
        cache.latest_for(43).unwrap().schema_version,
        SchemaVersionNo(i64::MAX)
    );
    assert_eq!(
        cache.latest_for(u32::MAX).unwrap().schema_version,
        SchemaVersionNo(i64::MAX)
    );
}

#[test]
fn btree_iterators_preserve_items_and_follow_key_order() {
    let entries = [
        build_cached(table_at(9, "nine"), SchemaVersionNo(3)).unwrap(),
        build_cached(table_at(2, "two"), SchemaVersionNo(7)).unwrap(),
        build_cached(table_at(9, "nine"), SchemaVersionNo(-1)).unwrap(),
        build_cached(table_at(3, "three"), SchemaVersionNo(0)).unwrap(),
    ];
    let mut cache: RelationCache = entries.into_iter().collect();
    let expected = vec![
        (2, SchemaVersionNo(7)),
        (3, SchemaVersionNo(0)),
        (9, SchemaVersionNo(-1)),
        (9, SchemaVersionNo(3)),
    ];

    let values: std::collections::btree_map::Values<
        '_,
        (u32, SchemaVersionNo),
        Arc<CachedRelation>,
    > = cache.iter();
    assert_eq!(
        values
            .map(|cached| (cached.relation.oid, cached.schema_version))
            .collect::<Vec<_>>(),
        expected
    );

    let shared: std::collections::btree_map::Values<
        '_,
        (u32, SchemaVersionNo),
        Arc<CachedRelation>,
    > = (&cache).into_iter();
    assert_eq!(
        shared
            .map(|cached| (cached.relation.oid, cached.schema_version))
            .collect::<Vec<_>>(),
        expected
    );

    let mutable: std::collections::btree_map::ValuesMut<
        '_,
        (u32, SchemaVersionNo),
        Arc<CachedRelation>,
    > = (&mut cache).into_iter();
    assert_eq!(
        mutable
            .map(|cached| (cached.relation.oid, cached.schema_version))
            .collect::<Vec<_>>(),
        expected
    );

    let owned: std::collections::btree_map::IntoValues<
        (u32, SchemaVersionNo),
        Arc<CachedRelation>,
    > = cache.into_iter();
    assert_eq!(
        owned
            .map(|cached| (cached.relation.oid, cached.schema_version))
            .collect::<Vec<_>>(),
        expected
    );
}
