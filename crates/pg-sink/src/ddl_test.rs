use super::*;

fn ddl_audit_rel() -> PgRelation {
    let col = |name: &str| PgColumn {
        name: name.into(),
        type_oid: 25,
        type_modifier: -1,
        is_key: false,
    };
    PgRelation {
        oid: 90_002,
        schema: "walrus".into(),
        name: "ddl_audit".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: [
            "id",
            "c_lsn",
            "c_event",
            "c_tag",
            "ts",
            "c_schema",
            "c_table",
            "c_columns",
            "c_dropped",
            "c_rel_oid",
            "c_replica_identity",
            "c_ddl_text",
        ]
        .into_iter()
        .map(col)
        .collect(),
    }
}

fn tuple(id: i64, tag: &str, columns: &str) -> Vec<TupleValue> {
    vec![
        TupleValue::Text(id.to_string()),
        TupleValue::Text("1/AB".into()),
        TupleValue::Text("ddl_command_end".into()),
        TupleValue::Text(tag.into()),
        TupleValue::Text("2026-07-07T12:00:00Z".into()),
        TupleValue::Text("public".into()),
        TupleValue::Text("orders".into()),
        TupleValue::Text(columns.into()),
        TupleValue::Null,
        TupleValue::Text("42".into()),
        TupleValue::Text("d".into()),
        TupleValue::Text("ALTER TABLE orders ADD COLUMN extra text".into()),
    ]
}

fn event(id: i64, schema: &str, table: &str, tag: &str) -> DdlEvent {
    DdlEvent {
        source_audit_id: id,
        capture_lsn: Lsn::new(u64::try_from(id).unwrap()),
        c_event: "ddl_command_end".into(),
        c_tag: tag.into(),
        source_schema: schema.into(),
        source_table: table.into(),
        c_rel_oid: Some(42),
        c_replica_identity: Some(ReplicaIdentity::Default),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: Some(format!("{tag} {schema}.{table}")),
    }
}

fn registry(table: &str, version: i64) -> control::RegistryRow {
    control::RegistryRow {
        epoch: EpochNo(1),
        source_schema: "public".into(),
        source_table: table.into(),
        schema_version: SchemaVersionNo(version),
        descriptors: Vec::new(),
        columns: serde_json::json!({"name": table, "version": version}),
    }
}

#[test]
fn ddl_audit_insert_parses_identity_snapshot_and_audit_sql() {
    let ev = DdlEvent::from_tuple(
        &ddl_audit_rel(),
        &tuple(
            17,
            "ALTER TABLE",
            r#"[{"name":"id","type_oid":23,"type_modifier":-1,"is_key":true}]"#,
        ),
    )
    .unwrap();
    assert_eq!(ev.source_audit_id, 17);
    assert_eq!(ev.capture_lsn, "1/AB".parse().unwrap());
    assert_eq!(ev.c_rel_oid, Some(42));
    assert_eq!(ev.c_replica_identity, Some(ReplicaIdentity::Default));
    assert_eq!(
        ev.c_ddl_text.as_deref(),
        Some("ALTER TABLE orders ADD COLUMN extra text")
    );
}

#[test]
fn malformed_column_snapshot_stays_the_json_class() {
    let err =
        DdlEvent::from_tuple(&ddl_audit_rel(), &tuple(1, "ALTER TABLE", "{not json")).unwrap_err();
    assert!(matches!(&err, DdlError::Json(_)));
    assert!(err.to_string().starts_with("parse ddl_audit json: "));
}

#[test]
fn relation_snapshot_preserves_key_from_catalog_and_previous_shape() {
    let previous = PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![PgColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
            is_key: true,
        }],
    };
    let mut ev = event(1, "public", "orders", "ALTER TABLE");
    ev.c_columns = Some(serde_json::json!([
        {"name":"id", "type_oid":23, "type_modifier":-1},
        {"name":"extra", "type_oid":25, "type_modifier":-1, "is_key":false}
    ]));
    let after = ev.relation_after(Some(&previous)).unwrap().unwrap();
    assert!(
        after.columns[0].is_key,
        "old trigger snapshots inherit key flags"
    );
    assert!(!after.columns[1].is_key);
    assert_eq!(after.columns.len(), 2);
}

#[test]
fn comment_is_metadata_only_and_does_not_advance_projected_version() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let observation = consumer.observe(
        TransactionScope::Ordinary,
        event(1, "public", "orders", "COMMENT"),
    );
    assert_eq!(observation.structural_version, None);
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(1));
}

#[test]
fn provisional_version_is_visible_only_inside_its_streamed_transaction() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let owner = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 101,
    };
    let neighbour = TransactionScope::Streamed {
        top_xid: 200,
        sub_xid: 200,
    };
    let observation = consumer.observe(owner, event(1, "public", "orders", "ALTER TABLE"));
    assert_eq!(observation.structural_version, Some(SchemaVersionNo(2)));
    assert_eq!(
        consumer.version_for(owner, "public", "orders"),
        SchemaVersionNo(2)
    );
    assert_eq!(
        consumer.version_for(neighbour, "public", "orders"),
        SchemaVersionNo(1)
    );
    assert_eq!(
        consumer.committed_version_of("public", "orders"),
        SchemaVersionNo(1)
    );
}

#[test]
fn subtransaction_abort_removes_only_its_schema_version() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    let orders_scope = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 101,
    };
    let items_scope = TransactionScope::Streamed {
        top_xid: 100,
        sub_xid: 102,
    };
    consumer.observe(orders_scope, event(1, "public", "orders", "ALTER TABLE"));
    consumer.observe(items_scope, event(2, "public", "items", "ALTER TABLE"));
    consumer.stage_registry(orders_scope, registry("orders", 2));
    consumer.stage_registry(items_scope, registry("items", 2));

    let removed = consumer.on_stream_abort(100, 101);
    assert_eq!(
        removed,
        vec![("public".into(), "orders".into(), SchemaVersionNo(2))]
    );
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(1));
    assert_eq!(consumer.version_of("public", "items"), SchemaVersionNo(2));
    assert_eq!(consumer.pending_registry.len(), 1);
    assert_eq!(consumer.pending_registry[0].row.source_table, "items");
}

#[test]
fn whole_stream_abort_removes_every_provisional_ddl_for_the_top_xid() {
    let mut consumer = DdlConsumer::new(EpochNo(1));
    for (id, sub) in [(1, 101), (2, 102)] {
        consumer.observe(
            TransactionScope::Streamed {
                top_xid: 100,
                sub_xid: sub,
            },
            event(id, "public", "orders", "ALTER TABLE"),
        );
    }
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(3));
    assert_eq!(consumer.on_stream_abort(100, 100).len(), 2);
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(1));
}

#[test]
fn hydrated_audit_identity_replays_without_another_version_bump() {
    let mut consumer = DdlConsumer::new(EpochNo(7));
    consumer.hydrate_history(vec![control::DdlRow {
        id: DdlId(9),
        epoch: EpochNo(7),
        source_audit_id: 55,
        source_schema: "public".into(),
        source_table: "orders".into(),
        c_lsn: Lsn::new(900),
        c_event: "ddl_command_end".into(),
        c_tag: "ALTER TABLE".into(),
        schema_version: SchemaVersionNo(2),
        c_rel_oid: Some(42),
        c_columns: Some(serde_json::json!([])),
        c_dropped: None,
        c_ddl_text: None,
    }]);

    let observation = consumer.observe(
        TransactionScope::Ordinary,
        event(55, "public", "orders", "ALTER TABLE"),
    );
    assert!(observation.replay);
    assert_eq!(observation.structural_version, Some(SchemaVersionNo(2)));
    assert_eq!(consumer.version_of("public", "orders"), SchemaVersionNo(2));
}
