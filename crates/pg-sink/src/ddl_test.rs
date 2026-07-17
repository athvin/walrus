use super::*;
use common::{PgColumn, ReplicaIdentity};

fn ddl_audit_rel() -> PgRelation {
    let col = |name: &str| PgColumn {
        name: name.into(),
        type_oid: 25,
        type_modifier: -1,
        is_key: false,
    };
    PgRelation {
        oid: 90002,
        schema: "walrus".into(),
        name: "ddl_audit".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id"),
            col("c_lsn"),
            col("c_event"),
            col("c_tag"),
            col("ts"),
            col("c_schema"),
            col("c_table"),
            col("c_columns"),
            col("c_dropped"),
        ],
    }
}

fn tuple(
    c_lsn: &str,
    event: &str,
    tag: &str,
    schema: &str,
    table: &str,
    cols: &str,
) -> Vec<TupleValue> {
    vec![
        TupleValue::Text("1".into()),
        TupleValue::Text(c_lsn.into()),
        TupleValue::Text(event.into()),
        TupleValue::Text(tag.into()),
        TupleValue::Text("2026-07-07T12:00:00Z".into()),
        TupleValue::Text(schema.into()),
        TupleValue::Text(table.into()),
        TupleValue::Text(cols.into()),
        TupleValue::Null,
    ]
}

#[test]
fn ddl_audit_insert_parses_into_event_with_c_lsn() {
    let rel = ddl_audit_rel();
    let ev = DdlEvent::from_tuple(
        &rel,
        &tuple(
            "1/AB",
            "ddl_command_end",
            "ALTER TABLE",
            "public",
            "orders",
            r#"[{"name":"id"}]"#,
        ),
    )
    .unwrap();
    assert_eq!(ev.c_lsn, "1/AB".parse().unwrap());
    assert_eq!(ev.c_tag, "ALTER TABLE");
    assert_eq!(ev.source_schema, "public");
    assert_eq!(ev.source_table, "orders");
    assert!(ev.c_columns.is_some());
}

#[test]
fn alter_table_is_structural_comment_is_metadata_only() {
    let rel = ddl_audit_rel();
    let alter = DdlEvent::from_tuple(
        &rel,
        &tuple(
            "0/1",
            "ddl_command_end",
            "ALTER TABLE",
            "public",
            "orders",
            "[]",
        ),
    )
    .unwrap();
    let comment = DdlEvent::from_tuple(
        &rel,
        &tuple(
            "0/2",
            "ddl_command_end",
            "COMMENT",
            "public",
            "orders",
            "[]",
        ),
    )
    .unwrap();
    assert!(alter.is_structural());
    assert!(!comment.is_structural(), "COMMENT is metadata-only");
}

#[test]
fn structural_ddl_bumps_version_metadata_does_not() {
    let mut c = DdlConsumer::new(1);
    assert_eq!(c.version_of("public", "orders"), 1);
    // Simulate the version bookkeeping consume() performs (no DB).
    assert!(c.versions.is_empty());
    // structural
    let v = c
        .versions
        .entry(("public".into(), "orders".into()))
        .or_insert(1);
    *v += 1;
    assert_eq!(c.version_of("public", "orders"), 2);
}
