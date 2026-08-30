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
fn a_malformed_column_snapshot_stays_the_json_class() {
    // `from_tuple` propagates the decode failure with `?` now; the variant it lands in is the
    // documented contract, so a bad snapshot must not read as a missing column.
    let rel = ddl_audit_rel();
    let err = DdlEvent::from_tuple(
        &rel,
        &tuple(
            "0/1",
            "ddl_command_end",
            "ALTER TABLE",
            "public",
            "orders",
            "{not json",
        ),
    )
    .unwrap_err();
    assert!(matches!(&err, DdlError::Json(_)));
    assert!(err.to_string().starts_with("parse c_columns json: "));
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
    let mut c = DdlConsumer::new(common::EpochNo(1));
    assert_eq!(c.version_of("public", "orders"), common::SchemaVersionNo(1));
    // The bookkeeping consume() performs for a structural event — the half that needs no DB.
    assert!(c.versions.is_empty());

    assert_eq!(c.bump("public", "orders"), common::SchemaVersionNo(2));
    // A metadata-only event skips that call entirely, so a later read finds the structural version.
    assert_eq!(c.version_of("public", "orders"), common::SchemaVersionNo(2));
}

/// The version lookup matches on BOTH halves of the qualified name, so a table sharing one half
/// with a bumped one still reads its own version (and a repeat bump edits the entry it found).
#[test]
fn each_qualified_table_name_keeps_its_own_version() {
    let mut c = DdlConsumer::new(common::EpochNo(1));
    assert_eq!(c.bump("public", "orders"), common::SchemaVersionNo(2));
    assert_eq!(c.bump("public", "orders"), common::SchemaVersionNo(3));

    assert_eq!(c.version_of("public", "orders"), common::SchemaVersionNo(3));
    assert_eq!(c.version_of("public", "items"), common::SchemaVersionNo(1));
    assert_eq!(c.version_of("other", "orders"), common::SchemaVersionNo(1));
    assert_eq!(c.bump("other", "orders"), common::SchemaVersionNo(2));
    assert_eq!(c.version_of("public", "orders"), common::SchemaVersionNo(3));
}
