use super::*;

fn col(name: &str, type_oid: u32, type_modifier: i32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.to_string(),
        type_oid,
        type_modifier,
        is_key,
    }
}

#[test]
fn replica_identity_from_wire_char() {
    assert_eq!(
        ReplicaIdentity::from_wire(b'd').unwrap(),
        ReplicaIdentity::Default
    );
    assert_eq!(
        ReplicaIdentity::from_wire(b'n').unwrap(),
        ReplicaIdentity::Nothing
    );
    assert_eq!(
        ReplicaIdentity::from_wire(b'f').unwrap(),
        ReplicaIdentity::Full
    );
    assert_eq!(
        ReplicaIdentity::from_wire(b'i').unwrap(),
        ReplicaIdentity::Index
    );
    assert!(ReplicaIdentity::from_wire(b'x').is_err());
    assert!(ReplicaIdentity::from_wire(0).is_err());
}

#[test]
fn numeric_typmod_decodes_precision_and_scale() {
    // The proto §4 example: atttypmod 655366 → numeric(10, 2).
    let c = col("amount", NUMERIC_OID, 655366, false);
    assert_eq!(c.numeric_precision_scale(), Some((10, 2)));

    // Unconstrained numeric → None (no panic).
    assert_eq!(
        col("n", NUMERIC_OID, -1, false).numeric_precision_scale(),
        None
    );

    // A non-numeric column with a typmod (e.g. varchar) → None.
    assert_eq!(
        col("label", 1043, 259, false).numeric_precision_scale(),
        None
    );
}

#[test]
fn key_columns_preserve_relation_order() {
    // customers has a COMPOSITE PK (region, id); order must be preserved.
    let rel = PgRelation {
        oid: 42,
        schema: "public".to_string(),
        name: "customers".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("region", 25, -1, true),
            col("id", 23, -1, true),
            col("name", 25, -1, false),
        ],
    };
    assert_eq!(rel.key_columns(), vec!["region", "id"]);

    // A key column declared after a non-key one still keeps relation order.
    let rel2 = PgRelation {
        columns: vec![
            col("a", 23, -1, false),
            col("b", 23, -1, true),
            col("c", 23, -1, true),
        ],
        ..rel
    };
    assert_eq!(rel2.key_columns(), vec!["b", "c"]);
}

#[test]
fn tuple_value_null_and_unchanged_toast_are_distinct() {
    assert_ne!(TupleValue::Null, TupleValue::UnchangedToast);
    assert_eq!(TupleValue::Null, TupleValue::Null);
    assert_eq!(
        TupleValue::Text("x".to_string()),
        TupleValue::Text("x".to_string())
    );
    // Binary carries bytes zero-copy.
    assert_eq!(
        TupleValue::Binary(Bytes::from_static(b"\x00\x01")),
        TupleValue::Binary(Bytes::from_static(b"\x00\x01"))
    );
    assert_ne!(
        TupleValue::Binary(Bytes::from_static(b"\x00")),
        TupleValue::Null
    );
}
