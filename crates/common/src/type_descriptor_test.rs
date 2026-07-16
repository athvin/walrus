use super::*;

/// The walrus-pg-sink.md §2.6 interval descriptor, comment-free.
const DOCS_DESCRIPTOR: &str = r#"{
        "column": "duration",
        "pg_type_oid": 1186,
        "pg_type": "interval",
        "tier": 2,
        "arrow": "Struct/Decomposed",
        "duckdb": "INTERVAL",
        "emit": ["duration_months:INT32", "duration_days:INT32", "duration_micros:INT64"],
        "recombine": "to_months(m)+to_days(d)+to_microseconds(us)",
        "meta": {
            "enum_labels": null,
            "bit_length": null,
            "char_length": null,
            "money_fraction_digits": null
        }
    }"#;

#[test]
fn tier_serializes_as_integer() {
    assert_eq!(serde_json::to_string(&Tier::One).unwrap(), "1");
    assert_eq!(serde_json::to_string(&Tier::Two).unwrap(), "2");
    assert_eq!(serde_json::to_string(&Tier::Three).unwrap(), "3");
    assert_eq!(serde_json::from_str::<Tier>("2").unwrap(), Tier::Two);
    assert!(serde_json::from_str::<Tier>("4").is_err());
    // A quoted string is NOT a valid tier — the contract is a JSON number.
    assert!(serde_json::from_str::<Tier>("\"2\"").is_err());
}

#[test]
fn type_descriptor_round_trips_the_docs_example() {
    let d: TypeDescriptor = serde_json::from_str(DOCS_DESCRIPTOR).unwrap();
    assert_eq!(d.column, "duration");
    assert_eq!(d.pg_type_oid, 1186);
    assert_eq!(d.pg_type, "interval");
    assert_eq!(d.tier, Tier::Two);
    assert_eq!(d.emit.len(), 3);
    assert_eq!(
        d.recombine.as_deref(),
        Some("to_months(m)+to_days(d)+to_microseconds(us)")
    );
    assert_eq!(d.meta, TypeMeta::default()); // all None

    // Re-serialize and confirm every key/value matches the §2.6 block (order-independent).
    let reserialized: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
    let expected: serde_json::Value = serde_json::from_str(DOCS_DESCRIPTOR).unwrap();
    assert_eq!(reserialized, expected);
    // `tier` is the integer 2, not the string "2".
    assert_eq!(reserialized["tier"], serde_json::json!(2));
}

#[test]
fn tier_one_scalar_descriptor_round_trips() {
    let d = TypeDescriptor {
        column: "id".to_string(),
        pg_type_oid: 23,
        pg_type: "int4".to_string(),
        tier: Tier::One,
        arrow: "Int32".to_string(),
        duckdb: "INTEGER".to_string(),
        emit: vec!["id:INT32".to_string()],
        recombine: None,
        meta: TypeMeta::default(),
    };
    let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
    assert_eq!(v["tier"], serde_json::json!(1));
    assert_eq!(v["recombine"], serde_json::Value::Null);

    let back: TypeDescriptor = serde_json::from_value(v).unwrap();
    assert_eq!(back, d);
}

#[test]
fn type_meta_carries_enum_labels() {
    let meta = TypeMeta {
        enum_labels: Some(vec![
            "happy".to_string(),
            "meh".to_string(),
            "sad".to_string(),
        ]),
        ..TypeMeta::default()
    };
    let v: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
    assert_eq!(v["enum_labels"], serde_json::json!(["happy", "meh", "sad"]));
    assert_eq!(v["bit_length"], serde_json::Value::Null);
}
