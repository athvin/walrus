use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};

fn col(name: &str, oid: u32, typmod: i32) -> PgColumn {
    PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key: false,
    }
}

fn orders() -> PgRelation {
    PgRelation {
        oid: 16397,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            PgColumn {
                name: "id".to_string(),
                type_oid: oids::INT4,
                type_modifier: -1,
                is_key: true,
            },
            col("amount", oids::NUMERIC, 655366), // numeric(10,2)
            col("created_at", oids::TIMESTAMPTZ, -1),
            col("note", oids::TEXT, -1),
        ],
    }
}

#[test]
fn orders_relation_maps_to_expected_tier1_schema() {
    let schema = build_schema(&orders()).unwrap();
    let f = schema.fields();
    assert_eq!(f.len(), 5);
    assert_eq!(f[0].name(), "id");
    assert_eq!(f[0].data_type(), &DataType::Int32);
    assert_eq!(f[1].name(), "amount");
    assert_eq!(f[1].data_type(), &DataType::Decimal128(10, 2));
    assert_eq!(f[2].name(), "created_at");
    assert_eq!(
        f[2].data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(f[3].name(), "note");
    assert_eq!(f[3].data_type(), &DataType::Utf8);
    assert_eq!(f[4].name(), SINK_META_COLUMN);
}

#[test]
fn timestamptz_carries_utc_but_timestamp_does_not() {
    assert_eq!(
        tier1_data_type(oids::TIMESTAMPTZ, -1),
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into())
        ))
    );
    assert_eq!(
        tier1_data_type(oids::TIMESTAMP, -1),
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    );
    assert_eq!(
        tier1_data_type(oids::TIME, -1),
        Some(DataType::Time64(TimeUnit::Microsecond))
    );
    assert_eq!(tier1_data_type(oids::DATE, -1), Some(DataType::Date32));
}

#[test]
fn numeric_10_2_is_decimal128_10_2() {
    assert_eq!(numeric_precision_scale(655366), Some((10, 2)));
    assert_eq!(
        tier1_data_type(oids::NUMERIC, 655366),
        Some(DataType::Decimal128(10, 2))
    );
}

#[test]
fn numeric_typmod_minus_one_is_unconstrained() {
    assert_eq!(numeric_precision_scale(-1), None);
    // unconstrained numeric is a Tier-3 VARCHAR carrier (PR 2.15), not Tier-1.
    assert_eq!(tier1_data_type(oids::NUMERIC, -1), None);
}

#[test]
fn unconstrained_and_over_38_numeric_emit_one_utf8_field() {
    // The Tier-3 numeric branch (PR 2.15): a single Utf8 carrier, not Decimal128.
    for typmod in [-1, ((40 << 16) | 10) + 4] {
        let fields = emit_fields(&col("amount", oids::NUMERIC, typmod)).unwrap();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "amount");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
    }
    // But numeric(10,2) still stays Tier-1 Decimal128 (no regression).
    let d = emit_fields(&col("amount", oids::NUMERIC, 655366)).unwrap();
    assert_eq!(d[0].data_type(), &DataType::Decimal128(10, 2));
}

#[test]
fn meta_column_is_last_and_non_null_utf8() {
    let schema = build_schema(&orders()).unwrap();
    let last = schema.fields().last().unwrap();
    assert_eq!(last.name(), SINK_META_COLUMN);
    assert_eq!(last.data_type(), &DataType::Utf8);
    assert!(!last.is_nullable(), "the meta column is always present");
    assert!(schema.fields()[0].is_nullable(), "data fields are nullable");
}

#[test]
fn tier1_column_still_emits_exactly_one_field() {
    // No regression from PR 2.9: a native type expands to a single field named after the column.
    let fields = emit_fields(&col("note", oids::TEXT, -1)).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "note");
    assert_eq!(fields[0].data_type(), &DataType::Utf8);
}

#[test]
fn interval_emits_three_signed_int_fields() {
    let fields = emit_fields(&col("dur", oids::INTERVAL, -1)).unwrap();
    let shape: Vec<(&str, &DataType)> = fields
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("dur_months", &DataType::Int32),
            ("dur_days", &DataType::Int32),
            ("dur_micros", &DataType::Int64),
        ]
    );
    assert!(fields.iter().all(|f| f.is_nullable()));
}

#[test]
fn timetz_emits_micros_and_offset_fields() {
    let fields = emit_fields(&col("t", oids::TIMETZ, -1)).unwrap();
    let shape: Vec<(&str, &DataType)> = fields
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("t_micros", &DataType::Int64),
            ("t_offset_seconds", &DataType::Int32),
        ]
    );
}

#[test]
fn interval_fields_expand_within_the_relation_schema() {
    // A Tier-2 column widens the flat schema: 1 int col + interval(3) + meta = 5 fields.
    let rel = PgRelation {
        oid: 2,
        schema: "public".to_string(),
        name: "spans".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", oids::INT4, -1), col("dur", oids::INTERVAL, -1)],
    };
    let schema = build_schema(&rel).unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "id",
            "dur_months",
            "dur_days",
            "dur_micros",
            SINK_META_COLUMN
        ]
    );
}

#[test]
fn still_unhandled_oid_errors() {
    // money (790) is a builtin with no mapping yet — still NotTier1 (and not caught by the
    // non-builtin enum rule, since 790 < FIRST_NORMAL_OID).
    let err = emit_fields(&col("m", 790, -1)).unwrap_err();
    assert!(matches!(err, Error::NotTier1 { oid: 790, .. }));
}

#[test]
fn uuid_emits_fixed_size_binary_with_extension_and_enum_is_utf8() {
    let u = emit_fields(&col("id", oids::UUID, -1)).unwrap();
    assert_eq!(u.len(), 1);
    assert_eq!(u[0].data_type(), &DataType::FixedSizeBinary(16));
    assert_eq!(
        u[0].metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.uuid")
    );
    // A non-builtin OID is treated as enum → Utf8 (interim, PR 2.22 resolves via catalog).
    let e = emit_fields(&col("status", 16400, -1)).unwrap();
    assert_eq!(e[0].data_type(), &DataType::Utf8);
}

#[test]
fn geometric_point_emits_one_struct_field() {
    let fields = emit_fields(&col("loc", oids::POINT, -1)).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "loc");
    match fields[0].data_type() {
        DataType::Struct(sf) => {
            let names: Vec<&str> = sf.iter().map(|f| f.name().as_str()).collect();
            assert_eq!(names, vec!["x", "y"]);
        }
        other => panic!("expected STRUCT(x,y), got {other:?}"),
    }
}

#[test]
fn int4range_emits_five_flat_columns() {
    let fields = emit_fields(&col("span", oids::INT4RANGE, -1)).unwrap();
    let shape: Vec<(&str, &DataType)> = fields
        .iter()
        .map(|f| (f.name().as_str(), f.data_type()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("span_lower", &DataType::Int32),
            ("span_upper", &DataType::Int32),
            ("span_lower_inc", &DataType::Boolean),
            ("span_upper_inc", &DataType::Boolean),
            ("span_empty", &DataType::Boolean),
        ]
    );
}

#[test]
fn int4multirange_emits_one_list_of_struct() {
    let fields = emit_fields(&col("spans", oids::INT4MULTIRANGE, -1)).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "spans");
    match fields[0].data_type() {
        DataType::List(item) => match item.data_type() {
            DataType::Struct(sf) => {
                let names: Vec<&str> = sf.iter().map(|f| f.name().as_str()).collect();
                assert_eq!(names, vec!["lower", "upper", "lower_inc", "upper_inc"]);
                assert_eq!(sf[0].data_type(), &DataType::Int32);
            }
            other => panic!("expected LIST<STRUCT>, item was {other:?}"),
        },
        other => panic!("expected LIST, got {other:?}"),
    }
}

#[test]
fn empty_relation_errors() {
    let rel = PgRelation {
        oid: 1,
        schema: "public".to_string(),
        name: "empty".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![],
    };
    assert!(matches!(
        build_schema(&rel),
        Err(Error::EmptyRelation { .. })
    ));
}
