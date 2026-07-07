//! Build the **Tier-1** Arrow schema for a source relation.
//!
//! The one rule that governs everything (walrus-pg-sink.md §2.1): DuckDB infers column types from
//! Parquet's **native logical types**, so every distinction must be a real logical type. That means
//! **MICROS for every temporal** (NANOS+tz silently downgrades in DuckDB; MILLIS truncates) and
//! `Decimal128` for `numeric(p ≤ 38)`. Anything not (yet) Tier-1 returns `None`/`NotTier1` rather
//! than a fallback field — a wrong-but-compiling mapping is the bug PR 2.11's conformance tests catch.

use crate::error::Error;
use crate::oids;
use crate::range::RangeFamily;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::{PgColumn, PgRelation};

/// The trailing provenance column every walrus Parquet file carries (JSON text).
pub const SINK_META_COLUMN: &str = "walrus_pg_sink_meta";

/// Full Arrow schema for `rel`: one field per Tier-1 column, then `walrus_pg_sink_meta` (Utf8,
/// non-null). Data fields are `nullable`; the meta column is not.
pub fn build_schema(rel: &PgRelation) -> Result<Schema, Error> {
    if rel.columns.is_empty() {
        return Err(Error::EmptyRelation {
            relation: format!("{}.{}", rel.schema, rel.name),
        });
    }
    // One source column may fan out to several fields (Tier-2, PR 2.12), so we `extend` rather than
    // `push` — the seam that interval/timetz and every remaining type PR (2.13–2.16) plug into.
    let mut fields: Vec<Field> = Vec::with_capacity(rel.columns.len() + 1);
    for col in &rel.columns {
        fields.extend(emit_fields(col)?);
    }
    // Provenance rides in one JSON-text column, always present → non-nullable.
    fields.push(Field::new(SINK_META_COLUMN, DataType::Utf8, false));
    Ok(Schema::new(fields))
}

/// One source column → the Arrow field(s) the sink emits for it. **Tier-1** maps 1:1 (one field);
/// **Tier-2** (`interval`, `timetz`, …) decomposes into several sibling fields the loader recombines
/// (§2.4). **Data fields stay `nullable(true)`**: delete old-images and unchanged-TOAST placeholders
/// arrive partial, so even a PK column can be absent on the wire; the mirror's PK-not-null is enforced
/// downstream in the loader, not here.
pub fn emit_fields(col: &PgColumn) -> Result<Vec<Field>, Error> {
    if let Some(dt) = tier1_data_type(col.type_oid, col.type_modifier) {
        return Ok(vec![Field::new(col.name.clone(), dt, true)]);
    }
    match col.type_oid {
        oids::INTERVAL => return Ok(crate::tier2::interval_fields(&col.name)),
        oids::TIMETZ => return Ok(crate::tier2::timetz_fields(&col.name)),
        _ => {}
    }
    // Range → 5 flat sibling columns; multirange → one LIST<STRUCT> (§2.4, PR 2.13).
    if let Some(fam) = RangeFamily::from_range_oid(col.type_oid) {
        return Ok(crate::tier2::range_fields(
            &col.name,
            fam,
            col.type_modifier,
        ));
    }
    if let Some(fam) = RangeFamily::from_multirange_oid(col.type_oid) {
        return Ok(vec![crate::tier2::multirange_field(
            &col.name,
            fam,
            col.type_modifier,
        )]);
    }
    Err(Error::NotTier1 {
        oid: col.type_oid,
        typmod: col.type_modifier,
    })
}

/// Arrow `DataType` for a Tier-1 OID+typmod, or `None` if the type is not (yet) Tier-1.
pub fn tier1_data_type(type_oid: u32, atttypmod: i32) -> Option<DataType> {
    Some(match type_oid {
        oids::BOOL => DataType::Boolean,
        oids::INT2 => DataType::Int16,
        oids::INT4 => DataType::Int32,
        oids::INT8 => DataType::Int64,
        oids::FLOAT4 => DataType::Float32,
        oids::FLOAT8 => DataType::Float64,
        // Two numeric cases, kept strictly apart (§2.3): p ≤ 38 is a lossless Decimal128;
        // unconstrained (typmod -1) or p > 38 is a Tier-3 VARCHAR carrier (PR 2.15) — NOT here.
        oids::NUMERIC => {
            let (precision, scale) = numeric_precision_scale(atttypmod)?;
            if precision == 0 || precision > 38 {
                return None;
            }
            DataType::Decimal128(precision, scale)
        }
        oids::TEXT | oids::VARCHAR | oids::BPCHAR | oids::CHAR => DataType::Utf8,
        oids::BYTEA => DataType::Binary,
        // json / jsonb ride as UTF-8 text (DuckDB infers JSON from the string).
        oids::JSON | oids::JSONB => DataType::Utf8,
        oids::DATE => DataType::Date32,
        oids::TIME => DataType::Time64(TimeUnit::Microsecond),
        oids::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, None),
        // Some("UTC") is the marker DuckDB reads as isAdjustedToUTC=true.
        oids::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        _ => return None,
    })
}

/// Decode a `numeric` `atttypmod` into `(precision, scale)`, or `None` for unconstrained (`-1`).
/// `numeric(p,s)` packs `((p << 16) | s) + VARHDRSZ`; subtract `VARHDRSZ` (4) before unpacking.
pub fn numeric_precision_scale(atttypmod: i32) -> Option<(u8, i8)> {
    if atttypmod < 4 {
        return None; // -1 (unconstrained) or an invalid value
    }
    let packed = (atttypmod - 4) as u32;
    let precision = ((packed >> 16) & 0xFFFF) as u8;
    let scale = (packed & 0xFFFF) as i8;
    Some((precision, scale))
}

#[cfg(test)]
mod tests {
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
        // point (600) is a geometric type, deferred to PR 2.14 — still NotTier1 here.
        let err = emit_fields(&col("p", 600, -1)).unwrap_err();
        assert!(matches!(err, Error::NotTier1 { oid: 600, .. }));
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
}
