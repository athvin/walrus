//! Build the per-column [`TypeDescriptor`] (walrus-pg-sink.md §2.6).
//!
//! This closes the loop on Phase 2b: every tier decision from PRs 2.9–2.16 becomes one serializable
//! record — tier, emitted columns, recombine expression, and the metadata (`enum_labels`,
//! `bit_length`, `char_length`) the loader must re-apply. It is what turns "reconcile to the exact
//! source shape" from a guess into a mechanical operation.
//!
//! **Single source of truth.** `emit_of` is derived by *calling* [`schema::emit_fields`] — the same
//! dispatch the schema/batch builders use — so the descriptor's `emit[]` can never drift from the
//! Parquet the sink actually writes. `tier_of` mirrors that dispatch's precedence branch-for-branch.

use crate::range::RangeFamily;
use crate::{geometric, oids, schema, tier3, uuid_enum};
use arrow::datatypes::DataType;
use common::{PgColumn, PgRelation, Tier, TypeDescriptor, TypeMeta};

/// Derive the per-column mapping descriptor (§2.6). Enum `enum_labels` are caller-supplied (the sink
/// hydrates them from the catalog in PR 2.22); use [`describe_column_with_labels`] to pass them.
pub fn describe_column(col: &PgColumn) -> TypeDescriptor {
    describe_column_with_labels(col, None)
}

/// [`describe_column`] plus the ordered enum label set (only used when the column is an enum).
pub fn describe_column_with_labels(
    col: &PgColumn,
    enum_labels: Option<Vec<String>>,
) -> TypeDescriptor {
    let tier = tier_of(col);
    TypeDescriptor {
        column: col.name.clone(),
        pg_type_oid: col.type_oid,
        pg_type: pg_type_name(col.type_oid).to_string(),
        tier,
        arrow: arrow_of(col, tier),
        duckdb: duckdb_of(col, tier),
        emit: emit_of(col),
        recombine: recombine_of(col),
        meta: meta_of(col, enum_labels),
    }
}

/// Descriptors for every column of a relation, in column order.
pub fn describe_relation(rel: &PgRelation) -> Vec<TypeDescriptor> {
    rel.columns.iter().map(describe_column).collect()
}

/// Tier for a column, mirroring [`schema::emit_fields`]'s dispatch precedence branch-for-branch.
fn tier_of(col: &PgColumn) -> Tier {
    if schema::tier1_data_type(col.type_oid, col.type_modifier).is_some() {
        return Tier::One;
    }
    if tier3::is_tier3_text(col.type_oid, col.type_modifier) {
        return Tier::Three;
    }
    if col.type_oid == oids::INTERVAL || col.type_oid == oids::TIMETZ {
        return Tier::Two;
    }
    if RangeFamily::from_range_oid(col.type_oid).is_some()
        || RangeFamily::from_multirange_oid(col.type_oid).is_some()
        || geometric::geo_kind(col.type_oid).is_some()
    {
        return Tier::Two;
    }
    if col.type_oid == oids::UUID {
        return Tier::One; // native DuckDB UUID is 1:1 (§2.4 uuid)
    }
    // enum (non-builtin OID) — and any unsupported column, which `describe_*` is not called for.
    Tier::Three
}

/// The emitted `name:ARROW_TYPE` list, taken **directly** from [`schema::emit_fields`] so it lists the
/// sibling columns in exactly the order (and with the names) the sink writes them. The loader
/// positional-binds against this list.
fn emit_of(col: &PgColumn) -> Vec<String> {
    schema::emit_fields(col)
        .map(|fields| {
            fields
                .iter()
                .map(|f| format!("{}:{}", f.name(), arrow_emit_name(f.data_type())))
                .collect()
        })
        .unwrap_or_default()
}

/// The loader-side recombine expression (a hint the loader phase finalizes). Only the types that
/// collapse to a *single* DuckDB scalar carry one — `interval`/`timetz`. `range` stays flat sibling
/// columns, `multirange`/geometric stay nested, Tier-1/Tier-3 need none.
fn recombine_of(col: &PgColumn) -> Option<String> {
    match col.type_oid {
        oids::INTERVAL => Some("to_months(m)+to_days(d)+to_microseconds(us)".to_string()),
        oids::TIMETZ => Some("make_timetz(us, off)".to_string()),
        _ => None,
    }
}

/// Metadata Parquet/DuckDB lose on read, re-applied by the loader (§2.6). Each field populated only
/// when it applies; `money_fraction_digits` is deferred (always `None` here).
fn meta_of(col: &PgColumn, enum_labels: Option<Vec<String>>) -> TypeMeta {
    let mut meta = TypeMeta::default();
    if uuid_enum::is_enum_oid(col.type_oid) {
        meta.enum_labels = enum_labels;
    }
    // bit(n)/varbit(n): atttypmod IS the bit count (no VARHDRSZ header).
    if (col.type_oid == oids::BIT || col.type_oid == oids::VARBIT) && col.type_modifier >= 0 {
        meta.bit_length = Some(col.type_modifier as u32);
    }
    // char(n)/varchar(n): atttypmod is n + VARHDRSZ (4).
    if (col.type_oid == oids::BPCHAR || col.type_oid == oids::VARCHAR) && col.type_modifier >= 4 {
        meta.char_length = Some((col.type_modifier - 4) as u32);
    }
    meta
}

/// The `arrow` descriptor string: Tier-2 is a decomposition; Tier-1/Tier-3 report the single Arrow type.
fn arrow_of(col: &PgColumn, tier: Tier) -> String {
    match tier {
        Tier::Two => "Struct/Decomposed".to_string(),
        _ => first_field_type(col)
            .map(|dt| format!("{dt:?}"))
            .unwrap_or_else(|| "Utf8".to_string()),
    }
}

/// The `duckdb` target type string.
fn duckdb_of(col: &PgColumn, tier: Tier) -> String {
    match tier {
        Tier::Two => match col.type_oid {
            oids::INTERVAL => "INTERVAL".to_string(),
            oids::TIMETZ => "TIMETZ".to_string(),
            _ if RangeFamily::from_range_oid(col.type_oid).is_some() => {
                "(flat sibling columns)".to_string()
            }
            _ if RangeFamily::from_multirange_oid(col.type_oid).is_some() => {
                "LIST(STRUCT)".to_string()
            }
            _ => "STRUCT".to_string(), // geometric
        },
        _ => first_field_type(col)
            .map(|dt| duckdb_scalar_name(&dt).to_string())
            .unwrap_or_else(|| "VARCHAR".to_string()),
    }
}

fn first_field_type(col: &PgColumn) -> Option<DataType> {
    schema::emit_fields(col)
        .ok()
        .and_then(|fields| fields.first().map(|f| f.data_type().clone()))
}

/// Parquet-ish emit-suffix name (matches the §2.6 interval example `INT32`/`INT64`).
fn arrow_emit_name(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int16 => "INT16".to_string(),
        DataType::Int32 => "INT32".to_string(),
        DataType::Int64 => "INT64".to_string(),
        DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        DataType::Utf8 => "VARCHAR".to_string(),
        DataType::Binary => "BLOB".to_string(),
        DataType::FixedSizeBinary(n) => format!("FIXEDBINARY({n})"),
        DataType::Date32 => "DATE".to_string(),
        DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Struct(_) => "STRUCT".to_string(),
        DataType::List(_) => "LIST".to_string(),
        other => format!("{other:?}"),
    }
}

/// DuckDB scalar type name for a Tier-1/Tier-3 single-field column.
fn duckdb_scalar_name(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Decimal128(..) => "DECIMAL",
        DataType::Utf8 => "VARCHAR",
        DataType::Binary => "BLOB",
        DataType::FixedSizeBinary(_) => "UUID",
        DataType::Date32 => "DATE",
        DataType::Time64(_) => "TIME",
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE",
        DataType::Timestamp(_, None) => "TIMESTAMP",
        _ => "VARCHAR",
    }
}

/// The Postgres type name for an OID (informational descriptor field). Non-builtin → `enum`.
fn pg_type_name(oid: u32) -> &'static str {
    match oid {
        oids::BOOL => "bool",
        oids::BYTEA => "bytea",
        oids::CHAR => "char",
        oids::INT8 => "int8",
        oids::INT2 => "int2",
        oids::INT4 => "int4",
        oids::TEXT => "text",
        oids::JSON => "json",
        oids::FLOAT4 => "float4",
        oids::FLOAT8 => "float8",
        oids::BPCHAR => "bpchar",
        oids::VARCHAR => "varchar",
        oids::DATE => "date",
        oids::TIME => "time",
        oids::TIMESTAMP => "timestamp",
        oids::TIMESTAMPTZ => "timestamptz",
        oids::NUMERIC => "numeric",
        oids::JSONB => "jsonb",
        oids::INTERVAL => "interval",
        oids::TIMETZ => "timetz",
        oids::INT4RANGE => "int4range",
        oids::NUMRANGE => "numrange",
        oids::TSRANGE => "tsrange",
        oids::TSTZRANGE => "tstzrange",
        oids::DATERANGE => "daterange",
        oids::INT8RANGE => "int8range",
        oids::INT4MULTIRANGE => "int4multirange",
        oids::NUMMULTIRANGE => "nummultirange",
        oids::TSMULTIRANGE => "tsmultirange",
        oids::TSTZMULTIRANGE => "tstzmultirange",
        oids::DATEMULTIRANGE => "datemultirange",
        oids::INT8MULTIRANGE => "int8multirange",
        oids::POINT => "point",
        oids::LSEG => "lseg",
        oids::PATH => "path",
        oids::BOX => "box",
        oids::POLYGON => "polygon",
        oids::LINE => "line",
        oids::CIRCLE => "circle",
        oids::XML => "xml",
        oids::XID => "xid",
        oids::CIDR => "cidr",
        oids::MACADDR8 => "macaddr8",
        oids::MACADDR => "macaddr",
        oids::INET => "inet",
        oids::BIT => "bit",
        oids::VARBIT => "varbit",
        oids::TXID_SNAPSHOT => "txid_snapshot",
        oids::PG_LSN => "pg_lsn",
        oids::TSVECTOR => "tsvector",
        oids::TSQUERY => "tsquery",
        oids::XID8 => "xid8",
        oids::UUID => "uuid",
        o if o >= oids::FIRST_NORMAL_OID => "enum",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, oid: u32, typmod: i32) -> PgColumn {
        PgColumn {
            name: name.to_string(),
            type_oid: oid,
            type_modifier: typmod,
            is_key: false,
        }
    }

    #[test]
    fn interval_descriptor_has_three_emits_and_recombine() {
        let d = describe_column(&col("duration", oids::INTERVAL, -1));
        assert_eq!(d.tier, Tier::Two);
        assert_eq!(
            d.emit,
            vec![
                "duration_months:INT32",
                "duration_days:INT32",
                "duration_micros:INT64",
            ]
        );
        assert_eq!(
            d.recombine.as_deref(),
            Some("to_months(m)+to_days(d)+to_microseconds(us)")
        );
        assert_eq!(d.duckdb, "INTERVAL");
        assert_eq!(d.pg_type, "interval");
    }

    #[test]
    fn enum_descriptor_carries_ordered_labels() {
        let labels = vec!["happy".to_string(), "meh".to_string(), "sad".to_string()];
        let d = describe_column_with_labels(&col("mood", 16400, -1), Some(labels.clone()));
        assert_eq!(d.tier, Tier::Three);
        assert_eq!(d.emit, vec!["mood:VARCHAR"]);
        assert_eq!(d.duckdb, "VARCHAR");
        assert_eq!(d.pg_type, "enum");
        assert_eq!(d.meta.enum_labels, Some(labels));
        // The ordered set is the only populated meta field.
        assert_eq!(d.meta.bit_length, None);
        assert_eq!(d.meta.char_length, None);
    }

    #[test]
    fn char_n_descriptor_carries_char_length() {
        // char(5) → bpchar, typmod = 5 + VARHDRSZ(4) = 9.
        let d = describe_column(&col("code", oids::BPCHAR, 9));
        assert_eq!(d.tier, Tier::One);
        assert_eq!(d.meta.char_length, Some(5));
        assert_eq!(d.emit, vec!["code:VARCHAR"]);
    }

    #[test]
    fn bit_descriptor_carries_bit_length() {
        // bit(8): atttypmod is the raw bit count (no VARHDRSZ), and it's a Tier-3 VARCHAR carrier.
        let d = describe_column(&col("flags", oids::BIT, 8));
        assert_eq!(d.tier, Tier::Three);
        assert_eq!(d.meta.bit_length, Some(8));
    }

    #[test]
    fn tier_matches_emit_fields_for_every_supported_oid() {
        // Representative (oid, typmod) across all tiers/shapes. For each, the descriptor's emit[] must
        // equal emit_fields verbatim (they share one dispatch), and the tier must be sensible.
        let samples: &[(u32, i32)] = &[
            (oids::BOOL, -1),
            (oids::INT4, -1),
            (oids::NUMERIC, 655366), // numeric(10,2) → Tier-1
            (oids::NUMERIC, -1),     // unconstrained → Tier-3
            (oids::BPCHAR, 9),       // char(5) → Tier-1
            (oids::TIMESTAMPTZ, -1),
            (oids::INTERVAL, -1),
            (oids::TIMETZ, -1),
            (oids::INT4RANGE, -1),
            (oids::INT4MULTIRANGE, -1),
            (oids::POINT, -1),
            (oids::PATH, -1),
            (oids::BIT, 8),
            (oids::INET, -1),
            (oids::UUID, -1),
            (16400, -1), // enum
        ];
        for &(oid, typmod) in samples {
            let c = col("c", oid, typmod);
            let expected: Vec<String> = schema::emit_fields(&c)
                .unwrap()
                .iter()
                .map(|f| format!("{}:{}", f.name(), arrow_emit_name(f.data_type())))
                .collect();
            let d = describe_column(&c);
            assert_eq!(d.emit, expected, "emit drift for oid {oid}");
            assert!(
                !d.emit.is_empty(),
                "supported oid {oid} must emit ≥1 column"
            );
            assert!(matches!(d.tier, Tier::One | Tier::Two | Tier::Three));
        }
    }

    #[test]
    fn descriptor_json_roundtrips() {
        let d = describe_column(&col("duration", oids::INTERVAL, -1));
        let json = serde_json::to_string(&d).unwrap();
        let back: TypeDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(back, d);
        // tier is the integer 2 in the §2.6 JSON shape.
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["tier"], serde_json::json!(2));
    }
}
