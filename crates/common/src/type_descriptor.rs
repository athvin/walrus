//! `TypeDescriptor` — the per-column type-mapping descriptor (walrus-pg-sink.md §2.6).
//!
//! Part of the same decoupling seam as [`crate::pg_shape`]: the sink writes one descriptor per
//! source column into `schema_registry` (keyed by `schema_version`); the loader reads it back to
//! recreate enum types / bit lengths / char lengths / interval structs and `CAST` the carried
//! columns into place. That is what makes "reconcile to the exact source shape" a **mechanical**
//! operation rather than a guess.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The three-tier mapping model (walrus-pg-sink.md §2.2). Serializes as the **integer** `1 | 2 | 3`
/// to match the `"tier": 2` form in the §2.6 descriptor JSON (not the string `"2"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Native 1:1 (Parquet-native logical type survives unchanged).
    One,
    /// Structural decomposition (one source column → several emitted columns / a nested type).
    Two,
    /// Canonical-text carrier (carried as VARCHAR, cast + metadata re-applied on load).
    Three,
}

impl Tier {
    fn as_int(self) -> u8 {
        match self {
            Tier::One => 1,
            Tier::Two => 2,
            Tier::Three => 3,
        }
    }
}

impl Serialize for Tier {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_u8(self.as_int())
    }
}

impl<'de> Deserialize<'de> for Tier {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        match u8::deserialize(d)? {
            1 => Ok(Tier::One),
            2 => Ok(Tier::Two),
            3 => Ok(Tier::Three),
            other => Err(serde::de::Error::custom(format!(
                "invalid tier {other}, expected 1, 2, or 3"
            ))),
        }
    }
}

/// Metadata that Parquet/DuckDB lose on read; the loader re-applies it (§2.6). Each field is
/// `None` unless the column's type needs it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeMeta {
    /// Ordered label set for an `enum`.
    pub enum_labels: Option<Vec<String>>,
    /// `n` for `bit(n)` / `varbit(n)`.
    pub bit_length: Option<u32>,
    /// `n` (+ bpchar padding) for `char(n)` / `varchar(n)`.
    pub char_length: Option<u32>,
    /// `lc_monetary` fractional digits for `money`.
    pub money_fraction_digits: Option<u32>,
}

/// Per-column mapping descriptor written to `schema_registry` (§2.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeDescriptor {
    pub column: String,
    pub pg_type_oid: u32,
    pub pg_type: String,
    pub tier: Tier,
    /// How the value is shaped in Arrow, e.g. `"Struct/Decomposed"`.
    pub arrow: String,
    /// The DuckDB target type, e.g. `"INTERVAL"`.
    pub duckdb: String,
    /// The flat columns this type expands to, e.g. `["duration_months:INT32", …]`.
    pub emit: Vec<String>,
    /// The loader-side recombine expression; `None` for a tier-1 scalar that needs none.
    pub recombine: Option<String>,
    pub meta: TypeMeta,
}

#[cfg(test)]
mod tests {
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
        let v: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&d).unwrap()).unwrap();
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
}
