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
#[path = "type_descriptor_test.rs"]
mod tests;
