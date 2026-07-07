//! The neutral Postgres shape types — the decoupling seam between the decoder and everything
//! downstream.
//!
//! These plain value types live in `common` on purpose: the pgoutput decoder (in `pg-sink`)
//! **produces** them, `pg-to-arrow` **consumes** them, `control` **persists** the descriptor, and
//! `loader` **reads it back** to rebuild types. That one decision is why `pg-to-arrow` is fully
//! unit-testable without the decoder, and why no crate ever has to depend on a binary.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::Error;

/// Postgres OID of the `numeric` type — `numeric_precision_scale` only decodes for this type.
const NUMERIC_OID: u32 = 1700;

/// Postgres `relreplident` — governs which old-image columns Update/Delete carry (proto §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    /// `'d'` — the default: Update/Delete carry key columns only (`'K'`).
    Default,
    /// `'n'` — nothing: Update/Delete carry no old image (unusable as a key source).
    Nothing,
    /// `'f'` — full: Update/Delete carry the whole old row (`'O'`).
    Full,
    /// `'i'` — a nominated unique index supplies the identity.
    Index,
}

impl ReplicaIdentity {
    /// Parse the Relation message's `relreplident` byte; error on any other value.
    pub fn from_wire(c: u8) -> Result<Self, Error> {
        match c {
            b'd' => Ok(ReplicaIdentity::Default),
            b'n' => Ok(ReplicaIdentity::Nothing),
            b'f' => Ok(ReplicaIdentity::Full),
            b'i' => Ok(ReplicaIdentity::Index),
            other => Err(Error::Internal(format!(
                "unknown relreplident byte {:?}",
                other as char
            ))),
        }
    }
}

/// One column of a relation, as seen in a Relation `'R'` message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgColumn {
    pub name: String,
    pub type_oid: u32,
    /// `atttypmod`; `-1` = no modifier. For `numeric` it packs `(precision, scale)`.
    pub type_modifier: i32,
    /// The Relation flags bit 1 — this column is part of the replica-identity key.
    pub is_key: bool,
}

impl PgColumn {
    /// Decode `numeric(p, s)` from `type_modifier` when this column is a `numeric`; `None` for a
    /// non-numeric column or an unconstrained `numeric` (`type_modifier == -1`).
    ///
    /// The packing (the exact math PR 2.3 relies on): `precision = ((mod - 4) >> 16) & 0xFFFF`,
    /// `scale = (mod - 4) & 0xFFFF`.
    pub fn numeric_precision_scale(&self) -> Option<(u16, u16)> {
        if self.type_oid != NUMERIC_OID || self.type_modifier < 0 {
            return None;
        }
        let packed = (self.type_modifier as u32).wrapping_sub(4);
        let precision = ((packed >> 16) & 0xFFFF) as u16;
        let scale = (packed & 0xFFFF) as u16;
        Some((precision, scale))
    }
}

/// The shape of a source table at one `schema_version`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgRelation {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<PgColumn>,
}

impl PgRelation {
    /// The key-column names (`is_key`) **in relation order** — the loader's MERGE/dedup key list.
    /// Order matters for composite PKs, so this preserves column order rather than sorting.
    pub fn key_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect()
    }
}

/// One column value inside a TupleData (proto §5).
///
/// **`Null` (`'n'`) and `UnchangedToast` (`'u'`) are DISTINCT** — a whole loader-correctness story
/// (PR 3.6) depends on the difference surviving from wire to `<table>_raw`, where the loader
/// resolves an unchanged-TOAST placeholder by back-scanning. It must never be collapsed to `Null`.
///
/// Not `Serialize`: this is an in-memory wire value, not a persisted document with a stable JSON
/// contract. serde is added only if a later PR needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    /// `'n'` — a real SQL NULL.
    Null,
    /// `'u'` — an unchanged out-of-line TOAST value, absent from the wire.
    UnchangedToast,
    /// `'t'` — the textual representation of the value.
    Text(String),
    /// `'b'` — the binary representation (zero-copy via `bytes::Bytes`).
    Binary(Bytes),
}

#[cfg(test)]
mod tests {
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
}
