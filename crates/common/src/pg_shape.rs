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
        if self.type_oid != crate::oids::NUMERIC || self.type_modifier < 0 {
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
#[path = "pg_shape_test.rs"]
mod tests;
