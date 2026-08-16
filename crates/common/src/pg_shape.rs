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
///
/// The pgoutput byte parsed below and this enum's serde string are distinct wire forms. The serde
/// form is a persisted control-plane contract inside `schema_registry.columns`. Rows written
/// before the lowercase form was introduced contain PascalCase names, so the per-variant aliases
/// are permanent compatibility for those historical rows; no data migration is needed.
///
/// Roll out readers before writers: deploy `walrus-loader` before `walrus-pg-sink`. The upgraded
/// reader accepts both spellings, while an old reader cannot parse newly written lowercase names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplicaIdentity {
    /// `'d'` — the default: Update/Delete carry key columns only (`'K'`).
    #[serde(alias = "Default")]
    Default,
    /// `'n'` — nothing: Update/Delete carry no old image (unusable as a key source).
    #[serde(alias = "Nothing")]
    Nothing,
    /// `'f'` — full: Update/Delete carry the whole old row (`'O'`).
    #[serde(alias = "Full")]
    Full,
    /// `'i'` — a nominated unique index supplies the identity.
    #[serde(alias = "Index")]
    Index,
}

impl TryFrom<u8> for ReplicaIdentity {
    type Error = crate::Error;

    /// Parse the Relation message's `relreplident` byte; error on any other value.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if `c` is not one of `b'd'`, `b'n'`, `b'f'`, or `b'i'`, the
    /// four bytes PostgreSQL emits for `pg_class.relreplident`. This protocol mismatch is terminal.
    fn try_from(c: u8) -> Result<Self, Self::Error> {
        match c {
            b'd' => Ok(Self::Default),
            b'n' => Ok(Self::Nothing),
            b'f' => Ok(Self::Full),
            b'i' => Ok(Self::Index),
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

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<PgColumn>() == 40,
    "PgColumn is stored once per source column"
);

impl PgColumn {
    /// Decode `numeric(p, s)` from `type_modifier` when this column is a `numeric`; `None` for a
    /// non-numeric column or an unconstrained `numeric` (`type_modifier == -1`).
    ///
    /// The packing (the exact math PR 2.3 relies on): `precision = ((mod - 4) >> 16) & 0xFFFF`,
    /// `scale = (mod - 4) & 0xFFFF`.
    #[must_use]
    pub fn numeric_precision_scale(&self) -> Option<(u16, u16)> {
        if self.type_oid != crate::oids::NUMERIC || self.type_modifier < 4 {
            return None;
        }
        let packed = u32::try_from(self.type_modifier - 4).ok()?;
        let precision = u16::try_from((packed >> 16) & 0xFFFF).ok()?;
        let scale = u16::try_from(packed & 0xFFFF).ok()?;
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

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<PgRelation>() == 80,
    "PgRelation is cached for every source table"
);

impl PgRelation {
    /// The key-column names (`is_key`) **in relation order** — the loader's MERGE/dedup key list.
    /// Order matters for composite PKs, so this preserves column order rather than sorting.
    #[must_use]
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

/// Move-cost budget for the per-column decode hot path (`own-move-large`).
///
/// Measured with `size_of::<TupleValue>()` on PR 9.7. If this trips, shrink the type, box the
/// offending variant in Phase 11, or raise the measured budget deliberately in review.
const TUPLE_VALUE_MAX_BYTES: usize = 40;
const _: () = assert!(size_of::<TupleValue>() <= TUPLE_VALUE_MAX_BYTES);

#[cfg(test)]
#[path = "pg_shape_test.rs"]
mod tests;
