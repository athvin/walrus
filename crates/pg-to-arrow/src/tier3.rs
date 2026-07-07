//! Tier-3 **canonical-text carriers** (walrus-pg-sink.md §2.5).
//!
//! Some Postgres types have no lossless structural target in Parquet/DuckDB, so we carry their
//! canonical text verbatim in one `VARCHAR` column — always exact as a string — and defer the lost
//! *type metadata* (bit length, enum labels, …) to the descriptor (PR 2.17). The value is carried
//! **verbatim**: the wire text is already canonical, and re-formatting risks a lossy round-trip.
//!
//! The headline case is `numeric`: `p ≤ 38` stays a Tier-1 `Decimal128` (PR 2.9), but **unconstrained
//! or `p > 38` must become `VARCHAR`** — DuckDB's `DECIMAL` caps at precision 38 and its Parquet
//! reader downcasts any decimal with precision > 38 to `DOUBLE` (verified in `parquet_reader.cpp`), so
//! a `Decimal256` would silently lose digits. `VARCHAR` is exact.

use crate::oids;
use arrow::datatypes::{DataType, Field};

/// True if this OID+typmod is carried as canonical `VARCHAR`.
///
/// **NOTE:** `numeric` with `p ≤ 38` is *not* Tier-3 — it stays Tier-1 `Decimal128`. Only unconstrained
/// (`numeric_precision_scale` → `None`) or `p > 38` numeric is Tier-3; `p == 38` is the Tier-1 boundary.
pub fn is_tier3_text(type_oid: u32, atttypmod: i32) -> bool {
    match type_oid {
        oids::NUMERIC => matches!(
            crate::schema::numeric_precision_scale(atttypmod),
            None | Some((39..=255, _))
        ),
        oids::BIT
        | oids::VARBIT
        | oids::INET
        | oids::CIDR
        | oids::MACADDR
        | oids::MACADDR8
        | oids::TSVECTOR
        | oids::TSQUERY
        | oids::PG_LSN
        | oids::XID
        | oids::XID8
        | oids::TXID_SNAPSHOT
        | oids::XML => true,
        _ => false,
    }
}

/// A single nullable `Utf8` field carrying the canonical text.
pub fn tier3_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::numeric_precision_scale;

    /// numeric(p,s) typmod: `((p << 16) | s) + VARHDRSZ` (VARHDRSZ = 4).
    fn numeric_typmod(p: i32, s: i32) -> i32 {
        ((p << 16) | s) + 4
    }

    #[test]
    fn numeric_p_le_38_is_not_tier3() {
        // numeric(10,2) and the p==38 boundary both stay Tier-1 Decimal128 (PR 2.9).
        assert!(!is_tier3_text(oids::NUMERIC, numeric_typmod(10, 2)));
        assert!(!is_tier3_text(oids::NUMERIC, numeric_typmod(38, 0)));
        assert_eq!(
            numeric_precision_scale(numeric_typmod(38, 0)),
            Some((38, 0))
        );
    }

    #[test]
    fn numeric_unconstrained_is_tier3_varchar() {
        assert!(is_tier3_text(oids::NUMERIC, -1));
        assert_eq!(tier3_field("amount").data_type(), &DataType::Utf8);
    }

    #[test]
    fn numeric_p_gt_38_is_tier3_varchar() {
        // p == 39 crosses the boundary; numeric(40,10) is well past it.
        assert!(is_tier3_text(oids::NUMERIC, numeric_typmod(39, 0)));
        assert!(is_tier3_text(oids::NUMERIC, numeric_typmod(40, 10)));
    }

    #[test]
    fn bit_and_inet_and_pglsn_are_varchar() {
        for oid in [
            oids::BIT,
            oids::VARBIT,
            oids::INET,
            oids::CIDR,
            oids::MACADDR,
            oids::MACADDR8,
            oids::TSVECTOR,
            oids::TSQUERY,
            oids::PG_LSN,
            oids::XID,
            oids::XID8,
            oids::TXID_SNAPSHOT,
            oids::XML,
        ] {
            assert!(is_tier3_text(oid, -1), "oid {oid} should be Tier-3 VARCHAR");
        }
        assert_eq!(tier3_field("x").data_type(), &DataType::Utf8);
        // uuid (2950) is deferred to PR 2.16 — NOT Tier-3 here.
        assert!(!is_tier3_text(2950, -1));
    }
}
