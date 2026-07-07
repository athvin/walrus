//! `uuid` (native DuckDB `UUID`) and `enum` (VARCHAR + ordered labels) — two types that each hinge on
//! one subtlety (walrus-pg-sink.md §2.4 uuid, §2.5 enum).
//!
//! **uuid.** DuckDB reads a native `UUID` back from Parquet *only* when arrow-rs annotates the
//! `FixedSizeBinary(16)` with the `arrow.uuid` **canonical extension** (`ARROW:extension:name`) — a
//! *plain* FSB(16) writes un-annotated and reads back as a 16-byte `BLOB`. So this mapping is guarded
//! by a CI `write → read_parquet → typeof == UUID` conformance test and a pinned arrow-rs (Cargo.toml),
//! with a `VARCHAR + CAST(x AS UUID)` fallback if a bump ever drops the annotation.
//!
//! **enum.** Values are lossless as `VARCHAR`; the **ordered label set** is lost on the wire and is
//! carried by the descriptor (PR 2.17), from which the loader recreates the DuckDB `ENUM`. Enum OIDs
//! are dynamic (≥ `FIRST_NORMAL_OID`), so we treat a non-builtin OID as `enum → VARCHAR` for now
//! ([`is_enum_oid`]); PR 2.22 resolves enum-ness from the source catalog / the decoder's `Type` message.

use crate::error::Error;
use crate::oids;
use arrow::datatypes::{DataType, Field};
use std::collections::HashMap;

/// The Arrow canonical-extension name that makes arrow-rs emit the Parquet UUID logical type.
pub const ARROW_UUID_EXTENSION: &str = "arrow.uuid";

/// `FixedSizeBinary(16)` carrying the `arrow.uuid` canonical extension → Parquet UUID → DuckDB `UUID`.
/// The extension metadata is the *only* thing that makes DuckDB see `UUID` rather than `BLOB`.
pub fn uuid_field(name: &str) -> Field {
    Field::new(name, DataType::FixedSizeBinary(16), true).with_metadata(HashMap::from([(
        "ARROW:extension:name".to_string(),
        ARROW_UUID_EXTENSION.to_string(),
    )]))
}

/// Fallback if a pinned arrow-rs release ever drops the UUID annotation on the normal column path:
/// carry the canonical text as `Utf8` and `CAST(x AS UUID)` on load.
pub fn uuid_as_varchar(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

/// `enum` → nullable `Utf8`; the ordered label set is carried by the descriptor (PR 2.17), not here.
pub fn enum_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

/// Interim enum detection: a non-builtin OID (≥ `FIRST_NORMAL_OID`) is treated as an enum carrier.
/// PR 2.22 replaces this with a catalog-derived marker (the decoder's `Type` message).
pub fn is_enum_oid(type_oid: u32) -> bool {
    type_oid >= oids::FIRST_NORMAL_OID
}

/// Parse canonical UUID text (`"550e8400-e29b-41d4-a716-446655440000"`) into 16 bytes. Rejects
/// malformed input with `ValueParse` (no silent zero-padding).
pub fn parse_uuid_bytes(text: &str) -> Result<[u8; 16], Error> {
    uuid::Uuid::parse_str(text)
        .map(|u| u.into_bytes())
        .map_err(|_| Error::ValueParse {
            column: "uuid".to_string(),
            value: text.to_string(),
            data_type: "uuid".to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_field_carries_arrow_uuid_extension() {
        let f = uuid_field("id");
        assert_eq!(f.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            f.metadata().get("ARROW:extension:name").map(String::as_str),
            Some(ARROW_UUID_EXTENSION)
        );
        assert!(f.is_nullable());
    }

    #[test]
    fn parse_uuid_bytes_roundtrips() {
        let text = "550e8400-e29b-41d4-a716-446655440000";
        let bytes = parse_uuid_bytes(text).unwrap();
        // Standard big-endian byte order: first byte is 0x55, last is 0x00.
        assert_eq!(bytes[0], 0x55);
        assert_eq!(bytes[15], 0x00);
        // Round-trips back to the same canonical text.
        assert_eq!(uuid::Uuid::from_bytes(bytes).to_string(), text);
    }

    #[test]
    fn parse_uuid_bytes_rejects_malformed() {
        assert!(parse_uuid_bytes("not-a-uuid").is_err());
        assert!(parse_uuid_bytes("550e8400").is_err());
    }

    #[test]
    fn enum_is_plain_utf8() {
        assert_eq!(enum_field("status").data_type(), &DataType::Utf8);
        // A user-defined OID looks like an enum; a builtin one does not.
        assert!(is_enum_oid(16400));
        assert!(!is_enum_oid(oids::UUID));
        assert!(!is_enum_oid(oids::TEXT));
    }
}
