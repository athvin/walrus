//! The [`Lsn`] newtype: a Postgres Log Sequence Number as a single `u64`.
//!
//! Every watermark, manifest bound, and provenance field in walrus is an LSN. Postgres shows them
//! two ways — the human `X/Y` form (`0/199BAC8`) and, in walrus's own JSON / control tables, a
//! **zero-padded 16-hex** string (`00000000019A2B3C`) chosen precisely so a *text* sort equals a
//! *numeric* sort. This type parses both forms, prints the padded form, orders numerically, and
//! serialises as the padded string — the ordering contract the whole `(commit_lsn, lsn)` pipeline
//! relies on.

use std::fmt;
use std::str::FromStr;

/// A Postgres Log Sequence Number as a single `u64`.
///
/// Canonical text form is **uppercase, zero-padded 16-hex** ([`Display`](fmt::Display)), chosen so
/// lexical order equals numeric order. `Ord` derives from the inner `u64`, so it *is* numeric
/// order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

impl Lsn {
    /// The zero LSN — orders below every nonzero LSN.
    pub const ZERO: Lsn = Lsn(0);

    /// Wrap a raw `u64` WAL position.
    pub const fn new(raw: u64) -> Self {
        Lsn(raw)
    }

    /// The raw `u64` WAL position.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Failure to parse either the `X/Y` or the 16-hex form of an [`Lsn`].
#[derive(Debug, thiserror::Error)]
#[error("invalid LSN {input:?}: {reason}")]
pub struct LsnParseError {
    pub input: String,
    pub reason: &'static str,
}

/// Parse one hexadecimal half of the `X/Y` form (each half fits a `u32`; empty / non-hex / a `+`
/// sign / an over-wide half all reject).
fn parse_hex_u32(part: &str) -> Option<u32> {
    if part.is_empty() || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u32::from_str_radix(part, 16).ok()
}

/// Parse the bare-hex form: 1–16 significant hex digits (leading zeros allowed), rejecting
/// non-hex, empty, a sign, and anything wider than `u64`.
fn parse_hex_u64(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    // More than 16 significant hex digits cannot fit a u64 — a caller bug, not a truncation.
    if s.trim_start_matches('0').len() > 16 {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

impl FromStr for Lsn {
    type Err = LsnParseError;

    /// Accepts `"0/199BAC8"` (two hex halves, `(high << 32) | low`) and `"00000000019A2B3C"`
    /// (bare 1–16 hex, with or without leading zeros).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let reject = |reason: &'static str| LsnParseError {
            input: s.to_string(),
            reason,
        };
        if let Some((hi, lo)) = s.split_once('/') {
            let high =
                parse_hex_u32(hi).ok_or_else(|| reject("X/Y half is not a valid hex u32"))?;
            let low = parse_hex_u32(lo).ok_or_else(|| reject("X/Y half is not a valid hex u32"))?;
            Ok(Lsn(((high as u64) << 32) | (low as u64)))
        } else {
            parse_hex_u64(s)
                .map(Lsn)
                .ok_or_else(|| reject("not a 1–16 digit hex value"))
        }
    }
}

impl fmt::Display for Lsn {
    /// Always 16 uppercase hex digits, zero-padded — e.g. `00000000019A2B3C`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016X}", self.0)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lsn({:016X})", self.0)
    }
}

impl serde::Serialize for Lsn {
    /// Emit the canonical padded string (never a bare JSON number) so the on-disk form sorts as
    /// text exactly as it sorts numerically.
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Lsn {
    /// Read a string in either accepted dialect via [`FromStr`].
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s: String = serde::Deserialize::deserialize(d)?;
        s.parse::<Lsn>().map_err(serde::de::Error::custom)
    }
}

/// Postgres `pg_lsn` support (feature `sqlx`), delegating to sqlx's `PgLsn` so an `Lsn` binds and
/// decodes as a native `pg_lsn` — which sorts as a WAL position, matching this newtype's ordering.
#[cfg(feature = "sqlx")]
mod sqlx_support {
    use super::Lsn;
    use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
    use sqlx::{Decode, Encode, Postgres, Type, TypeInfo};

    impl Type<Postgres> for Lsn {
        fn type_info() -> PgTypeInfo {
            // sqlx 0.8 has no built-in pg_lsn (OID 3220); resolve the type by name.
            PgTypeInfo::with_name("pg_lsn")
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            // Match by name so a catalog-resolved pg_lsn column (fetched by OID) is accepted the
            // same as our `with_name` declaration — the default PartialEq would reject that.
            ty.name().eq_ignore_ascii_case("pg_lsn")
        }
    }

    impl<'q> Encode<'q, Postgres> for Lsn {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            // pg_lsn's binary wire format is an 8-byte big-endian integer — identical to int8, so
            // reuse i64's encoder. `as i64` preserves the bit pattern.
            <i64 as Encode<Postgres>>::encode_by_ref(&(self.as_u64() as i64), buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for Lsn {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            let raw = <i64 as Decode<Postgres>>::decode(value)?;
            Ok(Lsn::new(raw as u64))
        }
    }
}

#[cfg(test)]
#[path = "lsn_test.rs"]
mod tests;
