//! `range` / `multirange` decomposition (walrus-pg-sink.md §2.4).
//!
//! DuckDB has no range type, so a Postgres range can never be a single 1:1 mirror column. A range is
//! **losslessly reconstructable** from exactly five values — `lower`, `upper`, `lower_inc`,
//! `upper_inc`, `isempty` — so the sink emits those as five flat sibling columns; a multirange (an
//! ordered set of non-empty, non-null members) becomes a `LIST<STRUCT>`.
//!
//! This module owns two things: the **family → element-type** dispatch (`int4range → INT32`,
//! `tstzrange → TIMESTAMPTZ`, …) and the **wire-literal parsers** (`[1,10)`, `empty`, `(,5]`,
//! `{[1,4),[7,9)}`). The three states NULL / `empty` / `unbounded` are kept strictly distinct.

use crate::error::Error;
use crate::oids;
use arrow::datatypes::{DataType, TimeUnit};

/// The six built-in range/multirange families. The element type — and, in Postgres, the
/// canonicalization — differ per family, but the wire form the sink parses is uniform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFamily {
    Int4,
    Int8,
    Num,
    Ts,
    TsTz,
    Date,
}

impl RangeFamily {
    pub fn from_range_oid(oid: u32) -> Option<Self> {
        Some(match oid {
            oids::INT4RANGE => Self::Int4,
            oids::INT8RANGE => Self::Int8,
            oids::NUMRANGE => Self::Num,
            oids::TSRANGE => Self::Ts,
            oids::TSTZRANGE => Self::TsTz,
            oids::DATERANGE => Self::Date,
            _ => return None,
        })
    }

    pub fn from_multirange_oid(oid: u32) -> Option<Self> {
        Some(match oid {
            oids::INT4MULTIRANGE => Self::Int4,
            oids::INT8MULTIRANGE => Self::Int8,
            oids::NUMMULTIRANGE => Self::Num,
            oids::TSMULTIRANGE => Self::Ts,
            oids::TSTZMULTIRANGE => Self::TsTz,
            oids::DATEMULTIRANGE => Self::Date,
            _ => return None,
        })
    }

    /// Arrow element type for `_lower`/`_upper`. Unconstrained `numrange` falls back to `Utf8` — the
    /// Tier-3 VARCHAR carrier wired here and *proven* in PR 2.15 (a range column carries no element
    /// typmod, so in practice `Num` is always `Utf8` today).
    pub fn elem_data_type(self, atttypmod: i32) -> DataType {
        match self {
            Self::Int4 => DataType::Int32,
            Self::Int8 => DataType::Int64,
            Self::Num => match crate::schema::numeric_precision_scale(atttypmod) {
                Some((p, s)) if (1..=38).contains(&p) => DataType::Decimal128(p, s),
                _ => DataType::Utf8,
            },
            Self::Ts => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::TsTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Date => DataType::Date32,
        }
    }
}

/// One parsed range. A `None` bound means *unbounded on that side* (unless `empty`). The three
/// states are distinct: `empty` (`isempty` true, both bounds `None`), an unbounded bound (`None` with
/// `empty=false`), and — at the column level — a whole SQL `NULL` (handled by the caller, not here).
/// `lower_inf`/`upper_inf` are **derivable** (`bound.is_none() && !empty`) and deliberately not stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRange {
    pub empty: bool,
    pub lower: Option<String>,
    pub upper: Option<String>,
    pub lower_inc: bool,
    pub upper_inc: bool,
}

fn range_err(text: &str) -> Error {
    Error::ValueParse {
        column: "range".to_string(),
        value: text.to_string(),
        data_type: "range".to_string(),
    }
}

/// Parse `[1,10)` / `empty` / `(,5]` / `[2024-01-01,)` into a `ParsedRange`. An inclusivity marker on
/// an unbounded side is forced to `false` (an infinite bound is never inclusive — matches Postgres'
/// `lower_inc`/`upper_inc`).
pub fn parse_range(text: &str) -> Result<ParsedRange, Error> {
    let t = text.trim();
    if t.eq_ignore_ascii_case("empty") {
        return Ok(ParsedRange {
            empty: true,
            lower: None,
            upper: None,
            lower_inc: false,
            upper_inc: false,
        });
    }
    let bytes = t.as_bytes();
    if bytes.len() < 3 {
        return Err(range_err(text));
    }
    let lower_inc = match bytes[0] {
        b'[' => true,
        b'(' => false,
        _ => return Err(range_err(text)),
    };
    let upper_inc = match bytes[bytes.len() - 1] {
        b']' => true,
        b')' => false,
        _ => return Err(range_err(text)),
    };
    let inner = &t[1..t.len() - 1];
    let (lo, hi) = split_top_level_comma(inner).ok_or_else(|| range_err(text))?;
    let lower = parse_bound(lo);
    let upper = parse_bound(hi);
    Ok(ParsedRange {
        empty: false,
        lower_inc: lower_inc && lower.is_some(),
        upper_inc: upper_inc && upper.is_some(),
        lower,
        upper,
    })
}

/// Parse `{[1,4),[7,9)}` (and `{}`) into member ranges. Members are non-empty and non-null (Postgres
/// guarantees this); an empty multirange yields an empty `Vec` — distinct from a NULL column.
pub fn parse_multirange(text: &str) -> Result<Vec<ParsedRange>, Error> {
    let t = text.trim();
    if !t.starts_with('{') || !t.ends_with('}') {
        return Err(range_err(text));
    }
    let inner = t[1..t.len() - 1].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    split_members(inner).into_iter().map(parse_range).collect()
}

/// One bound literal → its value: empty (unquoted) = unbounded (`None`); otherwise the raw text with
/// surrounding quotes stripped and `""`/`\x` un-escaped.
fn parse_bound(s: &str) -> Option<String> {
    if s.is_empty() {
        return None; // unbounded side
    }
    if s.starts_with('"') {
        return Some(unquote(s));
    }
    Some(s.to_string())
}

/// Strip the surrounding `"` and un-escape `""` → `"` and `\x` → `x` (Postgres' range/element quoting).
fn unquote(s: &str) -> String {
    let inner = &s[1..s.len().saturating_sub(1)];
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if chars.peek() == Some(&'"') => {
                out.push('"');
                chars.next();
            }
            '\\' => {
                if let Some(n) = chars.next() {
                    out.push(n);
                }
            }
            _ => out.push(c),
        }
    }
    out
}

/// Split `inner` at the single top-level comma (quote-aware), returning `(lower, upper)`.
fn split_top_level_comma(inner: &str) -> Option<(&str, &str)> {
    let b = inner.as_bytes();
    let mut i = 0;
    let mut in_quotes = false;
    while i < b.len() {
        match b[i] {
            b'"' => {
                if in_quotes && b.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'\\' if in_quotes => {
                i += 2;
                continue;
            }
            b',' if !in_quotes => return Some((&inner[..i], &inner[i + 1..])),
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split a multirange body into member-range literals at top-level commas (quote- and bracket-aware).
fn split_members(inner: &str) -> Vec<&str> {
    let b = inner.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth: i32 = 0;
    let mut in_quotes = false;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'"' => {
                if in_quotes && b.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'\\' if in_quotes => {
                i += 2;
                continue;
            }
            b'[' | b'(' if !in_quotes => depth += 1,
            b']' | b')' if !in_quotes => depth -= 1,
            b',' if !in_quotes && depth == 0 => {
                out.push(inner[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(inner[start..].trim());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn family_oid_dispatch_and_element_types() {
        assert_eq!(
            RangeFamily::from_range_oid(oids::INT4RANGE),
            Some(RangeFamily::Int4)
        );
        assert_eq!(
            RangeFamily::from_multirange_oid(oids::TSTZMULTIRANGE),
            Some(RangeFamily::TsTz)
        );
        assert_eq!(RangeFamily::from_range_oid(9999), None);
        assert_eq!(RangeFamily::Int8.elem_data_type(-1), DataType::Int64);
        assert_eq!(
            RangeFamily::TsTz.elem_data_type(-1),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        // A range column carries no element typmod → unconstrained numrange falls back to VARCHAR.
        assert_eq!(RangeFamily::Num.elem_data_type(-1), DataType::Utf8);
    }

    #[test]
    fn empty_sets_empty_true_and_bounds_null() {
        let r = parse_range("empty").unwrap();
        assert!(r.empty);
        assert_eq!(r.lower, None);
        assert_eq!(r.upper, None);
        assert!(!r.lower_inc && !r.upper_inc);
    }

    #[test]
    fn unbounded_lower_is_null_with_empty_false() {
        // Distinct from empty: a NULL lower bound but a present range.
        let r = parse_range("(,10)").unwrap();
        assert!(!r.empty);
        assert_eq!(r.lower, None);
        assert_eq!(r.upper, Some("10".to_string()));
        assert!(!r.lower_inc, "an unbounded bound is never inclusive");
    }

    #[test]
    fn discrete_int4range_canonicalizes_to_half_open() {
        // Postgres canonicalizes discrete ranges to `[)` before the wire; the parser reproduces it.
        let r = parse_range("[1,10)").unwrap();
        assert_eq!(r.lower, Some("1".to_string()));
        assert_eq!(r.upper, Some("10".to_string()));
        assert!(r.lower_inc);
        assert!(!r.upper_inc);
        assert!(!r.empty);
    }

    #[test]
    fn continuous_range_preserves_arbitrary_inclusivity() {
        let r = parse_range("(1.5,9.5]").unwrap();
        assert!(!r.lower_inc);
        assert!(r.upper_inc);
    }

    #[test]
    fn quoted_timestamp_bounds_are_unquoted() {
        let r = parse_range(r#"["2024-01-01 00:00:00","2024-01-02 00:00:00")"#).unwrap();
        assert_eq!(r.lower, Some("2024-01-01 00:00:00".to_string()));
        assert_eq!(r.upper, Some("2024-01-02 00:00:00".to_string()));
    }

    #[test]
    fn multirange_parses_members_and_empty_list() {
        let ms = parse_multirange("{[1,4),[7,9)}").unwrap();
        assert_eq!(ms.len(), 2);
        assert_eq!(ms[0].lower, Some("1".to_string()));
        assert_eq!(ms[0].upper, Some("4".to_string()));
        assert_eq!(ms[1].lower, Some("7".to_string()));
        assert_eq!(ms[1].upper, Some("9".to_string()));
        // Empty multirange → zero members (the caller keeps this distinct from a NULL column).
        assert_eq!(parse_multirange("{}").unwrap().len(), 0);
    }
}
