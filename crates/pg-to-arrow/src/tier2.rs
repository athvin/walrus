//! Tier-2 **column-expansion** helpers: the source types that carry more than any single
//! Arrow/Parquet/DuckDB scalar can hold, so the sink emits *several* sibling columns the loader
//! recombines (walrus-pg-sink.md §2.4).
//!
//! This PR (2.12) lands the first two: `interval` → 3 signed ints, `timetz` → micros + offset.
//! The canonical-text parsers here turn Postgres' output form (IntervalStyle `postgres`) into those
//! integer fields; the *reverse* (loader-side `to_months + to_days + …` / TIMETZ rebuild) is the
//! loader's job (PR 3.x), not the sink's.

use crate::error::Error;
use crate::range::RangeFamily;
use arrow::datatypes::{DataType, Field, Fields};
use std::sync::Arc;

/// `<c>_months INT32`, `<c>_days INT32`, `<c>_micros INT64` — Postgres' *un-normalized* three-field
/// interval (`'1 mon'` ≠ `'30 days'` ≠ `'720 hours'`), which is byte-identical to DuckDB's own
/// three-field `INTERVAL` struct (§2.4).
///
/// **Never a join key / `PARTITION BY`:** DuckDB normalizes intervals for equality and ordering
/// (days→24h, months→30d), so two byte-different intervals can compare equal. The loader rebuilds
/// with `to_months + to_days + to_microseconds`; it must never key on these columns (§2.4 caveat).
/// The three fields share one logical NULL — all three NULL ⇔ the source value was NULL — so a real
/// zero interval `(0,0,0)` stays distinct from absence.
pub fn interval_fields(name: &str) -> Vec<Field> {
    vec![
        Field::new(format!("{name}_months"), DataType::Int32, true),
        Field::new(format!("{name}_days"), DataType::Int32, true),
        Field::new(format!("{name}_micros"), DataType::Int64, true),
    ]
}

/// `<c>_micros BIGINT` (µs since midnight) + `<c>_offset_seconds INTEGER` (signed UTC offset).
/// Arrow has no tz-aware time type, so we carry the zone as a sibling column rather than dropping it
/// the way AWS DMS does (§2.4). Sign convention (pinned by the conformance test): `offset_seconds`
/// is the offset *as printed* — east of UTC positive, so `+05:30` → `+19800`, `-08` → `-28800`.
pub fn timetz_fields(name: &str) -> Vec<Field> {
    vec![
        Field::new(format!("{name}_micros"), DataType::Int64, true),
        Field::new(format!("{name}_offset_seconds"), DataType::Int32, true),
    ]
}

/// The five flat sibling columns a `range` decomposes into (§2.4). Element type per family; all five
/// share the whole-column NULL, so `_empty=false` + a NULL bound is a genuine *unbounded* side (which
/// `lower_inf`/`upper_inf` derive from) — kept distinct from both `empty` and a NULL column.
pub fn range_fields(name: &str, family: RangeFamily, atttypmod: i32) -> Vec<Field> {
    let elem = family.elem_data_type(atttypmod);
    vec![
        Field::new(format!("{name}_lower"), elem.clone(), true),
        Field::new(format!("{name}_upper"), elem, true),
        Field::new(format!("{name}_lower_inc"), DataType::Boolean, true),
        Field::new(format!("{name}_upper_inc"), DataType::Boolean, true),
        Field::new(format!("{name}_empty"), DataType::Boolean, true),
    ]
}

/// The 4-field struct a multirange member carries: `lower`/`upper` (nullable — a member may be
/// unbounded) and the always-present `lower_inc`/`upper_inc`. Shared by the schema field and the
/// builder so `RecordBatch::try_new` sees identical types.
pub fn multirange_struct_fields(family: RangeFamily, atttypmod: i32) -> Fields {
    let elem = family.elem_data_type(atttypmod);
    vec![
        Field::new("lower", elem.clone(), true),
        Field::new("upper", elem, true),
        Field::new("lower_inc", DataType::Boolean, false),
        Field::new("upper_inc", DataType::Boolean, false),
    ]
    .into()
}

/// A `multirange` → one `LIST(STRUCT(lower, upper, lower_inc, upper_inc))` field (§2.4). Empty
/// multirange = empty list; SQL NULL = NULL list — the two stay distinct via the outer list validity.
pub fn multirange_field(name: &str, family: RangeFamily, atttypmod: i32) -> Field {
    let item = Field::new_list_field(
        DataType::Struct(multirange_struct_fields(family, atttypmod)),
        true,
    );
    Field::new(name, DataType::List(Arc::new(item)), true)
}

fn parse_err(kind: &str, text: &str) -> Error {
    Error::ValueParse {
        column: kind.to_string(),
        value: text.to_string(),
        data_type: kind.to_string(),
    }
}

/// Parse a `HH:MM:SS[.ffffff]` clock (no sign) into microseconds. Fractional seconds are padded /
/// truncated to microsecond resolution.
fn hms_to_micros(body: &str) -> Option<i64> {
    let mut it = body.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let s = it.next().unwrap_or("0");
    if it.next().is_some() {
        return None;
    }
    let (sec, frac) = s.split_once('.').unwrap_or((s, ""));
    let sec: i64 = sec.parse().ok()?;
    let frac_digits: String = frac.chars().chain(std::iter::repeat('0')).take(6).collect();
    let frac_micros: i64 = frac_digits.parse().ok()?;
    Some((h * 3600 + m * 60 + sec) * 1_000_000 + frac_micros)
}

/// A signed `[-]HH:MM:SS[.ffffff]` time token (the negative sign applies to the whole clock).
fn signed_time_to_micros(tok: &str) -> Option<i64> {
    match tok.strip_prefix('-') {
        Some(body) => hms_to_micros(body).map(|us| -us),
        None => hms_to_micros(tok),
    }
}

/// Parse canonical interval text (`"1 year 2 mons 3 days 04:05:06.5"`) into `(months, days, micros)`.
///
/// Handles the server-default `postgres` IntervalStyle: word units (`year`/`mon`/`day` and, for
/// robustness, `hour`/`min`/`sec`) plus a trailing signed `HH:MM:SS[.f]` clock. The three fields stay
/// independent — `'1 mon'`→`(1,0,0)`, `'30 days'`→`(0,30,0)`, `'720:00:00'`→`(0,0,2_592_000_000_000)`.
pub fn parse_interval(text: &str) -> Result<(i32, i32, i64), Error> {
    let err = || parse_err("interval", text);
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i64 = 0;
    let mut ago = false;

    let toks: Vec<&str> = text.split_whitespace().collect();
    let mut i = 0;
    while i < toks.len() {
        let tok = toks[i];
        // A clock token (`04:05:06.5`) contributes microseconds directly.
        if tok.contains(':') {
            micros += signed_time_to_micros(tok).ok_or_else(err)?;
            i += 1;
            continue;
        }
        // `postgres_verbose` decorations: `@ 1 day ago`.
        if tok == "@" {
            i += 1;
            continue;
        }
        if tok == "ago" {
            ago = true;
            i += 1;
            continue;
        }
        // Otherwise a `<number> <unit>` pair.
        let n: i64 = tok.parse().map_err(|_| err())?;
        let unit = toks.get(i + 1).ok_or_else(err)?;
        match *unit {
            "year" | "years" | "yr" | "yrs" => months += n * 12,
            "mon" | "mons" | "month" | "months" => months += n,
            "day" | "days" => days += n,
            "hour" | "hours" | "hr" | "hrs" => micros += n * 3_600_000_000,
            "min" | "mins" | "minute" | "minutes" => micros += n * 60_000_000,
            "sec" | "secs" | "second" | "seconds" => micros += n * 1_000_000,
            _ => return Err(err()),
        }
        i += 2;
    }
    if ago {
        months = -months;
        days = -days;
        micros = -micros;
    }
    let months = i32::try_from(months).map_err(|_| err())?;
    let days = i32::try_from(days).map_err(|_| err())?;
    Ok((months, days, micros))
}

/// Parse canonical `timetz` text (`"12:34:56.789+05:30"`) into `(micros_since_midnight, offset_seconds)`.
///
/// The time part has no sign, so the first `+`/`-` in the string marks the offset. `offset_seconds`
/// keeps the printed sign (`+05:30` → `+19800`) — the loader's TIMETZ rebuild depends on it.
pub fn parse_timetz(text: &str) -> Result<(i64, i32), Error> {
    let err = || parse_err("timetz", text);
    let idx = text.find(['+', '-']).ok_or_else(err)?;
    let micros = hms_to_micros(&text[..idx]).ok_or_else(err)?;
    let sign: i32 = if text.as_bytes()[idx] == b'-' { -1 } else { 1 };
    let off = &text[idx + 1..];
    let mut it = off.split(':');
    let oh: i32 = it.next().ok_or_else(err)?.parse().map_err(|_| err())?;
    let om: i32 = it.next().unwrap_or("0").parse().map_err(|_| err())?;
    let os: i32 = it.next().unwrap_or("0").parse().map_err(|_| err())?;
    if it.next().is_some() {
        return Err(err());
    }
    Ok((micros, sign * (oh * 3600 + om * 60 + os)))
}

#[cfg(test)]
#[path = "tier2_test.rs"]
mod tests;
