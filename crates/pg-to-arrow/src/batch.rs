//! `BatchBuilder`: decoded `TupleValue`s + a `SinkMeta` → an Arrow `RecordBatch`.
//!
//! pgoutput ships values as canonical **text**; this builder parses each into its Tier-1 Arrow
//! representation, maps `Null` and `UnchangedToast` onto the validity bitmap (a null in both cases —
//! the TOAST placeholder's column name is recorded in `SinkMeta.unchanged_toast` upstream and echoed
//! into the meta JSON; *resolving* it is the loader's back-scan, PR 3.6), and serializes the
//! provenance into the trailing `walrus_pg_sink_meta` column. All column builders (including meta)
//! move in lockstep — every `append_row` pushes exactly one slot to every column.

use crate::error::Error;
use crate::oids;
use crate::range::RangeFamily;
use crate::schema::{build_schema, tier1_data_type, SINK_META_COLUMN};
use arrow::array::{
    make_builder, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder,
    Decimal128Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    ListBuilder, RecordBatch, StringBuilder, StructBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef, TimeUnit};
use common::{PgColumn, PgRelation, SinkMeta, TupleValue};
use std::sync::Arc;

/// How one source column's `TupleValue` fans out onto the flat builder list. Tier-1 consumes one
/// builder (the existing `append_value` path); Tier-2 spreads a single value across several sibling
/// builders (PR 2.12). Ordering here MUST match `emit_fields` / `build_schema` (§2.4, PR 2.17's
/// descriptor `emit[]` lists the same suffixes in the same order).
enum Emit {
    Scalar,     // 1 builder
    Interval,   // 3 builders: _months(i32), _days(i32), _micros(i64)
    Timetz,     // 2 builders: _micros(i64), _offset_seconds(i32)
    Range,      // 5 builders: _lower, _upper, _lower_inc, _upper_inc, _empty
    Multirange, // 1 builder: ListBuilder<StructBuilder>
}

/// Classify a source column into its fan-out shape. MUST stay in lockstep with `emit_fields` — the
/// widths (1/3/2/5/1) are how `append_row` advances the flat builder cursor.
fn emit_kind(col: &PgColumn) -> Result<Emit, Error> {
    if tier1_data_type(col.type_oid, col.type_modifier).is_some() {
        return Ok(Emit::Scalar);
    }
    match col.type_oid {
        oids::INTERVAL => return Ok(Emit::Interval),
        oids::TIMETZ => return Ok(Emit::Timetz),
        _ => {}
    }
    if RangeFamily::from_range_oid(col.type_oid).is_some() {
        return Ok(Emit::Range);
    }
    if RangeFamily::from_multirange_oid(col.type_oid).is_some() {
        return Ok(Emit::Multirange);
    }
    Err(Error::NotTier1 {
        oid: col.type_oid,
        typmod: col.type_modifier,
    })
}

/// Accumulates decoded rows for ONE relation into a single Arrow `RecordBatch`.
pub struct BatchBuilder {
    schema: SchemaRef,
    builders: Vec<Box<dyn ArrayBuilder>>, // one per EMITTED field, flat, in schema order
    plan: Vec<Emit>,                      // one per SOURCE column: how its value fans out
    meta: StringBuilder,                  // the trailing walrus_pg_sink_meta column
    rows: usize,
}

impl BatchBuilder {
    /// Build empty typed builders from the relation's Arrow schema (PR 2.9; Tier-2 fan-out, PR 2.12).
    pub fn new(rel: &PgRelation) -> Result<Self, Error> {
        let schema = Arc::new(build_schema(rel)?);
        // One flat builder per data field (every field except the trailing meta column).
        let data_field_count = schema.fields().len() - 1;
        let mut builders = Vec::with_capacity(data_field_count);
        for field in schema.fields().iter().take(data_field_count) {
            builders.push(column_builder(field)?);
        }
        // One routing entry per source column; its widths sum to `data_field_count`.
        let mut plan = Vec::with_capacity(rel.columns.len());
        for col in &rel.columns {
            plan.push(emit_kind(col)?);
        }
        Ok(BatchBuilder {
            schema,
            builders,
            plan,
            meta: StringBuilder::new(),
            rows: 0,
        })
    }

    /// Append one decoded tuple + its provenance. `values.len()` must equal the source column count
    /// (one `TupleValue` per source column — Tier-2 values fan out to several builders internally).
    pub fn append_row(&mut self, values: &[TupleValue], meta: &SinkMeta) -> Result<(), Error> {
        if values.len() != self.plan.len() {
            return Err(Error::RowLenMismatch {
                expected: self.plan.len(),
                got: values.len(),
            });
        }
        // Clone the Fields (Arc) so we can read the field types while mutably borrowing `builders`.
        let fields = self.schema.fields().clone();
        let mut bi = 0; // flat builder index; advances by each column's emit width
        for (emit, value) in self.plan.iter().zip(values) {
            match emit {
                Emit::Scalar => {
                    append_value(self.builders[bi].as_mut(), &fields[bi], value)?;
                    bi += 1;
                }
                Emit::Interval => {
                    append_interval(&mut self.builders[bi..bi + 3], fields[bi].name(), value)?;
                    bi += 3;
                }
                Emit::Timetz => {
                    append_timetz(&mut self.builders[bi..bi + 2], fields[bi].name(), value)?;
                    bi += 2;
                }
                Emit::Range => {
                    append_range(&mut self.builders[bi..bi + 5], &fields[bi..bi + 5], value)?;
                    bi += 5;
                }
                Emit::Multirange => {
                    append_multirange(self.builders[bi].as_mut(), &fields[bi], value)?;
                    bi += 1;
                }
            }
        }
        let json = serde_json::to_string(meta).map_err(|e| Error::ValueParse {
            column: SINK_META_COLUMN.to_string(),
            value: e.to_string(),
            data_type: "json".to_string(),
        })?;
        self.meta.append_value(json);
        self.rows += 1;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Finish all builders into arrays and assemble the schema-checked `RecordBatch`.
    pub fn finish(mut self) -> Result<RecordBatch, Error> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.builders.len() + 1);
        for builder in &mut self.builders {
            arrays.push(builder.finish());
        }
        arrays.push(Arc::new(self.meta.finish()));
        Ok(RecordBatch::try_new(self.schema.clone(), arrays)?)
    }
}

/// A typed builder matching `field`'s Arrow type. `make_builder` covers most types; Decimal128 and
/// Timestamp are built explicitly so `finish()` preserves the precision/scale and timezone that the
/// schema (and DuckDB read-back, PR 2.11) require.
fn column_builder(field: &Field) -> Result<Box<dyn ArrayBuilder>, Error> {
    Ok(match field.data_type() {
        DataType::Decimal128(p, s) => {
            Box::new(Decimal128Builder::new().with_precision_and_scale(*p, *s)?)
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            Box::new(TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()))
        }
        // Multirange: LIST<STRUCT>. Build the struct's child builders via `column_builder` too, so a
        // Decimal/Timestamp member bound keeps its precision/scale/tz (`make_builder` would drop them
        // and the finished array would mismatch the schema). ListBuilder's default item field is
        // `("item", …, nullable=true)`, matching `multirange_field`'s `Field::new_list_field`.
        DataType::List(item) => match item.data_type() {
            DataType::Struct(struct_fields) => {
                let mut children = Vec::with_capacity(struct_fields.len());
                for f in struct_fields.iter() {
                    children.push(column_builder(f)?);
                }
                let sb = StructBuilder::new(struct_fields.clone(), children);
                Box::new(ListBuilder::new(sb))
            }
            _ => make_builder(field.data_type(), 0),
        },
        other => make_builder(other, 0),
    })
}

macro_rules! downcast {
    ($builder:expr, $ty:ty, $col:expr) => {
        $builder
            .as_any_mut()
            .downcast_mut::<$ty>()
            .ok_or_else(|| Error::Downcast {
                column: $col.to_string(),
            })?
    };
}

/// Append one `TupleValue` to one typed builder. `Null`/`UnchangedToast` → `append_null`.
fn append_value(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    value: &TupleValue,
) -> Result<(), Error> {
    let col = field.name();
    let dt = field.data_type();
    // NULL and unchanged-TOAST both append a null on the validity bitmap. (The TOAST placeholder's
    // column name lives in SinkMeta.unchanged_toast, echoed into the meta JSON, not resolved here.)
    let is_null = matches!(value, TupleValue::Null | TupleValue::UnchangedToast);

    match dt {
        DataType::Boolean => {
            let b = downcast!(builder, BooleanBuilder, col);
            match is_null {
                true => b.append_null(),
                false => b.append_value(parse_bool(text(value, col, dt)?, col)?),
            }
        }
        DataType::Int16 => append_num::<Int16Builder, i16>(builder, value, col, dt, is_null)?,
        DataType::Int32 => append_num::<Int32Builder, i32>(builder, value, col, dt, is_null)?,
        DataType::Int64 => append_num::<Int64Builder, i64>(builder, value, col, dt, is_null)?,
        DataType::Float32 => append_num::<Float32Builder, f32>(builder, value, col, dt, is_null)?,
        DataType::Float64 => append_num::<Float64Builder, f64>(builder, value, col, dt, is_null)?,
        DataType::Decimal128(_, scale) => {
            let b = downcast!(builder, Decimal128Builder, col);
            match is_null {
                true => b.append_null(),
                false => b.append_value(parse_decimal(text(value, col, dt)?, *scale, col)?),
            }
        }
        DataType::Utf8 => {
            let b = downcast!(builder, StringBuilder, col);
            match is_null {
                true => b.append_null(),
                false => b.append_value(text(value, col, dt)?),
            }
        }
        DataType::Binary => {
            let b = downcast!(builder, BinaryBuilder, col);
            if is_null {
                b.append_null();
            } else {
                match value {
                    TupleValue::Binary(bytes) => b.append_value(bytes),
                    // bytea text is `\x…` hex under text mode.
                    TupleValue::Text(s) => b.append_value(&parse_bytea(s, col)?),
                    _ => unreachable!("is_null covers Null/UnchangedToast"),
                }
            }
        }
        DataType::Date32 => {
            let b = downcast!(builder, Date32Builder, col);
            match is_null {
                true => b.append_null(),
                false => b.append_value(parse_date_days(text(value, col, dt)?, col)?),
            }
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let b = downcast!(builder, Time64MicrosecondBuilder, col);
            match is_null {
                true => b.append_null(),
                false => b.append_value(parse_time_micros(text(value, col, dt)?, col)?),
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let b = downcast!(builder, TimestampMicrosecondBuilder, col);
            match is_null {
                true => b.append_null(),
                false => {
                    let s = text(value, col, dt)?;
                    let micros = if tz.is_some() {
                        parse_timestamptz_micros(s, col)?
                    } else {
                        parse_timestamp_micros(s, col)?
                    };
                    b.append_value(micros);
                }
            }
        }
        // `append_value` only runs for `Emit::Scalar` (Tier-1) columns; Tier-2 fan-out has its own
        // `append_interval`/`append_timetz`. So no other Arrow type reaches this arm.
        _ => {
            return Err(Error::Downcast {
                column: col.to_string(),
            })
        }
    }
    Ok(())
}

/// Fan a single `interval` value across its three sibling builders (`_months` i32, `_days` i32,
/// `_micros` i64). NULL / unchanged-TOAST sets all three null in lockstep — the one shared logical
/// NULL that keeps a real zero interval `(0,0,0)` distinguishable from absence (§2.4).
fn append_interval(
    builders: &mut [Box<dyn ArrayBuilder>],
    col: &str,
    value: &TupleValue,
) -> Result<(), Error> {
    let parts = match value {
        TupleValue::Null | TupleValue::UnchangedToast => None,
        _ => Some(crate::tier2::parse_interval(text(
            value,
            col,
            &DataType::Int64,
        )?)?),
    };
    let months = downcast!(builders[0], Int32Builder, col);
    match parts {
        Some((m, _, _)) => months.append_value(m),
        None => months.append_null(),
    }
    let days = downcast!(builders[1], Int32Builder, col);
    match parts {
        Some((_, d, _)) => days.append_value(d),
        None => days.append_null(),
    }
    let micros = downcast!(builders[2], Int64Builder, col);
    match parts {
        Some((_, _, us)) => micros.append_value(us),
        None => micros.append_null(),
    }
    Ok(())
}

/// Fan a single `timetz` value across `_micros` (i64) and `_offset_seconds` (i32); NULL sets both.
fn append_timetz(
    builders: &mut [Box<dyn ArrayBuilder>],
    col: &str,
    value: &TupleValue,
) -> Result<(), Error> {
    let parts = match value {
        TupleValue::Null | TupleValue::UnchangedToast => None,
        _ => Some(crate::tier2::parse_timetz(text(
            value,
            col,
            &DataType::Int64,
        )?)?),
    };
    let micros = downcast!(builders[0], Int64Builder, col);
    match parts {
        Some((us, _)) => micros.append_value(us),
        None => micros.append_null(),
    }
    let offset = downcast!(builders[1], Int32Builder, col);
    match parts {
        Some((_, off)) => offset.append_value(off),
        None => offset.append_null(),
    }
    Ok(())
}

/// Fan a single `range` value across its five sibling builders (`_lower`, `_upper`, `_lower_inc`,
/// `_upper_inc`, `_empty`). The three states stay distinct: a whole SQL NULL nulls all five; `empty`
/// sets `_empty=true` with NULL bounds; an unbounded side is a NULL bound with `_empty=false`.
fn append_range(
    builders: &mut [Box<dyn ArrayBuilder>],
    fields: &[FieldRef],
    value: &TupleValue,
) -> Result<(), Error> {
    let col = fields[0].name();
    if matches!(value, TupleValue::Null | TupleValue::UnchangedToast) {
        // Whole-column NULL → every sibling null (bounds via append_value, flags via BooleanBuilder).
        append_value(builders[0].as_mut(), &fields[0], &TupleValue::Null)?;
        append_value(builders[1].as_mut(), &fields[1], &TupleValue::Null)?;
        for b in builders[2..5].iter_mut() {
            bool_builder(b.as_mut(), col)?.append_null();
        }
        return Ok(());
    }
    let r = crate::range::parse_range(text(value, col, fields[0].data_type())?)?;
    // Bounds reuse the Tier-1 text parsing; a `None` (unbounded) bound appends null.
    append_value(builders[0].as_mut(), &fields[0], &opt_text_value(&r.lower))?;
    append_value(builders[1].as_mut(), &fields[1], &opt_text_value(&r.upper))?;
    bool_builder(builders[2].as_mut(), col)?.append_value(r.lower_inc);
    bool_builder(builders[3].as_mut(), col)?.append_value(r.upper_inc);
    bool_builder(builders[4].as_mut(), col)?.append_value(r.empty);
    Ok(())
}

/// Fan a single `multirange` value onto one `ListBuilder<StructBuilder>`: one struct per member, then
/// `append(true)`. Empty multirange = zero members + `append(true)` (empty list); NULL = `append_null`
/// (NULL list) — the two are kept distinct.
fn append_multirange(
    builder: &mut dyn ArrayBuilder,
    field: &Field,
    value: &TupleValue,
) -> Result<(), Error> {
    let col = field.name();
    let lb = downcast!(builder, ListBuilder<StructBuilder>, col);
    if matches!(value, TupleValue::Null | TupleValue::UnchangedToast) {
        lb.append_null();
        return Ok(());
    }
    let members = crate::range::parse_multirange(text(value, col, field.data_type())?)?;
    let elem = multirange_elem_type(field)?;
    {
        let sb = lb.values();
        for m in &members {
            append_struct_bound(sb, 0, &elem, m.lower.as_deref(), col)?;
            append_struct_bound(sb, 1, &elem, m.upper.as_deref(), col)?;
            struct_field::<BooleanBuilder>(sb, 2, col)?.append_value(m.lower_inc);
            struct_field::<BooleanBuilder>(sb, 3, col)?.append_value(m.upper_inc);
            sb.append(true);
        }
    }
    lb.append(true);
    Ok(())
}

/// The element (`_lower`/`_upper`) Arrow type carried inside a multirange's `LIST<STRUCT>` field.
fn multirange_elem_type(field: &Field) -> Result<DataType, Error> {
    if let DataType::List(item) = field.data_type() {
        if let DataType::Struct(fs) = item.data_type() {
            return Ok(fs[0].data_type().clone());
        }
    }
    Err(Error::Downcast {
        column: field.name().to_string(),
    })
}

/// Append one multirange member bound (parsed text, or `None` = unbounded → null) to struct child `idx`.
fn append_struct_bound(
    sb: &mut StructBuilder,
    idx: usize,
    dt: &DataType,
    bound: Option<&str>,
    col: &str,
) -> Result<(), Error> {
    match dt {
        DataType::Int32 => {
            let b = struct_field::<Int32Builder>(sb, idx, col)?;
            match bound {
                Some(s) => {
                    b.append_value(s.parse::<i32>().map_err(|_| value_err(col, s, "Int32"))?)
                }
                None => b.append_null(),
            }
        }
        DataType::Int64 => {
            let b = struct_field::<Int64Builder>(sb, idx, col)?;
            match bound {
                Some(s) => {
                    b.append_value(s.parse::<i64>().map_err(|_| value_err(col, s, "Int64"))?)
                }
                None => b.append_null(),
            }
        }
        DataType::Utf8 => {
            let b = struct_field::<StringBuilder>(sb, idx, col)?;
            match bound {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        DataType::Date32 => {
            let b = struct_field::<Date32Builder>(sb, idx, col)?;
            match bound {
                Some(s) => b.append_value(parse_date_days(s, col)?),
                None => b.append_null(),
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let micros = match bound {
                Some(s) if tz.is_some() => Some(parse_timestamptz_micros(s, col)?),
                Some(s) => Some(parse_timestamp_micros(s, col)?),
                None => None,
            };
            let b = struct_field::<TimestampMicrosecondBuilder>(sb, idx, col)?;
            match micros {
                Some(us) => b.append_value(us),
                None => b.append_null(),
            }
        }
        DataType::Decimal128(_, scale) => {
            let parsed = match bound {
                Some(s) => Some(parse_decimal(s, *scale, col)?),
                None => None,
            };
            let b = struct_field::<Decimal128Builder>(sb, idx, col)?;
            match parsed {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        _ => {
            return Err(Error::Downcast {
                column: col.to_string(),
            })
        }
    }
    Ok(())
}

/// Typed accessor for struct child `idx`, attributing a downcast failure to the column.
fn struct_field<'a, T: ArrayBuilder>(
    sb: &'a mut StructBuilder,
    idx: usize,
    col: &str,
) -> Result<&'a mut T, Error> {
    sb.field_builder::<T>(idx).ok_or_else(|| Error::Downcast {
        column: col.to_string(),
    })
}

/// Downcast a boxed builder to `BooleanBuilder` (the range inclusivity / empty flags).
fn bool_builder<'a>(
    builder: &'a mut dyn ArrayBuilder,
    col: &str,
) -> Result<&'a mut BooleanBuilder, Error> {
    builder
        .as_any_mut()
        .downcast_mut::<BooleanBuilder>()
        .ok_or_else(|| Error::Downcast {
            column: col.to_string(),
        })
}

/// A range bound as a `TupleValue`: `Some(text)` → `Text`, `None` (unbounded) → `Null` (→ append_null).
fn opt_text_value(bound: &Option<String>) -> TupleValue {
    match bound {
        Some(s) => TupleValue::Text(s.clone()),
        None => TupleValue::Null,
    }
}

/// Append a parsed number to a `FromStr` builder, attributing a failure to the column.
fn append_num<B, T>(
    builder: &mut dyn ArrayBuilder,
    value: &TupleValue,
    col: &str,
    dt: &DataType,
    is_null: bool,
) -> Result<(), Error>
where
    B: ArrayBuilder + ArrowNumBuilder<T>,
    T: std::str::FromStr,
{
    let b = builder
        .as_any_mut()
        .downcast_mut::<B>()
        .ok_or_else(|| Error::Downcast {
            column: col.to_string(),
        })?;
    if is_null {
        b.append_null_val();
    } else {
        let s = text(value, col, dt)?;
        let parsed = s.parse::<T>().map_err(|_| Error::ValueParse {
            column: col.to_string(),
            value: s.to_string(),
            data_type: dt.to_string(),
        })?;
        b.append_val(parsed);
    }
    Ok(())
}

/// Tiny bridge so `append_num` can be generic over the numeric builders.
trait ArrowNumBuilder<T> {
    fn append_val(&mut self, v: T);
    fn append_null_val(&mut self);
}
macro_rules! num_builder {
    ($b:ty, $t:ty) => {
        impl ArrowNumBuilder<$t> for $b {
            fn append_val(&mut self, v: $t) {
                self.append_value(v);
            }
            fn append_null_val(&mut self) {
                self.append_null();
            }
        }
    };
}
num_builder!(Int16Builder, i16);
num_builder!(Int32Builder, i32);
num_builder!(Int64Builder, i64);
num_builder!(Float32Builder, f32);
num_builder!(Float64Builder, f64);

/// Extract the text of a value (for the text-format Tier-1 types).
fn text<'a>(value: &'a TupleValue, col: &str, dt: &DataType) -> Result<&'a str, Error> {
    match value {
        TupleValue::Text(s) => Ok(s),
        other => Err(Error::ValueParse {
            column: col.to_string(),
            value: format!("{other:?}"),
            data_type: dt.to_string(),
        }),
    }
}

fn parse_bool(s: &str, col: &str) -> Result<bool, Error> {
    match s {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => Err(Error::ValueParse {
            column: col.to_string(),
            value: s.to_string(),
            data_type: "Boolean".to_string(),
        }),
    }
}

/// Parse `"19.99"` at the field's scale into the unscaled `i128`. Rejects a value carrying more
/// fractional digits than the declared scale (rounding is out of scope).
fn parse_decimal(s: &str, scale: i8, col: &str) -> Result<i128, Error> {
    let err = || Error::ValueParse {
        column: col.to_string(),
        value: s.to_string(),
        data_type: format!("Decimal128(scale {scale})"),
    };
    if scale < 0 {
        return Err(err());
    }
    let scale = scale as usize;
    let (sign, rest) = match s.strip_prefix('-') {
        Some(r) => (-1i128, r),
        None => (1, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = rest.split_once('.').unwrap_or((rest, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(err());
    }
    if frac_part.len() > scale {
        return Err(err());
    }
    let mut digits = String::with_capacity(int_part.len() + scale);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(scale - frac_part.len()) {
        digits.push('0');
    }
    let magnitude: i128 = digits.parse().map_err(|_| err())?;
    Ok(sign * magnitude)
}

fn parse_bytea(s: &str, col: &str) -> Result<Vec<u8>, Error> {
    let hex = s.strip_prefix("\\x").ok_or_else(|| Error::ValueParse {
        column: col.to_string(),
        value: s.to_string(),
        data_type: "Binary".to_string(),
    })?;
    hex::decode(hex).map_err(|_| Error::ValueParse {
        column: col.to_string(),
        value: s.to_string(),
        data_type: "Binary".to_string(),
    })
}

/// Micros since the Unix epoch for an RFC-3339 string.
fn rfc3339_micros(s: &str) -> Option<i64> {
    s.parse::<jiff::Timestamp>()
        .ok()
        .map(|t| t.as_microsecond())
}

fn value_err(col: &str, s: &str, dt: &str) -> Error {
    Error::ValueParse {
        column: col.to_string(),
        value: s.to_string(),
        data_type: dt.to_string(),
    }
}

/// `"2024-01-02"` → days since 1970-01-01.
fn parse_date_days(s: &str, col: &str) -> Result<i32, Error> {
    let micros =
        rfc3339_micros(&format!("{s}T00:00:00Z")).ok_or_else(|| value_err(col, s, "Date32"))?;
    i32::try_from(micros / 86_400_000_000).map_err(|_| value_err(col, s, "Date32"))
}

/// `"03:04:05.678901"` → micros since midnight.
fn parse_time_micros(s: &str, col: &str) -> Result<i64, Error> {
    rfc3339_micros(&format!("1970-01-01T{s}Z")).ok_or_else(|| value_err(col, s, "Time64"))
}

/// `"2024-01-02 03:04:05.678901"` (no offset) → micros since epoch, treated as UTC.
fn parse_timestamp_micros(s: &str, col: &str) -> Result<i64, Error> {
    let normalized = s.replacen(' ', "T", 1);
    rfc3339_micros(&format!("{normalized}Z")).ok_or_else(|| value_err(col, s, "Timestamp"))
}

/// Canonical Postgres `timestamptz` (`"…+00"`, already UTC upstream) → micros since epoch.
fn parse_timestamptz_micros(s: &str, col: &str) -> Result<i64, Error> {
    let mut n = s.replacen(' ', "T", 1);
    // Postgres prints whole-hour offsets as `+HH`; jiff wants `+HH:MM`.
    if let Some(t) = n.find('T') {
        if let Some(sign) = n[t..].rfind(['+', '-']) {
            if n[t + sign..].len() == 3 {
                n.push_str(":00");
            }
        }
    }
    rfc3339_micros(&n).ok_or_else(|| value_err(col, s, "TimestampTz"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oids;
    use arrow::array::{
        Array, AsArray, Decimal128Array, Int32Array, Int64Array, ListArray, StringArray,
        StructArray, TimestampMicrosecondArray,
    };
    use common::{Kind, Op, PgColumn, PgRelation, ReplicaIdentity, SinkMeta, UtcTimestamp};

    fn col(name: &str, oid: u32, typmod: i32) -> PgColumn {
        PgColumn {
            name: name.to_string(),
            type_oid: oid,
            type_modifier: typmod,
            is_key: false,
        }
    }

    fn orders() -> PgRelation {
        PgRelation {
            oid: 16397,
            schema: "public".to_string(),
            name: "orders".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                col("id", oids::INT4, -1),
                col("amount", oids::NUMERIC, 655366), // numeric(10,2)
                col("created_at", oids::TIMESTAMPTZ, -1),
                col("note", oids::TEXT, -1),
            ],
        }
    }

    fn meta(unchanged_toast: Vec<String>) -> SinkMeta {
        SinkMeta {
            op: Op::Insert,
            lsn: "0/10".parse().unwrap(),
            commit_lsn: "0/20".parse().unwrap(),
            commit_ts: UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00Z").unwrap(),
            xid: 1,
            epoch: 7,
            batch_id: "b1".to_string(),
            schema_version: 1,
            source_schema: "public".to_string(),
            source_table: "orders".to_string(),
            kind: Kind::Stream,
            unchanged_toast,
            sink_instance: "walrus-pg-sink-0".to_string(),
            sink_processed_at: UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00.123Z").unwrap(),
        }
    }

    fn text_vals(vals: &[&str]) -> Vec<TupleValue> {
        vals.iter()
            .map(|s| TupleValue::Text(s.to_string()))
            .collect()
    }

    #[test]
    fn builds_a_batch_from_an_orders_insert() {
        let mut b = BatchBuilder::new(&orders()).unwrap();
        b.append_row(
            &text_vals(&["42", "19.99", "2024-01-02 03:04:05.678901+00", "hi"]),
            &meta(vec![]),
        )
        .unwrap();
        assert_eq!(b.len(), 1);
        let batch = b.finish().unwrap();

        assert_eq!(batch.num_columns(), 5); // 4 data + meta
        assert_eq!(*batch.schema(), super::build_schema(&orders()).unwrap());

        let ids = batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(ids.value(0), 42);
        let amt = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(amt.value(0), 1999); // 19.99 at scale 2
        let ts = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(ts.value(0) > 0);
        let note = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(note.value(0), "hi");
    }

    #[test]
    fn null_value_sets_validity_false() {
        let mut b = BatchBuilder::new(&orders()).unwrap();
        // note is NULL
        let vals = vec![
            TupleValue::Text("42".to_string()),
            TupleValue::Text("1.00".to_string()),
            TupleValue::Text("2024-01-02 03:04:05+00".to_string()),
            TupleValue::Null,
        ];
        b.append_row(&vals, &meta(vec![])).unwrap();
        let batch = b.finish().unwrap();
        let note = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(note.is_null(0));
    }

    #[test]
    fn unchanged_toast_appends_null_and_is_listed_in_meta() {
        let mut b = BatchBuilder::new(&orders()).unwrap();
        let vals = vec![
            TupleValue::Text("42".to_string()),
            TupleValue::Text("1.00".to_string()),
            TupleValue::Text("2024-01-02 03:04:05+00".to_string()),
            TupleValue::UnchangedToast,
        ];
        b.append_row(&vals, &meta(vec!["note".to_string()]))
            .unwrap();
        let batch = b.finish().unwrap();
        let note = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(note.is_null(0), "unchanged-TOAST appends a null");
        // and the column name is carried in the meta JSON.
        let meta_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(meta_col.value(0).contains("\"unchanged_toast\":[\"note\"]"));
    }

    #[test]
    fn wrong_arity_row_is_rejected() {
        let mut b = BatchBuilder::new(&orders()).unwrap();
        let err = b
            .append_row(&text_vals(&["42", "1.00"]), &meta(vec![]))
            .unwrap_err();
        assert!(matches!(
            err,
            Error::RowLenMismatch {
                expected: 4,
                got: 2
            }
        ));
    }

    #[test]
    fn bad_int_text_reports_the_column_name() {
        let mut b = BatchBuilder::new(&orders()).unwrap();
        let err = b
            .append_row(
                &text_vals(&["abc", "1.00", "2024-01-02 03:04:05+00", "hi"]),
                &meta(vec![]),
            )
            .unwrap_err();
        match err {
            Error::ValueParse { column, .. } => assert_eq!(column, "id"),
            other => panic!("expected ValueParse, got {other:?}"),
        }
    }

    #[test]
    fn meta_column_holds_serialized_sink_meta_json() {
        let mut b = BatchBuilder::new(&orders()).unwrap();
        let m = meta(vec![]);
        b.append_row(
            &text_vals(&["42", "1.00", "2024-01-02 03:04:05+00", "hi"]),
            &m,
        )
        .unwrap();
        let batch = b.finish().unwrap();
        let meta_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(meta_col.value(0), serde_json::to_string(&m).unwrap());
    }

    #[test]
    fn decimal_rejects_too_many_fractional_digits() {
        assert!(parse_decimal("1.999", 2, "amount").is_err());
        assert_eq!(parse_decimal("19.99", 2, "amount").unwrap(), 1999);
        assert_eq!(parse_decimal("-0.05", 2, "amount").unwrap(), -5);
        assert_eq!(parse_decimal("7", 2, "amount").unwrap(), 700);
    }

    /// A one-column relation of `oid` (used for the Tier-2 fan-out tests).
    fn one_col_rel(name: &str, oid: u32, typmod: i32) -> PgRelation {
        PgRelation {
            oid: 3,
            schema: "public".to_string(),
            name: "t".to_string(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![col(name, oid, typmod)],
        }
    }

    #[test]
    fn interval_fans_out_to_three_builders() {
        let rel = one_col_rel("dur", oids::INTERVAL, -1);
        let mut b = BatchBuilder::new(&rel).unwrap();
        b.append_row(
            &[TupleValue::Text("1 mon 2 days 03:04:05".to_string())],
            &meta(vec![]),
        )
        .unwrap();
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_columns(), 4); // months + days + micros + meta
        let months = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let days = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let micros = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(months.value(0), 1);
        assert_eq!(days.value(0), 2);
        assert_eq!(micros.value(0), (3 * 3600 + 4 * 60 + 5) * 1_000_000);
    }

    #[test]
    fn timetz_fans_out_to_two_builders() {
        let rel = one_col_rel("t", oids::TIMETZ, -1);
        let mut b = BatchBuilder::new(&rel).unwrap();
        b.append_row(
            &[TupleValue::Text("12:34:56+05:30".to_string())],
            &meta(vec![]),
        )
        .unwrap();
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_columns(), 3); // micros + offset + meta
        let micros = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let offset = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(micros.value(0), (12 * 3600 + 34 * 60 + 56) * 1_000_000);
        assert_eq!(offset.value(0), 19_800);
    }

    #[test]
    fn interval_null_maps_all_three_columns_null() {
        let rel = one_col_rel("dur", oids::INTERVAL, -1);
        let mut b = BatchBuilder::new(&rel).unwrap();
        b.append_row(&[TupleValue::Null], &meta(vec![])).unwrap();
        let batch = b.finish().unwrap();
        let months = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let days = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let micros = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(
            months.is_null(0) && days.is_null(0) && micros.is_null(0),
            "all three interval siblings share one logical NULL"
        );
    }

    #[test]
    fn range_fans_out_to_five_builders() {
        let rel = one_col_rel("span", oids::INT4RANGE, -1);
        let mut b = BatchBuilder::new(&rel).unwrap();
        b.append_row(&[TupleValue::Text("[1,10)".to_string())], &meta(vec![]))
            .unwrap();
        let batch = b.finish().unwrap();
        assert_eq!(batch.num_columns(), 6); // 5 range cols + meta
        let lower = batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let upper = batch
            .column(1)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let lower_inc = batch.column(2).as_boolean();
        let upper_inc = batch.column(3).as_boolean();
        let empty = batch.column(4).as_boolean();
        assert_eq!(lower.value(0), 1);
        assert_eq!(upper.value(0), 10);
        assert!(lower_inc.value(0));
        assert!(!upper_inc.value(0));
        assert!(!empty.value(0));
    }

    #[test]
    fn range_empty_unbounded_and_null_are_three_distinct_states() {
        let rel = one_col_rel("span", oids::INT4RANGE, -1);
        let mut b = BatchBuilder::new(&rel).unwrap();
        b.append_row(&[TupleValue::Text("empty".to_string())], &meta(vec![]))
            .unwrap(); // row 0: empty
        b.append_row(&[TupleValue::Text("(,10)".to_string())], &meta(vec![]))
            .unwrap(); // row 1: unbounded lower
        b.append_row(&[TupleValue::Null], &meta(vec![])).unwrap(); // row 2: whole NULL
        let batch = b.finish().unwrap();
        let lower = batch
            .column(0)
            .as_primitive::<arrow::datatypes::Int32Type>();
        let empty = batch.column(4).as_boolean();
        // empty: _empty=true, bounds NULL.
        assert!(empty.value(0));
        assert!(lower.is_null(0));
        // unbounded-lower: _lower NULL but _empty=false (distinct from empty).
        assert!(lower.is_null(1));
        assert!(!empty.value(1));
        // whole NULL: both _lower and _empty NULL (distinct from empty and unbounded).
        assert!(lower.is_null(2));
        assert!(empty.is_null(2));
    }

    #[test]
    fn multirange_builds_list_of_structs_empty_vs_null() {
        let rel = one_col_rel("spans", oids::INT4MULTIRANGE, -1);
        let mut b = BatchBuilder::new(&rel).unwrap();
        b.append_row(
            &[TupleValue::Text("{[1,4),[7,9)}".to_string())],
            &meta(vec![]),
        )
        .unwrap(); // row 0: two members
        b.append_row(&[TupleValue::Text("{}".to_string())], &meta(vec![]))
            .unwrap(); // row 1: empty list
        b.append_row(&[TupleValue::Null], &meta(vec![])).unwrap(); // row 2: NULL list
        let batch = b.finish().unwrap();
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value_length(0), 2);
        assert!(!list.is_null(0));
        assert_eq!(list.value_length(1), 0, "empty multirange = empty list");
        assert!(!list.is_null(1), "empty list is distinct from NULL");
        assert!(list.is_null(2), "NULL column = NULL list");
        // Member bounds round-trip in order.
        let members = list.value(0);
        let s = members.as_any().downcast_ref::<StructArray>().unwrap();
        let lo = s.column(0).as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(lo.value(0), 1);
        assert_eq!(lo.value(1), 7);
    }
}
