use super::*;
use crate::oids;
use arrow::array::{
    Array, AsArray, Decimal128Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
    TimestampMicrosecondArray,
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
    // Order-independent: the amortized serialization (PR 5.7) splices batch-constant + per-row
    // fragments, so the key ORDER may differ from `to_string(SinkMeta)`; the loader parses by key.
    let got: serde_json::Value = serde_json::from_str(meta_col.value(0)).unwrap();
    let want: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
    assert_eq!(got, want);
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

#[test]
fn geometric_point_round_trips_and_nulls() {
    let rel = one_col_rel("loc", oids::POINT, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(&[TupleValue::Text("(1,2)".to_string())], &meta(vec![]))
        .unwrap();
    b.append_row(&[TupleValue::Null], &meta(vec![])).unwrap();
    let batch = b.finish().unwrap();
    let s = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let x = s.column(0).as_primitive::<arrow::datatypes::Float64Type>();
    let y = s.column(1).as_primitive::<arrow::datatypes::Float64Type>();
    assert_eq!(x.value(0), 1.0);
    assert_eq!(y.value(0), 2.0);
    assert!(s.is_null(1), "a NULL point is a null struct row");
}

#[test]
fn geometric_box_nests_two_points() {
    let rel = one_col_rel("bx", oids::BOX, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("(2,3),(0,1)".to_string())],
        &meta(vec![]),
    )
    .unwrap();
    let batch = b.finish().unwrap();
    let s = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let p1 = s.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    let p2 = s.column(1).as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        p1.column(0)
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(0),
        2.0
    ); // p1.x
    assert_eq!(
        p2.column(1)
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(0),
        1.0
    ); // p2.y
}

#[test]
fn geometric_path_open_vs_closed_only_differs_by_is_closed() {
    let rel = one_col_rel("pth", oids::PATH, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("[(0,0),(1,1)]".to_string())],
        &meta(vec![]),
    )
    .unwrap(); // open
    b.append_row(
        &[TupleValue::Text("((0,0),(1,1))".to_string())],
        &meta(vec![]),
    )
    .unwrap(); // closed
    let batch = b.finish().unwrap();
    let s = batch
        .column(0)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let is_closed = s.column(0).as_boolean();
    assert!(!is_closed.value(0), "brackets → open");
    assert!(is_closed.value(1), "double parens → closed");
    // Same points either way (the only difference is the flag).
    let pts = s.column(1).as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(pts.value_length(0), 2);
    assert_eq!(pts.value_length(1), 2);
}

#[test]
fn geometric_polygon_is_list_of_points() {
    let rel = one_col_rel("poly", oids::POLYGON, -1);
    let mut b = BatchBuilder::new(&rel).unwrap();
    b.append_row(
        &[TupleValue::Text("((0,0),(1,0),(1,1))".to_string())],
        &meta(vec![]),
    )
    .unwrap();
    let batch = b.finish().unwrap();
    let list = batch
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(list.value_length(0), 3);
    let members = list.value(0);
    let s = members.as_any().downcast_ref::<StructArray>().unwrap();
    let y = s.column(1).as_primitive::<arrow::datatypes::Float64Type>();
    assert_eq!(y.value(2), 1.0);
}
