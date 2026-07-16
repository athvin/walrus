use super::*;
use crate::batch::BatchBuilder;
use crate::oids;
use bytes::Bytes;
use common::{Kind, Op, PgColumn, PgRelation, ReplicaIdentity, SinkMeta, TupleValue, UtcTimestamp};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn orders() -> PgRelation {
    let col = |name: &str, oid, typmod| PgColumn {
        name: name.to_string(),
        type_oid: oid,
        type_modifier: typmod,
        is_key: false,
    };
    PgRelation {
        oid: 16397,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", oids::INT4, -1),
            col("amount", oids::NUMERIC, 655366),
            col("created_at", oids::TIMESTAMPTZ, -1),
            col("note", oids::TEXT, -1),
        ],
    }
}

fn meta() -> SinkMeta {
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
        unchanged_toast: vec![],
        sink_instance: "walrus-pg-sink-0".to_string(),
        sink_processed_at: UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00Z").unwrap(),
    }
}

#[test]
fn round_trips_a_batch_through_parquet() {
    let mut b = BatchBuilder::new(&orders()).unwrap();
    for v in [
        ["1", "10.00", "2024-01-02 03:04:05+00", "a"],
        ["2", "-3.50", "2024-06-30 23:59:59.999999+00", "b"],
    ] {
        let vals: Vec<TupleValue> = v.iter().map(|s| TupleValue::Text(s.to_string())).collect();
        b.append_row(&vals, &meta()).unwrap();
    }
    let batch = b.finish().unwrap();

    let bytes = write_parquet_bytes(&batch).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap();
    let read: Vec<_> = reader.map(|r| r.unwrap()).collect();
    assert_eq!(read.len(), 1);
    // arrow-rs restores the exact schema (incl. Decimal(10,2) and the "UTC" tz) and values.
    assert_eq!(read[0], batch);
}
