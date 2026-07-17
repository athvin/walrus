use super::*;
use crate::sink::FileKind;
use object_store::path::Path;

#[test]
fn maps_written_object_to_a_stream_ready_row() {
    let obj = WrittenObject {
        s3_uri: "s3://walrus/7/public/orders/000000000000A100-uuid.parquet".to_string(),
        key: Path::from("7/public/orders/000000000000A100-uuid.parquet"),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        lsn_start: "0/100".parse().unwrap(),
        lsn_end: "0/A100".parse().unwrap(),
        row_count: 42,
        schema_version: 3,
        kind: FileKind::Stream,
    };
    let row = to_ready_row(9, &obj, None);
    assert_eq!(row.epoch, 9);
    assert_eq!(
        row.reload_id, None,
        "stream objects never carry a reload_id"
    );
    assert_eq!(row.source_schema, "public");
    assert_eq!(row.source_table, "orders");
    assert_eq!(row.kind, FileKind::Stream);
    assert_eq!(row.row_count, 42);
    assert_eq!(row.lsn_end, "0/A100".parse().unwrap());
    assert_eq!(row.schema_version, 3);
}
