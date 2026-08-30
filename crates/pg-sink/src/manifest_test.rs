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
        schema_version: common::SchemaVersionNo(3),
        kind: FileKind::Stream,
    };
    // Compared as a whole record, not field by field: every column the loader's queue reads is
    // pinned, including the `s3_uri` and `lsn_start` a per-field assertion list is free to forget.
    assert_eq!(
        to_ready_row(common::EpochNo(9), &obj, None),
        control::NewManifestFile {
            epoch: common::EpochNo(9),
            source_schema: "public".to_string(),
            source_table: "orders".to_string(),
            s3_uri: "s3://walrus/7/public/orders/000000000000A100-uuid.parquet".to_string(),
            kind: FileKind::Stream,
            row_count: 42,
            lsn_start: "0/100".parse().unwrap(),
            lsn_end: "0/A100".parse().unwrap(),
            schema_version: common::SchemaVersionNo(3),
            // Stream objects never carry a reload_id — only the PR 6.5 chunk exporter sets one.
            reload_id: None,
        }
    );
}
