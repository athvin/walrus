use super::*;

/// The architecture.md §1.4 example block, comment-free (a real JSON document).
const DOCS_EXAMPLE: &str = r#"{
        "op": "u",
        "lsn": "00000000019A2B3C",
        "commit_lsn": "0000000001B4C000",
        "commit_ts": "2026-07-04T12:00:00Z",
        "xid": 918273,
        "epoch": 7,
        "batch_id": "3f2a0000-0000-0000-0000-000000000001",
        "schema_version": 12,
        "source_schema": "public",
        "source_table": "orders",
        "kind": "stream",
        "unchanged_toast": ["blob_col"],
        "sink_instance": "walrus-pg-sink-0",
        "sink_processed_at": "2026-07-04T12:00:00.123Z"
    }"#;

#[test]
fn op_serializes_as_single_char() {
    assert_eq!(serde_json::to_string(&Op::Insert).unwrap(), "\"i\"");
    assert_eq!(serde_json::to_string(&Op::Update).unwrap(), "\"u\"");
    assert_eq!(serde_json::to_string(&Op::Delete).unwrap(), "\"d\"");
    assert_eq!(serde_json::to_string(&Op::Truncate).unwrap(), "\"t\"");
    assert_eq!(serde_json::from_str::<Op>("\"d\"").unwrap(), Op::Delete);
}

#[test]
fn kind_serializes_lowercase() {
    assert_eq!(
        serde_json::to_string(&Kind::Snapshot).unwrap(),
        "\"snapshot\""
    );
    assert_eq!(serde_json::to_string(&Kind::Stream).unwrap(), "\"stream\"");
}

#[test]
fn meta_round_trips_exact_keys() {
    let meta: SinkMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    assert_eq!(meta.op, Op::Update);
    assert_eq!(meta.kind, Kind::Stream);
    assert_eq!(meta.epoch, 7);
    assert_eq!(meta.xid, 918273);
    assert_eq!(meta.unchanged_toast, vec!["blob_col".to_string()]);

    // Re-serialize and confirm every key/value matches the docs block (order-independent).
    let reserialized: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
    let expected: serde_json::Value = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    assert_eq!(reserialized, expected);

    // And the round-trip is the identity on the struct itself.
    let again: SinkMeta = serde_json::from_value(reserialized).unwrap();
    assert_eq!(again, meta);
}

#[test]
fn op_and_lsn_keys_serialize_as_documented() {
    let meta: SinkMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
    assert_eq!(v["op"], "u");
    // Lsn fields render as zero-padded 16-hex (reusing the PR 0.3 newtype).
    assert_eq!(v["lsn"], "00000000019A2B3C");
    assert_eq!(v["commit_lsn"], "0000000001B4C000");
}

#[test]
fn timestamps_always_render_with_z_suffix() {
    let meta: SinkMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
    assert_eq!(v["commit_ts"], "2026-07-04T12:00:00Z");
    assert_eq!(v["sink_processed_at"], "2026-07-04T12:00:00.123Z");
    assert!(v["commit_ts"].as_str().unwrap().ends_with('Z'));
    assert!(v["sink_processed_at"].as_str().unwrap().ends_with('Z'));

    // `now()` also renders with a Z suffix.
    assert!(serde_json::to_string(&UtcTimestamp::now())
        .unwrap()
        .ends_with("Z\""));
}

#[test]
fn non_utc_timestamp_is_rejected() {
    // A numeric offset is refused rather than silently converted to UTC.
    assert!(UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00+02:00").is_err());
    assert!(UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00-05:00").is_err());
    assert!(UtcTimestamp::parse_rfc3339("not a timestamp").is_err());
    // The UTC `Z` form is accepted.
    assert!(UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00Z").is_ok());
}

#[test]
fn deserializes_the_docs_example_block() {
    // The whole §1.4 block parses into a fully-populated SinkMeta.
    let meta: SinkMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    assert_eq!(meta.lsn, Lsn::new(0x19A2B3C));
    assert_eq!(meta.commit_lsn, Lsn::new(0x1B4C000));
    assert_eq!(meta.source_schema, "public");
    assert_eq!(meta.source_table, "orders");
    assert_eq!(meta.batch_id, "3f2a0000-0000-0000-0000-000000000001");
    assert_eq!(meta.schema_version, 12);
    assert_eq!(meta.sink_instance, "walrus-pg-sink-0");
}

#[test]
fn amortized_meta_matches_full() {
    // The amortized `{const,row}` splice (PR 5.7) must be byte-equivalent (key order aside) to
    // `serde_json::to_string(SinkMeta)` — with AND without unchanged-TOAST columns.
    let base: SinkMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    for toast in [vec!["blob_col".to_string()], Vec::new()] {
        let meta = SinkMeta {
            unchanged_toast: toast,
            ..base.clone()
        };
        let mut buf = String::from("{");
        buf.push_str(&meta.const_json_inner().unwrap());
        buf.push(',');
        meta.write_row_json_inner(&mut buf).unwrap();
        buf.push('}');

        let amortized: serde_json::Value = serde_json::from_str(&buf).unwrap();
        let full: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
        assert_eq!(
            amortized, full,
            "amortized meta ≠ full for unchanged_toast={:?}",
            meta.unchanged_toast
        );
    }
}

#[test]
fn pg_epoch_zero_is_y2k() {
    // pgoutput µs=0 is the Postgres epoch, 2000-01-01T00:00:00Z — NOT the Unix epoch.
    let ts = UtcTimestamp::from_pg_micros(0).unwrap();
    assert_eq!(
        serde_json::to_string(&ts).unwrap(),
        "\"2000-01-01T00:00:00Z\""
    );
}

#[test]
fn negative_micros_pre_y2k() {
    // One second before the Postgres epoch.
    let ts = UtcTimestamp::from_pg_micros(-1_000_000).unwrap();
    assert_eq!(
        serde_json::to_string(&ts).unwrap(),
        "\"1999-12-31T23:59:59Z\""
    );
}

#[test]
fn round_trips_a_known_commit_ts() {
    // The µs the sink would receive for a real commit time, reconstructed back to the same instant.
    let want = UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00.123Z").unwrap();
    let pg_micros = want.0.as_microsecond() - 946_684_800_000_000;
    assert_eq!(UtcTimestamp::from_pg_micros(pg_micros).unwrap(), want);
}

#[test]
fn overflow_is_an_error_not_a_panic() {
    assert!(UtcTimestamp::from_pg_micros(i64::MAX).is_err());
}
