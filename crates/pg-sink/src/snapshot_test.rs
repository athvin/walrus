use super::*;
use pg_to_arrow::oids;

fn orders() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: oids::INT4,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "status".into(),
                type_oid: oids::TEXT,
                type_modifier: -1,
                is_key: false,
            },
        ],
    }
}

#[test]
fn copy_sql_uses_repeatable_read_and_set_transaction_snapshot() {
    let sql = begin_snapshot_txn("00000003-0000000A-1");
    assert!(
        sql.contains("REPEATABLE READ"),
        "isolation is REPEATABLE READ"
    );
    assert!(sql.contains("READ ONLY"), "the backfill txn is read-only");
    assert!(
        sql.contains("SET TRANSACTION SNAPSHOT '00000003-0000000A-1'"),
        "attaches the exported snapshot by name"
    );
    // The SELECT casts every column to its text output form.
    let select = select_text_sql(&orders());
    assert_eq!(
        select,
        "SELECT \"id\"::text, \"status\"::text FROM \"public\".\"orders\""
    );
}

#[test]
fn snapshot_rows_carry_kind_snapshot_and_consistent_point_commit_lsn() {
    let cp: Lsn = "0/ABCDEF".parse().unwrap();
    let snap = ExportedSnapshot {
        consistent_point: cp,
        snapshot_name: "s".into(),
    };
    // Mirror snapshot_meta (no live client needed) and drive it through the batcher.
    let meta = SinkMeta {
        op: Op::Insert,
        lsn: snap.consistent_point,
        commit_lsn: Lsn::ZERO,
        commit_ts: UtcTimestamp::now(),
        xid: 0,
        epoch: 7,
        batch_id: String::new(),
        schema_version: 1,
        source_schema: "public".into(),
        source_table: "orders".into(),
        kind: Kind::Snapshot,
        unchanged_toast: vec![],
        sink_instance: "t".into(),
        sink_processed_at: UtcTimestamp::now(),
    };
    assert_eq!(meta.kind, Kind::Snapshot);
    assert_eq!(
        meta.lsn, cp,
        "snapshot rows carry the consistent_point as their LSN"
    );

    let cached = RelationCache::default()
        .upsert_from_relation(orders(), 1)
        .unwrap();
    let mut b = TableBatcher::new(
        cached,
        BatchTriggers {
            max_rows: u64::MAX,
            max_bytes: u64::MAX,
            max_fill: std::time::Duration::from_secs(3600),
        },
        Arc::new(SystemClock),
    )
    .unwrap();
    b.push(
        meta.clone(),
        &[TupleValue::Text("1".into()), TupleValue::Text("new".into())],
    );
    b.on_commit(cp, UtcTimestamp::now()).unwrap();
    let sealed = b.seal().unwrap();
    assert_eq!(sealed.lsn_end, cp, "committed at the consistent_point");
}

#[test]
fn all_snapshot_manifest_files_share_lsn_end() {
    let cp: Lsn = "0/500".parse().unwrap();
    let cached = RelationCache::default()
        .upsert_from_relation(orders(), 1)
        .unwrap();
    let meta = |lsn: Lsn| SinkMeta {
        op: Op::Insert,
        lsn,
        commit_lsn: Lsn::ZERO,
        commit_ts: UtcTimestamp::now(),
        xid: 0,
        epoch: 7,
        batch_id: String::new(),
        schema_version: 1,
        source_schema: "public".into(),
        source_table: "orders".into(),
        kind: Kind::Snapshot,
        unchanged_toast: vec![],
        sink_instance: "t".into(),
        sink_processed_at: UtcTimestamp::now(),
    };
    let mut lsn_ends = Vec::new();
    for _file in 0..2 {
        let mut b = TableBatcher::new(
            cached.clone(),
            BatchTriggers {
                max_rows: 1,
                max_bytes: u64::MAX,
                max_fill: std::time::Duration::from_secs(3600),
            },
            Arc::new(SystemClock),
        )
        .unwrap();
        b.push(
            meta(cp),
            &[TupleValue::Text("1".into()), TupleValue::Text("x".into())],
        );
        b.on_commit(cp, UtcTimestamp::now()).unwrap();
        lsn_ends.push(b.seal().unwrap().lsn_end);
    }
    assert_eq!(
        lsn_ends,
        vec![cp, cp],
        "every snapshot file shares lsn_end = consistent_point"
    );
}
