use super::*;
use common::{PgColumn, ReplicaIdentity};

fn composite_rel() -> PgRelation {
    let col = |name: &str, is_key: bool| PgColumn {
        name: name.to_string(),
        type_oid: 25,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 1,
        schema: "public".to_string(),
        name: "customers".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("region", true), col("id", true), col("name", false)],
    }
}

#[test]
fn first_chunk_has_no_predicate_and_orders_by_full_pk() {
    let pk = vec!["region".to_string(), "id".to_string()];
    let sql = continuation_sql(&composite_rel(), &pk, None, 1000).unwrap();
    assert_eq!(
        sql,
        "SELECT \"region\"::text, \"id\"::text, \"name\"::text \
             FROM \"public\".\"customers\" AS _src \
             ORDER BY _src.\"region\", _src.\"id\" LIMIT 1000"
    );
}

#[test]
fn continuation_sql_is_row_comparison_for_composite_pk() {
    let cursor = serde_json::json!(["eu", "42"]);
    let pk = vec!["region".to_string(), "id".to_string()];
    let sql = continuation_sql(&composite_rel(), &pk, Some(&cursor), 500).unwrap();
    assert!(
        sql.contains("WHERE (_src.\"region\", _src.\"id\") > ('eu', '42')"),
        "row comparison over the FULL composite key, table-qualified: {sql}"
    );
    assert!(sql.ends_with("ORDER BY _src.\"region\", _src.\"id\" LIMIT 500"));
}

#[test]
fn cursor_literals_are_quote_escaped() {
    let cursor = serde_json::json!(["o'brien"]);
    let rel = PgRelation {
        columns: vec![PgColumn {
            name: "id".into(),
            type_oid: 25,
            type_modifier: -1,
            is_key: true,
        }],
        ..composite_rel()
    };
    let sql = continuation_sql(&rel, &["id".to_string()], Some(&cursor), 10).unwrap();
    assert!(sql.contains("('o''brien')"), "escaped: {sql}");
}

#[test]
fn pagination_follows_pk_index_order_not_attnum_order() {
    // CREATE TABLE t (a int, b int, PRIMARY KEY (b, a)): the btree is (b, a); paging in
    // attnum order (a, b) would force a per-chunk top-N sort on exactly the huge tables
    // reloads target. The pk_cols list carries the INDEX order.
    let pk = vec!["b".to_string(), "a".to_string()];
    let rel = PgRelation {
        columns: vec![
            PgColumn {
                name: "a".into(),
                type_oid: 23,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "b".into(),
                type_oid: 23,
                type_modifier: -1,
                is_key: true,
            },
        ],
        ..composite_rel()
    };
    let cursor = serde_json::json!(["7", "3"]);
    let sql = continuation_sql(&rel, &pk, Some(&cursor), 100).unwrap();
    assert!(
        sql.contains("WHERE (_src.\"b\", _src.\"a\") > ('7', '3')"),
        "row comparison in INDEX order: {sql}"
    );
    assert!(
        sql.ends_with("ORDER BY _src.\"b\", _src.\"a\" LIMIT 100"),
        "ORDER BY in INDEX order: {sql}"
    );
}

#[test]
fn continuation_sql_quotes_embedded_double_quotes_in_every_identifier() {
    let rel = PgRelation {
        schema: "odd\"schema".into(),
        name: "customer\"table".into(),
        columns: vec![PgColumn {
            name: "customer\"id".into(),
            type_oid: 23,
            type_modifier: -1,
            is_key: true,
        }],
        ..composite_rel()
    };

    let sql = continuation_sql(&rel, &["customer\"id".to_string()], None, 10).unwrap();
    assert_eq!(
        sql,
        "SELECT \"customer\"\"id\"::text FROM \"odd\"\"schema\".\"customer\"\"table\" AS _src \
         ORDER BY _src.\"customer\"\"id\" LIMIT 10"
    );
}

#[test]
fn full_replica_identity_can_page_by_a_narrower_primary_key() {
    let mut rel = composite_rel();
    rel.replica_identity = ReplicaIdentity::Full;
    for column in &mut rel.columns {
        column.is_key = true;
    }

    validate_export_keys(&rel, &["id".to_string()]).unwrap();
}

#[test]
fn custom_replica_identity_can_differ_from_primary_key_cursor() {
    let mut rel = composite_rel();
    rel.replica_identity = ReplicaIdentity::Index;
    for column in &mut rel.columns {
        column.is_key = column.name == "name";
    }

    validate_export_keys(&rel, &["region".to_string(), "id".to_string()]).unwrap();
}

#[test]
fn primary_key_cursor_must_exist_in_the_frozen_shape() {
    let error = validate_export_keys(&composite_rel(), &["missing".to_string()]).unwrap_err();
    assert!(error.to_string().contains("absent from frozen relation"));
}

#[test]
fn schema_bump_between_chunks_interrupts_with_new_version() {
    // A structural bump past the frozen version restarts; equal (metadata-only DDL never bumps
    // the registry) and a stale backwards read do not.
    assert_eq!(
        version_changed(common::SchemaVersionNo(1), Some(common::SchemaVersionNo(2))),
        Some(common::SchemaVersionNo(2)),
        "1 → 2 restarts"
    );
    assert_eq!(
        version_changed(common::SchemaVersionNo(1), Some(common::SchemaVersionNo(1))),
        None,
        "metadata-only: no restart"
    );
    assert_eq!(
        version_changed(common::SchemaVersionNo(2), Some(common::SchemaVersionNo(1))),
        None,
        "never restart backwards"
    );
    assert_eq!(
        version_changed(common::SchemaVersionNo(1), None),
        None,
        "no registry row: no restart"
    );
}

#[test]
fn durable_end_recovery_requires_exact_f_h_schema_and_request_identity() {
    let rel = composite_rel();
    let reload_id = ReloadId(91);
    let epoch = EpochNo(7);
    let schema_version = SchemaVersionNo(3);
    let f: Lsn = "0/100".parse().unwrap();
    let h: Lsn = "0/200".parse().unwrap();
    let request_id = Uuid::from_u128(0x1234);
    let row = control::ReloadRow {
        reload_id,
        epoch,
        source_schema: rel.schema.clone(),
        source_table: rel.name.clone(),
        flavor: control::ReloadFlavor::Reload,
        source_request_id: Some(request_id),
        parent_request_id: None,
        scope: control::ReloadScope::Table,
        status: control::ReloadStatus::Exporting,
        chunk_no: 1,
        cursor_pk: Some(serde_json::json!(["42"])),
        start_lsn: Some(f),
        first_lsn: Some(f),
        final_lsn: None,
        schema_version: Some(schema_version),
        restart_count: 0,
        lease_holder: Some("sink-a".to_string()),
        error: None,
    };
    let markers = vec![
        control::ReloadMarkerRow {
            reload_id,
            kind: control::ReloadMarkerKind::Baseline,
            lsn: f,
            schema_version,
        },
        control::ReloadMarkerRow {
            reload_id,
            kind: control::ReloadMarkerKind::End,
            lsn: h,
            schema_version,
        },
    ];

    assert_eq!(
        validate_durable_end(&row, &markers, epoch, &rel, schema_version, f, request_id,).unwrap(),
        Some(h)
    );
    assert!(
        validate_durable_end(
            &row,
            &markers,
            epoch,
            &rel,
            schema_version,
            f,
            Uuid::from_u128(0x9999),
        )
        .unwrap_err()
        .to_string()
        .contains("request identity")
    );

    let mut missing_namespace = row.clone();
    missing_namespace.source_request_id = None;
    assert!(
        validate_durable_end(
            &missing_namespace,
            &markers,
            epoch,
            &rel,
            schema_version,
            f,
            request_id,
        )
        .unwrap_err()
        .to_string()
        .contains("no source-fence request namespace")
    );

    let mut wrong_baseline = markers.clone();
    wrong_baseline[0].lsn = "0/101".parse().unwrap();
    assert!(
        validate_durable_end(
            &row,
            &wrong_baseline,
            epoch,
            &rel,
            schema_version,
            f,
            request_id,
        )
        .unwrap_err()
        .to_string()
        .contains("baseline marker")
    );
}

#[test]
fn restart_cap_zero_means_first_ddl_fails_the_reload() {
    // The controller consults the same pure cap check the control-layer restart uses.
    assert!(
        control::reload::restart_would_exceed_cap(0, 0),
        "cap 0 ⇒ the first mid-export DDL fails the reload"
    );
    assert!(
        !control::reload::restart_would_exceed_cap(0, 3),
        "with headroom the first DDL restarts instead"
    );
}

#[test]
fn short_chunk_means_drained() {
    // The drain rule is pure arithmetic: fewer rows than the cap ⇒ nothing left past them.
    for (rows, cap, drained) in [
        (1000u64, 1000u64, false),
        (999, 1000, true),
        (0, 1000, true),
    ] {
        let outcome = if rows < cap {
            ChunkOutcome::Drained { rows }
        } else {
            ChunkOutcome::Exported { rows }
        };
        assert_eq!(
            matches!(outcome, ChunkOutcome::Drained { .. }),
            drained,
            "{rows}/{cap}"
        );
    }
}
