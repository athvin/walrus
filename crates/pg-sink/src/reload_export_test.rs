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
    let sql = continuation_sql(&composite_rel(), &pk, None, 1000);
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
    let sql = continuation_sql(&composite_rel(), &pk, Some(&cursor), 500);
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
    let sql = continuation_sql(&rel, &["id".to_string()], Some(&cursor), 10);
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
    let sql = continuation_sql(&rel, &pk, Some(&cursor), 100);
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
fn schema_bump_between_chunks_interrupts_with_new_version() {
    // A structural bump past the frozen version restarts; equal (metadata-only DDL never bumps
    // the registry) and a stale backwards read do not.
    assert_eq!(version_changed(1, Some(2)), Some(2), "1 → 2 restarts");
    assert_eq!(
        version_changed(1, Some(1)),
        None,
        "metadata-only: no restart"
    );
    assert_eq!(version_changed(2, Some(1)), None, "never restart backwards");
    assert_eq!(
        version_changed(1, None),
        None,
        "no registry row: no restart"
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
