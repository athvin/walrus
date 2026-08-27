use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};

fn relation() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![PgColumn {
            name: "id".into(),
            type_oid: 23,
            type_modifier: -1,
            is_key: true,
        }],
    }
}

#[test]
fn an_all_pk_table_self_assigns_its_key_in_the_matched_update() {
    let transform = TransformSql::from_relation(&relation());

    let sql = transform.render(Lsn::ZERO, None);
    assert!(sql.contains(r#""id" = s."id""#), "got {sql}");
}

#[test]
fn a_column_less_relation_renders_instead_of_panicking() {
    // `CREATE TABLE t ()` is legal in Postgres, so a published relation can arrive with no columns
    // at all — neither key nor non-key. Rendering must still produce a string; DuckDB then rejects
    // the degenerate MERGE as a classified error rather than the apply loop aborting.
    let empty = PgRelation {
        columns: Vec::new(),
        ..relation()
    };
    let transform = TransformSql::from_relation(&empty);

    let sql = transform.render(Lsn::ZERO, None);
    let stamps_only = r#""_applied_commit_lsn" = s."_walrus_commit_lsn""#;
    assert!(sql.contains(stamps_only), "got {sql}");
}

#[test]
fn the_wipe_and_tuple_bound_render_only_for_a_present_boundary() {
    // The truncate boundary is ONE `Option<TruncateBoundary>`: absent renders neither the wipe nor
    // the window predicate; present renders both from the SAME pair, so there is no half-resolved
    // state in between for `render` to second-guess. (`> ('` is unique to the tuple bound — the
    // per-PK guard compares against `t."_applied_*"`, never a quoted literal.)
    let transform = TransformSql::from_relation(&relation());
    let wipe = r#"DELETE FROM "orders";"#;
    let bound = "> ('0000000000000064', '0000000000000065')";

    let absent = transform.render(Lsn::ZERO, None);
    assert!(!absent.contains(wipe), "got {absent}");
    assert!(!absent.contains("> ('"), "got {absent}");

    let boundary = TruncateBoundary {
        ct: Lsn::new(0x64),
        lt: Lsn::new(0x65),
    };
    let present = transform.render(Lsn::ZERO, Some(boundary));
    assert!(present.contains(wipe), "got {present}");
    assert!(present.contains(bound), "got {present}");
}

#[test]
fn a_broken_raw_table_is_an_error_not_an_absent_truncate_boundary() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let transform = TransformSql::from_relation(&relation());

    let err = transform.latest_truncate(&conn, Lsn::ZERO).unwrap_err();
    assert!(matches!(err, LoaderError::Duck { .. }), "got {err:?}");
}
