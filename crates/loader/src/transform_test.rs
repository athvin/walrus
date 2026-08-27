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
fn a_broken_raw_table_is_an_error_not_an_absent_truncate_boundary() {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    let transform = TransformSql::from_relation(&relation());

    let err = transform.latest_truncate(&conn, Lsn::ZERO).unwrap_err();
    assert!(matches!(err, LoaderError::Duck { .. }), "got {err:?}");
}
