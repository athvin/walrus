use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};
use std::path::Path;

fn orders() -> PgRelation {
    let col = |name: &str, oid: u32, is_key: bool| PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    };
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

fn open_fresh(dir: &Path) -> TableDb {
    let db = TableDb::open(&dir.join("orders.duckdb")).unwrap();
    db.ensure_tables(&orders(), 1).unwrap();
    db
}

#[test]
fn rebuild_wipes_a_stale_generation_and_is_idempotent() {
    let dir = std::env::temp_dir().join("walrus-loader-epoch-rebuild");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = open_fresh(&dir);

    // A file built for epoch 1 with a row in raw + mirror.
    db.set_built_epoch(1).unwrap();
    db.conn()
        .execute_batch(
            "INSERT INTO orders VALUES (1, 'x', '0', '0'); \
                 INSERT INTO orders_raw (id, status, walrus_pg_sink_meta, _walrus_op, \
                     _walrus_commit_lsn, _walrus_lsn, _walrus_sink_processed_at) \
                 VALUES (1, 'x', '{}', 'Insert', '0', '0', 't');",
        )
        .unwrap();

    // Control epoch bumped to 2 → the file is stale → rebuild wipes it (raw + mirror gone).
    assert!(rebuild_for_new_epoch(&db, "orders", 2).unwrap());
    // Recreate empty (as bootstrap does) and confirm the stale rows are gone.
    db.ensure_tables(&orders(), 1).unwrap();
    db.set_built_epoch(2).unwrap();
    let mirror: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap();
    let raw: i64 = db
        .conn()
        .query_row("SELECT count(*) FROM orders_raw", [], |r| r.get(0))
        .unwrap();
    assert_eq!((mirror, raw), (0, 0), "the retired generation was wiped");

    // Idempotent: already at epoch 2 → no rebuild.
    assert!(!rebuild_for_new_epoch(&db, "orders", 2).unwrap());
}

#[test]
fn fresh_file_is_not_rebuilt() {
    let dir = std::env::temp_dir().join("walrus-loader-epoch-fresh");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = open_fresh(&dir);
    // Never stamped (built_epoch = None) → a fresh bootstrap, never a rebuild.
    assert!(!rebuild_for_new_epoch(&db, "orders", 1).unwrap());
    assert!(!rebuild_for_new_epoch(&db, "orders", 5).unwrap());
}
