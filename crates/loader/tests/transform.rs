//! Hermetic raw→mirror transform tests (loader §6) — `Connection::open_in_memory()`, **no docker
//! compose, no Postgres, no S3**. They replay every worked case from `walrus-loader.md §6` against the
//! *production* template (`loader::transform`), so test and Phase B (PR 3.4) share one source of truth.

use common::{PgColumn, PgRelation, ReplicaIdentity};
use duckdb::Connection;
use loader::transform::{apply_transform, TransformSql};

fn orders_rel() -> PgRelation {
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

fn lsn(n: u64) -> String {
    format!("{n:016X}")
}

/// Fresh in-memory DB with `orders` (mirror) + `orders_raw` (CDC log, minimal columns the transform
/// reads).
fn db() -> Connection {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status VARCHAR);
         CREATE TABLE orders_raw (id INTEGER, status VARCHAR,
             \"_walrus_op\" VARCHAR, \"_walrus_commit_lsn\" VARCHAR, \"_walrus_lsn\" VARCHAR);",
    )
    .unwrap();
    c
}

/// Seed `orders_raw`: rows of (id, op, commit_lsn n, lsn n, status).
fn seed(c: &Connection, rows: &[(i64, char, u64, u64, &str)]) {
    for (id, op, clsn, l, status) in rows {
        c.execute(
            "INSERT INTO orders_raw VALUES (?, ?, ?, ?, ?)",
            duckdb::params![id, status, op.to_string(), lsn(*clsn), lsn(*l)],
        )
        .unwrap();
    }
}

fn transform(c: &Connection) {
    apply_transform(
        c,
        &TransformSql::from_relation(&orders_rel()),
        &common::Lsn::ZERO,
    )
    .unwrap();
}

fn status_of(c: &Connection, id: i64) -> Option<String> {
    c.query_row("SELECT status FROM orders WHERE id = ?", [id], |r| r.get(0))
        .ok()
}

fn mirror_count(c: &Connection) -> i64 {
    c.query_row("SELECT count(*) FROM orders", [], |r| r.get(0))
        .unwrap()
}

// §6.1 primary + §6.3 set-based across many keys.
#[test]
fn n_keys_insert_delete_insert_each_row_equals_last_insert_and_count_is_n() {
    let c = db();
    let n = 50i64;
    for id in 1..=n {
        seed(
            &c,
            &[
                (id, 'i', 1, 3 * id as u64, "first"),
                (id, 'd', 1, 3 * id as u64 + 1, "first"),
                (id, 'i', 1, 3 * id as u64 + 2, "last"),
            ],
        );
    }
    transform(&c);
    assert_eq!(
        mirror_count(&c),
        n,
        "N keys → N mirror rows, no delete/first-insert survivors"
    );
    for id in 1..=n {
        assert_eq!(
            status_of(&c, id).as_deref(),
            Some("last"),
            "each row = its LAST insert"
        );
    }
}

// §6.2 variant matrix.
#[test]
fn insert_then_delete_is_absent() {
    let c = db();
    seed(&c, &[(42, 'i', 1, 1, "a"), (42, 'd', 1, 2, "a")]);
    transform(&c);
    assert_eq!(status_of(&c, 42), None, "i → d ⇒ absent");
}

#[test]
fn insert_update_delete_is_absent() {
    let c = db();
    seed(
        &c,
        &[
            (42, 'i', 1, 1, "a"),
            (42, 'u', 1, 2, "b"),
            (42, 'd', 1, 3, "b"),
        ],
    );
    transform(&c);
    assert_eq!(status_of(&c, 42), None, "i → u → d ⇒ absent");
}

#[test]
fn phantom_delete_for_never_seen_key_is_noop() {
    let c = db();
    seed(&c, &[(99, 'd', 1, 1, "gone")]);
    transform(&c);
    assert_eq!(
        mirror_count(&c),
        0,
        "a delete for an unseen key (NOT MATCHED AND op='d') is a no-op"
    );
}

#[test]
fn insert_a_delete_insert_b_resolves_to_b() {
    let c = db();
    seed(
        &c,
        &[
            (42, 'i', 1, 1, "A"),
            (42, 'd', 1, 2, "A"),
            (42, 'i', 1, 3, "B"),
        ],
    );
    transform(&c);
    assert_eq!(
        status_of(&c, 42).as_deref(),
        Some("B"),
        "i(A) → d → i(B) ⇒ B (distinct data proves B won)"
    );
}

#[test]
fn delete_then_insert_on_preseeded_key_updates_to_insert() {
    let c = db();
    c.execute("INSERT INTO orders VALUES (7, 'old')", [])
        .unwrap(); // pre-seeded mirror row
    seed(&c, &[(7, 'd', 2, 1, "old"), (7, 'i', 2, 2, "fresh")]);
    transform(&c);
    assert_eq!(
        status_of(&c, 7).as_deref(),
        Some("fresh"),
        "d → i on an existing key ⇒ MATCHED UPDATE"
    );
}

// §5.3 the counterfactual — proving the delete-after-ranking guard is load-bearing.
#[test]
fn deletes_filtered_after_ranking_never_resurrect_a_deleted_key() {
    let c = db();
    seed(&c, &[(42, 'i', 1, 1, "A"), (42, 'd', 1, 2, "A")]);
    transform(&c);
    assert_eq!(
        status_of(&c, 42),
        None,
        "shipped template (filter AFTER ranking) ⇒ deleted key absent"
    );

    // Counterfactual: pre-filtering op='d' BEFORE ranking picks the earlier insert → resurrection.
    let resurrected: Option<String> = c
        .query_row(
            "SELECT status FROM orders_raw WHERE id = 42 AND \"_walrus_op\" <> 'd' \
             QUALIFY row_number() OVER (PARTITION BY id ORDER BY \"_walrus_commit_lsn\" DESC, \"_walrus_lsn\" DESC) = 1",
            [],
            |r| r.get(0),
        )
        .ok();
    assert_eq!(
        resurrected.as_deref(),
        Some("A"),
        "pre-filtering d WOULD resurrect — which is why we don't"
    );
}

// Composite PK: PARTITION BY / ON expand to all key columns.
#[test]
fn composite_pk_partition_and_join_expand_to_all_key_columns() {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch(
        "CREATE TABLE kv (k1 INTEGER, k2 INTEGER, val VARCHAR, PRIMARY KEY (k1, k2));
         CREATE TABLE kv_raw (k1 INTEGER, k2 INTEGER, val VARCHAR,
             \"_walrus_op\" VARCHAR, \"_walrus_commit_lsn\" VARCHAR, \"_walrus_lsn\" VARCHAR);",
    )
    .unwrap();
    let rel = PgRelation {
        oid: 43,
        schema: "public".into(),
        name: "kv".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            PgColumn {
                name: "k1".into(),
                type_oid: 23,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "k2".into(),
                type_oid: 23,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "val".into(),
                type_oid: 25,
                type_modifier: -1,
                is_key: false,
            },
        ],
    };
    // Two distinct composite keys; (1,1) churns i→d→i, (1,2) is a lone insert — they must NOT collide.
    for (k1, k2, op, l, val) in [
        (1, 1, "i", 1u64, "first"),
        (1, 1, "d", 2, "first"),
        (1, 1, "i", 3, "last"),
        (1, 2, "i", 1, "other"),
    ] {
        c.execute(
            "INSERT INTO kv_raw VALUES (?, ?, ?, ?, ?, ?)",
            duckdb::params![k1, k2, val, op, lsn(1), lsn(l)],
        )
        .unwrap();
    }
    apply_transform(&c, &TransformSql::from_relation(&rel), &common::Lsn::ZERO).unwrap();

    let n: i64 = c
        .query_row("SELECT count(*) FROM kv", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "two distinct composite keys survive");
    let v11: String = c
        .query_row("SELECT val FROM kv WHERE k1=1 AND k2=1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        v11, "last",
        "(1,1) churn resolves to its last insert — partition keyed on BOTH columns"
    );
    let v12: String = c
        .query_row("SELECT val FROM kv WHERE k1=1 AND k2=2", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v12, "other", "(1,2) is independent of (1,1)");
}
