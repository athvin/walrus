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

// ---- PR 3.5: TRUNCATE — a mirror wipe keyed on the (commit_lsn, lsn) TUPLE, not the scalar. ----

/// A truncate + re-inserts: the mirror is emptied as of the truncate (incl. pre-existing rows from
/// earlier cycles) and holds ONLY the post-truncate rows.
#[test]
fn truncate_then_reinsert_keeps_only_post_truncate_rows() {
    let c = db();
    c.execute("INSERT INTO orders VALUES (99, 'pre-existing')", [])
        .unwrap(); // an earlier-cycle row
    seed(
        &c,
        &[
            (1, 'i', 1, 1, "old"), // before the truncate
            (0, 't', 1, 5, ""),    // TRUNCATE at (1, 5)
            (1, 'i', 2, 6, "new"), // after the truncate (later commit)
        ],
    );
    transform(&c);
    assert_eq!(
        status_of(&c, 1).as_deref(),
        Some("new"),
        "only the post-truncate insert survives"
    );
    assert_eq!(
        status_of(&c, 99),
        None,
        "the wipe removed the pre-existing (earlier-cycle) row"
    );
    assert_eq!(mirror_count(&c), 1);
}

/// The subtlety this PR exists to nail: a SAME-transaction `TRUNCATE; INSERT` shares one commit_lsn;
/// the tuple boundary `(commit_lsn, lsn) > (Ct, Lt)` keeps the inserts (the truncate's lsn is lower).
#[test]
fn same_commit_truncate_then_insert_survives_tuple_boundary() {
    let c = db();
    seed(&c, &[(0, 't', 100, 1, ""), (1, 'i', 100, 2, "kept")]); // shared commit_lsn 100
    transform(&c);
    assert_eq!(
        status_of(&c, 1).as_deref(),
        Some("kept"),
        "post-truncate insert at the SAME commit_lsn survives via the tuple boundary"
    );
    assert_eq!(mirror_count(&c), 1);
}

/// The counterfactual: a SCALAR `commit_lsn > Ct` filter would drop the same-commit inserts.
#[test]
fn scalar_commit_lsn_boundary_would_drop_same_commit_inserts() {
    let c = db();
    seed(&c, &[(0, 't', 100, 1, ""), (1, 'i', 100, 2, "kept")]);
    transform(&c);
    assert_eq!(
        status_of(&c, 1).as_deref(),
        Some("kept"),
        "tuple boundary keeps it (shipped)"
    );

    // A scalar `commit_lsn > '0000000000000064'` (= Ct) would exclude the insert (its commit_lsn == Ct).
    let dropped: i64 = c
        .query_row(
            "SELECT count(*) FROM orders_raw WHERE \"_walrus_op\" <> 't' AND \"_walrus_commit_lsn\" > '0000000000000064'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        dropped, 0,
        "a scalar boundary WOULD drop the same-commit insert — why we use the tuple"
    );
}

/// A bare TRUNCATE never stalls: the max-commit-lsn scan (which Phase B advances `transformed_lsn` to)
/// includes the `t` row, and `latest_truncate` resolves it.
#[test]
fn transformed_lsn_advances_past_a_truncate_only_tail() {
    let c = db();
    seed(&c, &[(0, 't', 100, 1, "")]); // ONLY a truncate
    transform(&c);
    let max_hex: Option<String> = c
        .query_row(
            "SELECT max(\"_walrus_commit_lsn\") FROM orders_raw WHERE \"_walrus_commit_lsn\" > '0000000000000000'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        max_hex.as_deref(),
        Some("0000000000000064"),
        "the max scan sees the truncate → watermark advances"
    );
    let b = TransformSql::from_relation(&orders_rel())
        .latest_truncate(&c, &common::Lsn::ZERO)
        .unwrap();
    assert_eq!(
        b.ct,
        Some("0/64".parse().unwrap()),
        "latest_truncate resolves the bare truncate"
    );
}

/// `<table>_raw` is NEVER truncated — the `t` op stays a logged row (raw-vs-mirror asymmetry).
#[test]
fn raw_retains_the_truncate_op_row() {
    let c = db();
    seed(&c, &[(1, 'i', 1, 1, "x"), (0, 't', 1, 5, "")]);
    transform(&c);
    let t_rows: i64 = c
        .query_row(
            "SELECT count(*) FROM orders_raw WHERE \"_walrus_op\" = 't'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        t_rows, 1,
        "the truncate op row is retained in <table>_raw (raw is never wiped)"
    );
}
