//! Hermetic raw→mirror transform tests (loader §6) — `Connection::open_in_memory()`, **no docker
//! compose, no Postgres, no S3**. They replay every worked case from `walrus-loader.md §6` against the
//! *production* template (`loader::transform`), so test and Phase B (PR 3.4) share one source of truth.

use common::{PgColumn, PgRelation, ReplicaIdentity};
use duckdb::Connection;
use loader::duck::TableDb;
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

/// Fresh in-memory DB with `orders` (mirror, incl. the hidden `_applied_*` guard columns — PR 3.7) +
/// `orders_raw` (CDC log, minimal columns the transform reads).
fn db() -> Connection {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status VARCHAR,
             \"_applied_commit_lsn\" VARCHAR DEFAULT '0000000000000000',
             \"_applied_lsn\" VARCHAR DEFAULT '0000000000000000');
         CREATE TABLE orders_raw (id INTEGER, status VARCHAR, walrus_pg_sink_meta VARCHAR,
             \"_walrus_op\" VARCHAR, \"_walrus_commit_lsn\" VARCHAR, \"_walrus_lsn\" VARCHAR);",
    )
    .unwrap();
    c
}

/// Pre-seed a mirror row with an EXPLICIT applied tuple `(ac, al)` — the guard's high-water mark for the
/// per-PK max-applied straddle tests.
fn seed_mirror(c: &Connection, id: i64, status: &str, ac: u64, al: u64) {
    c.execute(
        "INSERT INTO orders (id, status, \"_applied_commit_lsn\", \"_applied_lsn\") VALUES (?, ?, ?, ?)",
        duckdb::params![id, status, lsn(ac), lsn(al)],
    )
    .unwrap();
}

/// The hidden guard tuple currently stamped on a mirror row.
fn applied_of(c: &Connection, id: i64) -> Option<(String, String)> {
    c.query_row(
        "SELECT \"_applied_commit_lsn\", \"_applied_lsn\" FROM orders WHERE id = ?",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .ok()
}

/// Seed `orders_raw`: rows of (id, op, commit_lsn n, lsn n, status).
fn seed(c: &Connection, rows: &[(i64, char, u64, u64, &str)]) {
    for (id, op, clsn, l, status) in rows {
        c.execute(
            "INSERT INTO orders_raw VALUES (?, ?, '{}', ?, ?, ?)",
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
    c.execute("INSERT INTO orders (id, status) VALUES (7, 'old')", [])
        .unwrap(); // pre-seeded mirror row (applied tuple defaults to the low sentinel 0/0)
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
        "CREATE TABLE kv (k1 INTEGER, k2 INTEGER, val VARCHAR,
             \"_applied_commit_lsn\" VARCHAR DEFAULT '0000000000000000',
             \"_applied_lsn\" VARCHAR DEFAULT '0000000000000000', PRIMARY KEY (k1, k2));
         CREATE TABLE kv_raw (k1 INTEGER, k2 INTEGER, val VARCHAR, walrus_pg_sink_meta VARCHAR,
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
            "INSERT INTO kv_raw VALUES (?, ?, ?, '{}', ?, ?, ?)",
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
    c.execute(
        "INSERT INTO orders (id, status) VALUES (99, 'pre-existing')",
        [],
    )
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

// ---- PR 3.6: unchanged-TOAST resolution — the raw back-scan (§5.6). ----

fn docs_rel() -> PgRelation {
    let col = |n: &str, oid: u32, k: bool| PgColumn {
        name: n.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key: k,
    };
    PgRelation {
        oid: 44,
        schema: "public".into(),
        name: "docs".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", 23, true),
            col("big", 25, false),
            col("note", 25, false),
        ],
    }
}

fn docs_db() -> Connection {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, big VARCHAR, note VARCHAR,
             \"_applied_commit_lsn\" VARCHAR DEFAULT '0000000000000000',
             \"_applied_lsn\" VARCHAR DEFAULT '0000000000000000');
         CREATE TABLE docs_raw (id INTEGER, big VARCHAR, note VARCHAR, walrus_pg_sink_meta VARCHAR,
             \"_walrus_op\" VARCHAR, \"_walrus_commit_lsn\" VARCHAR, \"_walrus_lsn\" VARCHAR);",
    )
    .unwrap();
    c
}

/// `toast` is the JSON array text of the unchanged_toast list, e.g. `[]` or `["big"]`.
#[allow(clippy::too_many_arguments)]
fn docs_seed(
    c: &Connection,
    id: i64,
    big: Option<&str>,
    note: &str,
    op: char,
    clsn: u64,
    l: u64,
    toast: &str,
) {
    let meta = format!("{{\"unchanged_toast\":{toast}}}");
    c.execute(
        "INSERT INTO docs_raw VALUES (?, ?, ?, ?, ?, ?, ?)",
        duckdb::params![id, big, note, meta, op.to_string(), lsn(clsn), lsn(l)],
    )
    .unwrap();
}

fn docs_transform(c: &Connection) {
    apply_transform(
        c,
        &TransformSql::from_relation(&docs_rel()),
        &common::Lsn::ZERO,
    )
    .unwrap();
}

/// §5.6 worked case: INSERT big='X' @100 then UPDATE big=<sentinel> @200 for the SAME pk, mirror empty.
/// The mirror must end big='X' — resolved by back-scanning <table>_raw, NOT NULL (a mirror-only lookup
/// has no row yet in this same-batch case).
#[test]
fn same_batch_unchanged_toast_carries_forward_the_prior_value() {
    let c = docs_db();
    docs_seed(&c, 1, Some("X"), "n1", 'i', 100, 1, "[]"); // sets big='X'
    docs_seed(&c, 1, None, "n2", 'u', 100, 2, "[\"big\"]"); // unchanged-TOAST: big absent
    docs_transform(&c);
    let big: Option<String> = c
        .query_row("SELECT big FROM docs WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        big.as_deref(),
        Some("X"),
        "same-batch unchanged-TOAST carries forward the prior value"
    );
}

/// When raw has no non-sentinel value for the column, resolution falls back to the CURRENT mirror value.
#[test]
fn unchanged_toast_falls_back_to_current_mirror_when_raw_has_none() {
    let c = docs_db();
    c.execute(
        "INSERT INTO docs (id, big, note) VALUES (1, 'Y', 'old')",
        [],
    )
    .unwrap(); // pre-existing mirror row
    docs_seed(&c, 1, None, "n2", 'u', 100, 1, "[\"big\"]"); // only a sentinel in raw
    docs_transform(&c);
    let big: Option<String> = c
        .query_row("SELECT big FROM docs WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        big.as_deref(),
        Some("Y"),
        "fallback to the current mirror value, never NULL"
    );
}

/// A real SQL NULL (empty unchanged_toast list) is NEVER treated as unchanged-TOAST — it stays NULL.
#[test]
fn real_null_is_not_treated_as_unchanged_toast() {
    let c = docs_db();
    docs_seed(&c, 1, None, "n1", 'i', 100, 1, "[]"); // big is a REAL null (not listed)
    docs_transform(&c);
    let (cnt, big): (i64, Option<String>) = c
        .query_row(
            "SELECT count(*), any_value(big) FROM docs WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(cnt, 1, "the row exists");
    assert_eq!(big, None, "a real NULL stays NULL — not back-scanned");
}

/// A column NOT in the winner's unchanged_toast list passes through the winner's value untouched, even
/// when a sibling column IS resolved.
#[test]
fn non_toast_columns_pass_through_untouched() {
    let c = docs_db();
    docs_seed(&c, 1, Some("X"), "oldnote", 'i', 100, 1, "[]");
    docs_seed(&c, 1, None, "newnote", 'u', 100, 2, "[\"big\"]"); // big resolved; note passes through
    docs_transform(&c);
    let (big, note): (Option<String>, String) = c
        .query_row("SELECT big, note FROM docs WHERE id = 1", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(big.as_deref(), Some("X"), "big resolved via back-scan");
    assert_eq!(
        note, "newnote",
        "note (not listed) passes through the winner's value, not back-scanned"
    );
}

// ---- PR 3.7: the per-PK max-applied `(commit_lsn, lsn)` guard (§7, ⚠ extends architecture.md). The
// guarded MERGE + the relaxed `>=` window close the two straddle faces without losing idempotency. ----

/// Break face B — a stale delete + re-insert straddling the watermark (older than what last shaped the
/// mirror row) must NOT overwrite or resurrect it: the per-PK guard makes the stale winner a no-op.
#[test]
fn stale_delete_reinsert_across_watermark_does_not_resurrect() {
    let c = db();
    // The mirror row was last shaped by a NEWER tuple (200, 9).
    seed_mirror(&c, 1, "current", 200, 9);
    // A stale churn arriving late in the tail: delete then re-insert, both at an OLDER tuple.
    seed(
        &c,
        &[
            (1, 'd', 100, 1, "current"),
            (1, 'i', 100, 2, "stale-reinsert"),
        ],
    );
    transform(&c);
    assert_eq!(
        status_of(&c, 1).as_deref(),
        Some("current"),
        "the stale delete+reinsert is a no-op — the newer applied tuple wins"
    );
    assert_eq!(mirror_count(&c), 1);
    assert_eq!(
        applied_of(&c, 1),
        Some((lsn(200), lsn(9))),
        "the guard tuple is untouched by the rejected stale winner"
    );
}

/// Break face A — a snapshot row whose `commit_lsn == transformed_lsn` (all snapshot rows carry
/// `consistent_point`) is re-examined by the relaxed `>=` low bound and applied, never silently dropped.
#[test]
fn equal_commit_lsn_snapshot_row_is_still_applied() {
    let c = db();
    // transformed_lsn == 100; the snapshot row's commit_lsn is ALSO 100 (the equal-commit_lsn straddle).
    let after: common::Lsn = "0/64".parse().unwrap();
    assert_eq!(
        after.to_string(),
        lsn(100),
        "sanity: after == commit_lsn 100"
    );
    seed(&c, &[(1, 'i', 100, 1, "snap")]);

    // Counterfactual: a strict `commit_lsn > after` window would see NOTHING (the row is AT the bound).
    let strict: i64 = c
        .query_row(
            "SELECT count(*) FROM orders_raw WHERE \"_walrus_commit_lsn\" > '0000000000000064'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        strict, 0,
        "a strict `>` bound would exclude the snapshot row"
    );

    apply_transform(&c, &TransformSql::from_relation(&orders_rel()), &after).unwrap();
    assert_eq!(
        status_of(&c, 1).as_deref(),
        Some("snap"),
        "the `>=` bound re-examines the equal-commit_lsn snapshot row and the guard applies it"
    );
}

/// The guard applies a genuinely-newer winner and rejects a genuinely-stale one, decided by the FULL
/// `(commit_lsn, lsn)` tuple — proven side-by-side on two independent keys in one transform.
#[test]
fn guard_applies_newer_and_rejects_stale_by_tuple() {
    let c = db();
    seed_mirror(&c, 1, "old1", 100, 5); // key 1: applied at (100, 5)
    seed_mirror(&c, 2, "old2", 100, 5); // key 2: applied at (100, 5)
    seed(
        &c,
        &[
            (1, 'u', 50, 9, "stale"), // older commit_lsn (50 < 100) despite a higher lsn — tuple loses
            (2, 'u', 200, 1, "fresh"), // newer commit_lsn (200 > 100) despite a lower lsn — tuple wins
        ],
    );
    transform(&c);
    assert_eq!(
        status_of(&c, 1).as_deref(),
        Some("old1"),
        "stale winner rejected — the tuple compares commit_lsn FIRST"
    );
    assert_eq!(
        applied_of(&c, 1),
        Some((lsn(100), lsn(5))),
        "guard tuple unchanged"
    );
    assert_eq!(
        status_of(&c, 2).as_deref(),
        Some("fresh"),
        "newer winner applied"
    );
    assert_eq!(
        applied_of(&c, 2),
        Some((lsn(200), lsn(1))),
        "guard tuple advanced to the applied winner"
    );
}

/// The hidden `_applied_*` guard columns live on the mirror table but NEVER appear in the user-facing
/// `<table>_current` projection (DoD §7). Exercises the production `ensure_tables` schema.
#[test]
fn applied_columns_are_hidden_from_user_projections() {
    let db = TableDb::open(std::path::Path::new(":memory:")).unwrap();
    db.ensure_tables(&orders_rel(), 1).unwrap();
    let conn = db.conn();

    let mirror_cols = columns_of(conn, "orders");
    assert!(
        mirror_cols.iter().any(|c| c == "_applied_commit_lsn")
            && mirror_cols.iter().any(|c| c == "_applied_lsn"),
        "the mirror table itself carries the guard columns: {mirror_cols:?}"
    );

    let view_cols = columns_of(conn, "orders_current");
    assert_eq!(
        view_cols,
        vec!["id".to_string(), "status".to_string()],
        "the user-facing view exposes ONLY the source columns"
    );
    assert!(
        !view_cols.iter().any(|c| c.starts_with("_applied")),
        "the guard columns never leak into a user projection"
    );
}

/// Column names of a table/view, in ordinal order.
fn columns_of(conn: &Connection, name: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
        )
        .unwrap();
    let rows = stmt.query_map([name], |r| r.get::<_, String>(0)).unwrap();
    rows.map(|r| r.unwrap()).collect()
}
