//! PR 5.5 — criterion micro-benches for the loader's raw→mirror transform.
//!
//! Runs the **production** SQL (`loader::transform::apply_transform` over `TransformSql`) against an
//! in-memory DuckDB seeded exactly the way the PR 3.3 crown-jewel tests seed it — same harness, clock
//! on. Three views: transform scaling over an N×K grid, the unchanged-TOAST back-scan cost isolated
//! as a delta, and mirror-size sensitivity (the MERGE join + PK-index side). No production code is
//! touched — baselines PR 5.8 must beat.
//!
//! Seeding uses one `INSERT … SELECT range(N)` per iteration (individual inserts would dwarf the
//! measured transform at 1M rows). `SET threads = 4` is pinned so numbers don't drift with core count.
//!
//! Run: `cargo bench -p loader --bench transform` (or `just bench`). The 1M grid takes minutes.

use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use duckdb::Connection;
use loader::transform::{apply_transform, TransformSql};

fn col(name: &str, oid: u32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    }
}

/// `orders(id int4 PK, status text)` — the scaling + mirror-size shape.
fn orders_rel() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![col("id", 23, true), col("status", 25, false)],
    }
}

/// `orders(id int4 PK, t1/t2/t3 text)` — the TOAST back-scan shape (3 resolvable columns).
fn toast_rel() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", 23, true),
            col("t1", 25, false),
            col("t2", 25, false),
            col("t3", 25, false),
        ],
    }
}

/// A fresh in-memory DB with the mirror (+ hidden `_applied_*` guard cols) and `<table>_raw`, matching
/// the PR 3.3 test schema. `threads` pinned for reproducibility.
fn db(mirror_cols: &str, raw_cols: &str) -> Connection {
    let c = Connection::open_in_memory().unwrap();
    c.execute_batch(&format!(
        "SET threads = 4;
         CREATE TABLE orders ({mirror_cols},
             \"_applied_commit_lsn\" VARCHAR DEFAULT '0000000000000000',
             \"_applied_lsn\" VARCHAR DEFAULT '0000000000000000');
         CREATE TABLE orders_raw ({raw_cols}, walrus_pg_sink_meta VARCHAR,
             \"_walrus_op\" VARCHAR, \"_walrus_commit_lsn\" VARCHAR, \"_walrus_lsn\" VARCHAR);"
    ))
    .unwrap();
    c
}

fn orders_db() -> Connection {
    db(
        "id INTEGER PRIMARY KEY, status VARCHAR",
        "id INTEGER, status VARCHAR",
    )
}

fn toast_db() -> Connection {
    db(
        "id INTEGER PRIMARY KEY, t1 VARCHAR, t2 VARCHAR, t3 VARCHAR",
        "id INTEGER, t1 VARCHAR, t2 VARCHAR, t3 VARCHAR",
    )
}

/// 16-hex LSN SQL expression from a 0-based row index (offset by 1 so it exceeds the mirror's
/// `'000…0'` default and the Step-3 guard fires).
fn lsn_hex(idx: &str) -> String {
    format!("upper(lpad(to_hex({idx} + 1), 16, '0'))")
}

/// Seed `n` raw events across `n/k` PKs, `k` events/PK, mixed i/d (last-by-`(commit_lsn,lsn)` wins).
fn seed_scaling(c: &Connection, n: usize, k: usize) {
    let npk = (n / k).max(1);
    let lsn = lsn_hex("i");
    c.execute_batch(&format!(
        "INSERT INTO orders_raw
         SELECT (i % {npk}) AS id, 'v' || i AS status, '{{}}',
                CASE WHEN i % 7 = 0 THEN 'd' ELSE 'i' END,
                {lsn}, {lsn}
         FROM range({n}) t(i);"
    ))
    .unwrap();
}

/// Pre-seed the mirror with `m` rows (ids `0..m`, low guard tuple) so the tail MERGE hits UPDATE.
fn seed_mirror(c: &Connection, m: usize) {
    c.execute_batch(&format!(
        "INSERT INTO orders
         SELECT i AS id, 'm' || i, '0000000000000000', '0000000000000000'
         FROM range({m}) t(i);"
    ))
    .unwrap();
}

/// Seed `n` raw rows over `n/2` PKs (K=2: a setter `'i'` then a winner `'u'`). `pct` % of winners
/// carry the unchanged-TOAST sentinel on t1/t2/t3 (its meta lists them) — the *only* thing that
/// varies between the two back-scan benches, so the delta is pure back-scan.
fn seed_toast(c: &Connection, n: usize, pct: u32) {
    let lsn = lsn_hex("i");
    let sentinel = r#"{"unchanged_toast":["t1","t2","t3"]}"#;
    c.execute_batch(&format!(
        "INSERT INTO orders_raw
         SELECT (i // 2) AS id, 'val1_' || (i // 2), 'val2_' || (i // 2), 'val3_' || (i // 2),
                CASE WHEN (i % 2 = 1) AND ((i // 2) % 100 < {pct}) THEN '{sentinel}' ELSE '{{}}' END,
                CASE WHEN i % 2 = 0 THEN 'i' ELSE 'u' END,
                {lsn}, {lsn}
         FROM range({n}) t(i);"
    ))
    .unwrap();
}

fn bench_transform_scaling(c: &mut Criterion) {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel);
    let mut g = c.benchmark_group("loader/transform");
    g.sample_size(10); // 1M-row iterations are seconds, not micros
    for n in [10_000usize, 100_000, 1_000_000] {
        for k in [1usize, 10] {
            g.throughput(Throughput::Elements(n as u64));
            g.bench_with_input(
                BenchmarkId::new(format!("k{k}"), n),
                &(n, k),
                |b, &(n, k)| {
                    b.iter_batched(
                        || {
                            let db = orders_db();
                            seed_scaling(&db, n, k);
                            db
                        },
                        |db| apply_transform(&db, &t, &Lsn::ZERO).unwrap(),
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }
    g.finish();
}

fn bench_toast_backscan(c: &mut Criterion) {
    let rel = toast_rel();
    let t = TransformSql::from_relation(&rel);
    let n = 100_000usize;
    let mut g = c.benchmark_group("loader/toast_backscan");
    g.sample_size(10);
    for (label, pct) in [("no_toast", 0u32), ("toast_30pct", 30)] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(label), &pct, |b, &pct| {
            b.iter_batched(
                || {
                    let db = toast_db();
                    seed_toast(&db, n, pct);
                    db
                },
                |db| apply_transform(&db, &t, &Lsn::ZERO).unwrap(),
                BatchSize::PerIteration,
            );
        });
    }
    g.finish();
}

fn bench_mirror_size(c: &mut Criterion) {
    let rel = orders_rel();
    let t = TransformSql::from_relation(&rel);
    let tail = 100_000usize;
    let mut g = c.benchmark_group("loader/mirror_size");
    g.sample_size(10);
    for (label, mirror) in [("empty_mirror", 0usize), ("mirror_1m", 1_000_000)] {
        g.throughput(Throughput::Elements(tail as u64));
        g.bench_with_input(BenchmarkId::from_parameter(label), &mirror, |b, &mirror| {
            b.iter_batched(
                || {
                    let db = orders_db();
                    if mirror > 0 {
                        seed_mirror(&db, mirror);
                    }
                    seed_scaling(&db, tail, 1);
                    db
                },
                |db| apply_transform(&db, &t, &Lsn::ZERO).unwrap(),
                BatchSize::PerIteration,
            );
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    bench_transform_scaling,
    bench_toast_backscan,
    bench_mirror_size
);
criterion_main!(benches);
