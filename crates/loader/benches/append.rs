//! PR 5.5 — criterion micro-benches for the loader's Phase-A append (`TableDb::append_parquet`).
//!
//! Generates a local Parquet fixture with the sink's own Arrow→Parquet writer, then benches
//! `append_parquet` from a `file` path (no MinIO/httpfs — this isolates DuckDB ingest + the
//! `ON CONFLICT` composite-PK cost, not S3 latency). A second bench times the per-file `DESCRIBE`
//! introspection alone, so its overhead is a separate line item. No production code is touched.
//!
//! Run: `cargo bench -p loader --bench append` (or `just bench`).

use common::{
    Kind, Lsn, Op, PgColumn, PgRelation, ReplicaIdentity, SinkMeta, TupleValue, UtcTimestamp,
};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use loader::duck::TableDb;
use pg_to_arrow::{write_parquet_bytes, BatchBuilder};
use std::hint::black_box;
use std::io::Write;
use std::path::Path;

const ROWS: usize = 50_000;

fn col(name: &str, oid: u32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_oid: oid,
        type_modifier: -1,
        is_key,
    }
}

/// `id int4 PK, a int4, b text` — a small row.
fn narrow_rel() -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            col("id", 23, true),
            col("a", 23, false),
            col("b", 25, false),
        ],
    }
}

/// `id int4 PK + 29 text cols` — a wide row.
fn wide_rel() -> PgRelation {
    let mut columns = vec![col("id", 23, true)];
    columns.extend((0..29).map(|i| col(&format!("c{i}"), 25, false)));
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

/// One row's cell values for `rel` at index `i` (id distinct so the composite raw PK never collides).
fn row_values(rel: &PgRelation, i: usize) -> Vec<TupleValue> {
    rel.columns
        .iter()
        .map(|c| {
            let v = match c.type_oid {
                23 if c.is_key => i.to_string(),
                23 => (i as i64 * 2).to_string(),
                _ => format!("{}_{i}", c.name),
            };
            TupleValue::Text(v)
        })
        .collect()
}

/// A distinct-per-row `SinkMeta` (varying `lsn`) so `append_parquet` extracts a unique `_walrus_lsn`.
fn meta(i: usize) -> SinkMeta {
    SinkMeta {
        op: Op::Insert,
        lsn: Lsn::new(i as u64 + 1),
        commit_lsn: Lsn::new(i as u64 + 1),
        commit_ts: UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00Z").unwrap(),
        xid: 1,
        epoch: 7,
        batch_id: "3f2a0000-0000-0000-0000-000000000001".to_string(),
        schema_version: 1,
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        kind: Kind::Stream,
        unchanged_toast: vec![],
        sink_instance: "walrus-pg-sink-0".to_string(),
        sink_processed_at: UtcTimestamp::parse_rfc3339("2026-07-04T12:00:00.123Z").unwrap(),
    }
}

/// Build a `ROWS`-row Parquet fixture for `rel` with the sink's writer, into a temp file.
fn gen_parquet(rel: &PgRelation) -> tempfile::NamedTempFile {
    let mut bb = BatchBuilder::new(rel).unwrap();
    for i in 0..ROWS {
        bb.append_row(&row_values(rel, i), &meta(i)).unwrap();
    }
    let bytes = write_parquet_bytes(&bb.finish().unwrap()).unwrap();
    let mut f = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    f.write_all(&bytes).unwrap();
    f.flush().unwrap();
    f
}

fn bench_append(c: &mut Criterion) {
    let mut g = c.benchmark_group("loader/append_parquet");
    for (name, rel) in [("narrow", narrow_rel()), ("wide", wide_rel())] {
        let file = gen_parquet(&rel);
        let uri = file.path().to_string_lossy().into_owned();
        g.throughput(Throughput::Elements(ROWS as u64));
        g.bench_with_input(BenchmarkId::from_parameter(name), &rel, |b, rel| {
            b.iter_batched(
                || {
                    let db = TableDb::open(Path::new(":memory:")).unwrap();
                    db.ensure_tables(rel, 1).unwrap();
                    db
                },
                |db| black_box(db.append_parquet("orders", &uri, None).unwrap()),
                BatchSize::PerIteration,
            );
        });
    }
    g.finish();
}

/// The per-file `DESCRIBE SELECT * FROM read_parquet(...)` introspection alone — the same SQL
/// `append_parquet` runs internally to map columns by name.
fn bench_describe(c: &mut Criterion) {
    let mut g = c.benchmark_group("loader/parquet_describe");
    for (name, rel) in [("narrow", narrow_rel()), ("wide", wide_rel())] {
        let file = gen_parquet(&rel);
        let uri = file.path().to_string_lossy().into_owned();
        let sql = format!("DESCRIBE SELECT * FROM read_parquet('{uri}')");
        g.bench_with_input(BenchmarkId::from_parameter(name), &sql, |b, sql| {
            b.iter_batched(
                || TableDb::open(Path::new(":memory:")).unwrap(),
                |db| {
                    let mut stmt = db.conn().prepare(sql).unwrap();
                    let cols: Vec<String> = stmt
                        .query_map([], |r| r.get::<_, String>(0))
                        .unwrap()
                        .map(Result::unwrap)
                        .collect();
                    black_box(cols);
                },
                BatchSize::SmallInput,
            );
        });
    }
    g.finish();
}

criterion_group!(benches, bench_append, bench_describe);
criterion_main!(benches);
