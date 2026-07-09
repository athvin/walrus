# PR 5.5 — Criterion benches: the loader transform + Phase-A append

> **Phase:** 5 — Performance & CI · **Crates touched:** `loader` · **Est. size:** M ·
> **Depends on:** PR 5.4 · **Unlocks:** PR 5.6

The loader's cost centres are pure SQL — the dedup-window + `MERGE` transform, the unchanged-TOAST
back-scan (a correlated subquery per TOAST column per winner), and the `read_parquet` append with
its per-file `DESCRIBE` introspection. Because the transform is hermetic SQL over an in-process
DuckDB (the PR 3.3 property), it benches the same way it unit-tests: seed `<table>_raw`, run the
exact production template, measure. This PR quantifies how the transform scales with tail size and
churn, what the TOAST back-scan really costs, and what Phase-A append throughput looks like —
baselines PR 5.8 must beat.

## Why — learning objectives

By the end of this PR you will have practised:

- **Benchmarking a database, not a function** — seeding cost vs measured cost, why each iteration
  needs identical state (re-seed or snapshot per iteration), and criterion's `iter_batched` for
  setup-heavy benches.
- **Reading DuckDB's own opinion** — `EXPLAIN ANALYZE` on the transform SQL to correlate criterion
  numbers with the operators that dominate (window sort vs merge vs the correlated back-scan).
- **Scaling curves over point numbers** — running N ∈ {10k, 100k, 1M} tail rows and K ∈ {1, 10}
  events/PK to see *shape* (linear? superlinear?) rather than a single magic number.

## Read first

- `crates/loader/src/transform.sql` + `transform.rs` — the exact template under test; note the
  window (`QUALIFY row_number() ...`), the guard tuple, and the TOAST back-scan correlated subquery.
- `crates/loader/tests/transform.rs` — the hermetic in-memory seeding pattern to reuse (this is the
  crown-jewel test from PR 3.3; the bench is the same harness with the clock on).
- `crates/loader/src/duck.rs` — `append_parquet` (the built SQL, the `parquet_columns()` DESCRIBE
  per file) and `ensure_tables` (the raw composite PK whose index maintenance the append pays).
- `docs/walrus-loader.md` §5–§6, §9.2 — what the transform must do and the levers it claims.

## Scope

**In scope**

- `crates/loader/benches/transform.rs` (criterion, `harness = false`), all against in-memory DuckDB:
  - **transform scaling:** seed `<table>_raw` with N ∈ {10_000, 100_000, 1_000_000} un-transformed
    rows across N/K primary keys at K ∈ {1, 10} events/PK (mixed i/u/d); run the production
    transform; `Throughput::Elements(N)`.
  - **TOAST back-scan isolated:** same 100k-row seed twice — zero TOAST sentinels vs sentinels on
    3 text columns for ~30 % of winners; the delta is the back-scan cost.
  - **mirror-size sensitivity:** 100k-row tail merged into an empty mirror vs a 1M-row pre-seeded
    mirror (the `MERGE` join + PK index side).
- `crates/loader/benches/append.rs`:
  - generate a local Parquet fixture once per shape (reuse `pg-to-arrow`'s writer as a dev-dep,
    or check in a tiny generator) — narrow and wide shapes, ~50k rows;
  - bench `append_parquet` from `file://` URIs (no MinIO — this isolates DuckDB ingest + the
    ON CONFLICT composite-PK cost, not network);
  - bench the `parquet_columns()` DESCRIBE alone so the per-file introspection overhead is a
    separate line item.
- Append all baselines + an `EXPLAIN ANALYZE` summary of the 1M-row transform to
  `docs/benchmarks.md` (which operators dominate).

**Explicitly deferred** (do *not* build these here)

- Any transform/append changes → **PR 5.8** (this PR is measurement only).
- S3/MinIO in the loop, end-to-end rates → **PR 5.6**.
- Full-rebuild/compaction benches — the periodic path is deliberately off the hot cadence;
  measure only if 5.6 shows it interfering.

## Files to create / modify

```
crates/loader/Cargo.toml                 # + [dev-dependencies] criterion (+ pg-to-arrow if fixture-gen)
crates/loader/benches/transform.rs       # new
crates/loader/benches/append.rs          # new
docs/benchmarks.md                       # modify — loader baselines + EXPLAIN ANALYZE notes
justfile                                 # (bench recipe from 5.4 already covers --workspace)
```

## Skeleton

```rust
// crates/loader/benches/transform.rs
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

/// Seed an in-memory TableDb's `<table>_raw` with `n` events over n/k PKs (mixed i/u/d),
/// optionally marking `toast_cols` with unchanged-TOAST sentinels on ~30% of winners.
fn seed_raw(n: usize, events_per_pk: usize, toast: ToastMix) -> /* db handle */ { todo!() }

fn bench_transform_scaling(c: &mut Criterion) {
    let mut g = c.benchmark_group("loader/transform");
    g.sample_size(10); // 1M-row iterations are seconds, not micros
    for n in [10_000usize, 100_000, 1_000_000] {
        for k in [1usize, 10] {
            g.throughput(Throughput::Elements(n as u64));
            g.bench_with_input(BenchmarkId::new(format!("k{k}"), n), &(n, k), |b, &(n, k)| {
                b.iter_batched(
                    || seed_raw(n, k, ToastMix::None),
                    |db| { /* run the production TransformSql against db */ todo!() },
                    BatchSize::PerIteration,
                )
            });
        }
    }
}

criterion_group!(benches, bench_transform_scaling /*, bench_toast_backscan, bench_mirror_size */);
criterion_main!(benches);
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Transform benches run the **production** SQL template (via `TransformSql`), not a copy — one
      source of truth, same as the unit tests.
- [ ] Scaling table recorded for N × K grid; the write-up states whether cost is O(tail) as
      designed (`docs/walrus-loader.md` §6.3) and flags any superlinear term.
- [ ] The TOAST pair isolates the back-scan cost as a direct delta, and the number is in
      `docs/benchmarks.md` (this is the go/no-go input for PR 5.8's rewrite).
- [ ] The append benches separate `read_parquet` ingest from the DESCRIBE introspection line item.
- [ ] `EXPLAIN ANALYZE` summary of the 1M transform committed alongside the numbers.
- [ ] No production code changed.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`

## Hints & gotchas

- `iter_batched` with `BatchSize::PerIteration` keeps seeding out of the measurement — but 1M-row
  seeding per iteration is slow; seed once into a template DB and copy (`ATTACH` + `CREATE TABLE
  ... AS SELECT`, or file-copy a seeded on-disk DB into a temp path) if wall time hurts.
- Drop `sample_size` for the big benches (criterion minimum is 10) and expect minutes, not seconds;
  put the 1M grid behind `--bench transform -- --quick`-friendly defaults so the default run stays
  tolerable.
- DuckDB's in-memory vs on-disk behaviour differs (checkpointing, buffer manager). The transform
  unit tests use in-memory; bench both only if you see a surprising gap — otherwise stay in-memory
  and say so in the methodology.
- The back-scan delta must vary **only** the sentinel mix — same rows, same PKs, same LSNs.
- `file://` URIs keep MinIO/httpfs out of the append bench; DuckDB reads local Parquet natively.
  (S3 GET latency belongs to PR 5.6's end-to-end view, not here.)
- Threads: DuckDB defaults to per-connection parallelism — pin `SET threads = 4` (and record it) so
  numbers don't shift with the host machine's core count.

## References

- Design: `docs/walrus-loader.md` §5.2–§5.6 (window, MERGE, back-scan), §6.3 (the O(new events)
  claim this PR audits), §9.2 (levers).
- Prev: [PR 5.4](./pr-5.4-bench-sink-decode-arrow.md) ·
  Next: [PR 5.6](./pr-5.6-e2e-throughput-harness.md) · [Roadmap](../README.md)
