# PR 5.4 — Criterion micro-benches: pgoutput decode + Arrow batch building

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/85

> **Phase:** 5 — Performance & CI · **Crates touched:** `pg-sink`, `pg-to-arrow`, workspace root ·
> **Est. size:** M · **Depends on:** PR 5.3 · **Unlocks:** PR 5.5

The sink's hot path — decode a pgoutput frame, append the row into Arrow builders — has never been
measured. This PR builds the measuring instruments: criterion benches over the decoder
(`pgoutput::parse_stream` / `parse_tuple`) and over `BatchBuilder::append_row`, fed by synthetic
workloads whose shape we control (narrow vs wide tables, text-heavy rows, Tier-2 fan-out, streamed
frames). It also starts `docs/benchmarks.md` — the living record of methodology, baselines, and
(from PR 5.7 on) before/after deltas. **No production code changes in this PR**: the point is a
trustworthy baseline before anything is "optimized".

## Why — learning objectives

By the end of this PR you will have practised:

- **criterion.rs** — `[[bench]] harness = false` targets, `Criterion::bench_function`,
  `Throughput::Elements`/`Bytes` so results read as rows/s and MB/s, and `black_box` to keep the
  optimizer honest.
- **Benchmarking allocation-bound code** — why per-cell `String` allocation and per-row
  `serde_json::to_string` show up as throughput cliffs, and how to isolate one suspect per bench.
- **Synthetic-workload design** — generating valid pgoutput byte streams programmatically from the
  same message layouts the golden vectors prove (`proto-version.md` §4), so the bench exercises the
  real decoder on realistic bytes.

## Read first

- `crates/pg-sink/src/pgoutput/mod.rs` — `parse_stream`, `parse_one`, `parse_tuple` (the per-cell
  `'t'` branch allocates a `String` per text column: the first suspect).
- `crates/pg-to-arrow/src/batch.rs` — `append_row` (per-cell downcast dispatch) and the per-row
  `serde_json::to_string(meta)` at the meta column append (the second suspect).
- `crates/pg-sink/tests/pgoutput_vectors.rs` — the golden vectors; the bench generator should emit
  the same layouts (reuse the hex-building helpers if they extract cleanly).
- `docs/proto-version.md` §4–§8 — the byte layouts for Insert/Relation/Stream frames.

## Scope

**In scope**

- `criterion` in `[workspace.dependencies]`; dev-dependency + `[[bench]]` entries in `pg-sink` and
  `pg-to-arrow`.
- `crates/pg-sink/benches/decode.rs`:
  - a small frame-generator module (in the bench, or `#[doc(hidden)]` test-support) producing valid
    pgoutput streams: `Begin + N×Insert + Commit` for (a) a narrow 4-column int table, (b) a wide
    30-column mixed table, (c) a text-heavy table (10 × ~200-byte text cols);
  - a streamed variant (`Stream Start/Stop` blocks with per-message xids) at the same row counts;
  - benches for `parse_stream` end-to-end and `parse_tuple` alone, with `Throughput::Elements(rows)`.
- `crates/pg-to-arrow/benches/batch.rs`:
  - `append_row` throughput on the same three table shapes (Tier-1);
  - a Tier-2 fan-out shape (interval + range + timetz columns);
  - **meta-JSON isolated**: one bench that appends rows with the meta serialization and one
    identical bench with a pre-canned constant meta string, so the JSON cost is directly readable;
  - `finish()` included in a separate whole-batch bench (builders → RecordBatch).
- `just bench` recipe (`cargo bench --workspace`).
- `docs/benchmarks.md` — methodology (hardware note, warm-up, criterion defaults), the recorded
  baseline table, and a "how to re-run" section.

**Explicitly deferred** (do *not* build these here)

- Loader benches → **PR 5.5**. End-to-end throughput → **PR 5.6**. Fixes → **PR 5.7**.
- Benches as a CI *gate* — shared runners are too noisy; CI only compile-checks bench targets
  (clippy `--all-targets` already covers them).

## Files to create / modify

```
Cargo.toml                               # + criterion in [workspace.dependencies]
crates/pg-sink/Cargo.toml                # + [dev-dependencies] criterion; [[bench]] decode, harness=false
crates/pg-sink/benches/decode.rs         # new
crates/pg-to-arrow/Cargo.toml            # + criterion; [[bench]] batch, harness=false
crates/pg-to-arrow/benches/batch.rs      # new
justfile                                 # + bench recipe
docs/benchmarks.md                       # new — methodology + baselines
```

## Skeleton

```rust
// crates/pg-sink/benches/decode.rs
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Build a valid pgoutput stream: Begin, then `rows` Inserts for `shape`, then Commit.
/// Layouts per docs/proto-version.md §4 (same shapes the golden vectors prove).
fn synth_stream(shape: &TableShape, rows: usize) -> Vec<Vec<u8>> { todo!() }

fn bench_parse_stream(c: &mut Criterion) {
    let mut g = c.benchmark_group("pgoutput/parse_stream");
    for shape in [TableShape::NarrowInt4, TableShape::Wide30, TableShape::TextHeavy] {
        let frames = synth_stream(&shape, 10_000);
        g.throughput(Throughput::Elements(10_000));
        g.bench_with_input(BenchmarkId::from_parameter(shape.name()), &frames, |b, f| {
            b.iter(|| { /* parse every frame; black_box the messages */ todo!() })
        });
    }
}

criterion_group!(benches, bench_parse_stream /*, bench_parse_tuple, bench_streamed */);
criterion_main!(benches);
```

```toml
# crates/pg-sink/Cargo.toml
[dev-dependencies]
criterion = { workspace = true }

[[bench]]
name = "decode"
harness = false
```

```markdown
<!-- docs/benchmarks.md — shape -->
# walrus benchmarks
## Methodology            <!-- hardware, toolchain, criterion settings, how to re-run -->
## Baselines (PR 5.4)     <!-- table: bench · shape · rows/s · ns/row -->
## History                <!-- PR 5.7/5.8 append before/after deltas here -->
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `cargo bench -p pg-sink -p pg-to-arrow` runs to completion locally with stable numbers
      (criterion's own variance estimates, not gut feel).
- [x] Decode benches cover: narrow, wide, text-heavy, and a streamed variant; Arrow benches cover:
      the same Tier-1 shapes, a Tier-2 fan-out shape, and the isolated meta-JSON pair.
- [x] The meta-JSON pair reads as a direct subtraction (identical benches except the serialization),
      and the baseline quantifies its share of `append_row` time (≈576 ns/row = 58–91 %).
- [x] `docs/benchmarks.md` exists with methodology + the full baseline table, units in rows/s.
- [x] No production code changed (benches + manifests + docs only).
- [x] `just bench` works.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings` (bench targets compile-checked)
  - [x] `cargo test --workspace`

## Hints & gotchas

- `harness = false` or criterion silently doesn't run — the default libtest bench harness eats the
  target.
- Generate inputs **outside** `b.iter(..)` and `black_box` the parse *output*, not the input — or
  the compiler may hoist or discard the work you meant to measure.
- Per-iteration allocation: `parse_stream` returns owned messages, so the bench inherently measures
  the allocator. That is the point — but keep iteration inputs identical so runs compare.
- For the streamed variant remember each change message carries a 4-byte xid prefix under
  streaming (proto §7) — reuse the vector-building code rather than hand-rolling a second,
  subtly-wrong generator.
- Criterion writes HTML reports under `target/criterion/` — good for eyeballing distributions;
  gitignore already covers `target/`.
- Record baselines from a quiet machine, mains power, release profile (criterion builds benches
  with the `bench` profile which inherits release) — note all of it in the methodology section.

## References

- Design: `docs/proto-version.md` §4–§8; `docs/walrus-pg-sink.md` §2 (type tiers driving the
  bench shapes); plan findings (per-cell `String`, per-row meta JSON).
- Prev: [PR 5.3](./pr-5.3-docker-build-cache.md) ·
  Next: [PR 5.5](./pr-5.5-bench-loader-transform.md) · [Roadmap](../README.md)
