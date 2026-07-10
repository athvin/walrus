# walrus benchmarks

The living record of the sink/loader hot-path micro-benchmarks: methodology, the recorded baseline,
and — from PR 5.7/5.8 on — before/after deltas for each optimisation. Benches are **never a CI gate**
(shared runners are too noisy); CI only compile-checks the bench targets via `clippy --all-targets`.
Run them yourself on a quiet machine with `just bench`.

## Methodology

- **Harness**: [criterion.rs] 0.5 (`harness = false` targets, `criterion_main!`). `default-features`
  off (no plotters/rayon/html_reports) — we read the stdout stats, and keep the dev-dep tree lean.
- **Profile**: the `bench` profile (inherits `release`: opt-level 3, LTO off). `black_box` guards every
  measured input/output so the optimiser can't hoist or elide the work.
- **Throughput**: `Throughput::Elements(rows)`, so results read directly as **rows/s** (`Melem/s`).
- **Inputs**: generated *outside* the timed loop. Decoder benches synthesize valid pgoutput byte
  streams (`Begin/Relation/Insert/Commit`, and a streamed `StreamStart/…/StreamStop` variant) from the
  same message layouts the golden vectors prove (`docs/proto-version.md` §4–§8). Arrow benches build a
  `PgRelation` per shape and append the same row `ROWS` times.
- **Shapes**: `narrow_int4` (4 int4 cols), `wide30` (30 mixed cols), `text_heavy` (10 × ~200-byte
  text cols — allocation-bound), and `tier2_fanout` (interval + int4range + timetz — the Arrow
  fan-out path).
- **Reference machine**: Apple M2 (8 cores), 24 GB, macOS 26.5.1, rustc 1.95.0. Numbers below were
  taken with `--warm-up-time 1 --measurement-time 3` (tight enough CIs; the defaults 3 s/5 s give the
  same medians). **Absolute numbers are machine-specific — compare deltas, not machines.**

### How to re-run

```
just bench                                   # both crates, criterion defaults
cargo bench -p pg-sink --bench decode        # decoder only
cargo bench -p pg-to-arrow --bench batch     # Arrow only
# faster, still stable:
cargo bench -p pg-sink -p pg-to-arrow -- --warm-up-time 1 --measurement-time 3
```

Criterion writes per-bench estimates under `target/criterion/` (gitignored).

## Baselines (PR 5.4)

Medians from the reference machine. `ns/row` = median ÷ rows (10 000 for decode, 1 000 for Arrow).

### pgoutput decode — `crates/pg-sink/benches/decode.rs`

| bench | shape | median | ns/row | rows/s |
|---|---|---:|---:|---:|
| `parse_stream` | narrow_int4 | 2.479 ms | 248 | 4.03 M |
| `parse_stream` | wide30 | 18.91 ms | 1 891 | 529 K |
| `parse_stream` | text_heavy | 7.829 ms | 783 | 1.28 M |
| `parse_stream_streamed` | narrow_int4 | 2.471 ms | 247 | 4.05 M |
| `parse_stream_streamed` | wide30 | 18.89 ms | 1 889 | 529 K |
| `parse_stream_streamed` | text_heavy | 7.964 ms | 796 | 1.26 M |
| `parse_tuple` | narrow_int4 | 235.5 ns | 236 | 4.25 M |
| `parse_tuple` | wide30 | 1.822 µs | 1 822 | 549 K |
| `parse_tuple` | text_heavy | 709.7 ns | 710 | 1.41 M |

**Reading it.** Cost scales with column count, not just bytes: `wide30` (30 cols) is ~7.5× slower per
row than `narrow_int4` (4 cols) — the per-cell `String` allocation in the `'t'` branch dominates. The
**streamed variant is within noise of the non-streamed** one: the 4-byte sub-xid prefix per change is
negligible. `text_heavy` (10 × 200-byte cols) sits between — fewer cells than wide30 but larger
copies. First optimisation target for PR 5.7: the per-cell `String` allocation.

### Arrow batch building — `crates/pg-to-arrow/benches/batch.rs`

| bench | shape | median | ns/row | rows/s |
|---|---|---:|---:|---:|
| `append_row` | narrow_int4 | 633.9 µs | 634 | 1.58 M |
| `append_row` | wide30 | 1.001 ms | 1 001 | 999 K |
| `append_row` | text_heavy | 800.1 µs | 800 | 1.25 M |
| `append_row` | tier2_fanout | 1.052 ms | 1 052 | 951 K |
| `finish` (whole batch, 1 000 rows) | narrow_int4 | 856.7 ns | — | — |
| `finish` | wide30 | 4.901 µs | — | — |
| `finish` | text_heavy | 2.191 µs | — | — |
| `finish` | tier2_fanout | 1.701 µs | — | — |

`finish()` (builders → RecordBatch) is cheap — sub-µs to a few µs for the *whole* 1 000-row batch —
so `append_row` is where the per-row cost lives.

### The meta-JSON cost (isolated) — the headline

`append_row` serialises the per-row `SinkMeta` with `serde_json::to_string` and appends it to the
trailing meta column. The two benches below are **identical except that serialisation** — `serialize`
pays `serde_json::to_string(meta)` per row, `const` appends a pre-serialised constant:

| bench | median (1 000 rows) | ns/row | rows/s |
|---|---:|---:|---:|
| `meta_json/serialize` | 597.4 µs | 597 | 1.67 M |
| `meta_json/const` | 21.01 µs | 21 | 47.6 M |
| **difference (the JSON cost)** | **≈ 576 µs** | **≈ 576** | — |

**≈ 576 ns/row is spent serialising `SinkMeta` to JSON — and almost all of a `SinkMeta` is identical
for every row in a batch** (only `op`, `lsn`, and `unchanged_toast` vary; `commit_lsn`, `commit_ts`,
`xid`, `epoch`, `batch_id`, `schema_version`, source names, `sink_instance`, `sink_processed_at` are
batch-constant). That 576 ns is **~91 % of `append_row/narrow_int4` (634 ns), ~72 % of `text_heavy`,
and ~58 % of `wide30`.** Amortising the batch-constant part of the meta JSON is the single biggest
sink hot-path win available — PR 5.7's primary target.

## Baselines (PR 5.5) — the loader

All against an **in-memory DuckDB** with `SET threads = 4` (pinned), seeded via one
`INSERT … SELECT range(N)` per iteration (individual inserts would dwarf the measured transform).
`sample_size = 10` for the multi-hundred-ms benches. The transform benches run the **production**
SQL (`loader::transform::apply_transform` over `TransformSql`) — one source of truth with the tests.
`crates/loader/benches/transform.rs`, `append.rs`.

### Transform scaling (N events over N/K PKs, K events/PK)

| N | K=1 (median · rows/s) | K=10 (median · rows/s) |
|---:|---|---|
| 10 000 | 29.3 ms · 342 K | 25.1 ms · 399 K |
| 100 000 | 98.9 ms · 1.01 M | 57.4 ms · 1.74 M |
| 1 000 000 | 473.9 ms · 2.11 M | 249.8 ms · 4.00 M |

**Reading it.** Two clean signals: (1) **throughput rises with N** (342 K → 1.01 M → 2.11 M rows/s at
K=1) as fixed per-cycle overhead amortises — the transform is **O(new events)** as designed
(`docs/walrus-loader.md` §6.3); no superlinear term appears. (2) **K=10 is ~2× faster than K=1 at the
same N** (250 ms vs 474 ms at 1M) — because the window collapses 10 events → 1 winner per PK, so cost
tracks the **distinct-PK winner count** (the MERGE side), not the raw event count. Churny tables
(high K) are cheaper per event, not costlier.

### Unchanged-TOAST back-scan (isolated) — 100k rows, 50k PKs, K=2

| variant | median |
|---|---:|
| no TOAST sentinels | 138.5 ms |
| sentinels on 3 cols, 30 % of winners | 135.1 ms |
| **back-scan delta** | **≈ 0 (within noise)** |

**The back-scan is not a bottleneck.** Same rows/PKs/LSNs; only the winner's `unchanged_toast` meta
varies. The delta is within the confidence intervals. `EXPLAIN ANALYZE` (below) shows why: DuckDB
**decorrelates** the per-column correlated subquery into a single `LEFT_DELIM_JOIN`, not a per-row
loop. **Go/no-go for PR 5.8: do NOT rewrite the back-scan** — DuckDB already handles it; confirm with
`EXPLAIN ANALYZE` before touching it.

### Mirror-size sensitivity (100k-row tail)

| mirror | median |
|---|---:|
| empty | 100.3 ms |
| 1 000 000 rows pre-seeded | 106.0 ms |

Only **~6 % slower** merging into a 1M-row mirror — the `MERGE` join is PK-index-bounded, not a full
scan. Mirror size is not a hot-path concern.

### Phase-A append (`append_parquet` from a local file — no MinIO) — 50k rows

| bench | median | rows/s |
|---|---:|---:|
| `append_parquet/narrow` (3 cols) | 103.1 ms | 485 K |
| `append_parquet/wide` (30 cols) | 174.8 ms | 286 K |
| `parquet_describe/narrow` (per-file DESCRIBE) | 9.94 ms | — |
| `parquet_describe/wide` | 10.19 ms | — |

The per-file `DESCRIBE` introspection is a **fixed ~10 ms/file** (independent of width) — **~10 % of a
narrow-file append**, and paid once per manifest file regardless of row count. Caching the DESCRIBE
per `(table, schema_version)` is a candidate PR 5.8 win where files are small/many.

### `EXPLAIN ANALYZE` — the 1M-row transform (K=1), which operators dominate

The production SQL is two statements. Rendered + profiled (single run; profiling inflates absolute
time — read the **shape**, not the total):

- **Step 1+2 — dedup + TOAST-resolve + mirror LEFT JOIN → `_batch`** (the heavy step):
  `WINDOW` (row_number over 1M raw) → `HASH_GROUP_BY` → `LEFT_DELIM_JOIN` (the **decorrelated** TOAST
  back-scan) → `HASH_JOIN` (LEFT, mirror). The window/group-by + delim-join dominate.
- **Step 3 — `MERGE_INTO`** (`HASH_JOIN` of `_batch` to the mirror on the PK): ~1/7 the time of Step 1+2.

Takeaway: the **window dedup** is the transform's cost centre, not the TOAST back-scan (decorrelated)
nor the MERGE (index-joined). PR 5.8 should target the window/scan, if anything.

## End-to-end throughput (PR 5.6) — the system

Where the micro-benches rank suspects inside one process, this measures the whole pipeline on the
real compose stack (source PG → sink → S3, S3 → loader → mirror), reading the Prometheus metrics as
probes. Reproduce with `just bench-e2e <scenario>` (local-only; **never a CI job** — numbers are
hardware-relative). Release binaries; sink knobs `MAX_FILL=2s MAX_ROWS=5000 MAX_BYTES=2MB
MAX_INFLIGHT=4MB`, loader `POLL_INTERVAL=1s`; reference machine as above.

| scenario | source load | sink rows/s | sink flush (mean) | sink inflight / spill | loader lag peak | first to saturate |
|---|---|---:|---:|---:|---:|---|
| `mixed` (i/u/d, 4 clients, 30 s) | 16.8 k tps | 6 250 | 8.2 ms | 0 / 0 | **1.72 MB** | **loader** |
| `wide_text` (~1 KB notes) | 8.3 k tps | 6 886 | 4.9 ms | 0 / 0 | **11.4 MB** | **loader** |
| `large_txn` (one 200k-row txn) | 1 txn | 22 222 | 30.4 ms | — / **5** | 0 | sink streams; loader keeps up |

**Bottleneck ranking (what saturates first, with evidence).**

1. **The loader is the system bottleneck under sustained row-at-a-time load.** In `mixed` and
   `wide_text` the sink never backs up — `walrus_sink_inflight_bytes` stays 0 and `spill_total` = 0 —
   while the loader's `raw_append_lag_bytes` **and** `transform_lag_bytes` climb into the MBs. The
   backlog is *transient*, not runaway: both drain to 0 within a few seconds of load stopping. So the
   loader's per-cycle throughput trails the sink's, but the pipeline is stable.
2. **Wide rows amplify the loader backlog ~6.6×** (`wide_text` 11.4 MB vs `mixed` 1.72 MB peak lag) —
   larger per-row payloads hit the loader's per-row transform + append harder, exactly the ops the
   micro-benches (PR 5.4 `append_row`, PR 5.5 transform) flagged. The sink absorbs the wider rows with
   *more, smaller* flushes (141 vs 45; mean latency actually drops to 4.9 ms).
3. **The bulk path is loader-friendly and streams cleanly on the sink.** The 200k-row `large_txn`
   moves at 22 k rows/s with the loader never lagging (one file → per-file overhead amortised — cf. the
   PR 5.5 finding that transform cost tracks winner count, not raw rows), and `walrus_sink_spill_total`
   moves 0 → 5, confirming the txn is decoded **streamed** (reorder buffer spills at
   `logical_decoding_work_mem=64kB`, past the 4 MB inflight ceiling).

**Where PRs 5.7/5.8 should spend effort:** the loader (PR 5.8) has the higher system-level leverage —
it saturates first under steady load. The sink (PR 5.7) is not the throughput limiter here, but its
per-row meta-JSON cost (PR 5.4: ~576 ns/row, batch-constant) is cheap, high-confidence, and worth
taking. Net: **5.8 for throughput, 5.7 for a low-risk per-row win.**

## History

Before/after deltas from PR 5.7 (sink) and PR 5.8 (loader) land here, each citing the baseline row it
improves and the commit that made the change.

_(none yet — PR 5.4 establishes the baseline)_

[criterion.rs]: https://bheisler.github.io/criterion.rs/book/
