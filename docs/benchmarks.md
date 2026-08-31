# walrus benchmarks

The living record of Walrus performance measurements: Criterion micro-benchmarks, release-mode
end-to-end efficiency runs, and CPU/allocation/async diagnostics. Timed benchmarks are **never a CI
gate** (shared runners are too noisy); CI compile-checks every target and diagnostic feature. Run
measurements on a quiet machine and compare like-for-like run bundles.

## Methodology

- **Harness**: [criterion.rs] 0.5 (`harness = false` targets, `criterion_main!`). `default-features`
  off (no plotters/rayon/html_reports) — we read the stdout stats, and keep the dev-dep tree lean.
- **Profile**: the `bench` profile (inherits `release`: opt-level 3 and thin LTO). `black_box` guards
  every measured input/output so the optimiser can't hoist or elide the work.
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
just bench                                   # all three crates, criterion defaults
cargo bench -p pg-sink --bench decode        # decoder only
cargo bench -p pg-to-arrow --bench batch     # Arrow only
cargo bench -p loader --bench transform      # loader transform only (the 1M grid takes minutes)
cargo bench -p loader --bench append         # loader Phase-A append only
# faster, still stable:
cargo bench -p pg-sink -p pg-to-arrow -- --warm-up-time 1 --measurement-time 3
```

Criterion writes per-bench estimates under `target/criterion/` (gitignored).

The system harness measures the shipping release binaries and writes a self-describing bundle:

```
just perf-e2e mixed
just perf-e2e wide_text
just perf-e2e large_txn
```

Each run lands under `target/perf/<timestamp>-<scenario>-measure-<pid>/` with `metadata.json`,
`summary.json`, `samples.csv`, service logs, load-generator output, and any diagnostic artifact.
`summary.json` includes rows/s, CPU-seconds per 1,000 drained input rows, sampled peak RSS for each
Walrus process, flush cost, spills, and peak lag at each pipeline stage. It deliberately excludes
the Compose Postgres and MinIO processes from CPU/RSS accounting.

### Comparing a change against a baseline

Absolute medians drift between runs even on one machine — several entries below had to discount that
drift after the fact (the inline-accessor experiment's Arrow control moved backward with no timed
code changed; the decoder text-cell experiment used a much older baseline). Read an optimisation as a **delta measured
back-to-back**, not as two absolute numbers taken weeks apart:

```
just bench-baseline before   # on the commit you want to beat; saves target/criterion/**/before
just bench-compare before    # on the change; criterion prints the per-bench delta
```

Criterion re-runs each bench against the stored sample and reports the change plus whether it clears
its noise threshold, which is what "within noise" in the tables below should mean from here on.
Baselines live under `target/criterion/`, so `cargo clean` discards them.

End-to-end bundles are compared with:

```
just perf-compare target/perf/<baseline> target/perf/<candidate>
```

The comparison is direction-aware and has no pass/fail threshold. It refuses differences in mode,
scenario, workload knobs, OS/architecture/CPU, Rust version, or build profile. For an explicitly
non-authoritative look across unlike runs, call `python3 scripts/perf_report.py compare ...
--allow-mismatch`; the warning is intentionally impossible to miss.

## Profiling — finding the hot spot

The release and Criterion runs answer **whether** performance changed. Diagnostic builds answer
**where** the cost lives. `[profile.profiling]` inherits release, keeps thin LTO and all shipping
code-generation choices, and adds full debug information. It never supplies baseline timing data.
The profile guard rejects any additional code-generation override, and the repository still forbids
a committed `.cargo/config.toml` and host-specific ISA flags.

Install Samply once (`cargo install --locked samply`). On macOS, run `samply setup` after each Samply
upgrade so attach mode is signed correctly. On Linux, perf-event permissions must allow the current
user. Profiles stay local until explicitly uploaded.

### One Criterion path

```
just profile-bench pg-sink decode parse_tuple 10
just profile-bench pg-to-arrow batch append_row 10
just profile-bench loader transform transform 10
just profile-bench loader append append_parquet 10
```

The helper builds first, resolves the exact hashed benchmark executable from Cargo's JSON messages,
then runs that executable under Samply with Criterion's `--profile-time`. Compilation and Cargo are
therefore absent from the samples. The bundle contains `profile.json.gz`; open it with `samply load`.

### One service in the real pipeline

```
just profile-e2e loader wide_text
just profile-e2e sink large_txn
```

The complete source Postgres → sink → object store → loader → DuckDB workload still runs. Samply
attaches only to the selected Rust process and stops after drain, while the ordinary sampler captures
context metrics. On Linux Samply reports on-CPU samples; macOS can also expose off-CPU waits.

### Allocations

```
just profile-heap sink wide_text
just profile-heap loader mixed
```

Only the selected binary enables the `dhat-heap` feature and global tracking allocator. The resulting
`<service>-dhat-heap.json` opens in DHAT's viewer. DHAT changes allocator behavior and slows execution,
so the run is marked non-comparable: use it to rank allocation sites, then return to Criterion and a
release `perf-e2e` run to prove the change helped.

### Async scheduling

```
cargo install --locked tokio-console
just profile-async loader mixed
```

The recipe builds only the selected binary's console initializer with Tokio tracing and the required
`tokio_unstable` cfg, binds the console server to `127.0.0.1:6669`, and waits briefly before applying
load. Connect the UI from a second terminal. The recording is retained in the run bundle. This mode
is also non-comparable because task instrumentation changes the observed program.

### Walrus-specific interpretation

- **The loader's transform is mostly DuckDB.** If the sampled stack ends in DuckDB FFI, use the
  production SQL's `EXPLAIN ANALYZE` below to distinguish window/group-by, joins, and merge work.
- **Async and CPU profiles answer different questions.** Samply locates CPU consumption;
  tokio-console locates long polls, wakeups, and starvation. A blocked `LocalSet` can be serious
  without being wide in an on-CPU flame graph.
- **CPU efficiency is the cost signal.** Require an improvement in CPU-seconds/1,000 drained rows,
  not only elapsed time; RSS and backlog must not regress enough to move the bottleneck elsewhere.

### Future deterministic CI

The next regression-gating phase will use `iai-callgrind` on Linux for the smallest existing decode,
Arrow append, and transform cases. It will begin as a manual or scheduled job; thresholds become a PR
gate only after repeated runs establish stable instruction/cache baselines. No `iai-callgrind`
dependency or performance gate is part of the current local framework.

## Optimization log

### 2026-08-30 — one-pass loader transform rendering

The loader DHAT bundle `20260830T233544Z-mixed-heap-loader-90161` identified the eleven chained
`str::replace` calls in `TransformSql::render` as the largest application-owned allocation stream:
1,478,664 bytes across 253 allocations in that short run. The renderer now computes the final SQL
capacity and substitutes every template token in one traversal, copying the growing statement once
instead of once per token.

A dedicated Criterion benchmark captured `render-before` before the production change and compared
the new implementation against those saved samples:

| render shape | before | after | Criterion change |
|---|---:|---:|---:|
| 2 columns | 10.818 µs | 7.760 µs | **−28.70%**, `p < 0.05` |
| 30 columns | 35.284 µs | 26.798 µs | **−23.86%**, `p < 0.05` |

The matching post-change DHAT bundle `20260830T234946Z-mixed-heap-loader-98863` contains zero
`str::replace` allocations; the final rendered string is one allocation per call. Both DHAT runs
processed 20,000 rows, but they performed different numbers of polling cycles, so the removed
allocation site—not a percentage computed from whole-run allocation totals—is the valid comparison.

The release bundles `20260830T232444Z-mixed-measure-78627` (before) and
`20260830T235146Z-mixed-measure-297` (after) each drained 435,000 rows in 66 seconds. Loader CPU fell
from 0.1287 to 0.1265 seconds per 1,000 rows (**−1.71%**); total Walrus CPU fell from 0.1479 to 0.1455
(**−1.62%**), while loader peak RSS changed by +0.11%. That single back-to-back pair demonstrates no
system regression and is directionally consistent with the microbenchmark, but it is not a
statistical end-to-end claim; repeat alternating release runs before using the 1.71% figure for
capacity planning.

### 2026-08-30 — file-level replay ledger removes the raw per-row index

The release samples showed the loader dominating Walrus CPU while its RSS rose throughout the run;
a fixed 5,000-row transform tail changed by only 5.4% between an empty raw table and one million
historical rows, ruling out raw-history scan growth as the main cause. A direct
DuckDB isolation test made the real cost obvious: inserting one million synthetic raw rows took
0.72–1.08 seconds with the old composite primary key and 0.25–0.28 seconds into a heap.

Phase A now commits each raw file and one `_walrus_ingested_files` URI marker in the same DuckDB
transaction. Replays return zero before reopening Parquet. The raw log is a heap, so ordinary rows
no longer maintain a per-row ART index for a file-granularity failure mode. Existing databases keep
the old key until their pending control queue is empty, using it to absorb any pre-upgrade crash
replay; a transactional CTAS then preserves all columns/rows while removing the constraint.

The saved Criterion baseline `replay-before` and the identical post-change benchmark show:

| 50k-row append | before | after | Criterion change |
|---|---:|---:|---:|
| narrow (3 columns) | 109.23 ms | 87.83 ms | **−19.60%**, `p < 0.05` (+24.37% throughput) |
| wide (30 columns) | 196.99 ms | 193.18 ms | −1.94%, within the configured noise threshold |

The fixed-tail `raw_history` controls remained unchanged at 0, 100k, and 1M historical rows
(`p = 0.81`, `0.70`, and `0.43`), so the improvement is isolated to append rather than a transform
trade-off. Correctness tests cover marker-before-Parquet replay, rollback when the marker insert
fails after raw insertion, legacy crash replay, lossless migration, and migration idempotency.

Two independent release bundles compared with the immediate pre-ledger bundle
`20260830T235146Z-mixed-measure-297`:

| bundle | rows / elapsed | loader CPU s/1k | change | total CPU s/1k | change | loader peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| pre-ledger | 435k / 66s | 0.1265 | — | 0.1455 | — | 762.81 MiB |
| `20260831T005451Z-mixed-measure-37250` | 435k / 67s | 0.1183 | **−6.43%** | 0.1377 | **−5.42%** | 753.50 MiB |
| `20260831T005719Z-mixed-measure-38947` | 430k / 66s | 0.1221 | **−3.43%** | 0.1412 | **−3.01%** | 723.91 MiB |

The two candidate runs average 0.1202 loader CPU-seconds per 1,000 rows (**−4.93%**) and 0.1394
total CPU-seconds per 1,000 rows (**−4.21%**). Rows are normalized because the time-bounded load
generator produced 430k–435k. Wall throughput was 1.15–1.49% lower, so this is an efficiency win,
not a latency/throughput claim. RSS moved in the right direction but remains too sample-sensitive to
claim as a proven capacity reduction.

## Sink and Arrow micro-benchmark baselines

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
copies. The first optimization target was the per-cell `String` allocation.

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
sink hot-path win available and became the first implemented optimization.

## Loader micro-benchmark baselines

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
loop. **Decision: do not rewrite the back-scan** — DuckDB already handles it; confirm with
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
per `(table, schema_version)` is a worthwhile optimization where files are small and numerous.

### `EXPLAIN ANALYZE` — the 1M-row transform (K=1), which operators dominate

The production SQL is two statements. Rendered + profiled (single run; profiling inflates absolute
time — read the **shape**, not the total):

- **Step 1+2 — dedup + TOAST-resolve + mirror LEFT JOIN → `_batch`** (the heavy step):
  `WINDOW` (row_number over 1M raw) → `HASH_GROUP_BY` → `LEFT_DELIM_JOIN` (the **decorrelated** TOAST
  back-scan) → `HASH_JOIN` (LEFT, mirror). The window/group-by + delim-join dominate.
- **Step 3 — `MERGE_INTO`** (`HASH_JOIN` of `_batch` to the mirror on the PK): ~1/7 the time of Step 1+2.

Takeaway: the **window dedup** is the transform's cost centre, not the TOAST back-scan (decorrelated)
nor the MERGE (index-joined). Future work should target the window/scan, if anything.

## End-to-end throughput — the system baseline

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
   micro-benches (`append_row` and transform) flagged. The sink absorbs the wider rows with
   *more, smaller* flushes (141 vs 45; mean latency actually drops to 4.9 ms).
3. **The bulk path is loader-friendly and streams cleanly on the sink.** The 200k-row `large_txn`
   moves at 22 k rows/s with the loader never lagging (one file → per-file overhead amortised — cf. the
   finding that transform cost tracks winner count, not raw rows), and `walrus_sink_spill_total`
   moves 0 → 5, confirming the txn is decoded **streamed** (reorder buffer spills at
   `logical_decoding_work_mem=64kB`, past the 4 MB inflight ceiling).

**Optimization priority:** the loader has the higher system-level leverage because it saturates first
under steady load. The sink is not the throughput limiter here, but its batch-constant per-row
meta-JSON cost (~576 ns/row) was a cheap, high-confidence win.

## History

Before/after deltas land here, each citing the baseline row it improves and the change that produced it.

### Merged cache footprints

Measured with `std::mem::size_of` on the pinned Rust 1.95.0 64-bit toolchain after the layout
boxing work. The cache-line column uses an ideal aligned 64-byte span, `ceil(total bytes / 64)`:

| type | size | multiplicity | `narrow_int4` (4 cols) | `wide30` (30 cols) | `text_heavy` (10 cols) |
|---|---:|---|---:|---:|---:|
| `Emit` | 1 B | once per source column in the shared batch plan | 4 B → 1 line | 30 B → 1 line | 10 B → 1 line |
| `TupleValue` | 40 B | once per decoded cell | 160 B → 3 lines | 1,200 B → 19 lines | 400 B → 7 lines |
| `Message` | 88 B | once per decoded WAL record | 88 B → 2 lines | 88 B → 2 lines | 88 B → 2 lines |

These are inline container footprints: heap storage owned by `Vec`, `String`, and `Bytes` is not
included. The existing `TupleValue` and `Message` guards remain the owners of those layouts; this
change adds only the missing exact 64-bit guard for the one-byte `Emit` plan element.

### `#[inline]` on the cross-crate accessors

`common::Lsn::{new, as_u64}` and the eight mechanically small `Reader` accessors now carry
`#[inline]`, so downstream compilation units can see their bodies without depending on thin LTO.
Measured back-to-back on the reference machine (median ns/row, `--warm-up-time 1`,
`--measurement-time 3`):

| bench | shape | before | after | Δ |
|---|---|---:|---:|---:|
| `parse_tuple` | `narrow_int4` | 246.55 | 199.34 | **−19.1 %** |
| `parse_tuple` | `wide30` | 1,741.2 | 1,577.0 | **−9.4 %** |
| `parse_tuple` | `text_heavy` | 736.15 | 602.99 | **−18.1 %** |
| `append_row` | `narrow_int4` | 691.98 | 771.07 | +11.4 % |

All three decoder shapes improved, including the allocation-heavy case, so exporting the small
cursor bodies produced a real cross-crate decode win. The Arrow control moved backward even though
this change touched no timed `BatchBuilder` body; that result is recorded as run-to-run machine drift,
not attributed to the inline hints. No larger reader or `#[inline(always)]` exception was added to
chase either number.

### `SinkMeta` compact strings deferred

The unchanged `pg-to-arrow` batch suite was re-run after repeated `batch_id`
assignment reuse its existing `String` allocation with `clone_from` (median, 1,000 rows per
iteration, Apple M2, macOS 26.5.2, rustc 1.95.0, Criterion defaults):

| `arrow/append_row` shape | median | ns/row |
|---|---:|---:|
| `narrow_int4` | 757.89 µs | 757.89 |
| `wide30` | 1.4492 ms | 1,449.2 |
| `text_heavy` | 1.1449 ms | 1,144.9 |
| `tier2_fanout` | 1.3677 ms | 1,367.7 |

These are fresh absolute medians, not evidence for a compact-string change: the benchmark contains
no candidate implementation, and the committed system profile still shows the loader
saturating while sink inflight stays at zero. `Arc<str>` and compact-string crates remain deferred
until allocation profiling identifies `SinkMeta` strings as an end-to-end sink limiter.

### Key-column scratch (`SmallVec` declined)

`PgRelation::key_columns()` was measured in isolation for the common one-key shape and a composite
three-key shape (median, Apple M2, macOS 26.5.2, rustc 1.95.0):

| `loader/keycols` shape | median | fastest committed transform cycle | share of cycle |
|---|---:|---:|---:|
| one key | 41.554 ns | 25.1 ms | 0.00017 % |
| three keys | 45.635 ns | 25.1 ms | 0.00018 % |

An initial run measured 43.083 ns / 43.061 ns; Criterion found no performance change between runs.
Both shapes are within noise of each other, and even the faster 25.1 ms transform baseline is over
550,000× larger. `SmallVec` is therefore declined: its dependency/API/branching cost cannot move
the loader's DuckDB-dominated end-to-end profile.

### Decoder text cells

**Single-copy `'t'` cells (landed; measured regression).** `Reader::str` validates UTF-8 directly on
the borrowed frame, so a text cell is copied once into `TupleValue::Text` instead of first copying
into an intermediate `Bytes`. The allocation/copy removal is mechanically covered by the Reader
tests and source probe, but it did not improve this short micro-benchmark run (median ns/row, Apple
M2, macOS 26.5.2, rustc 1.95.0, `--warm-up-time 1 --measurement-time 3`):

| bench | shape | historical baseline | after | Δ |
|---|---|---:|---:|---:|
| `parse_tuple` | `narrow_int4` | 236 | 295 | **+25.0 %** |
| `parse_tuple` | `wide30` | 1 822 | 1 878 | +3.1 % |
| `parse_tuple` | `text_heavy` | 710 | 759 | +6.9 % |

This is an honest negative result rather than evidence of a throughput win. The absolute historical
baseline predates intervening decoder changes, and the largest regression is on the smallest cells;
the change is retained for its single-allocation invariant and simpler one-primitive cursor, not on
the strength of this timing sample.

### Sink hot path

**1. Meta-JSON amortization (landed).** `BatchBuilder` now serializes the batch-constant `SinkMeta`
fields once per sealed file and, per row, serializes only the varying fields, splicing `{const,row}`
into a reused buffer. Byte-equivalent to `serde_json::to_string(meta)` (key order aside; proven by
`common::sink_meta::amortized_meta_matches_full`). Measured on the original `append_row` suite (median
ns/row, same reference machine):

| shape | before | after | Δ |
|---|---:|---:|---:|
| `narrow_int4` | 634 | 460 | **−27.5 %** |
| `wide30` | 1 001 | 814 | −18.7 % |
| `text_heavy` | 800 | 616 | −23.0 % |
| `tier2_fanout` | 1 052 | 837 | −20.4 % |

The win is largest on narrow rows (where meta JSON was ~91 % of `append_row`); it removes ~half the
~576 ns/row meta cost (the batch-constant half, now cached). `finish` and the decoder benches are
untouched (within noise).

**2. `[profile.release] lto = "thin"` (landed, honest null on micro-benches).** Isolated on the
`append_row` suite the delta is **within noise** (−0.8 % to +1 %, mixed sign) — single-crate
micro-benches don't exercise the cross-crate inlining thin LTO buys. Kept as standard release-artifact
hygiene (the pg-sink/loader binaries span crates: decode → batch → arrow → parquet); `codegen-units`
left at default (cgu=1's few-percent gain doubles the release build, contrary to the goal of cutting
build time).

**3. Ownership-taking `push` / clone removal (measured-context defer).** `TableBatcher::push` still
`to_vec()`s the decoded values, and `on_commit` clones `batch_id` per row. Taking ownership cascades
into the sink's core decode loop (`route` borrows `&Message`; owning it restructures the loop + both
call sites + `stream_txn.rs`), and `batch_id → Arc<str>` ripples through every `SinkMeta` construction
site. **Deferred**: the e2e ranking shows the **sink is not the system bottleneck** (the loader
is; sink `inflight` stays 0 at 6–7 k rows/s), and the meta-amortization already removed the dominant
per-row sink cost — so this ordering-sensitive refactor is low-leverage. Recorded here rather than
taken, following the project's "measure, don't guess" rule.

**System-level (`mixed` re-run):** end-to-end throughput is **unchanged** — sink 6 081 rows/s,
flush 8.0 ms (vs the baseline's 6 250 rows/s, 8.2 ms; within run-to-run variance), the loader still
the bottleneck (`raw_append`+`transform` lag in the MBs, sink `inflight` 0). The `append_row` micro-win
is real but invisible at the system level **because the sink was never the limiter** — which is exactly
why candidate 3 (sink clone removal) was deferred in favor of loader work.

### Loader hot path

**1. Per-`schema_version` DESCRIBE cache (landed).** `append_parquet` used to run a
`DESCRIBE SELECT * FROM read_parquet(...)` against **every** claimed file to map its columns by name
(a fixed **~10 ms/file**, ~10 % of a narrow append). By the sink's homogeneous-file rule
(walrus-pg-sink §3.5) every file at a `schema_version` has the same columns, so `TableDb` now caches
the column list keyed on `schema_version` (a `RefCell<HashMap<i64, Arc<Vec<String>>>>`, never
invalidated — a DDL bump is a new key). A Phase-A cycle claiming **N same-version files runs one
`DESCRIBE`, not N** (asserted by `duck::tests::spill_override_…`: two v1 files → one cached entry).

Delta: the ~10 ms introspection is now paid **once per (table, schema_version)** instead of per file —
`(N−1) × ~10 ms` saved per cycle (at the default `max_files=100`, up to ~1 s/cycle). The single-cold-file
`append_parquet` bench is unchanged (the first file still DESCRIBEs; the win is the repeats). No
behavioural change: the same column list, just computed once.

**2. TOAST back-scan rewrite (declined — measured).** The baseline measured the back-scan delta at **≈0
(within noise)** on the 100k-row / 30 %-sentinel bench: DuckDB **decorrelates** the per-column
correlated subquery into a single `LEFT_DELIM_JOIN` (see the EXPLAIN ANALYZE section), so it is
already set-based. Rewriting it to a hand-rolled windowed `last_value(… IGNORE NULLS)` carry-forward
would add SQL complexity and a `NULL`-vs-sentinel trap (walrus-pg-sink §2.7) for **no measured gain**.
**Not taken** — the measurements showed no benefit.

**3. Window-rescan audit (O(tail) confirmed).** The transform's `>= after_lsn` tail scan: the
scaling grid shows throughput **rising** with N (342 K → 2.11 M rows/s at K=1) — i.e. O(new events)
with amortising fixed overhead, no superlinear term, cost tracking the distinct-PK winner count (the
window `HASH_GROUP_BY`, per EXPLAIN ANALYZE). Bounded in practice by the retention prune. No
pathology was found; the `>=` bound is **unchanged** because the snapshot straddle depends on it.

**System-level (`mixed` re-run):** sink 6 081 rows/s (unchanged), loader lag peak 1.97 MB —
**within run-to-run variance** of the baseline (1.72 MB). The DESCRIBE cache removes a genuine
per-cycle cost, but the loader's throughput is gated by `read_parquet` ingest + the transform window
(the baseline's dominant costs), so the introspection saving doesn't visibly shift end-to-end lag; correctness
is unchanged (mirror transforms, lags drain to 0). The next loader throughput lever is the ingest/
transform path, not per-file introspection.
