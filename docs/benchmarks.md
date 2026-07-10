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

## History

Before/after deltas from PR 5.7 (sink) and PR 5.8 (loader) land here, each citing the baseline row it
improves and the commit that made the change.

_(none yet — PR 5.4 establishes the baseline)_

[criterion.rs]: https://bheisler.github.io/criterion.rs/book/
