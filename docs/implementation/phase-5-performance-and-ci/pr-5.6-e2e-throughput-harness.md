# PR 5.6 — End-to-end throughput harness + the missing lag metric

> **Phase:** 5 — Performance & CI · **Crates touched:** `loader`, `common` (one metric), `scripts/`,
> `justfile` · **Est. size:** M–L · **Depends on:** PR 5.5 · **Unlocks:** PR 5.7

Micro-benches (5.4/5.5) rank suspects inside one process; this PR measures the *system*: rows/sec
from source Postgres through the sink to S3, and from S3 through the loader into the mirror, on the
real compose stack — using the Prometheus metrics that already exist (PR 4.10) as the probes. It
also implements the one metric that's still a placeholder, `walrus_loader_raw_append_lag_bytes`, so
end-to-end lag is finally observable. The output is a repeatable `just bench-e2e` run and a recorded
bottleneck ranking in `docs/benchmarks.md` — the ground truth that decides where PRs 5.7/5.8 spend
their effort.

## Why — learning objectives

By the end of this PR you will have practised:

- **Measuring a pipeline by its own telemetry** — deriving throughput from counters
  (`rate(walrus_sink_parquet_rows_written_total)`) and latency from histograms, instead of bolting
  on a second measurement system.
- **Load generation with pgbench** — custom scripts (`-f`), controlled client counts, and shaping a
  workload (insert/update/delete mix, wide text rows, one huge streamed transaction).
- **Little's-law thinking** — where the backlog accumulates (sink inflight? manifest queue? raw
  append? transform?) is where the bottleneck is; each stage already has a gauge.

## Read first

- `crates/common/src/metrics.rs` — every registered series; the
  `walrus_loader_raw_append_lag_bytes` placeholder and its intended meaning
  (`max ready lsn_end − raw_appended_lsn`).
- `crates/loader/src/phase_a.rs` + `crates/control/src/file_manifest.rs` — where the loader already
  polls ready files (the natural place to compute the new gauge from `max(lsn_end)` of ready rows).
- `deploy/docker/docker-compose.yml` — the stack; note `logical_decoding_work_mem=64kB` (streaming
  triggers early — good, the large-txn scenario relies on it).
- `docs/architecture.md` Observability; `docs/walrus-loader.md` §9.3 (the cadence dials the harness
  should expose as env knobs).

## Scope

**In scope**

- **The missing metric:** implement `walrus_loader_raw_append_lag_bytes{table}` — each Phase-A poll
  sets it to `max(lsn_end of ready manifest rows) − raw_appended_lsn` (0 when the queue is empty).
  Unit-test the computation; assert it appears in the `/metrics` scrape test.
- **Load generator** `scripts/loadgen.sh`: drives `pgbench` against the compose source-pg with
  custom script files under `scripts/loadgen/` —
  - `mixed.sql` (weighted insert/update/delete on the seeded tables),
  - `wide_text.sql` (rows with several ~1 KB text columns — exercises the decoder's text path),
  - `large_txn.sql` or a psql heredoc (one multi-100k-row transaction to force streaming);
  - knobs via env: duration, clients, mix weights.
- **Harness** `scripts/bench-e2e.sh`:
  1. `docker compose up --wait`; run migrations; start **release-build** sink + loader locally
     (`cargo build --release` then run the binaries with compose-pointing env, metrics ports on);
  2. run a scenario; scrape both `/metrics` endpoints at start/end (plus every 5 s into a CSV);
  3. print a summary: sink rows/s, p50/p99 flush latency, loader append rows/s, transform cycle
     time, and the three lag gauges over time;
  4. tear down cleanly.
- `just bench-e2e` recipe. **Not** a CI job (documented as local-only in the script header).
- Record one full run per scenario in `docs/benchmarks.md`, ending with an explicit **bottleneck
  ranking** ("what saturates first, at what rate") that names the stage and evidence.

**Explicitly deferred** (do *not* build these here)

- Fixing anything the ranking reveals → **PR 5.7 / 5.8**.
- A long-running soak/chaos harness — the e2e crash/WAL-runaway tests (PRs 4.4/4.5) already cover
  correctness under stress; this PR measures steady-state throughput only.
- Grafana dashboards for the bench CSVs — the PR 4.10 dashboard already graphs the live gauges.

## Files to create / modify

```
crates/common/src/metrics.rs             # modify — implement the raw_append_lag_bytes registration comment
crates/loader/src/phase_a.rs             # modify — compute + set the gauge each poll
crates/loader/tests/metrics_scrape.rs    # modify — assert the new series is exported
scripts/loadgen.sh                       # new
scripts/loadgen/{mixed,wide_text,large_txn}.sql   # new
scripts/bench-e2e.sh                     # new
justfile                                 # + bench-e2e recipe
docs/benchmarks.md                       # modify — e2e results + bottleneck ranking
```

## Skeleton

```bash
# scripts/bench-e2e.sh  (shape)
#!/usr/bin/env bash
# Local-only end-to-end throughput harness. NOT a CI job (numbers are hardware-relative).
set -euo pipefail
SCENARIO="${1:-mixed}"           # mixed | wide_text | large_txn
DURATION="${DURATION:-60}"       # seconds of load
CLIENTS="${CLIENTS:-4}"

docker compose -f deploy/docker/docker-compose.yml up --wait
cargo build --release -p pg-sink -p loader
# start walrus-pg-sink + walrus-loader (release) against the stack, metrics on :9187/:9188 …
# scrape_loop &  →  metrics-$SCENARIO.csv
bash scripts/loadgen.sh "$SCENARIO" "$DURATION" "$CLIENTS"
# wait for lag gauges to drain to 0, final scrape, print summary table, teardown
```

```rust
// crates/loader/src/phase_a.rs  (shape of the metric hook)
// after claiming: lag = max ready lsn_end (this poll's view) − checkpoint.raw_appended_lsn
metrics::gauge!("walrus_loader_raw_append_lag_bytes", "table" => plan.table.clone())
    .set(lag_bytes as f64);
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `walrus_loader_raw_append_lag_bytes{table}` is computed every Phase-A poll, is 0 on an empty
      queue, and appears in the loader `/metrics` scrape test.
- [ ] `just bench-e2e mixed` runs unattended on a laptop with docker: boots the stack, applies
      load, drains, prints the summary, tears down — exit 0.
- [ ] All three scenarios work; `large_txn` demonstrably streams (sink logs Stream frames /
      `walrus_sink_spill_total` moves if the inflight ceiling is set low for the run).
- [ ] `docs/benchmarks.md` gains one recorded run per scenario (rows/s per stage, latency, lag
      curves) and a written **bottleneck ranking** naming what saturates first with evidence.
- [ ] The harness uses release binaries (a debug-build measurement is a bug, not a baseline).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `docker compose up --wait` + the amended loader metrics-scrape integration test

## Hints & gotchas

- pgbench ships in the `postgres:16` image — `docker compose exec -T source-pg pgbench -f - ...`
  avoids needing it on the host; mount or pipe the custom scripts.
- Scrape *counters* and compute rates yourself (end−start ÷ duration); don't eyeball gauges.
  Histograms export `_bucket`/`_sum`/`_count` — p50/p99 from buckets, mean from sum/count.
- The lag gauge needs both operands in bytes: `Lsn` is already numeric-ordered (PR 0.3) —
  subtraction of the u64 forms is the value; guard the underflow when the checkpoint is ahead of
  an empty queue.
- Watch `wal_sender_timeout=5s` in the compose file — it's tuned for tests; a stalled scrape loop
  won't hurt, but don't blame the sink for reconnects the compose config invites.
- Give the run a drain phase: stop load, then wait until `raw_append_lag` and `transform_lag` hit 0
  before the final scrape — otherwise per-stage totals don't reconcile and rows/s undercounts the
  loader.
- Keep every knob (duration, clients, cadences via sink/loader env) printed in the summary header —
  a number without its knobs can't be compared later.

## References

- Design: `docs/architecture.md` Observability; `docs/walrus-loader.md` §9.3;
  `crates/common/src/metrics.rs` (the placeholder comment this PR fulfils).
- Prev: [PR 5.5](./pr-5.5-bench-loader-transform.md) ·
  Next: [PR 5.7](./pr-5.7-sink-hot-path.md) · [Roadmap](../README.md)
