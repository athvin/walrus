# PR 6.11 — reload observability: metrics, alerts, runbook

> **Status:** 📋 Planned

> **Phase:** 6 — single-table reload · **Crates touched:** `common`, `pg-sink`, `loader`,
> `deploy` · **Est. size:** S · **Depends on:** PR 6.9 · **Unlocks:** PR 6.12

A reload is a long-running, restartable, failable background job — exactly the kind of thing that
rots into a silent zombie without instruments. This PR gives the subsystem the same observability
treatment PR 4.10 gave the pipeline: named metrics in `common::metrics`, alert rules for the three
states an operator must hear about (stuck lease, restart-cap exhaustion, cross-check violation),
and the runbook that turns `table_reload` rows into decisions — how to request, watch, and unstick
a reload. It also writes down the one *expected* anomaly: a paused table's `raw_append_lag_bytes`
grows **by design** during a rebuild; an alert that doesn't know that will page someone for a
feature working correctly.

## Why — learning objectives

By the end of this PR you will have practised:

- **Instrumenting a state machine** — gauges for "where are we", counters for "what happened",
  a histogram for the one latency (echo wait) with correctness implications.
- **Alert design against false pages** — encoding "lag grows during a pause, and that's fine"
  so the alert fires on stuck, not on busy.
- **Runbooks as the operator API** — the `table_reload` row is the interface; the runbook is its
  documentation.

## Read first

- PR 4.10 (`pr-4.10-observability-metrics.md`) — where dashboards/alert rules live and their
  format; this PR extends those files, not a new system.
- `crates/common/src/metrics.rs` — the constants + `describe_all()` + per-table-label pattern
  (`LOADER_FILES_READY` / `init_table_series`).
- `../../single-table-reload.md` — H6 (cap), H7 (stuck lease), H9 (restart cap), H11 (the
  misconfiguration the echo-timeout metric surfaces).

## Scope

**In scope**

- Metrics (consolidating the provisional counters from 6.3/6.8 into the house registry):
  - `walrus_reload_active{flavor}` — gauge, currently non-terminal reloads.
  - `walrus_reload_chunks_total{table}` / `walrus_reload_rows_exported_total{table}` — counters.
  - `walrus_reload_echo_wait_seconds` — histogram (the H1 round-trip; its p99 bounds reload
    throughput).
  - `walrus_reload_restarts_total{table}`, `walrus_reload_failed_total{table}`,
    `walrus_reload_crosscheck_violations_total` — counters.
- Alert rules (wherever 4.10's live): reload non-terminal with `lease_expiry` stale beyond N
  minutes; cap-exhausted failure; any cross-check violation (a correctness-model breach — page,
  don't ticket).
- The paused-lag annotation on the existing `raw_append_lag_bytes` alert.
- Runbook section: request (`just reload`, flavor guide from 6.10), watch (the SQL to read
  progress + the dashboard row), unstick (expired lease ⇒ restart the sink or fail the row —
  with the exact UPDATE), and the retention note for pruning `walrus.reload_signal`.

**Explicitly deferred** (do *not* build these here)

- The e2e that exercises all of this under load → **PR 6.12**.
- New dashboard *systems* — extend 4.10's artifacts only.

## Files to create / modify

```
crates/common/src/metrics.rs          # modify — the seven metrics, described; table-label init
crates/pg-sink/src/reload*.rs         # modify — emit at the transition/chunk/echo sites
crates/loader/src/…                   # modify — emit at rebuild-trigger + completion sites
deploy/…                              # modify — dashboard rows + alert rules (4.10's locations)
docs/…                                # modify — runbook section (4.10's runbook location)
```

## Skeleton

```rust
// crates/common/src/metrics.rs  (additions, shape)

pub const RELOAD_ACTIVE: &str = "walrus_reload_active";
pub const RELOAD_CHUNKS_TOTAL: &str = "walrus_reload_chunks_total";
pub const RELOAD_ROWS_EXPORTED_TOTAL: &str = "walrus_reload_rows_exported_total";
pub const RELOAD_ECHO_WAIT_SECONDS: &str = "walrus_reload_echo_wait_seconds";
pub const RELOAD_RESTARTS_TOTAL: &str = "walrus_reload_restarts_total";
pub const RELOAD_FAILED_TOTAL: &str = "walrus_reload_failed_total";
pub const RELOAD_CROSSCHECK_VIOLATIONS_TOTAL: &str = "walrus_reload_crosscheck_violations_total";

/// Call-site helpers in the record_batch_flush() style — one per emission site, so the
/// metric names never appear at call sites.
pub fn record_reload_chunk(/* table, rows, echo_wait */) { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] All seven metrics are registered and described (`describe_all`), and every one **moves**
      during a compose reload run — zero-valued metrics that never tick are asserted against.
- [ ] `walrus_reload_active` returns to 0 after completion; `_failed_total` ticks on a forced
      failure; `_restarts_total` ticks on a forced DDL restart.
- [ ] The three alert rules ship in 4.10's format and their PromQL parses (whatever validation
      4.10 used, reuse).
- [ ] The `raw_append_lag_bytes` alert carries the paused-table annotation.
- [ ] The runbook section exists with copy-pasteable SQL for watch + unstick, the flavor guide,
      and the signal-table pruning note.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `docker compose up --wait`, run a reload, then the metrics-move assertion (named compose
        test or scripted scrape-diff — match 4.10's approach).

## What completed looks like

```
$ docker compose up --wait && just reload table='public.orders'
$ curl -s localhost:9187/metrics | grep '^walrus_reload'
walrus_reload_active{flavor="reload"} 1
walrus_reload_chunks_total{table="public.orders"} 2
walrus_reload_rows_exported_total{table="public.orders"} 2000
walrus_reload_echo_wait_seconds_count 2
# … after completion:
walrus_reload_active{flavor="reload"} 0
walrus_reload_crosscheck_violations_total 0
```

…and the runbook answers "a reload has been `exporting` for an hour — now what?" without reading
source code.

## Hints & gotchas

- `walrus_reload_active` is cheapest as a gauge derived from the controller's in-memory view;
  don't poll control-pg just to set a gauge the controller already knows.
- The echo-wait histogram's buckets should span sub-ms (local compose) to seconds (a lagging
  slot delays echoes — that's the interesting signal: echo wait ≈ end-to-end decode latency).
- Per-table labels: reuse `init_table_series` zero-init so dashboards don't show gaps before a
  table's first reload.
- The stuck-lease alert reads control-pg, not Prometheus — if 4.10's stack has no SQL-exporter
  precedent, expose a `walrus_reload_lease_stale` gauge from the controller tick instead of
  inventing one.
- Cross-check violation is the one page-severity alert here: it means the watermark model is
  wrong, i.e. possible silent data loss — the runbook entry for it is "stop reloads, open an
  issue with the log lines", not "restart the pod".

## References

- Design: `../../single-table-reload.md` H6, H7, H9, H11;
  `../../architecture.md` Observability; PR 4.10's artifacts.
- Prev: [PR 6.10](./pr-6.10-resync-flavor.md) ·
  Next: [PR 6.12](./pr-6.12-e2e-quarantine-recovery.md) · [Roadmap](../README.md)
