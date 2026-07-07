# PR 4.10 — Observability: Prometheus metrics, dashboard, and alerts

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `common`, `pg-sink`, `loader`
> (+ `deploy/`) · **Est. size:** M · **Depends on:** PR 4.9 · **Unlocks:** PR 4.11

Makes the pipeline observable. Both binaries expose a `/metrics` endpoint with the exact Prometheus series
the design's **Observability** section enumerates — replication lag bytes, retained WAL + `wal_status`,
heartbeat round-trip age + `beat_seq` gap, feedback age, batch/Parquet throughput, in-flight bytes +
spill/pause counts, files-ready-per-table, raw-append lag, transform lag, aborted-txn and failed-file
counts. It ships a committed Grafana dashboard and Prometheus alert rules for the four things that actually
page: slot growth, `wal_status`, heartbeat staleness, loader backlog. A scrape test asserts the metric
**names** exist.

## Why — learning objectives

By the end of this PR you will have practised:

- **Instrumenting a system with `metrics`** — registering counters/gauges/histograms with stable names and
  the right label cardinality (per-table where it matters, never per-row).
- **Turning design signals into series** — each Observability bullet becomes one named metric with a clear
  meaning (lag vs retained WAL vs feedback age are three different numbers).
- **Alerts that page on the right thing** — slot growth, `wal_status`, heartbeat staleness, backlog — and
  *not* on catch-up lag (which is normal after an outage).
- **A `/metrics` scrape test** — asserting the exposition contains the expected names, so a rename can't
  silently break a dashboard.

## Read first

- `../../architecture.md#observability` — the full metric list (copy it into the registry), the structured
  `tracing` field convention, and the four alert families.
- `../../architecture.md#19-slot-liveness--heartbeat--keepalive` — the two-LSN distinction the
  feedback-age and round-trip-age metrics measure; why staleness alerts, never a liveness kill.
- `../../walrus-pg-sink.md#43-probes--get-these-exactly-right` — metrics are the alerting surface;
  liveness stays progress-only.

## Scope

**In scope**

- A `common::metrics` module registering the named series and a `serve_metrics()` helper exposing
  `/metrics` (Prometheus text exposition) on the existing health server.
- **Sink metrics:** replication lag bytes (`pg_current_wal_lsn − confirmed_flush_lsn`), slot retained WAL
  bytes, `wal_status`, seconds-since-heartbeat-confirmed, heartbeat round-trip age, `beat_seq` gap,
  seconds-since-standby-feedback, batch flush latency (histogram), Parquet throughput, in-flight buffered
  bytes, memory-ceiling flush/spill count, speculative open-txn bytes staged, pause-poll activations,
  aborted-txn count, failed-file count. Increment/set them at the call sites built in Phase 2.
- **Loader metrics:** files-`ready`-per-table (backlog), raw-append lag (`sink lsn_end − raw_appended_lsn`),
  transform lag (`raw_appended_lsn − transformed_lsn`), `<table>_raw` row-count / file-size growth, DDL
  events pending, failed-file count. Set at the Phase-3 call sites.
- Committed `deploy/observability/dashboard.json` (Grafana) and `deploy/observability/alerts.yaml`
  (Prometheus rules) for slot growth, `wal_status ∈ {unreserved,lost}`, heartbeat staleness, loader
  backlog.
- A scrape test asserting the exposition contains the expected metric names.

**Explicitly deferred** (do *not* build these here)

- New behaviour — this PR only *exposes* signals already computed; it must not change pipeline logic.
- OpenTelemetry traces / log shipping — `tracing` structured fields already exist; exporters are out of
  scope.
- Prometheus scrape config in K8s (ServiceMonitor) beyond the `/metrics` port annotation.

## Files to create / modify

```
crates/common/src/metrics.rs             # new — register series + serve_metrics() (/metrics exposition)
crates/common/Cargo.toml                 # + metrics = "0.23", metrics-exporter-prometheus = "0.15"
crates/pg-sink/src/...                    # modify — increment/set sink series at existing call sites
crates/loader/src/...                     # modify — set loader series at existing call sites
crates/pg-sink/tests/metrics_scrape.rs   # new — /metrics contains the expected sink names
crates/loader/tests/metrics_scrape.rs    # new — /metrics contains the expected loader names
deploy/observability/dashboard.json      # new — Grafana dashboard
deploy/observability/alerts.yaml         # new — Prometheus alert rules
```

## Skeleton

```rust
// crates/common/src/metrics.rs
/// Register every walrus metric name once at startup (idempotent). Names are stable API —
/// the dashboard and alerts depend on them; a rename is a breaking change.
pub fn register() { todo!() }

/// Serve the Prometheus text exposition at /metrics on the given health server/router.
pub fn serve_metrics(/* router */) { todo!() }

/// Stable metric-name constants so call sites and the scrape test agree.
pub mod names {
    pub const SINK_REPLICATION_LAG_BYTES: &str = "walrus_sink_replication_lag_bytes";
    pub const SINK_SLOT_RETAINED_WAL_BYTES: &str = "walrus_sink_slot_retained_wal_bytes";
    pub const SINK_WAL_STATUS: &str = "walrus_sink_wal_status";                 // gauge: 0 reserved..2 lost
    pub const SINK_HEARTBEAT_ROUNDTRIP_AGE_SECONDS: &str = "walrus_sink_heartbeat_roundtrip_age_seconds";
    pub const SINK_BEAT_SEQ_GAP: &str = "walrus_sink_beat_seq_gap";
    pub const SINK_FEEDBACK_AGE_SECONDS: &str = "walrus_sink_feedback_age_seconds";
    pub const SINK_INFLIGHT_BYTES: &str = "walrus_sink_inflight_bytes";
    pub const SINK_SPILL_COUNT: &str = "walrus_sink_spill_total";
    pub const SINK_ABORTED_TXN_COUNT: &str = "walrus_sink_aborted_txn_total";
    pub const LOADER_FILES_READY: &str = "walrus_loader_files_ready";           // labelled by table
    pub const LOADER_RAW_APPEND_LAG_BYTES: &str = "walrus_loader_raw_append_lag_bytes";
    pub const LOADER_TRANSFORM_LAG_BYTES: &str = "walrus_loader_transform_lag_bytes";
    pub const LOADER_FAILED_FILE_COUNT: &str = "walrus_loader_failed_file_total";
    // … the remaining series from architecture.md#observability
}
```

```rust
// crates/pg-sink/tests/metrics_scrape.rs
#[tokio::test]
async fn metrics_endpoint_exposes_all_sink_series() {
    // scrape /metrics; assert every common::metrics::names::SINK_* constant appears.
    todo!()
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Both binaries expose `/metrics` in Prometheus text exposition, registering **every** series listed in
      `architecture.md#observability` (lag, retained WAL, `wal_status`, heartbeat round-trip age, `beat_seq`
      gap, feedback age, batch/Parquet throughput, in-flight bytes, spill/pause counts, files-ready/table,
      raw-append lag, transform lag, aborted-txn, failed-file).
- [ ] Metric names are stable constants shared by the call sites and the scrape test; per-table series are
      labelled by table (bounded cardinality — never per-row).
- [ ] A committed Grafana `dashboard.json` and Prometheus `alerts.yaml` cover slot growth, `wal_status`,
      heartbeat staleness, and loader backlog — and **no** alert pages on catch-up lag.
- [ ] This PR adds no pipeline behaviour — it only exposes existing signals.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace` including the scrape tests
        **`metrics_endpoint_exposes_all_sink_series`** and the loader equivalent.

## Hints & gotchas

- Watch label cardinality: label per **table**, never per PK/xid/batch — a per-row label melts Prometheus.
  The high-cardinality identifiers (`xid`, `commit_lsn`, `lsn`, `batch_uuid`) belong in `tracing` fields,
  not metric labels.
- Three lag numbers are distinct and all wanted: replication lag (WAL not yet confirmed), retained WAL
  (disk the slot pins), and feedback age (seconds since last standby update) — don't collapse them.
- `wal_status` is categorical; expose it as a small enum gauge (0 reserved / 1 unreserved / 2 lost) and
  alert on ≥1, mirroring §1.9's "alert well before the cap."
- Do **not** alert on transform/raw-append lag being high after an outage — that's healthy catch-up, the
  same reason liveness isn't tied to lag. Alert on backlog *growth that doesn't drain*, not on absolute lag.
- Register metrics once (idempotent) — re-registering on reconnect will panic or double-count with some
  exporters.

## References

- Design: `../../architecture.md#observability`, `#19-slot-liveness--heartbeat--keepalive`;
  `../../walrus-pg-sink.md#43-probes--get-these-exactly-right`.
- Prev: [PR 4.9](./pr-4.9-kubernetes-manifests.md) · Next: [PR 4.11](./pr-4.11-deferred-goal-scaffolding.md) ·
  [Roadmap](../README.md)
