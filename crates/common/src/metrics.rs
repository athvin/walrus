//! Prometheus metrics for both binaries (PR 4.10).
//!
//! The design's Observability section enumerates a fixed set of series; this module owns their **stable
//! names** (a rename breaks the committed dashboard + alerts), installs the process-wide Prometheus
//! recorder, and renders the `/metrics` text exposition the health server serves.
//!
//! Two properties keep this cheap and safe:
//! - The `metrics` façade macros are **no-ops until a recorder is installed**, so every instrumentation
//!   call sprinkled through the pipeline is inert in unit/integration tests that never call [`init`].
//! - [`init`] both *describes* and *zero-initialises* every global series, so a fresh `/metrics` lists
//!   the whole catalogue (the scrape tests assert this) before any real traffic moves a needle.
//!
//! Scope note (this PR *exposes*, it does not change pipeline behaviour): series computable at an
//! existing call site are populated there via the helpers below; the few that would need a **new**
//! query — replication-lag / retained-WAL (a `pg_current_wal_lsn` / `pg_replication_slots` poll),
//! files-ready / ddl-pending backlog counts, dead-letter failed-file counts, and the not-yet-wired
//! pause-poll counter — are registered (so the dashboard/alerts have a target) but left at zero here.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

/// Stable metric-name constants. Call sites and the scrape tests import these so a rename is a single
/// edit that the tests catch. Loader series are per-table, labelled by [`names::TABLE_LABEL`].
pub mod names {
    // --- sink (global) ---
    pub const SINK_REPLICATION_LAG_BYTES: &str = "walrus_sink_replication_lag_bytes";
    pub const SINK_SLOT_RETAINED_WAL_BYTES: &str = "walrus_sink_slot_retained_wal_bytes";
    /// Categorical gauge: 0 reserved · 1 unreserved · 2 lost (alert on ≥ 1).
    pub const SINK_WAL_STATUS: &str = "walrus_sink_wal_status";
    pub const SINK_HEARTBEAT_CONFIRMED_AGE_SECONDS: &str =
        "walrus_sink_heartbeat_confirmed_age_seconds";
    pub const SINK_HEARTBEAT_ROUNDTRIP_AGE_SECONDS: &str =
        "walrus_sink_heartbeat_roundtrip_age_seconds";
    pub const SINK_BEAT_SEQ_GAP: &str = "walrus_sink_beat_seq_gap";
    pub const SINK_FEEDBACK_AGE_SECONDS: &str = "walrus_sink_feedback_age_seconds";
    pub const SINK_BATCH_FLUSH_LATENCY_SECONDS: &str = "walrus_sink_batch_flush_latency_seconds";
    pub const SINK_PARQUET_ROWS_WRITTEN: &str = "walrus_sink_parquet_rows_written_total";
    pub const SINK_INFLIGHT_BYTES: &str = "walrus_sink_inflight_bytes";
    pub const SINK_SPILL_COUNT: &str = "walrus_sink_spill_total";
    pub const SINK_SPECULATIVE_OPEN_TXN_BYTES: &str = "walrus_sink_speculative_open_txn_bytes";
    pub const SINK_PAUSE_POLL_COUNT: &str = "walrus_sink_pause_poll_total";
    pub const SINK_ABORTED_TXN_COUNT: &str = "walrus_sink_aborted_txn_total";
    pub const SINK_FAILED_FILE_COUNT: &str = "walrus_sink_failed_file_total";

    // --- loader (per-table; labelled by TABLE_LABEL = "schema.table") ---
    pub const LOADER_FILES_READY: &str = "walrus_loader_files_ready";
    pub const LOADER_RAW_APPEND_LAG_BYTES: &str = "walrus_loader_raw_append_lag_bytes";
    pub const LOADER_TRANSFORM_LAG_BYTES: &str = "walrus_loader_transform_lag_bytes";
    pub const LOADER_RAW_ROW_COUNT: &str = "walrus_loader_raw_row_count";
    pub const LOADER_RAW_FILE_BYTES: &str = "walrus_loader_raw_file_bytes";
    pub const LOADER_DDL_PENDING: &str = "walrus_loader_ddl_pending";
    pub const LOADER_FAILED_FILE_COUNT: &str = "walrus_loader_failed_file_total";

    /// The one label on every loader series — a fully-qualified `schema.table`. Bounded cardinality:
    /// per-table, **never** per-row/xid/batch (those high-cardinality ids live in `tracing` fields).
    pub const TABLE_LABEL: &str = "table";

    /// Every global (unlabelled) series, for zero-init + the sink scrape test.
    pub const SINK_ALL: &[&str] = &[
        SINK_REPLICATION_LAG_BYTES,
        SINK_SLOT_RETAINED_WAL_BYTES,
        SINK_WAL_STATUS,
        SINK_HEARTBEAT_CONFIRMED_AGE_SECONDS,
        SINK_HEARTBEAT_ROUNDTRIP_AGE_SECONDS,
        SINK_BEAT_SEQ_GAP,
        SINK_FEEDBACK_AGE_SECONDS,
        SINK_BATCH_FLUSH_LATENCY_SECONDS,
        SINK_PARQUET_ROWS_WRITTEN,
        SINK_INFLIGHT_BYTES,
        SINK_SPILL_COUNT,
        SINK_SPECULATIVE_OPEN_TXN_BYTES,
        SINK_PAUSE_POLL_COUNT,
        SINK_ABORTED_TXN_COUNT,
        SINK_FAILED_FILE_COUNT,
    ];

    /// Every per-table loader series, for per-table zero-init + the loader scrape test.
    pub const LOADER_ALL: &[&str] = &[
        LOADER_FILES_READY,
        LOADER_RAW_APPEND_LAG_BYTES,
        LOADER_TRANSFORM_LAG_BYTES,
        LOADER_RAW_ROW_COUNT,
        LOADER_RAW_FILE_BYTES,
        LOADER_DDL_PENDING,
        LOADER_FAILED_FILE_COUNT,
    ];
}

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install the process-wide Prometheus recorder and register every global series. Idempotent: safe to
/// call from `main` and from each scrape test; later calls are no-ops. Until this runs, the call-site
/// helpers below do nothing.
pub fn init() {
    HANDLE.get_or_init(|| {
        let handle = PrometheusBuilder::new()
            .install_recorder()
            .expect("install a global Prometheus recorder exactly once");
        describe_all();
        zero_init_global();
        handle
    });
}

/// Render the current Prometheus text exposition. Empty until [`init`] installs the recorder.
pub fn render() -> String {
    HANDLE
        .get()
        .map(PrometheusHandle::render)
        .unwrap_or_default()
}

/// Zero-init every per-table loader series for one `schema.table`, so it renders before the first real
/// update. Called once per owned table at loader bootstrap, and by the loader scrape test for a demo
/// table. No-op until [`init`] runs.
pub fn init_table_series(table: &str) {
    for name in names::LOADER_ALL {
        if name.ends_with("_total") {
            metrics::counter!(*name, names::TABLE_LABEL => table.to_string()).increment(0);
        } else {
            metrics::gauge!(*name, names::TABLE_LABEL => table.to_string()).set(0.0);
        }
    }
}

fn describe_all() {
    use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};
    describe_gauge!(
        names::SINK_REPLICATION_LAG_BYTES,
        Unit::Bytes,
        "WAL not yet confirmed: pg_current_wal_lsn − confirmed_flush_lsn"
    );
    describe_gauge!(
        names::SINK_SLOT_RETAINED_WAL_BYTES,
        Unit::Bytes,
        "WAL bytes the slot pins on disk"
    );
    describe_gauge!(
        names::SINK_WAL_STATUS,
        "slot wal_status: 0 reserved, 1 unreserved, 2 lost"
    );
    describe_gauge!(
        names::SINK_HEARTBEAT_CONFIRMED_AGE_SECONDS,
        Unit::Seconds,
        "seconds since the last heartbeat round-trip confirmed"
    );
    describe_gauge!(
        names::SINK_HEARTBEAT_ROUNDTRIP_AGE_SECONDS,
        Unit::Seconds,
        "age of the last heartbeat write→observe-return (the slot-liveness signal)"
    );
    describe_gauge!(
        names::SINK_BEAT_SEQ_GAP,
        "gap between the latest sent and last observed heartbeat beat_seq"
    );
    describe_gauge!(
        names::SINK_FEEDBACK_AGE_SECONDS,
        Unit::Seconds,
        "seconds since the last standby-status feedback (keep well under wal_sender_timeout)"
    );
    describe_histogram!(
        names::SINK_BATCH_FLUSH_LATENCY_SECONDS,
        Unit::Seconds,
        "batch flush latency: encode → Parquet → S3 PUT → manifest commit"
    );
    describe_counter!(
        names::SINK_PARQUET_ROWS_WRITTEN,
        "total rows PUT to object storage as Parquet (throughput)"
    );
    describe_gauge!(
        names::SINK_INFLIGHT_BYTES,
        Unit::Bytes,
        "aggregate in-memory buffered bytes across all builders"
    );
    describe_counter!(
        names::SINK_SPILL_COUNT,
        "memory-ceiling flush / speculative spill events"
    );
    describe_gauge!(
        names::SINK_SPECULATIVE_OPEN_TXN_BYTES,
        Unit::Bytes,
        "bytes staged speculatively for open streamed txns"
    );
    describe_counter!(
        names::SINK_PAUSE_POLL_COUNT,
        "back-pressure pause-poll activations"
    );
    describe_counter!(
        names::SINK_ABORTED_TXN_COUNT,
        "streamed transactions (or subtransactions) that aborted"
    );
    describe_counter!(
        names::SINK_FAILED_FILE_COUNT,
        "files that failed to write / PUT"
    );

    describe_gauge!(
        names::LOADER_FILES_READY,
        "manifest files in state 'ready' awaiting apply, per table"
    );
    describe_gauge!(
        names::LOADER_RAW_APPEND_LAG_BYTES,
        Unit::Bytes,
        "sink lsn_end − raw_appended_lsn, per table (Phase-A backlog)"
    );
    describe_gauge!(
        names::LOADER_TRANSFORM_LAG_BYTES,
        Unit::Bytes,
        "raw_appended_lsn − transformed_lsn, per table (Phase-B backlog)"
    );
    describe_gauge!(
        names::LOADER_RAW_ROW_COUNT,
        "<table>_raw row count, per table"
    );
    describe_gauge!(
        names::LOADER_RAW_FILE_BYTES,
        Unit::Bytes,
        ".duckdb file size, per table"
    );
    describe_gauge!(
        names::LOADER_DDL_PENDING,
        "DDL events not yet applied, per table"
    );
    describe_counter!(
        names::LOADER_FAILED_FILE_COUNT,
        "files the loader failed to apply, per table"
    );
}

fn zero_init_global() {
    for name in names::SINK_ALL {
        if *name == names::SINK_BATCH_FLUSH_LATENCY_SECONDS {
            // A histogram only appears in the exposition once it has a sample; seed one 0s observation so
            // the series (and the dashboard panel) exists from startup. Negligible against real traffic.
            metrics::histogram!(*name).record(0.0);
        } else if name.ends_with("_total") {
            metrics::counter!(*name).increment(0);
        } else {
            metrics::gauge!(*name).set(0.0);
        }
    }
}

// ---------------------------------------------------------------------------------------------------
// Call-site helpers. Each is a no-op until `init` installs the recorder, so the pipeline can call them
// unconditionally. Only the signals computable at an existing site are wired (see the module note).
// ---------------------------------------------------------------------------------------------------

/// Slot `wal_status` as the categorical gauge (0 reserved / 1 unreserved / 2 lost).
pub fn set_wal_status(code: u8) {
    metrics::gauge!(names::SINK_WAL_STATUS).set(code as f64);
}

/// One batch flush: its wall-clock latency and the row count written (Parquet throughput).
pub fn record_batch_flush(latency_secs: f64, rows: u64) {
    metrics::histogram!(names::SINK_BATCH_FLUSH_LATENCY_SECONDS).record(latency_secs);
    metrics::counter!(names::SINK_PARQUET_ROWS_WRITTEN).increment(rows);
}

/// A memory-ceiling flush / speculative spill happened.
pub fn inc_spill() {
    metrics::counter!(names::SINK_SPILL_COUNT).increment(1);
}

/// A streamed transaction (or subtransaction) aborted.
pub fn inc_aborted_txn() {
    metrics::counter!(names::SINK_ABORTED_TXN_COUNT).increment(1);
}

/// Current aggregate in-memory buffered bytes.
pub fn set_inflight_bytes(bytes: u64) {
    metrics::gauge!(names::SINK_INFLIGHT_BYTES).set(bytes as f64);
}

/// Phase-B transform lag for one table: `raw_appended_lsn − transformed_lsn` in bytes.
pub fn set_transform_lag(table: &str, bytes: u64) {
    metrics::gauge!(names::LOADER_TRANSFORM_LAG_BYTES, names::TABLE_LABEL => table.to_string())
        .set(bytes as f64);
}

/// Phase-A raw-append lag for one table: `sink lsn_end − raw_appended_lsn` in bytes.
pub fn set_raw_append_lag(table: &str, bytes: u64) {
    metrics::gauge!(names::LOADER_RAW_APPEND_LAG_BYTES, names::TABLE_LABEL => table.to_string())
        .set(bytes as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_lists_every_series() {
        init();
        init_table_series("public.demo");
        // Exercise a couple of helpers to prove the wired path renders too.
        set_wal_status(0);
        record_batch_flush(0.01, 4096);
        set_transform_lag("public.demo", 0);

        let text = render();
        for name in names::SINK_ALL {
            assert!(
                text.contains(name),
                "sink series {name} missing from /metrics"
            );
        }
        for name in names::LOADER_ALL {
            assert!(
                text.contains(name),
                "loader series {name} missing from /metrics"
            );
        }
    }
}
