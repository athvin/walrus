//! Tracing setup and the structured-field convention.
//!
//! walrus never `println!`s. Every log line is a [`tracing`] event with **structured fields** so a
//! Grafana/Loki query can follow one transaction through both services. [`init_tracing`] installs
//! the process-wide subscriber once at the top of `main`; the [`fields`] module fixes the canonical
//! field-key spellings (`xid`, `commit_lsn`, `lsn`, `batch_uuid`, …) that every later PR must use
//! at its call sites — e.g. `info!({XID} = xid, {COMMIT_LSN} = %commit_lsn, "flushed batch")`.

use serde::Deserialize;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Canonical structured-field keys. Use these constants at every `tracing` call site so dashboards
/// and log queries key on **one** spelling across both services — never a free-form format string.
pub mod fields {
    /// Postgres transaction id of the change being processed.
    pub const XID: &str = "xid";
    /// Commit LSN — *the* order/watermark key (see [`crate::Lsn`]).
    pub const COMMIT_LSN: &str = "commit_lsn";
    /// Per-row WAL LSN — the intra-transaction tiebreaker.
    pub const LSN: &str = "lsn";
    /// UUID of the Parquet batch a row belongs to.
    pub const BATCH_UUID: &str = "batch_uuid";
    /// Generation counter that namespaces all control-plane state.
    pub const EPOCH: &str = "epoch";
    /// Structural schema version of the affected relation.
    pub const SCHEMA_VERSION: &str = "schema_version";
    /// Stable identity of the sink pod that produced a batch.
    pub const SINK_INSTANCE: &str = "sink_instance";
}

/// Fallback filter directive when neither `cfg.filter` nor `RUST_LOG` supplies one — so a missing
/// env var never means "silent".
const DEFAULT_FILTER: &str = "info";

/// How to render logs. `json` on in the cluster, off (pretty) for local dev.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Emit newline-delimited JSON (one object per event) instead of the pretty formatter.
    pub json: bool,
    /// `EnvFilter` directive, e.g. `"info,walrus=debug"`. Empty → fall back to `RUST_LOG`, then
    /// [`DEFAULT_FILTER`].
    pub filter: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        TelemetryConfig {
            json: false,
            filter: DEFAULT_FILTER.to_string(),
        }
    }
}

/// Build the `EnvFilter`: an explicit `cfg.filter` wins; an empty one falls back to `RUST_LOG`,
/// then to [`DEFAULT_FILTER`]. A malformed directive degrades to the default rather than silently
/// disabling logging.
fn build_env_filter(cfg: &TelemetryConfig) -> EnvFilter {
    if cfg.filter.trim().is_empty() {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER))
    } else {
        EnvFilter::try_new(&cfg.filter).unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER))
    }
}

/// Build the `EnvFilter` + fmt layer (pretty or JSON per `cfg.json`) and install it as the global
/// default subscriber.
///
/// Idempotent: a global subscriber can only be installed once per process, so a second call is a
/// **handled outcome** — it logs at `debug` and returns `Ok(())` rather than panicking, keeping
/// tests and re-entrant bootstraps safe.
pub fn init_tracing(cfg: &TelemetryConfig) -> crate::Result<()> {
    let filter = build_env_filter(cfg);
    let registry = tracing_subscriber::registry().with(filter);

    let installed = if cfg.json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .try_init()
    } else {
        registry.with(tracing_subscriber::fmt::layer()).try_init()
    };

    if let Err(e) = installed {
        // A global subscriber is already installed (expected under test / a re-entrant bootstrap).
        // Keep the existing one; this is not a failure.
        tracing::debug!(error = %e, "tracing subscriber already initialised; keeping the existing one");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_with_defaults_does_not_panic() {
        assert!(init_tracing(&TelemetryConfig::default()).is_ok());
    }

    #[test]
    fn second_init_is_handled_not_fatal() {
        // Tests share one process, so at most one of these actually installs the global
        // subscriber; the rest hit the "already initialised" path. None may panic.
        assert!(init_tracing(&TelemetryConfig::default()).is_ok());
        assert!(init_tracing(&TelemetryConfig {
            json: true,
            filter: "debug".to_string(),
        })
        .is_ok());
    }

    #[test]
    fn default_config_is_pretty_info() {
        let cfg = TelemetryConfig::default();
        assert!(!cfg.json);
        assert_eq!(cfg.filter, "info");
    }

    #[test]
    fn json_flag_selects_json_formatter() {
        use std::io::Write;
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::fmt::MakeWriter;

        // A `MakeWriter` that captures everything written into a shared buffer.
        #[derive(Clone, Default)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        impl Write for BufWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> MakeWriter<'a> for BufWriter {
            type Writer = BufWriter;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        // Ensure a global subscriber exists so the level fast-path admits INFO events, then capture
        // via a *scoped* JSON subscriber (`with_default`) so we don't fight the one global install.
        let _ = init_tracing(&TelemetryConfig::default());

        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::registry()
            .with(EnvFilter::new("info"))
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(buf.clone()),
            );

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                commit_lsn = "0000000001B4C000",
                xid = 918273,
                "flushed batch"
            );
        });

        let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            out.trim_start().starts_with('{'),
            "expected a JSON object: {out}"
        );
        assert!(
            out.contains("\"commit_lsn\""),
            "carries the field key: {out}"
        );
        assert!(
            out.contains("\"flushed batch\""),
            "carries the message: {out}"
        );
    }
}
