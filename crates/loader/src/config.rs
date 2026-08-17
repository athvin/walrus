//! `LoaderConfig` — the fully-validated loader configuration (bootstrap step 0). Mirrors the sink's
//! pattern: `WALRUS_`-prefixed env (optional file underneath) → typed serde struct → bounds-check.
//! Invalid config is a **terminal** bootstrap error → [`common::ExitCode::Config`].

use common::TelemetryConfig;
use common::config::ObjectStoreConfig;
use serde::Deserialize;
use std::net::SocketAddr;
use std::num::NonZeroI64;
use std::time::Duration;

const fn nonzero_i64(value: i64) -> NonZeroI64 {
    match NonZeroI64::new(value) {
        Some(value) => value,
        None => NonZeroI64::MIN,
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoaderConfig {
    /// Control Postgres (leases / manifest / checkpoints).
    pub control_db_url: String,
    /// S3/MinIO staging bucket the sink writes and the loader reads.
    pub object_store: ObjectStoreConfig,
    pub telemetry: TelemetryConfig,
    /// Tokio worker threads. `None` uses `available_parallelism()`; configured values must be
    /// within 1..=64. A loader pod wants a small value because every apply loop shares one
    /// `LocalSet` thread; `WALRUS_WORKER_THREADS=2` is plenty for the remaining async work.
    pub worker_threads: Option<usize>,
    /// This pod's identity — the lease `owner_pod`.
    pub instance: String,
    /// Local directory holding the `<table>.duckdb` files (an RWO PVC in production).
    pub duckdb_dir: String,
    /// The ownership-lease TTL; renewed well under it.
    #[serde(with = "humantime_serde")]
    pub lease_ttl: Duration,
    /// The apply-loop poll cadence (incremental Phase A + Phase B).
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,
    /// The compaction cadence (PR 3.11) — the per-table full-rebuild + retention prune. **Distinct** from
    /// `poll_interval`, slower, and run on the SAME worker thread serialized after an apply cycle (it
    /// holds the exclusive writer and needs ~2× transient space, so size it for low-traffic windows).
    #[serde(with = "humantime_serde")]
    pub compaction_interval: Duration,
    /// Raw retention as an LSN-byte lag behind `transformed_lsn`: compaction prunes `<table>_raw` below
    /// `transformed_lsn - retention_lsn_lag`. The rebuild's mirror baseline makes even an aggressive
    /// prune lossless (a pruned value survives as the current mirror row).
    pub retention_lsn_lag: u64,
    /// Manifest files claimed per Phase-A cycle. A zero cap would make the loop claim nothing
    /// forever, so the invariant is encoded in the type and enforced during deserialization.
    pub max_files_per_cycle: NonZeroI64,
    /// Bootstrap retry budget for transient deps.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Where the K8s health endpoints bind.
    pub health_addr: SocketAddr,
}

impl Default for LoaderConfig {
    fn default() -> Self {
        LoaderConfig {
            control_db_url: String::new(),
            object_store: ObjectStoreConfig::default(),
            telemetry: TelemetryConfig::default(),
            worker_threads: None,
            instance: String::new(),
            duckdb_dir: String::new(),
            lease_ttl: Duration::from_secs(30),
            poll_interval: Duration::from_secs(5),
            compaction_interval: Duration::from_secs(3600),
            retention_lsn_lag: 16 << 20, // 16 MiB of WAL retained behind transformed_lsn
            max_files_per_cycle: nonzero_i64(32),
            startup_deadline: Duration::from_secs(60),
            health_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        }
    }
}

impl LoaderConfig {
    /// Load the optional config file under `WALRUS_` environment overrides, then validate it.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a file or environment value cannot be deserialized, an unknown
    /// field is present, or the merged configuration fails [`Self::validate`].
    pub fn load() -> Result<Self, ConfigError> {
        use figment::Figment;
        use figment::providers::{Env, Format, Toml, Yaml};

        let mut figment = Figment::new();
        if let Ok(path) = std::env::var("WALRUS_CONFIG") {
            let path = std::path::PathBuf::from(path);
            let is_yaml = matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("yaml") | Some("yml")
            );
            figment = if is_yaml {
                figment.merge(Yaml::file(&path))
            } else {
                figment.merge(Toml::file(&path))
            };
        }
        let cfg: LoaderConfig = figment
            .merge(
                Env::prefixed("WALRUS_")
                    .ignore(&["config", "CONFIG"])
                    .split("__"),
            )
            .extract()
            .map_err(|e| ConfigError(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate required strings and the ownership-lease duration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a required value is empty, a cadence is zero, `lease_ttl` is too
    /// short for renewal, or the runtime worker count is outside its documented bound. These are
    /// terminal failures.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, v) in [
            ("control_db_url", &self.control_db_url),
            ("instance", &self.instance),
            ("duckdb_dir", &self.duckdb_dir),
            ("object_store.bucket", &self.object_store.bucket),
        ] {
            if v.trim().is_empty() {
                return Err(ConfigError(format!("missing required field: {field}")));
            }
        }
        const MIN_LEASE_TTL: Duration = Duration::from_secs(3);
        if self.lease_ttl < MIN_LEASE_TTL {
            return Err(ConfigError(format!(
                "lease_ttl {:?} is too short — renewal runs at TTL/3 and must land inside the TTL; \
                 use >= {MIN_LEASE_TTL:?}",
                self.lease_ttl
            )));
        }
        for (field, value) in [
            ("poll_interval", self.poll_interval),
            ("compaction_interval", self.compaction_interval),
        ] {
            if value.is_zero() {
                return Err(ConfigError(format!("{field} must be greater than zero")));
            }
        }
        common::runtime::validate_worker_threads(self.worker_threads)
            .map_err(|detail| ConfigError(format!("worker_threads: {detail}")))?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid loader configuration: {0}")]
pub struct ConfigError(pub String);

impl From<ConfigError> for common::Error {
    fn from(e: ConfigError) -> Self {
        common::Error::Config(e.0)
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
