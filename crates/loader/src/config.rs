//! `LoaderConfig` — the fully-validated loader configuration (bootstrap step 0). Mirrors the sink's
//! pattern: `WALRUS_`-prefixed env (optional file underneath) → typed serde struct → bounds-check.
//! Invalid config is a **terminal** bootstrap error → [`common::ExitCode::Config`].

use common::config::ObjectStoreConfig;
use common::TelemetryConfig;
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoaderConfig {
    /// Control Postgres (leases / manifest / checkpoints).
    pub control_db_url: String,
    /// S3/MinIO staging bucket the sink writes and the loader reads.
    pub object_store: ObjectStoreConfig,
    pub telemetry: TelemetryConfig,
    /// This pod's identity — the lease `owner_pod`.
    pub instance: String,
    /// Local directory holding the `<table>.duckdb` files (an RWO PVC in production).
    pub duckdb_dir: String,
    /// The ownership-lease TTL; renewed well under it.
    #[serde(with = "humantime_serde")]
    pub lease_ttl: Duration,
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
            instance: String::new(),
            duckdb_dir: String::new(),
            lease_ttl: Duration::from_secs(30),
            startup_deadline: Duration::from_secs(60),
            health_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        }
    }
}

impl LoaderConfig {
    pub fn load() -> Result<Self, ConfigError> {
        use figment::providers::{Env, Format, Toml, Yaml};
        use figment::Figment;

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
        if self.lease_ttl.is_zero() {
            return Err(ConfigError("lease_ttl must be > 0".into()));
        }
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
