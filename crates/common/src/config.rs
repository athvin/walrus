//! Shared, bounds-validated configuration for both walrus services.
//!
//! Bootstrap step 1 for *both* services is "load & validate config; invalid → terminal". This
//! module reads env (with an optional file underneath) into typed serde structs and then
//! [`CommonConfig::validate`]s them, so an out-of-range cadence or an empty DB URL becomes a
//! terminal [`Error::Config`] *at the edge* — never a panic three modules later. Service-specific
//! knobs (`SinkConfig`, `LoaderConfig`) embed [`CommonConfig`] in their own crates.

use crate::telemetry::TelemetryConfig;
use crate::{Error, Result};
use serde::Deserialize;
use std::time::Duration;

/// Upper bound on `startup_deadline` — a bootstrap that would retry transient deps for longer than
/// an hour is almost certainly a misconfiguration, not an intent.
const MAX_STARTUP_DEADLINE: Duration = Duration::from_secs(60 * 60);

/// Configuration shared by both walrus services. Service-specific knobs embed this.
/// Flattening is deferred; see `docs/implementation/notes/rust-skills/serde-flatten.md`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CommonConfig {
    /// Control Postgres connection string (holds manifest/checkpoint/registry).
    pub control_db_url: String,
    /// S3/MinIO staging bucket + endpoint.
    pub object_store: ObjectStoreConfig,
    /// Logging setup (PR 0.4).
    pub telemetry: TelemetryConfig,
    /// Bootstrap retry budget: transient deps are retried until this elapses, then terminal.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Human tag for this process instance, e.g. `"walrus-pg-sink-0"`.
    pub instance: String,
}

/// Where staged Parquet lives.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ObjectStoreConfig {
    pub bucket: String,
    /// `None` = real AWS; `Some` = MinIO / localstack.
    pub endpoint: Option<String>,
    pub region: String,
}

impl Default for CommonConfig {
    fn default() -> Self {
        CommonConfig {
            control_db_url: String::new(),
            object_store: ObjectStoreConfig::default(),
            telemetry: TelemetryConfig::default(),
            startup_deadline: Duration::from_secs(60),
            instance: String::new(),
        }
    }
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig {
            bucket: String::new(),
            endpoint: None,
            region: "us-east-1".to_string(),
        }
    }
}

impl CommonConfig {
    /// Load config: an optional file at `WALRUS_CONFIG` (TOML or YAML by extension) underneath,
    /// `WALRUS_`-prefixed environment on top (`__` marks nesting), then [`validate`](Self::validate).
    /// An invalid config can never escape as `Ok`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] — always terminal — when the configured file is unreadable or
    /// invalid, an environment value cannot be deserialized (including unknown fields), or the
    /// merged configuration fails [`Self::validate`].
    pub fn load() -> Result<Self> {
        use figment::Figment;
        use figment::providers::{Env, Format, Toml, Yaml};

        let mut figment = Figment::new();

        // Optional file underneath, chosen by extension (default TOML). `var_os`, not `var`: the
        // only thing the `Result` adds here is a `VarError` for an `if let Ok` to drop, and its two
        // variants are not the same event. `NotPresent` is the documented no-file case; `NotUnicode`
        // is an operator who set a path this process then ignored in silence, leaving `validate()`
        // to blame a missing field instead. `Option` is the honest type — `None` *is* "unset" —
        // and `PathBuf: From<OsString>`, so a path the OS accepts is simply opened.
        if let Some(path) = std::env::var_os("WALRUS_CONFIG") {
            let path = std::path::PathBuf::from(path);
            let ext = path.extension().and_then(|e| e.to_str());
            let is_yaml = matches!(ext, Some("yaml" | "yml"));
            figment = if is_yaml {
                figment.merge(Yaml::file(&path))
            } else {
                figment.merge(Toml::file(&path))
            };
        }

        // Environment on top. `WALRUS_CONFIG` is the file selector, not a field — ignore it so
        // `deny_unknown_fields` doesn't reject it as a stray `config` key.
        let figment = figment.merge(
            Env::prefixed("WALRUS_")
                .ignore(&["config", "CONFIG"])
                .split("__"),
        );

        let cfg: CommonConfig = figment
            .extract()
            .map_err(|e| Error::Config(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Bounds-check every field. Pure and offline — no sockets. Any violation is a terminal
    /// [`Error::Config`]; connectivity is a separate *transient* bootstrap check in the bins.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] when a required string is empty, `startup_deadline` is zero, or
    /// that deadline exceeds the supported ceiling. All such failures are terminal.
    pub fn validate(&self) -> Result<()> {
        // Assert the constant ceiling; configured input remains runtime, Result-returning data.
        const {
            assert!(
                !MAX_STARTUP_DEADLINE.is_zero(),
                "MAX_STARTUP_DEADLINE must be nonzero or every positive deadline exceeds the ceiling"
            );
        }

        if self.control_db_url.trim().is_empty() {
            return Err(Error::Config(
                "control_db_url must not be empty".to_string(),
            ));
        }
        if self.object_store.bucket.trim().is_empty() {
            return Err(Error::Config(
                "object_store.bucket must not be empty".to_string(),
            ));
        }
        if self.instance.trim().is_empty() {
            return Err(Error::Config("instance must not be empty".to_string()));
        }
        if self.startup_deadline.is_zero() {
            return Err(Error::Config(
                "startup_deadline must be greater than zero".to_string(),
            ));
        }
        if self.startup_deadline > MAX_STARTUP_DEADLINE {
            return Err(Error::Config(format!(
                "startup_deadline {:?} exceeds the {MAX_STARTUP_DEADLINE:?} ceiling",
                self.startup_deadline
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
