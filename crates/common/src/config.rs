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
    pub fn load() -> Result<Self> {
        use figment::providers::{Env, Format, Toml, Yaml};
        use figment::Figment;

        let mut figment = Figment::new();

        // Optional file underneath, chosen by extension (default TOML).
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
    pub fn validate(&self) -> Result<()> {
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
mod tests {
    use super::*;

    /// A config that passes `validate()`, so a test can mutate exactly one field to prove the
    /// bound it targets.
    fn valid_config() -> CommonConfig {
        CommonConfig {
            control_db_url: "postgres://localhost/walrus".to_string(),
            object_store: ObjectStoreConfig {
                bucket: "walrus".to_string(),
                endpoint: None,
                region: "us-east-1".to_string(),
            },
            telemetry: TelemetryConfig::default(),
            startup_deadline: Duration::from_secs(30),
            instance: "walrus-test-0".to_string(),
        }
    }

    /// Run `body` inside a hermetic `figment::Jail` (fresh temp CWD + scoped env), so config tests
    /// don't leak env across the shared test process. The one `#[allow]` lives here: `Jail`'s
    /// closure must return `Result<(), figment::Error>`, and that error type is large — a
    /// constraint of figment's API, not of our code.
    #[allow(clippy::result_large_err)]
    fn in_jail(body: impl FnOnce(&mut figment::Jail)) {
        figment::Jail::expect_with(|jail| {
            body(jail);
            Ok(())
        });
    }

    #[test]
    fn loads_from_env_over_file() {
        in_jail(|jail| {
            jail.create_file(
                "walrus.toml",
                r#"
                    control_db_url = "postgres://file/db"
                    instance = "from-file"
                    startup_deadline = "45s"

                    [object_store]
                    bucket = "file-bucket"
                    region = "eu-west-1"
                "#,
            )
            .unwrap();
            jail.set_env("WALRUS_CONFIG", "walrus.toml");
            // Env overlays the file for these two (one top-level, one nested).
            jail.set_env("WALRUS_CONTROL_DB_URL", "postgres://env/db");
            jail.set_env("WALRUS_OBJECT_STORE__BUCKET", "env-bucket");

            let cfg = CommonConfig::load().expect("valid config should load");
            assert_eq!(cfg.control_db_url, "postgres://env/db"); // env wins
            assert_eq!(cfg.object_store.bucket, "env-bucket"); // env wins (nested, deep-merged)
            assert_eq!(cfg.object_store.region, "eu-west-1"); // untouched → from file
            assert_eq!(cfg.instance, "from-file"); // from file
            assert_eq!(cfg.startup_deadline, Duration::from_secs(45)); // humantime from file
        });
    }

    #[test]
    fn humantime_durations_parse() {
        in_jail(|jail| {
            jail.set_env("WALRUS_CONTROL_DB_URL", "postgres://x/y");
            jail.set_env("WALRUS_INSTANCE", "i");
            jail.set_env("WALRUS_OBJECT_STORE__BUCKET", "b");
            jail.set_env("WALRUS_STARTUP_DEADLINE", "250ms");

            let cfg = CommonConfig::load().expect("valid config should load");
            assert_eq!(cfg.startup_deadline, Duration::from_millis(250));
        });
    }

    #[test]
    fn unknown_key_is_rejected() {
        in_jail(|jail| {
            jail.set_env("WALRUS_CONTROL_DB_URL", "postgres://x/y");
            jail.set_env("WALRUS_INSTANCE", "i");
            jail.set_env("WALRUS_OBJECT_STORE__BUCKET", "b");
            jail.set_env("WALRUS_NONSENSE", "boom"); // typo'd ConfigMap key

            let err = CommonConfig::load().expect_err("unknown key must fail the load");
            assert!(
                matches!(err, Error::Config(_)) && err.is_terminal(),
                "unknown key must be a terminal Config error: {err:?}"
            );
        });
    }

    #[test]
    fn empty_control_db_url_is_config_error() {
        let mut cfg = valid_config();
        cfg.control_db_url = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)) && err.is_terminal());
    }

    #[test]
    fn empty_bucket_is_config_error() {
        let mut cfg = valid_config();
        cfg.object_store.bucket = "   ".to_string(); // whitespace-only is still empty
        assert!(matches!(cfg.validate().unwrap_err(), Error::Config(_)));
    }

    #[test]
    fn zero_startup_deadline_is_rejected() {
        let mut cfg = valid_config();
        cfg.startup_deadline = Duration::ZERO;
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, Error::Config(_)) && err.is_terminal());

        // An absurdly large deadline is rejected too.
        cfg.startup_deadline = Duration::from_secs(48 * 3600);
        assert!(matches!(cfg.validate().unwrap_err(), Error::Config(_)));
    }

    #[test]
    fn a_fully_valid_config_passes() {
        assert!(valid_config().validate().is_ok());
    }
}
