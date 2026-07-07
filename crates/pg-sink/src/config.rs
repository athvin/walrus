//! `SinkConfig` — the fully-validated sink configuration (bootstrap step 1).
//!
//! Mirrors [`common::CommonConfig`]'s pattern: read a `WALRUS_`-prefixed environment (with an
//! optional file underneath) into a typed serde struct, then bounds-check it. **Invalid config is a
//! *terminal* bootstrap error** — a missing field or an out-of-range threshold becomes
//! [`ConfigError`] at the edge and maps to [`common::ExitCode::Config`] in `main`, never a panic
//! three modules later. Connectivity (control PG, S3) is a *separate, transient* bootstrap check.

use common::config::ObjectStoreConfig;
use common::TelemetryConfig;
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;

/// A cadence/deadline longer than an hour is almost certainly a misconfig, not an intent.
const MAX_DURATION: Duration = Duration::from_secs(60 * 60);

/// Fully-typed, bounds-validated sink configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SinkConfig {
    /// Control Postgres (manifest/checkpoint/registry).
    pub control_db_url: String,
    /// Source Postgres (the logical-replication origin).
    pub source_db_url: String,
    /// S3/MinIO staging bucket + endpoint + region.
    pub object_store: ObjectStoreConfig,
    /// Logging setup.
    pub telemetry: TelemetryConfig,
    /// Human tag for this process instance, e.g. `"walrus-pg-sink-0"`.
    pub instance: String,
    /// The single replication slot this sink owns.
    pub slot_name: String,
    /// The publication the slot streams.
    pub publication_name: String,
    /// Batch cadence — flush a file at least this often (see PR 2.23 / §1.3).
    #[serde(with = "humantime_serde")]
    pub max_fill: Duration,
    /// Fire an idle heartbeat only after the published tables have been idle this long (PR 2.27 / §1.9).
    #[serde(with = "humantime_serde")]
    pub heartbeat_idle_after: Duration,
    /// A beat un-returned after this long marks the sink `degraded` (observability, never a kill).
    #[serde(with = "humantime_serde")]
    pub heartbeat_roundtrip_deadline: Duration,
    /// Row-count flush threshold.
    pub max_rows: u64,
    /// Byte-size flush threshold.
    pub max_bytes: u64,
    /// Back-pressure ceiling on un-acked in-flight bytes.
    pub max_inflight_bytes: u64,
    /// Bootstrap retry budget: transient deps are retried until this elapses, then terminal.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Where the K8s health endpoints bind.
    pub health_addr: SocketAddr,
    /// If true, the sink creates/alters `publication_name` to cover the required tables; else a gap
    /// is terminal (the operator owns the source setup — PR 2.19 `migrations/source`).
    pub manage_publication: bool,
    /// `true` (default) = **strict** keys: a published user table with no usable replica identity is
    /// terminal. `false` = **lenient**: quarantine + alert + continue (surfaced in the `PkReport`).
    pub strict_keys: bool,
}

impl Default for SinkConfig {
    fn default() -> Self {
        SinkConfig {
            control_db_url: String::new(),
            source_db_url: String::new(),
            object_store: ObjectStoreConfig::default(),
            telemetry: TelemetryConfig::default(),
            instance: String::new(),
            slot_name: String::new(),
            publication_name: String::new(),
            max_fill: Duration::from_secs(5),
            heartbeat_idle_after: Duration::from_secs(10),
            heartbeat_roundtrip_deadline: Duration::from_secs(30),
            max_rows: 100_000,
            max_bytes: 128 * 1024 * 1024,
            max_inflight_bytes: 512 * 1024 * 1024,
            startup_deadline: Duration::from_secs(60),
            health_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            manage_publication: false,
            strict_keys: true,
        }
    }
}

/// A terminal configuration failure. `main` maps this to [`common::ExitCode::Config`].
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config load/parse failed: {0}")]
    Load(String),
    #[error("missing required field: {0}")]
    Missing(&'static str),
    #[error("field {field} out of bounds: {detail}")]
    OutOfBounds { field: &'static str, detail: String },
}

impl From<ConfigError> for common::Error {
    fn from(e: ConfigError) -> Self {
        common::Error::Config(e.to_string())
    }
}

impl SinkConfig {
    /// Load config: an optional `WALRUS_CONFIG` file underneath, `WALRUS_`-prefixed env on top (`__`
    /// marks nesting), then [`validate`](Self::validate). An invalid config can never escape as `Ok`.
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
        let figment = figment.merge(
            Env::prefixed("WALRUS_")
                .ignore(&["config", "CONFIG"])
                .split("__"),
        );
        let cfg: SinkConfig = figment
            .extract()
            .map_err(|e| ConfigError::Load(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// The validated idle-heartbeat settings (PR 2.27).
    pub fn heartbeat_config(&self) -> crate::heartbeat::HeartbeatConfig {
        crate::heartbeat::HeartbeatConfig {
            idle_after: self.heartbeat_idle_after,
            roundtrip_deadline: self.heartbeat_roundtrip_deadline,
        }
    }

    /// The keyless-table policy for the source preflight (§1.1, PR 2.19).
    pub fn pk_mode(&self) -> crate::preflight::PkMode {
        if self.strict_keys {
            crate::preflight::PkMode::Strict
        } else {
            crate::preflight::PkMode::Lenient
        }
    }

    /// Bounds-check every field. Pure and offline — no sockets. Any violation is terminal.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            ("control_db_url", &self.control_db_url),
            ("source_db_url", &self.source_db_url),
            ("instance", &self.instance),
            ("slot_name", &self.slot_name),
            ("publication_name", &self.publication_name),
            ("object_store.bucket", &self.object_store.bucket),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::Missing(field));
            }
        }
        duration_bound("max_fill", self.max_fill)?;
        duration_bound("startup_deadline", self.startup_deadline)?;
        duration_bound("heartbeat_idle_after", self.heartbeat_idle_after)?;
        duration_bound(
            "heartbeat_roundtrip_deadline",
            self.heartbeat_roundtrip_deadline,
        )?;
        if self.heartbeat_idle_after >= self.heartbeat_roundtrip_deadline {
            return Err(ConfigError::OutOfBounds {
                field: "heartbeat_idle_after",
                detail: format!(
                    "must be < heartbeat_roundtrip_deadline ({:?}) — a beat needs time to return",
                    self.heartbeat_roundtrip_deadline
                ),
            });
        }
        positive("max_rows", self.max_rows)?;
        positive("max_bytes", self.max_bytes)?;
        positive("max_inflight_bytes", self.max_inflight_bytes)?;
        if self.max_inflight_bytes < self.max_bytes {
            return Err(ConfigError::OutOfBounds {
                field: "max_inflight_bytes",
                detail: format!(
                    "must be ≥ max_bytes ({}) so at least one full batch can be in flight",
                    self.max_bytes
                ),
            });
        }
        Ok(())
    }
}

fn duration_bound(field: &'static str, d: Duration) -> Result<(), ConfigError> {
    if d.is_zero() {
        return Err(ConfigError::OutOfBounds {
            field,
            detail: "must be greater than zero".to_string(),
        });
    }
    if d > MAX_DURATION {
        return Err(ConfigError::OutOfBounds {
            field,
            detail: format!("{d:?} exceeds the {MAX_DURATION:?} ceiling"),
        });
    }
    Ok(())
}

fn positive(field: &'static str, v: u64) -> Result<(), ConfigError> {
    if v == 0 {
        return Err(ConfigError::OutOfBounds {
            field,
            detail: "must be greater than zero".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> SinkConfig {
        SinkConfig {
            control_db_url: "postgres://localhost/walrus_control".to_string(),
            source_db_url: "postgres://localhost/walrus".to_string(),
            object_store: ObjectStoreConfig {
                bucket: "walrus".to_string(),
                endpoint: Some("http://localhost:9000".to_string()),
                region: "us-east-1".to_string(),
            },
            instance: "walrus-pg-sink-0".to_string(),
            slot_name: "walrus_slot".to_string(),
            publication_name: "walrus_pub".to_string(),
            ..SinkConfig::default()
        }
    }

    #[test]
    fn a_fully_valid_config_passes() {
        assert!(valid().validate().is_ok());
    }

    #[test]
    fn a_missing_field_is_terminal() {
        let mut cfg = valid();
        cfg.slot_name = "   ".to_string(); // whitespace-only is still empty
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::Missing("slot_name")));
        // Maps to the terminal Config exit class.
        assert!(common::Error::from(err).is_terminal());
    }

    #[test]
    fn out_of_bounds_thresholds_are_terminal() {
        let mut cfg = valid();
        cfg.max_rows = 0;
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::OutOfBounds {
                field: "max_rows",
                ..
            }
        ));

        let mut cfg = valid();
        cfg.startup_deadline = Duration::ZERO;
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::OutOfBounds {
                field: "startup_deadline",
                ..
            }
        ));

        let mut cfg = valid();
        cfg.max_inflight_bytes = cfg.max_bytes - 1;
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::OutOfBounds {
                field: "max_inflight_bytes",
                ..
            }
        ));
    }

    #[test]
    fn heartbeat_idle_after_must_be_below_roundtrip_deadline() {
        let mut cfg = valid();
        cfg.heartbeat_idle_after = Duration::from_secs(30);
        cfg.heartbeat_roundtrip_deadline = Duration::from_secs(30);
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::OutOfBounds {
                field: "heartbeat_idle_after",
                ..
            }
        ));
    }

    #[test]
    fn config_error_maps_to_config_exit_code() {
        let e = common::Error::from(ConfigError::Missing("control_db_url"));
        assert_eq!(e.exit_code(), common::ExitCode::Config);
    }
}
