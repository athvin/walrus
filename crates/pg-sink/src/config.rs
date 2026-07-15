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
    /// `statement_timeout` for each initial-backfill copy session (PR 2.29). `0` = disabled — a huge
    /// table's snapshot copy must not be killed mid-flight (the whole backfill is bounded by the slot's
    /// WAL retention, not a per-statement clock).
    #[serde(with = "humantime_serde")]
    pub backfill_statement_timeout: Duration,
    /// Row-count flush threshold.
    pub max_rows: u64,
    /// Byte-size flush threshold.
    pub max_bytes: u64,
    /// Back-pressure ceiling on aggregate in-flight buffered bytes (§1.3) — process-wide, distinct from
    /// the per-batch `max_bytes`. Must sit **below** the pod memory limit so a graceful spill beats a
    /// cgroup OOM-kill; `logical_decoding_work_mem` does NOT bound this.
    pub max_inflight_bytes: u64,
    /// Pause-poll backstop **activate** ratio of `max_inflight_bytes` (high band).
    pub backpressure_activate_ratio: f64,
    /// Pause-poll backstop **resume** ratio (low band) — must be `< activate` so intake doesn't flap.
    pub backpressure_resume_ratio: f64,
    /// Bootstrap retry budget: transient deps are retried until this elapses, then terminal.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Where the K8s health endpoints bind.
    pub health_addr: SocketAddr,
    /// Concurrent single-table reload exports (PR 6.4 / reload H6). "Reload N tables" drains a
    /// queue this wide — a polite cap, never N simultaneous load spikes on the source. ≥ 1.
    pub max_concurrent_reloads: u64,
    /// Reload lease TTL (PR 6.4 / reload H7): a live exporter renews at TTL/3, so a died-mid-export
    /// sink is detectable within one TTL. Bounds-checked so the renewal cadence fits inside it.
    #[serde(with = "humantime_serde")]
    pub reload_lease_ttl: Duration,
    /// Rows per reload chunk (PR 6.5 / reload H2): each chunk is one short PK-ordered SELECT — no
    /// hours-long transaction pinning xmin. Bounds each statement; `max_concurrent_reloads` bounds
    /// tables. ≥ 1.
    pub reload_chunk_rows: u64,
    /// How long a chunk waits for its watermark echo before the reload fails loudly (PR 6.5 /
    /// reload H11): an unpublished signal table never echoes — this timeout turns that silent
    /// failure into a `failed` row naming the fix.
    #[serde(with = "humantime_serde")]
    pub reload_echo_timeout: Duration,
    /// How many times a reload may restart because DDL bumped its `schema_version` mid-export
    /// (PR 6.8 / reload H9). Every attempt is single-schema by construction; a schema change past
    /// chunk 1 invalidates the attempt and re-exports from zero at the new shape. This caps that
    /// churn so a migration-heavy window can't livelock a huge table's reload. `0` fails the first
    /// mid-export DDL; must be ≥ 0.
    pub reload_max_restarts: i32,
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
            backfill_statement_timeout: Duration::ZERO, // disabled — never kill a big table's copy

            max_rows: 100_000,
            max_bytes: 128 * 1024 * 1024,
            max_inflight_bytes: 512 * 1024 * 1024,
            backpressure_activate_ratio: 0.85,
            backpressure_resume_ratio: 0.75,
            startup_deadline: Duration::from_secs(60),
            health_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            max_concurrent_reloads: 2,
            reload_lease_ttl: Duration::from_secs(60),
            reload_chunk_rows: 10_000,
            reload_echo_timeout: Duration::from_secs(30),
            reload_max_restarts: 3,
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

    /// The validated backpressure hysteresis gate (PR 2.32).
    pub fn backpressure(&self) -> crate::memory::Backpressure {
        crate::memory::Backpressure::new(
            self.backpressure_activate_ratio,
            self.backpressure_resume_ratio,
        )
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
        positive("max_concurrent_reloads", self.max_concurrent_reloads)?;
        positive("reload_chunk_rows", self.reload_chunk_rows)?;
        duration_bound("reload_echo_timeout", self.reload_echo_timeout)?;
        duration_bound("reload_lease_ttl", self.reload_lease_ttl)?;
        // 0 is legal (fail on first mid-export DDL); only a negative cap is a misconfig.
        if self.reload_max_restarts < 0 {
            return Err(ConfigError::OutOfBounds {
                field: "reload_max_restarts",
                detail: format!(
                    "{} is negative — 0 disables restarts (first DDL fails the reload); use ≥ 0",
                    self.reload_max_restarts
                ),
            });
        }
        // The exporter renews at TTL/3 (crate::reload); a TTL under ~15s leaves too little slack
        // for a renewal round-trip before expiry — a misconfig, not an intent.
        if self.reload_lease_ttl < Duration::from_secs(15) {
            return Err(ConfigError::OutOfBounds {
                field: "reload_lease_ttl",
                detail: format!(
                    "{:?} is too short — renewal runs at TTL/3 and needs headroom; use ≥ 15s",
                    self.reload_lease_ttl
                ),
            });
        }
        if self.max_inflight_bytes < self.max_bytes {
            return Err(ConfigError::OutOfBounds {
                field: "max_inflight_bytes",
                detail: format!(
                    "must be ≥ max_bytes ({}) so at least one full batch can be in flight",
                    self.max_bytes
                ),
            });
        }
        // Hysteresis band: 0 < resume < activate < 1.0 so the backstop never flaps.
        if !(0.0 < self.backpressure_resume_ratio
            && self.backpressure_resume_ratio < self.backpressure_activate_ratio
            && self.backpressure_activate_ratio < 1.0)
        {
            return Err(ConfigError::OutOfBounds {
                field: "backpressure_activate_ratio",
                detail: format!(
                    "require 0 < resume ({}) < activate ({}) < 1.0",
                    self.backpressure_resume_ratio, self.backpressure_activate_ratio
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
    fn reload_knobs_are_bounds_checked() {
        let mut cfg = valid();
        cfg.max_concurrent_reloads = 0;
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::OutOfBounds {
                field: "max_concurrent_reloads",
                ..
            }
        ));

        let mut cfg = valid();
        cfg.reload_lease_ttl = Duration::from_secs(5); // renewal at TTL/3 has no headroom
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::OutOfBounds {
                field: "reload_lease_ttl",
                ..
            }
        ));

        // 0 restarts is a legal policy (fail on the first mid-export DDL); only negative is a misconfig.
        let mut cfg = valid();
        cfg.reload_max_restarts = 0;
        assert!(cfg.validate().is_ok(), "a cap of 0 is a valid policy");
        cfg.reload_max_restarts = -1;
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::OutOfBounds {
                field: "reload_max_restarts",
                ..
            }
        ));
    }

    #[test]
    fn backpressure_ratios_must_form_a_hysteresis_band() {
        let mut cfg = valid();
        cfg.backpressure_resume_ratio = 0.9; // resume >= activate → invalid
        cfg.backpressure_activate_ratio = 0.85;
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ConfigError::OutOfBounds {
                field: "backpressure_activate_ratio",
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
