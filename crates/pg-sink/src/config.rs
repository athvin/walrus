//! [`SinkConfig`] — the fully-validated sink configuration (bootstrap step 1).
//!
//! Mirrors [`common::CommonConfig`]'s pattern: read a `WALRUS_`-prefixed environment (with an
//! optional file underneath) into a typed serde struct, then bounds-check it. **Invalid config is a
//! *terminal* bootstrap error** — a missing field or an out-of-range threshold becomes
//! [`ConfigError`] at the edge and maps to [`common::ExitCode::Config`] in `main`, never a panic
//! three modules later. Connectivity (control PG, S3) is a *separate, transient* bootstrap check.

use crate::memory::{HysteresisBand, Ratio};
use common::{ObjectStoreConfig, Redacted, TelemetryConfig};
use serde::Deserialize;
use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::time::Duration;

/// A cadence/deadline longer than an hour is almost certainly a misconfig, not an intent.
const MAX_DURATION: Duration = Duration::from_secs(60 * 60);

/// Postgres' own ceiling on a replication slot name: `NAMEDATALEN - 1`.
const MAX_SLOT_NAME_LEN: usize = 63;

/// The whole alphabet `ReplicationSlotValidateName` (`backend/replication/slot.c`) admits.
const fn is_slot_char(c: char) -> bool {
    matches!(c, 'a'..='z' | '0'..='9' | '_')
}

/// A replication slot name proven to satisfy Postgres' own rule: 1–[`MAX_SLOT_NAME_LEN`] bytes drawn
/// from `[a-z0-9_]` (`ReplicationSlotValidateName`).
///
/// [`SinkConfig::slot_name`] stays a bare [`String`] because that is its *wire* shape: a `slot_name`
/// key in a config file or a `WALRUS_SLOT_NAME` variable. [`SlotName::new`] is the single gate that
/// turns that raw text into a name the server can accept before `START_REPLICATION` embeds it as an
/// unquoted identifier. The catalog lookups in
/// [`crate::slot`] and [`crate::epoch`] bind the name as a query *parameter* and likewise stay on
/// `&str`.
///
/// This is the loader's `LeaseTtl` shape: a raw configured value, one parse at the edge, and a
/// consumer whose precondition the type *is*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotName(String);

impl SlotName {
    /// Parse raw configured text into a name `CREATE_REPLICATION_SLOT` will accept.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidSlotName`] — carrying the rejected text — when `raw` is empty,
    /// longer than [`MAX_SLOT_NAME_LEN`] bytes, or contains anything outside `[a-z0-9_]`. The
    /// failure is terminal, and [`SinkConfig::validate`] runs this gate at load so it lands there
    /// rather than after bootstrap has already connected and preflighted the source.
    pub fn new(raw: &str) -> Result<Self, ConfigError> {
        let reject = |detail: String| ConfigError::InvalidSlotName {
            slot: raw.to_string(),
            detail,
        };
        if raw.is_empty() {
            return Err(reject("must not be empty".to_string()));
        }
        if raw.len() > MAX_SLOT_NAME_LEN {
            return Err(reject(format!(
                "is {} bytes; Postgres accepts at most {MAX_SLOT_NAME_LEN}",
                raw.len()
            )));
        }
        if let Some(bad) = raw.chars().find(|&c| !is_slot_char(c)) {
            return Err(reject(format!(
                "contains {bad:?}; a slot name may only contain lower case letters, numbers, \
                 and the underscore character"
            )));
        }
        Ok(SlotName(raw.to_string()))
    }

    /// The bare name, for the catalog queries that bind it as a parameter.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SlotName {
    /// The bare name — a slot name needs no quoting *by construction*, which is this type's point.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Construct a non-zero shipped default without using runtime-only unwrap/expect APIs.
const fn nz(value: u64) -> NonZeroU64 {
    match NonZeroU64::new(value) {
        Some(value) => value,
        None => NonZeroU64::MIN,
    }
}

/// Fully-typed, bounds-validated sink configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SinkConfig {
    /// Control Postgres (manifest/checkpoint/registry). [`Redacted`] because a libpq URL carries
    /// its password inline and this struct derives `Debug`.
    pub control_db_url: Redacted<String>,
    /// Source Postgres (the logical-replication origin). Redacted for the same reason, and this one
    /// holds the *replication* role's credential — the most privileged the sink is given.
    pub source_db_url: Redacted<String>,
    /// S3/MinIO staging bucket + endpoint + region.
    pub object_store: ObjectStoreConfig,
    /// Logging setup.
    pub telemetry: TelemetryConfig,
    /// Tokio worker threads. `None` uses [`std::thread::available_parallelism`]; configured values
    /// must be within the inclusive 1..=64 bound.
    pub worker_threads: Option<usize>,
    /// Human tag for this process instance, e.g. `"walrus-pg-sink-0"`.
    pub instance: String,
    /// The single replication slot this sink owns.
    pub slot_name: String,
    /// The publication the slot streams.
    pub publication_name: String,
    /// Batch cadence — flush a file at least this often (§1.3).
    #[serde(with = "humantime_serde")]
    pub max_fill: Duration,
    /// Fire an idle heartbeat only after the published tables have been idle this long (§1.9).
    #[serde(with = "humantime_serde")]
    pub heartbeat_idle_after: Duration,
    /// A beat un-returned after this long marks the sink `degraded` (observability, never a kill).
    #[serde(with = "humantime_serde")]
    pub heartbeat_roundtrip_deadline: Duration,
    /// Compatibility-only input retained for rolling upgrades. Snapshot backfill no longer exists,
    /// so this value is accepted but ignored; remove it from deployment overlays when convenient.
    #[serde(with = "humantime_serde")]
    pub backfill_statement_timeout: Duration,
    /// Row-count flush threshold.
    pub max_rows: NonZeroU64,
    /// Byte-size flush threshold.
    pub max_bytes: NonZeroU64,
    /// Back-pressure ceiling on aggregate in-flight buffered bytes (§1.3) — process-wide, distinct from
    /// the per-batch `max_bytes`. Must sit **below** the pod memory limit so a graceful spill beats a
    /// cgroup OOM-kill; `logical_decoding_work_mem` does NOT bound this.
    pub max_inflight_bytes: NonZeroU64,
    /// Pause-poll backstop **activate** ratio of `max_inflight_bytes` (high band).
    pub backpressure_activate_ratio: Ratio,
    /// Pause-poll backstop **resume** ratio (low band) — must be `< activate` so intake doesn't flap.
    pub backpressure_resume_ratio: Ratio,
    /// Bootstrap retry budget: transient deps are retried until this elapses, then terminal.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Where the K8s health endpoints bind.
    pub health_addr: SocketAddr,
    /// Concurrent single-table reload exports (reload H6). "Reload N tables" drains a
    /// queue this wide — a polite cap, never N simultaneous load spikes on the source. ≥ 1.
    pub max_concurrent_reloads: NonZeroU64,
    /// Reload lease TTL (reload H7): a live exporter renews at TTL/3, so a failed exporter
    /// sink is detectable within one TTL. Bounds-checked so the renewal cadence fits inside it.
    #[serde(with = "humantime_serde")]
    pub reload_lease_ttl: Duration,
    /// Rows per reload chunk (reload H2): bounds each PK-ordered SELECT and its in-memory result.
    /// All chunks share one repeatable-read transaction so the baseline is coherent; total export
    /// time therefore determines how long the source snapshot can hold back VACUUM.
    /// `max_concurrent_reloads` bounds the number of tables doing this concurrently. ≥ 1.
    pub reload_chunk_rows: NonZeroU64,
    /// How long a chunk waits for its watermark echo before the reload fails loudly (reload H11):
    /// an unpublished signal table never echoes, so this timeout turns that silent
    /// failure into a `failed` row naming the fix.
    #[serde(with = "humantime_serde")]
    pub reload_echo_timeout: Duration,
    /// How many times a reload may restart because DDL bumped its `schema_version` mid-export
    /// (reload H9). Every attempt is single-schema by construction; a schema change past
    /// chunk 1 invalidates the attempt and re-exports from zero at the new shape. This caps that
    /// churn so a migration-heavy window can't livelock a huge table's reload. `0` fails the first
    /// mid-export DDL; must be ≥ 0.
    pub reload_max_restarts: i32,
    /// If true, the sink creates/alters `publication_name` to cover the required tables; else a gap
    /// is terminal (the operator owns the source setup in `migrations/source`).
    pub manage_publication: bool,
    /// `true` (default) = **strict** keys: a published user table with no usable replica identity is
    /// terminal. `false` = **lenient**: quarantine + alert + continue (surfaced in the
    /// [`PkReport`](crate::preflight::PkReport)).
    pub strict_keys: bool,
}

impl Default for SinkConfig {
    fn default() -> Self {
        SinkConfig {
            control_db_url: Redacted::default(),
            source_db_url: Redacted::default(),
            object_store: ObjectStoreConfig::default(),
            telemetry: TelemetryConfig::default(),
            worker_threads: None,
            instance: String::new(),
            slot_name: String::new(),
            publication_name: String::new(),
            max_fill: Duration::from_secs(5),
            heartbeat_idle_after: Duration::from_secs(10),
            heartbeat_roundtrip_deadline: Duration::from_secs(30),
            backfill_statement_timeout: Duration::ZERO,

            max_rows: nz(100_000),
            max_bytes: nz(128 * 1024 * 1024),
            max_inflight_bytes: nz(512 * 1024 * 1024),
            backpressure_activate_ratio: HysteresisBand::DEFAULT.activate(),
            backpressure_resume_ratio: HysteresisBand::DEFAULT.resume(),
            startup_deadline: Duration::from_secs(60),
            health_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            max_concurrent_reloads: nz(2),
            reload_lease_ttl: Duration::from_secs(60),
            reload_chunk_rows: nz(10_000),
            reload_echo_timeout: Duration::from_secs(30),
            reload_max_restarts: 3,
            manage_publication: false,
            strict_keys: true,
        }
    }
}

/// A terminal configuration failure. `main` maps this to [`common::ExitCode::Config`].
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A file or environment value could not be deserialized. Figment already renders the offending
    /// profile/key, and the failure itself stays in the chain so a reporter can reach the structure
    /// behind that sentence instead of re-parsing it out. Boxed because `figment::Error` is wide
    /// enough that carrying it inline would push every `Result<_, ConfigError>` here towards
    /// `clippy::result_large_err`.
    #[error("config load/parse failed: {0}")]
    Load(#[source] Box<figment::Error>),
    /// A required string field was absent or blank. Names the field, since that is the fix.
    #[error("missing required field: {0}")]
    Missing(&'static str),
    /// A field parsed but sits outside its documented range. `detail` states the bound, so the
    /// message is actionable without opening the config reference.
    #[error("field {field} out of bounds: {detail}")]
    OutOfBounds { field: &'static str, detail: String },
    /// `slot_name` is not a name Postgres would accept for a replication slot, so
    /// `CREATE_REPLICATION_SLOT` could only fail. The rejected text is kept as data.
    #[error("invalid slot_name {slot:?}: {detail}")]
    InvalidSlotName { slot: String, detail: String },
}

impl From<ConfigError> for common::Error {
    fn from(e: ConfigError) -> Self {
        common::Error::Config(e.to_string())
    }
}

impl SinkConfig {
    /// Load config: an optional `WALRUS_CONFIG` file underneath, `WALRUS_`-prefixed env on top (`__`
    /// marks nesting), then [`validate`](Self::validate). An invalid config can never escape as `Ok`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Load`] when file or environment values cannot be deserialized, or the
    /// [`ConfigError::Missing`] / [`ConfigError::InvalidSlotName`] / [`ConfigError::OutOfBounds`]
    /// produced by [`Self::validate`].
    pub fn load() -> Result<Self, ConfigError> {
        use figment::Figment;
        use figment::providers::{Env, Format, Toml, Yaml};

        let mut figment = Figment::new();
        // `var_os` rather than `var`, for the reason `CommonConfig::load` gives at length: the
        // `Result` here exists only to have its `VarError` discarded by an `if let Ok`, which
        // silently equates "unset" with "set to a path that is not UTF-8". `None` means unset.
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
        let figment = figment.merge(
            Env::prefixed("WALRUS_")
                .ignore(&["config", "CONFIG"])
                .split("__"),
        );
        let cfg: SinkConfig = figment
            .extract()
            .map_err(|source| ConfigError::Load(Box::new(source)))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// The parsed hysteresis band — the single gate for the cross-field invariant.
    fn hysteresis_band(&self) -> Result<HysteresisBand, ConfigError> {
        HysteresisBand::new(
            self.backpressure_activate_ratio,
            self.backpressure_resume_ratio,
        )
        .map_err(|e| ConfigError::OutOfBounds {
            field: "backpressure_activate_ratio",
            detail: e.to_string(),
        })
    }

    /// The validated backpressure hysteresis gate.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::OutOfBounds`] if the resume threshold is not below activation.
    pub fn backpressure(&self) -> Result<crate::memory::Backpressure, ConfigError> {
        Ok(crate::memory::Backpressure::new(self.hysteresis_band()?))
    }

    /// The parsed replication slot name — `CREATE_REPLICATION_SLOT`'s precondition.
    /// [`Self::validate`] runs the same gate at load, so a config that booted cannot fail here.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidSlotName`] when `slot_name` is not a name Postgres would accept
    /// for a replication slot.
    pub fn slot(&self) -> Result<SlotName, ConfigError> {
        SlotName::new(&self.slot_name)
    }

    /// The validated idle-heartbeat settings.
    #[must_use]
    pub const fn heartbeat_config(&self) -> crate::heartbeat::HeartbeatConfig {
        crate::heartbeat::HeartbeatConfig {
            idle_after: self.heartbeat_idle_after,
            roundtrip_deadline: self.heartbeat_roundtrip_deadline,
        }
    }

    /// The keyless-table policy for the source preflight (§1.1).
    #[must_use]
    pub const fn pk_mode(&self) -> crate::preflight::PkMode {
        if self.strict_keys {
            crate::preflight::PkMode::Strict
        } else {
            crate::preflight::PkMode::Lenient
        }
    }

    /// Bounds-check every field. Pure and offline — no sockets. Any violation is terminal.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Missing`] for an empty required string,
    /// [`ConfigError::InvalidSlotName`] when `slot_name` is not a legal Postgres slot name, or
    /// [`ConfigError::OutOfBounds`] when a duration, count, ratio, or reload setting violates its
    /// documented bound. All configuration failures are terminal.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            ("control_db_url", self.control_db_url.expose()),
            ("source_db_url", self.source_db_url.expose()),
            ("instance", &self.instance),
            ("slot_name", &self.slot_name),
            ("publication_name", &self.publication_name),
            ("object_store.bucket", &self.object_store.bucket),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::Missing(field));
            }
        }
        // The same gate slot creation demands, run at the edge: an illegal name fails here rather
        // than at `CREATE_REPLICATION_SLOT`, after bootstrap has connected and preflighted a source.
        self.slot()?;
        common::runtime::validate_worker_threads(self.worker_threads).map_err(|e| {
            ConfigError::OutOfBounds {
                field: "worker_threads",
                detail: e.to_string(),
            }
        })?;
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
        self.hysteresis_band()?;
        Ok(())
    }
}

fn duration_bound(field: &'static str, d: Duration) -> Result<(), ConfigError> {
    // Assert the constant ceiling; the configured duration stays runtime, Result-returning data.
    const {
        assert!(
            !MAX_DURATION.is_zero(),
            "MAX_DURATION must be nonzero or every positive cadence exceeds the ceiling"
        );
    }

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

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
