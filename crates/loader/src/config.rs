//! [`LoaderConfig`] — the fully-validated loader configuration (bootstrap step 0). Mirrors the
//! sink's pattern: `WALRUS_`-prefixed env (optional file underneath) → typed serde struct →
//! bounds-check.
//! Invalid config is a **terminal** bootstrap error → [`common::ExitCode::Config`].

use common::{ObjectStoreConfig, Redacted, TelemetryConfig};
use serde::Deserialize;
use std::net::SocketAddr;
use std::num::{NonZeroI64, NonZeroU32};
use std::time::Duration;

/// Floor for `lease_ttl`: renewal runs at TTL/3, so anything shorter cannot land inside the TTL.
/// Public because [`ConfigError::LeaseTtlTooShort`] reports it as data.
pub const MIN_LEASE_TTL: Duration = Duration::from_secs(3);

/// A lease TTL proven to be at least [`MIN_LEASE_TTL`] — the renewal cadence's precondition, carried
/// by the type instead of re-checked (or merely hoped for) at each use.
///
/// [`LoaderConfig::lease_ttl`] stays a bare [`Duration`] because that is its *wire* shape: humantime
/// text in a config file or a `WALRUS_LEASE_TTL` variable. [`LeaseTtl::new`] is the single gate that
/// turns that raw duration into a renewable one, and [`crate::lease::spawn_renewer`] accepts nothing
/// else — so the renewer cannot be handed a TTL its own renew interval could not fit inside.
/// Acquiring or releasing a lease has no such floor and keeps taking a plain `Duration`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTtl(Duration);

impl LeaseTtl {
    /// Parse a raw TTL into a renewable one, rejecting anything below [`MIN_LEASE_TTL`].
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::LeaseTtlTooShort`] — carrying both the rejected value and the floor it
    /// missed — when renewal at TTL/3 could not land inside `ttl`. That failure is terminal.
    pub fn new(ttl: Duration) -> Result<Self, ConfigError> {
        if ttl < MIN_LEASE_TTL {
            return Err(ConfigError::LeaseTtlTooShort {
                ttl,
                minimum: MIN_LEASE_TTL,
            });
        }
        Ok(LeaseTtl(ttl))
    }

    /// The admitted TTL.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl TryFrom<Duration> for LeaseTtl {
    type Error = ConfigError;

    /// The standard spelling of [`LeaseTtl::new`], so the raw [`Duration`] the config carries reaches
    /// the renewable type through `.try_into()?` and a generic bound can name this conversion.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::LeaseTtlTooShort`] — carrying both the rejected value and the floor it
    /// missed — when renewal at TTL/3 could not land inside `ttl`.
    fn try_from(ttl: Duration) -> Result<Self, Self::Error> {
        LeaseTtl::new(ttl)
    }
}

const fn nonzero_i64(value: i64) -> NonZeroI64 {
    match NonZeroI64::new(value) {
        Some(value) => value,
        None => NonZeroI64::MIN,
    }
}

const fn nonzero_u32(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => NonZeroU32::MIN,
    }
}

/// The shared DuckLake catalog and object-data location.
///
/// The catalog URL is independent from `control_db_url`: DuckLake owns and migrates its metadata
/// tables, while control Postgres remains Walrus's manifest/checkpoint authority. Keeping the two
/// databases separate also lets operators scale and back them up independently.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DuckLakeConfig {
    /// PostgreSQL URI for the dedicated DuckLake metadata database.
    pub catalog_url: Redacted<String>,
    /// DuckDB attachment/catalog name exposed to SQL rendered by the loader and read clients.
    pub attach_name: String,
    /// PostgreSQL schema in the catalog database that DuckLake owns.
    pub metadata_schema: String,
    /// Root object-store path for DuckLake-managed Parquet, e.g. `s3://walrus/ducklake/prod/`.
    pub data_path: String,
    /// Optional pre-populated DuckDB extension directory. Production images set this and never
    /// download executable extensions at startup.
    pub extension_directory: Option<String>,
    /// Development escape hatch. When true, missing extensions may be installed before they load.
    /// Shipping manifests leave this false; the image build installs the pinned artifacts.
    pub install_extensions: bool,
    /// Time-travel history retained before snapshots become eligible for expiration.
    #[serde(with = "humantime_serde")]
    pub snapshot_retention: Duration,
    /// Additional age before unreferenced/orphaned files may be physically deleted.
    #[serde(with = "humantime_serde")]
    pub cleanup_grace: Duration,
    /// Cadence for the singleton catalog-level expiration/cleanup pass.
    #[serde(with = "humantime_serde")]
    pub maintenance_interval: Duration,
}

impl Default for DuckLakeConfig {
    fn default() -> Self {
        Self {
            catalog_url: Redacted::default(),
            attach_name: "walrus".to_string(),
            metadata_schema: "walrus_ducklake".to_string(),
            data_path: String::new(),
            extension_directory: None,
            install_extensions: false,
            snapshot_retention: Duration::from_secs(7 * 24 * 60 * 60),
            cleanup_grace: Duration::from_secs(7 * 24 * 60 * 60),
            maintenance_interval: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// The loader's fully-resolved configuration.
///
/// Every field has a default, so an omitted key is not an error; `deny_unknown_fields` makes a
/// *misspelled* one fatal instead of silently ignored. Range and consistency checks are not in the
/// deserializer but in [`Self::validate`], which [`Self::load`] runs before handing the value back.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LoaderConfig {
    /// Control Postgres (leases / manifest / checkpoints). [`Redacted`] because a libpq URL carries
    /// its password inline and this struct derives `Debug`.
    pub control_db_url: Redacted<String>,
    /// S3/MinIO staging bucket the sink writes and the loader reads.
    pub object_store: ObjectStoreConfig,
    /// Log format and filter; see [`TelemetryConfig`].
    pub telemetry: TelemetryConfig,
    /// Tokio worker threads. `None` uses [`std::thread::available_parallelism`]; configured values
    /// must be within 1..=64. A loader pod wants a small value because every apply loop shares one
    /// `LocalSet` thread; `WALRUS_WORKER_THREADS=2` is plenty for the remaining async work. Small,
    /// though — never a *current-thread* runtime: the spawned side tasks must keep running through
    /// a blocking full rebuild on the `LocalSet` thread (see the flavor note in `main`).
    pub worker_threads: Option<usize>,
    /// This pod's identity — the lease `owner_pod`.
    pub instance: String,
    /// Shared DuckLake catalog and object-data settings.
    pub ducklake: DuckLakeConfig,
    /// Number of deterministic table shards in the loader StatefulSet.
    pub shard_count: NonZeroU32,
    /// This instance's zero-based shard ordinal. When omitted, parse the trailing StatefulSet
    /// ordinal from `instance` (e.g. `walrus-loader-2` → `2`).
    pub shard_index: Option<u32>,
    /// The ownership-lease TTL; renewed well under it.
    #[serde(with = "humantime_serde")]
    pub lease_ttl: Duration,
    /// The apply-loop poll cadence (incremental Phase A + Phase B).
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,
    /// The compaction cadence — the per-table full-rebuild + retention prune. **Distinct** from
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
    /// Maximum fresh full-table snapshots scheduled after immutable staging-object verification
    /// fails. `1` permits one automatic replacement; a bad input in that replacement quarantines
    /// the table instead of retrying forever. `0` quarantines the first incident.
    pub max_integrity_resnapshots: u32,
    /// Bootstrap retry budget for transient deps.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Where the K8s health endpoints bind.
    pub health_addr: SocketAddr,
}

impl Default for LoaderConfig {
    fn default() -> Self {
        LoaderConfig {
            control_db_url: Redacted::default(),
            object_store: ObjectStoreConfig::default(),
            telemetry: TelemetryConfig::default(),
            worker_threads: None,
            instance: String::new(),
            ducklake: DuckLakeConfig::default(),
            shard_count: nonzero_u32(1),
            shard_index: None,
            lease_ttl: Duration::from_secs(30),
            poll_interval: Duration::from_secs(5),
            compaction_interval: Duration::from_secs(3600),
            retention_lsn_lag: 16 << 20, // 16 MiB of WAL retained behind transformed_lsn
            max_files_per_cycle: nonzero_i64(32),
            max_integrity_resnapshots: 1,
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
    /// Returns [`ConfigError::Load`] when a file or environment value cannot be deserialized or an
    /// unknown field is present, or whichever variant [`Self::validate`] rejects the merged
    /// configuration with.
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
        let cfg: LoaderConfig = figment
            .merge(
                Env::prefixed("WALRUS_")
                    .ignore(&["config", "CONFIG"])
                    .split("__"),
            )
            .extract()
            .map_err(|source| ConfigError::Load(Box::new(source)))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate required strings and the ownership-lease duration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Missing`] for an empty required value, [`ConfigError::ZeroInterval`]
    /// for a zero cadence, [`ConfigError::LeaseTtlTooShort`] when renewal could not land inside the
    /// TTL, or [`ConfigError::WorkerThreads`] when the runtime worker count is outside its
    /// documented bound. These are terminal failures.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, v) in [
            ("control_db_url", self.control_db_url.expose()),
            ("instance", &self.instance),
            ("ducklake.catalog_url", self.ducklake.catalog_url.expose()),
            ("ducklake.attach_name", &self.ducklake.attach_name),
            ("ducklake.metadata_schema", &self.ducklake.metadata_schema),
            ("ducklake.data_path", &self.ducklake.data_path),
            ("object_store.bucket", &self.object_store.bucket),
        ] {
            if v.trim().is_empty() {
                return Err(ConfigError::Missing(field));
            }
        }
        // The same constructor the renewer demands, so the config gate and the renew-cadence
        // precondition can never drift apart.
        LeaseTtl::new(self.lease_ttl)?;
        for (field, value) in [
            ("poll_interval", self.poll_interval),
            ("compaction_interval", self.compaction_interval),
            (
                "ducklake.snapshot_retention",
                self.ducklake.snapshot_retention,
            ),
            ("ducklake.cleanup_grace", self.ducklake.cleanup_grace),
            (
                "ducklake.maintenance_interval",
                self.ducklake.maintenance_interval,
            ),
        ] {
            if value.is_zero() {
                return Err(ConfigError::ZeroInterval(field));
            }
        }
        let shard_index = self.effective_shard_index()?;
        if shard_index >= self.shard_count.get() {
            return Err(ConfigError::ShardIndex {
                index: shard_index,
                count: self.shard_count.get(),
            });
        }
        for (field, value) in [
            ("ducklake.attach_name", self.ducklake.attach_name.as_str()),
            (
                "ducklake.metadata_schema",
                self.ducklake.metadata_schema.as_str(),
            ),
        ] {
            common::sql::SqlIdent::new(value)
                .map_err(|source| ConfigError::Identifier { field, source })?;
        }
        if !self.ducklake.data_path.starts_with("s3://") {
            return Err(ConfigError::DuckLakeDataPath(
                self.ducklake.data_path.clone(),
            ));
        }
        common::runtime::validate_worker_threads(self.worker_threads)?;
        if i32::try_from(self.max_integrity_resnapshots).is_err() {
            return Err(ConfigError::IntegrityResnapshotBudget {
                configured: self.max_integrity_resnapshots,
            });
        }
        Ok(())
    }

    /// Configured shard index, or the trailing StatefulSet ordinal from `instance`.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ShardIdentity`] when sharding is enabled but `instance` does not end
    /// in a numeric ordinal.
    pub fn effective_shard_index(&self) -> Result<u32, ConfigError> {
        if let Some(index) = self.shard_index {
            return Ok(index);
        }
        if self.shard_count.get() == 1 {
            return Ok(0);
        }
        match self.instance.rsplit_once('-') {
            Some((_, ordinal)) => ordinal.parse().map_err(|_| ConfigError::ShardIdentity {
                instance: self.instance.clone(),
            }),
            None => Err(ConfigError::ShardIdentity {
                instance: self.instance.clone(),
            }),
        }
    }
}

/// Why a loader configuration is unusable. Every variant is terminal — `main` maps them all to
/// [`common::ExitCode::Config`] — but *which* knob is wrong is modelled as data, so a caller can
/// branch on the variant (and recover the offending value) instead of re-reading the rendered
/// message. The "invalid loader configuration" framing lives at the two display boundaries
/// (`main`'s stderr line and [`crate::error::LoaderError::Config`]), keeping this taxonomy's
/// messages usable on their own.
///
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// A file or environment value could not be deserialized into [`LoaderConfig`] — bad syntax, a
    /// wrong type, or an unknown key under `deny_unknown_fields`. Figment already renders the
    /// offending profile/key, so the detail is passed through verbatim — and the failure itself
    /// stays in the chain, so a reporter can reach the structure behind that sentence (profile, key
    /// path, expected type) instead of re-parsing it out. Boxed because `figment::Error` is wide
    /// enough that carrying it inline would push every `Result<_, ConfigError>` here towards
    /// `clippy::result_large_err`.
    #[error("{0}")]
    Load(#[source] Box<figment::Error>),
    /// A required string field was absent or blank.
    #[error("missing required field: {0}")]
    Missing(&'static str),
    /// `lease_ttl` is under [`MIN_LEASE_TTL`], so the TTL/3 renewal could not land inside it.
    #[error(
        "lease_ttl {ttl:?} is too short — renewal runs at TTL/3 and must land inside the TTL; \
         use >= {minimum:?}"
    )]
    LeaseTtlTooShort { ttl: Duration, minimum: Duration },
    /// A cadence that drives a loop was zero, which would make that loop spin instead of poll.
    #[error("{0} must be greater than zero")]
    ZeroInterval(&'static str),
    /// A StatefulSet ordinal outside the configured shard ring can never own a table.
    #[error("shard_index {index} must be less than shard_count {count}")]
    ShardIndex { index: u32, count: u32 },
    /// Deterministic sharding needs the StatefulSet ordinal when no explicit index is provided.
    #[error("cannot derive shard_index from instance {instance:?}; set shard_index explicitly")]
    ShardIdentity { instance: String },
    /// DuckDB catalog/schema names are quoted, but still pass through the shared identifier policy.
    #[error("{field}: {source}")]
    Identifier {
        field: &'static str,
        #[source]
        source: common::sql::IdentError,
    },
    /// DuckLake writes object data; local/file paths would reintroduce node-local state.
    #[error("ducklake.data_path must be an s3:// URI, got {0}")]
    DuckLakeDataPath(String),
    /// The configured Tokio worker count is outside its documented `1..=64` bound. Keeps the typed
    /// [`common::runtime::WorkerThreadsError`] so callers can recover the offending count.
    #[error("worker_threads: {0}")]
    WorkerThreads(#[from] common::runtime::WorkerThreadsError),
    /// PostgreSQL stores the durable retry counter as `int`; larger budgets could not be persisted
    /// exactly and are rejected before the loader starts.
    #[error("max_integrity_resnapshots {configured} exceeds PostgreSQL int")]
    IntegrityResnapshotBudget { configured: u32 },
}

impl From<ConfigError> for common::Error {
    fn from(e: ConfigError) -> Self {
        common::Error::Config(e.to_string())
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
