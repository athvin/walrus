use super::*;
use common::FailureClass;

fn valid() -> LoaderConfig {
    LoaderConfig {
        control_db_url: "postgres://localhost/walrus_control".to_string(),
        object_store: ObjectStoreConfig {
            bucket: "walrus".to_string(),
            ..ObjectStoreConfig::default()
        },
        instance: "walrus-loader-0".to_string(),
        duckdb_dir: "/var/lib/walrus".to_string(),
        ..LoaderConfig::default()
    }
}

/// Run `body` inside a hermetic `figment::Jail` (fresh temp CWD + scoped env), so config tests
/// do not leak env across the shared test process. The error type is fixed by figment's API.
#[allow(
    clippy::result_large_err,
    reason = "figment Jail requires Result<(), figment::Error>, whose error variant is intentionally large"
)]
fn in_jail(body: impl FnOnce(&mut figment::Jail)) {
    figment::Jail::expect_with(|jail| {
        body(jail);
        Ok(())
    });
}

#[test]
fn humantime_durations_parse_for_every_field() {
    in_jail(|jail| {
        jail.set_env("WALRUS_CONTROL_DB_URL", "postgres://x/y");
        jail.set_env("WALRUS_INSTANCE", "walrus-loader-0");
        jail.set_env("WALRUS_DUCKDB_DIR", "/var/lib/walrus");
        jail.set_env("WALRUS_OBJECT_STORE__BUCKET", "b");
        jail.set_env("WALRUS_LEASE_TTL", "45s");
        jail.set_env("WALRUS_POLL_INTERVAL", "250ms");
        jail.set_env("WALRUS_COMPACTION_INTERVAL", "30m");
        jail.set_env("WALRUS_STARTUP_DEADLINE", "1m 30s");

        let cfg = LoaderConfig::load().expect("valid humantime config should load");
        assert_eq!(cfg.lease_ttl, Duration::from_secs(45));
        assert_eq!(cfg.poll_interval, Duration::from_millis(250));
        assert_eq!(cfg.compaction_interval, Duration::from_secs(30 * 60));
        assert_eq!(cfg.startup_deadline, Duration::from_secs(90));
    });
}

fn duration_attribute_mismatch(src: &str, expected_fields: usize) -> Option<String> {
    let fields = src.matches(": Duration,").count();
    let attrs = src.matches("humantime_serde").count();
    if fields == expected_fields && attrs == fields {
        None
    } else {
        Some(format!(
            "{fields} Duration fields but {attrs} humantime attributes"
        ))
    }
}

#[test]
fn every_duration_field_carries_humantime() {
    let mismatch = duration_attribute_mismatch(include_str!("config.rs"), 4);
    assert!(mismatch.is_none(), "{mismatch:?}");
}

#[test]
fn every_duration_field_rejects_missing_attribute_fixture() {
    const FIXTURE: &str = "struct Config { missing: Duration, }";
    let mismatch = duration_attribute_mismatch(FIXTURE, 1);
    assert_eq!(
        mismatch.as_deref(),
        Some("1 Duration fields but 0 humantime attributes")
    );
}

/// `#[serde(default)]` makes these the shipped values for omitted fields; changing one is a
/// deliberate product configuration change, not a test-maintenance detail.
#[test]
fn defaults_are_the_shipped_contract() {
    let cfg = LoaderConfig::default();
    assert_eq!(cfg.control_db_url, "");
    assert_eq!(cfg.object_store.bucket, "");
    assert_eq!(cfg.object_store.endpoint, None);
    assert_eq!(cfg.object_store.region, "us-east-1");
    assert!(!cfg.telemetry.json);
    assert_eq!(cfg.telemetry.filter, "info");
    assert_eq!(cfg.instance, "");
    assert_eq!(cfg.duckdb_dir, "");
    assert_eq!(cfg.lease_ttl, Duration::from_secs(30));
    assert_eq!(cfg.poll_interval, Duration::from_secs(5));
    assert_eq!(cfg.compaction_interval, Duration::from_secs(3600));
    assert_eq!(cfg.retention_lsn_lag, 16 << 20);
    assert_eq!(cfg.max_files_per_cycle.get(), 32);
    assert_eq!(cfg.startup_deadline, Duration::from_secs(60));
    assert_eq!(cfg.health_addr, SocketAddr::from(([0, 0, 0, 0], 8080)));
    assert_eq!(cfg.worker_threads, None);
}

#[test]
fn zero_worker_threads_is_rejected_as_terminal_config() {
    let mut cfg = valid();
    cfg.worker_threads = Some(0);
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("worker_threads"));
    assert_eq!(
        common::Error::from(err).exit_code(),
        common::ExitCode::Config
    );
}

#[test]
fn worker_threads_above_the_ceiling_is_rejected_as_terminal_config() {
    let mut cfg = valid();
    cfg.worker_threads = Some(65);
    let err = cfg.validate().unwrap_err();
    assert!(err.to_string().contains("worker_threads"));
    assert_eq!(
        common::Error::from(err).exit_code(),
        common::ExitCode::Config
    );
}

#[test]
fn default_lease_ttl_is_accepted() {
    let cfg = valid();
    assert_eq!(cfg.lease_ttl, Duration::from_secs(30));
    assert!(cfg.validate().is_ok());
}

#[test]
fn lease_ttl_below_three_seconds_is_a_terminal_config_error() {
    let mut cfg = valid();
    cfg.lease_ttl = Duration::from_secs(1);
    let err = cfg.validate().expect_err("1s TTL must be rejected");
    assert!(err.to_string().contains("lease_ttl"), "{err}");
    assert!(common::Error::from(err).is_terminal());
}

#[test]
fn zero_lease_ttl_is_still_rejected() {
    let mut cfg = valid();
    cfg.lease_ttl = Duration::ZERO;
    assert!(cfg.validate().is_err());
}

#[test]
fn zero_max_files_per_cycle_is_rejected_during_deserialization() {
    use figment::providers::{Format, Toml};

    let result = figment::Figment::new()
        .merge(Toml::string("max_files_per_cycle = 0"))
        .extract::<LoaderConfig>();
    let err = result.expect_err("zero must not deserialize into max_files_per_cycle");
    assert!(err.to_string().contains("max_files_per_cycle"), "{err}");
}

#[test]
fn zero_poll_and_compaction_intervals_are_rejected() {
    let mut cfg = valid();
    cfg.poll_interval = Duration::ZERO;
    let err = cfg
        .validate()
        .expect_err("zero poll interval must be rejected");
    assert!(err.to_string().contains("poll_interval"), "{err}");

    let mut cfg = valid();
    cfg.compaction_interval = Duration::ZERO;
    let err = cfg
        .validate()
        .expect_err("zero compaction interval must be rejected");
    assert!(err.to_string().contains("compaction_interval"), "{err}");
}
