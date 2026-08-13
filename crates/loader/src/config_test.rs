use super::*;

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
    assert_eq!(cfg.max_files_per_cycle, 32);
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
