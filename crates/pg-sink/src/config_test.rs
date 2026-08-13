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
    cfg.startup_deadline = Duration::ZERO;
    assert!(matches!(
        cfg.validate().unwrap_err(),
        ConfigError::OutOfBounds {
            field: "startup_deadline",
            ..
        }
    ));

    let mut cfg = valid();
    cfg.max_inflight_bytes = nz(cfg.max_bytes.get() - 1);
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
fn config_error_maps_to_config_exit_code() {
    let e = common::Error::from(ConfigError::Missing("control_db_url"));
    assert_eq!(e.exit_code(), common::ExitCode::Config);
}

#[test]
fn zero_thresholds_are_rejected_during_deserialization() {
    use figment::providers::{Format, Toml};

    for source in ["max_rows = 0", "max_concurrent_reloads = 0"] {
        let result = figment::Figment::new()
            .merge(Toml::string(source))
            .extract::<SinkConfig>();
        assert!(result.is_err(), "zero parsed successfully from {source:?}");
    }
}

#[test]
fn numeric_wire_names_and_shapes_are_unchanged() {
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = ENV_LOCK.lock().unwrap();
    let saved: Vec<_> = std::env::vars_os()
        .filter(|(key, _)| key.to_string_lossy().starts_with("WALRUS_"))
        .collect();
    for (key, _) in &saved {
        std::env::remove_var(key);
    }
    for (key, value) in [
        (
            "WALRUS_CONTROL_DB_URL",
            "postgres://localhost/walrus_control",
        ),
        ("WALRUS_SOURCE_DB_URL", "postgres://localhost/walrus"),
        ("WALRUS_OBJECT_STORE__BUCKET", "walrus"),
        ("WALRUS_INSTANCE", "walrus-pg-sink-test"),
        ("WALRUS_SLOT_NAME", "walrus_slot"),
        ("WALRUS_PUBLICATION_NAME", "walrus_pub"),
        ("WALRUS_MAX_ROWS", "250000"),
        ("WALRUS_BACKPRESSURE_ACTIVATE_RATIO", "0.9"),
    ] {
        std::env::set_var(key, value);
    }

    let result = SinkConfig::load();
    for (key, _) in
        std::env::vars_os().filter(|(key, _)| key.to_string_lossy().starts_with("WALRUS_"))
    {
        std::env::remove_var(key);
    }
    for (key, value) in saved {
        std::env::set_var(key, value);
    }

    let cfg = result.expect("bare numeric environment values should parse");
    assert_eq!(cfg.max_rows.get(), 250_000);
    assert_eq!(cfg.backpressure_activate_ratio.as_f64(), 0.9);
}
