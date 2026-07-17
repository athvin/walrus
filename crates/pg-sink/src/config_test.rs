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
