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
