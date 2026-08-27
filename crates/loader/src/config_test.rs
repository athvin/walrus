use super::*;
use common::FailureClass;
use common::runtime::WorkerThreadsError;

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

/// Serde-deserialized duration knobs, counted in both shapes `humantime_serde` supports. A needle
/// spelled only `": Duration,"` would miss an `Option<Duration>` knob entirely — it would be neither
/// counted as a field nor caught by the expected-count check below, so an optional timeout could
/// skip its attribute in silence.
fn duration_fields(src: &str) -> usize {
    src.matches(": Duration,").count() + src.matches(": Option<Duration>,").count()
}

/// `#[serde(with = "humantime_serde")]` occurrences, matched as the attribute rather than the bare
/// crate name so prose naming the crate cannot pad the count over a missing attribute. Whitespace
/// is stripped first, so the attribute counts the same however rustfmt wraps it.
fn humantime_attributes(src: &str) -> usize {
    let compact: String = src.chars().filter(|ch| !ch.is_whitespace()).collect();
    compact.matches(r#"with="humantime_serde""#).count()
}

fn duration_attribute_mismatch(src: &str, expected_fields: usize) -> Option<String> {
    let fields = duration_fields(src);
    let attrs = humantime_attributes(src);
    if fields == expected_fields && attrs == fields {
        None
    } else {
        Some(format!(
            "{fields} Duration fields but {attrs} humantime attributes"
        ))
    }
}

/// Just the `LoaderConfig` struct body. `config.rs` also defines `ConfigError`, whose
/// `LeaseTtlTooShort` carries `Duration` *data* — not a serde-deserialized field — so the scan
/// below must not count it.
fn loader_config_struct_body(src: &str) -> &str {
    let start = src
        .find("pub struct LoaderConfig {")
        .expect("config.rs defines LoaderConfig");
    let body = &src[start..];
    let end = body.find("\n}").expect("LoaderConfig has a closing brace");
    &body[..end]
}

#[test]
fn every_duration_field_carries_humantime() {
    let body = loader_config_struct_body(include_str!("config.rs"));
    let mismatch = duration_attribute_mismatch(body, 4);
    assert!(mismatch.is_none(), "{mismatch:?}");
}

/// The slice really is the struct: it stops before `ConfigError`'s duration-carrying variant.
#[test]
fn the_struct_slice_excludes_the_error_taxonomy() {
    let body = loader_config_struct_body(include_str!("config.rs"));
    assert!(body.contains("pub lease_ttl: Duration,"));
    assert!(!body.contains("LeaseTtlTooShort"), "{body}");
}

/// Both duration shapes are seen, and a doc comment naming the crate is not an attribute — without
/// either refinement the matching fixture reports no fields (or a satisfied count) and slips past.
#[test]
fn every_duration_field_rejects_missing_attribute_fixture() {
    for fixture in [
        "struct Config { missing: Duration, }",
        "struct Config { missing: Option<Duration>, }",
        "/// Parsed by humantime_serde.\n    missing: Duration,",
    ] {
        assert_eq!(
            duration_attribute_mismatch(fixture, 1).as_deref(),
            Some("1 Duration fields but 0 humantime attributes"),
            "{fixture}"
        );
    }

    let attributed = "#[serde(with = \"humantime_serde\")]\n    present: Option<Duration>,";
    assert_eq!(duration_attribute_mismatch(attributed, 1), None);
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
    assert!(
        matches!(err, ConfigError::WorkerThreads(WorkerThreadsError::Zero)),
        "{err:?}"
    );
    assert_eq!(
        err.to_string(),
        "worker_threads: must be >= 1 (omit for automatic sizing)"
    );
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
    // The bound violation stays typed all the way down: the offending count is recoverable
    // without re-reading the rendered message.
    assert!(
        matches!(
            err,
            ConfigError::WorkerThreads(WorkerThreadsError::TooMany { configured: 65 })
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains("worker_threads"));
    assert_eq!(
        common::Error::from(err).exit_code(),
        common::ExitCode::Config
    );
}

/// Each required string names itself as a `&'static str`, so a caller can point at the offending
/// field instead of substring-matching the message.
#[test]
fn every_missing_required_field_is_named_by_the_variant() {
    fn expect_missing(cfg: &LoaderConfig, field: &str) {
        let err = cfg
            .validate()
            .expect_err("a blank required field is terminal");
        assert!(
            matches!(err, ConfigError::Missing(f) if f == field),
            "{err:?}"
        );
        assert_eq!(err.to_string(), format!("missing required field: {field}"));
    }

    // Whitespace-only counts as blank, so this first case also pins the `trim()`.
    let mut cfg = valid();
    cfg.control_db_url = "   ".to_string();
    expect_missing(&cfg, "control_db_url");

    let mut cfg = valid();
    cfg.instance = String::new();
    expect_missing(&cfg, "instance");

    let mut cfg = valid();
    cfg.duckdb_dir = String::new();
    expect_missing(&cfg, "duckdb_dir");

    let mut cfg = valid();
    cfg.object_store.bucket = String::new();
    expect_missing(&cfg, "object_store.bucket");
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
    // Both the rejected value and the floor it missed travel with the error.
    assert!(
        matches!(
            err,
            ConfigError::LeaseTtlTooShort { ttl, minimum }
                if ttl == Duration::from_secs(1) && minimum == MIN_LEASE_TTL
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains("lease_ttl"), "{err}");
    assert!(common::Error::from(err).is_terminal());
}

#[test]
fn zero_lease_ttl_is_still_rejected() {
    let mut cfg = valid();
    cfg.lease_ttl = Duration::ZERO;
    assert!(matches!(
        cfg.validate(),
        Err(ConfigError::LeaseTtlTooShort { ttl, .. }) if ttl.is_zero()
    ));
}

/// `TryFrom` is the standard hook onto the same floor `LeaseTtl::new` enforces; the two must not be
/// able to disagree about which TTL the renewer may be handed.
#[test]
fn lease_ttl_try_from_is_the_constructor_under_the_standard_hook() {
    let admitted: LeaseTtl = Duration::from_secs(30)
        .try_into()
        .expect("30s clears the floor");
    assert_eq!(admitted.get(), Duration::from_secs(30));
    assert_eq!(
        LeaseTtl::try_from(MIN_LEASE_TTL)
            .expect("MIN_LEASE_TTL is an inclusive floor")
            .get(),
        MIN_LEASE_TTL
    );
    assert!(matches!(
        LeaseTtl::try_from(Duration::from_secs(1)),
        Err(ConfigError::LeaseTtlTooShort { ttl, minimum })
            if ttl == Duration::from_secs(1) && minimum == MIN_LEASE_TTL
    ));
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
    assert!(
        matches!(err, ConfigError::ZeroInterval("poll_interval")),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "poll_interval must be greater than zero");

    let mut cfg = valid();
    cfg.compaction_interval = Duration::ZERO;
    let err = cfg
        .validate()
        .expect_err("zero compaction interval must be rejected");
    assert!(
        matches!(err, ConfigError::ZeroInterval("compaction_interval")),
        "{err:?}"
    );
    assert!(err.to_string().contains("compaction_interval"), "{err}");
}

/// Structuring the type must not move the operator-facing text: `main` prints
/// `walrus-loader: invalid loader configuration: {e}` and `LoaderError::Config` re-adds the same
/// framing, while the `common::Error` classifier keeps carrying the bare detail.
#[test]
fn the_rendered_message_is_unchanged_at_every_display_boundary() {
    let err = ConfigError::Missing("instance");
    assert_eq!(err.to_string(), "missing required field: instance");
    assert_eq!(
        crate::error::LoaderError::from(err).to_string(),
        "invalid loader configuration: missing required field: instance"
    );
    assert_eq!(
        common::Error::from(ConfigError::Missing("instance")).to_string(),
        "invalid configuration: missing required field: instance"
    );
}

/// A typo'd ConfigMap key is a deserialization failure, not a bounds failure — a distinction the
/// old single-string type could not express. Figment's detail rides through verbatim.
#[test]
fn an_unknown_key_is_a_load_failure_carrying_figments_detail() {
    in_jail(|jail| {
        jail.set_env("WALRUS_CONTROL_DB_URL", "postgres://x/y");
        jail.set_env("WALRUS_INSTANCE", "walrus-loader-0");
        jail.set_env("WALRUS_DUCKDB_DIR", "/var/lib/walrus");
        jail.set_env("WALRUS_OBJECT_STORE__BUCKET", "b");
        jail.set_env("WALRUS_NONSENSE", "boom"); // typo'd ConfigMap key

        let err = LoaderConfig::load().expect_err("unknown key must fail the load");
        let ConfigError::Load(detail) = &err else {
            panic!("expected a Load failure, got {err:?}");
        };
        assert!(detail.contains("nonsense"), "{detail}");
    });
}
