use super::*;
use common::FailureClass;

fn valid() -> SinkConfig {
    SinkConfig {
        control_db_url: "postgres://localhost/walrus_control".into(),
        source_db_url: "postgres://localhost/walrus".into(),
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

/// Assert a ratio within a documented tolerance. `f64::EPSILON` is the ULP of 1.0, not a
/// general-purpose margin, so it reads as a tolerance while behaving as bit equality. Config
/// ratios are order-1 decimals, so a fixed absolute epsilon is adequate here (large-magnitude
/// data would need relative error). Mirrors the helpers in `tests/reload_{ddl,metrics}.rs`.
#[track_caller]
fn assert_approx_eq(got: f64, want: f64) {
    const EPSILON: f64 = 1e-9;
    assert!(
        (got - want).abs() < EPSILON,
        "{got} != {want} (absolute tolerance {EPSILON})"
    );
}

#[test]
fn humantime_durations_parse_for_every_field() {
    in_jail(|jail| {
        jail.set_env("WALRUS_CONTROL_DB_URL", "postgres://x/y");
        jail.set_env("WALRUS_SOURCE_DB_URL", "postgres://x/z");
        jail.set_env("WALRUS_INSTANCE", "walrus-pg-sink-0");
        jail.set_env("WALRUS_SLOT_NAME", "walrus_slot");
        jail.set_env("WALRUS_PUBLICATION_NAME", "walrus_pub");
        jail.set_env("WALRUS_OBJECT_STORE__BUCKET", "b");
        jail.set_env("WALRUS_MAX_FILL", "2s");
        jail.set_env("WALRUS_HEARTBEAT_IDLE_AFTER", "3s");
        jail.set_env("WALRUS_HEARTBEAT_ROUNDTRIP_DEADLINE", "45s");
        jail.set_env("WALRUS_BACKFILL_STATEMENT_TIMEOUT", "250ms");
        jail.set_env("WALRUS_STARTUP_DEADLINE", "1m 30s");
        jail.set_env("WALRUS_RELOAD_LEASE_TTL", "20s");
        jail.set_env("WALRUS_RELOAD_ECHO_TIMEOUT", "1500ms");

        let cfg = SinkConfig::load().expect("valid humantime config should load");
        assert_eq!(cfg.max_fill, Duration::from_secs(2));
        assert_eq!(cfg.heartbeat_idle_after, Duration::from_secs(3));
        assert_eq!(cfg.heartbeat_roundtrip_deadline, Duration::from_secs(45));
        assert_eq!(cfg.backfill_statement_timeout, Duration::from_millis(250));
        assert_eq!(cfg.startup_deadline, Duration::from_secs(90));
        assert_eq!(cfg.reload_lease_ttl, Duration::from_secs(20));
        assert_eq!(cfg.reload_echo_timeout, Duration::from_millis(1500));
    });
}

/// Serde-deserialized duration knobs, counted in both shapes `humantime_serde` supports. A needle
/// spelled only `": Duration,"` would miss an `Option<Duration>` knob entirely — it would be neither
/// counted as a field nor caught by the field-count assertion, so an optional timeout could skip its
/// attribute in silence.
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

#[test]
fn every_duration_field_carries_humantime() {
    const SRC: &str = include_str!("config.rs");
    let fields = duration_fields(SRC);
    assert_eq!(fields, 7, "SinkConfig Duration field count changed");
    assert_eq!(
        humantime_attributes(SRC),
        fields,
        "every Duration field needs humantime serde"
    );
}

/// The guard must see both duration shapes and must not accept a mention for an attribute.
#[test]
fn the_humantime_guard_bites_on_both_duration_shapes() {
    for fixture in [
        "struct C { missing: Duration, }",
        "struct C { missing: Option<Duration>, }",
        "/// Parsed by humantime_serde.\n    missing: Duration,",
    ] {
        assert_eq!(duration_fields(fixture), 1, "{fixture}");
        assert_eq!(humantime_attributes(fixture), 0, "{fixture}");
    }

    let attributed = "#[serde(with = \"humantime_serde\")]\n    present: Option<Duration>,";
    assert_eq!(duration_fields(attributed), 1);
    assert_eq!(humantime_attributes(attributed), 1);
}

/// A misspelled `WALRUS_*` ConfigMap key must fail the load, not silently leave the shipped default
/// in place — `#[serde(default)]` alone would make the typo invisible. `deny_unknown_fields` does
/// **not** propagate into a nested container, so each struct a `SinkConfig` load traverses needs its
/// own guard; one case per container proves all three are armed.
#[test]
fn a_misspelled_key_is_a_terminal_load_failure() {
    for (key, value, offender) in [
        ("WALRUS_NONSENSE", "boom", "nonsense"),
        ("WALRUS_TELEMETRY__JSN", "true", "jsn"),
        ("WALRUS_OBJECT_STORE__BUCKT", "b", "buckt"),
    ] {
        in_jail(|jail| {
            for (k, v) in [
                ("WALRUS_CONTROL_DB_URL", "postgres://x/y"),
                ("WALRUS_SOURCE_DB_URL", "postgres://x/z"),
                ("WALRUS_OBJECT_STORE__BUCKET", "b"),
                ("WALRUS_INSTANCE", "walrus-pg-sink-0"),
                ("WALRUS_SLOT_NAME", "walrus_slot"),
                ("WALRUS_PUBLICATION_NAME", "walrus_pub"),
            ] {
                jail.set_env(k, v);
            }
            jail.set_env(key, value);

            let err = SinkConfig::load().expect_err("a misspelled key must fail the load");
            let ConfigError::Load(source) = &err else {
                panic!("{key}: expected a Load failure, got {err:?}");
            };
            // Naming the offending key is what makes the error actionable in a pod log.
            let detail = source.to_string();
            assert!(detail.contains(offender), "{key}: {detail}");
            // …and figment's own error stays in the chain rather than being flattened into that
            // sentence, so a reporter can walk to it.
            let cause = std::error::Error::source(&err).expect("load keeps figment's error");
            assert_eq!(cause.to_string(), detail, "{key}");
            assert!(common::Error::from(err).is_terminal(), "{key}");
        });
    }
}

/// `#[serde(default)]` makes these the shipped values for omitted fields; changing one is a
/// deliberate product configuration change, not a test-maintenance detail.
#[test]
fn defaults_are_the_shipped_contract() {
    let cfg = SinkConfig::default();
    assert_eq!(cfg.control_db_url.expose(), "");
    assert_eq!(cfg.source_db_url.expose(), "");
    assert_eq!(cfg.object_store.bucket, "");
    assert_eq!(cfg.object_store.endpoint, None);
    assert_eq!(cfg.object_store.region, "us-east-1");
    assert!(!cfg.telemetry.json);
    assert_eq!(cfg.telemetry.filter, "info");
    assert_eq!(cfg.instance, "");
    assert_eq!(cfg.slot_name, "");
    assert_eq!(cfg.publication_name, "");
    assert_eq!(cfg.max_fill, Duration::from_secs(5));
    assert_eq!(cfg.heartbeat_idle_after, Duration::from_secs(10));
    assert_eq!(cfg.heartbeat_roundtrip_deadline, Duration::from_secs(30));
    assert_eq!(cfg.backfill_statement_timeout, Duration::ZERO);
    assert_eq!(cfg.max_rows.get(), 100_000);
    assert_eq!(cfg.max_bytes.get(), 128 * 1024 * 1024);
    assert_eq!(cfg.max_inflight_bytes.get(), 512 * 1024 * 1024);
    assert_approx_eq(cfg.backpressure_activate_ratio.as_f64(), 0.85);
    assert_approx_eq(cfg.backpressure_resume_ratio.as_f64(), 0.75);
    assert_eq!(cfg.startup_deadline, Duration::from_secs(60));
    assert_eq!(cfg.health_addr, SocketAddr::from(([0, 0, 0, 0], 8080)));
    assert_eq!(cfg.max_concurrent_reloads.get(), 2);
    assert_eq!(cfg.reload_workers_per_table.get(), 4);
    assert_eq!(cfg.reload_lease_ttl, Duration::from_secs(60));
    assert_eq!(cfg.reload_chunk_rows.get(), 10_000);
    assert_eq!(cfg.reload_echo_timeout, Duration::from_secs(30));
    assert_eq!(cfg.reload_max_restarts, 3);
    assert!(!cfg.manage_publication);
    assert!(cfg.strict_keys);
    assert_eq!(cfg.worker_threads, None);
}

#[test]
fn a_fully_valid_config_passes() {
    assert!(valid().validate().is_ok());
}

/// Both DSNs carry a password inline, and `source_db_url` carries the replication role's — the most
/// privileged credential the sink holds. `SinkConfig` derives `Debug`, so a single `?cfg` anywhere
/// would ship both to the log aggregator; the wrappers are what make that impossible.
#[test]
fn debug_renders_neither_dsn() {
    let cfg = SinkConfig {
        control_db_url: "postgres://walrus:hunter2@control-pg/walrus".into(),
        source_db_url: "postgres://replicator:s3cr3t@source-pg/walrus".into(),
        ..valid()
    };

    let rendered = format!("{cfg:?}");

    assert!(!rendered.contains("hunter2"), "{rendered}");
    assert!(!rendered.contains("s3cr3t"), "{rendered}");
    assert_eq!(rendered.matches(common::REDACTED).count(), 2, "{rendered}");
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

/// The slot name reaches `CREATE_REPLICATION_SLOT` unquoted, so a name Postgres could only reject
/// must not survive config load: the parse is the check, and it happens at the edge.
#[test]
fn a_slot_name_postgres_would_reject_is_terminal_at_config_time() {
    for bad in ["Walrus_Slot", "walrus-slot", "walrus slot", "walrus.slot"] {
        let mut cfg = valid();
        cfg.slot_name = bad.to_string();

        let err = cfg.validate().unwrap_err();

        assert!(
            matches!(&err, ConfigError::InvalidSlotName { slot, .. } if slot == bad),
            "{bad:?} should be rejected as a slot name, got {err:?}"
        );
        assert!(common::Error::from(err).is_terminal(), "{bad:?}");
    }
}

/// The bounds are Postgres' own (`ReplicationSlotValidateName`): `NAMEDATALEN - 1` bytes, and a
/// non-empty name. An empty `slot_name` still reports [`ConfigError::Missing`] because the
/// required-field loop runs first.
#[test]
fn slot_name_bounds_are_postgres_bounds() {
    assert!(matches!(
        SlotName::new(""),
        Err(ConfigError::InvalidSlotName { .. })
    ));
    assert!(SlotName::new(&"s".repeat(MAX_SLOT_NAME_LEN)).is_ok());
    assert!(SlotName::new(&"s".repeat(MAX_SLOT_NAME_LEN + 1)).is_err());

    let mut cfg = valid();
    cfg.slot_name = String::new();
    assert!(matches!(
        cfg.validate().unwrap_err(),
        ConfigError::Missing("slot_name")
    ));
}

/// A parsed name renders bare — it needs no quoting by construction — and round-trips its text.
#[test]
fn a_parsed_slot_name_renders_bare() {
    let slot = valid().slot().unwrap();

    assert_eq!(slot.as_str(), "walrus_slot");
    assert_eq!(slot.to_string(), "walrus_slot");
    assert_eq!(slot, SlotName::new("walrus_slot").unwrap());
    assert_eq!(SlotName::new("s_9_0").unwrap().as_str(), "s_9_0");
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
fn zero_worker_threads_is_rejected_as_terminal_config() {
    let mut cfg = valid();
    cfg.worker_threads = Some(0);

    let err = cfg.validate().unwrap_err();

    // The pattern binds nothing, so the classification below still sees the same rejection.
    assert!(matches!(
        err,
        ConfigError::OutOfBounds {
            field: "worker_threads",
            ..
        }
    ));
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

    assert!(matches!(
        err,
        ConfigError::OutOfBounds {
            field: "worker_threads",
            ..
        }
    ));
    assert_eq!(
        common::Error::from(err).exit_code(),
        common::ExitCode::Config
    );
}

#[test]
fn zero_thresholds_are_rejected_during_deserialization() {
    use figment::providers::{Format, Toml};

    for source in [
        "max_rows = 0",
        "max_concurrent_reloads = 0",
        "reload_workers_per_table = 0",
        "reload_chunk_rows = 0",
    ] {
        let result = figment::Figment::new()
            .merge(Toml::string(source))
            .extract::<SinkConfig>();
        assert!(result.is_err(), "zero parsed successfully from {source:?}");
    }
}

#[test]
fn reload_table_worker_product_must_fit_the_connection_count() {
    let mut cfg = valid();
    let factor = nz(1_u64 << (usize::BITS / 2));
    cfg.max_concurrent_reloads = factor;
    cfg.reload_workers_per_table = factor;

    let err = cfg.validate().unwrap_err();
    assert!(matches!(
        err,
        ConfigError::OutOfBounds {
            field: "reload_workers_per_table",
            ..
        }
    ));
}

#[test]
fn reload_table_limit_cannot_panic_the_runtime_semaphore() {
    let Some(too_many) = tokio::sync::Semaphore::MAX_PERMITS.checked_add(1) else {
        return;
    };
    let Ok(too_many) = u64::try_from(too_many) else {
        return;
    };
    let mut cfg = valid();
    cfg.max_concurrent_reloads = nz(too_many);

    let err = cfg.validate().unwrap_err();
    assert!(matches!(
        err,
        ConfigError::OutOfBounds {
            field: "max_concurrent_reloads",
            ..
        }
    ));
}

#[test]
fn non_finite_backpressure_ratios_fail_during_deserialization() {
    use figment::providers::{Format, Toml};

    for (field, source) in [
        (
            "backpressure_activate_ratio",
            "backpressure_activate_ratio = nan",
        ),
        (
            "backpressure_activate_ratio",
            "backpressure_activate_ratio = inf",
        ),
        (
            "backpressure_resume_ratio",
            "backpressure_resume_ratio = -inf",
        ),
    ] {
        let error = figment::Figment::new()
            .merge(Toml::string(source))
            .extract::<SinkConfig>()
            .expect_err("non-finite ratio must not construct SinkConfig");
        let message = error.to_string();
        assert!(message.contains(field), "{field}: {message}");
        assert!(message.contains("finite"), "{field}: {message}");
    }
}

#[test]
fn option_nonzero_u64_is_the_same_size_as_u64() {
    assert_eq!(
        std::mem::size_of::<Option<std::num::NonZeroU64>>(),
        std::mem::size_of::<u64>()
    );
}

#[test]
fn numeric_wire_names_and_shapes_are_unchanged() {
    in_jail(|jail| {
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
            ("WALRUS_MAX_CONCURRENT_RELOADS", "3"),
            ("WALRUS_RELOAD_WORKERS_PER_TABLE", "7"),
            ("WALRUS_RELOAD_CHUNK_ROWS", "2048"),
            ("WALRUS_BACKPRESSURE_ACTIVATE_RATIO", "0.9"),
        ] {
            jail.set_env(key, value);
        }

        let cfg = SinkConfig::load().expect("bare numeric environment values should parse");
        assert_eq!(cfg.max_rows.get(), 250_000);
        assert_eq!(cfg.max_concurrent_reloads.get(), 3);
        assert_eq!(cfg.reload_workers_per_table.get(), 7);
        assert_eq!(cfg.reload_chunk_rows.get(), 2_048);
        assert_approx_eq(cfg.backpressure_activate_ratio.as_f64(), 0.9);
    });
}
