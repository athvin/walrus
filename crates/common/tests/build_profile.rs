//! Guards PR 5.7's workspace release-profile decision against silent drift.

use std::path::Path;

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");
const CI_WORKFLOW: &str = include_str!("../../../.github/workflows/ci.yml");
const DOCKERFILE_PG_SINK: &str = include_str!("../../../deploy/docker/Dockerfile.pg-sink");
const DOCKERFILE_LOADER: &str = include_str!("../../../deploy/docker/Dockerfile.loader");
const JUSTFILE: &str = include_str!("../../../justfile");
const BENCH_E2E: &str = include_str!("../../../scripts/bench-e2e.sh");
const CODEGEN_UNITS_ADR: &str = "docs/implementation/notes/rust-skills/opt-codegen-units.md";
const CODEGEN_UNITS_NOTE: &str =
    include_str!("../../../docs/implementation/notes/rust-skills/opt-codegen-units.md");
const TARGET_CPU_ADR: &str = "docs/implementation/notes/rust-skills/opt-target-cpu.md";
const TARGET_CPU_NOTE: &str =
    include_str!("../../../docs/implementation/notes/rust-skills/opt-target-cpu.md");
const PGO_ADR: &str = "docs/implementation/notes/rust-skills/opt-pgo-profile.md";
const PGO_NOTE: &str =
    include_str!("../../../docs/implementation/notes/rust-skills/opt-pgo-profile.md");
const BUILD_SURFACES: &[(&str, &str)] = &[
    ("Cargo.toml", WORKSPACE_MANIFEST),
    (".github/workflows/ci.yml", CI_WORKFLOW),
    ("deploy/docker/Dockerfile.pg-sink", DOCKERFILE_PG_SINK),
    ("deploy/docker/Dockerfile.loader", DOCKERFILE_LOADER),
];
// Every surface that invokes cargo, which is where a codegen-unit override can be reinstated from
// outside `[profile.release]`. The manifest is deliberately absent: its rejection prose names the
// key, and `codegen_units_declaration` already parses the tables there.
const CODEGEN_UNITS_SURFACES: &[(&str, &str)] = &[
    (".github/workflows/ci.yml", CI_WORKFLOW),
    ("deploy/docker/Dockerfile.pg-sink", DOCKERFILE_PG_SINK),
    ("deploy/docker/Dockerfile.loader", DOCKERFILE_LOADER),
    ("justfile", JUSTFILE),
    ("scripts/bench-e2e.sh", BENCH_E2E),
];
const PGO_SURFACES: &[(&str, &str)] = &[
    ("Cargo.toml", WORKSPACE_MANIFEST),
    (".github/workflows/ci.yml", CI_WORKFLOW),
    ("deploy/docker/Dockerfile.pg-sink", DOCKERFILE_PG_SINK),
    ("deploy/docker/Dockerfile.loader", DOCKERFILE_LOADER),
    ("justfile", JUSTFILE),
    ("scripts/bench-e2e.sh", BENCH_E2E),
];

fn table_body<'a>(manifest: &'a str, header: &str) -> Option<&'a str> {
    let mut offset = 0;
    let mut body_start: Option<usize> = None;

    for line in manifest.split_inclusive('\n') {
        let trimmed = line.trim();
        if let Some(start) = body_start {
            if trimmed.starts_with('[') {
                return Some(&manifest[start..offset]);
            }
        } else if trimmed == header {
            body_start = Some(offset + line.len());
        }
        offset += line.len();
    }

    body_start.map(|start| &manifest[start..])
}

fn assignment_value<'a>(body: &'a str, key: &str) -> Option<&'a str> {
    body.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }

        let (name, value) = line.split_once('=')?;
        if name.trim() != key {
            return None;
        }

        Some(value.split('#').next()?.trim())
    })
}

fn release_lto(manifest: &str) -> Result<&str, &'static str> {
    let release = table_body(manifest, "[profile.release]").ok_or("missing [profile.release]")?;
    let value = assignment_value(release, "lto").ok_or("missing lto")?;
    let value = value.trim_matches('"');

    if matches!(value, "false" | "off") {
        Err("lto disabled")
    } else {
        Ok(value)
    }
}

/// The `[profile.…]` header that declares `codegen-units`, if any. *Every* profile table counts,
/// not only `[profile.release]`: the rule parks the override in a `bench`, `production` or
/// `release-with-debug` table just as readily, and `bench` inherits `release` — an override there
/// would quietly de-couple `docs/benchmarks.md`'s numbers from the shipped artifact. Per-package
/// tables (`[profile.release.package.…]`) accept the key too. Comments are not assignments, so the
/// rationale above the table may keep naming it.
fn codegen_units_declaration(manifest: &str) -> Option<&str> {
    let mut profile = None;

    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            profile = line.starts_with("[profile").then_some(line);
        } else if let Some(header) = profile
            && assignment_value(line, "codegen-units").is_some()
        {
            return Some(header);
        }
    }

    None
}

fn codegen_units_policy(manifest: &str) -> Result<(), &'static str> {
    if codegen_units_declaration(manifest).is_some() {
        Err("codegen-units declared")
    } else if !manifest.contains(CODEGEN_UNITS_ADR) {
        Err("missing codegen-units ADR link")
    } else {
        Ok(())
    }
}

/// Cargo also reads the setting from outside the manifest: `CARGO_PROFILE_<name>_CODEGEN_UNITS` in
/// the environment, `-C codegen-units` in a rustc flag, or a `--config` override on the command
/// line. Each of those reinstates the rejected setting with the manifest guard still green.
fn codegen_units_override_policy(body: &str) -> Result<(), &'static str> {
    for (needle, diagnostic) in [
        ("CODEGEN_UNITS", "a codegen-units env override is set"),
        ("codegen-units", "a codegen-units build flag is set"),
    ] {
        if body.contains(needle) {
            return Err(diagnostic);
        }
    }
    Ok(())
}

fn target_cpu_policy(body: &str) -> Result<(), &'static str> {
    for (needle, diagnostic) in [
        ("target-cpu", "target-cpu is set"),
        ("RUSTFLAGS", "RUSTFLAGS is set"),
        ("rustflags", "rustflags is set"),
    ] {
        if body.contains(needle) {
            return Err(diagnostic);
        }
    }
    Ok(())
}

const fn cargo_config_policy(
    config_toml_exists: bool,
    legacy_config_exists: bool,
) -> Result<(), &'static str> {
    if config_toml_exists {
        Err(".cargo/config.toml exists")
    } else if legacy_config_exists {
        Err(".cargo/config exists")
    } else {
        Ok(())
    }
}

fn pgo_policy(body: &str) -> Result<(), &'static str> {
    for (needle, diagnostic) in [
        ("profile-generate", "PGO profile generation is enabled"),
        ("profile-use", "PGO profile use is enabled"),
        ("llvm-profdata", "llvm-profdata is invoked"),
    ] {
        if body.contains(needle) {
            return Err(diagnostic);
        }
    }
    Ok(())
}

#[test]
fn workspace_release_profile_keeps_thin_lto() {
    assert_eq!(release_lto(WORKSPACE_MANIFEST), Ok("thin"));
}

#[test]
fn lto_policy_rejects_disabled_missing_and_comment_only_values() {
    let cases = [
        (
            "[workspace]\nmembers = []\n",
            Err("missing [profile.release]"),
        ),
        ("[profile.release]\nopt-level = 3\n", Err("missing lto")),
        ("[profile.release]\nlto = false\n", Err("lto disabled")),
        ("[profile.release]\nlto = \"off\"\n", Err("lto disabled")),
        (
            "[profile.release]\n# lto = \"thin\"\ncodegen-units = 16\n",
            Err("missing lto"),
        ),
    ];

    for (manifest, expected) in cases {
        assert_eq!(release_lto(manifest), expected, "manifest:\n{manifest}");
    }
}

#[test]
fn profile_comment_does_not_declare_codegen_units() {
    let release = table_body(WORKSPACE_MANIFEST, "[profile.release]").expect("release profile");
    assert_eq!(assignment_value(release, "codegen-units"), None);
}

#[test]
fn no_profile_table_declares_codegen_units() {
    assert_eq!(
        codegen_units_declaration(WORKSPACE_MANIFEST),
        None,
        "walrus keeps the default codegen-unit count; see {CODEGEN_UNITS_ADR}"
    );
}

#[test]
fn no_build_surface_overrides_codegen_units() {
    for (name, body) in CODEGEN_UNITS_SURFACES {
        assert_eq!(
            codegen_units_override_policy(body),
            Ok(()),
            "{name} must leave codegen-units at the default; see {CODEGEN_UNITS_ADR}"
        );
    }
}

#[test]
fn codegen_units_rejection_rationale_is_still_recorded() {
    assert_eq!(codegen_units_policy(WORKSPACE_MANIFEST), Ok(()));
    assert!(
        !CODEGEN_UNITS_NOTE.trim().is_empty(),
        "{CODEGEN_UNITS_ADR} must contain the recorded decision"
    );
}

#[test]
fn codegen_units_policy_rejects_fabricated_input() {
    let linked_default = concat!(
        "# docs/implementation/notes/rust-skills/opt-codegen-units.md\n",
        "[profile.release]\n",
        "lto = \"thin\"\n",
    );
    let linked_override = concat!(
        "# docs/implementation/notes/rust-skills/opt-codegen-units.md\n",
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "codegen-units = 1\n",
    );
    let linked_comment = concat!(
        "# docs/implementation/notes/rust-skills/opt-codegen-units.md\n",
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "# codegen-units = 1\n",
    );
    let bench_override = concat!(
        "# docs/implementation/notes/rust-skills/opt-codegen-units.md\n",
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "\n",
        "[profile.bench]\n",
        "inherits = \"release\"\n",
        "codegen-units = 1\n",
    );
    let package_override = concat!(
        "# docs/implementation/notes/rust-skills/opt-codegen-units.md\n",
        "[profile.release]\n",
        "lto = \"thin\"\n",
        "\n",
        "[profile.release.package.duckdb]\n",
        "codegen-units = 1\n",
    );
    let missing_link = "[profile.release]\nlto = \"thin\"\n";

    let cases = [
        (linked_default, Ok(())),
        (linked_comment, Ok(())),
        (linked_override, Err("codegen-units declared")),
        (bench_override, Err("codegen-units declared")),
        (package_override, Err("codegen-units declared")),
        (missing_link, Err("missing codegen-units ADR link")),
    ];

    for (manifest, expected) in cases {
        assert_eq!(
            codegen_units_policy(manifest),
            expected,
            "manifest:\n{manifest}"
        );
    }
}

#[test]
fn codegen_units_override_policy_rejects_fabricated_input() {
    let cases = [
        ("RUN cargo build --release", Ok(())),
        (
            "ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1",
            Err("a codegen-units env override is set"),
        ),
        (
            "RUSTFLAGS=\"-C codegen-units=1\" cargo build --release",
            Err("a codegen-units build flag is set"),
        ),
        (
            "cargo build --release --config profile.release.codegen-units=1",
            Err("a codegen-units build flag is set"),
        ),
    ];

    for (body, expected) in cases {
        assert_eq!(
            codegen_units_override_policy(body),
            expected,
            "surface:\n{body}"
        );
    }
}

#[test]
fn no_build_surface_sets_a_target_cpu() {
    for (name, body) in BUILD_SURFACES {
        assert_eq!(
            target_cpu_policy(body),
            Ok(()),
            "{name} must remain portable; see {TARGET_CPU_ADR}"
        );
    }
}

#[test]
fn no_cargo_config_exists() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_eq!(
        cargo_config_policy(
            root.join(".cargo/config.toml").exists(),
            root.join(".cargo/config").exists(),
        ),
        Ok(()),
        "workspace Cargo config must remain absent; see {TARGET_CPU_ADR}"
    );
}

#[test]
fn target_cpu_policy_rejects_fabricated_input() {
    let surface_cases = [
        ("RUN cargo build --release", Ok(())),
        (
            "ENV RUSTFLAGS=\"-C target-cpu=native\"",
            Err("target-cpu is set"),
        ),
        ("ENV RUSTFLAGS=-Copt-level=3", Err("RUSTFLAGS is set")),
        (
            "rustflags = [\"-C\", \"target-feature=+avx2\"]",
            Err("rustflags is set"),
        ),
    ];
    for (body, expected) in surface_cases {
        assert_eq!(target_cpu_policy(body), expected, "surface:\n{body}");
    }

    let config_cases = [
        ((false, false), Ok(())),
        ((true, false), Err(".cargo/config.toml exists")),
        ((false, true), Err(".cargo/config exists")),
    ];
    for ((config_toml_exists, legacy_config_exists), expected) in config_cases {
        assert_eq!(
            cargo_config_policy(config_toml_exists, legacy_config_exists),
            expected
        );
    }
}

#[test]
fn target_cpu_rejection_is_recorded() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(
        root.join(TARGET_CPU_ADR).is_file(),
        "missing target-CPU decision at {TARGET_CPU_ADR}"
    );
    assert!(
        !TARGET_CPU_NOTE.trim().is_empty(),
        "{TARGET_CPU_ADR} must contain the recorded decision"
    );
}

#[test]
fn no_build_surface_enables_pgo() {
    for (name, body) in PGO_SURFACES {
        assert_eq!(
            pgo_policy(body),
            Ok(()),
            "{name} must remain free of PGO instrumentation; see {PGO_ADR}"
        );
    }
}

#[test]
fn pgo_policy_rejects_fabricated_input() {
    let cases = [
        ("RUN cargo build --release", Ok(())),
        (
            "ENV RUSTFLAGS=\"-Cprofile-generate=/tmp/pgo\"",
            Err("PGO profile generation is enabled"),
        ),
        (
            "RUSTFLAGS=\"-Cprofile-use=/tmp/pgo.profdata\" cargo build",
            Err("PGO profile use is enabled"),
        ),
        (
            "pgo:\n    llvm-profdata merge -o merged.profdata raw",
            Err("llvm-profdata is invoked"),
        ),
    ];

    for (body, expected) in cases {
        assert_eq!(pgo_policy(body), expected, "surface:\n{body}");
    }
}

#[test]
fn pgo_decision_is_still_recorded() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert!(
        root.join(PGO_ADR).is_file(),
        "missing PGO decision at {PGO_ADR}"
    );
    assert!(
        !PGO_NOTE.trim().is_empty(),
        "{PGO_ADR} must contain the recorded decision and re-open trigger"
    );
}
