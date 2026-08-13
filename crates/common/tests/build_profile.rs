//! Guards PR 5.7's workspace release-profile decision against silent drift.

use std::path::Path;

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");
const CODEGEN_UNITS_ADR: &str = "docs/implementation/notes/rust-skills/opt-codegen-units.md";
const CODEGEN_UNITS_NOTE: &str =
    include_str!("../../../docs/implementation/notes/rust-skills/opt-codegen-units.md");
const TARGET_CPU_ADR: &str = "docs/implementation/notes/rust-skills/opt-target-cpu.md";
const TARGET_CPU_NOTE: &str =
    include_str!("../../../docs/implementation/notes/rust-skills/opt-target-cpu.md");
const PGO_ADR: &str = "docs/implementation/notes/rust-skills/opt-pgo-profile.md";
const BUILD_SURFACES: &[(&str, &str)] = &[
    ("Cargo.toml", WORKSPACE_MANIFEST),
    (
        ".github/workflows/ci.yml",
        include_str!("../../../.github/workflows/ci.yml"),
    ),
    (
        "deploy/docker/Dockerfile.pg-sink",
        include_str!("../../../deploy/docker/Dockerfile.pg-sink"),
    ),
    (
        "deploy/docker/Dockerfile.loader",
        include_str!("../../../deploy/docker/Dockerfile.loader"),
    ),
];
const PGO_SURFACES: &[(&str, &str)] = &[
    ("Cargo.toml", WORKSPACE_MANIFEST),
    (
        ".github/workflows/ci.yml",
        include_str!("../../../.github/workflows/ci.yml"),
    ),
    (
        "deploy/docker/Dockerfile.pg-sink",
        include_str!("../../../deploy/docker/Dockerfile.pg-sink"),
    ),
    (
        "deploy/docker/Dockerfile.loader",
        include_str!("../../../deploy/docker/Dockerfile.loader"),
    ),
    ("justfile", include_str!("../../../justfile")),
    (
        "scripts/bench-e2e.sh",
        include_str!("../../../scripts/bench-e2e.sh"),
    ),
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

fn codegen_units_policy(manifest: &str) -> Result<(), &'static str> {
    let release = table_body(manifest, "[profile.release]").ok_or("missing [profile.release]")?;

    if assignment_value(release, "codegen-units").is_some() {
        Err("codegen-units declared")
    } else if !manifest.contains(CODEGEN_UNITS_ADR) {
        Err("missing codegen-units ADR link")
    } else {
        Ok(())
    }
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

fn cargo_config_policy(
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
    let missing_link = "[profile.release]\nlto = \"thin\"\n";

    let cases = [
        (linked_default, Ok(())),
        (linked_override, Err("codegen-units declared")),
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
}
