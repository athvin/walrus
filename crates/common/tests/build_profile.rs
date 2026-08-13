//! Guards PR 5.7's workspace release-profile decision against silent drift.

const WORKSPACE_MANIFEST: &str = include_str!("../../../Cargo.toml");
const CODEGEN_UNITS_ADR: &str = "docs/implementation/notes/rust-skills/opt-codegen-units.md";

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
