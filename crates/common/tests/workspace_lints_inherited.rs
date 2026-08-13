#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous manifest and \
              clippy-config reads are themselves repository-policy checks, not runtime I/O"
)]
//! Guard for `err-no-unwrap-prod` (PR 10.11). `[workspace.lints]` is a default a member must
//! *request*: without `[lints] workspace = true` in its own manifest, a crate silently opts out of
//! `deny(warnings)`, `clippy::all`, `unwrap_used` and `expect_used` — and CI stays green. This test
//! is the thing that goes red instead.

use std::path::{Path, PathBuf};

/// Repo root, from this crate's manifest dir (`<root>/crates/common`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("CARGO_MANIFEST_DIR must be inside the walrus workspace")
}

/// Every path in the root manifest's `[workspace] members = [ … ]`, in declaration order.
/// Parsed, never hard-coded: crate number seven must be covered the day it is added.
fn workspace_members(root_manifest: &str) -> Vec<String> {
    let workspace = section(root_manifest, "[workspace]")
        .expect("root manifest must define a [workspace] section");
    let mut offset = 0;
    let mut assignment = None;
    for line in workspace.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if let Some(after_key) = trimmed.strip_prefix("members") {
            if after_key.trim_start().starts_with('=') {
                assignment = Some(offset + (line.len() - trimmed.len()) + "members".len());
                break;
            }
        }
        offset += line.len();
    }

    let assignment = &workspace[assignment.expect("[workspace] must define members")..];
    let array = &assignment[assignment
        .find('[')
        .expect("workspace members must be an array")
        + 1..];

    let mut members = Vec::new();
    let mut member = None::<String>;
    let mut escaped = false;
    let mut comment = false;
    let mut closed = false;
    for ch in array.chars() {
        if comment {
            if ch == '\n' {
                comment = false;
            }
            continue;
        }
        if let Some(value) = member.as_mut() {
            if escaped {
                value.push(ch);
                escaped = false;
            } else {
                match ch {
                    '\\' => escaped = true,
                    '"' => members.push(member.take().expect("member string is open")),
                    _ => value.push(ch),
                }
            }
            continue;
        }
        match ch {
            '"' => member = Some(String::new()),
            '#' => comment = true,
            ']' => {
                closed = true;
                break;
            }
            _ => {}
        }
    }
    assert!(closed, "workspace members array has no closing bracket");
    assert!(
        member.is_none(),
        "workspace members array has an open string"
    );
    members
}

/// The body of `[section]` in `manifest`, up to the next `[` at column 0. `None` if absent.
fn section<'a>(manifest: &'a str, header: &str) -> Option<&'a str> {
    let mut offset = 0;
    let mut body_start = None;
    for line in manifest.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == header {
            body_start = Some(offset + line.len());
            break;
        }
        offset += line.len();
    }

    let body = &manifest[body_start?..];
    let mut body_end = body.len();
    let mut offset = 0;
    for line in body.split_inclusive('\n') {
        if line.starts_with('[') {
            body_end = offset;
            break;
        }
        offset += line.len();
    }
    Some(&body[..body_end])
}

fn member_inherits_lints(manifest: &str) -> bool {
    section(manifest, "[lints]").is_some_and(|body| body.contains("workspace = true"))
}

#[test]
fn every_member_opts_into_the_workspace_lint_table() {
    let wrapped_members = r#"[workspace]
members = [
    "crates/common",
    # Non-crates members must remain visible to the guard.
    "tests/e2e",
]

[package]
name = "synthetic"
"#;
    assert_eq!(
        workspace_members(wrapped_members),
        ["crates/common", "tests/e2e"]
    );

    let dependency_only = "[dependencies]\ncommon = { workspace = true }\n";
    assert!(!member_inherits_lints(dependency_only));

    let root = repo_root();
    assert!(Path::new(&root).is_absolute());
    let root_manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let members = workspace_members(&root_manifest);
    assert!(
        members.len() >= 6,
        "member list parsed as {members:?} — the parser is broken"
    );

    let mut missing: Vec<String> = Vec::new();
    for member in &members {
        let path = root.join(member).join("Cargo.toml");
        let manifest = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!("member {member} has no manifest at {}: {e}", path.display())
        });
        if !member_inherits_lints(&manifest) {
            missing.push(member.clone());
        }
    }
    assert!(
        missing.is_empty(),
        "these workspace members do not inherit [workspace.lints] — add\n\n    [lints]\n    workspace = true\n\nto: {missing:?}"
    );
}

#[test]
fn the_workspace_lint_table_still_denies_unwrap_and_expect() {
    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let rust = section(&root_manifest, "[workspace.lints.rust]")
        .expect("root manifest must define [workspace.lints.rust]");
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert!(rust.contains(r#"warnings = "deny""#));
    assert!(clippy.contains(r#"unwrap_used = "deny""#));
    assert!(clippy.contains(r#"expect_used = "deny""#));
}

#[test]
fn the_clippy_carve_out_is_still_scoped_to_tests() {
    let cfg = std::fs::read_to_string(repo_root().join("clippy.toml")).unwrap();
    assert!(cfg.contains("allow-unwrap-in-tests = true"));
    assert!(cfg.contains("allow-expect-in-tests = true"));

    for line in cfg.lines().map(str::trim) {
        let Some((key, _)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.contains("unwrap") || key.contains("expect") {
            assert!(
                key.ends_with("-in-tests"),
                "clippy.toml key {key:?} widens unwrap/expect beyond tests"
            );
        }
    }
}
