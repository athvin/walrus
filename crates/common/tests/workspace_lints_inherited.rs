#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Guard for `err-no-unwrap-prod` (PR 10.11). `[workspace.lints]` is a default a member must
//! *request*: without `[lints] workspace = true` in its own manifest, a crate silently opts out of
//! `deny(warnings)`, `clippy::all`, `unwrap_used` and `expect_used` — and CI stays green. This test
//! is the thing that goes red instead.

use std::path::{Path, PathBuf};

/// Repo root, from this crate's manifest dir (`<root>/crates/common`).
fn repo_root() -> PathBuf {
    todo!("resolve the workspace root from CARGO_MANIFEST_DIR")
}

/// Every path in the root manifest's `[workspace] members = [ … ]`, in declaration order.
/// Parsed, never hard-coded: crate number seven must be covered the day it is added.
fn workspace_members(_root_manifest: &str) -> Vec<String> {
    todo!("parse the workspace members array")
}

/// The body of `[section]` in `manifest`, up to the next `[` at column 0. `None` if absent.
fn section<'a>(_manifest: &'a str, _header: &str) -> Option<&'a str> {
    todo!("return the requested top-level section body")
}

fn member_inherits_lints(manifest: &str) -> bool {
    section(manifest, "[lints]").is_some_and(|body| body.contains("workspace = true"))
}

#[test]
fn every_member_opts_into_the_workspace_lint_table() {
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
        let manifest = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("member {member} has no manifest at {}: {e}", path.display()));
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
