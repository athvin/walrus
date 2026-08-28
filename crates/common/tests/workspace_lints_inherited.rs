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
        if let Some(after_key) = trimmed.strip_prefix("members")
            && after_key.trim_start().starts_with('=')
        {
            assignment = Some(offset + (line.len() - trimmed.len()) + "members".len());
            break;
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

/// How many entries in a lint-table body pin `lint` at `deny`, whatever their indentation.
fn clippy_deny_count(table: &str, lint: &str) -> usize {
    let entry = format!(r#"{lint} = "deny""#);
    table.lines().filter(|line| line.trim() == entry).count()
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
fn the_workspace_lint_table_denies_redundant_method_closures() {
    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy
            .lines()
            .filter(|line| line.trim() == r#"redundant_closure_for_method_calls = "deny""#)
            .count(),
        1,
        "workspace policy must deny redundant method-forwarding closures exactly once"
    );
}

/// `extern` blocks are the one FFI surface walrus does not own: the native engine arrives through
/// `duckdb`'s pinned `libduckdb-sys`, so first-party sources declare none. Edition 2024 already
/// makes the pre-2024 `extern "C" { … }` spelling a hard error — but only for a member on that
/// edition. A crate added with `edition = "2021"` gets nothing but this named deny, which is
/// allow-by-default and so out of reach of `warnings = "deny"`.
#[test]
fn the_workspace_lint_table_still_denies_pre_2024_extern_blocks() {
    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let rust = section(&root_manifest, "[workspace.lints.rust]")
        .expect("root manifest must define [workspace.lints.rust]");
    assert_eq!(
        rust.lines()
            .filter(|line| line.trim() == r#"missing_unsafe_on_extern = "deny""#)
            .count(),
        1,
        "workspace policy must reject un-`unsafe` extern blocks exactly once"
    );
}

/// walrus exports no symbols: zero `#[no_mangle]` / `#[export_name]` / `#[link_section]`
/// attributes and no `crate-type`, so both binaries are ordinary executables with no plugin ABI.
/// Two items claiming one exported symbol is linker-level UB with no diagnostic, so the bare
/// spelling must stay unreachable. Edition 2024 makes it a hard error — but again only for a member
/// on that edition, and the lint that covers an earlier-edition one is allow-by-default, hence
/// beyond `warnings = "deny"`.
#[test]
fn the_workspace_lint_table_still_denies_bare_export_attributes() {
    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let rust = section(&root_manifest, "[workspace.lints.rust]")
        .expect("root manifest must define [workspace.lints.rust]");
    assert_eq!(
        rust.lines()
            .filter(|line| line.trim() == r#"unsafe_attr_outside_unsafe = "deny""#)
            .count(),
        1,
        "workspace policy must reject bare `#[no_mangle]`-family attributes exactly once"
    );
}

/// Both directions of the `# Safety` section. The workspace forbid means walrus authors no
/// `unsafe fn`, so the caller-obligation lint is belt-and-braces like its siblings above: it is
/// what still demands the section if a member ever stops inheriting that forbid, and it reaches
/// the tree today only through the `clippy::all` group. Its inverse is the half with live reach —
/// while the forbid holds, every `# Safety` heading this tree could grow sits on a *safe* item,
/// promising callers an obligation the compiler never imposes.
#[test]
fn the_workspace_lint_table_still_polices_safety_doc_sections() {
    let synthetic = "missing_safety_doc = \"allow\"\n  unnecessary_safety_doc = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "missing_safety_doc"), 0);
    assert_eq!(clippy_deny_count(synthetic, "unnecessary_safety_doc"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    for lint in ["missing_safety_doc", "unnecessary_safety_doc"] {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "workspace policy must pin {lint} = \"deny\" exactly once"
        );
    }
}

/// Diagnostics are `tracing` events, never raw stream writes. That was prose in
/// `crates/common/src/telemetry.rs` before these two lints backed it: `println!` and `eprintln!`
/// compile cleanly under every other entry in the table, so nothing went red when one
/// appeared. Both are `clippy::restriction` lints, outside the `clippy::all` group denied above,
/// so that group entry does not reach them either — only these named denies do.
#[test]
fn the_workspace_lint_table_still_denies_raw_stream_prints() {
    let synthetic = "print_stdout = \"warn\"\n  print_stderr = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "print_stdout"), 0);
    assert_eq!(clippy_deny_count(synthetic, "print_stderr"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    for lint in ["print_stdout", "print_stderr"] {
        assert_eq!(
            clippy_deny_count(clippy, lint),
            1,
            "workspace policy must pin {lint} = \"deny\" exactly once"
        );
    }
}

/// Multi-file module layout is a convention with nothing behind it: `foo.rs` + `foo/` and
/// `foo/mod.rs` both compile, so a second style can arrive one directory at a time. walrus has
/// exactly one such directory (`crates/pg-sink/src/pgoutput/`) and it uses `mod.rs`, which means
/// this lint has zero sites — no source file goes red if the entry is dropped, so this test is the
/// thing that does. Its inverse must stay absent for the same reason it was not chosen: enabling
/// `mod_module_files` would ban the very file the tree standardised on.
#[test]
fn the_workspace_lint_table_still_pins_one_module_layout() {
    let synthetic = "self_named_module_files = \"warn\"\n  mod_module_files = \"deny\"\n";
    assert_eq!(clippy_deny_count(synthetic, "self_named_module_files"), 0);
    assert_eq!(clippy_deny_count(synthetic, "mod_module_files"), 1);

    let root_manifest = std::fs::read_to_string(repo_root().join("Cargo.toml")).unwrap();
    let clippy = section(&root_manifest, "[workspace.lints.clippy]")
        .expect("root manifest must define [workspace.lints.clippy]");
    assert_eq!(
        clippy_deny_count(clippy, "self_named_module_files"),
        1,
        "workspace policy must pin self_named_module_files = \"deny\" exactly once"
    );
    assert_eq!(
        clippy_deny_count(clippy, "mod_module_files"),
        0,
        "mod_module_files is the inverse policy — it would ban pgoutput/mod.rs"
    );
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
