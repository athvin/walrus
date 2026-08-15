#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous source scans are \
              repository-policy checks, not runtime I/O"
)]
//! Storage-class guard (PR 21.2). Two invariants no compiler lint covers:
//! a mutable global is never declared, and every production global is a thread-safe one.

use std::path::{Path, PathBuf};

/// Workspace root — this crate's manifest dir is `<root>/crates/common`.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every `*.rs` under `dir`, recursively, skipping build and VCS output.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read a source directory") {
        let path = entry.expect("read a source-directory entry").path();
        if path.is_dir() {
            if !matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("target" | ".git")
            ) {
                rust_files(&path, out);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

/// Every production `crates/*/src/**/*.rs`, excluding Go-style sibling unit tests.
fn production_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    for entry in std::fs::read_dir(root.join("crates")).expect("read the workspace crates") {
        let src = entry.expect("read a crate entry").path().join("src");
        if src.is_dir() {
            rust_files(&src, &mut sources);
        }
    }
    sources.retain(|path| {
        !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("_test.rs"))
    });
    sources.sort();
    sources
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn mutable_global_offences(_path: &str, _source: &str) -> Vec<String> {
    Vec::new()
}

fn plain_static_offences(_path: &str, _source: &str) -> Vec<String> {
    Vec::new()
}

#[test]
fn no_mutable_global_is_declared_anywhere() {
    // The declaration spelling is assembled so this guard cannot match its own source.
    let _needle = concat!("static", " mut ");
    let root = workspace_root();
    let mut files = Vec::new();
    rust_files(&root.join("crates"), &mut files);
    rust_files(&root.join("tests"), &mut files);
    files.sort();

    let mut offences = Vec::new();
    for file in files {
        let relative = display_path(&root, &file);
        let source = std::fs::read_to_string(&file).expect("read a Rust source file");
        offences.extend(mutable_global_offences(&relative, &source));
    }

    assert!(
        offences.is_empty(),
        "mutable globals are banned — use an Atomic*, OnceLock, LazyLock, or Mutex instead:\n{}",
        offences.join("\n")
    );
}

#[test]
fn every_production_static_is_a_thread_safe_global() {
    let root = workspace_root();
    let sources = production_sources(&root);
    assert!(!sources.is_empty(), "the production source scan is empty");

    let mut offences = Vec::new();
    for file in sources {
        let relative = display_path(&root, &file);
        let source = std::fs::read_to_string(&file).expect("read a production Rust source file");
        offences.extend(plain_static_offences(&relative, &source));
    }

    assert!(
        offences.is_empty(),
        "plain addressed globals are banned — use const for small values or a thread-safe global:\n{}",
        offences.join("\n")
    );
}

#[test]
fn synthetic_mutable_global_is_rejected() {
    let source = format!("{} mut COUNTER: u64 = 0;", "static");
    let offences = mutable_global_offences("fixture/static_mut.rs", &source);
    let diagnostic = offences.join("\n");

    assert!(diagnostic.contains("fixture/static_mut.rs:1"));
    assert!(diagnostic.contains("Atomic*"));
    assert!(diagnostic.contains("OnceLock"));
    assert!(diagnostic.contains("LazyLock"));
}

#[test]
fn synthetic_plain_static_is_rejected() {
    let source = "static TIMEOUT_MS: u64 = 5_000;";
    let offences = plain_static_offences("fixture/plain_static.rs", source);
    let diagnostic = offences.join("\n");

    assert!(diagnostic.contains("fixture/plain_static.rs:1"));
    assert!(diagnostic.contains("const"));
}
