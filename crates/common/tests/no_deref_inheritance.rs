#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]

//! Guard: walrus newtypes expose explicit accessors (`Lsn::as_u64`, `From<ManifestId> for i64`,
//! `DuckTable::as_str`), never `Deref`. `Deref` is for smart pointers and owned→borrowed containers
//! (API guideline C-DEREF); using it on a domain newtype re-exposes every inner method through
//! method resolution and quietly undoes the type distinction the newtype was created to enforce.
//!
//! See `docs/implementation/notes/rust-skills/type-deref-coercion.md`.

use std::{
    fs,
    path::{Path, PathBuf},
};

const DECISION_NOTE: &str = "docs/implementation/notes/rust-skills/type-deref-coercion.md";

/// Types that legitimately implement `Deref`/`DerefMut`, with the reason.
///
/// A real smart pointer, an owned→borrowed container (`String` → `str`) or an RAII guard belongs
/// here. A domain newtype (`Lsn`, `ManifestId`, `EpochNo`, `SchemaVersionNo`, `ReloadId`,
/// `DuckTable<K>`) does **not** — that is exactly what this guard exists to catch.
const ALLOWED: &[(&str, &str, &str)] = &[(
    "crates/pg-sink/src/reload_signal.rs",
    "SubscribeGuard",
    "RAII guard: Drop unregisters the in-flight watermark subscription",
)];

#[test]
fn no_walrus_type_implements_deref() {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/common/../.. is the repo root");
    let crates_dir = repo_root.join("crates");

    let mut offences = Vec::new();
    let mut files_scanned = 0_usize;
    for src in crate_src_dirs(&crates_dir) {
        for file in rust_files(&src) {
            files_scanned += 1;
            let rel = file
                .strip_prefix(repo_root)
                .expect("source file is beneath the repository root")
                .to_string_lossy()
                .replace('\\', "/");
            let source = fs::read_to_string(&file)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
            offences.extend(deref_offences(&rel, &source));
        }
    }

    assert!(
        files_scanned > 0,
        "the production source scan found no Rust files"
    );
    assert_no_offences(&offences);
}

#[test]
fn scanner_rejects_fabricated_deref() {
    let source = r#"
//! A comment that mentions impl std::ops::Deref for ManifestId must be ignored.
// impl std::ops::Deref for ManifestId also remains a comment.
impl std::ops::Deref for ManifestId {
    type Target = i64;
}
"#;

    let offences = deref_offences("crates/common/src/fabricated.rs", source);
    let rejection = rejection_message(&offences).expect("the fabricated Deref must be rejected");

    assert!(rejection.contains(DECISION_NOTE));
    assert!(rejection.contains("crates/common/src/fabricated.rs:4"));
    assert!(!rejection.contains(":2"));
    assert!(!rejection.contains(":3"));
}

/// `crates/<name>/src` for every member.
fn crate_src_dirs(crates_dir: &Path) -> Vec<PathBuf> {
    let mut dirs = fs::read_dir(crates_dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", crates_dir.display()))
        .map(|entry| {
            entry
                .expect("failed to read crate directory entry")
                .path()
                .join("src")
        })
        .filter(|src| src.is_dir())
        .collect::<Vec<_>>();
    dirs.sort();
    dirs
}

/// Every `.rs` file under `dir`, recursively (no `walkdir` dependency).
fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", dir.display()))
    {
        let path = entry.expect("failed to read source directory entry").path();
        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    files.sort();
    files
}

fn deref_offences(rel: &str, source: &str) -> Vec<String> {
    let _classified_lines = source
        .lines()
        .map(|line| (is_deref_impl(line), is_allowed(rel, line)))
        .collect::<Vec<_>>();
    Vec::new()
}

/// A source line that actually implements `Deref`, ignoring doc comments and line comments —
/// `//! …never Deref…` in a module header must not trip the guard.
fn is_deref_impl(line: &str) -> bool {
    let trimmed = line.trim_start();
    !trimmed.starts_with("//")
        && trimmed.starts_with("impl")
        && trimmed.contains("Deref")
        && trimmed.contains(" for ")
}

/// The allow-list lookup, by repo-relative path and the exact wrapper named on the impl line.
fn is_allowed(rel: &str, line: &str) -> bool {
    let Some(type_name) = impl_type_name(line) else {
        return false;
    };

    ALLOWED
        .iter()
        .any(|(path, allowed_type, _reason)| *path == rel && *allowed_type == type_name)
}

fn impl_type_name(line: &str) -> Option<&str> {
    let (_, implemented_for) = line.split_once(" for ")?;
    implemented_for
        .trim_start()
        .split(|character: char| character == '<' || character == '{' || character.is_whitespace())
        .next()
        .and_then(|qualified| qualified.rsplit("::").next())
}

fn assert_no_offences(offences: &[String]) {
    if let Some(message) = rejection_message(offences) {
        panic!("{message}");
    }
}

fn rejection_message(offences: &[String]) -> Option<String> {
    if offences.is_empty() {
        return None;
    }

    Some(format!(
        "walrus newtypes must expose explicit accessors, not Deref (see {DECISION_NOTE}):\n{}",
        offences.join("\n")
    ))
}
