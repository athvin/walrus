#![allow(clippy::unwrap_used, clippy::expect_used)] // integration test — unwrap/expect fine in setup + helpers
//! Conformance gate for `err-lowercase-msg` (PR 10.7): every production `#[error("…")]` literal
//! starts lowercase (or with an allow-listed acronym) and carries no trailing sentence punctuation,
//! so `{:#}` chains read as one sentence. Pure source scanning — no Docker, no new dependency.

use std::path::{Path, PathBuf};

/// Acronyms / proper nouns the rule permits at the start of a message. Keep this SHORT: every
/// entry is an exception, and a long list means the convention is being eroded, not enforced.
const ACRONYM_ALLOW: &[&str] = &["DuckDB", "DDL", "LSN", "JSON", "TLS", "S3", "UTF-8"];

/// Repo root, derived from this crate's manifest dir (`<root>/crates/common`).
fn repo_root() -> PathBuf {
    todo!("derive and canonicalize the repo root")
}

/// Every production `crates/*/src/**/*.rs` under `root`. Recurses (see `src/pgoutput/`) and skips
/// the Go-style sibling `*_test.rs` unit-test files.
fn production_sources(_root: &Path) -> Vec<PathBuf> {
    todo!("walk production sources recursively")
}

/// Each `#[error("…")]` literal in `src`, as `(1-based line number, logical message)`.
/// `#[error(transparent)]` yields nothing; `\`-continuations are joined into one message.
fn error_literals(_src: &str) -> Vec<(usize, String)> {
    todo!("extract logical thiserror messages")
}

/// `Err(reason)` when `msg` breaks the convention; `Ok(())` otherwise.
fn check(_msg: &str) -> Result<(), String> {
    let _ = ACRONYM_ALLOW;
    todo!("enforce lowercase starts and punctuation-free endings")
}

#[test]
fn every_production_error_literal_follows_the_convention() {
    let root = repo_root();
    let sources = production_sources(&root);
    assert!(
        sources.len() >= 80,
        "source walk found only {} files",
        sources.len()
    );
    assert!(
        sources
            .iter()
            .any(|path| path.ends_with("crates/pg-sink/src/pgoutput/error.rs")),
        "source walk did not recurse into pgoutput"
    );
    assert!(
        sources.iter().all(|path| !path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with("_test.rs"))),
        "source walk included a sibling test"
    );

    let mut violations: Vec<String> = Vec::new();
    for path in sources {
        let src = std::fs::read_to_string(&path).expect("read a production source file");
        for (line, msg) in error_literals(&src) {
            if let Err(why) = check(&msg) {
                violations.push(format!("{}:{line}: {why} — {msg:?}", path.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "err-lowercase-msg violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn the_gate_rejects_what_it_is_supposed_to_reject() {
    let fixture = r#"
        #[error("invalid configuration: {0}")]
        #[error("{0}")]
        #[error("DuckDB: {0}")]
        #[error("DDL capture not installed: {detail}")]
        #[error("Failed to read config")]
        #[error("failed to read config.")]
        #[error("invalid JSON format!")]
    "#;
    let results: Vec<_> = error_literals(fixture)
        .into_iter()
        .map(|(_, message)| check(&message))
        .collect();
    assert_eq!(results.len(), 7);
    assert!(results[..4].iter().all(Result::is_ok));
    assert!(results[4..].iter().all(Result::is_err));
    assert!(check("DuckDB: {0}").is_ok());
    assert!(check("Failed.").is_err());
}

#[test]
fn the_extractor_sees_multi_line_and_skips_transparent() {
    let src = r#"
        #[error(transparent)]
        Config(#[from] ConfigError),
        #[error(
            "publication {p} missing table {s}.{t} \
             (fix: ALTER PUBLICATION {p} ADD TABLE {s}.{t})"
        )]
        PublicationGap { p: String, s: String, t: String },
    "#;
    let found = error_literals(src);
    assert_eq!(found.len(), 1, "transparent must not be extracted");
    assert!(found[0].1.starts_with("publication "));
    assert!(
        found[0].1.contains("ALTER PUBLICATION"),
        "continuation must be joined"
    );
}
