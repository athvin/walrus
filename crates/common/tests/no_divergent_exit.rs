#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
#![allow(
    clippy::disallowed_methods,
    reason = "the synchronous integration-test scan is a repository-policy check, not runtime I/O"
)]

//! Guard (PR 18.13): walrus crate source must not declare never-returning functions or terminate
//! the process directly. Errors reach `main` as values and map onto `common::ExitCode`; shutdown
//! must unwind so Parquet flushes, watermarks commit, the ownership lease is released, and DuckDB
//! checkpoints. See `docs/implementation/notes/rust-skills/type-never-diverge.md`.

use std::fs;
use std::path::{Path, PathBuf};

/// Assembled so this guard never contains the direct-exit spelling that it hunts.
const EXIT_NEEDLE: &str = concat!("process", "::", "exit");
/// Compared with lines after removing ASCII whitespace, covering every spaced arrow spelling.
const NEVER_NEEDLE: &str = concat!("->", "!");

/// Repo-relative crate source paths permitted to diverge.
///
/// Empty today. Adding an entry is a design decision that must also update the evidence note.
const ALLOW_LIST: &[&str] = &[];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/common/../.. is the repository root")
        .to_path_buf()
}

fn crate_sources(root: &Path) -> Vec<PathBuf> {
    fs::read_dir(root.join("crates")).expect("the workspace crates directory must be readable");
    Vec::new()
}

fn offenders(_source: &str) -> Vec<(usize, &'static str)> {
    Vec::new()
}

#[test]
fn no_divergent_functions_in_crate_sources() {
    let root = repo_root();
    let sources = crate_sources(&root);
    assert!(
        !sources.is_empty(),
        "the crate-source scan must not be vacuous"
    );

    let mut violations = Vec::new();
    for path in sources {
        let relative = path
            .strip_prefix(&root)
            .expect("crate source must be beneath the repository root")
            .to_string_lossy()
            .replace('\\', "/");
        if ALLOW_LIST.contains(&relative.as_str()) {
            continue;
        }

        let source = fs::read_to_string(&path).expect("crate source must be readable");
        violations.extend(
            offenders(&source)
                .into_iter()
                .map(|(line, label)| format!("{relative}:{line}: {label}")),
        );
    }

    assert!(
        violations.is_empty(),
        "walrus crate source must unwind through its exit-code boundary; forbidden constructs:\n{}",
        violations.join("\n")
    );
}

#[test]
fn scanner_detects_a_planted_needle() {
    let planted = format!(
        "fn never_returns() {} {{ loop {{}} }}\nfn exits() {{ std::{}(1); }}",
        NEVER_NEEDLE, EXIT_NEEDLE
    );

    assert_eq!(
        offenders(&planted),
        vec![(1, "never-returning function"), (2, "direct process exit")]
    );
}
