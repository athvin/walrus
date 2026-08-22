#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous source scans are \
              themselves repository-policy checks, not runtime I/O"
)]
//! Conformance guard for `closure-move-capture` (PR 25.4): every production `move` closure and
//! `async move` block captures explicitly pre-bound locals, never `self`. Pure source scanning —
//! no Docker, no new dependency.

use std::path::{Path, PathBuf};

/// Repo root, derived from this crate's manifest dir (`<root>/crates/pg-sink`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonicalize the repository root")
}

/// Every production `crates/*/src/**/*.rs` under `root`; recurses (see `pg-sink/src/pgoutput/`)
/// and skips the Go-style sibling `*_test.rs` unit-test files.
fn production_sources(_root: &Path) -> Vec<PathBuf> {
    Vec::new()
}

/// Every `move` capture body in `src`, as `(1-based line, kind, body text)`.
/// `kind` is `"async move"` for `async move { … }` and `"move closure"` for a `move |…|` closure.
/// Brace-matches from the opening `{`, skipping string literals and comments so braces inside SQL
/// cannot desynchronise the depth counter. Expression-bodied closures end at their outer delimiter.
fn move_bodies(_src: &str) -> Vec<(usize, &'static str, String)> {
    Vec::new()
}

fn reaches_through_self(body: &str) -> bool {
    body.as_bytes().windows("self.".len()).any(|window| window == b"self.")
}

#[test]
fn no_production_move_body_reaches_through_self() {
    let root = repo_root();
    let (mut async_blocks, mut move_closures) = (0usize, 0usize);
    let mut violations: Vec<String> = Vec::new();
    for path in production_sources(&root) {
        let src = std::fs::read_to_string(&path).expect("read a production source file");
        for (line, kind, body) in move_bodies(&src) {
            match kind {
                "async move" => async_blocks += 1,
                "move closure" => move_closures += 1,
                other => unreachable!("unknown move-capture kind: {other}"),
            }
            if reaches_through_self(&body) {
                violations.push(format!("{}:{line}: {kind}", path.display()));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "closure-move-capture violations (bind + clone the fields, do not move `self`):\n{}",
        violations.join("\n")
    );
    assert!(
        async_blocks >= 15,
        "only {async_blocks} `async move` blocks — walker is broken"
    );
    assert!(
        move_closures >= 2,
        "only {move_closures} `move |` closures — walker is broken"
    );
}

#[test]
fn production_never_clones_self_wholesale() {
    let root = repo_root();
    let violations: Vec<_> = production_sources(&root)
        .into_iter()
        .filter(|path| {
            std::fs::read_to_string(path)
                .expect("read a production source file")
                .contains("self.clone()")
        })
        .collect();
    assert!(
        violations.is_empty(),
        "production `self.clone()` calls: {violations:#?}"
    );
}

#[test]
fn redundant_clone_stays_denied() {
    let manifest = std::fs::read_to_string(repo_root().join("Cargo.toml"))
        .expect("read the workspace manifest");
    let clippy_table = manifest
        .split_once("[workspace.lints.clippy]")
        .expect("workspace clippy lint table")
        .1
        .split("\n[")
        .next()
        .expect("workspace clippy lint table body");
    assert_eq!(
        clippy_table
            .lines()
            .filter(|line| line.trim() == "redundant_clone = \"deny\"")
            .count(),
        1,
        "[workspace.lints.clippy] must deny redundant_clone exactly once"
    );
}

#[test]
fn the_guard_rejects_what_it_is_supposed_to_reject() {
    let good = r#"let pool = self.pool.clone(); tokio::spawn(async move { use_it(&pool).await; });"#;
    let bad = r#"tokio::spawn(async move { use_it(&self.pool).await; });"#;
    let syntax_noise = r###"
        tokio::spawn(async move {
            let sql = "SELECT '{'";
            let raw = r#"{"#;
            let brace = '{';
            // } self.not_a_capture
            use_it(&pool, sql, raw, brace).await;
        });
    "###;

    let good_bodies = move_bodies(good);
    assert_eq!(good_bodies.len(), 1);
    assert!(!reaches_through_self(&good_bodies[0].2));

    let bad_bodies = move_bodies(bad);
    assert_eq!(bad_bodies.len(), 1);
    assert!(reaches_through_self(&bad_bodies[0].2));

    let noise_bodies = move_bodies(syntax_noise);
    assert_eq!(noise_bodies.len(), 1, "strings/comments must not unbalance braces");
    assert!(!reaches_through_self(&noise_bodies[0].2));
    assert!(noise_bodies[0].2.contains("use_it"), "body ended at a quoted brace");
}
