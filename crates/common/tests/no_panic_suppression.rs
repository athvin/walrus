#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; synchronous source scans are \
              themselves repository-policy checks, not runtime I/O"
)]
//! Guard for `anti-panic-expected`: the last way the no-panic policy can be undone in silence.
//!
//! A recoverable failure — a bad config value, a short wire frame, a malformed range literal —
//! reaches `main` as a typed `Err` and leaves as a `common::ExitCode`. Nothing in the type system
//! requires that. What requires it is `[workspace.lints.clippy]` denying `panic`, `todo`,
//! `unimplemented`, `unreachable` and `panic_in_result_fn`, which
//! `tests/workspace_lints_inherited.rs` pins entry by entry. One vector walks past that pin: lint
//! levels are innermost-wins, so a single `#![allow(clippy::panic, reason = "…")]` at the top of a
//! production module reopens `panic!` for that whole file — manifest untouched, `clippy.toml`
//! untouched, build green. Nothing in the toolchain reports it, because the suppression *is* the
//! toolchain's answer. Only a source scan notices, which is what this file is.
//!
//! Scope is `crates/*/src/**/*.rs` minus the `*_test.rs` siblings, matching
//! `no_unwrap_suppression.rs`. Tests are the rule's own carve-out — asserting an impossible branch
//! is exactly where `unreachable!` belongs — so `crates/*/tests/` targets and the `*_test.rs`
//! siblings carry these allows on purpose. `clippy.toml`'s `allow-panic-in-tests` covers `panic`
//! under `#[cfg(test)]` and has no counterpart for `unreachable`, which is why
//! `crates/pg-sink/src/reload_test.rs` names that one for itself.
//!
//! walrus keeps exactly one production exception, and its *spelling* is load-bearing: [`CARVE_OUT`]
//! writes `#[expect(…)]`, not `#[allow(…)]`. `backfill::plan_ctid_ranges` is the marked seam for
//! the deferred parallel CTID-range backfill and nothing calls it; the day someone implements it
//! the expectation goes unfulfilled, `warnings = "deny"` fails the build, and the exception retires
//! itself. An `#[allow]` there would outlive its reason with nothing to say so.

use std::path::{Path, PathBuf};

/// The five lints that keep an expected failure from aborting the process. A production file that
/// names any of them — in any attribute — is turning that lint off for itself, so the mention alone
/// is the offence.
const PANIC_LINTS: [&str; 5] = [
    "clippy::panic",
    "clippy::todo",
    "clippy::unimplemented",
    "clippy::unreachable",
    "clippy::panic_in_result_fn",
];

/// The one production module allowed to name one of them, and the only lint it may name. The
/// deferred CTID-range seam is an unwritten function, not a failure walrus chose to crash on.
const CARVE_OUT: &str = "crates/pg-sink/src/backfill.rs";
const CARVE_OUT_LINT: &str = "clippy::unimplemented";

/// The attribute form that expires on its own. Written without the leading `#` so one needle matches
/// both the outer `#[expect(…)]` and the inner `#![expect(…)]` spelling.
const EXPECT_FORM: &str = "[expect(";

/// The form that does not expire: a suppression that outlives the reason for it.
const ALLOW_FORM: &str = "[allow(";

/// Repo root, derived from this crate's manifest dir (`<root>/crates/common`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every production `crates/*/src/**/*.rs` under `root`. Recurses (see `src/pgoutput/`) and skips
/// the Go-style sibling `*_test.rs` unit-test files.
fn production_sources(root: &Path) -> Vec<PathBuf> {
    fn visit(dir: &Path, sources: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read a source directory") {
            let path = entry.expect("read a source-directory entry").path();
            if path.is_dir() {
                visit(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs")
                && !path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().ends_with("_test.rs"))
            {
                sources.push(path);
            }
        }
    }

    let mut sources = Vec::new();
    for entry in std::fs::read_dir(root.join("crates")).expect("read the workspace crates") {
        let src = entry.expect("read a crate entry").path().join("src");
        if src.is_dir() {
            visit(&src, &mut sources);
        }
    }
    sources.sort();
    sources
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// Whether `line` names `lint` as a whole lint path. `clippy::panic` is a prefix of
/// `clippy::panic_in_result_fn`, so a plain `contains` would report the longer entry twice — once
/// under a name nobody touched.
fn names_lint(line: &str, lint: &str) -> bool {
    let mut rest = line;
    while let Some(at) = rest.find(lint) {
        let tail = &rest[at + lint.len()..];
        if !tail.starts_with(|c: char| c.is_alphanumeric() || c == '_') {
            return true;
        }
        rest = tail;
    }
    false
}

/// Every suppression of a [`PANIC_LINTS`] entry in `source`, as a reportable line. The carve-out is
/// exempt for its one lint only — the seam is no licence to suppress the other four. A line whose
/// first non-space characters are `//` is prose: a doc comment may name the lint it explains.
fn offences(relative: &str, source: &str) -> Vec<String> {
    let exempt = (relative == CARVE_OUT).then_some(CARVE_OUT_LINT);

    let mut found = Vec::new();
    for (index, line) in source.lines().enumerate() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        for lint in PANIC_LINTS {
            if Some(lint) == exempt || !names_lint(line, lint) {
                continue;
            }
            let line_number = index + 1;
            found.push(format!("{relative}:{line_number}: suppresses {lint}"));
        }
    }
    found
}

/// The attribute enclosing the first mention of `lint` in `source`: which family it belongs to, and
/// its text up to the closing `)]`. `None` when `source` never names the lint, or names it outside
/// any attribute.
fn enclosing_attribute<'a>(source: &'a str, lint: &str) -> Option<(&'static str, &'a str)> {
    let head = &source[..source.find(lint)?];
    let (form, start) = match (head.rfind(EXPECT_FORM), head.rfind(ALLOW_FORM)) {
        (Some(expect_at), None) => (EXPECT_FORM, expect_at),
        (None, Some(allow_at)) => (ALLOW_FORM, allow_at),
        (Some(expect_at), Some(allow_at)) if expect_at > allow_at => (EXPECT_FORM, expect_at),
        (Some(_), Some(allow_at)) => (ALLOW_FORM, allow_at),
        (None, None) => return None,
    };
    let end = start + source[start..].find(")]")?;
    Some((form, &source[start..end]))
}

#[test]
fn no_production_source_reopens_the_panic_denies() {
    let root = repo_root();
    let sources = production_sources(&root);
    let relatives = sources
        .iter()
        .map(|path| display_path(&root, path))
        .collect::<Vec<_>>();

    assert!(
        relatives.iter().any(|path| path == CARVE_OUT),
        "the scan must reach {CARVE_OUT}, the one file it exempts — otherwise it is vacuous"
    );

    let mut found = Vec::new();
    for (path, relative) in sources.iter().zip(&relatives) {
        let source = std::fs::read_to_string(path).expect("read a production source");
        found.extend(offences(relative, &source));
    }

    assert!(
        found.is_empty(),
        "a scoped allow turns an expected error back into a crash for a whole module while the \
         manifest still reads as policy; {CARVE_OUT} is the one exception:\n{}",
        found.join("\n")
    );
}

/// The other half of "one exception": the exception itself. It must stay an `#[expect]`, because
/// that is what makes it self-retiring, and it must stay scoped to `unimplemented`, because a
/// deferred seam is the only thing in this tree entitled to diverge on purpose.
#[test]
fn the_deferred_backfill_seam_is_still_a_self_retiring_expect() {
    let source = std::fs::read_to_string(repo_root().join(CARVE_OUT))
        .expect("read the one production module with a carve-out");

    for lint in PANIC_LINTS {
        assert!(
            lint == CARVE_OUT_LINT || !source.contains(lint),
            "{CARVE_OUT} is exempt for the deferred seam alone; it must never suppress {lint}"
        );
    }

    let (form, attribute) = enclosing_attribute(&source, CARVE_OUT_LINT)
        .expect("the carve-out module must still carry the deferred-seam suppression");

    assert_eq!(
        form, EXPECT_FORM,
        "the carve-out must be #[expect(…)], so an unfulfilled expectation retires it the day the \
         parallel CTID-range backfill lands; #[allow(…)] would sit there forever"
    );
    assert!(
        attribute.contains("reason ="),
        "a suppression must say why (clippy::allow_attributes_without_reason):\n{attribute}"
    );
}

#[test]
fn the_scan_rejects_a_planted_suppression_and_spares_prose() {
    let planted = concat!(
        "#![allow(clippy::panic, reason = \"legacy module\")]\n",
        "//! Contrast `clippy::todo`, which this module never suppresses.\n",
        "pub fn port(raw: &str) -> u16 {\n",
        "    #[allow(clippy::unreachable, reason = \"validated at startup\")]\n",
        "    match raw.parse() {\n",
        "        Ok(port) => port,\n",
        "        Err(_) => unreachable!(),\n",
        "    }\n",
        "}\n",
    );

    let found = offences("crates/loader/src/port.rs", planted);
    let report = found.join("\n");

    assert_eq!(found.len(), 2, "{report}");
    for expected in [
        "crates/loader/src/port.rs:1: suppresses clippy::panic",
        "crates/loader/src/port.rs:4: suppresses clippy::unreachable",
    ] {
        assert!(report.contains(expected), "{report}");
    }
}

/// The carve-out is a property of one path, not of the lint: the same attribute anywhere else is an
/// offence.
#[test]
fn the_carve_out_exempts_only_its_own_file() {
    let seam = "#[expect(clippy::unimplemented, reason = \"deferred goal\")]\n";

    assert!(offences(CARVE_OUT, seam).is_empty());
    assert_eq!(offences("crates/loader/src/port.rs", seam).len(), 1);
}

/// `clippy::panic` is a prefix of `clippy::panic_in_result_fn`. Reporting the longer entry under
/// the shorter name would send a reader to a deny that was never touched.
#[test]
fn the_lint_matcher_reads_whole_paths_not_prefixes() {
    let longer = "#[allow(clippy::panic_in_result_fn, reason = \"legacy\")]\n";
    let expected = "crates/loader/src/port.rs:1: suppresses clippy::panic_in_result_fn";
    let found = offences("crates/loader/src/port.rs", longer);

    assert_eq!(found, [expected]);
    assert!(names_lint("clippy::panic,", "clippy::panic"));
    assert!(!names_lint("clippy::panicky", "clippy::panic"));
}

#[test]
fn the_attribute_reader_tells_a_self_retiring_expect_from_an_allow() {
    let retiring = concat!(
        "#[expect(\n",
        "    clippy::unimplemented,\n",
        "    reason = \"deferred goal\"\n",
        ")]\n",
        "fn plan() -> Plan { unimplemented!() }\n",
    );
    let permanent = "#![allow(clippy::unimplemented, reason = \"legacy\")]\n";
    let unsuppressed = "fn plan() -> Plan { unimplemented!() }\n";

    let (form, attribute) = enclosing_attribute(retiring, CARVE_OUT_LINT).unwrap();
    let (permanent_form, _) = enclosing_attribute(permanent, CARVE_OUT_LINT).unwrap();

    assert_eq!(form, EXPECT_FORM);
    assert!(attribute.contains("reason = \"deferred goal\""));
    assert_eq!(permanent_form, ALLOW_FORM);
    assert!(enclosing_attribute(unsuppressed, CARVE_OUT_LINT).is_none());
}
