//! Guard for the mutable-conversion decision note: walrus deliberately has no generic mutable
//! write target because it has no fixed-length byte sink. If that changes, update the ADR in the
//! same PR instead of deleting this test.

use std::path::{Path, PathBuf};

/// Build the bound marker at runtime so this file does not match itself.
fn needle() -> String {
    ["As", "Mut", "<"].concat()
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read source directory {}: {error}", dir.display()))
    {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry in {}: {error}", dir.display()))
            .path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            rs_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn no_asmut_bounds_anywhere_in_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    rs_files(&root.join("crates"), &mut files);
    assert!(!files.is_empty(), "the source walk found nothing — bad root path");

    let needle = needle();
    let mut hits = Vec::new();
    for path in files {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read source file {}: {error}", path.display()));
        for (line_index, line) in source.lines().enumerate() {
            if line.contains(&needle) {
                hits.push(format!("{}:{}", path.display(), line_index + 1));
            }
        }
    }

    assert!(
        hits.is_empty(),
        "generic mutable bound(s) appeared: {hits:?}\n\
         walrus has no fixed-length byte sink — see \
         docs/implementation/notes/rust-skills/conv-asmut-mutable.md before adding one",
    );
}
