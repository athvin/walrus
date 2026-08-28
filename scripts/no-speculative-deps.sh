#!/usr/bin/env bash
# no-speculative-deps.sh — PR 11.17. Phase 11 evaluated five allocator/container crates and
# declined each with a measurement or a structural argument; the `perf-ahash` audit added the three
# faster-hasher crates on the same terms, and the `anti-premature-optimize` audit the four
# global-allocator swaps. This guard keeps those decisions honest: none may become a DIRECT
# dependency without first updating the ADR that declined it.
#
#   bash scripts/no-speculative-deps.sh
#
# Manifest-scoped ON PURPOSE: smallvec / arrayvec / tinyvec / bumpalo / ahash are legitimately present
# in Cargo.lock transitively (bumpalo arrives via wasm-bindgen-macro-support and zopfli; ahash via
# arrow and two vendored hashbrown versions). A lock-file guard would fail on day one and prove nothing.
set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)

# package name | ADR that declined it | dependency-key/package spelling accepted by Cargo.
# If a later PR adopts one on a measurement, delete its row and update that ADR. This list encodes
# decisions, not dogma.
#
# The last four rows are a different axis from the rest: a global-allocator swap replaces the system
# allocator for the whole process rather than one container or hasher, and — unlike a hand-written
# `GlobalAlloc`, which `unsafe_code = "forbid"` rejects — installing one takes no unsafe code in
# walrus, only a manifest line. None of the four is in Cargo.lock even transitively.
DECLINED=(
  "smallvec|docs/implementation/notes/rust-skills/mem-smallvec.md|smallvec"
  "arrayvec|docs/implementation/notes/rust-skills/mem-arrayvec.md|arrayvec"
  "thin-vec|docs/implementation/notes/rust-skills/mem-thinvec.md|thin[-_]vec"
  "compact_str|docs/implementation/notes/rust-skills/mem-compact-string.md|compact[-_]str"
  "bumpalo|docs/implementation/notes/rust-skills/mem-arena-allocator.md|bumpalo"
  "ahash|docs/implementation/notes/rust-skills/perf-ahash.md|ahash"
  "rustc-hash|docs/implementation/notes/rust-skills/perf-ahash.md|rustc[-_]hash"
  "gxhash|docs/implementation/notes/rust-skills/perf-ahash.md|gxhash"
  "tikv-jemallocator|docs/implementation/notes/rust-skills/anti-premature-optimize.md|tikv[-_]jemallocator"
  "jemallocator|docs/implementation/notes/rust-skills/anti-premature-optimize.md|jemallocator"
  "mimalloc|docs/implementation/notes/rust-skills/anti-premature-optimize.md|mimalloc"
  "snmalloc-rs|docs/implementation/notes/rust-skills/anti-premature-optimize.md|snmalloc[-_]rs"
)

scan_manifests() {
  local root=$1
  shift
  local manifests=("$@")
  local fail=0

  cd "$root"
  for manifest in "${manifests[@]}"; do
    if [ ! -f "$manifest" ]; then
      echo "no-speculative-deps: manifest not found: $manifest" >&2
      return 1
    fi
  done

  for entry in "${DECLINED[@]}"; do
    local crate adr spelling pattern output
    IFS='|' read -r crate adr spelling <<<"$entry"
    # Match ordinary dependency keys, dependency-table form, and renamed dependencies such as
    # `allocator = { package = "bumpalo", ... }`. Anchoring the key avoids comment-only hits.
    pattern="(^[[:space:]]*\"?${spelling}\"?[[:space:]]*=)|(^[[:space:]]*\[[^]]*(dependencies|dev-dependencies|build-dependencies)\.\"?${spelling}\"?\][[:space:]]*$)|(^[^#]*package[[:space:]]*=[[:space:]]*['\"]${spelling}['\"])"
    output=$(grep -nHE "$pattern" "${manifests[@]}" || true)
    while IFS=: read -r file line _rest; do
      [ -n "$file" ] || continue
      echo "::error file=${file},line=${line}::${file}:${line}: declined direct dependency '${crate}'; decision: ${adr}" >&2
      fail=1
    done <<<"$output"
  done

  if [ "$fail" -eq 0 ]; then
    echo "no-speculative-deps: ${#DECLINED[@]} declined crates checked across ${#manifests[@]} manifests — none is a direct dependency. OK"
  fi
  return "$fail"
}

self_test() {
  local output
  SPEC_DEPS_FIXTURE_DIR=$(mktemp -d)
  trap 'rm -rf "$SPEC_DEPS_FIXTURE_DIR"' EXIT
  mkdir -p "$SPEC_DEPS_FIXTURE_DIR/crates/example" "$SPEC_DEPS_FIXTURE_DIR/tests/e2e"

  cat >"$SPEC_DEPS_FIXTURE_DIR/Cargo.toml" <<'EOF'
[workspace]
members = ["crates/example", "tests/e2e"]

[workspace.dependencies]
serde = "1"
EOF
  cat >"$SPEC_DEPS_FIXTURE_DIR/crates/example/Cargo.toml" <<'EOF'
[package]
name = "example"
version = "0.1.0"

[dependencies]
serde = { workspace = true }
EOF
  cat >"$SPEC_DEPS_FIXTURE_DIR/tests/e2e/Cargo.toml" <<'EOF'
[package]
name = "e2e"
version = "0.1.0"
EOF

  local fixture_manifests=(Cargo.toml crates/example/Cargo.toml tests/e2e/Cargo.toml)
  if ! output=$(scan_manifests "$SPEC_DEPS_FIXTURE_DIR" "${fixture_manifests[@]}" 2>&1); then
    echo "$output" >&2
    echo "no-speculative-deps self-test: clean temporary manifest set failed" >&2
    return 1
  fi
  echo "ok: clean temporary manifest set passes"

  cat >"$SPEC_DEPS_FIXTURE_DIR/crates/example/Cargo.toml" <<'EOF'
[package]
name = "example"
version = "0.1.0"

[dependencies]
allocator = { package = "bumpalo", version = "3" }
EOF
  if output=$(scan_manifests "$SPEC_DEPS_FIXTURE_DIR" "${fixture_manifests[@]}" 2>&1); then
    echo "no-speculative-deps self-test: temporary bumpalo dependency unexpectedly passed" >&2
    return 1
  fi
  if ! grep -Fq "crates/example/Cargo.toml:6" <<<"$output" ||
    ! grep -Fq "docs/implementation/notes/rust-skills/mem-arena-allocator.md" <<<"$output"; then
    echo "$output" >&2
    echo "no-speculative-deps self-test: rejection omitted its file:line or ADR path" >&2
    return 1
  fi
  echo "ok: temporary bumpalo dependency is rejected with its ADR path"
  echo "no-speculative-deps self-test: PASS"
}

case "${1:-}" in
  "")
    cd "$REPO_ROOT"
    MANIFESTS=(Cargo.toml crates/*/Cargo.toml tests/e2e/Cargo.toml)
    scan_manifests "$REPO_ROOT" "${MANIFESTS[@]}"
    ;;
  --self-test)
    self_test
    ;;
  *)
    echo "usage: bash scripts/no-speculative-deps.sh [--self-test]" >&2
    exit 2
    ;;
esac
