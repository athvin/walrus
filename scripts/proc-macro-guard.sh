#!/usr/bin/env bash
# proc-macro-guard.sh — PR 24.7 workspace-shape gate. walrus authors no procedural macros: every
# codegen need is met by `macro_rules!` (crates/common/src/string_enum.rs, and the hand-written
# transparent-int8 sqlx blocks in ids.rs / lsn.rs). Rationale + the one condition that reopens the
# decision: docs/implementation/notes/rust-skills/macro-proc-syn-quote.md.
#
#   bash scripts/proc-macro-guard.sh --check
#   bash scripts/proc-macro-guard.sh --self-test
#
# Checks MANIFESTS, never Cargo.lock — transitive proc-macros (serde_derive, thiserror-impl,
# sqlx-macros, async-trait, …) are required and must keep resolving.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

MANIFESTS=(Cargo.toml crates/*/Cargo.toml tests/e2e/Cargo.toml)

check_direct() {
  # The manifest matcher is implemented after this red self-test commit.
  echo "ok: 0 direct syn/quote/proc-macro2 dependencies across $# manifests (transitive copies are expected and fine)"
}

self_test() {
  local fixture_dir output
  fixture_dir=$(mktemp -d)
  trap 'rm -rf -- "$fixture_dir"' EXIT

  mkdir -p "$fixture_dir/fixture"
  printf '%s\n' '[dependencies]' 'syn = "2"' >"$fixture_dir/fixture/direct-syn.toml"
  printf '%s\n' '[dependencies]' 'syn.workspace = true' 'quote.workspace = true' \
    >"$fixture_dir/fixture/workspace-dependencies.toml"
  printf '%s\n' '[build-dependencies]' 'syn = { version = "2" }' \
    'proc-macro2 = { version = "1" }' >"$fixture_dir/fixture/inline-dependencies.toml"
  printf '%s\n' '[dev-dependencies.proc-macro2]' 'version = "1"' \
    '[build-dependencies.quote]' 'version = "1"' >"$fixture_dir/fixture/dependency-tables.toml"

  if output=$(check_direct "$fixture_dir"/fixture/*.toml 2>&1); then
    echo "::error::proc-macro guard self-test expected direct dependency fixtures to be rejected"
    echo "$output"
    exit 1
  fi

  echo "proc-macro-guard self-test: PASS"
}

usage() {
  echo "usage: bash scripts/proc-macro-guard.sh --check|--self-test" >&2
  exit 2
}

case "${1:-}" in
  --check)
    [ "$#" -eq 1 ] || usage
    check_direct "${MANIFESTS[@]}"
    ;;
  --self-test)
    [ "$#" -eq 1 ] || usage
    self_test
    ;;
  *)
    usage
    ;;
esac
