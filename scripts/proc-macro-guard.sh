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
  local direct
  # grep exits 1 when it matches nothing, which is the success case. Keep that status from
  # terminating a clean check under `set -e`, then decide from the captured diagnostics.
  direct=$(grep -nEH -- \
    '^[[:space:]]*(syn|quote|proc-macro2)[[:space:]]*[.=]|^[[:space:]]*\[[^]]*dependencies\.(syn|quote|proc-macro2)\]' \
    "$@" || true)

  if [ -n "$direct" ]; then
    echo "::error::direct syn/quote/proc-macro2 dependency declared in a workspace manifest — walrus authors no proc-macros (docs/implementation/notes/rust-skills/macro-proc-syn-quote.md)"
    echo "$direct"
    return 1
  fi

  echo "ok: 0 direct syn/quote/proc-macro2 dependencies across $# manifests (transitive copies are expected and fine)"
}

self_test() {
  local output
  PROC_MACRO_GUARD_FIXTURE_DIR=$(mktemp -d)
  trap 'rm -rf -- "$PROC_MACRO_GUARD_FIXTURE_DIR"' EXIT

  mkdir -p "$PROC_MACRO_GUARD_FIXTURE_DIR/fixture"
  printf '%s\n' '[dependencies]' 'syn = "2"' \
    >"$PROC_MACRO_GUARD_FIXTURE_DIR/fixture/direct-syn.toml"
  printf '%s\n' '[dependencies]' 'syn.workspace = true' 'quote.workspace = true' \
    >"$PROC_MACRO_GUARD_FIXTURE_DIR/fixture/workspace-dependencies.toml"
  printf '%s\n' '[build-dependencies]' 'syn = { version = "2" }' \
    'proc-macro2 = { version = "1" }' \
    >"$PROC_MACRO_GUARD_FIXTURE_DIR/fixture/inline-dependencies.toml"
  printf '%s\n' '[dev-dependencies.proc-macro2]' 'version = "1"' \
    '[build-dependencies.quote]' 'version = "1"' \
    >"$PROC_MACRO_GUARD_FIXTURE_DIR/fixture/dependency-tables.toml"

  if output=$(check_direct "$PROC_MACRO_GUARD_FIXTURE_DIR"/fixture/*.toml 2>&1); then
    echo "::error::proc-macro guard self-test expected direct dependency fixtures to be rejected"
    echo "$output"
    exit 1
  fi

  echo "$output"
  for expected in \
    '::error::direct syn/quote/proc-macro2 dependency declared' \
    'fixture/direct-syn.toml' \
    'fixture/workspace-dependencies.toml' \
    'fixture/inline-dependencies.toml' \
    'fixture/dependency-tables.toml' \
    'syn = "2"' \
    'syn.workspace = true' \
    'quote.workspace = true' \
    'syn = { version = "2" }' \
    'proc-macro2 = { version = "1" }' \
    '[dev-dependencies.proc-macro2]' \
    '[build-dependencies.quote]'; do
    if ! grep -Fq -- "$expected" <<<"$output"; then
      echo "::error::proc-macro guard self-test did not report expected fixture declaration: $expected"
      exit 1
    fi
  done

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
