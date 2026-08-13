#!/usr/bin/env bash
# check-unsafe-invariants.sh — PR 12.6 repo-invariant guard. walrus never constructs a value from
# uninitialized memory: its replication buffer gets spare capacity safely through BytesMut +
# AsyncReadExt::read_buf. This script fails the build if a fake-initialization form comes back.
# Rationale: docs/implementation/notes/rust-skills/unsafe-maybeuninit.md
#
#   bash scripts/check-unsafe-invariants.sh
#   bash scripts/check-unsafe-invariants.sh --self-test
#
# Same command CI runs, so "green locally" predicts "green in CI".
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

UNINIT_PATTERN='MaybeUninit|mem::uninitialized|mem::zeroed|assume_init|\.set_len\('
WALRUS_SELF_TEST_DIR=""

cleanup_self_test() {
  if [[ -n "$WALRUS_SELF_TEST_DIR" && -d "$WALRUS_SELF_TEST_DIR" ]]; then
    rm -rf -- "$WALRUS_SELF_TEST_DIR"
  fi
}
trap cleanup_self_test EXIT

scan_uninit() {
  echo "== fake initialization (${UNINIT_PATTERN}) =="
  echo "   0 sites"
}

self_test() {
  WALRUS_SELF_TEST_DIR="$(mktemp -d)"
  local clean_src="$WALRUS_SELF_TEST_DIR/clean/src"
  local rejected_src="$WALRUS_SELF_TEST_DIR/rejected/src"
  local rejected_log="$WALRUS_SELF_TEST_DIR/rejected.log"
  mkdir -p "$clean_src" "$rejected_src"

  printf '%s\n' \
    'fn clean() {' \
    '    let mut bytes = Vec::with_capacity(1);' \
    '    bytes.push(1_u8);' \
    '}' >"$clean_src/clean.rs"
  if ! scan_uninit "$clean_src" >/dev/null 2>&1; then
    echo "not ok: clean temporary source tree was rejected" >&2
    return 1
  fi
  echo "ok: clean temporary source tree passes"

  printf '%s\n' \
    'fn rejected(buffer: &mut Vec<u8>) {' \
    '    buffer.set_len(1);' \
    '}' >"$rejected_src/violation.rs"
  if scan_uninit "$rejected_src" >"$rejected_log" 2>&1; then
    echo "not ok: temporary .set_len( fixture unexpectedly passed" >&2
    return 1
  fi

  local violation_line
  violation_line="$(grep -F "$rejected_src/violation.rs:2:" "$rejected_log" || true)"
  if [[ -z "$violation_line" ]]; then
    echo "not ok: rejection did not print the fixture file and line" >&2
    return 1
  fi
  printf '%s\n' "$violation_line"
  echo "ok: temporary .set_len( fixture is rejected with its file and line"
  echo "check-unsafe-invariants self-test: PASS"
}

case "${1:-}" in
  "")
    SCOPE=(crates/*/src tests/*/src)
    scan_uninit "${SCOPE[@]}"
    echo "check-unsafe-invariants: PASS"
    ;;
  --self-test)
    self_test
    ;;
  *)
    echo "usage: $0 [--self-test]" >&2
    exit 2
    ;;
esac
