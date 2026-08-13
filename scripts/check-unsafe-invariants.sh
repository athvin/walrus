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
ADR="docs/implementation/notes/rust-skills/unsafe-miri-ci.md"
WALRUS_SELF_TEST_DIR=""

cleanup_self_test() {
  if [[ -n "$WALRUS_SELF_TEST_DIR" && -d "$WALRUS_SELF_TEST_DIR" ]]; then
    rm -rf -- "$WALRUS_SELF_TEST_DIR"
  fi
}
trap cleanup_self_test EXIT

scan_uninit() {
  echo "== fake initialization (${UNINIT_PATTERN}) =="
  local hits
  hits="$(grep -rnE --include='*.rs' "$UNINIT_PATTERN" "$@" 2>/dev/null || true)"
  if [[ -n "$hits" ]]; then
    printf '%s\n' "$hits" >&2
    echo "FAIL: uninitialized-memory construction found in first-party sources." >&2
    echo "      walrus gets uninitialized spare capacity safely via BytesMut + read_buf." >&2
    echo "      See docs/implementation/notes/rust-skills/unsafe-maybeuninit.md" >&2
    return 1
  fi
  echo "   0 sites"
}

fail() {
  echo "FAIL: $*" >&2
  return 1
}

has_workspace_unsafe_forbid() {
  awk '
    /^[[:space:]]*\[workspace\.lints\.rust\][[:space:]]*$/ { inside = 1; next }
    /^[[:space:]]*\[/ { inside = 0 }
    inside && /^[[:space:]]*unsafe_code[[:space:]]*=[[:space:]]*"forbid"([[:space:]]*(#.*)?)?$/ {
      found = 1
    }
    END { exit(found ? 0 : 1) }
  ' "$1"
}

member_inherits_workspace_lints() {
  awk '
    /^[[:space:]]*\[lints\][[:space:]]*$/ { inside = 1; next }
    /^[[:space:]]*\[/ { inside = 0 }
    inside && /^[[:space:]]*workspace[[:space:]]*=[[:space:]]*true([[:space:]]*(#.*)?)?$/ {
      found = 1
    }
    END { exit(found ? 0 : 1) }
  ' "$1"
}

workspace_members() {
  sed -nE 's/^[[:space:]]*members[[:space:]]*=[[:space:]]*\[([^]]*)\].*/\1/p' "$1" \
    | tr ',' '\n' \
    | tr -d '" '
}

check_unsafe_policy() {
  local root="${1%/}"
  local workspace_manifest="$root/Cargo.toml"
  echo "== unsafe policy still in force (PR 12.1 forbid + per-member inheritance) =="

  if ! has_workspace_unsafe_forbid "$workspace_manifest"; then
    fail "$workspace_manifest lost \`unsafe_code = \"forbid\"\` (PR 12.1). Read $ADR before removing it."
    return 1
  fi

  local members
  members="$(workspace_members "$workspace_manifest")"
  if [[ -z "$members" ]]; then
    fail "$workspace_manifest has no parseable one-line workspace members list. See $ADR."
    return 1
  fi

  local member
  while IFS= read -r member; do
    [[ -n "$member" ]] || continue
    local member_manifest="$root/$member/Cargo.toml"
    if ! member_inherits_workspace_lints "$member_manifest"; then
      fail "$member_manifest no longer inherits \`[lints] workspace = true\`; the workspace forbid does not reach it. See $ADR."
      return 1
    fi
    echo "   ok: $member inherits [lints] workspace = true"
  done < <(printf '%s\n' "$members")
}

write_policy_fixture() {
  local root="$1"
  local root_lint="$2"
  local member_lint="$3"
  mkdir -p "$root/crates/example"
  printf '%s\n' \
    '[workspace]' \
    'members = ["crates/example"]' \
    '' \
    '[workspace.lints.rust]' \
    "$root_lint" >"$root/Cargo.toml"
  printf '%s\n' \
    '[package]' \
    'name = "example"' \
    'version = "0.0.0"' \
    'edition.workspace = true' \
    '' \
    '[lints]' \
    "$member_lint" >"$root/crates/example/Cargo.toml"
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

  local clean_policy="$WALRUS_SELF_TEST_DIR/policy-clean"
  local missing_root="$WALRUS_SELF_TEST_DIR/policy-missing-root"
  local missing_member="$WALRUS_SELF_TEST_DIR/policy-missing-member"
  local missing_root_log="$WALRUS_SELF_TEST_DIR/policy-missing-root.log"
  local missing_member_log="$WALRUS_SELF_TEST_DIR/policy-missing-member.log"
  write_policy_fixture "$clean_policy" 'unsafe_code = "forbid"' 'workspace = true'
  write_policy_fixture "$missing_root" 'warnings = "deny"' 'workspace = true'
  write_policy_fixture "$missing_member" 'unsafe_code = "forbid"' ''

  if ! check_unsafe_policy "$clean_policy" >/dev/null 2>&1; then
    echo "not ok: clean temporary policy fixture was rejected" >&2
    return 1
  fi
  echo "ok: clean temporary policy fixture passes"

  if check_unsafe_policy "$missing_root" >"$missing_root_log" 2>&1; then
    echo "not ok: temporary root without unsafe_code = \"forbid\" unexpectedly passed" >&2
    return 1
  fi
  if ! grep -F "$ADR" "$missing_root_log" >/dev/null; then
    echo "not ok: missing-root diagnostic did not point to $ADR" >&2
    return 1
  fi
  grep -F 'FAIL:' "$missing_root_log"
  echo "ok: temporary root without unsafe_code = \"forbid\" is rejected"

  if check_unsafe_policy "$missing_member" >"$missing_member_log" 2>&1; then
    echo "not ok: temporary member without workspace = true unexpectedly passed" >&2
    return 1
  fi
  if ! grep -F "$ADR" "$missing_member_log" >/dev/null; then
    echo "not ok: missing-member diagnostic did not point to $ADR" >&2
    return 1
  fi
  grep -F 'FAIL:' "$missing_member_log"
  echo "ok: temporary member without workspace = true is rejected"
  echo "check-unsafe-invariants self-test: PASS"
}

case "${1:-}" in
  "")
    # First-party Rust source roots only. Dependency and generated-code unsafe internals are outside
    # this policy; legitimate reserve-only calls such as `with_capacity` are not in the pattern.
    SCOPE=(crates/*/src tests/*/src)
    scan_uninit "${SCOPE[@]}"
    check_unsafe_policy "."
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
