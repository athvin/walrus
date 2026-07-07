#!/usr/bin/env bash
# green.sh — run walrus's baseline "green" gates (fmt, clippy, test) from the repo root and report
# which failed. Task-specific gates (docker compose, sqlx offline, conformance, cargo-deny,
# kubeconform) come from each task's Definition of Done — run those in addition.
# See reference/green-gates.md.
set -uo pipefail

repo="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$repo" ]]; then
  # Fall back to path math: scripts/ -> skill/ -> skills/ -> .claude/ -> repo root.
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  repo="$(cd "$here/../../../.." && pwd)"
fi

# Verify we actually resolved the walrus workspace before deciding anything — a mis-resolved root
# must fail LOUDLY, never silently report "nothing to check" (that would be a false green on a gate).
if [[ ! -f "$repo/docs/implementation/README.md" ]]; then
  echo "green.sh: '$repo' is not the walrus workspace (no docs/implementation/README.md) — refusing to report green." >&2
  exit 2
fi
cd "$repo" || { echo "green.sh: cannot cd to repo root '$repo'" >&2; exit 2; }

if [[ ! -f Cargo.toml ]]; then
  echo "green.sh: no Cargo.toml at the walrus root yet — the Cargo workspace lands in PR 0.1." >&2
  echo "green.sh: nothing to check until then." >&2
  exit 0
fi

fail=0
run() {
  echo "==> $*"
  if ! "$@"; then
    echo "green.sh: FAILED — $*" >&2
    fail=1
  fi
}

run cargo fmt --check
run cargo clippy --all-targets --all-features -- -D warnings
run cargo test --workspace

if [[ "$fail" -ne 0 ]]; then
  echo "green.sh: one or more baseline gates failed." >&2
  exit 1
fi
echo "green.sh: all baseline gates passed."
