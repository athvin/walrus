#!/usr/bin/env bash
# next-task.sh — print the absolute path of the next task in the walrus implementation roadmap
# that is not yet done, or "DONE" if the roadmap is complete.
#
# A task counts as done when EITHER signal says so: its README roadmap box is ✅, or its task file
# carries the completion marker "**Status:** ✅ Done" (written by the skill's mark-done commit, which
# sets the marker AND the README box together). This makes the per-file marker a first-class
# "done via code" signal.
#
# Drift where the marker is present but the README box is still ☐ therefore means a mark-done that
# only PARTLY landed (e.g. a squash conflict dropped the README row) — NOT a hand-merged task. It is
# reported on stderr so the skill can reconcile the README box (via a PR, never a commit on main).
#
# The case this file-only check CANNOT see: a task finished OUTSIDE this skill carries neither signal,
# so it is returned as the next task. The skill's `gh pr list --state merged` cross-check is what
# catches that. Run this only on an up-to-date `main`, so it reflects merged state, not a feature branch.
#
# Deterministic and read-only.
set -uo pipefail

DONE_MARKER='**Status:** ✅ Done'   # keep in sync with reference/pr-and-merge.md + SKILL.md

repo="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$repo" ]]; then
  # Fall back to path math: scripts/ -> skill/ -> skills/ -> .claude/ -> repo root.
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  repo="$(cd "$here/../../../.." && pwd)"
fi

readme="$repo/docs/implementation/README.md"
if [[ ! -f "$readme" ]]; then
  echo "next-task.sh: roadmap not found at $readme" >&2
  exit 2
fi

# Walk every roadmap/status row in order. A row looks like:
#   | ☐ | [0.1](./phase-0-foundations/pr-0.1-….md) | Delivers | Design |
# grep keeps only rows whose first cell is ☐ or ✅ (this also matches the per-table header row
# "| ✅ | PR | … |", which we skip below); separator rows (|---|…) are excluded.
while IFS= read -r row; do
  # README says done → skip (also skips the "| ✅ | PR | … |" header rows harmlessly).
  if printf '%s\n' "$row" | grep -q '^[|][[:space:]]*✅'; then
    continue
  fi

  # It's a ☐ row: extract the task-file link from the PR cell.
  rel="$(printf '%s\n' "$row" | sed -E 's/.*\]\(([^)]+)\).*/\1/')"
  rel="${rel#./}"
  if [[ "$rel" == "$row" || -z "$rel" ]]; then
    echo "next-task.sh: could not parse a task link from roadmap row:" >&2
    echo "  $row" >&2
    exit 3
  fi

  file="$repo/docs/implementation/$rel"
  if [[ -f "$file" ]] && grep -qF "$DONE_MARKER" "$file"; then
    # Drift: the task file is marked Done but its README box is still ☐ (a partly-landed mark-done).
    echo "next-task.sh: reconcile — '$rel' is marked Done in-file but ☐ in README; tick its README box via a reconcile PR." >&2
    continue
  fi

  printf '%s\n' "$file"
  exit 0
done < <(grep -E '^[|][[:space:]]*(☐|✅)[[:space:]]*[|]' "$readme")

echo "DONE"
