#!/usr/bin/env bash
# Repo-state and resume probe for the implementing-walrus-roadmap loop.
#
# Repo mode:    preflight.sh
#   Asserts the session can start: on main, tree clean, main synced with origin,
#   gh authed, main's CI green, and which optional local gates are runnable
#   (docker daemon, cargo-deny, kubeconform, sqlx-cli).
#   Prints KEY=VALUE lines and a final PREFLIGHT=PASS|FAIL.
#
# Branch mode:  preflight.sh <code-branch> <task-id>
#   Probes where a (possibly interrupted) task stands and prints one ROUTE= line:
#     FRESH            no branch, no PR — start from scratch
#     CONTINUE_IMPL    branch has commits, no PR — resume implementation
#     POLL_CI          open code PR — go poll CI
#     MARK_DONE        code PR merged, task not yet marked done — bookkeeping PR
#     POLL_MARK_DONE   the mark-done PR is open — poll its (fast) CI, then merge
#     DONE             merged and marked done — nothing to do
#     STOP_AMBIGUOUS   closed-unmerged PR (a human veto), diverged branches, or
#                      any state the loop must not guess around
#   Leaves the checkout on the branch the route needs (code branch, mark-done
#   branch, or main), so the caller can resume without extra git commands.
set -u

here=$(cd "$(dirname "$0")" && pwd)

# Retry wrapper for network calls: gh/git hiccups are common on long unattended
# runs; 3 tries with 2s/8s backoff outlasts a transient failure without hiding a
# real outage.
net() {
  local i
  for i in 1 2 3; do
    if "$@"; then return 0; fi
    if [ "$i" -eq 1 ]; then sleep 2; elif [ "$i" -eq 2 ]; then sleep 8; fi
  done
  return 1
}

fail=0
say() { echo "$1"; }
bad() { say "$1"; fail=1; }

if [ $# -eq 0 ]; then
  # ---------- repo mode ----------
  net git fetch origin --quiet || { say "FETCH=FAIL"; say "PREFLIGHT=FAIL"; exit 1; }

  branch=$(git rev-parse --abbrev-ref HEAD)
  if [ "$branch" = "main" ]; then say "BRANCH=main"; else bad "BRANCH=$branch (expected main)"; fi

  # --untracked-files=no: untracked noise (.DS_Store, scratch notes, an
  # in-progress roadmap directory) must never wedge an unattended loop; only
  # tracked modifications block.
  if [ -z "$(git status --porcelain --untracked-files=no)" ]; then
    say "CLEAN=yes"
  else
    bad "CLEAN=no"
  fi

  if [ "$(git rev-parse main 2>/dev/null)" = "$(git rev-parse origin/main 2>/dev/null)" ]; then
    say "SYNCED=yes"
  elif git merge-base --is-ancestor main origin/main && [ "$branch" = "main" ] \
       && git merge --ff-only --quiet origin/main; then
    say "SYNCED=fast-forwarded"     # behind but clean: solve it, don't defer it
  else
    bad "SYNCED=no (diverged or not fast-forwardable)"
  fi

  if net gh auth status >/dev/null 2>&1; then say "GH_AUTH=yes"; else bad "GH_AUTH=no"; fi

  # Red main = stop before cutting any branch. CI fires on both `push` and
  # `pull_request`, so main's newest run is the push run for the squash commit.
  run=$(gh run list --branch main --limit 1 --json status,conclusion \
        --jq '.[0] | .status + ":" + (.conclusion // "")' 2>/dev/null || true)
  case "$run" in
    completed:success) say "MAIN_CI=green" ;;
    "")                say "MAIN_CI=none" ;;
    completed:*)       bad "MAIN_CI=red ($run)" ;;
    *)                 say "MAIN_CI=running" ;;
  esac

  for tool in git gh python3 cargo; do
    command -v "$tool" >/dev/null || bad "TOOL_$tool=missing"
  done

  # Optional local capabilities. These are NOT failures — run_gate.sh SKIPs the
  # gates they enable and CI still runs them. They are reported so the loop knows
  # up front which DoD lines it cannot prove locally.
  if command -v docker >/dev/null && timeout 20 docker info >/dev/null 2>&1; then
    say "DOCKER=up"
  elif command -v docker >/dev/null; then
    say "DOCKER=daemon-down"       # compose / integration / e2e gates: CI-only
  else
    say "DOCKER=missing"
  fi
  command -v cargo-deny   >/dev/null && say "CARGO_DENY=yes"  || say "CARGO_DENY=no"
  command -v kubeconform  >/dev/null && say "KUBECONFORM=yes" || say "KUBECONFORM=no"
  command -v sqlx         >/dev/null && say "SQLX_CLI=yes"    || say "SQLX_CLI=no"

  if [ $fail -eq 0 ]; then say "PREFLIGHT=PASS"; else say "PREFLIGHT=FAIL"; fi
  exit $fail
fi

# ---------- branch mode ----------
code_branch=$1
task_id=${2:?usage: preflight.sh <code-branch> <task-id>}
md_branch="pr-${task_id}-mark-done"

net git fetch origin --quiet || { say "FETCH=FAIL"; say "ROUTE=STOP_AMBIGUOUS"; exit 1; }

task_state=$(python3 "$here/next_task.py" --task "$task_id" 2>/dev/null)
box=$(printf '%s\n' "$task_state" | sed -n 's/^BOX=//p')
marker=$(printf '%s\n' "$task_state" | sed -n 's/^MARKER=//p')
say "BOX=${box:-unknown}"
say "MARKER=${marker:-unknown}"

pr_info=$(net gh pr list --head "$code_branch" --state all --json number,state \
  --jq 'map("\(.number):\(.state)") | join(",")' 2>/dev/null || true)
say "PR=${pr_info:-none}"

md_info=$(net gh pr list --head "$md_branch" --state all --json number,state \
  --jq 'map("\(.number):\(.state)") | join(",")' 2>/dev/null || true)
say "MARK_DONE_PR=${md_info:-none}"

has_local=no;  git show-ref --verify --quiet "refs/heads/$code_branch" && has_local=yes
has_remote=no; git show-ref --verify --quiet "refs/remotes/origin/$code_branch" && has_remote=yes
say "LOCAL_BRANCH=$has_local"
say "REMOTE_BRANCH=$has_remote"

ahead=0; behind=0
if [ "$has_local" = yes ] && [ "$has_remote" = yes ]; then
  read -r behind ahead <<EOF
$(git rev-list --left-right --count "origin/$code_branch...$code_branch")
EOF
  say "AHEAD=$ahead"; say "BEHIND=$behind"
fi

switch_to_main() {
  git switch --quiet main 2>/dev/null && git merge --ff-only --quiet origin/main 2>/dev/null
}

# Reconcile local/remote before resuming ON the code branch.
resume_on_branch() {
  local b=$1
  if [ "$ahead" -gt 0 ] && [ "$behind" -gt 0 ]; then
    say "ROUTE=STOP_AMBIGUOUS"; say "REASON=local and remote diverged (ahead=$ahead behind=$behind)"
    exit 1
  fi
  if git show-ref --verify --quiet "refs/heads/$b"; then
    git switch --quiet "$b" \
      || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot switch to $b"; exit 1; }
    if [ "$behind" -gt 0 ]; then
      git merge --ff-only --quiet "origin/$b" \
        || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=fast-forward from origin/$b failed"; exit 1; }
      say "RECONCILED=pulled"
    fi
    [ "$ahead" -gt 0 ] && say "PUSH_NEEDED=yes"
  elif git show-ref --verify --quiet "refs/remotes/origin/$b"; then
    git switch --quiet -c "$b" --track "origin/$b" \
      || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot check out origin/$b"; exit 1; }
    say "RECONCILED=checked-out-remote"
  fi
}

case "$pr_info" in
  *,*)
    say "ROUTE=STOP_AMBIGUOUS"; say "REASON=multiple PRs for branch $code_branch"; exit 1 ;;
  *:MERGED)
    if [ "$box" = "checked" ] && [ "$marker" = "done" ]; then
      switch_to_main; say "ROUTE=DONE"; exit 0
    fi
    case "$md_info" in
      *:OPEN)   resume_on_branch "$md_branch"; say "ROUTE=POLL_MARK_DONE"; exit 0 ;;
      *:CLOSED) say "ROUTE=STOP_AMBIGUOUS"; say "REASON=mark-done PR closed without merge (human veto)"; exit 1 ;;
      *:MERGED)
        # A merged mark-done PR that did not leave both signals set: reconcile it
        # (docs-only, on a chore-reconcile-roadmap branch), never re-implement.
        switch_to_main
        say "ROUTE=MARK_DONE"
        say "REASON=mark-done PR merged but BOX=$box MARKER=$marker — reconcile on chore-reconcile-roadmap"
        exit 0 ;;
      *) switch_to_main; say "ROUTE=MARK_DONE"; exit 0 ;;
    esac ;;
  *:CLOSED)
    say "ROUTE=STOP_AMBIGUOUS"; say "REASON=code PR closed without merge (human veto)"; exit 1 ;;
  *:OPEN)
    resume_on_branch "$code_branch"; say "ROUTE=POLL_CI"; exit 0 ;;
  *)
    if [ "$has_local" = no ] && [ "$has_remote" = no ]; then
      switch_to_main; say "ROUTE=FRESH"; exit 0
    fi
    resume_on_branch "$code_branch"; say "ROUTE=CONTINUE_IMPL"; exit 0 ;;
esac
