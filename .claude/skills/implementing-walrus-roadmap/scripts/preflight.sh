#!/usr/bin/env bash
# Repo-state and resume probe for the implementing-walrus-roadmap loop.
#
# Repo mode:    preflight.sh [--wait-main <seconds>]
#   Asserts the session can start: tree clean, switches a resumable task branch
#   back to main, main synced with origin,
#   gh authed, main's CI green, and which optional local gates are runnable
#   (docker daemon, cargo-deny, kubeconform, sqlx-cli).
#   Prints KEY=VALUE lines and a final PREFLIGHT=PASS|DRIFT|FAIL.
#
# Branch mode:  preflight.sh <code-branch> <task-id>
#   Probes where a (possibly interrupted) task stands and prints one ROUTE= line:
#     FRESH            no branch, no PR — start from scratch
#     CONTINUE_IMPL    branch has commits, no PR — resume implementation
#     PUSH_CI          open code PR has local commits — push before polling
#     POLL_CI          open code PR — go poll CI
#     MARK_DONE        code PR merged, task not yet marked done — bookkeeping PR
#     CONTINUE_MARK_DONE  bookkeeping branch exists without a PR — resume it
#     PUSH_MARK_DONE   open bookkeeping PR has local commits — push before polling
#     POLL_MARK_DONE   the mark-done PR is open — poll its (fast) CI, then merge
#     RECONCILE        merged bookkeeping PR left done signals inconsistent
#     DONE             merged and marked done — nothing to do
#     STOP_AMBIGUOUS   closed-unmerged PR (a human veto), diverged branches, or
#                      any state the loop must not guess around
#   Leaves the checkout on the branch the route needs (code branch, mark-done
#   branch, or main), so the caller can resume without extra git commands.
#
# Reconcile mode: preflight.sh --reconcile <drift-task-id>
#   Uses a deterministic per-drift branch and emits FRESH_RECONCILE,
#   CONTINUE_RECONCILE, PUSH_RECONCILE, POLL_RECONCILE, RECONCILE_DONE, or
#   STOP_AMBIGUOUS. This makes a docs-only drift repair resumable at every
#   branch/push/PR/merge boundary.
set -u

here=$(cd "$(dirname "$0")" && pwd)

mode=branch
main_wait_cap=0
if [ $# -eq 0 ]; then
  mode=repo
elif [ $# -eq 2 ] && [ "$1" = "--wait-main" ]; then
  mode=repo
  main_wait_cap=$2
  if ! printf '%s\n' "$main_wait_cap" | grep -Eq '^[0-9]+$'; then
    echo "usage: preflight.sh [--wait-main <seconds> | <code-branch> <task-id> | --reconcile <drift-task-id>]"
    exit 2
  fi
elif [ $# -eq 2 ] && [ "$1" = "--reconcile" ]; then
  mode=reconcile
elif [ $# -ne 2 ]; then
  echo "usage: preflight.sh [--wait-main <seconds> | <code-branch> <task-id> | --reconcile <drift-task-id>]"
  exit 2
fi

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

# The loop may not inspect or mutate external state until the complete roadmap
# corpus is coherent. This catches partial Rust activation, draft/untracked
# tasks, metadata errors, broken links, dependency drift, and status drift.
validation=$(python3 "$here/next_task.py" --validate-all --require-tracked 2>&1)
validation_status=$?
printf '%s\n' "$validation"
validation_drift=no
if [ "$validation_status" -eq 5 ] && printf '%s\n' "$validation" | grep -q '^VALIDATION=DRIFT$'; then
  validation_drift=yes
elif [ "$validation_status" -ne 0 ]; then
  if [ "$mode" = repo ]; then
    say "PREFLIGHT=FAIL"
  else
    say "ROUTE=STOP_AMBIGUOUS"
    say "REASON=roadmap validation failed"
  fi
  exit 1
fi
rust_rows=$(printf '%s\n' "$validation" | sed -n 's/^RUST_ROWS=//p')
if [ "$rust_rows" != "265" ]; then
  say "ROADMAP_ACTIVATION=inactive (RUST_ROWS=${rust_rows:-unknown}, expected 265)"
  if [ "$mode" = repo ]; then
    say "PREFLIGHT=FAIL"
  else
    say "ROUTE=STOP_AMBIGUOUS"
    say "REASON=the audited Rust roadmap has not been atomically activated"
  fi
  exit 1
fi
if [ "$validation_drift" = yes ] && [ "$mode" = branch ]; then
  say "ROUTE=STOP_AMBIGUOUS"
  say "REASON=roadmap drift must be reconciled from repo-mode preflight before task routing"
  exit 1
fi
consistent_unset_reconcile=no
if [ "$mode" = reconcile ] && [ "$validation_drift" != yes ]; then
  # A second explicitly recoverable state exists: both done signals are still
  # unset even though the code and mark-done PRs merged. Reconcile mode proves
  # both merges independently below before it is allowed to repair that state.
  consistent_unset_reconcile=yes
fi

switch_to_main() {
  git switch --quiet main 2>/dev/null \
    || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot switch to main"; exit 1; }
  git merge --ff-only --quiet origin/main 2>/dev/null \
    || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot fast-forward main from origin/main"; exit 1; }
}

# Reconcile local/remote before resuming on any loop-owned branch. The globals
# let the caller distinguish "checked out" from "must push before polling" and
# choose `git push -u` for a local-only interrupted branch.
RESUME_PUSH_NEEDED=no
RESUME_SET_UPSTREAM=no
resume_on_branch() {
  local b=$1
  local branch_has_local=no branch_has_remote=no branch_ahead=0 branch_behind=0 counts
  RESUME_PUSH_NEEDED=no
  RESUME_SET_UPSTREAM=no
  git show-ref --verify --quiet "refs/heads/$b" && branch_has_local=yes
  git show-ref --verify --quiet "refs/remotes/origin/$b" && branch_has_remote=yes
  say "RESUME_LOCAL_BRANCH=$branch_has_local"
  say "RESUME_REMOTE_BRANCH=$branch_has_remote"
  if [ "$branch_has_local" = no ] && [ "$branch_has_remote" = no ]; then
    say "ROUTE=STOP_AMBIGUOUS"; say "REASON=route requires missing branch $b"
    exit 1
  fi
  if [ "$branch_has_local" = yes ] && [ "$branch_has_remote" = yes ]; then
    if ! counts=$(git rev-list --left-right --count "origin/$b...$b" 2>/dev/null); then
      say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot compare local and remote $b"
      exit 1
    fi
    read -r branch_behind branch_ahead <<EOF
$counts
EOF
    say "AHEAD=$branch_ahead"; say "BEHIND=$branch_behind"
  fi
  if [ "$branch_ahead" -gt 0 ] && [ "$branch_behind" -gt 0 ]; then
    say "ROUTE=STOP_AMBIGUOUS"
    say "REASON=local and remote $b diverged (ahead=$branch_ahead behind=$branch_behind)"
    exit 1
  fi
  if [ "$branch_has_local" = yes ]; then
    git switch --quiet "$b" \
      || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot switch to $b"; exit 1; }
    if [ "$branch_behind" -gt 0 ]; then
      git merge --ff-only --quiet "origin/$b" \
        || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=fast-forward from origin/$b failed"; exit 1; }
      say "RECONCILED=pulled"
    fi
    if [ "$branch_ahead" -gt 0 ] || [ "$branch_has_remote" = no ]; then
      RESUME_PUSH_NEEDED=yes
      say "PUSH_NEEDED=yes"
    fi
    if [ "$branch_has_remote" = no ]; then
      RESUME_SET_UPSTREAM=yes
      say "PUSH_SET_UPSTREAM=yes"
    fi
  elif [ "$branch_has_remote" = yes ]; then
    git switch --quiet -c "$b" --track "origin/$b" \
      || { say "ROUTE=STOP_AMBIGUOUS"; say "REASON=cannot check out origin/$b"; exit 1; }
    say "RECONCILED=checked-out-remote"
  fi
}

if [ "$mode" = repo ]; then
  # ---------- repo mode ----------
  net git fetch origin --quiet || { say "FETCH=FAIL"; say "PREFLIGHT=FAIL"; exit 1; }

  # --untracked-files=no: untracked noise (.DS_Store, scratch notes, an
  # in-progress roadmap directory) must never wedge an unattended loop; only
  # tracked modifications block.
  if [ -z "$(git status --porcelain --untracked-files=no)" ]; then
    say "CLEAN=yes"
    tree_clean=yes
  else
    bad "CLEAN=no"
    tree_clean=no
  fi

  start_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)
  say "START_BRANCH=${start_branch:-unknown}"
  if [ "$tree_clean" = yes ] && [ "$start_branch" != "main" ]; then
    if git switch --quiet main 2>/dev/null; then
      say "BRANCH=main (switched from ${start_branch:-detached})"
    else
      bad "BRANCH=${start_branch:-unknown} (cannot switch to main)"
    fi
  elif [ "$start_branch" = "main" ]; then
    say "BRANCH=main"
  else
    bad "BRANCH=${start_branch:-unknown} (tracked changes prevent switching to main)"
  fi
  branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)

  if [ "$(git rev-parse main 2>/dev/null)" = "$(git rev-parse origin/main 2>/dev/null)" ]; then
    say "SYNCED=yes"
  elif git merge-base --is-ancestor main origin/main && [ "$branch" = "main" ] \
       && [ "$tree_clean" = yes ] \
       && git merge --ff-only --quiet origin/main; then
    say "SYNCED=fast-forwarded"     # behind but clean: solve it, don't defer it
  else
    bad "SYNCED=no (diverged or not fast-forwardable)"
  fi

  # Validation ran before fetch/switch so malformed local inputs could not
  # trigger network or branch mutation. Re-run it on the now-synced main: an
  # interrupted reconcile may have been valid on its branch but drifted on
  # main, while a just-merged reconcile may have repaired a stale local main.
  post_validation=$(python3 "$here/next_task.py" --validate-all --require-tracked 2>&1)
  post_validation_status=$?
  post_validation_kind=$(printf '%s\n' "$post_validation" | sed -n 's/^VALIDATION=//p' | head -1)
  case "$post_validation_status:$post_validation_kind" in
    0:PASS)
      validation_drift=no
      say "POST_SYNC_VALIDATION=PASS" ;;
    5:DRIFT)
      validation_drift=yes
      say "POST_SYNC_VALIDATION=DRIFT" ;;
    *)
      printf '%s\n' "$post_validation"
      say "POST_SYNC_VALIDATION=FAIL"
      say "PREFLIGHT=FAIL"
      exit 1 ;;
  esac
  post_rust_rows=$(printf '%s\n' "$post_validation" | sed -n 's/^RUST_ROWS=//p' | head -1)
  if [ "$post_rust_rows" != "265" ]; then
    say "ROADMAP_ACTIVATION=inactive (RUST_ROWS=${post_rust_rows:-unknown}, expected 265)"
    say "PREFLIGHT=FAIL"
    exit 1
  fi

  if net gh auth status >/dev/null 2>&1; then say "GH_AUTH=yes"; else bad "GH_AUTH=no"; fi

  # Red main = stop before cutting any branch. At an iteration boundary the
  # exact-SHA push run may not be registered/terminal yet, so --wait-main polls
  # it to a bounded conclusion instead of mistaking normal registration for a
  # safe result or an immediate terminal failure.
  main_sha=$(git rev-parse origin/main 2>/dev/null || true)
  main_ci_elapsed=0
  while :; do
    if ! run=$(net gh run list --workflow ci.yml --branch main --event push --limit 20 \
        --json headSha,status,conclusion,event \
        --jq "map(select(.headSha == \"$main_sha\" and .event == \"push\"))[0] | if . == null then \"\" else .status + \":\" + (.conclusion // \"\") end" \
        2>/dev/null); then
      bad "MAIN_CI=query-failed"
      break
    fi
    case "$run" in
      completed:success)
        say "MAIN_CI=green"
        [ "$main_ci_elapsed" -gt 0 ] && say "MAIN_CI_WAITED=${main_ci_elapsed}s"
        break ;;
      completed:*)
        bad "MAIN_CI=red ($run)"
        break ;;
      queued:*|in_progress:*|waiting:*|pending:*|requested:*|"")
        if [ "$main_ci_elapsed" -ge "$main_wait_cap" ]; then
          if [ -z "$run" ]; then
            bad "MAIN_CI=none (exact SHA $main_sha; wait cap ${main_wait_cap}s reached)"
          else
            bad "MAIN_CI=running ($run; exact SHA $main_sha; wait cap ${main_wait_cap}s reached)"
          fi
          break
        fi
        say "MAIN_CI=waiting (${run:-not-registered}; exact SHA $main_sha)"
        sleep 30
        main_ci_elapsed=$((main_ci_elapsed + 30)) ;;
      *)
        bad "MAIN_CI=anomaly ($run; exact SHA $main_sha)"
        break ;;
    esac
  done

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

  if [ $fail -eq 0 ]; then
    if [ "$validation_drift" = yes ]; then say "PREFLIGHT=DRIFT"; else say "PREFLIGHT=PASS"; fi
  else
    say "PREFLIGHT=FAIL"
  fi
  exit $fail
fi

if [ "$mode" = reconcile ]; then
  # ---------- reconcile mode ----------
  reconcile_id=$2
  if ! printf '%s\n' "$reconcile_id" | grep -Eq '^[0-9]+\.[0-9]+[a-z]?$'; then
    say "ROUTE=STOP_AMBIGUOUS"
    say "REASON=invalid drift task id $reconcile_id"
    exit 1
  fi
  reconcile_branch="chore-reconcile-roadmap-$reconcile_id"
  say "RECONCILE_BRANCH=$reconcile_branch"

  net git fetch origin --quiet \
    || { say "FETCH=FAIL"; say "ROUTE=STOP_AMBIGUOUS"; exit 1; }

  if [ "$consistent_unset_reconcile" = yes ]; then
    if ! unset_state=$(python3 "$here/next_task.py" --task "$reconcile_id" 2>/dev/null); then
      say "ROUTE=STOP_AMBIGUOUS"
      say "REASON=cannot parse consistent-unset task $reconcile_id"
      exit 1
    fi
    unset_box=$(printf '%s\n' "$unset_state" | sed -n 's/^BOX=//p' | head -1)
    unset_marker=$(printf '%s\n' "$unset_state" | sed -n 's/^MARKER=//p' | head -1)
    unset_code_branch=$(printf '%s\n' "$unset_state" \
      | sed -n 's/^[[:space:]]*"branch": "\([^"]*\)",\{0,1\}$/\1/p' | head -1)
    if [ "$unset_box" != unchecked ] || [ "$unset_marker" != planned ] \
        || [ -z "$unset_code_branch" ]; then
      say "ROUTE=STOP_AMBIGUOUS"
      say "REASON=reconcile without DRIFT requires BOX=unchecked MARKER=planned and a parsed code branch"
      exit 1
    fi
    unset_md_branch="pr-${reconcile_id}-mark-done"
    if ! unset_code_pr=$(net gh pr list --head "$unset_code_branch" --state all \
        --json number,state --jq 'map("\(.number):\(.state)") | join(",")' 2>/dev/null); then
      say "ROUTE=STOP_AMBIGUOUS"
      say "REASON=GitHub query failed while proving merged code PR for $reconcile_id"
      exit 1
    fi
    if ! unset_md_pr=$(net gh pr list --head "$unset_md_branch" --state all \
        --json number,state --jq 'map("\(.number):\(.state)") | join(",")' 2>/dev/null); then
      say "ROUTE=STOP_AMBIGUOUS"
      say "REASON=GitHub query failed while proving merged mark-done PR for $reconcile_id"
      exit 1
    fi
    case "$unset_code_pr" in
      *,*|""|*:OPEN|*:CLOSED)
        say "ROUTE=STOP_AMBIGUOUS"
        say "REASON=consistent-unset repair requires exactly one merged code PR (found ${unset_code_pr:-none})"
        exit 1 ;;
      *:MERGED) ;;
      *)
        say "ROUTE=STOP_AMBIGUOUS"
        say "REASON=unrecognized code PR proof ${unset_code_pr:-none}"
        exit 1 ;;
    esac
    case "$unset_md_pr" in
      *,*|""|*:OPEN|*:CLOSED)
        say "ROUTE=STOP_AMBIGUOUS"
        say "REASON=consistent-unset repair requires exactly one merged mark-done PR (found ${unset_md_pr:-none})"
        exit 1 ;;
      *:MERGED) ;;
      *)
        say "ROUTE=STOP_AMBIGUOUS"
        say "REASON=unrecognized mark-done PR proof ${unset_md_pr:-none}"
        exit 1 ;;
    esac
    say "CONSISTENT_UNSET_PROOF=yes"
    say "MERGED_CODE_PR=${unset_code_pr%:MERGED}"
    say "MERGED_MARK_DONE_PR=${unset_md_pr%:MERGED}"
  fi

  if ! reconcile_pr=$(net gh pr list --head "$reconcile_branch" --state all \
      --json number,state --jq 'map("\(.number):\(.state)") | join(",")' \
      2>/dev/null); then
    say "ROUTE=STOP_AMBIGUOUS"
    say "REASON=GitHub query failed for reconcile branch $reconcile_branch"
    exit 1
  fi
  say "RECONCILE_PR=${reconcile_pr:-none}"

  case "$reconcile_pr" in
    *,*)
      say "ROUTE=STOP_AMBIGUOUS"
      say "REASON=multiple PRs for reconcile branch $reconcile_branch"
      exit 1 ;;
    *:OPEN)
      resume_on_branch "$reconcile_branch"
      if [ "$RESUME_PUSH_NEEDED" = yes ]; then
        say "ROUTE=PUSH_RECONCILE"
      else
        say "ROUTE=POLL_RECONCILE"
      fi
      exit 0 ;;
    *:CLOSED)
      say "ROUTE=STOP_AMBIGUOUS"
      say "REASON=reconcile PR closed without merge (human veto)"
      exit 1 ;;
    *:MERGED)
      switch_to_main
      post_merge=$(python3 "$here/next_task.py" 2>&1)
      post_status=$?
      next_drift=$(printf '%s\n' "$post_merge" | sed -n 's/^TASK=//p' | head -1)
      if [ "$post_status" -eq 5 ]; then
        say "ROUTE=STOP_AMBIGUOUS"
        say "REASON=merged reconcile PR left roadmap drift at task ${next_drift:-unknown}; reconciliation must cover every reported drift"
        exit 1
      fi
      if [ "$post_status" -eq 0 ] || [ "$post_status" -eq 2 ]; then
        if ! reconciled_state=$(python3 "$here/next_task.py" --task "$reconcile_id" 2>/dev/null); then
          say "ROUTE=STOP_AMBIGUOUS"
          say "REASON=cannot verify task $reconcile_id after merged reconcile PR"
          exit 1
        fi
        reconciled_box=$(printf '%s\n' "$reconciled_state" | sed -n 's/^BOX=//p' | head -1)
        reconciled_marker=$(printf '%s\n' "$reconciled_state" | sed -n 's/^MARKER=//p' | head -1)
        if [ "$reconciled_box" != checked ] || [ "$reconciled_marker" != done ]; then
          say "ROUTE=STOP_AMBIGUOUS"
          say "REASON=merged reconcile PR left task $reconcile_id BOX=${reconciled_box:-unknown} MARKER=${reconciled_marker:-unknown}"
          exit 1
        fi
      fi
      if [ "$post_status" -eq 0 ] || [ "$post_status" -eq 2 ]; then
        say "ROUTE=RECONCILE_DONE"
        [ "$post_status" -eq 2 ] && say "ROADMAP=ALL_DONE"
        exit 0
      fi
      printf '%s\n' "$post_merge"
      say "ROUTE=STOP_AMBIGUOUS"
      say "REASON=roadmap is invalid after merged reconcile PR"
      exit 1 ;;
    *)
      if git show-ref --verify --quiet "refs/heads/$reconcile_branch" \
          || git show-ref --verify --quiet "refs/remotes/origin/$reconcile_branch"; then
        resume_on_branch "$reconcile_branch"
        say "ROUTE=CONTINUE_RECONCILE"
        exit 0
      fi
      switch_to_main
      say "ROUTE=FRESH_RECONCILE"
      exit 0 ;;
  esac
fi

# ---------- branch mode ----------
code_branch=$1
task_id=${2:?usage: preflight.sh <code-branch> <task-id>}
md_branch="pr-${task_id}-mark-done"

net git fetch origin --quiet || { say "FETCH=FAIL"; say "ROUTE=STOP_AMBIGUOUS"; exit 1; }

if ! task_state=$(python3 "$here/next_task.py" --task "$task_id" 2>/dev/null); then
  say "ROUTE=STOP_AMBIGUOUS"
  say "REASON=cannot parse task state for PR $task_id"
  exit 1
fi
box=$(printf '%s\n' "$task_state" | sed -n 's/^BOX=//p')
marker=$(printf '%s\n' "$task_state" | sed -n 's/^MARKER=//p')
say "BOX=${box:-unknown}"
say "MARKER=${marker:-unknown}"
if [ -z "$box" ] || [ -z "$marker" ]; then
  say "ROUTE=STOP_AMBIGUOUS"
  say "REASON=task state omitted BOX or MARKER"
  exit 1
fi

if ! pr_info=$(net gh pr list --head "$code_branch" --state all --json number,state \
    --jq 'map("\(.number):\(.state)") | join(",")' 2>/dev/null); then
  say "ROUTE=STOP_AMBIGUOUS"
  say "REASON=GitHub query failed for code branch $code_branch"
  exit 1
fi
say "PR=${pr_info:-none}"

if ! md_info=$(net gh pr list --head "$md_branch" --state all --json number,state \
    --jq 'map("\(.number):\(.state)") | join(",")' 2>/dev/null); then
  say "ROUTE=STOP_AMBIGUOUS"
  say "REASON=GitHub query failed for mark-done branch $md_branch"
  exit 1
fi
say "MARK_DONE_PR=${md_info:-none}"

has_local=no;  git show-ref --verify --quiet "refs/heads/$code_branch" && has_local=yes
has_remote=no; git show-ref --verify --quiet "refs/remotes/origin/$code_branch" && has_remote=yes
say "LOCAL_BRANCH=$has_local"
say "REMOTE_BRANCH=$has_remote"

case "$pr_info" in
  *,*)
    say "ROUTE=STOP_AMBIGUOUS"; say "REASON=multiple PRs for branch $code_branch"; exit 1 ;;
  *:MERGED)
    if [ "$box" = "checked" ] && [ "$marker" = "done" ]; then
      switch_to_main; say "ROUTE=DONE"; exit 0
    fi
    case "$md_info" in
      *,*)      say "ROUTE=STOP_AMBIGUOUS"; say "REASON=multiple mark-done PRs for branch $md_branch"; exit 1 ;;
      *:OPEN)
        resume_on_branch "$md_branch"
        if [ "$RESUME_PUSH_NEEDED" = yes ]; then
          say "ROUTE=PUSH_MARK_DONE"
        else
          say "ROUTE=POLL_MARK_DONE"
        fi
        exit 0 ;;
      *:CLOSED) say "ROUTE=STOP_AMBIGUOUS"; say "REASON=mark-done PR closed without merge (human veto)"; exit 1 ;;
      *:MERGED)
        # Branch mode only sees validator-consistent state. A merged mark-done
        # PR that nevertheless left both signals unset needs docs-only repair;
        # half-landed state is caught earlier as repo-mode DRIFT.
        switch_to_main
        say "ROUTE=RECONCILE"
        say "RECONCILE_TASK=$task_id"
        say "REASON=mark-done PR merged but BOX=$box MARKER=$marker"
        exit 0 ;;
      *)
        if git show-ref --verify --quiet "refs/heads/$md_branch" \
            || git show-ref --verify --quiet "refs/remotes/origin/$md_branch"; then
          resume_on_branch "$md_branch"
          say "ROUTE=CONTINUE_MARK_DONE"
          exit 0
        fi
        switch_to_main; say "ROUTE=MARK_DONE"; exit 0 ;;
    esac ;;
  *:CLOSED)
    say "ROUTE=STOP_AMBIGUOUS"; say "REASON=code PR closed without merge (human veto)"; exit 1 ;;
  *:OPEN)
    resume_on_branch "$code_branch"
    if [ "$RESUME_PUSH_NEEDED" = yes ]; then
      say "ROUTE=PUSH_CI"
    else
      say "ROUTE=POLL_CI"
    fi
    exit 0 ;;
  *)
    if [ "$has_local" = no ] && [ "$has_remote" = no ]; then
      switch_to_main; say "ROUTE=FRESH"; exit 0
    fi
    resume_on_branch "$code_branch"; say "ROUTE=CONTINUE_IMPL"; exit 0 ;;
esac
