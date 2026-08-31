#!/usr/bin/env bash
# Normalized CI verdict for a walrus PR, safe against the gh footguns:
#   - `gh pr checks` exits 1 for BOTH "checks failed" and "no checks configured",
#     and buckets `cancelled` as something other than a failure — so this script
#     never uses it, and neither should the loop.
#   - statusCheckRollup is empty for a few minutes after a push while GitHub
#     registers the runs — callers pass --grace to treat that window as PENDING
#     instead of NO_CHECKS.
#   - walrus PR CI triggers once through `pull_request` (`push` is main-only).
#     PASS requires one copy of every job declared in ci.yml; RUN_ID below is
#     resolved by head SHA and lists every failing terminal run.
#
# Usage: ci_status.sh <pr-number> [--wait <seconds>] [--grace <seconds>]
#
# Prints KEY=VALUE lines then a final VERDICT= line:
#   PASS       every check concluded success/neutral/skipped
#   FAIL       any check concluded failure/cancelled/timed_out/… (FAILING= names
#              them, FLAKE_CANDIDATE= says whether they are all known flakes,
#              RUN_ID= points the fixer at the failing run(s))
#   PENDING    checks still queued/running (or empty rollup within --grace)
#   NO_CHECKS  no checks on the head commit at all
#   ANOMALY    PR not open, or an unrecognized status/conclusion value
#
# Exit codes: 0 PASS · 1 FAIL · 2 PENDING · 3 NO_CHECKS · 4 ANOMALY
set -u

pr=${1:?usage: ci_status.sh <pr-number> [--wait <seconds>] [--grace <seconds>]}
shift
wait_cap=0
grace=0
while [ $# -gt 0 ]; do
  case "$1" in
    --wait)  wait_cap=$2; shift 2 ;;
    --grace) grace=$2; shift 2 ;;
    *) echo "VERDICT=ANOMALY"; echo "ERROR=unknown flag $1"; exit 4 ;;
  esac
done

here=$(cd "$(dirname "$0")" && pwd)

# 30s poll interval: GitHub updates check runs at roughly minute granularity, so
# finer polling only burns API budget (~120 calls/hr at 30s is well inside it).
POLL_INTERVAL=30

run_once() {
  local json out rc sha ref runs terminal_failures active_runs
  json=$(gh pr view "$pr" --json statusCheckRollup,headRefOid,headRefName,state,mergeable 2>/dev/null) || {
    echo "VERDICT=ANOMALY"; echo "ERROR=gh pr view failed for PR #$pr"; return 4
  }
  out=$(printf '%s' "$json" | python3 "$here/classify_checks.py"); rc=$?
  if [ $rc -eq 1 ]; then
    # Hand the fixer the failing run id(s) for THIS head sha so it can fetch the
    # logs itself; logs never transit the orchestrator's context.
    sha=$(echo "$out" | sed -n 's/^HEAD_SHA=//p')
    ref=$(echo "$out" | sed -n 's/^HEAD_REF=//p')
    if ! runs=$(gh run list --workflow ci.yml --branch "$ref" --limit 30 \
        --json databaseId,headSha,status,conclusion,event \
        --jq ".[] | select(.headSha == \"$sha\") | \"RUN_ID=\(.databaseId) EVENT=\(.event) STATUS=\(.status) CONCLUSION=\(.conclusion // \"-\")\"" \
        2>/dev/null); then
      printf '%s\n' "$out" | sed '/^VERDICT=/d'
      echo "ERROR=gh run list failed for failing head $sha"
      echo "VERDICT=ANOMALY"
      return 4
    fi
    terminal_failures=$(printf '%s\n' "$runs" | grep -E \
      ' STATUS=completed CONCLUSION=(failure|cancelled|timed_out|action_required|startup_failure|stale)$' \
      || true)
    if [ -n "$terminal_failures" ]; then
      echo "$out"
      echo "$terminal_failures"
      return 1
    fi
    active_runs=$(printf '%s\n' "$runs" | grep -v ' STATUS=completed ' || true)
    if [ -n "$active_runs" ]; then
      # A job can fail before the rest of its workflow reaches a terminal
      # conclusion. `gh run rerun --failed` is unavailable until then, so keep
      # polling instead of turning this normal transition into ANOMALY.
      printf '%s\n' "$out" | sed '/^VERDICT=/d; /^FLAKE_CANDIDATE=/d'
      printf '%s\n' "$active_runs" | sed 's/^RUN_ID=/WAITING_RUN_ID=/'
      echo "NOTE=failed check belongs to a workflow run that is still active"
      echo "VERDICT=PENDING"
      return 2
    fi
    printf '%s\n' "$out" | sed '/^VERDICT=/d'
    echo "ERROR=no rerunnable workflow run found for failing head $sha"
    echo "VERDICT=ANOMALY"
    return 4
  fi
  echo "$out"
  return $rc
}

if [ "$wait_cap" -eq 0 ]; then
  run_once; rc=$?
  if [ $rc -eq 3 ] && [ "$grace" -gt 0 ]; then
    echo "NOTE=within-grace, treat as PENDING"; exit 2
  fi
  exit $rc
fi

elapsed=0
while :; do
  out=$(run_once); rc=$?
  if [ $rc -eq 2 ] || { [ $rc -eq 3 ] && [ "$elapsed" -lt "$grace" ]; }; then
    if [ "$elapsed" -ge "$wait_cap" ]; then
      echo "$out"
      echo "NOTE=wait cap ${wait_cap}s reached"
      exit 2
    fi
    sleep $POLL_INTERVAL
    elapsed=$((elapsed + POLL_INTERVAL))
    continue
  fi
  echo "$out"
  exit $rc
done
