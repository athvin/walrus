#!/usr/bin/env bash
# Local quality gate for one walrus task, with terse verdicts so neither the
# orchestrator nor a subagent has to ingest thousands of lines of cargo output.
#
# Usage: run_gate.sh <gate>[,<gate>…] [--pkgs a,b] [--keep-stack]
#   gates:    fmt clippy test sqlx conformance deny msrv compose integration
#             e2e manifests images        (alias: `baseline` = fmt,clippy,test)
#   --pkgs    packages for a fast `cargo test -p …` pre-check before the
#             workspace run (from next_task.py's `test_packages`)
#   --keep-stack  leave `docker compose` running afterwards (default: down -v)
#
# The gate list comes from next_task.py's `gates` field. Phase 9+ tasks declare
# it explicitly; only legacy phase 0-8 tasks use Definition-of-Done inference.
#
# Prints one CHECK:<name>=PASS|FAIL|SKIP:<reason> line per check, a 40-line tail
# of each failing check's output, and a final GATE=PASS|FAIL.
# Exit code: 0 gate passed · 1 gate failed · 2 usage/environment anomaly.
set -u

COMPOSE="docker compose -f deploy/docker/docker-compose.yml"
CONTROL_DB_URL="postgres://postgres:postgres@localhost:5433/walrus_control"

gates=${1:-}
[ -n "$gates" ] || { echo "GATE=FAIL"; echo "ANOMALY=usage: run_gate.sh <gate>[,<gate>…]"; exit 2; }
shift
pkgs=""
keep_stack=no
while [ $# -gt 0 ]; do
  case "$1" in
    --pkgs)
      if [ $# -lt 2 ] || [ -z "$2" ] || [[ "$2" == --* ]]; then
        echo "GATE=FAIL"
        echo "ANOMALY=--pkgs requires a non-empty comma-separated value; omit --pkgs when no packages are declared"
        exit 2
      fi
      pkgs=$2
      shift 2
      ;;
    --keep-stack) keep_stack=yes; shift ;;
    *) echo "GATE=FAIL"; echo "ANOMALY=unknown flag $1"; exit 2 ;;
  esac
done
[ "$gates" = "baseline" ] && gates="fmt,clippy,test"

# Validate the complete request before running anything. A misspelled gate used
# to produce SKIP and a false-green final verdict; it is now an anomaly.
supported_gates="fmt clippy test sqlx conformance deny msrv compose integration e2e manifests images"
if [[ "$gates" == ,* ]] || [[ "$gates" == *, ]] || [[ "$gates" == *,,* ]] \
   || [[ "$gates" =~ [^a-z0-9,-] ]]; then
  echo "GATE=FAIL"
  echo "ANOMALY=invalid comma-separated gate list $gates"
  exit 2
fi
seen_gates=" "
for gate in ${gates//,/ }; do
  if [ -z "$gate" ] || ! [[ " $supported_gates " == *" $gate "* ]]; then
    echo "CHECK:${gate:-<empty>}=FAIL"
    echo "GATE=FAIL"
    echo "ANOMALY=unknown gate name ${gate:-<empty>}"
    exit 2
  fi
  if [[ "$seen_gates" == *" $gate "* ]]; then
    echo "GATE=FAIL"
    echo "ANOMALY=duplicate gate name $gate"
    exit 2
  fi
  seen_gates+="$gate "
done

if [ -n "$pkgs" ]; then
  if [[ "$pkgs" == ,* ]] || [[ "$pkgs" == *, ]] || [[ "$pkgs" == *,,* ]] \
     || [[ "$pkgs" =~ [^a-z0-9,-] ]]; then
    echo "GATE=FAIL"
    echo "ANOMALY=invalid --pkgs value $pkgs"
    exit 2
  fi
fi

repo=$(git rev-parse --show-toplevel 2>/dev/null || true)
if [ -z "$repo" ]; then
  repo=$(cd "$(dirname "$0")/../../../.." && pwd)
fi
# A mis-resolved root must fail LOUDLY — silently reporting "nothing to check"
# would be a false green on a merge gate.
[ -f "$repo/docs/implementation/README.md" ] || {
  echo "GATE=FAIL"; echo "ANOMALY=$repo is not the walrus workspace (no docs/implementation/README.md)"; exit 2; }
cd "$repo" || { echo "GATE=FAIL"; echo "ANOMALY=cannot cd to $repo"; exit 2; }
[ -f Cargo.toml ] || {
  echo "GATE=FAIL"; echo "ANOMALY=no Cargo.toml at the walrus root (the workspace landed in PR 0.1)"; exit 2; }

fail=0
tmpdir=$(mktemp -d)
cleanup() {
  if [ "$stack_up" = yes ] && [ "$keep_stack" = no ]; then
    $COMPOSE down -v >/dev/null 2>&1 || true
  fi
  rm -rf "$tmpdir"
}
stack_up=no
trap cleanup EXIT

pass() { echo "CHECK:$1=PASS"; }
skip() { echo "CHECK:$1=SKIP:$2"; }
failed() {
  echo "CHECK:$1=FAIL"
  fail=1
  if [ -s "$tmpdir/$1.log" ]; then
    echo "--- last 40 lines of $1 ---"      # enough for cargo's error summary
    tail -40 "$tmpdir/$1.log"
    echo "--- end $1 ---"
  fi
}
run_check() { # run_check NAME cmd...
  local name=$1; shift
  if "$@" >"$tmpdir/$name.log" 2>&1; then pass "$name"; else failed "$name"; fi
}

docker_up() { command -v docker >/dev/null && timeout 20 docker info >/dev/null 2>&1; }

ensure_stack() { # boot compose once per run; returns 1 if it cannot
  [ "$stack_up" = yes ] && return 0
  if $COMPOSE up --wait >"$tmpdir/compose-up.log" 2>&1; then
    stack_up=yes
    return 0
  fi
  return 1
}

for gate in ${gates//,/ }; do
  case "$gate" in

    fmt)    run_check fmt cargo fmt --check ;;
    clippy) run_check clippy cargo clippy --all-targets --all-features -- -D warnings ;;

    test)
      if [ -n "$pkgs" ]; then
        # Fast signal first: the packages the task names, then the full workspace
        # (which is what CI runs and therefore what the gate must assert).
        pkg_args=()
        for p in ${pkgs//,/ }; do pkg_args+=(-p "$p"); done
        run_check test-pkgs cargo test "${pkg_args[@]}"
      fi
      run_check test cargo test --workspace ;;

    # The offline sqlx cache cannot be regenerated without a live control PG, so
    # the cheap guard runs ALWAYS: any change to a compile-time-checked query
    # (crates/*/sql/**) without a matching .sqlx change fails CI's
    # `cargo sqlx prepare --check` and cannot be fixed locally with the daemon
    # down. Catch it here rather than after a 20-minute CI round trip.
    sqlx)
      base=$(git merge-base origin/main HEAD 2>/dev/null || true)
      if [ -z "$base" ]; then
        skip sqlx-guard "no merge-base with origin/main"
      else
        changed_sql=$(git diff --name-only "$base"...HEAD -- 'crates/*/sql/**' | tr '\n' ' ')
        changed_cache=$(git diff --name-only "$base"...HEAD -- '.sqlx' | tr '\n' ' ')
        if [ -n "$changed_sql" ] && [ -z "$changed_cache" ]; then
          {
            echo "changed SQL without a .sqlx cache update:"
            echo "  $changed_sql"
            echo "CI runs 'cargo sqlx prepare --check --workspace'; regenerating the cache"
            echo "needs a live control PG (docker compose). If the daemon is down, keep the"
            echo "query text byte-identical or STOP and ask the operator."
          } >"$tmpdir/sqlx-guard.log"
          failed sqlx-guard
        else
          pass sqlx-guard
        fi
      fi
      if command -v sqlx >/dev/null && docker_up && ensure_stack; then
        export DATABASE_URL="$CONTROL_DB_URL"
        # `prepare --check` re-type-checks every query against a LIVE schema, and a freshly booted
        # control-pg is empty (compose mounts no init SQL for it). Migrate first — otherwise every
        # `sqlx::query_file!` fails with `relation "walrus.…" does not exist`, because the
        # `integration` gate's migrate step runs only later in the list. `migrate run` is
        # idempotent, so running it in both places is a no-op the second time.
        if sqlx migrate run --source migrations/control >"$tmpdir/sqlx-prepare.log" 2>&1; then
          run_check sqlx-prepare cargo sqlx prepare --check --workspace
        else
          failed sqlx-prepare
        fi
      else
        skip sqlx-prepare "needs sqlx-cli + a running control PG (docker daemon)"
      fi ;;

    conformance) run_check conformance cargo test -p pg-to-arrow --features conformance ;;

    deny)
      if command -v cargo-deny >/dev/null; then run_check deny cargo deny check
      else skip deny "cargo-deny not installed (cargo install --locked cargo-deny)"; fi ;;

    msrv)
      declared=$(sed -nE 's/^[[:space:]]*rust-version[[:space:]]*=[[:space:]]*"([0-9]+\.[0-9]+)".*/\1/p' Cargo.toml | head -1)
      pinned=$(sed -nE 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"([0-9]+\.[0-9]+).*/\1/p' rust-toolchain.toml | head -1)
      if [ -n "$declared" ] && [ "$declared" = "$pinned" ]; then
        pass msrv
      else
        echo "declared=${declared:-<none>} pinned=${pinned:-<none>}" >"$tmpdir/msrv.log"
        failed msrv
      fi ;;

    compose)
      if ! docker_up; then skip compose "docker daemon not running — CI covers this job"
      elif ensure_stack; then
        run_check compose-smoke bash -c '
          set -euo pipefail
          C="docker compose -f deploy/docker/docker-compose.yml"
          $C exec -T source-pg pg_isready -U postgres -d walrus
          $C exec -T control-pg pg_isready -U postgres -d walrus_control
          curl -sf http://localhost:9000/minio/health/live
          test "$($C exec -T source-pg psql -U postgres -d walrus -tAc "SHOW wal_level")" = logical
          $C exec -T createbucket mc ls local/walrus'
      else
        cp "$tmpdir/compose-up.log" "$tmpdir/compose.log" 2>/dev/null || true
        failed compose
      fi ;;

    integration)
      if ! docker_up; then skip integration "docker daemon not running — CI covers this job"
      elif ensure_stack; then
        export DATABASE_URL="$CONTROL_DB_URL"
        if command -v sqlx >/dev/null; then
          run_check control-migrations sqlx migrate run --source migrations/control
        else
          skip control-migrations "sqlx-cli not installed"
        fi
        run_check integration-control cargo test -p control --features integration
        # pg-sink's integration tests are #[ignore]d, not feature-gated; CI runs
        # them one file at a time. Locally, one serialized sweep is the mirror.
        run_check integration-ignored cargo test --workspace -- --ignored --test-threads=1
      else
        cp "$tmpdir/compose-up.log" "$tmpdir/integration.log" 2>/dev/null || true
        failed integration
      fi ;;

    e2e)
      if ! docker_up; then skip e2e "docker daemon not running — CI covers this job"
      elif ensure_stack; then
        run_check e2e-quarantine cargo test -p e2e --features it --test reload_quarantine -- --ignored --test-threads=1
        run_check e2e-scale      cargo test -p e2e --features it --test reload_scale      -- --ignored --test-threads=1
      else
        cp "$tmpdir/compose-up.log" "$tmpdir/e2e.log" 2>/dev/null || true
        failed e2e
      fi ;;

    manifests)
      if command -v kubeconform >/dev/null && command -v kustomize >/dev/null; then
        run_check manifests bash scripts/k8s-validate.sh
      else skip manifests "kustomize/kubeconform not installed"; fi ;;

    images)
      if docker_up; then run_check images bash scripts/image-smoke.sh
      else skip images "docker daemon not running — CI covers this job"; fi ;;

    # Unreachable because the request is validated before any command runs.
    *) echo "CHECK:$gate=FAIL"; fail=1 ;;
  esac
done

if [ $fail -eq 0 ]; then echo "GATE=PASS"; else echo "GATE=FAIL"; fi
exit $fail
