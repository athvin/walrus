#!/usr/bin/env bash
# image-smoke.sh — PR 4.8 PID-1 SIGTERM smoke for the two container images.
#
# Proves the load-bearing property of the Dockerfiles: because the entrypoint is exec-form under
# `tini`, a SIGTERM (as Kubernetes sends on pod stop) reaches the Rust process, whose handler runs a
# graceful shutdown and exits 0 — it is NOT swallowed by a shell and SIGKILLed (exit 137).
#
# Why a full mini-pipeline and not `docker run; docker stop`: neither binary reaches a
# SIGTERM-handling state without its dependencies. The sink only exits 0 on SIGTERM once past
# bootstrap and in the streaming decode loop (needs control-pg + MinIO + source-pg); the loader owns
# no tables — and so has no apply loop to signal — until the sink has established an epoch and
# registered them. So: boot compose, run the sink to `streaming`, run the loader to `apply loops`,
# then SIGTERM each and assert a clean exit 0.
#
# Also checks the runtime images are slim (no cargo/rustc) and carry the CA bundle, and — implicitly,
# by the loader reaching its apply loop — that the bundled-DuckDB binary runs (libstdc++ present).
#
# Self-contained: builds the images if missing, owns the compose lifecycle. Run locally with just
#   bash scripts/image-smoke.sh
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

SINK_IMG="${SINK_IMG:-walrus-pg-sink:ci}"
LOADER_IMG="${LOADER_IMG:-walrus-loader:ci}"
COMPOSE="docker compose -f deploy/docker/docker-compose.yml"
NET="walrus_default" # compose project name is `walrus` → its default network
GRACE=90             # docker stop grace: SIGTERM, then SIGKILL after this many seconds
SINK_C="walrus-sink-smoke"
LOADER_C="walrus-loader-smoke"

fail() { echo "!! $*" >&2; exit 1; }

wait_log() { # container marker timeout_secs
  local c="$1" marker="$2" t="${3:-60}"
  for _ in $(seq 1 "$t"); do
    if docker logs "$c" 2>&1 | grep -q "$marker"; then return 0; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null)" != "true" ]; then
      echo "-- $c exited before logging '$marker'; last logs:"; docker logs "$c" 2>&1 | tail -40
      return 1
    fi
    sleep 1
  done
  echo "-- $c never logged '$marker' within ${t}s; last logs:"; docker logs "$c" 2>&1 | tail -40
  return 1
}

assert_sigterm_zero() { # container
  local c="$1" rc
  echo "-- SIGTERM $c (docker stop --time $GRACE) ..."
  docker stop --time "$GRACE" "$c" >/dev/null
  rc="$(docker inspect -f '{{.State.ExitCode}}' "$c")"
  echo "   $c exit code: $rc"
  if [ "$rc" != "0" ]; then
    docker logs "$c" 2>&1 | tail -40
    fail "$c: expected graceful exit 0, got $rc (137 = SIGKILL = signal was swallowed, not handled)"
  fi
}

cleanup() { docker rm -f "$SINK_C" "$LOADER_C" >/dev/null 2>&1; $COMPOSE down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

# 1. Build the images if they aren't present (CI builds them explicitly first; this makes a bare
#    local run one command).
docker image inspect "$SINK_IMG"   >/dev/null 2>&1 || docker build -f deploy/docker/Dockerfile.pg-sink -t "$SINK_IMG" .
docker image inspect "$LOADER_IMG" >/dev/null 2>&1 || docker build -f deploy/docker/Dockerfile.loader -t "$LOADER_IMG" .

# 2. Runtime images are slim and CA-equipped: no build toolchain leaked in, CA bundle present.
for img in "$SINK_IMG" "$LOADER_IMG"; do
  docker run --rm --entrypoint sh "$img" -c 'test -f /etc/ssl/certs/ca-certificates.crt' \
    || fail "$img: CA bundle missing (ca-certificates not installed)"
  if docker run --rm --entrypoint sh "$img" -c 'command -v cargo >/dev/null || command -v rustc >/dev/null'; then
    fail "$img: build toolchain (cargo/rustc) leaked into the runtime image"
  fi
done
echo "runtime images: slim (no toolchain) + CA bundle present"

# 3. Boot the backing stack and apply the source migrations the sink's preflight requires (publication
#    + ddl_audit/triggers; idempotent — mirrors scripts/sink-smoke.sh).
$COMPOSE up --wait
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0001_publication.sql
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0002_ddl_triggers.sql

# Credentials + object-store config shared by both containers (they reach compose by service DNS on
# the compose network — portable across Linux CI and local Docker Desktop).
COMMON_ENV=(
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin
  -e WALRUS_OBJECT_STORE__BUCKET=walrus
  -e WALRUS_OBJECT_STORE__ENDPOINT=http://minio:9000
  -e WALRUS_OBJECT_STORE__REGION=us-east-1
)

# 4. Sink: creates the slot + establishes the epoch, registers orders/customers/items, then streams.
echo "=== starting $SINK_C ==="
docker run -d --name "$SINK_C" --network "$NET" "${COMMON_ENV[@]}" \
  -e WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@control-pg:5432/walrus_control \
  -e WALRUS_SOURCE_DB_URL=postgres://postgres:postgres@source-pg:5432/walrus \
  -e WALRUS_INSTANCE=walrus-pg-sink-imgsmoke \
  -e WALRUS_SLOT_NAME=walrus_imgsmoke_slot \
  -e WALRUS_PUBLICATION_NAME=walrus_pub \
  -e WALRUS_HEALTH_ADDR=0.0.0.0:8088 \
  -e WALRUS_MAX_FILL=5s -e WALRUS_MAX_ROWS=1000 -e WALRUS_MAX_BYTES=1000000 -e WALRUS_MAX_INFLIGHT_BYTES=2000000 \
  -e WALRUS_STARTUP_DEADLINE=60s \
  "$SINK_IMG" >/dev/null
wait_log "$SINK_C" "streaming logical replication" 90 || fail "$SINK_C never reached the streaming decode loop"

# 5. Loader: owns the just-registered tables (epoch from the sink), apply loops poll waiting on the
#    shutdown token. Reaching "starting apply loops" also proves the bundled-DuckDB binary runs and
#    the httpfs extension loaded in the image.
echo "=== starting $LOADER_C ==="
docker run -d --name "$LOADER_C" --network "$NET" "${COMMON_ENV[@]}" \
  -e WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@control-pg:5432/walrus_control \
  -e WALRUS_INSTANCE=walrus-loader-imgsmoke \
  -e WALRUS_DUCKDB_DIR=/var/lib/walrus \
  -e WALRUS_HEALTH_ADDR=0.0.0.0:8090 \
  -e WALRUS_POLL_INTERVAL=1s \
  "$LOADER_IMG" >/dev/null
wait_log "$LOADER_C" "starting apply loops" 90 || fail "$LOADER_C never reached its apply loops"

# 6. The assertion: SIGTERM each container; both must handle it and exit 0 within the grace window.
assert_sigterm_zero "$LOADER_C"
assert_sigterm_zero "$SINK_C"

echo "image-smoke: PASS"
