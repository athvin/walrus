#!/usr/bin/env bash
# PR 5.6 — end-to-end throughput harness. **Local-only; NOT a CI job** (numbers are hardware-relative).
#
# Measures the *system*: source Postgres → walrus-pg-sink → S3, and S3 → walrus-loader → mirror, on the
# real compose stack, using the Prometheus metrics (PR 4.10) as the probes. Boots the stack, runs the
# **release** sink + loader locally against it, applies a scenario's load, drains, prints a per-stage
# summary + a bottleneck ranking, and tears down.
#
#   scripts/bench-e2e.sh <mixed|wide_text|large_txn>
#   DURATION=60 CLIENTS=4  scripts/bench-e2e.sh mixed
#
# Every knob is echoed in the summary header — a number without its knobs can't be compared later.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

SCENARIO="${1:-mixed}"
DURATION="${DURATION:-60}"          # seconds of load (ignored for large_txn)
CLIENTS="${CLIENTS:-4}"
# Sink cadence knobs (small = flush often; the harness prints them).
MAX_FILL="${WALRUS_MAX_FILL:-2s}"
MAX_ROWS="${WALRUS_MAX_ROWS:-5000}"
MAX_BYTES="${WALRUS_MAX_BYTES:-2000000}"
# Must be ≥ MAX_BYTES (config bound). Kept low so a large_txn exceeds it and spills.
MAX_INFLIGHT="${WALRUS_MAX_INFLIGHT_BYTES:-4000000}"
POLL_INTERVAL="${WALRUS_POLL_INTERVAL:-1s}"

COMPOSE="docker compose -f deploy/docker/docker-compose.yml"
SINK_ADDR="127.0.0.1:8188"
LOADER_ADDR="127.0.0.1:8190"
DUCKDB_DIR="$(mktemp -d)/duckdb"
CSV="/tmp/bench-e2e-${SCENARIO}.csv"
SINK_PID=""; LOADER_PID=""; SCRAPE_PID=""

cleanup() {
  [ -n "$SCRAPE_PID" ] && kill "$SCRAPE_PID" 2>/dev/null
  [ -n "$LOADER_PID" ] && kill -TERM "$LOADER_PID" 2>/dev/null
  [ -n "$SINK_PID" ] && kill -TERM "$SINK_PID" 2>/dev/null
  wait 2>/dev/null
  $COMPOSE down -v >/dev/null 2>&1 || true
  rm -rf "$(dirname "$DUCKDB_DIR")" 2>/dev/null || true
}
trap cleanup EXIT

# --- helpers --------------------------------------------------------------------------------------
scrape() { curl -s "http://$1/metrics" 2>/dev/null; }

# Sum the value column of every sample whose name matches $2 (summed across label sets).
mval() { # metrics_text name
  awk -v n="$2" '$1 ~ ("^" n "([{ ]|$)") {s += $2} END {printf "%.4f", s+0}' <<<"$1"
}

wait_ready() { # addr timeout
  for _ in $(seq 1 "$((${2:-60} * 2))"); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "http://$1/ready" 2>/dev/null)" = "200" ] && return 0
    sleep 0.5
  done
  echo "!! $1/ready never reached 200" >&2; return 1
}

# --- 1. stack + migrations + release build --------------------------------------------------------
echo "=== bench-e2e: $SCENARIO (DURATION=${DURATION}s CLIENTS=$CLIENTS) ==="
$COMPOSE up --wait
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0001_publication.sql
$COMPOSE exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 -f - <migrations/source/0002_ddl_triggers.sql
echo "--- building release binaries ---"
cargo build --release -p pg-sink -p loader

# --- 2. start sink + loader (release) against the stack -------------------------------------------
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
export WALRUS_OBJECT_STORE__BUCKET=walrus
export WALRUS_OBJECT_STORE__ENDPOINT=http://localhost:9000
export WALRUS_OBJECT_STORE__REGION=us-east-1
export WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@localhost:5433/walrus_control

echo "--- starting walrus-pg-sink (release) on $SINK_ADDR ---"
WALRUS_SOURCE_DB_URL=postgres://postgres:postgres@localhost:5432/walrus \
WALRUS_INSTANCE=bench-sink WALRUS_SLOT_NAME=bench_slot WALRUS_PUBLICATION_NAME=walrus_pub \
WALRUS_HEALTH_ADDR="$SINK_ADDR" WALRUS_STARTUP_DEADLINE=60s \
WALRUS_MAX_FILL="$MAX_FILL" WALRUS_MAX_ROWS="$MAX_ROWS" WALRUS_MAX_BYTES="$MAX_BYTES" \
WALRUS_MAX_INFLIGHT_BYTES="$MAX_INFLIGHT" \
  target/release/walrus-pg-sink &
SINK_PID=$!
wait_ready "$SINK_ADDR" 60 || exit 1
echo "  sink ready (streaming); tables registered"

echo "--- starting walrus-loader (release) on $LOADER_ADDR ---"
mkdir -p "$DUCKDB_DIR"
WALRUS_INSTANCE=bench-loader WALRUS_DUCKDB_DIR="$DUCKDB_DIR" \
WALRUS_HEALTH_ADDR="$LOADER_ADDR" WALRUS_POLL_INTERVAL="$POLL_INTERVAL" \
  target/release/walrus-loader &
LOADER_PID=$!
wait_ready "$LOADER_ADDR" 40 || exit 1
echo "  loader ready (owns tables, apply loops running)"

# --- 3. baseline scrape + periodic scrape loop → CSV ----------------------------------------------
sink0="$(scrape "$SINK_ADDR")"; loader0="$(scrape "$LOADER_ADDR")"
rows_start="$(mval "$sink0" walrus_sink_parquet_rows_written_total)"
flush_sum0="$(mval "$sink0" walrus_sink_batch_flush_latency_seconds_sum)"
flush_cnt0="$(mval "$sink0" walrus_sink_batch_flush_latency_seconds_count)"
t_start=$(date +%s)

echo "t,sink_rows,raw_append_lag,transform_lag,files_ready,raw_rows,inflight,spill" >"$CSV"
(
  while true; do
    s="$(scrape "$SINK_ADDR")"; l="$(scrape "$LOADER_ADDR")"
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$(($(date +%s) - t_start))" \
      "$(mval "$s" walrus_sink_parquet_rows_written_total)" \
      "$(mval "$l" walrus_loader_raw_append_lag_bytes)" \
      "$(mval "$l" walrus_loader_transform_lag_bytes)" \
      "$(mval "$l" walrus_loader_files_ready)" \
      "$(mval "$l" walrus_loader_raw_row_count)" \
      "$(mval "$s" walrus_sink_inflight_bytes)" \
      "$(mval "$s" walrus_sink_spill_total)" >>"$CSV"
    sleep 5
  done
) & SCRAPE_PID=$!
disown "$SCRAPE_PID" 2>/dev/null || true

# --- 4. apply load ---------------------------------------------------------------------------------
echo "--- applying load ---"
bash scripts/loadgen.sh "$SCENARIO" "$DURATION" "$CLIENTS" || true

# --- 5. drain: wait until the WHOLE pipeline catches up (sink WAL + both loader lags → ~0), and stays
#        that way for 3 consecutive reads so a transient dip between batches doesn't end it early.
echo "--- draining (waiting for sink WAL + loader lags to settle) ---"
zeros=0
for _ in $(seq 1 150); do
  s="$(scrape "$SINK_ADDR")"; l="$(scrape "$LOADER_ADDR")"
  rlag="$(printf '%.0f' "$(mval "$s" walrus_sink_replication_lag_bytes)")"
  ra="$(mval "$l" walrus_loader_raw_append_lag_bytes)"; tr="$(mval "$l" walrus_loader_transform_lag_bytes)"
  fr="$(mval "$l" walrus_loader_files_ready)"
  if [ "${ra%.*}" = "0" ] && [ "${tr%.*}" = "0" ] && [ "${fr%.*}" = "0" ] && [ "$rlag" -lt 200000 ]; then
    zeros=$((zeros + 1)); [ "$zeros" -ge 3 ] && break
  else
    zeros=0
  fi
  sleep 2
done
kill "$SCRAPE_PID" 2>/dev/null; wait "$SCRAPE_PID" 2>/dev/null; SCRAPE_PID=""
t_end=$(date +%s); elapsed=$((t_end - t_start))

# --- 6. summary ------------------------------------------------------------------------------------
sink1="$(scrape "$SINK_ADDR")"; loader1="$(scrape "$LOADER_ADDR")"
rows_end="$(mval "$sink1" walrus_sink_parquet_rows_written_total)"
flush_sum1="$(mval "$sink1" walrus_sink_batch_flush_latency_seconds_sum)"
flush_cnt1="$(mval "$sink1" walrus_sink_batch_flush_latency_seconds_count)"
raw_rows_end="$(mval "$loader1" walrus_loader_raw_row_count)"

echo ""
echo "=========================================================================="
echo "  bench-e2e SUMMARY — scenario=$SCENARIO"
echo "  knobs: DURATION=${DURATION}s CLIENTS=$CLIENTS elapsed=${elapsed}s"
echo "         sink MAX_FILL=$MAX_FILL MAX_ROWS=$MAX_ROWS MAX_BYTES=$MAX_BYTES MAX_INFLIGHT=$MAX_INFLIGHT"
echo "         loader POLL_INTERVAL=$POLL_INTERVAL   (release binaries)"
echo "--------------------------------------------------------------------------"
awk -v rs="$rows_start" -v re="$rows_end" -v el="$elapsed" \
    -v fs0="$flush_sum0" -v fc0="$flush_cnt0" -v fs1="$flush_sum1" -v fc1="$flush_cnt1" \
    -v rr="$raw_rows_end" 'BEGIN{
  drows = re - rs; if (el<1) el=1;
  printf "  sink  Parquet rows written : %d  (%.0f rows/s)\n", drows, drows/el;
  dc = fc1 - fc0;
  if (dc>0) printf "  sink  mean flush latency    : %.1f ms  (%d flushes)\n", 1000*(fs1-fs0)/dc, dc;
}'
echo "--------------------------------------------------------------------------"
echo "  backlog over time (peak gauges — where the pipeline queued):"
awk -F, 'NR>1{if($3>ra)ra=$3; if($4>tr)tr=$4; if($5>fr)fr=$5; if($6>rr)rr=$6; if($7>inf)inf=$7; if($8>sp)sp=$8}
  END{printf "    loader raw_append_lag peak=%d   transform_lag peak=%d   raw_rows peak=%d\n    loader files_ready peak=%d   sink inflight peak=%d   spill_total=%d\n", ra,tr,rr,fr,inf,sp}' "$CSV"
echo "  full curve: $CSV"
echo "=========================================================================="
echo ""
echo "(interpret: the stage whose lag gauge stays highest/longest is the bottleneck —"
echo " sink_inflight → decode/flush bound; raw_append_lag → loader ingest bound;"
echo " transform_lag → transform bound.)"
