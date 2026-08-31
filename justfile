# justfile — everyday walrus dev commands. Run `just <recipe>`; `just --list` shows them all.
# Recipes are shell by default (`just` is not `make`).

compose := "docker compose -f deploy/docker/docker-compose.yml"

# List available recipes.
default:
    @just --list

# Boot the dev stack (source-pg, control-pg, minio + bucket) and block until healthy.
up:
    {{compose}} up --wait

# Tear the stack down, removing containers *and* volumes.
down:
    {{compose}} down -v

# Baseline gates (mirror CI).
fmt:
    cargo fmt --all --check

clippy:
    RUSTFLAGS="--cfg tokio_unstable" cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --workspace

# Feature-gated integration tests.
it:
    cargo test --workspace --features it

# Criterion micro-benches: sink decode, Arrow batch building, loader transform, and Phase-A append.
# Run on a quiet machine; results print to stdout. Never a CI gate (shared
# runners are too noisy) — CI only compile-checks the bench targets via `clippy --all-targets`.
# Baselines *and* the profiling workflow — how to get from "this bench is slow" to a flamegraph —
# live in docs/benchmarks.md. The loader targets build DuckDB (`bundled`), so a cold first run
# compiles for ~20 min before it measures anything; the 1M-row transform grid takes minutes.
bench:
    cargo bench -p pg-sink -p pg-to-arrow -p loader

# Record a named baseline for every bench (criterion stores it under `target/criterion/**/<name>`).
# `--` hands the flag to the bench binary (criterion), not to cargo.
bench-baseline name="main":
    cargo bench -p pg-sink -p pg-to-arrow -p loader -- --save-baseline {{name}}

# Re-run the benches against a saved baseline: criterion prints the per-bench delta and whether it
# clears its noise threshold. Usage: `just bench-baseline before`, apply the change, then
# `just bench-compare before` — the only honest read on hardware that drifts between runs.
bench-compare name="main":
    cargo bench -p pg-sink -p pg-to-arrow -p loader -- --baseline {{name}}

# End-to-end release measurement. The structured run bundle under target/perf includes the workload
# knobs, host/toolchain identity, one-second samples, CPU-seconds/1k rows and peak process RSS.
perf-e2e scenario="mixed":
    PERF_MODE=measure bash scripts/bench-e2e.sh {{scenario}}

# Compatibility name for the original benchmark entry point; it delegates to the structured run.
bench-e2e scenario="mixed":
    @just perf-e2e {{scenario}}

# Direction-aware comparison. It refuses different workloads, CPU/OS, toolchains or build modes.
perf-compare baseline candidate:
    python3 scripts/perf_report.py compare {{baseline}} {{candidate}}

# Profile one Criterion target, excluding Cargo/compilation from the captured process.
# Examples: `just profile-bench pg-sink decode parse_tuple 10`, `just profile-bench loader transform`.
profile-bench package bench filter="" seconds="10":
    bash scripts/profile-bench.sh {{package}} {{bench}} '{{filter}}' {{seconds}}

# Profile exactly one service while the complete real pipeline is under load.
profile-e2e target="loader" scenario="mixed":
    PERF_MODE=cpu PERF_TARGET={{target}} bash scripts/bench-e2e.sh {{scenario}}

# Allocation profile for exactly one service. Timing/CPU numbers in this bundle are diagnostic only.
profile-heap target="loader" scenario="mixed":
    PERF_MODE=heap PERF_TARGET={{target}} bash scripts/bench-e2e.sh {{scenario}}

# Tokio task/resource diagnostics for exactly one service. Connect the UI to 127.0.0.1:6669.
profile-async target="loader" scenario="mixed":
    PERF_MODE=async PERF_TARGET={{target}} bash scripts/bench-e2e.sh {{scenario}}

# Request a single-table reload (flavor: reload | resync) — the operator entry point (reload §5.1).
# INSERTs the request into control-pg's walrus.table_reload at the current epoch; the sink's reload
# controller picks it up within one heartbeat cadence. Runs psql inside the compose
# control-pg (the host needs no postgres-client), like `smoke`. Just args are positional, so both
# `just reload public.orders` and the self-documenting `just reload table='public.orders'` work —
# the optional key= prefixes are stripped.
reload table flavor='reload':
    #!/usr/bin/env bash
    set -euo pipefail
    t="{{table}}"; t="${t#table=}"
    f="{{flavor}}"; f="${f#flavor=}"
    {{compose}} exec -T control-pg psql -U postgres -d walrus_control -v ON_ERROR_STOP=1 \
      -c "INSERT INTO walrus.table_reload (epoch, source_schema, source_table, flavor) \
          SELECT COALESCE(MAX(epoch), 1), split_part('$t', '.', 1), split_part('$t', '.', 2), '$f' \
          FROM walrus.replication_state \
          RETURNING reload_id, source_schema, source_table, flavor, status"

# Connectivity smoke: both Postgres instances ready + MinIO health + the walrus bucket exists.
# Postgres checks run inside the containers (the host needs no postgres-client); MinIO health is
# hit on the published port.
smoke:
    {{compose}} exec -T source-pg pg_isready -U postgres -d walrus
    {{compose}} exec -T control-pg pg_isready -U postgres -d walrus_control
    curl -sf http://localhost:9000/minio/health/live
    {{compose}} exec -T createbucket mc ls local/walrus
