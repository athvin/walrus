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
    cargo fmt --check

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --workspace

# Feature-gated integration tests (the `it` feature lands in later PRs).
it:
    cargo test --workspace --features it

# Criterion micro-benches (sink decode + Arrow batch building — PR 5.4). Run on a quiet machine;
# results print to stdout. Never a CI gate (shared runners are too noisy) — CI only compile-checks
# the bench targets via `clippy --all-targets`. Baselines live in docs/benchmarks.md.
bench:
    cargo bench -p pg-sink -p pg-to-arrow

# End-to-end throughput harness (PR 5.6): boots the compose stack, runs the release sink + loader
# against it, applies a load scenario, drains, and prints a per-stage summary + bottleneck ranking.
# LOCAL-ONLY (never a CI job — numbers are hardware-relative). Scenario: mixed | wide_text | large_txn.
bench-e2e scenario="mixed":
    bash scripts/bench-e2e.sh {{scenario}}

# Request a single-table reload (flavor: reload | resync) — the operator entry point (reload §5.1).
# INSERTs the request into control-pg's walrus.table_reload at the current epoch; the sink's reload
# controller (PR 6.4) picks it up within one heartbeat cadence. Runs psql inside the compose
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
