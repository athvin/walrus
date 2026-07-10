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

# Connectivity smoke: both Postgres instances ready + MinIO health + the walrus bucket exists.
# Postgres checks run inside the containers (the host needs no postgres-client); MinIO health is
# hit on the published port.
smoke:
    {{compose}} exec -T source-pg pg_isready -U postgres -d walrus
    {{compose}} exec -T control-pg pg_isready -U postgres -d walrus_control
    curl -sf http://localhost:9000/minio/health/live
    {{compose}} exec -T createbucket mc ls local/walrus
