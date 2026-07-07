# PR 0.6 — Dev harness: docker-compose (source PG + control PG + MinIO) + `justfile`

> **Phase:** 0 — Foundations & CI · **Crates touched:** none (infra: `deploy/docker/`, root `justfile`)
> · **Est. size:** M · **Depends on:** PR 0.5 · **Unlocks:** PR 1.3 (first integration test), phase 1

Every later "compose" Definition of Done — the sink writing to MinIO, the loader reading a manifest —
runs against **this** stack. This PR stands up the three backing services locally and in CI: a
**source Postgres** (`wal_level=logical`, tiny `logical_decoding_work_mem` so streaming trips on small
txns, seeded with the proto-harness schema), a **control Postgres**, and **MinIO** — all health-gated
so `docker compose up --wait` blocks until they're truly ready. A `justfile` wraps the everyday
commands, and CI gains a compose job that boots the stack, smoke-tests connectivity, and tears down.

## Why — learning objectives

By the end of this PR you will have practised:

- **Reproducible infra** — Compose healthchecks + `up --wait` as a deterministic gate (no `sleep`s).
- **Postgres for logical replication** — the exact server flags walrus's source needs, and *why* the
  work-mem is set absurdly low for the streaming tests to come.
- **A task runner** — `just` recipes so `up`/`down`/`fmt`/`clippy`/`test`/`it` are one word each.
- **CI service orchestration** — booting a stack in Actions and asserting it's healthy before tests.

## Read first

- [`../../examples/proto-version/docker-compose.yml`](../../examples/proto-version/docker-compose.yml)
  — the template: `postgres:16`, `wal_level=logical`, `max_replication_slots/max_wal_senders=10`,
  `logical_decoding_work_mem=64kB`, and the `pg_isready` healthcheck gating `up --wait`.
- [`../../examples/proto-version/01-setup.sql`](../../examples/proto-version/01-setup.sql) — the schema
  every compose test reuses: `orders` (single PK), `customers` (composite PK), `items` (REPLICA
  IDENTITY FULL), the `mood` enum. Adapt it as the source-PG init script.
- [`../../examples/proto-version/README.md`](../../examples/proto-version/README.md) "Quickstart" —
  the `up --wait` → seed → down flow this harness generalises.
- [`../../architecture.md`](../../architecture.md#kubernetes-deployment) "Kubernetes deployment" *WAL
  safety cap* row — why `logical_decoding_work_mem` is the streaming knob.

## Scope

**In scope**

- `deploy/docker/docker-compose.yml`: `source-pg` (5432), `control-pg` (5433), `minio` (9000/9001) —
  each with a healthcheck; `up --wait` succeeds only when all three are healthy.
- `deploy/docker/initdb/source/01-schema.sql`: the adapted proto-harness schema + `CREATE PUBLICATION`
  (do **not** create walrus's real slot here — the sink does that at bootstrap in Phase 2).
- `deploy/docker/initdb/minio/` bootstrap (a one-shot `mc` container or `minio/mc` job) that creates
  the `walrus` bucket so the store is usable immediately.
- Root `justfile`: `up`, `down`, `fmt`, `clippy`, `test`, `it`, `smoke`.
- `.env.example` documenting `WALRUS_*` vars (DB URLs, S3 endpoint/keys/bucket) that line up with
  `CommonConfig` (PR 0.5).
- CI: a `compose` job that runs `docker compose … up --wait`, a connectivity **smoke**, then `down -v`.

**Explicitly deferred** (do *not* build these here)

- Control-plane **migrations** (creating `file_manifest` etc.) → **PR 1.3** (`sqlx::migrate!`).
- Source **slot / DDL triggers / heartbeat** install → **PR 2.19 / 2.33** (`migrations/source/`).
- Any Rust integration test *using* the stack → first one lands in **PR 1.3**; this PR only proves the
  stack boots and is reachable.
- Kubernetes manifests / Dockerfiles → **PR 4.8 / 4.9**.

## Files to create / modify

```
deploy/docker/docker-compose.yml            # new — source-pg, control-pg, minio (+ mc bucket init)
deploy/docker/initdb/source/01-schema.sql   # new — adapted from examples/.../01-setup.sql (no slots)
.env.example                                # new — WALRUS_* vars mirroring CommonConfig
justfile                                     # new — up/down/fmt/clippy/test/it/smoke
.github/workflows/ci.yml                     # modify — add the `compose` job (up --wait → smoke → down)
```

## Skeleton

```yaml
# deploy/docker/docker-compose.yml
services:
  source-pg:                     # the replicated-from database
    image: postgres:16
    environment: { POSTGRES_PASSWORD: postgres, POSTGRES_DB: walrus }
    ports: ["5432:5432"]
    command:
      - postgres
      - -c
      - wal_level=logical               # required for logical replication
      - -c
      - max_replication_slots=10
      - -c
      - max_wal_senders=10
      - -c
      - logical_decoding_work_mem=64kB  # tiny → streaming trips on small txns (for Phase 2 tests)
    volumes: ["./initdb/source:/docker-entrypoint-initdb.d:ro"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d walrus"]
      interval: 2s
      timeout: 3s
      retries: 15

  control-pg:                    # walrus control plane (manifest/checkpoint/registry)
    image: postgres:16
    environment: { POSTGRES_PASSWORD: postgres, POSTGRES_DB: walrus_control }
    ports: ["5433:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d walrus_control"]
      interval: 2s
      timeout: 3s
      retries: 15

  minio:                         # S3-compatible staging bucket
    image: minio/minio
    command: server /data --console-address ":9001"
    environment: { MINIO_ROOT_USER: minioadmin, MINIO_ROOT_PASSWORD: minioadmin }
    ports: ["9000:9000", "9001:9001"]
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9000/minio/health/live || exit 1"]
      interval: 2s
      timeout: 3s
      retries: 15

  createbucket:                  # one-shot: make the `walrus` bucket, then exit 0
    image: minio/mc
    depends_on: { minio: { condition: service_healthy } }
    entrypoint: >
      /bin/sh -c "mc alias set local http://minio:9000 minioadmin minioadmin &&
                  mc mb --ignore-existing local/walrus"
```

```make
# justfile
compose := "docker compose -f deploy/docker/docker-compose.yml"

up:      ; {{compose}} up --wait
down:    ; {{compose}} down -v
fmt:     ; cargo fmt --check
clippy:  ; cargo clippy --all-targets --all-features -- -D warnings
test:    ; cargo test --workspace
it:      ; cargo test --workspace --features it        # feature-gated integration tests (land later)
smoke:   ; pg_isready -h localhost -p 5432 && pg_isready -h localhost -p 5433 && \
           curl -sf http://localhost:9000/minio/health/live
```

```dotenv
# .env.example — copy to .env; values match deploy/docker/docker-compose.yml + CommonConfig (PR 0.5)
WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@localhost:5433/walrus_control
WALRUS_OBJECT_STORE__BUCKET=walrus
WALRUS_OBJECT_STORE__ENDPOINT=http://localhost:9000
WALRUS_OBJECT_STORE__REGION=us-east-1
WALRUS_STARTUP_DEADLINE=60s
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `just up` (i.e. `docker compose … up --wait`) exits 0 only after **all three** services report
      healthy; `just down` removes containers *and* volumes (`-v`).
- [ ] `just smoke` passes: both Postgres ports accept connections and MinIO `/health/live` returns 200.
- [ ] The source PG comes up with `wal_level=logical` and is **seeded** with `orders` / `customers`
      (composite PK) / `items` (REPLICA IDENTITY FULL) / the `mood` enum + `CREATE PUBLICATION`
      (verify e.g. `SELECT count(*) FROM pg_publication_tables` ≥ 3).
- [ ] The MinIO `walrus` bucket exists after `up --wait` (the `createbucket` one-shot completed).
- [ ] `.env.example` keys map 1:1 onto `CommonConfig` (PR 0.5) so `cp .env.example .env` yields a
      loadable config.
- [ ] CI's new `compose` job runs `up --wait` → smoke → `down -v` and is red if any step fails.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `docker compose -f deploy/docker/docker-compose.yml up --wait` then `just smoke`

## Hints & gotchas

- `up --wait` only means anything if **every** service defines a `healthcheck` — a service without one
  is treated as "started = healthy" and can let tests race a not-yet-ready dependency. Give all three
  a real check (and use `depends_on: { condition: service_healthy }` for `createbucket`).
- **Do not** create walrus's lifelong replication slot in the init SQL. The sink creates and owns it at
  bootstrap (Phase 2, capturing the exported snapshot); a slot made here would be orphaned and pin WAL.
  Publication-only is correct for this harness.
- Map source PG to **5432** and control PG to **5433** on the host — two `postgres:16` containers will
  fight over 5432 otherwise, and the `.env` URLs must match the split.
- Keep `logical_decoding_work_mem=64kB` — it's not a mistake. Phase-2 streaming/large-txn tests rely on
  a few-thousand-row txn tripping `streaming 'on'`; the real 64MB default would need millions of rows.
- In CI, `minio/mc mb --ignore-existing` keeps the bucket step idempotent across reruns; and always
  run `down -v` in an `always()`/post step so a failed job doesn't leak volumes into the next run.
- `just` isn't `make` — recipes are shell by default; keep multi-command recipes on one logical line
  (`&&` / trailing `\`) or use a recipe body with proper indentation.

## References

- Design: [`../../architecture.md`](../../architecture.md#phased-roadmap) "Phased roadmap" step 0
  (compose: Postgres `wal_level=logical` + MinIO);
  [`../../architecture.md`](../../architecture.md#kubernetes-deployment) "Kubernetes deployment"
  (WAL safety cap / `logical_decoding_work_mem`). Reuses
  [`../../examples/proto-version/docker-compose.yml`](../../examples/proto-version/docker-compose.yml)
  + [`01-setup.sql`](../../examples/proto-version/01-setup.sql).
- Prev: [PR 0.5](./pr-0.5-common-config.md) · Next (phase boundary → Phase 1):
  [PR 1.1](../phase-1-shared-core/pr-1.1-common-sink-meta.md) · [Roadmap](../README.md)
