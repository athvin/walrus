# walrus — implementation curriculum

A **PR-by-PR guide to building walrus in Rust**, from an empty repo to a running Postgres → DuckDB
CDC pipeline on Kubernetes. It exists so you can *learn Rust by building a real system you already
understand*, one small green PR at a time.

The **design is already finished** and lives one directory up:

- [`../architecture.md`](../architecture.md) — the master sketch (sink, loader, S3 hand-off,
  slot/WAL safety, snapshot bootstrap, the raw→mirror transform, K8s, verification plan).
- [`../walrus-pg-sink.md`](../walrus-pg-sink.md) — the sink deep-dive (type conversion, DDL capture,
  pod lifecycle).
- [`../walrus-loader.md`](../walrus-loader.md) — the loader deep-dive (work-handoff, commit-gating,
  the two-phase append→transform, PK-churn collapse, lifecycle).
- [`../proto-version.md`](../proto-version.md) — the pgoutput wire format proven byte-by-byte, with a
  reproducible Docker harness and a Python decoder + golden vectors under
  [`../examples/proto-version/`](../examples/proto-version/).

This curriculum turns that design into **89 PRs across 7 phases** (phases 0–4 build v1; phase 5
hardens it — benchmarking, hot-path cleanup, and a much faster CI; phase 6 opens post-v1 feature
work — single-table reload through the one slot). Each PR is a self-contained task file with an
explicit *Definition of Done*. You write the code; the task tells you what "done and green" means.

---

## How to use this guide

1. Work **top to bottom**. Each PR depends only on ones before it (the [index](#the-roadmap) lists
   dependencies). Do not skip ahead — the ordering is the lesson.
2. For each PR: create a branch, open the task file, read its **Read first** links, then implement
   against the **Skeleton** until every box in **Definition of Done** is checked.
3. "Green" is non-negotiable and always means at least:
   ```
   cargo fmt --check
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test --workspace
   ```
   plus, where the task says so, `docker compose up --wait` and named integration assertions.
4. Open the PR, let CI go green, merge, tick the box in the [roadmap](#the-roadmap), move on.
5. When you add your own follow-up tasks, copy [`TEMPLATE.md`](./TEMPLATE.md) so they stay consistent.

The tasks give you **shapes, not solutions** — public signatures, enum variants, error types, and
test names, with `todo!()` bodies. The thinking (and the Rust) is yours. Every task links back to the
exact design section that already answers "but how should it behave?", so you are never guessing at
*intent* — only implementing it.

---

## What you're building (end-state recap)

Two services that share a control-plane Postgres and an S3 bucket:

```
 source Postgres ──pgoutput v2, streaming 'on'──►  walrus-pg-sink  ──Parquet──►  S3
 (wal_level=logical)                                    │                          │
                                                        └── file_manifest row ──►  │
                                                            (control Postgres)     │
                                                                                   ▼
   per-table  <table>.duckdb  ◄── MERGE transform ◄── append <table>_raw ◄──  walrus-loader
   (mirror + raw CDC log)                                     (reads Parquet from S3, polls manifest)
```

- **`walrus-pg-sink`** drains the WAL fast and safely: decode pgoutput → Arrow → Parquet → S3 →
  manifest, then advance the slot **only after** that's durable.
- **`walrus-loader`** reconciles accurately: poll the manifest → append each CDC row verbatim into
  `<table>_raw` → transform (dedup-to-latest by `(commit_lsn, lsn)`, then `MERGE`) into the
  current-state mirror `<table>`.

Delivery is **eventually consistent on a tunable budget** and **self-healing on Kubernetes** — not
real-time.

---

## Target workspace layout

The curriculum builds exactly the workspace proposed in
[`../architecture.md`](../architecture.md#proposed-rust-workspace-layout) — five crates, member names
un-prefixed (the workspace is already `walrus`):

```
walrus/
├── Cargo.toml            # [workspace] resolver="2", shared [workspace.lints] + dep versions
├── rust-toolchain.toml   # pinned stable channel
├── crates/
│   ├── common/           # lib — Lsn, errors + exit codes, config, telemetry, SinkMeta,
│   │                     #        the Postgres shape types (PgRelation/PgColumn/TupleValue) + TypeDescriptor
│   ├── pg-to-arrow/      # lib — PgRelation/TupleValue → Arrow → Parquet (+ DuckDB read-back conformance)
│   ├── control/          # lib — sqlx models for manifest / checkpoint / ddl / registry / ownership
│   ├── pg-sink/          # bin (+lib) — hand-rolled pgoutput decoder → Arrow → Parquet → S3 → manifest
│   └── loader/           # bin (+lib) — manifest poll → S3 → append <tbl>_raw → transform → <tbl>
├── migrations/{control,source}/   # sqlx control-plane DDL; source publication + ddl_audit triggers + heartbeat
├── tests/e2e/            # cross-service integration crate (feature-gated; needs docker compose)
└── deploy/{docker,k8s}/  # Dockerfiles; kustomize StatefulSets / PVC / probes / PDB / ConfigMap
```

**Crate dependency DAG** (`A → B` = A depends on B):

```
pg-sink ─┐
         ├─► pg-to-arrow ─► common
loader ──┤                    ▲
         ├─► control ─────────┘
         └─► common
```

Two deliberate structural notes:

- The **pgoutput decoder lives inside `pg-sink`** (a `pgoutput` module), not a separate crate — per
  the design's layout. `pg-sink` is built as **`lib.rs` + a thin `main.rs`** so `pg-sink/tests/` can
  import the decoder and drive it with the Python golden vectors. (You can promote it to its own
  crate later; nothing depends on that decision.)
- The neutral value types **`PgRelation`, `PgColumn`, `ReplicaIdentity`, `TupleValue`
  (`Null | UnchangedToast | Text | Binary`), and `TypeDescriptor` live in `common`.** The decoder
  *produces* them, `pg-to-arrow` *consumes* them, `control` *persists* the descriptor, and `loader`
  *reads it back* to rebuild types. This is why `pg-to-arrow` is fully unit-testable without the
  decoder — and why a binary crate never has to be a dependency (Cargo forbids that anyway).

---

## Conventions (hold these from PR 0.1)

| Area | Rule |
|---|---|
| Errors — libraries | `thiserror` enums; terminal-vs-transient is modelled, not stringly-typed. |
| Errors — binaries | `anyhow` with context; map to `common::ExitCode` at the top of `main`. |
| Logging | `tracing`; structured fields (`xid`, `commit_lsn`, `lsn`, `batch_uuid`), never `println!`. |
| Async | `tokio` in the binaries and `control`; `pgoutput` decode and the loader transform stay **sync + pure**. |
| Config | `serde`-typed, loaded from env/file, **bounds-validated** — invalid config is a terminal error. |
| Time | every walrus-stamped datetime is **UTC, RFC-3339, `Z`** — never local, never source offset. |
| Ordering | everything keys on **commit LSN** (`(commit_lsn, lsn)` tuples), never max-row-LSN. |
| Lints | `#![deny(warnings)]` via `[workspace.lints]`; `clippy --all-targets -D warnings` in CI. |
| Tests | unit tests inline (`#[cfg(test)]`); golden-vector & conformance tests in `tests/`; e2e feature-gated. |
| Commits/PRs | one PR per task file; PR description links the task file and pastes its DoD checklist. |

### Testing layers (fastest first — prefer the cheapest that proves the thing)

1. **Pure unit** (milliseconds, no Docker): `Lsn`, `SinkMeta`, the pgoutput decoder, the loader
   transform SQL on an in-memory DuckDB. The two hardest correctness stories live here.
2. **Conformance** (feature `conformance`): write Parquet → read it back with in-process DuckDB,
   assert both the inferred type and the value.
3. **Integration** (`docker compose up --wait`): a crate's `tests/` against a real Postgres / MinIO.
4. **End-to-end** (feature `it`, `tests/e2e/`): both services wired together against the compose stack.

---

## The roadmap

89 PRs. Tick each box as you merge. Every DoD is traceable to a design section (right column).

### Phase 0 — Foundations & CI  ·  [`phase-0-foundations/`](./phase-0-foundations/)

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [0.1](./phase-0-foundations/pr-0.1-workspace-skeleton-and-ci.md) | Cargo workspace, `rust-toolchain.toml`, `.gitignore`, MIT `LICENSE`, first CI gate | workspace layout |
| ✅ | [0.2](./phase-0-foundations/pr-0.2-common-errors-exit-codes.md) | `common` error taxonomy + terminal/transient + `ExitCode` | fail-fast preflight |
| ✅ | [0.3](./phase-0-foundations/pr-0.3-common-lsn-newtype.md) | `Lsn` newtype (parse, zero-padded Display, numeric `Ord`) | §1.4 / coordination |
| ✅ | [0.4](./phase-0-foundations/pr-0.4-common-telemetry.md) | `init_tracing()` + structured-field convention | Observability |
| ✅ | [0.5](./phase-0-foundations/pr-0.5-common-config.md) | typed, validated config loading | K8s config/cadence |
| ✅ | [0.6](./phase-0-foundations/pr-0.6-dev-harness-compose.md) | docker-compose (source PG + control PG + MinIO) + `justfile` | Verification harness |

### Phase 1 — Shared core  ·  [`phase-1-shared-core/`](./phase-1-shared-core/)

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [1.1](./phase-1-shared-core/pr-1.1-common-sink-meta.md) | `SinkMeta` provenance model (UTC `Z`) | §1.4 |
| ✅ | [1.2](./phase-1-shared-core/pr-1.2-common-pg-shape-types.md) | `PgRelation` / `PgColumn` / `TupleValue` / `TypeDescriptor` | §2.6 / proto §4 |
| ✅ | [1.3](./phase-1-shared-core/pr-1.3-control-migrations.md) | control-plane migrations + `sqlx::migrate!` runner | Coordination contract |
| ✅ | [1.4](./phase-1-shared-core/pr-1.4-control-file-manifest.md) | `file_manifest` claim/insert/delete (`ORDER BY lsn_end, id`) | loader §2 |
| ✅ | [1.5](./phase-1-shared-core/pr-1.5-control-checkpoint-replication-state.md) | two-watermark checkpoint + epoch, CHECK-guarded | loader §4 |
| ✅ | [1.6](./phase-1-shared-core/pr-1.6-control-schema-registry-ddl-manifest.md) | `schema_registry` + `ddl_manifest` models | §2.6 / DDL capture |

### Phase 2 — walrus-pg-sink  ·  [`phase-2-pg-sink/`](./phase-2-pg-sink/)

**2a — hand-rolled pgoutput decoder (TDD against the 24 golden vectors)**

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [2.1](./phase-2-pg-sink/pr-2.1-pgoutput-scaffold-golden-vectors.md) | `pg-sink` lib+bin; port `test_decode_pgoutput.py::VECTORS` to Rust fixtures | proto §14 |
| ✅ | [2.2](./phase-2-pg-sink/pr-2.2-pgoutput-reader-framing-begin-commit.md) | Reader primitives, framing, Begin/Commit | proto §4 / §7 |
| ✅ | [2.3](./phase-2-pg-sink/pr-2.3-pgoutput-relation-type.md) | Relation + Type (typmod → `numeric(p,s)`) | proto §4 / sink §2.3 |
| ✅ | [2.4](./phase-2-pg-sink/pr-2.4-pgoutput-tuple-insert.md) | TupleData (`n`/`u`/`t`/`b`) + Insert | proto §4–§5 |
| ✅ | [2.5](./phase-2-pg-sink/pr-2.5-pgoutput-update-delete.md) | Update + Delete (K/O old-image; NULL vs TOAST) | proto §4 / §6 |
| ✅ | [2.6](./phase-2-pg-sink/pr-2.6-pgoutput-truncate-message.md) | Truncate + logical Message | proto §4 |
| ✅ | [2.7](./phase-2-pg-sink/pr-2.7-pgoutput-streaming-frames.md) | v2 Stream frames + per-msg xid + subtxn-abort | proto §7–§10 |
| ✅ | [2.8](./phase-2-pg-sink/pr-2.8-pgoutput-two-phase.md) | v3 two-phase parse-without-misalign + `K` disambiguation | proto §12 |

**2b — pg-to-arrow conversion crate**

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [2.9](./phase-2-pg-sink/pr-2.9-pgarrow-tier1-schema.md) | Tier-1 Arrow schema from `PgRelation` (MICROS, Decimal128) | sink §2.1 / §2.3 |
| ✅ | [2.10](./phase-2-pg-sink/pr-2.10-pgarrow-tier1-recordbatch.md) | Tier-1 `TupleValue` → Arrow builders → RecordBatch | sink §2 / §2.7 |
| ✅ | [2.11](./phase-2-pg-sink/pr-2.11-pgarrow-parquet-duckdb-conformance.md) | Parquet write + DuckDB read-back conformance harness | sink §2.1 / §2.8 |
| ✅ | [2.12](./phase-2-pg-sink/pr-2.12-pgarrow-interval-timetz.md) | Tier-2 `interval` (3 cols) + `timetz` (2 cols) | sink §2.4 |
| ✅ | [2.13](./phase-2-pg-sink/pr-2.13-pgarrow-range-multirange.md) | Tier-2 `range` (5 cols) + `multirange` | sink §2.4 |
| ✅ | [2.14](./phase-2-pg-sink/pr-2.14-pgarrow-geometric.md) | Tier-2 geometric types → STRUCT/LIST of doubles | sink §2.4 |
| ✅ | [2.15](./phase-2-pg-sink/pr-2.15-pgarrow-tier3-text-carriers.md) | Tier-3 canonical-text carriers (numeric>38, bit, inet, …) | sink §2.5 |
| ✅ | [2.16](./phase-2-pg-sink/pr-2.16-pgarrow-uuid-enum.md) | `uuid` (arrow.uuid) + `enum` (VARCHAR + labels) | sink §2.4 / §2.5 |
| ✅ | [2.17](./phase-2-pg-sink/pr-2.17-pgarrow-type-descriptor.md) | `TypeDescriptor` → `schema_registry` | sink §2.6 |

**2c — the sink binary**

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [2.18](./phase-2-pg-sink/pr-2.18-sink-skeleton-health-shutdown.md) | bin skeleton: bootstrap scaffold, health endpoints, SIGTERM | sink §4.2–§4.3 |
| ✅ | [2.19](./phase-2-pg-sink/pr-2.19-sink-source-preflight.md) | source preflight (`wal_level`, headroom, publication) | §1.1 |
| ✅ | [2.20](./phase-2-pg-sink/pr-2.20-sink-replication-connection-keepalive.md) | `START_REPLICATION` + keepalive feedback (the spike) | §1.2 / §1.9 |
| ✅ | [2.21](./phase-2-pg-sink/pr-2.21-sink-wire-decoder.md) | wire the decoder to the live stream | proto §4 |
| ✅ | [2.22](./phase-2-pg-sink/pr-2.22-sink-relation-cache.md) | relation cache + Arrow schema per `schema_version` | bootstrap 7 / §2.6 |
| ✅ | [2.23](./phase-2-pg-sink/pr-2.23-sink-batching-cadence.md) | micro-batching + cadence flush triggers | §1.3 |
| ✅ | [2.24](./phase-2-pg-sink/pr-2.24-sink-parquet-s3-put.md) | Arrow → Parquet → S3 PUT (object_store) | §1.4 |
| ✅ | [2.25](./phase-2-pg-sink/pr-2.25-sink-manifest-insert.md) | manifest INSERT (`lsn_end` = commit LSN) | §1.5 |
| ✅ | [2.26](./phase-2-pg-sink/pr-2.26-sink-durability-checkpoint.md) | advance `confirmed_flush_lsn` only after S3 + manifest | §1.5 invariant |
| ✅ | [2.27](./phase-2-pg-sink/pr-2.27-sink-heartbeat-liveness.md) | idle heartbeat + round-trip liveness | §1.9 / sink §4.4 |
| ✅ | [2.28](./phase-2-pg-sink/pr-2.28-sink-graceful-shutdown.md) | graceful SIGTERM drain (never drop the slot) | sink §4.5 |
| ✅ | [2.29](./phase-2-pg-sink/pr-2.29-sink-snapshot-backfill.md) | snapshot/backfill via exported snapshot | §1.7 |
| ✅ | [2.30](./phase-2-pg-sink/pr-2.30-sink-streaming-large-txn.md) | streaming large-txn: demux + speculative staging + commit-gate | §1.6 / proto §8 |
| ✅ | [2.31](./phase-2-pg-sink/pr-2.31-sink-subtransaction-exclusion.md) | rolled-back subtransaction exclusion (flagship) | proto §9b / §1.6 |
| ✅ | [2.32](./phase-2-pg-sink/pr-2.32-sink-max-inflight-bytes.md) | aggregate `max_inflight_bytes` ceiling + spill | §1.3 |
| ✅ | [2.33](./phase-2-pg-sink/pr-2.33-sink-ddl-capture.md) | DDL capture consumption (ddl_audit → ddl_manifest + version bump) | sink §3 |

### Phase 3 — walrus-loader  ·  [`phase-3-loader/`](./phase-3-loader/)

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [3.1](./phase-3-loader/pr-3.1-loader-skeleton-bootstrap-lease.md) | bin skeleton: bootstrap (lease, DuckDB open, checkpoints) + health | loader §8.1–§8.2 |
| ✅ | [3.2](./phase-3-loader/pr-3.2-loader-phase-a-append.md) | Phase A: claim + append verbatim to `<table>_raw` + watermark/delete | loader §3–§4 |
| ✅ | [3.3](./phase-3-loader/pr-3.3-loader-transform-template.md) | transform SQL template + pure in-memory tests (crown jewel) | loader §5.2–§6 |
| ✅ | [3.4](./phase-3-loader/pr-3.4-loader-phase-b.md) | Phase B wiring + advance `transformed_lsn` | loader §4 |
| ✅ | [3.5](./phase-3-loader/pr-3.5-loader-truncate.md) | TRUNCATE (tuple-boundary wipe) | loader §5.5 |
| ✅ | [3.6](./phase-3-loader/pr-3.6-loader-unchanged-toast.md) | unchanged-TOAST resolution (raw back-scan) | loader §5.6 |
| ✅ | [3.7](./phase-3-loader/pr-3.7-loader-max-applied-lsn-guard.md) | per-PK max-applied-commit-LSN guard | loader §7 |
| ✅ | [3.8](./phase-3-loader/pr-3.8-loader-ddl-additive.md) | DDL apply — additive (add/rename/widen/comment) | per-change-type table |
| ✅ | [3.9](./phase-3-loader/pr-3.9-loader-ddl-destructive.md) | DDL apply — destructive (drop / lossy quarantine) | sink §3 / taxonomy |
| ✅ | [3.10](./phase-3-loader/pr-3.10-loader-snapshot-stream-boundary.md) | snapshot/stream boundary via the transform | §1.7 / loader §7 |
| ✅ | [3.11](./phase-3-loader/pr-3.11-loader-full-rebuild-compaction.md) | periodic full-rebuild / compaction + retention prune | loader §5.7 / §9.4 |
| ✅ | [3.12](./phase-3-loader/pr-3.12-loader-graceful-shutdown.md) | graceful SIGTERM drain + full-rebuild abort | loader §8.5 |

### Phase 4 — End-to-end, ops & resilience  ·  [`phase-4-end-to-end/`](./phase-4-end-to-end/)

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [4.1](./phase-4-end-to-end/pr-4.1-e2e-thin-slice.md) | e2e thin slice: INSERT/UPDATE/DELETE → mirror | Verification harness |
| ✅ | [4.2](./phase-4-end-to-end/pr-4.2-e2e-type-matrix.md) | e2e type round-trip matrix + unchanged-TOAST | Verification "Types" |
| ✅ | [4.3](./phase-4-end-to-end/pr-4.3-e2e-large-txn-streaming.md) | e2e large-txn + commit-order + subtxn-abort | Verification (large-txn) |
| ✅ | [4.4](./phase-4-end-to-end/pr-4.4-e2e-crash-safety.md) | e2e crash safety (effectively-once) | Verification "Crash safety" |
| ✅ | [4.5](./phase-4-end-to-end/pr-4.5-e2e-wal-runaway-heartbeat.md) | e2e WAL-runaway + heartbeat + keepalive-vs-durability | Verification (chaos) |
| ✅ | [4.6](./phase-4-end-to-end/pr-4.6-total-restart-epoch.md) | total-restart / epoch bump on slot loss | §1.8 |
| ✅ | [4.7](./phase-4-end-to-end/pr-4.7-ci-cargo-deny.md) | supply-chain CI: `cargo-deny` + MSRV | CI-grows |
| ✅ | [4.8](./phase-4-end-to-end/pr-4.8-dockerfiles.md) | multi-stage Dockerfiles (PID-1 SIGTERM) | sink §4.5 |
| ✅ | [4.9](./phase-4-end-to-end/pr-4.9-kubernetes-manifests.md) | Kubernetes manifests (StatefulSets, PVC, probes, PDB) | K8s deployment |
| ✅ | [4.10](./phase-4-end-to-end/pr-4.10-observability-metrics.md) | Prometheus metrics + dashboard + alerts | Observability |
| ✅ | [4.11](./phase-4-end-to-end/pr-4.11-deferred-goal-scaffolding.md) | deferred-goal scaffolding (CTID snapshot, sharding hooks) | Deferred goals |

> **🏁 v1 complete.** Phases 0 → 4 are done: the Postgres → DuckDB CDC pipeline is built, wired
> end-to-end, containerised, deployed to Kubernetes, observable, and its three deferred goals are
> documented with marked seams. The [deferred goals](../deferred-goals.md) remain future feature work;
> **Phase 5 below is the post-v1 hardening pass** — measure it, clean up the proven hot paths, and make
> CI fast.

### Phase 5 — Performance & CI  ·  [`phase-5-performance-and-ci/`](./phase-5-performance-and-ci/)

Post-v1 hardening: make CI fast (the bundled-DuckDB C++ build currently compiles up to four times per
cold run), build the benchmark instruments the design's performance claims have never been tested
against, then fix **only the measured** hot-path bottlenecks — every optimization lands with a
before/after delta recorded in `docs/benchmarks.md`. Closes with a dependency/debt sweep (the DuckDB
1.4.x LTS EOL clock is 2026-09-16).

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [5.1](./phase-5-performance-and-ci/pr-5.1-ci-restructure-path-filters.md) | CI restructure: drop redundant build, docs-only path filtering | CI-grows |
| ✅ | [5.2](./phase-5-performance-and-ci/pr-5.2-ci-sccache.md) | sccache: cache DuckDB's C++ objects across jobs/profiles | CI-grows |
| ✅ | [5.3](./phase-5-performance-and-ci/pr-5.3-docker-build-cache.md) | Docker builds: cargo-chef + GHA layer cache | CI-grows |
| ✅ | [5.4](./phase-5-performance-and-ci/pr-5.4-bench-sink-decode-arrow.md) | criterion benches: pgoutput decode + Arrow batch build; `docs/benchmarks.md` | proto §4–§8 / sink §2 |
| ✅ | [5.5](./phase-5-performance-and-ci/pr-5.5-bench-loader-transform.md) | criterion benches: transform scaling, TOAST back-scan, Phase-A append | loader §5–§6, §9.2 |
| ✅ | [5.6](./phase-5-performance-and-ci/pr-5.6-e2e-throughput-harness.md) | e2e throughput harness + `raw_append_lag_bytes` metric + bottleneck ranking | Observability / loader §9.3 |
| ✅ | [5.7](./phase-5-performance-and-ci/pr-5.7-sink-hot-path.md) | sink hot-path fixes (meta-JSON amortization, clone removal, release profile) — measured only | §1.4 / benchmarks |
| ✅ | [5.8](./phase-5-performance-and-ci/pr-5.8-loader-hot-path.md) | loader hot-path fixes (DESCRIBE cache, TOAST back-scan rewrite) — measured only | loader §5.6 / sink §3.5 |
| ✅ | [5.9](./phase-5-performance-and-ci/pr-5.9-dependency-debt-sweep.md) | debt sweep: commit_ts TODO, object_store advisories, DuckDB next-LTS bump | Open Q4(b) / proto §4 |

> **🚧 Feature work begins.** Phase 6 is the first post-v1 *feature* phase: it implements
> [deferred goal §1](../deferred-goals.md#1-single-table-reload--re-sync-while-streaming) per the
> decided design in [`single-table-reload.md`](../single-table-reload.md) — reload or re-sync N
> tables through the **one lifelong slot**, no stream pause. Its task files carry two pattern
> extensions, now in [`TEMPLATE.md`](./TEMPLATE.md): a **Status** line (`📋 Planned → ✅ Done`) and
> a **What completed looks like** section (the observable demo, distinct from the DoD checklist).
> "reload §Hn" in the Design column = a hole-section of that design doc.

### Phase 6 — Single-table reload  ·  [`phase-6-single-table-reload/`](./phase-6-single-table-reload/)

Chunked, watermark-stamped reloads in the Debezium/DBLog lineage: chunk-start watermarks flow
in-band through `walrus.reload_signal` (echo-wait gives each chunk its low watermark `L_i`), chunk
Parquet stamped `commit_lsn = lsn = L_i` sorts into the loader's existing `(lsn_end, id)` claim
order, and Phase B's dedup algebra absorbs snapshot/stream overlap — no extra slots, no stream
pause, no chunk buffer. Control-pg owns the state machine; restart-on-DDL keeps every attempt
single-schema; **quarantine recovery after a lossy `ALTER COLUMN TYPE`** — v1's only terminal
state — is the anchor use case and the phase-closing e2e.

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [6.1](./phase-6-single-table-reload/pr-6.1-control-table-reload-state-machine.md) | control-pg `table_reload` state machine + manifest `kind='reload'`/`reload_id` | reload §H4/H5/H10 |
| ✅ | [6.2](./phase-6-single-table-reload/pr-6.2-source-reload-signal-table.md) | source `walrus.reload_signal` (insert-only, published) + preflight | reload §H1/H5/H11 |
| ✅ | [6.3](./phase-6-single-table-reload/pr-6.3-sink-echo-routing-watermark.md) | echo routing + watermark waiter (`L_i` = decoded commit LSN) + race note | reload §H1/§6 |
| ✅ | [6.4](./phase-6-single-table-reload/pr-6.4-sink-reload-controller.md) | reload controller: pickup, preflight, lease, `max_concurrent_reloads` | reload §H6/H7/H11 |
| ✅ | [6.5](./phase-6-single-table-reload/pr-6.5-sink-chunk-export-engine.md) | chunk export engine: watermark → echo → stamped Parquet → manifest | reload §H1/H2/§5 |
| ✅ | [6.6](./phase-6-single-table-reload/pr-6.6-loader-pause-claims.md) | loader pauses a rebuilding table's claims (frontier freezes at `W`) | reload §2/H8 |
| ✅ | [6.7](./phase-6-single-table-reload/pr-6.7-loader-rebuild-trigger.md) | rebuild trigger: `CREATE OR REPLACE` on first reload file; latest-id wins | reload §H3/H8/H9 |
| ✅ | [6.8](./phase-6-single-table-reload/pr-6.8-ddl-invalidation-restart.md) | restart-on-DDL: fresh reload_id, purge, retry cap | reload §H9 |
| ✅ | [6.9](./phase-6-single-table-reload/pr-6.9-completion-crash-recovery.md) | completion (`transformed_lsn ≥ H`) + crash recovery from the chunk cursor | reload §H7/H10 |
| ✅ | [6.10](./phase-6-single-table-reload/pr-6.10-resync-flavor.md) | `resync` flavor: merge over the live mirror; the phantom caveat | reload §H3 |
| ✅ | [6.11](./phase-6-single-table-reload/pr-6.11-reload-observability.md) | reload metrics, alerts, runbook (stuck lease / restart cap / cross-check) | Observability |
| ☐ | [6.12](./phase-6-single-table-reload/pr-6.12-e2e-quarantine-recovery.md) | e2e quarantine recovery + N-table scale on one slot; docs sweep | reload §2/§5 |

---

## CI grows with the phases

CI is added in PR 0.1 and every "green" from then on runs through it. New gates switch on as the code
that needs them lands:

| From PR | New CI gate |
|---|---|
| 0.1 | `fmt --check`, `clippy --all-targets -D warnings`, `build --workspace`, `test --workspace` |
| 0.6 | compose job: `docker compose up --wait` → smoke → `down` |
| 1.3 | integration job vs compose (control PG); `sqlx` offline (`cargo sqlx prepare --check`) |
| 2.11 | DuckDB-bundled **conformance** job (feature-gated; registry/sccache cache) |
| 4.7 | `cargo-deny` (licenses / advisories / bans / sources); MSRV **1.95** guard (declared `rust-version` == pinned toolchain) |
| 4.8–4.9 | image build; `kubeconform` / kind manifest validation |
| 4.1+ | full `tests/e2e` job (feature `it`) |
| 5.1 | docs-only changes skip the compile-heavy jobs; redundant `build --workspace` step removed |
| 5.2 | sccache (Rust + bundled-DuckDB C++ object cache, GHA backend) in every compiling job |
| 5.3 | image builds via buildx with BuildKit cache mounts + `type=gha` layer cache |
| 5.4+ | bench targets compile-checked by `clippy --all-targets` (benches run locally, never a CI gate) |

---

## Reused assets from `../examples/proto-version/`

The pgoutput proof harness you already built is not throwaway — it seeds the hardest tests:

| Asset | Reused as |
|---|---|
| [`test_decode_pgoutput.py`](../examples/proto-version/test_decode_pgoutput.py) — 24 golden vectors | Rust fixture table in **PR 2.1**, asserted across **PRs 2.2–2.8** |
| [`run-tests.sh`](../examples/proto-version/run-tests.sh) — 28 live-wire assertions | the Rust compose assertions in **PRs 2.21 / 2.30 / 2.31** |
| [`docker-compose.yml`](../examples/proto-version/docker-compose.yml) + [`01-setup.sql`](../examples/proto-version/01-setup.sql) | the **PR 0.6** dev harness + every compose test's schema (orders single-PK, customers composite-PK, items REPLICA IDENTITY FULL, `mood` enum, `logical_decoding_work_mem=64kB`) |

### Golden-vector → PR map

| Vectors | Implemented in |
|---|---|
| `begin`, `commit`, `parse_stream` framing | PR 2.2 |
| `relation_*`, `type_enum` | PR 2.3 |
| `insert`, `insert_generated_column_omitted` | PR 2.4 |
| `update_*`, `delete_*`, `unchanged_toast_update`, NULL-vs-TOAST | PR 2.5 |
| `truncate_*`, `message_*` | PR 2.6 |
| `stream_*`, `stream_abort_*`, `streamed_insert_carries_xid` | PR 2.7 |
| `TwoPhase` (`begin_prepare`/`commit_prepared`/… + `K` disambiguation) | PR 2.8 |

---

## Design → verification traceability

Every Phase-4 e2e task implements a bullet from
[`../architecture.md` "Verification"](../architecture.md#verification-how-well-prove-it-works-end-to-end-later):
thin slice (4.1), types + TOAST (4.2), large-txn / commit-order / subtxn-abort (4.3), crash safety
(4.4), WAL-runaway + heartbeat + keepalive-vs-durability (4.5), slot-loss/total-restart (4.6). The
correctness unit tests those e2e cases mirror are proven earlier and cheaper: the decoder (2.2–2.8),
the type round-trip (2.11–2.16), and the transform's PK-churn / TRUNCATE / TOAST / guard cases
(3.3, 3.5, 3.6, 3.7).

---

*Design phase authored in the docs above. Implementation phase starts at
[PR 0.1](./phase-0-foundations/pr-0.1-workspace-skeleton-and-ci.md).*
