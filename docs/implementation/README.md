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

This curriculum turns that design into **367 PRs across 35 phases** (phases 0–4 build v1; phase 5
hardens it — benchmarking, hot-path cleanup, and a much faster CI; phase 6 opens post-v1 feature
work — single-table reload through the one slot; phase 7 is a conventions-hardening hygiene pass —
sibling test files, SQL-in-folders, no-unwrap lints, identifier audit; phase 8 is a cleanup audit —
DRY and type-modeling refinements over the finished tree, no new behaviour; phases 9–34 apply all
265 rules from `rust-skills` as audited tasks with a predetermined change, evidence, or superseded
outcome). Each PR is a self-contained task file with an explicit *Definition of Done*. You write the
code; the task tells you what "done and green" means.

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

For phases 9–34, `Readiness: audited` is a machine-checked safety boundary. `Outcome: change`
names a fixed implementation; `evidence` records why the rule is declined or already satisfied;
`superseded` verifies the earlier owning task still covers the rule. A baseline mismatch stops for
re-authoring—it is never an invitation to invent a different change or outcome. Most changes
preserve runtime behaviour; migrations, public compatibility, documentation, and metadata follow
the task-specific acceptance contract.

To execute the sequence, invoke the
[`implementing-walrus-roadmap`](../../.claude/skills/implementing-walrus-roadmap/SKILL.md) skill.
Its preflight validates the complete tracked corpus before selecting PR 9.1, then advances one
green task and mark-done PR at a time. A draft, missing/untracked ticket, partial index, baseline
mismatch, or failed labeled verification stops the loop instead of letting it improvise.

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
| Identifiers | walrus-authored columns are `lower_snake_case` (`_walrus_op`, `_walrus_commit_lsn`, `_walrus_lsn`, `_walrus_sink_processed_at`, `_applied_commit_lsn`, `_applied_lsn`, `walrus_pg_sink_meta`); source-derived DuckDB/Arrow columns are quoted **only** to mirror the source name faithfully (`{col}_months`, `{col}_lower`, …), never invented case-sensitive/spaced names of walrus's own; the sqlx `AS "col: Type"` in `control/sql/postgres/queries/*.sql` (e.g. `first_lsn AS "first_lsn: Lsn"`) is a compile-time type-cast, not a rename. |
| Ordering | everything keys on **commit LSN** (`(commit_lsn, lsn)` tuples), never max-row-LSN. |
| Lints | `#![deny(warnings)]` + `clippy = all/deny` via `[workspace.lints]`; `unwrap_used`/`expect_used` denied in production (a `clippy.toml` allows them in `#[cfg(test)]`/`#[test]` code; benches, integration test files, and the e2e harness lib carry a file-level allow); `clippy --all-targets -D warnings` in CI. |
| Tests | unit tests in a sibling `foo_test.rs` (`src/foo.rs` → `src/foo_test.rs`, Go-style, via `#[cfg(test)] #[path = "foo_test.rs"] mod tests;`; private access preserved); golden-vector & conformance tests in `tests/`; e2e feature-gated. |
| SQL location | per-crate `sql/<engine>/{queries,templates,test}/` (engine at the head); control's Postgres queries via `sqlx::query_file!` (compile-time checked; offline `.sqlx` cache committed); the loader's DuckDB DDL via `include_str!` templates with `{placeholder}` substitution; schema migrations stay under `/migrations/{control,source}/`. |
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

367 PRs. Tick each box as you merge. Every DoD is traceable to a design section or Rust rule
(right column).

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
| ✅ | [6.12](./phase-6-single-table-reload/pr-6.12-e2e-quarantine-recovery.md) | e2e quarantine recovery + N-table scale on one slot; docs sweep | reload §2/§5 |

> **🧹 Hardening pass.** Phase 7 is a post-v1 *hygiene* sweep over the finished v1+reload codebase —
> no new behaviour: relocate every inline `#[cfg(test)] mod tests { … }` to a sibling `mod tests;`
> file, pull inline SQL into per-crate `sql/<engine>/` folders (control's `sqlx::query!` →
> `query_file!`; the loader's `format!`-built DuckDB DDL → `include_str!` templates), and ban
> `unwrap`/`expect` outside tests (fix the offenders first, flip the lint last). The Conventions table
> is the deliverable that ships with it.

### Phase 7 — Conventions hardening  ·  [`phase-7-conventions-hardening/`](./phase-7-conventions-hardening/)

A debt pass, not feature work: every PR is a behaviour-preserving refactor/lint/docs delta that stays
green. Tests move into sibling files so a source file shows only its production surface; SQL moves into
per-engine folders so a query is a reviewable `.sql`, not a buried string; and the compiler starts
forbidding a production `unwrap`. The fix-then-flip split (7.6 fixes, 7.7 denies) makes CI-green the
proof that production is panic-free, and the phase closes with an identifier-naming audit that retires
the `"first_lsn: Lsn"` false alarm (it was always a sqlx type-cast, never a column name).

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [7.1](./phase-7-conventions-hardening/pr-7.1-tests-sibling-common-control-loader.md) | inline `mod tests` → sibling `src/*_test.rs` (common, control, loader) | Conventions (Tests) |
| ✅ | [7.2](./phase-7-conventions-hardening/pr-7.2-tests-sibling-pg-to-arrow.md) | same for `pg-to-arrow` (9 files; `batch`/`schema` largest) | Conventions (Tests) |
| ✅ | [7.3](./phase-7-conventions-hardening/pr-7.3-tests-sibling-pg-sink.md) | same for `pg-sink` (21 files, incl. nested `pgoutput/typmod`) | Conventions (Tests) |
| ✅ | [7.4](./phase-7-conventions-hardening/pr-7.4-control-sql-query-file.md) | control SQL → `sql/postgres/` via `sqlx::query_file!` | Conventions (SQL) |
| ✅ | [7.5](./phase-7-conventions-hardening/pr-7.5-loader-duckdb-templates.md) | loader DuckDB DDL → `sql/duckdb/` `include_str!` templates | Conventions (SQL) |
| ✅ | [7.6](./phase-7-conventions-hardening/pr-7.6-fix-unwrap-expect.md) | remove production `unwrap`/`expect` (parking_lot, typed errors) | Conventions (Lints) |
| ✅ | [7.7](./phase-7-conventions-hardening/pr-7.7-deny-unwrap-expect-lint.md) | deny `unwrap_used`/`expect_used` + `clippy.toml` (allow in tests) | Conventions (Lints) |
| ✅ | [7.8](./phase-7-conventions-hardening/pr-7.8-identifier-convention-audit.md) | identifier convention + naming audit (docs) | Conventions (Identifiers) |

> **🧽 Cleanup audit.** Phase 8 is a critical-but-honest pass over the finished v1 + reload
> codebase: no new behaviour, only DRY and type-modeling refinements. The tree is already
> strong (centralized lints, a real error taxonomy, no production `unwrap`, ~51% test ratio),
> so these rank *cleanup value*, not defect severity — see the phase
> [README](./phase-8-cleanup/README.md) for the honest headline and the *rejected findings*
> (e.g. the `as_common` "drift" that the exhaustive match already prevents).

### Phase 8 — Codebase cleanup  ·  [`phase-8-cleanup/`](./phase-8-cleanup/)

A cleanup audit, not feature work: each PR is a behaviour-preserving refactor that stays
green. Findings are tiered — SQL-escaping and stringly-typed manifest columns (Tier 1),
duplicated OID literals (Tier 2), the opt-in domain-ID newtype sweep (Tier 3), and true nits
(Tier 4). The PRs are independent; suggested order is 8.1 → 8.2 → 8.3 → 8.5, then the opt-in
8.4. See the phase README for the full findings table.

| ✅ | PR | Delivers | Design |
|---|---|---|---|
| ✅ | [8.1](./phase-8-cleanup/pr-8.1-sql-literal-helper.md) | one audited `common::sql::sql_literal` (6 hand-rolled escapes) | Conventions (SQL) |
| ✅ | [8.2](./phase-8-cleanup/pr-8.2-manifest-kind-status-enums.md) | type manifest `kind`/`status`; retire the stringly-typed columns (spill drift) | Conventions (Errors) |
| ✅ | [8.3](./phase-8-cleanup/pr-8.3-centralize-pg-oids.md) | one home for pg OID constants in `common::oids` (4 duplicate literal sites) | Crate DAG |
| ✅ | [8.4](./phase-8-cleanup/pr-8.4-domain-id-newtypes.md) | `ManifestId` newtype (slice 1/4; `EpochNo`/`SchemaVersion`/`ReloadId` deferred) | PR 0.3 `Lsn` precedent |
| ✅ | [8.5](./phase-8-cleanup/pr-8.5-nits-cluster.md) | nits: `pause_began` visibility, plan-tier dispatch documented, `Clock` documented-keep | Conventions / tiers |

> **Rust-skills curriculum.** Phases 9–34 form one audited serial chain over the finished
> Phase-8 tree. Every rule has exactly one task. `Outcome: evidence` and `superseded` tasks
> still land reproducible notes; they never manufacture code merely to demonstrate a rule.

### Phase 9 — Rust ownership & borrowing  ·  [`phase-9-rust-ownership/`](./phase-9-rust-ownership/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ✅ | [9.1](./phase-9-rust-ownership/pr-9.1-own-borrow-over-clone.md) | Delete the redundant and implicit clones the borrow checker never needed | `own-borrow-over-clone` |
| ✅ | [9.2](./phase-9-rust-ownership/pr-9.2-own-slice-over-vec.md) | Take `Option<&str>` not `&Option<String>`, and pin the borrowed-argument lints | `own-slice-over-vec` |
| ✅ | [9.3](./phase-9-rust-ownership/pr-9.3-own-clone-explicit.md) | Reuse the allocation with `clone_from` in the additive-DDL rename fold | `own-clone-explicit` |
| ☐ | [9.4](./phase-9-rust-ownership/pr-9.4-own-copy-small.md) | Derive `Copy` on the small value types and gate it with `missing_copy_implementations` | `own-copy-small` |
| ☐ | [9.5](./phase-9-rust-ownership/pr-9.5-own-cow-conditional.md) | Return `Cow<'_, str>` from `sql_literal` so the common no-quote case never allocates | `own-cow-conditional` |
| ☐ | [9.6](./phase-9-rust-ownership/pr-9.6-own-lifetime-elision.md) | Replace the two named lifetimes that elision already covers | `own-lifetime-elision` |
| ☐ | [9.7](./phase-9-rust-ownership/pr-9.7-own-move-large.md) | Pin compile-time size budgets on the hot decode types and deny the large-value lints | `own-move-large` |
| ☐ | [9.8](./phase-9-rust-ownership/pr-9.8-own-arc-shared.md) | Make every refcount bump explicit with `Arc::clone` and deny `clone_on_ref_ptr` | `own-arc-shared` |
| ☐ | [9.9](./phase-9-rust-ownership/pr-9.9-own-rc-single-thread.md) | Swap the loader's Parquet-column cache from `Arc<Vec<String>>` to `Rc<[String]>` | `own-rc-single-thread` |
| ☐ | [9.10](./phase-9-rust-ownership/pr-9.10-own-refcell-interior.md) | Use `Cell` and `RefCell` for the per-worker latches a thread-safe `Mutex` is guarding for nothing | `own-refcell-interior` |
| ☐ | [9.11](./phase-9-rust-ownership/pr-9.11-own-mutex-interior.md) | Drop the mutex guard before the match body in the reload watermark registry | `own-mutex-interior` |
| ☐ | [9.12](./phase-9-rust-ownership/pr-9.12-own-rwlock-readers.md) | Record why walrus has no `RwLock` and guard the lock-choice decision | `own-rwlock-readers` |

### Phase 10 — Rust error handling  ·  [`phase-10-rust-errors/`](./phase-10-rust-errors/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [10.1](./phase-10-rust-errors/pr-10.1-err-from-impl.md) | Give `ControlError` a classifying `From<sqlx::Error>` | `err-from-impl` |
| ☐ | [10.2](./phase-10-rust-errors/pr-10.2-err-question-mark.md) | Drop the redundant `map_err` boilerplate in `control` and propagate with `?` | `err-question-mark` |
| ☐ | [10.3](./phase-10-rust-errors/pr-10.3-err-source-chain.md) | Preserve the `duckdb::Error` source chain in `LoaderError::Duck` | `err-source-chain` |
| ☐ | [10.4](./phase-10-rust-errors/pr-10.4-err-custom-type.md) | Replace the `LoaderError::Internal` catch-all with named domain variants | `err-custom-type` |
| ☐ | [10.5](./phase-10-rust-errors/pr-10.5-err-context-chain.md) | Add anyhow context to the contextless pg-sink await sites | `err-context-chain` |
| ☐ | [10.6](./phase-10-rust-errors/pr-10.6-err-anyhow-app.md) | Close the exit-code downcast hole at the pg-sink app boundary | `err-anyhow-app` |
| ☐ | [10.7](./phase-10-rust-errors/pr-10.7-err-lowercase-msg.md) | Guard the lowercase no-punctuation error-message convention | `err-lowercase-msg` |
| ☐ | [10.8](./phase-10-rust-errors/pr-10.8-err-thiserror-lib.md) | Ban anyhow from the pure library crates via cargo-deny | `err-thiserror-lib` |
| ☐ | [10.9](./phase-10-rust-errors/pr-10.9-err-result-over-panic.md) | Deny panic todo unimplemented and unreachable in production | `err-result-over-panic` |
| ☐ | [10.10](./phase-10-rust-errors/pr-10.10-err-expect-bugs-only.md) | Turn the justified expect allow into an expect attribute with a reason | `err-expect-bugs-only` |
| ☐ | [10.11](./phase-10-rust-errors/pr-10.11-err-no-unwrap-prod.md) | Gate that every workspace member inherits the unwrap and expect denies | `err-no-unwrap-prod` |
| ☐ | [10.12](./phase-10-rust-errors/pr-10.12-err-doc-errors.md) | Document every fallible public function with an Errors section | `err-doc-errors` |

### Phase 11 — Rust memory optimization  ·  [`phase-11-rust-memory/`](./phase-11-rust-memory/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [11.1](./phase-11-rust-memory/pr-11.1-mem-with-capacity.md) | Pre-size the loader's per-cycle plan and bootstrap vectors | `mem-with-capacity` |
| ☐ | [11.2](./phase-11-rust-memory/pr-11.2-mem-assert-type-size.md) | Add the remaining hot-type size assertions after PR 9.7 | `mem-assert-type-size` |
| ☐ | [11.3](./phase-11-rust-memory/pr-11.3-mem-clone-from.md) | Reuse the remaining batch-id assignment with clone_from | `mem-clone-from` |
| ☐ | [11.4](./phase-11-rust-memory/pr-11.4-mem-write-over-format.md) | Write DDL SQL into the buffer instead of formatting a throwaway `String` | `mem-write-over-format` |
| ☐ | [11.5](./phase-11-rust-memory/pr-11.5-mem-avoid-format.md) | Cache the per-table metric label instead of formatting it every poll | `mem-avoid-format` |
| ☐ | [11.6](./phase-11-rust-memory/pr-11.6-mem-reuse-collections.md) | Reuse scratch buffers in the Arrow append and commit-promotion loops | `mem-reuse-collections` |
| ☐ | [11.7](./phase-11-rust-memory/pr-11.7-mem-take-replace.md) | Move rows out of the streamed-txn buffers with mem::take instead of cloning | `mem-take-replace` |
| ☐ | [11.8](./phase-11-rust-memory/pr-11.8-mem-drop-order.md) | Tighten the residual loader RefCell borrow lifetime | `mem-drop-order` |
| ☐ | [11.9](./phase-11-rust-memory/pr-11.9-mem-smaller-integers.md) | Right-size the decoder error offsets and give TypeMeta NonZero niches | `mem-smaller-integers` |
| ☐ | [11.10](./phase-11-rust-memory/pr-11.10-mem-box-large-variant.md) | Box the oversized pg-to-arrow error variants | `mem-box-large-variant` |
| ☐ | [11.11](./phase-11-rust-memory/pr-11.11-mem-boxed-slice.md) | Freeze the never-grown collections into Box<[T]> and Arc<[T]> | `mem-boxed-slice` |
| ☐ | [11.12](./phase-11-rust-memory/pr-11.12-mem-zero-copy.md) | Stop copying every text cell twice in the pgoutput decoder | `mem-zero-copy` |
| ☐ | [11.13](./phase-11-rust-memory/pr-11.13-mem-smallvec.md) | Measure and decline SmallVec for key-column scratch | `mem-smallvec` |
| ☐ | [11.14](./phase-11-rust-memory/pr-11.14-mem-arrayvec.md) | Record why ArrayVec has no hard-capacity home in walrus | `mem-arrayvec` |
| ☐ | [11.15](./phase-11-rust-memory/pr-11.15-mem-thinvec.md) | Measure ThinVec against the already-niche-optimised Option<Vec> | `mem-thinvec` |
| ☐ | [11.16](./phase-11-rust-memory/pr-11.16-mem-compact-string.md) | Re-measure and re-affirm the SinkMeta string-layout defer | `mem-compact-string` |
| ☐ | [11.17](./phase-11-rust-memory/pr-11.17-mem-arena-allocator.md) | Record why walrus has no arena-shaped allocation lifetime | `mem-arena-allocator` |

### Phase 12 — Rust unsafe-code policy  ·  [`phase-12-rust-unsafe/`](./phase-12-rust-unsafe/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [12.1](./phase-12-rust-unsafe/pr-12.1-unsafe-safety-comment.md) | Forbid unsafe code workspace-wide and require SAFETY comments if it ever returns | `unsafe-safety-comment` |
| ☐ | [12.2](./phase-12-rust-unsafe/pr-12.2-unsafe-minimize-scope.md) | Deny unsafe_op_in_unsafe_fn and multiple unsafe ops per block | `unsafe-minimize-scope` |
| ☐ | [12.3](./phase-12-rust-unsafe/pr-12.3-unsafe-extern-block.md) | Deny missing_unsafe_on_extern and document the one native FFI boundary | `unsafe-extern-block` |
| ☐ | [12.4](./phase-12-rust-unsafe/pr-12.4-unsafe-no-mangle-unsafe.md) | Deny unsafe_attr_outside_unsafe so exported symbols stay auditable | `unsafe-no-mangle-unsafe` |
| ☐ | [12.5](./phase-12-rust-unsafe/pr-12.5-unsafe-send-sync-manual.md) | Correct the loader's Send/Sync claims and pin them with compile-time assertions | `unsafe-send-sync-manual` |
| ☐ | [12.6](./phase-12-rust-unsafe/pr-12.6-unsafe-maybeuninit.md) | Guard against fake initialization and record why walrus never needs MaybeUninit | `unsafe-maybeuninit` |
| ☐ | [12.7](./phase-12-rust-unsafe/pr-12.7-unsafe-miri-ci.md) | Record the Miri decision and add a tripwire that forces it to be revisited | `unsafe-miri-ci` |

### Phase 13 — Rust API design  ·  [`phase-13-rust-api-design/`](./phase-13-rust-api-design/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [13.1](./phase-13-rust-api-design/pr-13.1-api-common-traits.md) | Derive Debug on every public type and deny missing_debug_implementations | `api-common-traits` |
| ☐ | [13.2](./phase-13-rust-api-design/pr-13.2-api-must-use.md) | Annotate pure accessors with must_use and deny clippy::must_use_candidate | `api-must-use` |
| ☐ | [13.3](./phase-13-rust-api-design/pr-13.3-api-newtype-safety.md) | Introduce the EpochNo newtype (domain-ID newtypes, slice 2 of 4) | `api-newtype-safety` |
| ☐ | [13.4](./phase-13-rust-api-design/pr-13.4-api-from-not-into.md) | Replace LoaderError::as_common with From, and give Lsn the From impls ManifestId already has | `api-from-not-into` |
| ☐ | [13.5](./phase-13-rust-api-design/pr-13.5-api-operator-overload.md) | Give Lsn the arithmetic operators its call sites are hand-rolling | `api-operator-overload` |
| ☐ | [13.6](./phase-13-rust-api-design/pr-13.6-api-parse-dont-validate.md) | Parse the sink's backpressure and threshold knobs into types that cannot be invalid | `api-parse-dont-validate` |
| ☐ | [13.7](./phase-13-rust-api-design/pr-13.7-api-default-impl.md) | Pin the shipped configuration defaults with a golden test | `api-default-impl` |
| ☐ | [13.8](./phase-13-rust-api-design/pr-13.8-api-builder-pattern.md) | Replace the 14-argument decode-loop signature with a builder | `api-builder-pattern` |
| ☐ | [13.9](./phase-13-rust-api-design/pr-13.9-api-builder-must-use.md) | Make ignoring a builder method a compile error | `api-builder-must-use` |
| ☐ | [13.10](./phase-13-rust-api-design/pr-13.10-api-impl-into.md) | Accept `impl Into<String>` in the sink's owned-String constructors | `api-impl-into` |
| ☐ | [13.11](./phase-13-rust-api-design/pr-13.11-api-impl-asref.md) | Take `impl AsRef<Path>` when opening a DuckDB table file | `api-impl-asref` |
| ☐ | [13.12](./phase-13-rust-api-design/pr-13.12-api-extension-trait.md) | Add a DuckDB result extension trait to kill 29 hand-written `map_err` closures | `api-extension-trait` |
| ☐ | [13.13](./phase-13-rust-api-design/pr-13.13-api-impl-fromiterator.md) | Make RelationCache a real collection: FromIterator, Extend, IntoIterator | `api-impl-fromiterator` |
| ☐ | [13.14](./phase-13-rust-api-design/pr-13.14-api-sealed-trait.md) | Seal the Clock trait so the test seam cannot become an extension point | `api-sealed-trait` |
| ☐ | [13.15](./phase-13-rust-api-design/pr-13.15-api-typestate.md) | Turn the snapshot-export handshake into a typestate | `api-typestate` |
| ☐ | [13.16](./phase-13-rust-api-design/pr-13.16-api-non-exhaustive.md) | Mark the error enums non_exhaustive so a new variant is not a cross-crate break | `api-non-exhaustive` |
| ☐ | [13.17](./phase-13-rust-api-design/pr-13.17-api-serde-optional.md) | Record why serde stays a hard dependency and gate the feature seam that already exists | `api-serde-optional` |

### Phase 14 — Rust async/await  ·  [`phase-14-rust-async/`](./phase-14-rust-async/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [14.1](./phase-14-rust-async/pr-14.1-async-select-racing.md) | Make shutdown win every select! race with biased | `async-select-racing` |
| ☐ | [14.2](./phase-14-rust-async/pr-14.2-async-cancel-safety.md) | Pin the replication frame future so a dropped select branch cannot tear a feedback write | `async-cancel-safety` |
| ☐ | [14.3](./phase-14-rust-async/pr-14.3-async-no-lock-await.md) | Complete the residual no-lock-across-await lint policy | `async-no-lock-await` |
| ☐ | [14.4](./phase-14-rust-async/pr-14.4-async-async-fn-bounds.md) | Replace the two-generic Fn-returning-Future bounds with AsyncFnMut | `async-async-fn-bounds` |
| ☐ | [14.5](./phase-14-rust-async/pr-14.5-async-oneshot-response.md) | Make the watermark oneshot waiter unsubscribe on drop | `async-oneshot-response` |
| ☐ | [14.6](./phase-14-rust-async/pr-14.6-async-cancellation-token.md) | Hold a CancellationToken DropGuard so every early return drains the pod | `async-cancellation-token` |
| ☐ | [14.7](./phase-14-rust-async/pr-14.7-async-tokio-fs.md) | Move the last blocking std::fs call to tokio::fs and ban the blocking file APIs | `async-tokio-fs` |
| ☐ | [14.8](./phase-14-rust-async/pr-14.8-async-clone-before-await.md) | Deny non-Send futures outside the loader and document the LocalSet exception | `async-clone-before-await` |
| ☐ | [14.9](./phase-14-rust-async/pr-14.9-async-fn-in-trait.md) | Ban a direct async-trait dependency now that AFIT is available | `async-fn-in-trait` |
| ☐ | [14.10](./phase-14-rust-async/pr-14.10-async-tokio-runtime.md) | Size and name the tokio runtime from bounds-validated config | `async-tokio-runtime` |
| ☐ | [14.11](./phase-14-rust-async/pr-14.11-async-try-join.md) | Race the control-DB and object-store bootstrap checks with try_join | `async-try-join` |
| ☐ | [14.12](./phase-14-rust-async/pr-14.12-async-join-parallel.md) | Join the two independent Phase-A control reads on every poll | `async-join-parallel` |
| ☐ | [14.13](./phase-14-rust-async/pr-14.13-async-joinset-structured.md) | Own the reload exporter tasks in a JoinSet instead of detaching them | `async-joinset-structured` |
| ☐ | [14.14](./phase-14-rust-async/pr-14.14-async-mpsc-queue.md) | Report loader worker failures over a bounded mpsc instead of mutating shared shutdown state | `async-mpsc-queue` |
| ☐ | [14.15](./phase-14-rust-async/pr-14.15-async-bounded-channel.md) | Forbid unbounded channels and make the last one bounded | `async-bounded-channel` |
| ☐ | [14.16](./phase-14-rust-async/pr-14.16-async-watch-latest.md) | Broadcast the current epoch over a watch channel instead of polling it per table | `async-watch-latest` |
| ☐ | [14.17](./phase-14-rust-async/pr-14.17-async-broadcast-pubsub.md) | Record why walrus has no broadcast channel and guard the decision | `async-broadcast-pubsub` |
| ☐ | [14.18](./phase-14-rust-async/pr-14.18-async-spawn-blocking.md) | Document why DuckDB work cannot use spawn_blocking and assert the !Send boundary | `async-spawn-blocking` |

### Phase 15 — Rust concurrency  ·  [`phase-15-rust-concurrency/`](./phase-15-rust-concurrency/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [15.1](./phase-15-rust-concurrency/pr-15.1-conc-atomic-ordering.md) | Replace SeqCst with the weakest correct ordering on the health and reload atomics | `conc-atomic-ordering` |
| ☐ | [15.2](./phase-15-rust-concurrency/pr-15.2-conc-thread-local.md) | Record why the thread-local scratch rule is superseded | `conc-thread-local` |
| ☐ | [15.3](./phase-15-rust-concurrency/pr-15.3-conc-rayon-par-iter.md) | Ban rayon in deny.toml and record why walrus has no data-parallel hot path | `conc-rayon-par-iter` |
| ☐ | [15.4](./phase-15-rust-concurrency/pr-15.4-conc-scoped-threads.md) | Guard the zero-OS-thread invariant and document the async structured-concurrency equivalent | `conc-scoped-threads` |

### Phase 16 — Rust compiler optimization  ·  [`phase-16-rust-codegen-opt/`](./phase-16-rust-codegen-opt/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [16.1](./phase-16-rust-codegen-opt/pr-16.1-opt-lto-release.md) | Gate the release LTO profile and fix the stale bench-profile note | `opt-lto-release` |
| ☐ | [16.2](./phase-16-rust-codegen-opt/pr-16.2-opt-bounds-check.md) | Replace hot-path slice indexing with slice patterns in the decoder and Arrow builder | `opt-bounds-check` |
| ☐ | [16.3](./phase-16-rust-codegen-opt/pr-16.3-opt-inline-small.md) | Add `#[inline]` to the cross-crate hot-path accessors | `opt-inline-small` |
| ☐ | [16.4](./phase-16-rust-codegen-opt/pr-16.4-opt-inline-always-rare.md) | Deny `clippy::inline_always` in the workspace lints | `opt-inline-always-rare` |
| ☐ | [16.5](./phase-16-rust-codegen-opt/pr-16.5-opt-inline-never-cold.md) | Extract the decoder's EOF error construction into a `#[cold] #[inline(never)]` helper | `opt-inline-never-cold` |
| ☐ | [16.6](./phase-16-rust-codegen-opt/pr-16.6-opt-cold-unlikely.md) | Mark the Arrow value-parse error constructor `#[cold]` | `opt-cold-unlikely` |
| ☐ | [16.7](./phase-16-rust-codegen-opt/pr-16.7-opt-likely-hint.md) | Mark the decoder's rare framing branches with std::hint::cold_path | `opt-likely-hint` |
| ☐ | [16.8](./phase-16-rust-codegen-opt/pr-16.8-opt-cache-friendly.md) | Measure cache footprints and guard only the residual Emit type | `opt-cache-friendly` |
| ☐ | [16.9](./phase-16-rust-codegen-opt/pr-16.9-opt-codegen-units.md) | Record the codegen-units rejection and guard the default | `opt-codegen-units` |
| ☐ | [16.10](./phase-16-rust-codegen-opt/pr-16.10-opt-target-cpu.md) | Reject target-cpu=native and guard against RUSTFLAGS drift | `opt-target-cpu` |
| ☐ | [16.11](./phase-16-rust-codegen-opt/pr-16.11-opt-pgo-profile.md) | Decline profile-guided optimization with the measured evidence | `opt-pgo-profile` |
| ☐ | [16.12](./phase-16-rust-codegen-opt/pr-16.12-opt-simd-portable.md) | Decline portable SIMD and ban the SIMD crates | `opt-simd-portable` |

### Phase 17 — Rust numeric safety  ·  [`phase-17-rust-numeric/`](./phase-17-rust-numeric/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [17.1](./phase-17-rust-numeric/pr-17.1-num-overflow-explicit.md) | Make integer overflow explicit in the interval parser and the in-flight meter | `num-overflow-explicit` |
| ☐ | [17.2](./phase-17-rust-numeric/pr-17.2-num-saturating-clamp.md) | Bound the bootstrap backoff and the lease renew interval with clamp instead of one-sided min/max | `num-saturating-clamp` |
| ☐ | [17.3](./phase-17-rust-numeric/pr-17.3-num-cast-try-from.md) | Replace lossy as-casts with TryFrom and deny the four lossy cast lints | `num-cast-try-from` |
| ☐ | [17.4](./phase-17-rust-numeric/pr-17.4-num-nonzero.md) | Encode must-be-positive config bounds as NonZero types | `num-nonzero` |
| ☐ | [17.5](./phase-17-rust-numeric/pr-17.5-num-float-compare.md) | Gate float equality with clippy::float_cmp and validate the backpressure ratios as finite | `num-float-compare` |

### Phase 18 — Rust type safety  ·  [`phase-18-rust-type-safety/`](./phase-18-rust-type-safety/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [18.1](./phase-18-rust-type-safety/pr-18.1-type-newtype-ids.md) | Finish the residual SchemaVersionNo and ReloadId newtypes | `type-newtype-ids` |
| ☐ | [18.2](./phase-18-rust-type-safety/pr-18.2-type-repr-transparent.md) | Make the transparent newtypes actually repr(transparent) and assert their layout | `type-repr-transparent` |
| ☐ | [18.3](./phase-18-rust-type-safety/pr-18.3-type-numeric-fmt.md) | Give Lsn the LowerHex, UpperHex, Octal and Binary formatters | `type-numeric-fmt` |
| ☐ | [18.4](./phase-18-rust-type-safety/pr-18.4-type-display-vs-debug.md) | Record that the Debug sweep is owned by PR 13.1 | `type-display-vs-debug` |
| ☐ | [18.5](./phase-18-rust-type-safety/pr-18.5-type-no-stringly.md) | Replace the four stringly-typed FromStr error types in control | `type-no-stringly` |
| ☐ | [18.6](./phase-18-rust-type-safety/pr-18.6-type-newtype-validated.md) | Add a validated SqlIdent newtype and retire the duplicated identifier quoting | `type-newtype-validated` |
| ☐ | [18.7](./phase-18-rust-type-safety/pr-18.7-type-enum-states.md) | Model the loader health lifecycle as one enum instead of two independent bools | `type-enum-states` |
| ☐ | [18.8](./phase-18-rust-type-safety/pr-18.8-type-option-nullable.md) | Replace the batcher's empty-string and `Lsn::ZERO` sentinels with `Option` | `type-option-nullable` |
| ☐ | [18.9](./phase-18-rust-type-safety/pr-18.9-type-result-fallible.md) | Stop swallowing errors into `Option` and deny `let_underscore_must_use` | `type-result-fallible` |
| ☐ | [18.10](./phase-18-rust-type-safety/pr-18.10-type-generic-bounds.md) | Add the three residual generic-bound hygiene lints | `type-generic-bounds` |
| ☐ | [18.11](./phase-18-rust-type-safety/pr-18.11-type-phantom-marker.md) | Tag DuckDB table names with `PhantomData` so mirror and raw cannot be swapped | `type-phantom-marker` |
| ☐ | [18.12](./phase-18-rust-type-safety/pr-18.12-type-deref-coercion.md) | Guard the domain newtypes against `Deref` inheritance | `type-deref-coercion` |
| ☐ | [18.13](./phase-18-rust-type-safety/pr-18.13-type-never-diverge.md) | Record and guard walrus's no-diverging-function rule | `type-never-diverge` |

### Phase 19 — Rust traits & generics  ·  [`phase-19-rust-traits/`](./phase-19-rust-traits/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [19.1](./phase-19-rust-traits/pr-19.1-trait-associated-type-vs-generic.md) | Give ArrowNumBuilder an associated Val type instead of a generic parameter | `trait-associated-type-vs-generic` |
| ☐ | [19.2](./phase-19-rust-traits/pr-19.2-trait-default-methods.md) | Model terminal-vs-transient as a FailureClass trait with defaulted methods | `trait-default-methods` |
| ☐ | [19.3](./phase-19-rust-traits/pr-19.3-trait-coherence-newtype.md) | Finish the residual UtcTimestamp conversion API | `trait-coherence-newtype` |
| ☐ | [19.4](./phase-19-rust-traits/pr-19.4-trait-blanket-impl.md) | Blanket-impl Clock for Arc<T> and &T so shared clocks satisfy a Clock bound | `trait-blanket-impl` |
| ☐ | [19.5](./phase-19-rust-traits/pr-19.5-trait-dyn-vs-generic.md) | Dispatch the sink clock statically and document why the remaining dyn stays dyn | `trait-dyn-vs-generic` |
| ☐ | [19.6](./phase-19-rust-traits/pr-19.6-trait-object-safety.md) | Lock in Clock's dyn compatibility with a compile-time guard and a Self: Sized gate | `trait-object-safety` |

### Phase 20 — Rust conversions  ·  [`phase-20-rust-conversions/`](./phase-20-rust-conversions/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [20.1](./phase-20-rust-conversions/pr-20.1-conv-tryfrom-fallible.md) | Implement TryFrom for the pgoutput wire-byte conversions | `conv-tryfrom-fallible` |
| ☐ | [20.2](./phase-20-rust-conversions/pr-20.2-conv-fromstr-parsing.md) | Give every FromStr a concrete error type and parse UtcTimestamp through FromStr | `conv-fromstr-parsing` |
| ☐ | [20.3](./phase-20-rust-conversions/pr-20.3-conv-asmut-mutable.md) | Record why AsMut has no write target and deny needless_pass_by_ref_mut instead | `conv-asmut-mutable` |

### Phase 21 — Rust const & compile-time  ·  [`phase-21-rust-const/`](./phase-21-rust-const/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [21.1](./phase-21-rust-const/pr-21.1-const-fn.md) | Make every eligible function a const fn and deny missing_const_for_fn | `const-fn` |
| ☐ | [21.2](./phase-21-rust-const/pr-21.2-const-vs-static.md) | Gate the const-vs-static storage-class policy and hoist the Postgres epoch constant | `const-vs-static` |
| ☐ | [21.3](./phase-21-rust-const/pr-21.3-const-block.md) | Assert walrus wire and exit-code invariants at compile time with const blocks | `const-block` |
| ☐ | [21.4](./phase-21-rust-const/pr-21.4-const-generics.md) | Collapse the fixed-width big-endian readers with const generics | `const-generics` |

### Phase 22 — Rust serde  ·  [`phase-22-rust-serde/`](./phase-22-rust-serde/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [22.1](./phase-22-rust-serde/pr-22.1-serde-deny-unknown-fields.md) | Close the last config typo hole with `deny_unknown_fields` on `TelemetryConfig` | `serde-deny-unknown-fields` |
| ☐ | [22.2](./phase-22-rust-serde/pr-22.2-serde-default-compat.md) | Make the sink-meta wire contract survive a mixed-version rollout with `serde(default)` | `serde-default-compat` |
| ☐ | [22.3](./phase-22-rust-serde/pr-22.3-serde-skip-empty.md) | Drop the empty `unchanged_toast` array from every row's sink-meta JSON | `serde-skip-empty` |
| ☐ | [22.4](./phase-22-rust-serde/pr-22.4-serde-rename-all.md) | Give `ReplicaIdentity` an explicit lowercase wire form with legacy aliases | `serde-rename-all` |
| ☐ | [22.5](./phase-22-rust-serde/pr-22.5-serde-try-from-validate.md) | Replace `Tier`'s hand-written deserializer with `serde(try_from)` and `serde(into)` | `serde-try-from-validate` |
| ☐ | [22.6](./phase-22-rust-serde/pr-22.6-serde-enum-representation.md) | Lock the scalar wire representation of every serde enum with an exhaustive-match test | `serde-enum-representation` |
| ☐ | [22.7](./phase-22-rust-serde/pr-22.7-serde-custom-with.md) | Prove and guard the humantime duration form on every config Duration field | `serde-custom-with` |
| ☐ | [22.8](./phase-22-rust-serde/pr-22.8-serde-flatten.md) | Record why serde flatten is rejected and guard the wire structs against it | `serde-flatten` |

### Phase 23 — Rust pattern matching  ·  [`phase-23-rust-patterns/`](./phase-23-rust-patterns/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [23.1](./phase-23-rust-patterns/pr-23.1-pat-let-else.md) | Replace diverging match-binds with let-else in the reload controller and DSN parser | `pat-let-else` |
| ☐ | [23.2](./phase-23-rust-patterns/pr-23.2-pat-matches-macro.md) | Collapse boolean matches into if/matches! and deny clippy::match_bool | `pat-matches-macro` |
| ☐ | [23.3](./phase-23-rust-patterns/pr-23.3-pat-exhaustive-enum.md) | Match TupleValue and Tier exhaustively instead of falling through a wildcard | `pat-exhaustive-enum` |
| ☐ | [23.4](./phase-23-rust-patterns/pr-23.4-pat-at-bindings.md) | Bind the Decimal128 precision boundary with an @ range pattern | `pat-at-bindings` |
| ☐ | [23.5](./phase-23-rust-patterns/pr-23.5-pat-if-let-chains.md) | Move the workspace to edition 2024 and collapse nested if-let into let chains | `pat-if-let-chains` |

### Phase 24 — Rust macros  ·  [`phase-24-rust-macros/`](./phase-24-rust-macros/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [24.1](./phase-24-rust-macros/pr-24.1-macro-prefer-functions.md) | Replace the `downcast!` macro with a generic downcast helper in pg-to-arrow | `macro-prefer-functions` |
| ☐ | [24.2](./phase-24-rust-macros/pr-24.2-macro-export-crate-path.md) | Export a `string_enum!` macro from common and adopt it for the manifest enums | `macro-export-crate-path` |
| ☐ | [24.3](./phase-24-rust-macros/pr-24.3-macro-fragment-specifiers.md) | Type the `string_enum!` arms with `:meta`/`:vis`/`:literal` and gate `:tt` slurping | `macro-fragment-specifiers` |
| ☐ | [24.4](./phase-24-rust-macros/pr-24.4-macro-rules-hygiene.md) | Route the `string_enum!` error path through a `$crate::` path | `macro-rules-hygiene` |
| ☐ | [24.5](./phase-24-rust-macros/pr-24.5-macro-private-helpers.md) | Hide the macro's runtime helper behind `#[doc(hidden)] pub mod __private` | `macro-private-helpers` |
| ☐ | [24.6](./phase-24-rust-macros/pr-24.6-macro-proc-error-spans.md) | Give `string_enum!` a `compile_error!` fallback arm instead of a token-soup diagnostic | `macro-proc-error-spans` |
| ☐ | [24.7](./phase-24-rust-macros/pr-24.7-macro-proc-syn-quote.md) | Record why walrus takes no direct syn/quote/proc-macro2 dependency | `macro-proc-syn-quote` |
| ☐ | [24.8](./phase-24-rust-macros/pr-24.8-macro-proc-two-crate.md) | Guard the workspace against an unjustified proc-macro plus facade crate split | `macro-proc-two-crate` |

### Phase 25 — Rust closures  ·  [`phase-25-rust-closures/`](./phase-25-rust-closures/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [25.1](./phase-25-rust-closures/pr-25.1-closure-impl-fn-return.md) | Return an impl FnOnce error mapper for the loader's DuckDB map_err closures | `closure-impl-fn-return` |
| ☐ | [25.2](./phase-25-rust-closures/pr-25.2-closure-fn-trait-bounds.md) | Add a FnOnce transaction seam to TableDb so rollback cannot be forgotten | `closure-fn-trait-bounds` |
| ☐ | [25.3](./phase-25-rust-closures/pr-25.3-closure-disjoint-capture.md) | Narrow the streamed-txn survivor closure from &self to the aborted set | `closure-disjoint-capture` |
| ☐ | [25.4](./phase-25-rust-closures/pr-25.4-closure-move-capture.md) | Gate the clone-before-move discipline with clippy::redundant_clone | `closure-move-capture` |
| ☐ | [25.5](./phase-25-rust-closures/pr-25.5-closure-static-vs-dyn.md) | Replace redundant method-call closures with function paths and deny the lint | `closure-static-vs-dyn` |

### Phase 26 — Rust collections  ·  [`phase-26-rust-collections/`](./phase-26-rust-collections/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [26.1](./phase-26-rust-collections/pr-26.1-coll-map-choice.md) | Key the relation cache with a BTreeMap so latest-version lookup is a range query | `coll-map-choice` |
| ☐ | [26.2](./phase-26-rust-collections/pr-26.2-coll-set-membership.md) | Index streamed-change ownership in a set instead of rescanning every open transaction | `coll-set-membership` |
| ☐ | [26.3](./phase-26-rust-collections/pr-26.3-coll-binaryheap.md) | Pop spill candidates from a BinaryHeap instead of rescanning the meter for the max | `coll-binaryheap` |
| ☐ | [26.4](./phase-26-rust-collections/pr-26.4-coll-seq-choice.md) | Drain pending reload signals in one pass and deny LinkedList workspace-wide | `coll-seq-choice` |

### Phase 27 — Rust naming conventions  ·  [`phase-27-rust-naming/`](./phase-27-rust-naming/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [27.1](./phase-27-rust-naming/pr-27.1-name-as-free.md) | Rename the allocating `LoaderError::as_common` to `to_common` | `name-as-free` |
| ☐ | [27.2](./phase-27-rust-naming/pr-27.2-name-to-expensive.md) | Give the allocating column projections a `to_` cost signal | `name-to-expensive` |
| ☐ | [27.3](./phase-27-rust-naming/pr-27.3-name-into-ownership.md) | Rename `BatchBuilder::finish` to `into_record_batch` to signal the move | `name-into-ownership` |
| ☐ | [27.4](./phase-27-rust-naming/pr-27.4-name-is-has-bool.md) | Prefix the predicate methods with `is_` | `name-is-has-bool` |
| ☐ | [27.5](./phase-27-rust-naming/pr-27.5-name-lifetime-short.md) | Elide the two nameable-but-pointless sqlx encoder lifetimes and deny `elidable_lifetime_names` | `name-lifetime-short` |
| ☐ | [27.6](./phase-27-rust-naming/pr-27.6-name-iter-method.md) | Give `PgRelation` `iter()` and `iter_mut()` over its columns | `name-iter-method` |
| ☐ | [27.7](./phase-27-rust-naming/pr-27.7-name-iter-convention.md) | Implement `IntoIterator` for `PgRelation` and its two reference forms | `name-iter-convention` |
| ☐ | [27.8](./phase-27-rust-naming/pr-27.8-name-iter-type-match.md) | Name the `PgRelation` iterator types `Iter`, `IterMut`, `IntoIter` | `name-iter-type-match` |
| ☐ | [27.9](./phase-27-rust-naming/pr-27.9-name-funcs-snake.md) | Deny `non_snake_case` explicitly and add the naming guard script | `name-funcs-snake` |
| ☐ | [27.10](./phase-27-rust-naming/pr-27.10-name-types-camel.md) | Deny `non_camel_case_types` explicitly and guard type names | `name-types-camel` |
| ☐ | [27.11](./phase-27-rust-naming/pr-27.11-name-variants-camel.md) | Make the enum variant the single source of truth for its SQL literal | `name-variants-camel` |
| ☐ | [27.12](./phase-27-rust-naming/pr-27.12-name-consts-screaming.md) | Deny `non_upper_case_globals` explicitly and guard const naming | `name-consts-screaming` |
| ☐ | [27.13](./phase-27-rust-naming/pr-27.13-name-acronym-word.md) | Turn on aggressive acronym casing in clippy.toml | `name-acronym-word` |
| ☐ | [27.14](./phase-27-rust-naming/pr-27.14-name-no-get-prefix.md) | Guard the tree against `get_` getters | `name-no-get-prefix` |
| ☐ | [27.15](./phase-27-rust-naming/pr-27.15-name-type-param-single.md) | Guard generic parameters to the conventional short names | `name-type-param-single` |
| ☐ | [27.16](./phase-27-rust-naming/pr-27.16-name-crate-no-rs.md) | Guard crate and binary names against `-rs` / `-rust` suffixes | `name-crate-no-rs` |

### Phase 28 — Rust testing  ·  [`phase-28-rust-testing/`](./phase-28-rust-testing/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [28.1](./phase-28-rust-testing/pr-28.1-test-cfg-test-module.md) | Guard sibling unit-test module wiring | `test-cfg-test-module` |
| ☐ | [28.2](./phase-28-rust-testing/pr-28.2-test-use-super.md) | Require `use super::*;` in sibling unit tests | `test-use-super` |
| ☐ | [28.3](./phase-28-rust-testing/pr-28.3-test-descriptive-names.md) | Reject placeholder names in sibling unit tests | `test-descriptive-names` |
| ☐ | [28.4](./phase-28-rust-testing/pr-28.4-test-arrange-act-assert.md) | Split and label four multi-concern unit tests | `test-arrange-act-assert` |
| ☐ | [28.5](./phase-28-rust-testing/pr-28.5-test-integration-dir.md) | Verify crate-root integration-test placement | `test-integration-dir` |
| ☐ | [28.6](./phase-28-rust-testing/pr-28.6-test-fixture-raii.md) | Replace fixed loader test paths with `TempDir` | `test-fixture-raii` |
| ☐ | [28.7](./phase-28-rust-testing/pr-28.7-test-tokio-async.md) | Run the reload concurrency test on paused Tokio time | `test-tokio-async` |
| ☐ | [28.8](./phase-28-rust-testing/pr-28.8-test-mock-traits.md) | Add hermetic object-store success and failure tests | `test-mock-traits` |
| ☐ | [28.9](./phase-28-rust-testing/pr-28.9-test-mockall-mocking.md) | Record PR 28.8 as the owner of object-store test doubles | `test-mockall-mocking` |
| ☐ | [28.10](./phase-28-rust-testing/pr-28.10-test-proptest-properties.md) | Property-test `Lsn` round trips and textual ordering | `test-proptest-properties` |
| ☐ | [28.11](./phase-28-rust-testing/pr-28.11-test-should-panic.md) | Record why Walrus has no `#[should_panic]` contract | `test-should-panic` |
| ☐ | [28.12](./phase-28-rust-testing/pr-28.12-test-doctest-examples.md) | Verify API doctests and explicit non-Rust fences | `test-doctest-examples` |
| ☐ | [28.13](./phase-28-rust-testing/pr-28.13-test-snapshot-testing.md) | Snapshot loader transform and additive-DDL output | `test-snapshot-testing` |
| ☐ | [28.14](./phase-28-rust-testing/pr-28.14-test-criterion-bench.md) | Add loader and comparison workflows to Criterion recipes | `test-criterion-bench` |
| ☐ | [28.15](./phase-28-rust-testing/pr-28.15-test-loom-concurrency.md) | Guard the absence of first-party atomic mutation | `test-loom-concurrency` |

### Phase 29 — Rust documentation  ·  [`phase-29-rust-documentation/`](./phase-29-rust-documentation/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [29.1](./phase-29-rust-documentation/pr-29.1-doc-module-inner.md) | Document and gate production module responsibilities | `doc-module-inner` |
| ☐ | [29.2](./phase-29-rust-documentation/pr-29.2-doc-all-public.md) | Document every reachable public API | `doc-all-public` |
| ☐ | [29.3](./phase-29-rust-documentation/pr-29.3-doc-errors-section.md) | Document caller-facing error contracts | `doc-errors-section` |
| ☐ | [29.4](./phase-29-rust-documentation/pr-29.4-doc-panics-section.md) | Verify no public API requires a `# Panics` contract | `doc-panics-section` |
| ☐ | [29.5](./phase-29-rust-documentation/pr-29.5-doc-safety-section.md) | Record the zero-unsafe documentation decision | `doc-safety-section` |
| ☐ | [29.6](./phase-29-rust-documentation/pr-29.6-doc-question-mark.md) | Verify runnable docs avoid `unwrap` and `expect` | `doc-question-mark` |
| ☐ | [29.7](./phase-29-rust-documentation/pr-29.7-doc-intra-links.md) | Link related API items with resolvable rustdoc links | `doc-intra-links` |
| ☐ | [29.8](./phase-29-rust-documentation/pr-29.8-doc-examples-section.md) | Add runnable examples for pure public APIs | `doc-examples-section` |
| ☐ | [29.9](./phase-29-rust-documentation/pr-29.9-doc-hidden-setup.md) | Record why PR 29.8 examples need no hidden setup | `doc-hidden-setup` |
| ☐ | [29.10](./phase-29-rust-documentation/pr-29.10-doc-link-types.md) | Record PR 29.7 as the owner of related-item links | `doc-link-types` |
| ☐ | [29.11](./phase-29-rust-documentation/pr-29.11-doc-cargo-metadata.md) | Mark internal workspace crates as non-publishable | `doc-cargo-metadata` |
| ☐ | [29.12](./phase-29-rust-documentation/pr-29.12-doc-crate-readme.md) | Record why monorepo crates must not include the root README | `doc-crate-readme` |

### Phase 30 — Rust observability  ·  [`phase-30-rust-observability/`](./phase-30-rust-observability/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [30.1](./phase-30-rust-observability/pr-30.1-obs-tracing-over-log.md) | Verify tracing-only diagnostics after subscriber initialization | `obs-tracing-over-log` |
| ☐ | [30.2](./phase-30-rust-observability/pr-30.2-obs-library-facade.md) | Verify subscriber initialization stays in binary bootstrap | `obs-library-facade` |
| ☐ | [30.3](./phase-30-rust-observability/pr-30.3-obs-structured-fields.md) | Move tracing values into stable structured fields | `obs-structured-fields` |
| ☐ | [30.4](./phase-30-rust-observability/pr-30.4-obs-instrument-spans.md) | Record why long-lived loops use events instead of instrument spans | `obs-instrument-spans` |
| ☐ | [30.5](./phase-30-rust-observability/pr-30.5-obs-levels-filter.md) | Verify telemetry filter precedence and safe defaults | `obs-levels-filter` |
| ☐ | [30.6](./phase-30-rust-observability/pr-30.6-obs-error-chain.md) | Preserve error source chains in warning and error events | `obs-error-chain` |
| ☐ | [30.7](./phase-30-rust-observability/pr-30.7-obs-no-sensitive-data.md) | Verify tracing emits only allowlisted non-secret data | `obs-no-sensitive-data` |

### Phase 31 — Rust performance patterns  ·  [`phase-31-rust-performance/`](./phase-31-rust-performance/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [31.1](./phase-31-rust-performance/pr-31.1-perf-iter-over-index.md) | Record why variable-step parsers retain indexed loops | `perf-iter-over-index` |
| ☐ | [31.2](./phase-31-rust-performance/pr-31.2-perf-iter-lazy.md) | Verify collections cross ownership or reuse boundaries | `perf-iter-lazy` |
| ☐ | [31.3](./phase-31-rust-performance/pr-31.3-perf-collect-once.md) | Verify no iterator pipeline collects an intermediate result | `perf-collect-once` |
| ☐ | [31.4](./phase-31-rust-performance/pr-31.4-perf-entry-api.md) | Verify map updates already use the correct entry semantics | `perf-entry-api` |
| ☐ | [31.5](./phase-31-rust-performance/pr-31.5-perf-drain-reuse.md) | Record why move-out beats drain for owned work buffers | `perf-drain-reuse` |
| ☐ | [31.6](./phase-31-rust-performance/pr-31.6-perf-extend-batch.md) | Record why stateful builder loops do not use `extend` | `perf-extend-batch` |
| ☐ | [31.7](./phase-31-rust-performance/pr-31.7-perf-chain-avoid.md) | Verify the remaining iterator chain is bounded and cold | `perf-chain-avoid` |
| ☐ | [31.8](./phase-31-rust-performance/pr-31.8-perf-collect-into.md) | Record why collected destinations cannot reuse capacity | `perf-collect-into` |
| ☐ | [31.9](./phase-31-rust-performance/pr-31.9-perf-black-box-bench.md) | Verify every Criterion target prevents dead-code elimination | `perf-black-box-bench` |
| ☐ | [31.10](./phase-31-rust-performance/pr-31.10-perf-release-profile.md) | Verify the existing thin-LTO release-profile decision | `perf-release-profile` |
| ☐ | [31.11](./phase-31-rust-performance/pr-31.11-perf-profile-first.md) | Record the benchmark evidence required before optimization | `perf-profile-first` |
| ☐ | [31.12](./phase-31-rust-performance/pr-31.12-perf-ahash.md) | Record why the default hasher remains the safe baseline | `perf-ahash` |
| ☐ | [31.13](./phase-31-rust-performance/pr-31.13-perf-io-buffering.md) | Verify existing I/O layers already provide appropriate buffering | `perf-io-buffering` |

### Phase 32 — Rust project structure  ·  [`phase-32-rust-project-structure/`](./phase-32-rust-project-structure/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [32.1](./phase-32-rust-project-structure/pr-32.1-proj-lib-main-split.md) | Move service orchestration out of `main.rs` | `proj-lib-main-split` |
| ☐ | [32.2](./phase-32-rust-project-structure/pr-32.2-proj-mod-by-feature.md) | Verify top-level modules follow product capabilities | `proj-mod-by-feature` |
| ☐ | [32.3](./phase-32-rust-project-structure/pr-32.3-proj-flat-small.md) | Record why the current per-crate module depth stays flat | `proj-flat-small` |
| ☐ | [32.4](./phase-32-rust-project-structure/pr-32.4-proj-mod-rs-dir.md) | Verify only the multi-file decoder needs `mod.rs` | `proj-mod-rs-dir` |
| ☐ | [32.5](./phase-32-rust-project-structure/pr-32.5-proj-pub-crate-internal.md) | Verify public visibility has external consumers | `proj-pub-crate-internal` |
| ☐ | [32.6](./phase-32-rust-project-structure/pr-32.6-proj-pub-super-parent.md) | Record why no item qualifies for `pub(super)` | `proj-pub-super-parent` |
| ☐ | [32.7](./phase-32-rust-project-structure/pr-32.7-proj-pub-use-reexport.md) | Verify current re-exports form the intended public facades | `proj-pub-use-reexport` |
| ☐ | [32.8](./phase-32-rust-project-structure/pr-32.8-proj-prelude-module.md) | Record why Walrus should not add a prelude | `proj-prelude-module` |
| ☐ | [32.9](./phase-32-rust-project-structure/pr-32.9-proj-bin-dir.md) | Verify one named binary per service package | `proj-bin-dir` |
| ☐ | [32.10](./phase-32-rust-project-structure/pr-32.10-proj-workspace-large.md) | Verify the six-package workspace boundary | `proj-workspace-large` |
| ☐ | [32.11](./phase-32-rust-project-structure/pr-32.11-proj-workspace-deps.md) | Verify third-party versions inherit from the workspace | `proj-workspace-deps` |
| ☐ | [32.12](./phase-32-rust-project-structure/pr-32.12-proj-feature-additive.md) | Verify every Cargo feature is additive | `proj-feature-additive` |
| ☐ | [32.13](./phase-32-rust-project-structure/pr-32.13-proj-msrv-declare.md) | Verify one inherited Rust 1.95 MSRV contract | `proj-msrv-declare` |
| ☐ | [32.14](./phase-32-rust-project-structure/pr-32.14-proj-build-rs-minimal.md) | Record the absence of first-party build scripts | `proj-build-rs-minimal` |

### Phase 33 — Rust linting  ·  [`phase-33-rust-linting/`](./phase-33-rust-linting/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [33.1](./phase-33-rust-linting/pr-33.1-lint-deny-correctness.md) | Verify `clippy::all = deny` already enforces correctness | `lint-deny-correctness` |
| ☐ | [33.2](./phase-33-rust-linting/pr-33.2-lint-warn-suspicious.md) | Record PR 33.1 as the owner of `clippy::suspicious` enforcement | `lint-warn-suspicious` |
| ☐ | [33.3](./phase-33-rust-linting/pr-33.3-lint-warn-style.md) | Record PR 33.1 as the owner of `clippy::style` enforcement | `lint-warn-style` |
| ☐ | [33.4](./phase-33-rust-linting/pr-33.4-lint-warn-complexity.md) | Record PR 33.1 as the owner of `clippy::complexity` enforcement | `lint-warn-complexity` |
| ☐ | [33.5](./phase-33-rust-linting/pr-33.5-lint-warn-perf.md) | Record PR 33.1 as the owner of `clippy::perf` enforcement | `lint-warn-perf` |
| ☐ | [33.6](./phase-33-rust-linting/pr-33.6-lint-pedantic-selective.md) | Record why Walrus selects lints instead of enabling all pedantic | `lint-pedantic-selective` |
| ☐ | [33.7](./phase-33-rust-linting/pr-33.7-lint-missing-docs.md) | Deny missing docs and broken rustdoc links workspace-wide | `lint-missing-docs` |
| ☐ | [33.8](./phase-33-rust-linting/pr-33.8-lint-unsafe-doc.md) | Record why zero-unsafe policy needs no extra doc lint | `lint-unsafe-doc` |
| ☐ | [33.9](./phase-33-rust-linting/pr-33.9-lint-cargo-metadata.md) | Record why internal crates do not enable `clippy::cargo` | `lint-cargo-metadata` |
| ☐ | [33.10](./phase-33-rust-linting/pr-33.10-lint-rustfmt-check.md) | Verify CI and `just fmt` enforce `cargo fmt --check` | `lint-rustfmt-check` |
| ☐ | [33.11](./phase-33-rust-linting/pr-33.11-lint-workspace-lints.md) | Verify every workspace member inherits centralized lints | `lint-workspace-lints` |
| ☐ | [33.12](./phase-33-rust-linting/pr-33.12-lint-cfg-check.md) | Verify denied warnings already catch unexpected cfgs | `lint-cfg-check` |
| ☐ | [33.13](./phase-33-rust-linting/pr-33.13-lint-clippy-nursery-selected.md) | Record the selective nursery-lint decision | `lint-clippy-nursery-selected` |

### Phase 34 — Rust anti-patterns  ·  [`phase-34-rust-anti-patterns/`](./phase-34-rust-anti-patterns/)

| ☐ | PR | Delivers | Rust rule |
|---|---|---|---|
| ☐ | [34.1](./phase-34-rust-anti-patterns/pr-34.1-anti-unwrap-abuse.md) | Record PR 7.7 as the owner of production `unwrap` enforcement | `anti-unwrap-abuse` |
| ☐ | [34.2](./phase-34-rust-anti-patterns/pr-34.2-anti-expect-lazy.md) | Record PR 7.7 as the owner of production `expect` enforcement | `anti-expect-lazy` |
| ☐ | [34.3](./phase-34-rust-anti-patterns/pr-34.3-anti-clone-excessive.md) | Record PR 9.1 as the owner of the clone audit | `anti-clone-excessive` |
| ☐ | [34.4](./phase-34-rust-anti-patterns/pr-34.4-anti-lock-across-await.md) | Record PR 14.3 as the owner of lock-across-await enforcement | `anti-lock-across-await` |
| ☐ | [34.5](./phase-34-rust-anti-patterns/pr-34.5-anti-string-for-str.md) | Record PR 9.2 as the owner of borrowed string parameters | `anti-string-for-str` |
| ☐ | [34.6](./phase-34-rust-anti-patterns/pr-34.6-anti-vec-for-slice.md) | Record PR 9.2 as the owner of borrowed slice parameters | `anti-vec-for-slice` |
| ☐ | [34.7](./phase-34-rust-anti-patterns/pr-34.7-anti-index-over-iter.md) | Record PR 31.1 as the owner of the indexed-loop audit | `anti-index-over-iter` |
| ☐ | [34.8](./phase-34-rust-anti-patterns/pr-34.8-anti-panic-expected.md) | Record PR 10.9 as the owner of production panic enforcement | `anti-panic-expected` |
| ☐ | [34.9](./phase-34-rust-anti-patterns/pr-34.9-anti-empty-catch.md) | Log discarded cleanup and task-join errors | `anti-empty-catch` |
| ☐ | [34.10](./phase-34-rust-anti-patterns/pr-34.10-anti-over-abstraction.md) | Verify first-party abstractions have concrete consumers | `anti-over-abstraction` |
| ☐ | [34.11](./phase-34-rust-anti-patterns/pr-34.11-anti-premature-optimize.md) | Record PR 31.11 as the owner of profile-before-optimize policy | `anti-premature-optimize` |
| ☐ | [34.12](./phase-34-rust-anti-patterns/pr-34.12-anti-type-erasure.md) | Record PR 19.5 as the owner of dynamic-dispatch decisions | `anti-type-erasure` |
| ☐ | [34.13](./phase-34-rust-anti-patterns/pr-34.13-anti-format-hot-path.md) | Record why profiled hot paths retain required formatting | `anti-format-hot-path` |
| ☐ | [34.14](./phase-34-rust-anti-patterns/pr-34.14-anti-collect-intermediate.md) | Record PR 31.3 as the owner of intermediate-collection audits | `anti-collect-intermediate` |
| ☐ | [34.15](./phase-34-rust-anti-patterns/pr-34.15-anti-stringly-typed.md) | Record PR 18.5 as the owner of typed domain-state decisions | `anti-stringly-typed` |

---

## CI grows with the phases

CI is added in PR 0.1 and every "green" from then on runs through it. New gates switch on as the code
that needs them lands:

| From PR | New CI gate |
|---|---|
| Roadmap | `next_task.py --validate-all --require-tracked` checks the index, rule coverage, dependencies, audited task contracts, links, and verification commands before any task can be selected |
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
| 7.7 | `clippy` denies `unwrap_used` + `expect_used` in production (a `clippy.toml` re-allows both under `#[cfg(test)]`/`#[test]`; benches, integration test files, and the e2e harness lib carry a file-level allow) |

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
