# Design docs map

Which of the four canonical design docs to read for a given task, plus each doc's section index so
you can jump straight to the section a task's **Read first** cites instead of re-reading the whole
doc. All four total ~4,100 lines — read them in full once per run, then navigate by section.

## Contents
- When to consult which doc
- architecture.md — section index
- proto-version.md — section index
- walrus-loader.md — section index
- walrus-pg-sink.md — section index

## When to consult which doc

| You're working on… | Primary doc | Also |
|---|---|---|
| Workspace layout, phases, delivery semantics, verification plan | `architecture.md` | `docs/implementation/README.md` |
| pgoutput wire format: framing, messages, streaming, xid, abort/rollback | `proto-version.md` | golden vectors in `docs/examples/proto-version/` |
| The sink: type conversion, DDL capture, sink pod lifecycle | `walrus-pg-sink.md` | `architecture.md` §1 |
| The loader: manifest queue, commit-gating, raw→mirror transform, guards, lifecycle | `walrus-loader.md` | `architecture.md` §2 |

The deep-dive docs **extend and sometimes correct** `architecture.md`. When they disagree, the
component doc (`walrus-pg-sink.md` / `walrus-loader.md` / `proto-version.md`) wins for its own area.

## architecture.md — section index
- Context; Goals / Non-goals; High-level architecture
- Component 1 — Postgres Sink (walrus-pg-sink)
  - 1.1 Source-side setup · 1.2 Replication consumer · 1.3 In-memory batching & cadence
  - 1.4 Arrow conversion & Parquet write · 1.5 Durability checkpoint (WAL-bounding invariant)
  - 1.6 Large-transaction safety · 1.7 Snapshot / backfill · 1.8 Single slot for life
  - 1.9 Slot liveness / heartbeat / keepalive
- Component 2 — Data Sink (walrus-loader) · 2.1 raw→mirror transform model
- Delivery semantics; DDL taxonomy; Verification; Open questions; Deferred goals
- Proposed Rust workspace layout (~line 1440); Phased roadmap (~line 1470)

## proto-version.md — section index
- TL;DR (five load-bearing facts)
- 1 What proto_version is · 2 Version matrix · 3 test_decoding vs pgoutput
- 4 Message catalog decoded byte-by-byte · 5 TupleData + unchanged-TOAST placeholder
- 6 REPLICA IDENTITY · 7 Per-message xid (v2+)
- 8 Streaming: chopping a big txn · 9 Abort/rollback (the mirror-corruption case) · 10 Interleaving & commit order
- 11 Protocol axis side by side · 12 Two-phase (v3) & parallel apply (v4)
- 13 Consumer contract for walrus · 14 Reproduce it yourself

## walrus-loader.md — section index
- 1 Mission recap · 2 Work-handoff contract (file_manifest as queue) · 3 Commit-gating
- 4 Two-phase apply (append then transform) · 5 raw→mirror transform in depth
- 6 Intra-batch PK churn (insert→delete→insert) · 7 Straddling the watermark (per-PK max-applied-LSN guard)
- 8 Kubernetes pod lifecycle · 9 Performance & scaling · 10 What it extends in architecture.md

## walrus-pg-sink.md — section index
- 1 Mission recap
- 2 Data-type conversion (Postgres → Arrow → Parquet → DuckDB): tiers, full type table, interval/
  range/timetz decompositions, canonical-text carriers, descriptors, round-trip tests
- 3 DDL capture (event triggers → audit table → sink consumption; limitations)
- 4 Kubernetes pod lifecycle (startup, probes, steady state, graceful drain, decommission)
- 5 What it supersedes in architecture.md
