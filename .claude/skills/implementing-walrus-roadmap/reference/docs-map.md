# Design docs map

Which canonical doc to read for a given task, plus each doc's section index so you can jump straight
to the section a task's **Read first** cites instead of re-reading the whole doc. Read by the
implementer subagent — never by the orchestrator, and never in full: a task names its sections, and
this map turns those names into locations.

## Contents
- When to consult which doc
- architecture.md — section index
- proto-version.md — section index
- walrus-loader.md — section index
- walrus-pg-sink.md — section index
- Phases 9+ — the rust-skills rule files

## When to consult which doc

| You're working on… | Primary doc | Also |
|---|---|---|
| Workspace layout, phases, delivery semantics, verification plan | `architecture.md` | `docs/implementation/README.md` |
| pgoutput wire format: framing, messages, streaming, xid, abort/rollback | `proto-version.md` | golden vectors in `docs/examples/proto-version/` |
| The sink: type conversion, DDL capture, sink pod lifecycle | `walrus-pg-sink.md` | `architecture.md` §1 |
| The loader: manifest queue, commit-gating, raw→mirror transform, guards, lifecycle | `walrus-loader.md` | `architecture.md` §2 |
| A phase 9–34 Rust-rule task | the rule file the task cites under `.claude/skills/rust-skills/rules/` | the task's exact source paths/symbols and verification commands |

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

## Phases 9+ — the rust-skills rule files

Phases 9–34 audit the finished tree against `.claude/skills/rust-skills` (265 rules across 26
categories), with exactly one task per rule. A phase-9+ task cites its rule by filename, e.g.
`.claude/skills/rust-skills/rules/own-borrow-over-clone.md`. Read **the cited rule plus the exact
source paths/symbols the task names** — line numbers are orientation only; do not read the whole
rule set or unrelated rules in the category.

| Phase | Directory | Rule family |
|---|---|---|
| 9 | `phase-9-rust-ownership/` | `own-*` — borrow over clone, slices over `Vec`, `Cow`, `Arc`/`Rc`, interior mutability |
| 10 | `phase-10-rust-errors/` | `err-*` — `From` impls, `?`, source chains, thiserror/anyhow split, no unwrap in production |
| 11 | `phase-11-rust-memory/` | `mem-*` — `with_capacity`, `clone_from`, `take`/`replace`, boxed slices, zero-copy |
| 12 | `phase-12-rust-unsafe/` | `unsafe-*` — safety comments, minimal scope, extern blocks, Miri in CI |
| 13 | `phase-13-rust-api-design/` | `api-*` — `Default` impls and the rest of the API-guideline surface |
| 14 | `phase-14-rust-async/` | `async-*` — `select!` racing, cancel safety, structured `JoinSet` |
| 15 | `phase-15-rust-concurrency/` | `conc-*` — atomic ordering, thread-locals |
| 16 | `phase-16-rust-codegen-opt/` | `opt-*` — codegen and branch hints |
| 17 | `phase-17-rust-numeric/` | `num-*` — overflow, casts, floats, and numeric wrappers |
| 18 | `phase-18-rust-type-safety/` | `type-*` — newtypes, invariants, and state modeling |
| 19 | `phase-19-rust-traits/` | `trait-*` — associated types, coherence, object safety, and dispatch |
| 20 | `phase-20-rust-conversions/` | `conv-*` — `TryFrom`, `FromStr`, `AsRef`, and mutable conversions |
| 21 | `phase-21-rust-const/` | `const-*` — const functions, blocks, generics, and statics |
| 22 | `phase-22-rust-serde/` | `serde-*` — wire defaults, validation, enums, and compatibility |
| 23 | `phase-23-rust-patterns/` | `pat-*` — matching, destructuring, and let-else idioms |
| 24 | `phase-24-rust-macros/` | `macro-*` — function-first design, hygiene, fragments, and proc macros |
| 25 | `phase-25-rust-closures/` | `closure-*` — capture, `Fn` bounds, returned and dynamic closures |
| 26 | `phase-26-rust-collections/` | `coll-*` — map, set, sequence, heap, and membership choices |
| 27 | `phase-27-rust-naming/` | `name-*` — API naming, conversions, iterators, and accessors |
| 28 | `phase-28-rust-testing/` | `test-*` — module layout, fixtures, properties, snapshots, and doctests |
| 29 | `phase-29-rust-documentation/` | `doc-*` — public docs, examples, links, metadata, and README coverage |
| 30 | `phase-30-rust-observability/` | `obs-*` — tracing, fields, spans, metrics, and secret handling |
| 31 | `phase-31-rust-performance/` | `perf-*` — profiling-led iteration, allocation, I/O, and benchmarks |
| 32 | `phase-32-rust-project-structure/` | `proj-*` — crate layout, feature hygiene, editions, and build scripts |
| 33 | `phase-33-rust-linting/` | `lint-*` — lint levels, workspace policy, CI, and dependency checks |
| 34 | `phase-34-rust-anti-patterns/` | `anti-*` — residual audits for common correctness/design traps |

The mapping is fixed and validated: directory `phase-N-rust-<topic>/`, task file
`pr-N.k-<rule-name>.md`, and rule `.claude/skills/rust-skills/rules/<rule-name>.md`.

The task—not a blanket curriculum assumption—is the contract. Most adjustments preserve runtime
behaviour; edition, public-compatibility, documentation, metadata, and evidence outcomes use the
task's authored acceptance criteria. In every case, follow the cited rule, named paths/symbols,
predetermined outcome, and exact verification commands.
