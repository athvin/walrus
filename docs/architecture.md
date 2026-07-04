# Walrus — Postgres WAL → DuckDB Replication Service (Architecture Sketch)

> **Status: design sketch for review.** This is a thorough, cited design intended to be read
> and critiqued before any code is written. Findings are grounded in a deep-research pass
> (24 sources fetched, 25 claims adversarially verified with 3-vote review, 23 confirmed).
> Inline `[n]` markers reference the **[Sources](#sources)** section. Where research could
> *not* confirm something, it is explicitly flagged as **⚠ unverified / spike needed** rather
> than asserted.

> **North star — self-healing on Kubernetes; eventual, not real-time.** The end goal is a service
> that **runs on Kubernetes and keeps working through pod churn**: if a pod is rescheduled or
> evicted, or a node is drained, the system **recovers on its own and resumes exactly where it
> left off** — no data loss, no manual babysitting — and you **scale it by adding pods**, not by
> re-architecting. That resilience is not bolted on: it falls out of durable checkpoints (the
> slot's `confirmed_flush_lsn`, the manifest work-queue, and the loader's two watermarks) plus
> fail-fast bootstrap, so *every* restart is just a resume. Delivery is **eventually consistent on
> a tunable latency budget** — data arrives in a *reasonable* time and arrives *sooner* when you
> loosen the cadence / memory-footprint / record-count knobs — but it is deliberately **not a
> real-time system**. When correctness and speed conflict, walrus picks correctness.

---

## Context

**Problem.** We want to continuously replicate a Postgres database into DuckDB for
analytics, on Kubernetes, in the cloud. Postgres logical replication (the WAL / pgoutput
stream) is the right change-data-capture (CDC) source, but it has three hard edges that
shape the whole design:

1. **Logical decoding emits only DML** (Insert/Update/Delete + transaction/metadata
   messages) — it **never emits DDL** [3][5]. Schema changes must be captured out-of-band.
2. **The replication slot retains WAL** until the consumer confirms it has durably
   persisted those changes. If the consumer is slow or stalls, WAL grows without bound and
   can fill the source disk [12][13]. Reading fast and checkpointing correctly is the
   central operational concern — exactly the "don't let the WAL build up" requirement.
3. **Postgres has a huge type system**; DuckDB and Parquet have their own. We need a single,
   well-defined translation layer. Apache Arrow is the right intermediate representation.

**The CDC transport everything is built on.** Walrus reads every change over **one**
logical-replication stream, negotiated per connection at **`proto_version '2'` with
`streaming 'on'`** — this pair *is* the transport contract, and it is **mandatory, not an
optional mode**. **Why it's non-negotiable:** `streaming 'on'` is the *only* mechanism Postgres
gives us to receive a multi-million-row transaction **incrementally, before it commits**, instead
of buffering the whole thing first. Without it, one large write OOMs the sink (or backs the slot
up until the WAL runs away) — and surviving exactly those large transactions is the reason this
tool exists. If we can't stream them, walrus doesn't work. (The version-by-version rationale and
the concrete pitfalls of running *without* streaming are laid out in
[§1.1](#11-source-side-setup-one-time-via-migrationjob).) The stream is decoded from Postgres's
built-in **pgoutput** plugin, so to be
unmistakable about the layering: **pgoutput is the output plugin (the on-the-wire message format
we decode), while `proto_version '2'` and `streaming 'on'` are the `START_REPLICATION` options
we run it at — they are settings *of* pgoutput, negotiated per connection, not an alternative
to it.** All CDC flows
through this single stream: `streaming 'on'` lets the server hand us the largest **in-progress**
transactions incrementally (before commit) so a 50-million-row transaction never has to be
buffered whole, while smaller transactions still arrive complete at commit — same stream, same
decoder, same Arrow → Parquet → S3 path [3][4]. The sink is, first and foremost, a fast, correct
consumer of this stream, and Component 1 is organized around that.

**Desired outcome.** Two cleanly separated Rust services on Kubernetes:

- a **Postgres Sink** (`walrus-pg-sink`) that consumes that `proto_version 2` streaming pgoutput
  stream in memory, batches changes,
  converts them to Arrow → Parquet, dumps files to S3, and records each file's location +
  LSN range in a Postgres control table — advancing the slot only after that is durable; and
- a **Data Sink** (`walrus-loader`) that polls the control table on a user-chosen cadence,
  pulls Parquet from S3, **appends each change verbatim into a `<table>_raw` CDC log**, then
  **transforms that log into `<table>`** — the current-state mirror. One `.duckdb` file per
  source table still holds; it now contains **two tables** (the raw log + the derived mirror).

**Two non-negotiable missions, cleanly split.** The sink's one job is **speed and safety off the
WAL**: take work off the WAL and land it in storage on the operator's terms (cadence / memory
footprint / record count), advancing the slot only after durability — nothing more. The loader's
one job is **accuracy**: reconcile that staged work back into the *exact shape the data has in
Postgres*. This is **near-real-time, not real-time** — the user picks the cadence, and for the
loader **real-time is explicitly not the priority; matching source truth is.** The goal is a
fast, backpressure-safe pipeline that never lets the source WAL run away while the loader catches
up, correctly, on its own schedule.

### Decisions locked in (from requirements)

| Decision | Choice | Consequence for the design |
|---|---|---|
| CDC transport | **One pgoutput stream at `proto_version '2'` + `streaming 'on'`** | Every change flows over this single stream — the foundation of the design. Negotiated per-connection on `START_REPLICATION` (not a slot property); large in-progress txns stream incrementally so they never buffer whole, small txns still arrive at commit ([§1.1](#11-source-side-setup-one-time-via-migrationjob), [§1.6](#16-large-transaction-safety)). The consumer is **hand-rolled** — no framework owns slot management ([§1.2](#12-replication-consumer--hand-rolled)). |
| DuckDB layout | **One `.duckdb` file per source table** — holding **two tables**: `<table>_raw` (append-only CDC log) + `<table>` (derived mirror) | Per-table single-writer loaders → natural parallelism & isolation; each file carries **two watermarks** (`raw_appended_lsn` ≥ `transformed_lsn`, both **commit-LSN** valued); cross-table point-in-time consistency is *relaxed*. |
| Target semantics | **Current-state mirror (upsert/delete)** | The `<table>` mirror is the current state; the loader **first appends CDC rows verbatim to `<table>_raw`** (no dedup, meta retained), **then derives `<table>`** by dedup-to-latest + `MERGE INTO` on the (possibly composite) PK. Makes PK metadata, `REPLICA IDENTITY`, TOAST handling, and the DDL-audit table load-bearing. |
| Transform primitive | **`MERGE INTO` (DuckDB ≥ 1.4.0 LTS)** | Single-statement insert/update/delete from `<table>_raw` → `<table>`; incremental-from-watermark by default, periodic full-rebuild (`CREATE OR REPLACE … AS SELECT`) for self-heal + compaction. `INSERT … ON CONFLICT` + `DELETE` is the < 1.4.0 fallback. |
| Raw retention | **Append-only, retained behind a rolling window** | `<table>_raw` keeps history behind `transformed_lsn` (default ~7 days / last-K batches) for debug + cheap replay; pruned + compacted below the floor. Not prune-immediately-after-transform. |
| History-table key | **`<table>_raw` PK = source PK + `sink_processed_at` + `lsn`** | The source PK repeats across CDC events, so the history table needs its own composite key; `lsn` guarantees uniqueness and enables `ON CONFLICT DO NOTHING` idempotent appends. |
| Bootstrap | **Consistent snapshot → then stream** | Backfill runs under the slot's **exported snapshot** (`CREATE_REPLICATION_SLOT … SNAPSHOT 'export'` → `snapshot_name` + `consistent_point`), *not* a "COPY at an LSN"; a watermark handoff dedups streamed changes against the snapshot ([§1.7](#17-snapshot--backfill-bootstrap)). |
| Primary key | **Mandatory** | Keyless tables are rejected at preflight — no `REPLICA IDENTITY FULL`, no full reloads ([§1.1](#11-source-side-setup-one-time-via-migrationjob)). |
| Replication slot | **Exactly one slot, for the system's life** | Sink is a single-slot / single-consumer service; if the slot is deleted/invalidated the system enters **total-restart** ([§1.8](#18-single-slot-for-life--total-restart)). |
| DDL: comments | **Mirror `COMMENT ON` onto the `<table>` mirror** | A metadata revision, not a structural bump; **not** applied to `<table>_raw` (a CDC log has no source-comment semantics) ([DDL handling](#per-change-type-handling-schema-evolution-semantics)). |
| DDL: drop column | **Physically drop on the `<table>` mirror** | True current-state mirror of the source shape. `<table>_raw` **retains** the column (nullable) to preserve verbatim CDC history; post-drop files fill it NULL. |
| DDL: type change | **Apply in place on the `<table>` mirror; on cast failure → quarantine + alert (terminal)** | **Single-table reloads are out of scope** — a failed cast is *accepted, non-recovered*. `<table>_raw` **never casts history**: lossless widening applies the same `ALTER`; incompatible changes **widen the raw column to `VARCHAR`** so all schema_versions coexist. |
| Metadata timestamps | **UTC only (RFC 3339 / ISO-8601, `Z`)** | Every datetime walrus records as metadata — `commit_ts`, `sink_processed_at`, control-table `*_at`, checkpoints — is UTC; never a local or source-server offset. |

---

## Goals / Non-goals

**Goals**
- Correct, ordered, effectively-once replication of DML into per-table **raw CDC logs + derived DuckDB mirrors**.
- Bounded WAL growth under all conditions via correct slot-feedback checkpointing **and a
  heartbeat** (so an idle publication over a busy database can't pin WAL — [§1.9](#19-slot-liveness--heartbeat--keepalive)).
- **Consume all CDC over one pgoutput stream** at `proto_version '2'` + `streaming 'on'` (the
  transport the whole sink is built on), and thereby handle **large transactions** without
  buffering them entirely in memory [3][4].
- Capture **DDL / schema evolution** and apply it to the DuckDB targets.
- **User-configurable cadence** for both flush (sink) and apply (loader). The loader apply is **two operations** — append-to-raw then transform-to-mirror — sharing one poll cadence in v1 (separate transactions, two watermarks), with a slower per-table **compaction/full-rebuild** cadence exposed separately.
- **Eventual delivery on a tunable latency budget** — data reaches DuckDB in a *reasonable* time and *sooner* when the operator loosens the cadence / memory-footprint / record-count knobs. Explicitly **not** real-time; correctness is never traded for latency.
- **Self-healing on Kubernetes** — the system survives pod rescheduling / eviction / node drain and **resumes exactly where it left off** with no data loss and no manual intervention (durable checkpoints + fail-fast bootstrap make every restart a resume). "**Scale by adding pods and it just works.**"
- Cloud-native: S3 staging, Kubernetes deployment, horizontal-ish scaling by table sharding.

**Non-goals (v1)**
- Real-time / sub-second latency.
- Bi-directional or multi-master replication.
- Cross-table transactional/point-in-time consistency in the target (relaxed by the one-file-per-table choice).
- Replicating non-table objects (functions, sequences-as-truth, roles). Sequences/`TRUNCATE` are in-scope as metadata only.
- **Leaf-partition-level replication of partitioned tables.** Partitioned tables are supported
  **only** with the publication's **`publish_via_partition_root = true`**, so changes arrive under
  the *root* table name and map to one DuckDB file. Without it, pgoutput reports changes under
  **leaf-partition** names (which v1 does not map to targets); preflight asserts the setting
  ([§1.1](#11-source-side-setup-one-time-via-migrationjob)).
- **Tables without a `PRIMARY KEY`.** A PK is a hard prerequisite; keyless tables are rejected at preflight, never full-reloaded.
- **Single-table reloads / re-syncs.** There is **no per-table recovery path**. If a lossy `ALTER COLUMN TYPE` cast fails, the table is quarantined + alerted and left as-is — an accepted terminal outcome, explicitly not solved in v1. (The only full re-sync that exists is the whole-system [total-restart](#18-single-slot-for-life--total-restart) on slot loss, which rebuilds *every* table — never just one.)

---

## High-level architecture

```
   SOURCE POSTGRES (wal_level=logical)
   ┌───────────────────────────────────────────────┐
   │ publication  +  logical replication slot (v2)  │
   │ walrus.ddl_audit  ←  event trigger (DDL)        │
   │ user tables ...                                 │
   └───────────────┬─────────────────────────────────┘
                   │ pgoutput v2 stream, streaming='on' (DML + inline DDL-audit INSERTs)
                   ▼
   ┌───────────────────────────────────────────────┐        CONTROL POSTGRES
   │  POSTGRES SINK  (walrus-pg-sink)               │        ┌────────────────┐
   │  StatefulSet, exactly 1 active consumer / slot │        │ file_manifest  │
   │                                                │        │ ddl_manifest   │
   │  1. stream WAL in memory, micro-batch          │  write │ loader_ckpt    │
   │  2. Postgres row → Arrow RecordBatch           │───────▶│ schema_registry│
   │  3. Arrow → Parquet (+compression)             │        └───────┬────────┘
   │  4. PUT Parquet to S3 (object_store)           │                │ poll (cadence)
   │  5. INSERT manifest row (s3_uri, lsn range)    │                ▼
   │  6. advance confirmed_flush_lsn  ONLY after    │        ┌──────────────────────────┐
   │     S3 + manifest are durable  ◄── backpressure │        │ DATA SINK (walrus-loader)│
   └───────────────┬───────────────────────────────┘        │ 1 worker per table (owns  │
                   │ PUT                                     │        the .duckdb file)   │
                   ▼                                         │ 1. read manifest > wm     │
        ┌────────────────────┐   GET   ◄────────────────────│ 2. GET Parquet from S3    │
        │  S3 / object store │─────────────────────────────▶│ 3. APPEND verbatim→_raw   │
        │ <epoch>/<tbl>/*.pq │                              │ 4. dedup+MERGE _raw→tbl   │
        └────────────────────┘                              │ 5. apply DDL @ its LSN    │
                                                            │ 6. advance raw+xform wm   │
                                                            └───────────┬──────────────┘
                                                                        ▼
                                              ┌───────────────────────────────────────┐
                                              │ per-table DuckDB files (PVC-backed)     │
                                              │  each file = <tbl> + <tbl>_raw tables   │
                                              └───────────────────────────────────────┘
```

**Why S3 in the middle?** It decouples the two services completely. The sink's only job is
to drain the WAL fast and durably; the loader's only job is to catch up on its own cadence.
The control-table manifest is the hand-off contract. Neither service blocks the other, and
either can crash/restart independently without data loss (see [Delivery semantics](#delivery-semantics-ordering--idempotency)).

---

## Component 1 — Postgres Sink (`walrus-pg-sink`)

> **Non-negotiable mission: drain the WAL to storage, fast, so the slot can never run away.**
> `walrus-pg-sink` exists to **take work off the WAL and write it to durable storage** (Arrow →
> Parquet → S3 + a manifest row), on the schedule the operator dictates — flushing when **any**
> configured limit trips: the **cadence** (`max_fill_ms`), the **memory footprint**
> (`max_bytes` / memory backpressure), or the **record count** (`max_rows`). It advances the
> replication slot **only after** that write is durable, so the source WAL is bounded by
> construction. That is its whole job. It does **not** interpret, dedup, reconcile, or model the
> data into its final shape — it moves change events off the WAL and onto S3 verbatim, quickly
> and safely. Everything downstream is `walrus-loader`'s concern.

The WAL reader. This is the latency- and throughput-critical service.

### 1.1 Source-side setup (one-time, via migration/Job)

```sql
-- Enable on source: wal_level=logical, sufficient max_replication_slots / max_wal_senders.
CREATE PUBLICATION walrus_pub FOR TABLE orders, customers, ...   -- or FOR ALL TABLES
  WITH (publish_via_partition_root = true);   -- partitioned tables replicate as their ROOT (see Non-goals)
--   Must ALSO publish walrus.ddl_audit (so DDL flows inline, see DDL capture) and
--   walrus.heartbeat (so the slot advances while user tables are idle, see §1.9).

-- The slot is created by walrus-pg-sink over a REPLICATION connection — NOT by this migration —
-- because a consistent backfill needs the slot's EXPORTED SNAPSHOT, which the SQL helper
-- pg_create_logical_replication_slot() does NOT give you (it returns only (slot_name, lsn)):
--
--   -- on a replication-mode connection, kept OPEN and idle until backfill has attached:
--   CREATE_REPLICATION_SLOT walrus_slot LOGICAL pgoutput (SNAPSHOT 'export');
--   -- ^ returns (slot_name, consistent_point LSN, snapshot_name). A PLAIN pgoutput slot:
--   --   proto_version / streaming are NOT slot properties and cannot be set here — they are
--   --   per-connection options on START_REPLICATION (below), re-negotiated on every connect.
--   --   The same slot can be consumed with or without streaming depending on those options.
--   -- The initial COPY then runs in a REPEATABLE READ read-only txn under
--   --   SET TRANSACTION SNAPSHOT '<snapshot_name>' (§1.7). There is NO "COPY at an LSN".

-- HARD REQUIREMENT: every replicated table MUST have a PRIMARY KEY.
--   PK present  → REPLICA IDENTITY DEFAULT (the PK) gives UPDATE/DELETE their key. ✅
--   No PK       → REJECTED at preflight; the table is NOT added to the publication.
--                 We do not support keyless tables — the only correct alternative is
--                 REPLICA IDENTITY FULL + recurring full reloads, which is out of scope.

-- REQUIRED: build the DDL-capture trigger on the SOURCE database. Logical decoding NEVER emits
--   DDL [3][5], so this trigger is the ONLY way walrus learns a schema change happened. Full DDL
--   is in "DDL capture" below (AWS DMS awsdms_ddl_audit pattern [6]); the shape is:
--     CREATE TABLE walrus.ddl_audit (...);                     -- fixed-schema audit table
--     CREATE EVENT TRIGGER walrus_intercept_ddl  ON ddl_command_end EXECUTE FUNCTION walrus.intercept_ddl();
--     CREATE EVENT TRIGGER walrus_intercept_drop ON sql_drop        EXECUTE FUNCTION walrus.intercept_drop();
--   walrus.ddl_audit MUST be in walrus_pub (above) so its INSERTs ride the SAME slot, in commit
--   order with the DML they describe — no separate DDL channel, no ordering guesswork.
```

- **Primary key is mandatory (hard edge).** Before a table is added to the publication,
  `walrus-pg-sink` runs a **preflight check**: the table must have a `PRIMARY KEY` and a usable
  replica identity (`DEFAULT` on the PK — i.e. not `REPLICA IDENTITY NOTHING`). A table without
  a PK is **rejected** — logged, surfaced as a metric, and left out of replication entirely.
  Rationale: current-state MERGE needs a stable key for targeted upsert/delete; without one the
  only correct behavior is `REPLICA IDENTITY FULL` + periodic full reloads, which we
  deliberately **do not support**. If a `PRIMARY KEY` is later dropped (observed via the DDL
  audit stream), the sink **quarantines** that table and alerts rather than silently degrading.
  The PK may be **composite** (multiple columns) — every key column is captured from
  `schema_registry` and used *together* as the merge/dedup key throughout (partition, `MERGE … ON`,
  delete predicate); nothing in the design assumes a single-column key.
  *(A `UNIQUE NOT NULL` column via `REPLICA IDENTITY USING INDEX` could relax this later; out of
  scope for v1 by design.)*

- **Protocol version & streaming — walrus's CDC transport.** Every change walrus replicates
  arrives over this one stream. The replication *slot* is a plain `pgoutput` logical slot — the
  protocol version is **not** baked into it (there is **no special slot *type*** for large
  transactions) — so the transport is negotiated **per connection** via options on
  `START_REPLICATION`:

  ```sql
  START_REPLICATION SLOT walrus_slot LOGICAL 0/0 (
      proto_version '2',              -- pgoutput logical-replication protocol version (min for streaming)
      streaming 'on',                 -- what ACTUALLY streams in-progress transactions
      publication_names 'walrus_pub'
  )
  ```

  **Both knobs are required and distinct.** `proto_version '2'` (server 14+) is the *minimum*
  protocol that *permits* streaming, but with `streaming 'off'` (the default) each transaction
  is still buffered until commit. **`streaming 'on'`** is what actually streams large
  in-progress transactions, and it in turn *requires* `proto_version >= 2` [3][4]. So
  `proto_version '2'` **alone does not** give you the large-transaction behavior — you need the
  pair (**why `v2` is the target, what each version adds, and the pitfalls of running *without*
  streaming are spelled out just below**). Because `proto_version` is a connection option (not a
  slot property), you can change it by reconnecting — no slot recreation — so make both
  configurable.

#### Why `proto_version '2'` + `streaming 'on'` (and the pitfalls it avoids)

The pair isn't a tuning preference — it's forced by how logical decoding behaves **without**
streaming. This is the single most consequential protocol decision in the design, so the reasoning
is made explicit here rather than left implied.

**What each protocol version adds** (a per-connection `pgoutput` option, never a slot property [21]):

| `proto_version` | Server | What it adds |
|---|---|---|
| `1` | PG 10+ | Baseline logical-replication messages (`Begin`/`Commit`, `Relation`, `Insert`/`Update`/`Delete`, `Truncate`). **No streaming** — every transaction is buffered until commit. |
| **`2`** | **PG 14+** | **Streaming of in-progress (large) transactions** — `Stream Start`/`Stop`/`Commit`/`Abort` frames plus an `xid` on every change message. **← walrus's target.** |
| `3` | PG 15+ | Streaming of **two-phase-commit** (prepared) transactions. |
| `4` | PG 16+ | **Parallel apply** of a single large streamed transaction on the subscriber. |

We target **`v2`**: it is the *minimum* version that supports streaming, and v3/v4 add nothing we
use — walrus stages to files and reconciles downstream, so two-phase-commit streaming and
subscriber-side parallel apply are irrelevant [3][4].

**The pitfall of *not* streaming** (`streaming 'off'`, the default — the failure walrus must
avoid). With streaming off, the walsender **cannot emit a single row of a transaction until it
decodes that transaction's `COMMIT`**. To hold everything until then it accumulates the
transaction's *entire* decoded change-set in the **reorder buffer** — in memory up to
`logical_decoding_work_mem`, then **spilled to disk** under `pg_replslot/` once that limit is
crossed [14][19]. For the multi-million-row transactions walrus exists to survive, that produces
four distinct failures:

- **Memory *and* disk pressure on the source primary** — borne by the database we replicate
  *from*, not by our sink. One large transaction can spill gigabytes to the primary's disk before
  a single change reaches us [14].
- **WAL runaway → slot loss.** The slot's `restart_lsn` / `confirmed_flush_lsn` cannot advance
  while a large transaction is open and undelivered, so retained WAL grows for its entire duration.
  A long-enough transaction breaches `max_slot_wal_keep_size`, invalidates the slot
  (`wal_status = 'lost'`), and triggers the
  [total-restart](#18-single-slot-for-life--total-restart) disaster [12][13][14].
- **A latency cliff.** The consumer receives *nothing* for the life of the transaction, then a
  single flood at commit — the opposite of the steady, backpressure-safe drain the sink is built
  around.
- **Sink OOM.** Even if the primary survives, walrus would then have to buffer that whole
  transaction in memory to convert it to Arrow → Parquet.

Every one of these breaks the tool's core promise, and all four are provoked by exactly the
workload — large writes — that walrus exists for. That is why the transport is **mandatory, not an
optional mode** [14].

**How `v2` + `streaming 'on'` removes the pitfall.** Once the *total* decoded changes across all
in-progress transactions exceed `logical_decoding_work_mem`, the server streams the **largest
in-progress top-level transaction to the consumer *before* it commits** — bounding server-side
buffering and letting walrus drain it to S3 incrementally instead of waiting for commit. Small
transactions still arrive whole at commit. The streaming mechanics (per-`xid` demultiplexing,
`Stream Commit` / `Stream Abort`) are covered in the next bullet and
[§1.6](#16-large-transaction-safety) [3][19].

**Trade-offs we knowingly accept.** Streaming shifts work onto the *consumer*: walrus must
demultiplex interleaved per-`xid` segments, keep streamed rows invisible until `Stream Commit`,
and discard the rows of aborted top-level txns (`Stream Abort`) and of rolled-back subtransactions.
Those rules — and why they keep us correct — live in [§1.6](#16-large-transaction-safety). We take
on that consumer-side complexity deliberately: it is the price of never letting a large transaction
threaten the source.
- **Large-transaction handling (what actually streams).** `streaming 'on'` does **not** chop
  every transaction into pre-commit pieces. Streaming kicks in only when the **total** decoded
  changes across **all** in-progress transactions exceed `logical_decoding_work_mem`; at that
  point the server selects the **single largest top-level transaction** and streams *it*,
  bracketed between `Stream Start` / `Stream Stop`, with the final segment carrying `Stream
  Commit` or `Stream Abort` [3][4]. Small transactions are still delivered **whole, only at
  commit**, even with streaming on (the `Stream Abort` message is the proof changes are sent
  before the fate is decided). Every `Insert/Update/Delete/Relation/Truncate` carries its
  **`xid`** (a v2+ field) and each `Stream Start` carries the xid + a first-segment flag, because
  a large txn's segments are **non-contiguous and interleaved** with other in-progress txns on the
  wire — the consumer demultiplexes per `xid`. This is what stops a 50M-row transaction from
  ballooning slot/WAL memory — see [1.6](#16-large-transaction-safety).
- **DDL-capture trigger is a mandatory source-side prerequisite.** Because Postgres logical
  decoding **never emits DDL** [3][5], the source database **must** have the `walrus.ddl_audit`
  table plus the `ddl_command_end` and `sql_drop` **event triggers** installed (the AWS DMS
  `awsdms_ddl_audit` pattern [6]) — this is *the* mechanism the replication tool uses to know a
  schema change happened and to apply `ADD/DROP/RENAME/TYPE` to the DuckDB targets at the correct
  LSN. It is **not optional**: with the trigger absent, walrus would silently miss every schema
  change and drift from the source. The elegance is that `walrus.ddl_audit` is itself a
  **published** table, so its `INSERT`s travel **inline through the same replication slot** as the
  DML — the sink sees "schema changed to X at LSN L" in commit order relative to the data, with no
  separate polling channel. It is installed once (migration/Job), verified at
  [bootstrap](#walrus-pg-sink-bootstrap) (missing → terminal), and its full DDL + the important
  caveat that **event triggers are not exhaustive** live in [DDL capture](#ddl-capture-schema-evolution).

### 1.2 Replication consumer — hand-rolled

We **hand-roll the replication consumer** rather than adopt a high-level framework. The linchpin
of "don't let the WAL build up" is advancing `confirmed_flush_lsn` **only after** our S3 +
manifest write is durable ([§1.5](#15-the-durability-checkpoint-wal-bounding-invariant)); that
coupling is the whole point of the sink, so we want it **directly under our control**, not
mediated by a framework's own slot-advance timing. Hand-rolling means we own — and can reason
about — every one of these:

- **The replication connection.** Open a replication-mode connection and issue
  `START_REPLICATION SLOT walrus_slot LOGICAL <lsn> (proto_version '2', streaming 'on', ...)` over
  a thin, replication-capable Postgres client (e.g. `tokio-postgres`'s replication support or the
  lean `pgwire-replication` crate [2]) — used only as the raw byte-stream / standby-status
  plumbing, **not** as a framework.
- **The pgoutput decoder.** Parse the pgoutput message model ourselves — `Begin`/`Commit`,
  `Relation`, `Type`, `Insert`/`Update`/`Delete`, `Truncate`, and the v2 streaming frames
  `Stream Start`/`Stop`/`Commit`/`Abort` — demultiplexing per `xid`
  ([§1.6](#16-large-transaction-safety)). film42's walkthrough is the reference pattern for
  exactly this [15].
- **Slot-feedback / `confirmed_flush_lsn`.** Emit standby status updates ourselves so the slot
  advances on **our** schedule (post-durability), and never past an open streamed txn.
- **Snapshot / initial copy** ([§1.7](#17-snapshot--backfill-bootstrap)), **in-memory batching +
  memory backpressure** ([§1.3](#13-in-memory-batching--cadence)), retries, and per-table
  parallelism — all ours.

We deliberately **do not** build on `supabase/etl` (formerly `pg_replicate`) [1]: it solves
initial-copy + streaming + batching + pluggable destinations, but it is an opinionated framework
that would own slot management for us — the one thing we most need explicit control over. We still
treat it (and its `BatchConfig` / `MemoryBackpressureConfig` shapes) as **prior art** to crib
batching and backpressure design from [1]. The build cost this buys is the **hand-rolled pgoutput
decoder + snapshot**, now the highest-effort area of Component 1 (see
[Open questions](#open-questions--risks)).

### 1.3 In-memory batching & cadence

The sink accumulates decoded changes in memory into per-(table, batch) Arrow builders and
flushes a Parquet file when **any** threshold trips — all user-configurable:

- `max_fill_ms` — wall-clock cadence (the primary "how often do we dump files" knob).
- `max_bytes` / `max_rows` — size caps so batches stay bounded (batching + memory-backpressure
  shape cribbed from etl's `BatchConfig` / `MemoryBackpressureConfig` as prior art [1]).
- transaction-commit boundary — never split a committed txn's tail across the visibility line (see [1.6](#16-large-transaction-safety)).

Larger cadence ⇒ larger batches ⇒ **more WAL retained between flushes** ⇒ better Parquet
compression but higher slot lag. This trade-off is the knob the user tunes.

### 1.4 Arrow conversion & Parquet write

- Decode pgoutput tuples → typed Rust values → **Arrow `RecordBatch`** using the
  [type mapping](#data-type-translation-postgres--arrow--parquet). Each output row = the
  **source columns** plus **one added column, `walrus_pg_sink_meta`** — a JSON document
  (Arrow `Utf8`) bunching *all* batch/row provenance so it can be extracted later:

  ```json
  {
    "op": "u",                       // i | u | d | t (truncate)
    "lsn": "00000000019A2B3C",       // per-row WAL LSN; zero-padded 16 hex, per-PK tiebreaker only
    "commit_lsn": "0000000001B4C000", // txn commit LSN; zero-padded 16 hex — THE order/watermark key
    "commit_ts": "2026-07-04T12:00:00Z",     // UTC, RFC 3339 / ISO-8601 (`Z`)
    "xid": 918273,
    "epoch": 7,
    "batch_id": "3f2a…-uuid",
    "schema_version": 12,
    "source_schema": "public",
    "source_table": "orders",
    "kind": "stream",                // snapshot | stream
    "unchanged_toast": ["blob_col"], // cols sent as unchanged-TOAST placeholders
    "sink_instance": "walrus-pg-sink-0",
    "sink_processed_at": "2026-07-04T12:00:00.123Z"  // UTC (`Z`)
  }
  ```
  **Everything lives in this one column** so nothing is lost for later lineage/debug. Both `lsn`
  (per-row WAL position) and `commit_lsn` (the transaction's commit position) are emitted
  **zero-padded / monotonic-comparable** so they order correctly as text. The loader
  **persists this column verbatim into `<table>_raw`** (the append-only CDC log) and, at append
  time, **promotes `op`, `commit_lsn`, `lsn`, and `sink_processed_at` (the sink time) to typed
  columns** there. **`commit_lsn` is the progress/order key** — the transform filters on it and
  the loader watermarks on it, because it is monotonic with commit/delivery order. **`lsn` (row)
  is only the per-PK last-writer tiebreaker** (safe because row-level locking serializes writes to
  a single PK, so per-PK row-LSN order equals commit order); together with the source PK and
  `sink_processed_at` it also forms the composite primary key of the raw history table (`lsn`
  guarantees uniqueness), see [§2.1](#21-the-raw-to-mirror-transform-model). The meta is **dropped
  from the derived `<table>` mirror by default** (it's provenance, not current state); it stays
  queryable in `<table>_raw` for the retention window (and in the staged Parquet under the S3
  lifecycle TTL). For deletes, only key columns are guaranteed populated in the source columns
  (identity), the rest null.
- **All datetimes walrus records as metadata are UTC.** `commit_ts`, `sink_processed_at`, and
  every `*_at` / timestamp column in the control tables and checkpoints are emitted and stored as
  **RFC 3339 / ISO-8601 with a `Z` suffix** (UTC) — never a local or source-server offset. This
  is a hard rule so LSN/commit ordering and cross-instance provenance are always comparable.
  (Source *data* columns are a separate concern, mapped per the
  [type table](#data-type-translation-postgres--arrow--parquet), where `timestamptz` is likewise
  normalized to UTC.)
- Write batches with **arrow-rs `parquet::arrow::ArrowWriter`** (or `AsyncArrowWriter`),
  which is generic over any `W: Write + Send`, buffers multiple `RecordBatch`es into one
  row group up to `max_row_group_row_count` / `max_row_group_bytes`, and flushes on
  `close()`; `WriterProperties` sets compression (e.g. Snappy/Zstd) [7][8].
- Stream the writer's output straight into an **S3 multipart upload** via the `object_store`
  crate (arrow-rs ecosystem) so files never fully materialize on local disk.
- **File key layout:** `s3://<bucket>/<epoch>/<source_schema>/<table>/<lsn_end>-<batch_uuid>.parquet` (epoch-namespaced — see [§1.8](#18-single-slot-for-life--total-restart)).

### 1.5 The durability checkpoint (WAL-bounding invariant)

This is the heart of "the WAL can't build up faster than we drain it, and we never lose data":

> **Invariant:** advance `confirmed_flush_lsn` to `lsn_end` of a batch **only after**
> (a) its Parquet is durable in S3 **and** (b) its `file_manifest` row is committed in the
> control DB.

Ordering per batch: **PUT to S3 → COMMIT manifest INSERT → send standby status update /
`update_applied_lsn(lsn_end)`** [2][13]. Consequences:

- A crash *before* the checkpoint just re-streams from the last confirmed LSN — at-least-once, no loss.
- Slot lag is bounded to **at most one in-flight batch** of WAL. If S3/manifest slows, the
  sink naturally exerts **backpressure** by not advancing the slot; the slot grows only up
  to the safety cap `max_slot_wal_keep_size` (beyond which Postgres invalidates the slot —
  alert *well* before that) [12][13].
- `restart_lsn` (WAL still needed to resume) vs `confirmed_flush_lsn` (consumer progress)
  are distinct — monitor both [13].
- **This durability gate advances only `confirmed_flush_lsn`.** The *keepalive* feedback that
  keeps the walsender connection alive (`wal_sender_timeout`) is a **separate, unconditional**
  standby status update and must not wait on S3/manifest durability — see
  [§1.9](#19-slot-liveness--heartbeat--keepalive). And when the published tables are idle, a
  **heartbeat** ([§1.9](#19-slot-liveness--heartbeat--keepalive)) is what gives this checkpoint a
  fresh LSN to confirm, so `restart_lsn` can still advance.

### 1.6 Large-transaction safety

With `streaming='on'`, we may receive changes for a **large** transaction **before its commit**
(only the largest in-progress txn over `logical_decoding_work_mem` is streamed; smaller txns still
arrive whole at commit) [3][4]. Rules to stay correct while bounding memory:

- **Demultiplex per `xid`.** Blocks for multiple in-progress transactions interleave on the wire,
  and some may abort. Key a speculative buffer by the `xid` carried on every
  `Insert/Update/Delete/Relation/Truncate` (v2+) and the `Stream Start` first-segment flag, so
  each transaction's non-contiguous segments are reassembled correctly [3].
- **Stage speculatively, commit-gate visibility.** Streamed sub-batches for an in-progress `xid`
  may be written to S3 (to keep memory bounded), but we **do not write the `ready` manifest row
  until we receive `Stream Commit`** for that txn. On **`Stream Abort`**, delete the speculative
  S3 files and drop the buffered state.
- **Never advance the slot past an open txn.** `confirmed_flush_lsn` must **not** move past the
  oldest still-open streamed transaction — hold it at that txn's begin / first-segment LSN until
  its `Stream Commit`, so a crash can always re-stream an incomplete-or-aborted txn from the WAL.
- **Discard rolled-back *sub*transactions, not just aborted top-level txns.** Under `streaming
  'on'` the changes of a **savepoint / subtransaction that later rolls back** are *still streamed*
  inside an otherwise-committed top-level txn; only the top-level `Stream Commit` reveals which
  subtransactions survived. Since we stage streamed rows to S3 **before** `Stream Commit`, the
  decoder must track subtxn structure (streamed subxact abort markers) and **exclude
  rolled-back-subxact rows when materializing the `ready` file** — a whole-txn `Stream Abort` is
  the coarse case; a surviving commit with dead subxacts is the subtle one. Never let a
  rolled-back subtransaction's rows reach `<table>_raw`.

This gives: memory bounded (we stream to disk/S3), WAL bounded (advance on commit), and
correctness — aborted/uncommitted txns never become visible to the loader, and changes are applied
in **commit order**, exactly as non-streaming mode guarantees.

### 1.7 Snapshot / backfill (bootstrap)

Per the "snapshot → then stream" choice:

1. Create the slot over a **replication connection** with
   `CREATE_REPLICATION_SLOT walrus_slot LOGICAL pgoutput (SNAPSHOT 'export')`, which returns the
   **`consistent_point` LSN** (the stream start position) **and a `snapshot_name`**. Keep that
   connection **open and idle** — the exported snapshot is valid only until another command runs
   on it or it closes, so **every backfill session must attach before then**. *(The SQL helper
   `pg_create_logical_replication_slot()` returns no `snapshot_name` and cannot be used for a
   consistent backfill — see [§1.1](#11-source-side-setup-one-time-via-migrationjob).)*
2. Export existing rows via our own **initial copy**: for each published table, open a
   `REPEATABLE READ` **read-only** transaction, `SET TRANSACTION SNAPSHOT '<snapshot_name>'`, and
   `COPY`/`SELECT` under that snapshot. **There is no "COPY at an LSN"** in Postgres — consistency
   comes from the exported MVCC snapshot, not a time-travel read. These rows flow through the
   *same* Arrow → Parquet → S3 → manifest path, marked as `snapshot` files with
   `lsn_end = consistent_point` — **all snapshot files share this one commit LSN**, so the loader
   disambiguates them by `manifest_id`, never by `lsn_end` alone (see the
   [coordination contract](#coordination-contract-control-plane-tables)).
3. **Watermark handoff:** snapshot files are **appended into `<table>_raw`** (marked
   `kind='snapshot'`) alongside streamed files, so raw may briefly hold both the snapshot tail
   and the stream head for the same PK. The **transform** collapses that overlap: dedup-to-latest
   by **`(commit_lsn, lsn)`** keeps the winning row (a streamed change has
   `commit_lsn > consistent_point`, so it beats the snapshot row for that PK), so the classic
   snapshot/stream boundary dedup falls out of the raw→mirror transform, not a direct MERGE of
   staged Parquet.

### 1.8 Single slot for life & total-restart

> **Design intent — open one slot, once, and never open another.** The system creates **exactly
> one** logical replication slot at first bootstrap and consumes it for the system's **entire
> life**. In normal operation walrus **never** creates a second slot, re-creates a slot, or
> does a per-table re-sync off a fresh slot — a restart is always a *resume* from the existing
> slot's `confirmed_flush_lsn`. The **only** time a new slot is ever created is the
> **total-restart** disaster path below (the existing slot was *lost/invalidated* and its WAL is
> physically gone) — a rare, loud, whole-system nuke-and-repave, not a routine operation. Keeping
> that path rare is exactly why slot liveness matters: the heartbeat + WAL-safety-cap machinery in
> [§1.9](#19-slot-liveness--heartbeat--keepalive) exists to keep this one slot healthy forever so
> we never have to open a second one.
>
> **Single-table reloads / re-syncs are a deliberate NON-goal for now.** There is **no per-table
> recovery path** in v1 (see [Non-goals](#goals--non-goals)): a table that quarantines on a lossy
> `ALTER COLUMN TYPE` cast is left as-is and alerted, *not* individually reloaded. The vision is
> for the tool to eventually own single-table reloads too, but **we are explicitly not solving
> that here** — the only full re-sync that exists is the whole-system total-restart, which rebuilds
> *every* table together, never just one.

The system consumes **exactly one** logical replication slot for its **entire life** — there
is no multi-slot sharding. That makes `walrus-pg-sink` inherently a **single active consumer of
a single slot** (one active pod: StatefulSet `replicas=1` or leader-elected). The single sink is
the decode/stage throughput ceiling; the **parallelism lever is the loader** (one worker per
table/DuckDB file), which lives downstream of S3 and never touches the slot.

**Epoch (generation).** The slot's lifetime = one **epoch**, tracked in
`walrus.replication_state`. *Everything* is namespaced by epoch — the S3 prefix
(`s3://<bucket>/<epoch>/…`), `file_manifest` rows, `loader_checkpoint` (both watermarks), and the
`.duckdb` files (each holding **both** `<table>` and `<table>_raw`) all belong to an epoch — so a
generation can be cleanly abandoned and rebuilt together.

**Total-restart mode.** A slot cannot be resumed once its WAL is gone. If — on a **successful**
source connection — the sink finds the slot **absent** (`pg_replication_slots` empty) or
**invalidated** (`wal_status = 'lost'` after exceeding `max_slot_wal_keep_size` [12][13]), the
change history since `confirmed_flush_lsn` is **permanently lost** and the only correct recovery
is a full re-sync:

1. The sink **bumps the epoch** and creates a **new slot** over a replication connection
   (capturing a fresh **exported snapshot + `consistent_point`**, per
   [§1.7](#17-snapshot--backfill-bootstrap)).
2. Old-epoch state is retired — the manifest queue is abandoned and old-epoch S3 files are left
   to their lifecycle TTL.
3. **All tables are re-snapshotted** under the new epoch ([§1.7](#17-snapshot--backfill-bootstrap)).
4. Loaders detect the new epoch and **rebuild every `.duckdb` file** — re-append the new-epoch
   snapshot into `<table>_raw` and re-derive `<table>` via the transform — resetting **both**
   watermarks (`raw_appended_lsn` and `transformed_lsn`).

This is deliberately "nuke and repave" — a deleted slot is a disaster event, so total-restart is
**loud** (alerts) and **guarded against false positives**: a transient connection failure is
*not* slot-loss (it's retried with backoff per the [bootstrap rules](#startup--bootstrap-fail-fast-preflight)); only an
authoritative "connected, slot absent/lost" triggers the epoch bump. The sink **never** creates
a second slot alongside a healthy one.

### 1.9 Slot liveness — heartbeat & keepalive

Two things must keep working even when there is **nothing to replicate**, or the single
lifelong slot silently degrades into the very total-restart disaster [§1.8](#18-single-slot-for-life--total-restart) exists to avoid. Both were missing from the
first sketch and are non-negotiable.

- **Heartbeat — so an idle publication can't pin WAL forever.** `restart_lsn` /
  `confirmed_flush_lsn` only advance when the consumer *confirms progress*, and the consumer only
  receives events for **published** tables. So if the published tables are **idle while the rest
  of the database is busy** (heavy WAL elsewhere), the sink produces no files, never confirms a
  newer LSN, and **WAL grows without bound** until the slot invalidates (`wal_status='lost'`) —
  the sink can be perfectly healthy and still detonate the whole system. This is a well-documented
  production failure. Mitigation, both halves required:
  1. Emit a **heartbeat** on an interval so there is always *something* recent in the slot to
     confirm past: a periodic write to a tiny **published `walrus.heartbeat` table** (must be in
     `walrus_pub`), **or** `pg_logical_emit_message()` (works with `pgoutput`, PG14+). Either gives
     the sink a fresh LSN it can durably checkpoint, dragging `restart_lsn` forward.
  2. Even with no pending rows, periodically **send a standby status update** advancing the
     confirmed LSN to the safe point the heartbeat established.
  Keep `max_slot_wal_keep_size` as the **backstop only**, and **alert on retained WAL well before
  it** — hitting that cap converts "bloat" into "slot lost → total-restart," which we never want.

- **Keepalive feedback is unconditional — and is *not* the same as advancing the slot.** The
  walsender drops the connection after **`wal_sender_timeout`** (default **60s**) if the consumer
  doesn't reply to keepalive requests / send periodic feedback — regardless of whether anything
  was durably persisted. A slow S3 flush or a long initial snapshot can easily exceed 60s of
  "silence" and trigger `terminating walsender process due to replication timeout` and reconnect
  storms. So walrus tracks **two distinct LSNs** in its standby status updates:
  - a **keepalive / written-flushed LSN**, sent on **every** keepalive request and on a
    sub-`wal_sender_timeout` interval, purely to stay connected — it may report WAL we've merely
    *received*, decoupled from durability; and
  - **`confirmed_flush_lsn`** — the slot-advancing number — which moves **only after** the
    S3 + manifest durability checkpoint ([§1.5](#15-the-durability-checkpoint-wal-bounding-invariant))
    **and never past an open streamed txn** ([§1.6](#16-large-transaction-safety)).
  Conflating the two (only sending feedback after durability) is what causes the disconnects.

---

## Data type translation (Postgres → Arrow → Parquet)

Apache Arrow is the single intermediate representation. **⚠ This is the design's biggest
unknown and highest-effort area:** research confirmed the Arrow↔Parquet *writer/reader*
mechanics [7][8] but **could not source-verify a canonical per-type Postgres→Arrow mapping**.
Treat the table below as the **conventional starting point to validate in a spike**, not
gospel.

| Postgres type | Arrow `DataType` (proposed) | Notes / risk |
|---|---|---|
| `bool` | `Boolean` | |
| `int2/4/8` | `Int16/32/64` | |
| `float4/8` | `Float32/64` | |
| `numeric(p,s)` | `Decimal128(p,s)` (p≤38) / `Decimal256` | ⚠ **unconstrained `numeric`** has no column-wide p/s → fall back to **`Utf8`** (what ADBC / `connector_arrow` do; a max-precision `Decimal` risks overflow/scale loss). Decide the DuckDB target type (VARCHAR vs DECIMAL+`CAST`) deliberately — a silent load `CAST` failure aborts the MERGE. |
| `money` | `Decimal128(19,2)` or `Int64` | locale-sensitive; treat carefully. |
| `char/varchar/text` | `Utf8` (`LargeUtf8` if huge) | |
| `bytea` | `Binary` / `LargeBinary` | |
| `uuid` | `FixedSizeBinary(16)` | ⚠ round-trips into DuckDB as **BLOB**, not `UUID`, unless the Parquet **UUID logical-type** annotation is emitted (arrow-rs omits it by default) → prefer canonical **text**, or cast on load. |
| `json` / `jsonb` | `Utf8` (canonical text) + field metadata tag | DuckDB re-parses to `JSON`; keeping text is simplest and lossless. |
| `date` | `Date32` | |
| `time` | `Time64(Microsecond)` | |
| `timetz` | `Utf8` or `Int64`+offset | ⚠ Arrow has no tz-aware time. |
| `timestamp` | `Timestamp(Microsecond, None)` | |
| `timestamptz` | `Timestamp(Microsecond, Some("UTC"))` | store normalized to UTC. |
| `interval` | `Interval(MonthDayNano)` | ⚠ **no clean Parquet round-trip** — Parquet's interval logical type is month/day/**millis**, so nanoseconds truncate or the write is rejected → store as `bigint` µs (or text) unless verified end-to-end. |
| arrays `T[]` | `List<T>` | nested arrays → nested `List`. |
| `enum` | `Dictionary(Int32, Utf8)` or `Utf8` | enum labels come from catalog / `Type` messages. |
| composite/row | `Struct<...>` | |
| `inet/cidr/macaddr`, ranges, `tsvector`, `bit`, geometric, `hstore` | `Utf8` fallback (or `Map` for hstore) | ⚠ lossy-to-text unless we invest per-type. |
| **NULL** (any) | Arrow **validity bitmap** (nullable field) | pgoutput signals null vs unchanged-TOAST distinctly — see below. |

**Three correctness gotchas that must be in v1:**

- **Unchanged TOAST columns.** In an `UPDATE`, pgoutput sends an *"unchanged TOAST value"*
  placeholder for large columns that didn't change (not NULL, not the value). `<table>_raw`
  stores that sentinel **verbatim**; the **carry-forward runs in the transform** (raw → mirror),
  resolved **per column**: for any column named in the winning event's `unchanged_toast`, the
  transform substitutes the **last non-sentinel value for that PK, found by scanning `<table>_raw`
  backward from the winning row's `(commit_lsn, lsn)`** — *not* the current mirror value. This
  matters when the write that set the TOAST value and a later unchanged-TOAST update land in the
  **same batch** (e.g. `INSERT big='X'` @L100, then `UPDATE …, big=<sentinel>` @L200): the mirror
  has no row for that PK yet, so a mirror-only lookup would write `NULL` and silently drop `'X'`
  even though it's still sitting in raw at L100. The resolution is `COALESCE(` latest raw value ≤
  winner's `(commit_lsn, lsn)` where the column isn't the sentinel `, current mirror value )`.
  (The periodic full-rebuild additionally unions the current mirror in as an LSN-floor baseline, so
  a TOAST value whose last real write was already pruned from raw is still never lost.) *(This
  raw-log back-scan is stronger than a stateless "enrich with the current value" carry-forward,
  which is prone to a data race — but its correctness depends on the **commit-LSN ordering** above,
  not row LSN.)*
- **`REPLICA IDENTITY` (PK required).** Updates/deletes only carry key columns; with a
  `PRIMARY KEY` and default replica identity that key *is* the PK (**all columns of a composite
  PK**) — exactly what the MERGE needs. **Keyless tables are not supported** (rejected at preflight, see §1.1 and Non-goals);
  we never fall back to `REPLICA IDENTITY FULL` + full reloads.
- **`STORED` generated columns arrive as NULL.** pgoutput does **not** replicate the computed
  value of a `GENERATED ALWAYS AS ... STORED` column — it decodes as NULL. The mirror must either
  **recompute** it (only if the expression is DuckDB-portable) or **mark it derived / exclude it**
  from the target rather than storing a false NULL. Detect generated columns from the catalog
  during `schema_registry` hydration and record the decision per column.

Implementation: the `pg-to-arrow` crate maps the source relation schema (from pgoutput
`Relation`/`Type` messages + `information_schema`) to an Arrow schema once per
`schema_version`, cached in the `schema_registry` table.

---

## DDL capture (schema evolution)

Logical decoding **does not emit DDL** [3][5], so the source database **must have a DDL-capture
trigger built on it** — a **required prerequisite** installed once on the source
([§1.1](#11-source-side-setup-one-time-via-migrationjob)), and the sole mechanism by which the
replication tool learns that a schema change happened. We use the **AWS DMS `awsdms_ddl_audit`
pattern** ([AWS: PostgreSQL as a DMS source](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html)
[6]), adapted:

```sql
-- Fixed-schema audit table (AWS DMS awsdms_ddl_audit shape) [6]
CREATE TABLE walrus.ddl_audit (
  c_key    bigserial PRIMARY KEY,
  c_time   timestamptz,   -- UTC (our metadata-timestamps rule; not the AWS bare `timestamp`)
  c_user   varchar(64),
  c_txn    varchar(16),
  c_tag    varchar(24),   -- 'CREATE TABLE' | 'ALTER TABLE' | 'DROP TABLE'
  c_oid    integer,
  c_name   varchar(64),
  c_schema varchar(64),
  c_ddlqry text           -- raw DDL text (current_query())
);

-- SECURITY DEFINER functions capture DDL into the audit table [6]. TWO triggers are needed:
--   ddl_command_end — fires for CREATE / ALTER / COMMENT / etc.; snapshots the resulting column set.
--   sql_drop        — REQUIRED to catch DROP TABLE / DROP COLUMN, which ddl_command_end does NOT
--                     enumerate (uses pg_event_trigger_dropped_objects()). See caveat below.
CREATE EVENT TRIGGER walrus_intercept_ddl  ON ddl_command_end
  EXECUTE FUNCTION walrus.intercept_ddl();   -- EXECUTE FUNCTION (PG11+ spelling; we target 14+)
CREATE EVENT TRIGGER walrus_intercept_drop ON sql_drop
  EXECUTE FUNCTION walrus.intercept_drop();

-- REQUIRED: walrus.ddl_audit must be part of walrus_pub (see §1.1). ONLY because it is a
-- PUBLISHED table do its INSERTs travel through the SAME slot as DML, inline and in commit order:
ALTER PUBLICATION walrus_pub ADD TABLE walrus.ddl_audit;   -- unless the publication is FOR ALL TABLES
```

**Key elegance — DDL is ordered *inline* with DML.** Because `walrus.ddl_audit` is an
ordinary **published** table (it **must** be in `walrus_pub` for this to hold), **its `INSERT`s
flow through the same logical replication slot as regular DML**. The sink therefore sees "schema changed to X at LSN L" **in commit order relative to
the data changes** — no separate polling channel, no ordering guesswork. When the sink
decodes a `ddl_audit` insert, it writes a `ddl_manifest` row and bumps the affected table's
`schema_version`; the loader applies the schema change to the **`<table>` mirror** (and, where
the [taxonomy](#per-change-type-handling-schema-evolution-semantics) requires — `ADD`/`RENAME
COLUMN`, lossless type widen, `RENAME TABLE` — also to **`<table>_raw`**, just before the first
file of the new `schema_version` is appended) at that LSN before applying later data files.

> **Deviation from AWS:** the AWS function `INSERT`s then `DELETE`s the audit row in-txn
> (DMS just needs to see the INSERT) [6]. **We keep the row** (drop the DELETE) so we retain
> a durable, replayable schema history. We must still ensure the sink acts on the INSERT op.

**⚠ Event triggers are NOT exhaustive** — a verified caveat from research: the claim that
event triggers "reliably fire on all DDL events" was **refuted (0-3)**. Three concrete gaps:
(a) `ddl_command_end` **does not fire for every command**; (b) **drops** (incl. `DROP COLUMN`)
are only enumerable via the separate **`sql_drop`** event + `pg_event_trigger_dropped_objects()`;
and (c) event triggers fire for **no** command on **shared/global objects** (roles, databases,
tablespaces) or on event triggers themselves — those are invisible to this mechanism (acceptable:
walrus replicates *tables*, not globals). Mitigations:
- Add a `sql_drop` event trigger for `DROP`/column drops (shown in the DDL above).
- Guard tags we care about (`CREATE/ALTER/DROP TABLE`, `CREATE TABLE AS`) [6] but also
  reconcile against pgoutput **`Relation`** messages (which carry live column metadata) as a
  backstop — if a `Relation` shows a schema the registry doesn't know, flag drift.
- Periodic schema-diff audit job as defense-in-depth.

### Per-change-type handling (schema-evolution semantics)

Capturing DDL is only half the job; each change class affects the pipeline differently. Two
principles keep this tractable:

- **Schema-diff, not DDL-text replay.** Rather than parse and re-execute the raw `c_ddlqry`
  across the Postgres→DuckDB dialect gap (fragile), the trigger *also* snapshots the affected
  table's **resulting column set** (name, type, `attnum`/position, nullability, comment) from
  the catalog at `ddl_command_end` into `schema_registry`. The loader derives the exact DuckDB
  DDL from `new − old`. Handles every change uniformly, no SQL parser. *(This means expanding
  the captured event tags beyond AWS's `CREATE/ALTER/DROP TABLE` to include `COMMENT`, plus a
  `sql_drop` trigger.)*
- **Homogeneous files.** The sink **cuts a fresh Parquet file at every structural schema
  change**, so each file carries exactly one `schema_version`. The loader, on a file whose
  `schema_version` exceeds the DuckDB table's current version, first applies the pending
  structural DDL, then merges the file. No file ever straddles a schema boundary.

We split the version counter: a **structural `schema_version`** (bumped by add/drop/type/rename
— gates data application and cuts a file boundary) vs a **metadata revision** (comments /
constraints — recorded, and for comments *mirrored*, but does **not** gate data).

Each change touches the two tables differently: **`<table>`** tracks the exact current source
shape, while **`<table>_raw`** is an *additive, history-preserving* superset that only ever adds
columns, widens types, or follows renames — it never destructively drops or casts history.

| Source change | Structural bump? | `<table>` mirror action | `<table>_raw` (CDC log) action |
|---|---|---|---|
| `COMMENT ON TABLE/COLUMN` | No (metadata) | **Mirror** → `COMMENT ON` the DuckDB object. No shape/data change. | **None** (a CDC log has no source-comment semantics). |
| `ADD COLUMN` (const/NULL default) | Yes | `ALTER TABLE ADD COLUMN`; pre-change files omit it → merged as NULL/default. | `ALTER TABLE ADD COLUMN` **nullable** so post-change verbatim appends align; old raw rows NULL. |
| `ADD COLUMN` (volatile default → table rewrite) | Yes | same `ADD`; rewritten rows arrive as ordinary DML and merge by PK. | same `ADD` nullable; rewritten rows land as ordinary appended CDC rows. |
| `DROP COLUMN` | Yes | **`ALTER TABLE DROP COLUMN`** (physical drop); post-drop files omit it. | **Retain** the column (nullable) — preserves verbatim history; post-drop files fill it NULL. |
| `ALTER COLUMN TYPE` (widening/lossless) | Yes | `ALTER TABLE ALTER COLUMN TYPE` — DuckDB casts existing rows in place; `pg-to-arrow` maps the new type going forward. | Apply the **same lossless `ALTER`** (never re-casts historical values that already fit). |
| `ALTER COLUMN TYPE` (lossy/incompatible) | Yes | attempt the in-place cast; **on failure → quarantine the table + alert, and stop.** *(Accepted terminal outcome — single-table reloads out of scope.)* | **Widen the raw column to `VARCHAR`** so rows of multiple schema_versions coexist; **never casts** existing rows. |
| `RENAME COLUMN` | Yes | `ALTER TABLE RENAME COLUMN`; columns tracked by `attnum`/position so renames are unambiguous. | `ALTER TABLE RENAME COLUMN` (same logical column, tracked by `attnum`) so appends keep aligning. |
| `RENAME TABLE` | Yes | re-map/rename the per-table DuckDB file; table identity tracked by a stable id, not name alone. | rename `<table>_raw` too (same file, same stable id). |
| `NOT NULL` / `DEFAULT` / `CHECK` add/drop | No (metadata) | recorded for lineage; **not enforced** on the mirror in v1 (values still flow as DML). | **None** (raw never enforces constraints). |
| `TRUNCATE` | n/a (stream op) | carries no tuple/PK, so it's **not** a `MERGE` branch: the transform **empties the mirror** as of the truncate commit LSN `T` (`DELETE FROM <table>`) and repopulates from rows with `commit_lsn > T` ([§2.1](#21-the-raw-to-mirror-transform-model)). | the `t` op is **appended as a logged row** (raw is **not** truncated). |
| `DROP TABLE` | Yes | drop/retire the DuckDB table; stop replicating it. | drop/retire `<table>_raw` and the file too. |
| `CREATE TABLE` (added to publication) | new registry entry | PK preflight applies → create the `.duckdb` file with **both** `<table>` and `<table>_raw` + snapshot the new table. | created alongside the mirror (both live in the new file). |

---

## Coordination contract (control-plane tables)

The hand-off between the two services. Recommended to live in a **dedicated control-plane
Postgres** (a small managed instance or a `walrus` schema separate from source) so control
traffic doesn't load the source. Core tables:

```sql
-- One row per slot lifetime; a new slot = a new epoch (see §1.8).
CREATE TABLE walrus.replication_state (
  epoch        bigint PRIMARY KEY,        -- monotonic generation id
  slot_name    text NOT NULL,
  created_lsn  pg_lsn NOT NULL,           -- consistent snapshot LSN at slot creation
  status       text NOT NULL,             -- bootstrapping | streaming | total_restart
  created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE walrus.file_manifest (
  id             bigserial PRIMARY KEY,   -- also the tiebreaker for equal-lsn_end files (snapshot)
  epoch          bigint NOT NULL,         -- FK → replication_state; namespaces ALL state
  source_schema  text NOT NULL,
  source_table   text NOT NULL,
  s3_uri         text NOT NULL,
  kind           text NOT NULL,           -- 'snapshot' | 'stream'
  row_count      bigint NOT NULL,
  lsn_start      pg_lsn NOT NULL,         -- commit LSN of the file's first txn
  lsn_end        pg_lsn NOT NULL,         -- COMMIT LSN of the file's last txn (NOT max row lsn) —
                                          --   monotonic with commit order; see the note below
  schema_version bigint NOT NULL,         -- FK → schema_registry
  status         text NOT NULL DEFAULT 'ready',  -- ready | failed  (applied rows are DELETED, not kept)
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON walrus.file_manifest (epoch, source_schema, source_table, lsn_end, id)
  WHERE status = 'ready';

CREATE TABLE walrus.loader_checkpoint (
  epoch            bigint NOT NULL,
  source_schema    text NOT NULL,
  source_table     text NOT NULL,
  raw_appended_lsn pg_lsn NOT NULL,     -- Phase A: CDC log durable up to this COMMIT LSN
  transformed_lsn  pg_lsn NOT NULL,     -- Phase B: mirror derived up to this COMMIT LSN (<= raw_appended_lsn)
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table),
  CHECK (transformed_lsn <= raw_appended_lsn)
);
-- plus: walrus.ddl_manifest (schema-change events w/ LSN), walrus.schema_registry (versioned Arrow/DuckDB schemas)
```

**Why every progress key is a COMMIT LSN, not a row LSN (correctness-critical).** Postgres
logical decoding delivers changes in **commit order**, but each change's *row* LSN is its
WAL-write position. With `streaming 'on'`, a large transaction that opens early and commits late
can carry row LSNs *entirely below* the commit LSN of a smaller transaction that committed first.
If files were ordered or watermarked by **max row LSN**, that late large-txn file (with a lower
max-row-LSN) would sort before — and be **skipped by** — a watermark that the smaller,
earlier-committed txn already advanced past, **silently dropping the whole transaction**. So
`lsn_end` is defined as the **commit LSN of the file's last transaction** (monotonic with commit
order), the loader claims and orders files by **`(lsn_end, id)`** (the `id` disambiguates
equal-`lsn_end` files, e.g. the many snapshot files that all share `consistent_point`), and both
`loader_checkpoint` watermarks are **commit-LSN valued**. The per-row `lsn` lives on only inside
`<table>_raw`, as the per-PK last-writer tiebreaker (safe because row-level locking serializes
writes to a single PK, so per-PK row-LSN order equals commit order — see
[§2.1](#21-the-raw-to-mirror-transform-model)).

**Lifecycle — the manifest is a work queue, not a history.** A `file_manifest` row is retired
(**deleted**, not flipped to a terminal state) as soon as its rows are durably **appended to
`<table>_raw`** — because `<table>_raw` (not the staged Parquet) is now the source of truth for
the transform, the queue row's job is done once the append commits. Because DuckDB and the
control Postgres are separate databases, the delete can't share the DuckDB write's transaction;
the crash-safe order is: **append the file's rows to `<table>_raw` and commit the DuckDB write →
then, in one control-DB transaction, advance `loader_checkpoint.raw_appended_lsn` *and* `DELETE`
the claimed rows together.** The **transform** (`<table>_raw` → `<table>`) runs afterward off the
raw log and advances `transformed_lsn` on its own commit — it never re-reads the manifest. A
repeatedly-failing file is moved aside as `status='failed'` (dead-letter) so a poison file can't
block the queue. `ddl_manifest` / `schema_registry` are **not** pruned — they're low-volume and
are the schema history needed to reconstruct a table at any `schema_version`.

**Idempotency / exactly-once-effect (two stages, two guarantees):** a verbatim **append is *not*
idempotent** — replaying a file would duplicate raw rows — so it rests on **two** guards, and it
matters to be precise about which does what. **(1)** The manifest is a **work queue**: a file is
**deleted** the moment its rows are appended, so an already-applied file is simply gone and cannot
be re-claimed. **(2)** In the **crash window** between the DuckDB append-commit and the control-DB
transaction that advances `raw_appended_lsn` + deletes the queue rows, the file is still in the
queue and **will be re-claimed and re-appended** — and here the **file-level watermark does *not*
save us** (it was never advanced, precisely because that txn didn't commit). What makes the
re-append safe is the **row-level `ON CONFLICT DO NOTHING`** on `<table>_raw`'s real composite PK
(source PK + `sink_processed_at` + `lsn`). **That row-level PK is therefore load-bearing, not a
mere backstop** — do not demote it to a non-enforced key without an equivalent row-dedup (see the
caveat in [§2.1](#21-the-raw-to-mirror-transform-model)). The `raw_appended_lsn` watermark's job is
**ordering and resume** (process files in `(lsn_end, id)` commit order; give the transform its
floor), *not* append-dedup. The **transform**, by contrast, *is* naturally idempotent: it scans
only `<table>_raw` rows with **`commit_lsn > transformed_lsn`**, dedups to the latest op per PK by
**`(commit_lsn DESC, lsn DESC)`**, and `MERGE`s (PK-keyed, last-writer-wins) — so a crash between
append and transform just re-runs the transform harmlessly.

**Staged S3 objects ≠ queue rows.** Don't delete the Parquet inline with the row. Put objects
under an **S3 lifecycle TTL** (e.g. 7–30 days) to keep a bounded replay/debug window after the
queue row is gone (set TTL to `0`/delete-inline if you want no replay window). At high volume,
mitigate delete-driven Postgres bloat by tuning autovacuum or **partitioning `file_manifest`
by day and DROPping old partitions** instead of row-by-row deletes.

---

## Component 2 — Data Sink (`walrus-loader`)

> **Non-negotiable mission: reconcile the staged work back into the exact shape the data has in
> Postgres — accuracy over latency.** `walrus-loader` exists to turn the sink's stream of change
> events into a DuckDB `<table>` mirror that **matches the source table as it stands in
> Postgres** — same rows, same current values, same schema. **Real-time is explicitly not the
> goal; correctness is.** The loader may run as far behind the cadence as it needs to, but when
> it settles, `<table>` must be a faithful, current-state copy of the Postgres table (via the
> append-to-`<table>_raw`-then-transform model). If there is ever a tension between "fast" and
> "correct," the loader chooses **correct** — it is the accuracy authority of the system, and the
> periodic full-rebuild exists precisely to self-heal any drift back to source truth.

Owns the per-table DuckDB files. **One logical worker owns each `.duckdb` file** (DuckDB is
single-writer) — and that one writer owns **both** tables in the file (`<table>_raw` +
`<table>`), which is exactly why one-file-per-table gives clean parallelism.

**Apply loop (per table, on the user's poll cadence) — two phases, two watermarks:**

*Phase A — append to the raw log (`<table>_raw`):*
1. `SELECT ... FROM file_manifest WHERE table=? AND status='ready' ORDER BY lsn_end, id` — claim
   the next batch of files **in commit order** (`id` breaks ties between equal-`lsn_end` files,
   e.g. the snapshot files that all share `consistent_point`). Applied files are **deleted** from
   the queue, so what remains is exactly the un-applied (or crash-window) tail — the non-idempotent
   append is made safe by that deletion **plus** the row-level `ON CONFLICT DO NOTHING`, *not* by an
   `lsn_end > raw_appended_lsn` filter (which would wrongly skip equal-`lsn_end` snapshot files).
2. For each file in LSN order: `GET` Parquet from S3 (or let DuckDB read it directly via
   `read_parquet` + `httpfs`/`SET s3_*` [10][11]) and **`APPEND` its rows verbatim** into
   `<table>_raw` — no dedup, `walrus_pg_sink_meta` kept, `op`/`lsn` promoted to typed columns.
3. In **one control-DB transaction** (after the DuckDB append commits), advance
   `loader_checkpoint.raw_appended_lsn` **to the max `lsn_end` (commit LSN) just claimed** **and
   `DELETE` the claimed `file_manifest` rows** — queue semantics: appended files are removed. A
   repeatedly-failing file is instead set `status='failed'` (dead-letter) so it can't block the
   queue.

*Phase B — transform the raw log into the mirror (`<table>`):*
4. Apply any pending **structural DDL** whose LSN we're about to cross to the `<table>` mirror
   (and, per the [taxonomy](#per-change-type-handling-schema-evolution-semantics), to
   `<table>_raw`), driven by `ddl_manifest` + `schema_registry`, **before** transforming data
   past that LSN.
5. **Apply truncates first.** A pgoutput `TRUNCATE` carries **no tuple/PK**, so it can't be a
   `MERGE` join branch. Take `T = max(commit_lsn)` of any `op='t'` row in the new tail; if present,
   the mirror is empty as of `T`, so `DELETE FROM <table>` (a truncate wipes the whole table) and
   let only rows with `commit_lsn > T` repopulate it in the next step.
6. **Dedup-to-latest per PK** over `<table>_raw` rows with `commit_lsn > max(transformed_lsn, T)`
   (window `row_number() OVER (PARTITION BY pk ORDER BY commit_lsn DESC, lsn DESC)` — commit LSN
   first, row LSN as the intra-txn tiebreaker), **excluding `op='t'`**, into a `TEMP` staging
   table; resolve any `unchanged_toast` columns by **scanning `<table>_raw` backward from the
   winner's `(commit_lsn, lsn)` for that PK** (current mirror only as a last resort — see the
   [TOAST gotcha](#data-type-translation-postgres--arrow--parquet)). Then **`MERGE INTO <table>`**
   from the staging table (DuckDB ≥ 1.4.0): `WHEN MATCHED AND op='d' THEN DELETE`, `WHEN MATCHED
   THEN UPDATE`, `WHEN NOT MATCHED AND op<>'d' THEN INSERT`. Commit, then advance `transformed_lsn`
   to the max **commit LSN** applied (including any truncate `commit_lsn`).

A slower, per-table **full-rebuild / compaction** job runs on its own cadence to self-heal drift,
reclaim space, and prune `<table>_raw` below the retention floor — see
[§2.1](#21-the-raw-to-mirror-transform-model).

### 2.1 The raw-to-mirror transform model

Each `.duckdb` file holds **two tables** for one source table:

- **`<table>_raw` — the append-only CDC log / history table (bronze).** The verbatim
  union-superset of every source column ever seen (dropped columns kept nullable, incompatible
  type changes widened to `VARCHAR`, renames tracked by `attnum`), **plus** `walrus_pg_sink_meta`
  stored verbatim, **plus** promoted typed `op` / `commit_lsn` / `lsn` / `sink_processed_at`
  columns (`commit_lsn` is the order/watermark key; `lsn` the intra-txn tiebreaker). Rows are
  appended, never updated; snapshot rows land here too (`kind='snapshot'`).
  Its **primary key is composite**: the **source table's PK column(s) + `sink_processed_at` (the
  walrus-pg-sink sink time) + `lsn`**. The source PK alone repeats across events (this *is* a
  history log), so it can't be the key; `sink_processed_at` + the source PK is the natural key,
  and **`lsn` is the deterministic tiebreaker that guarantees uniqueness** — two events for one
  source PK can share a millisecond-resolution sink time, but every WAL change has a distinct LSN
  (and snapshot rows are unique by source PK). This real PK also lets the append use
  `INSERT … ON CONFLICT DO NOTHING` for **row-level idempotency** — which is the **load-bearing**
  dedup in the crash window between the DuckDB append-commit and the queue-delete (the file-level
  watermark does **not** cover that window; see the
  [coordination contract](#coordination-contract-control-plane-tables)). *(If PK-index maintenance
  on the append-hot path proves costly it can be a `UNIQUE` constraint instead, but it must stay
  **enforced** — do **not** demote it to a logical-only / non-enforced key without an equivalent
  row-dedup, or crash-window replays will duplicate raw rows.)*
- **`<table>` — the derived current-state mirror (silver).** Exactly the current source shape;
  the meta column is dropped. Produced *only* by the transform below.

**Transform — incremental (default).** Each cycle, in one atomic DuckDB transaction:

```sql
BEGIN;
-- T := (SELECT max(commit_lsn) FROM <table>_raw WHERE commit_lsn > :transformed_lsn AND op = 't')  -- may be NULL
-- 0. TRUNCATE has no tuple/PK, so it can't be a MERGE branch. If the tail contains one,
--    the table is empty as of T: wipe the mirror, and apply only post-T rows below.
DELETE FROM <table> WHERE :T IS NOT NULL;   -- no-op when the tail has no truncate

-- 1. Dedup the post-truncate tail to the latest op per PK (truncate rows excluded).
--    Order by commit_lsn (delivery/commit order); row lsn only breaks ties within one txn.
CREATE TEMP TABLE _batch AS
  SELECT * EXCLUDE (rn) FROM (
    SELECT *, row_number() OVER (PARTITION BY <pk> ORDER BY commit_lsn DESC, lsn DESC) AS rn
    FROM <table>_raw
    WHERE commit_lsn > COALESCE(:T, :transformed_lsn) -- rows at/below a truncate are moot
      AND op <> 't'                                   -- truncate handled above, never in the MERGE
  ) WHERE rn = 1;                                     -- latest op per PK (dedup AFTER ranking)
-- 2. Resolve unchanged_toast per column: for each column in s.unchanged_toast, replace the
--    sentinel with the last NON-sentinel value for that PK found by scanning <table>_raw back
--    from the winner's (commit_lsn, lsn); fall back to the current <table> value only if raw has none.
MERGE INTO <table> t USING _batch s ON t.<pk> = s.<pk>
  WHEN MATCHED AND s.op = 'd'      THEN DELETE
  WHEN MATCHED                     THEN UPDATE SET <every non-key col> = s.<col>
  WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (<cols>) VALUES (s.<cols>);
COMMIT;   -- then advance transformed_lsn = max(commit_lsn) applied (including any truncate commit_lsn)
```

`<pk>` above denotes the table's **full primary-key column list**, which may be **composite**:
`PARTITION BY` and the `MERGE … ON` predicate expand to *all* key columns (`PARTITION BY k1, k2 …`;
`ON t.k1=s.k1 AND t.k2=s.k2 …`), and the `ON CONFLICT (k1, k2, …)` fallback targets the composite
PK/UNIQUE constraint — the loader generates that column list from `schema_registry`, never
assuming a single key column.

Deletes are dropped **after** ranking (filtering `op='d'` before the window would let an earlier
insert resurrect a key). `TEMP` staging never touches the persistent file. Cost is O(new events),
but the `MERGE` reconciles against the whole mirror so cross-batch state stays correct. `MERGE
INTO` needs **no** PK/UNIQUE on the target and does insert/update/delete in one pass; the
`INSERT … ON CONFLICT (pk) DO UPDATE` + separate `DELETE` fallback (DuckDB < 1.4.0) needs a
PK/UNIQUE and a pre-deduped batch, and must list every non-key column in `SET`. The truncate
pre-step (step 0/5) and the unchanged-TOAST back-scan (step 2/6) apply **identically** to the
fallback path — it inherits the same gaps otherwise.

**Transform — full-rebuild (periodic self-heal + compaction).** On a slower cadence (and after
any drift/quarantine recovery), `CREATE OR REPLACE TABLE <table> AS SELECT …` window-dedups over
the **retained raw ∪ the current mirror injected as an LSN-floor baseline** (so a value whose last
real write was already pruned from raw is preserved), dropping `op='d'` winners. `CREATE OR
REPLACE … AS SELECT` is atomic (readers see the old table until commit) and yields a fresh,
un-bloated table.

**Retention & reclamation.** `<table>_raw` is retained behind `transformed_lsn` by a rolling
window (default ~7 days / last-K batches) for debug + cheap replay, then pruned below the floor.
Because DuckDB `DELETE` only tombstones (`CHECKPOINT` reclaims only heavily-deleted row groups;
`VACUUM FULL` is unimplemented), real space reclamation rides on the periodic full-rebuild /
`COPY FROM DATABASE`. Set the window toward `0` only if a file is severely size-constrained — the
transform is unchanged, so the window can widen later.

**Loading mechanism (Rust):** Phase A uses the **`duckdb` crate** (`duckdb-rs`) **Appender API**
(`append_record_batch`, `appender-arrow` feature) — or `INSERT INTO <table>_raw SELECT * FROM
read_parquet(...)` — to write Parquet rows straight into the **persistent** `<table>_raw`; DuckDB
reads Parquet from S3 natively with credentials configured (`parquet` feature implies `bundled`)
[9][10][11]. Phase B runs the transform **table-to-table inside DuckDB** — `MERGE INTO <table>`
reading `FROM <table>_raw` (via a `TEMP` deduped staging table), **not** from a transient Parquet
view. `MERGE INTO` requires **DuckDB ≥ 1.4.0 LTS**; older engines fall back to `INSERT ... ON
CONFLICT (pk) DO UPDATE` **plus a separate `DELETE`**.

**DuckDB file storage & querying (recommendation + ⚠ open question):** put each `.duckdb`
file on a **PVC** owned by a `StatefulSet` replica; periodically snapshot/export to S3 for
backup. Alternatives to weigh: **MotherDuck** (managed, removes single-writer HA pain) or a
**DuckLake/Parquet-in-object-store** query layer if you'd rather query the staged Parquet
directly. How downstream users *query* these files (read replicas? scheduled export?) is
unresolved — see [Open questions](#open-questions--risks).

---

## Delivery semantics, ordering & idempotency

- **At-least-once + two-stage idempotency = effectively-once.** Both hops (WAL→S3 and
  S3→DuckDB) are at-least-once. Correctness rests on **two** guarantees, not one MERGE: (a) the
  **raw append** is de-duplicated by the manifest **work-queue deletion** plus the row-level
  `ON CONFLICT DO NOTHING` on `<table>_raw`'s composite PK — a verbatim append is not itself
  idempotent, and the file-level watermark is for *ordering/resume*, not dedup (see the
  [coordination contract](#coordination-contract-control-plane-tables)); and (b) the
  **raw→mirror transform** is idempotent (PK-keyed, last-writer-wins dedup by `(commit_lsn, lsn)`
  + `MERGE`).
- **Per-table ordering** by **commit LSN** (`lsn_end` = the file's last commit LSN; files applied
  in `(lsn_end, id)` order) — **never by row LSN**, which `streaming 'on'` would make
  non-monotonic; **cross-table ordering is not guaranteed** (consequence of one-file-per-table) —
  acceptable per goals.
- **Aborted large txns** never become visible (commit-gated staging, [1.6](#16-large-transaction-safety)).
- **Snapshot/stream boundary** dedups through the merge (idempotent overlap).

---

## Startup & bootstrap (fail-fast preflight)

Both services run an ordered **bootstrap phase** on process start — *before* any WAL is read
or any file is applied — that (a) **gathers the working state** the service needs and (b)
**asserts every precondition**, exiting non-zero on anything missing or misconfigured. On
Kubernetes a non-zero exit → `CrashLoopBackOff`, so a broken deploy is **loud and immediate**
rather than silently degrading. Nothing in the main loop runs until bootstrap succeeds and the
readiness probe goes green.

**Transient vs terminal.** Dependencies that may simply be "still coming up" during a rollout
(source/control Postgres, S3) are retried with capped exponential backoff up to a **startup
deadline** (aligned to the K8s `startupProbe`), then treated as terminal. Misconfiguration
(wrong `wal_level`, missing publication, invalid config, keyless table in strict mode) is
**terminal immediately** — no retry. Every terminal failure emits a **structured, actionable
error** (which precondition, observed vs expected) and a distinct **exit code** so the reason
is greppable in `kubectl logs` / crash events.

### Shared bootstrap (both services)
1. **Load & validate config** — schema-validate the ConfigMap/env; required fields present,
   cadence/threshold values within sane bounds. Invalid → terminal.
2. **Control Postgres reachable** — connect; verify the `walrus` control schema exists and
   **migrations are at the expected version** (`file_manifest`, `loader_checkpoint`,
   `ddl_manifest`, `schema_registry` present). Missing/behind → terminal (or run migrations if
   `auto_migrate=true`).
3. **Object store reachable + usable** — verify the bucket exists and credentials
   (IRSA/Workload Identity) work via a canary `head`/`put`/`get`. Failure → terminal.
4. **Bind metrics/health endpoints** before entering the main loop.

### `walrus-pg-sink` bootstrap
1. **Source Postgres reachable** on a **replication connection** (verify `REPLICATION`
   privilege). Can't connect → hard error (your explicit example).
2. **Server prerequisites** — assert `wal_level = logical`, server version ≥ 14 (proto v2),
   and `max_replication_slots` / `max_wal_senders` have headroom. Any mismatch → terminal.
3. **Publication** — verify `walrus_pub` exists and covers the configured tables **plus
   `walrus.ddl_audit` and `walrus.heartbeat`** (create if `manage_publication=true`, else terminal
   if absent). The DDL-audit and heartbeat tables *must* be published — the first so DDL flows
   inline, the second so an idle publication can't pin WAL ([§1.9](#19-slot-liveness--heartbeat--keepalive)).
4. **Replication slot** — verify the logical slot exists; **read its `restart_lsn` /
   `confirmed_flush_lsn`** to establish the resume position. Absent → create it over a
   replication connection (capturing the **exported snapshot + `consistent_point`** for the
   initial-load path, [§1.7](#17-snapshot--backfill-bootstrap)) or terminal, per mode.
5. **DDL capture + heartbeat installed** — verify `walrus.ddl_audit` + the `walrus_intercept_ddl`
   (+ `sql_drop`) event triggers exist on the source, and that the `walrus.heartbeat` table +
   its writer are in place ([§1.9](#19-slot-liveness--heartbeat--keepalive)); install or terminal.
6. **Per-table PK preflight** — enumerate published tables; assert each has a `PRIMARY KEY` and
   a usable replica identity ([§1.1](#11-source-side-setup-one-time-via-migrationjob)). Keyless tables are **terminal in `strict` mode
   (default)**, or **quarantine + alert and continue** in `lenient` mode. *(Resolves Open-Q6's
   operator-UX question.)*
7. **Hydrate schema registry** — load the relation→Arrow schema cache for each table at its
   current `schema_version`.

### `walrus-loader` bootstrap
1. **Control Postgres reachable** (shared) — plus **acquire table-ownership leases** so no two
   loaders ever write the same table/DuckDB file (the single-writer guarantee). Contended lease
   → terminal.
2. **DuckDB files openable + writable** — for each owned table, mount-check the PVC, open (or
   create) its `.duckdb` file, take the single-writer lock, ensure **both** `<table>` and
   `<table>_raw` exist (create the raw log if missing), and integrity-check both. PVC not
   mounted / file locked / corrupt → terminal.
3. **Load per-table checkpoints** — read **both** `loader_checkpoint.raw_appended_lsn` and
   `transformed_lsn` for each owned table so append and transform can resume independently.
4. **Schema reconcile** — compare **both** tables to the expected `schema_version` in
   `schema_registry`: `<table>` must match the exact source shape, `<table>_raw` its additive
   superset (retained-nullable drops, widened types, `attnum`-tracked renames). If behind, apply
   pending `ddl_manifest` changes to the correct table(s) **before** the apply loop starts.
   Irreconcilable drift → terminal (surfaces the ⚠ event-trigger-gap risk early).
5. **Object store read path** — verify it can `GET` / list the staged prefix.

### Kubernetes wiring
- **`startupProbe`** gates the (possibly slow) bootstrap with a generous
  `failureThreshold × periodSeconds`; **`readinessProbe`** keeps the pod out of
  rotation/work until bootstrap completes; **`livenessProbe`** covers the running loop.
- Optionally hoist the read-only environment checks (DB reachable, migrations current) into an
  **`initContainer`** so the main container only starts once the world is sane.

---

## Kubernetes deployment

| Concern | Approach |
|---|---|
| **`walrus-pg-sink`** | **`StatefulSet` replicas=1** — exactly one active consumer of the **single, lifelong slot** ([§1.8](#18-single-slot-for-life--total-restart)) *or* `Deployment` + **leader election** (`coordination.k8s.io/Lease`). On failover, the new pod resumes from the slot's `confirmed_flush_lsn` — Postgres retained the WAL, so **no loss**. |
| **`walrus-loader`** | **`StatefulSet`**, each replica owns a disjoint set of tables (consistent hashing) with a **PVC per replica** for the `.duckdb` files. Scale by resharding table ownership (not naive HPA — file ownership is exclusive). |
| **Control Postgres** | Small managed instance (or `walrus` schema in source). Holds manifest/checkpoint/ddl/registry. |
| **S3 access** | IRSA / Workload Identity — no static keys. `object_store` (sink) + DuckDB `httpfs`/`SET s3_*` (loader) [11]. |
| **Config / cadence** | `ConfigMap`: sink `max_fill_ms`/`max_bytes`/`max_rows`, loader poll interval (append + transform share it in v1) **plus** a slower, **per-table-overridable** transform **compaction/full-rebuild + raw-retention** cadence — the user's cadence control. Secrets for DB/S3 creds. |
| **Slot lifecycle** | Create the one slot+publication+DDL trigger via an init `Job`/migration; record its epoch in `replication_state`. If the slot is ever lost/invalidated → **total-restart** ([§1.8](#18-single-slot-for-life--total-restart)). **Orphan cleanup:** a decommissioned sink must *drop its slot* — an abandoned slot pins WAL forever [12]. |
| **WAL safety cap** | Set `max_slot_wal_keep_size` on source as a backstop; tune `logical_decoding_work_mem` for the streaming threshold [3]. **Alert on retained WAL well before the cap.** |
| **Slot liveness** | A **heartbeat** `CronJob`/timer writes `walrus.heartbeat` (or emits `pg_logical_emit_message`) on an interval so an idle publication can't pin WAL; the sink sends keepalive feedback under `wal_sender_timeout` regardless of durability ([§1.9](#19-slot-liveness--heartbeat--keepalive)). |
| **Health / probes** | `startupProbe` gates the fail-fast [bootstrap](#startup--bootstrap-fail-fast-preflight); `readinessProbe` holds work until bootstrap completes; `livenessProbe` = replication connection alive + slot lag < cap (sink) / apply loop progressing (loader). |
| **Scaling (single-slot constraint)** | **No multi-slot sharding** — one slot for life. The sink is the single-stream ceiling; scale *out* on the **loader** (one worker per table/DuckDB file) and *up* on the sink pod (CPU/decode). If one sink truly can't keep up, that's a capacity conversation, not more slots. |

---

## Observability

Prometheus metrics: replication lag bytes (`pg_current_wal_lsn − confirmed_flush_lsn`),
slot **retained WAL size** (alarm) and **`pg_replication_slots.wal_status`** (alert on
`unreserved`/`lost`), **seconds since last heartbeat confirmed** and **since last standby-status
feedback** (both must stay well under `wal_sender_timeout` — [§1.9](#19-slot-liveness--heartbeat--keepalive)),
batch flush latency & Parquet throughput, files `ready` per table (loader backlog),
**raw-append lag** (`sink lsn_end − raw_appended_lsn`), **transform lag**
(`raw_appended_lsn − transformed_lsn`), `<table>_raw` row-count / file-size growth, DDL events
pending, aborted-txn count, failed-file count.
Structured `tracing` logs keyed by `xid`/`commit_lsn`/`lsn`/`batch_uuid`. Grafana dashboard +
alerts on slot growth, `wal_status`, heartbeat staleness, and loader backlog.

---

## Proposed Rust workspace layout (for when coding starts)

```
walrus/
├── Cargo.toml                 # workspace (name = walrus; member crates are unprefixed)
├── crates/
│   ├── common/                # config, LSN/types, manifest & registry models, errors
│   ├── pg-to-arrow/           # Postgres → Arrow schema+value mapping (the risky spike)
│   ├── control/               # sqlx models for manifest/checkpoint/ddl/registry
│   ├── pg-sink/               # bin: hand-rolled pgoutput consumer → Arrow → Parquet → S3 → manifest
│   └── loader/                # bin: manifest poll → S3 → append→<tbl>_raw → transform (dedup/MERGE) → <tbl>
├── migrations/                # control-plane DDL + source DDL-trigger install
└── deploy/                    # (later) k8s manifests, Dockerfiles
```

The workspace is already named `walrus`, so member crates drop the redundant prefix. The
`pg-to-arrow` crate is named for what it does (Postgres→Arrow conversion), which also sidesteps
any clash with the external `arrow` (arrow-rs) dependency it uses. The two binary crates
(`pg-sink`, `loader`) build the images/Deployments referred to elsewhere as `walrus-pg-sink` /
`walrus-loader` — the `walrus-` prefix there is the cluster-level service/image name, not the
crate name.

Key deps: `tokio`, a thin replication-capable Postgres client (**`tokio-postgres`** replication
support or **`pgwire-replication`** [2]) — the pgoutput decoder is **ours**
([§1.2](#12-replication-consumer--hand-rolled)) — `arrow` + **`parquet`** [7][8],
`object_store` (S3), **`duckdb`** (`appender-arrow`, `parquet`, `bundled`) [9][10],
`sqlx` (control DB), `serde`, `tracing`, `metrics`.

---

## Phased roadmap

0. **Scaffold + control plane.** Workspace, control-table migrations, DDL trigger install, docker-compose (Postgres `wal_level=logical` + MinIO).
1. **Thin end-to-end slice.** One table, simple types, stream path only (proto v2 + `streaming 'on'`, no snapshot yet) → Arrow → Parquet → S3 → manifest → loader **appends into `<tbl>_raw`, then transforms into `<tbl>`** in one DuckDB file. Prove both the append and the transform, keyed on **`commit_lsn`** watermarks. Include the **heartbeat + keepalive** loop ([§1.9](#19-slot-liveness--heartbeat--keepalive)) from day one so the single slot can't stall.
2. **Full type mapping + DML correctness.** The [type table](#data-type-translation-postgres--arrow--parquet), NULLs, updates/deletes with `REPLICA IDENTITY` (incl. **composite PKs**); **the raw→mirror transform** (dedup-to-latest, **unchanged-TOAST** carry-forward) + the two-watermark checkpoint.
3. **Snapshot/backfill** with watermark handoff.
4. **DDL / schema evolution** applied in the loader from the audit stream.
5. **In-progress large-transaction handling.** proto v2 + `streaming 'on'` is the transport from
   phase 1; this phase adds the *speculative staging* + `Stream Commit`/`Stream Abort` gating for
   txns streamed before commit, **incl. rolled-back-subtransaction exclusion**
   ([§1.6](#16-large-transaction-safety)).
6. **Scale & ops:** loader table-sharding, K8s manifests, leader election/HA, observability, backpressure tuning.
7. **Resilience:** epoch/total-restart on slot loss/invalidation; orphan-slot cleanup.

---

## Open questions & risks

> **Resolved in this revision (were latent bugs in the first sketch) — listed so they don't
> silently regress.** (a) **Row-LSN → commit-LSN watermarking:** all ordering/watermarks now key on
> **commit LSN + `manifest_id`**, closing a silent-data-loss hole under `streaming 'on'` (see the
> [coordination contract](#coordination-contract-control-plane-tables) and
> [§2.1](#21-the-raw-to-mirror-transform-model)). (b) **Snapshot export:** backfill runs under the
> slot's **exported snapshot** (`SNAPSHOT 'export'` → `SET TRANSACTION SNAPSHOT`), not an impossible
> "COPY at an LSN" ([§1.7](#17-snapshot--backfill-bootstrap)). (c) **Slot liveness:** a **heartbeat**
> + unconditional **keepalive** keep the single lifelong slot from stalling on WAL or timing out on
> `wal_sender_timeout` ([§1.9](#19-slot-liveness--heartbeat--keepalive)).

1. **⚠ Postgres→Arrow per-type mapping (highest effort, unverified).** Research confirmed the
   Arrow/Parquet plumbing [7][8] but **not** a canonical per-type mapping. Spike: `numeric`
   (unconstrained precision), `jsonb`, arrays of composites, `enum`, ranges, domains, custom
   types. Owner of most implementation risk.
2. **⚠ Event triggers aren't exhaustive** (verified caveat, refuted 0-3 claim). The design now
   installs the **`sql_drop`** trigger for drops and requires `walrus.ddl_audit` to be published
   ([DDL capture](#ddl-capture-schema-evolution)); residual: **shared/global-object DDL** (roles,
   databases, tablespaces) fires no trigger at all, so `Relation`-message reconciliation + a
   periodic schema-diff remain the backstop against drift.
3. **⚠ Hand-rolled pgoutput decoder + snapshot (highest Component-1 effort).** Because we
   **hand-roll** the replication consumer ([§1.2](#12-replication-consumer--hand-rolled)), the
   slot-advance ↔ durability coupling — the linchpin of "don't let the WAL build up" — is **ours
   by construction** rather than a framework hook to verify. The cost is that we now own the
   pgoutput message model (incl. v2 `Stream *` frames + per-`xid` demux), the standby-status
   feedback loop, and the consistent-snapshot handoff. Build against the message-format spec
   [3][20] and film42's walkthrough [15]; test the streaming/abort and snapshot/stream-boundary
   paths hard. *Top de-risking area of the sink.*
4. **⚠ DuckDB single-writer HA + storage + query path.** PVC vs MotherDuck vs
   DuckLake-over-object-store; backup/restore of `.duckdb` files; how end users query them.
   Two concrete residuals: (a) **loader ownership leases need fencing** — a lease + a
   `ReadWriteOnce` PVC still allows split-brain during table-ownership resharding if a demoted owner
   keeps writing, so carry a **fencing token / generation number**; (b) **version currency** —
   `MERGE INTO` needs DuckDB **≥ 1.4.0 LTS**, whose community support ends **2026-09-16**, so pin
   the latest 1.4.x patch and plan the next-LTS bump rather than shipping bare 1.4.0.
5. **Relaxed cross-table consistency** from one-file-per-table — confirm acceptable for the
   analytics use cases.
6. **Keyless tables — resolved: unsupported.** A `PRIMARY KEY` is mandatory; keyless tables
   are rejected at preflight (no `REPLICA IDENTITY FULL`, no full reloads). Residual decision:
   operator UX for a rejected/quarantined table (hard error vs skip-and-alert) and how to
   inventory existing source tables for PK coverage before onboarding.
7. **Control-plane DB placement** — dedicated instance (recommended) vs a schema in source.
8. **Intra-batch PK churn (`insert → delete → insert` on the same key) — deferred.** When one
   batch/window contains an insert, then a delete, then another insert for the *same* primary
   key, the loader's dedup-to-latest transform needs a precise, verified collapse rule.
   Keeping the highest-LSN op per PK usually yields the right final state, but ordering /
   tombstone edge cases — and PK *reuse* by a genuinely different logical row — are not yet
   formally handled. **Explicitly out of scope for now**; revisit with transform hardening.
9. **Faster initial snapshot / backfill of large tables — deferred.** The consistent snapshot
   ([bootstrap](#startup--bootstrap-fail-fast-preflight)) is the slowest onboarding step
   for big tables. PeerDB's technique for making `pg_dump`/`pg_restore` ~5× faster (parallel,
   chunked **CTID-range** export with tuned settings) is a candidate optimization for our
   snapshot path — parallelize the initial per-table read instead of a single serial copy.
   Out of scope for v1; revisit when snapshot time becomes the bottleneck.
   ([PeerDB: making pg_dump/pg_restore 5× faster](https://blog.peerdb.io/how-can-we-make-pgdump-and-pgrestore-5-times-faster))
10. **Transform vs load cadence (v1 = one poll, two watermarks).** Append-to-raw and
    transform-to-mirror share the loader poll in v1 (two transactions, two watermarks); a slower
    per-table compaction/full-rebuild cadence is separate. Confirm whether the transform ever
    warrants its own independent streaming cadence.
11. **`<table>_raw` schema evolution.** Recommended: additive/preserving — dropped columns
    retained nullable, incompatible type changes widened to `VARCHAR`, renames by `attnum`.
    Confirm; note raw's append-by-name is more exposed to the ⚠ event-trigger gap (Open Q2)
    than the mirror.
12. **`<table>_raw` retention, compaction & growth.** DuckDB `DELETE` only tombstones and
    `VACUUM FULL` is unimplemented, so the single file won't shrink on ordinary deletes;
    reclamation depends on the periodic full-rebuild (`CREATE OR REPLACE`) / `COPY FROM DATABASE`
    actually running. Pick the retention-window default and the compaction cadence — noting the
    full-rebuild needs the **exclusive single writer** and ~**2× transient space**, so it contends
    with that table's apply loop; schedule it low-traffic.
13. **Transform scope & cross-batch ordering (ties to Open Q8).** Incremental scans only
    `commit_lsn > transformed_lsn` but `MERGE`s against the full mirror. Watermarking by **commit
    LSN** (not row LSN) removes the streaming-reorder class of loss; the residual hazard is a
    delete + re-insert straddling the watermark, which still needs a **per-PK max-applied-commit-LSN
    guard** to avoid silently resurrecting a killed row. The periodic full-rebuild is the safety net.

---

## Verification (how we'll prove it works end-to-end, later)

- **Local harness:** docker-compose with Postgres (`wal_level=logical`) + MinIO (S3). Run
  both services; `INSERT/UPDATE/DELETE`, assert Parquet lands in MinIO, the CDC row lands
  **verbatim in `<table>_raw`** (with `walrus_pg_sink_meta` intact), and the derived `<table>`
  row equals the current source row after the transform.
- **Types:** table exercising every mapped type incl. `numeric`, `jsonb`, arrays, `uuid`,
  `timestamptz`, `bytea`, NULLs, and an **unchanged-TOAST** update; assert round-trip fidelity.
- **Intra-batch TOAST carry-forward:** `INSERT big='X'` then `UPDATE …, big=<unchanged-TOAST>`
  for the same PK **within one batch** (mirror empty for that PK); assert the mirror ends with
  `big='X'` (resolved by scanning `<table>_raw` back from the winner's `(commit_lsn, lsn)`), **not** NULL.
- **TRUNCATE:** stream a `TRUNCATE`, then re-`INSERT` rows after it in the same tail; assert the
  transform empties the mirror as of the truncate LSN and keeps only the post-truncate rows —
  and that a user `TRUNCATE` never stalls the pipeline (no NOT-NULL-PK insert of a truncate row).
- **Large txn:** a multi-million-row transaction with `streaming='on'`; assert **memory and
  slot size stay bounded** and the txn appears atomically after commit; then a large txn that
  **aborts** and assert nothing leaks to DuckDB.
- **Commit-order under streaming (regression guard for the row-LSN bug):** run a **large txn that
  commits *after*** a smaller, later-started txn, both touching an overlapping PK, with
  `streaming='on'`; assert the large txn's file is **not skipped** (its `lsn_end` is a commit LSN),
  files apply in `(lsn_end, id)` commit order, and the mirror reflects true last-writer-by-commit —
  **no transaction silently dropped**.
- **Streamed sub-transaction abort:** a committed top-level txn containing a **rolled-back
  savepoint**; assert the rolled-back rows **never reach `<table>_raw`** (only surviving-subxact
  rows materialize).
- **WAL-runaway chaos:** pause the loader, keep writing to source; assert slot grows only to
  the safety cap and alerts fire; resume and assert full catch-up with no loss/dupes.
- **Idle-publication heartbeat:** keep the **published** tables idle while an **unpublished** part
  of the DB churns WAL; assert the **heartbeat** keeps `restart_lsn`/`confirmed_flush_lsn` advancing
  and retained WAL / `wal_status` stay healthy (WAL does **not** grow unbounded).
- **Keepalive vs durability (`wal_sender_timeout`):** stall the S3 flush (or run a long snapshot)
  past `wal_sender_timeout`; assert keepalive feedback keeps the walsender **connected** (no
  `terminating walsender` churn) while `confirmed_flush_lsn` does **not** advance until durable.
- **Crash safety:** kill the sink mid-batch and the loader mid-MERGE; assert
  effectively-once (no loss, no regressions) via checkpoint replay.
- **DDL:** comment mirrored onto `<table>` (not raw); `ADD COLUMN` / `DROP COLUMN` (physical on
  the mirror, **retained-nullable in `<table>_raw`**); **widening** type change casts existing
  mirror rows in place (raw widened / same `ALTER`); **lossy** type change quarantines + alerts;
  rename column/table (both tables); assert each Parquet file carries exactly one `schema_version`
  (homogeneous-file boundary) and both tables' schemas evolve at the correct LSN relative to data.
- **Exported-snapshot backfill boundary:** create the slot with `SNAPSHOT 'export'`, write to the
  source **during** the backfill, and assert the `COPY` runs under `SET TRANSACTION SNAPSHOT` so the
  snapshot/stream boundary dedups with **zero loss/dupes**; assert **equal-`lsn_end` snapshot files
  split across multiple loader batches are all applied** (none skipped by the watermark).
- **Slot loss / total-restart:** delete the slot mid-run → assert epoch bump, new slot,
  re-snapshot of all tables, **both tables per `.duckdb` file rebuilt** (raw re-appended, mirror
  re-derived), **both watermarks reset**; and assert a **transient disconnect does NOT** trigger
  total-restart.
- **Provenance column:** assert every Parquet row has a well-formed `walrus_pg_sink_meta` JSON
  with the full field set, that both `commit_lsn` and `lsn` order correctly as text, that the meta
  is persisted **verbatim into `<table>_raw`** (and `op`/`commit_lsn`/`lsn`/`sink_processed_at`
  promoted to typed columns) yet **absent from the `<table>` mirror** by default, and that the
  transform can drive its merge from the raw log.

---

## Sources

Primary sources are Postgres/AWS/vendor docs and project repos; blogs are corroborating.

1. supabase/etl (formerly `pg_replicate`) — https://github.com/supabase/etl *(primary)*
2. `pgwire-replication` — https://github.com/vnvo/pgwire-replication *(primary)*
3. PostgreSQL — Logical Replication Message Formats / Protocol — https://www.postgresql.org/docs/current/protocol-logical-replication.html *(primary)*
4. PeerDB — Exploring versions of the Postgres logical replication protocol — https://blog.peerdb.io/exploring-versions-of-the-postgres-logical-replication-protocol *(blog)*
5. PostgreSQL — Logical Replication Restrictions — https://www.postgresql.org/docs/current/logical-replication-restrictions.html *(primary)*
6. AWS DMS — Using PostgreSQL as a source (awsdms_ddl_audit / event trigger) — https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html *(primary)*
7. arrow-rs `parquet::arrow` (ArrowWriter/AsyncArrowWriter, readers) — https://docs.rs/parquet/latest/parquet/arrow/index.html *(primary)*
8. arrow-rs `ArrowWriter` — https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/struct.ArrowWriter.html *(primary)*
9. `duckdb-rs` (Appender, `appender-arrow`, `parquet` feature) — https://github.com/duckdb/duckdb-rs *(primary)*
10. DuckDB Rust client docs — https://duckdb.org/docs/current/clients/rust *(primary)*
11. DuckDB — Import from S3 — https://duckdb.org/docs/current/guides/network_cloud_storage/s3_import *(primary)*
12. G. Morling — The Insatiable Postgres Replication Slot — https://www.morling.dev/blog/insatiable-postgres-replication-slot/ *(blog)*
13. G. Morling — confirmed_flush_lsn vs restart_lsn — https://www.morling.dev/blog/postgres-replication-slots-confirmed-flush-lsn-vs-restart-lsn/ *(blog)*
14. PeerDB — Overcoming pitfalls of Postgres logical decoding — https://blog.peerdb.io/overcoming-pitfalls-of-postgres-logical-decoding *(blog)*
15. film42 — Getting Postgres logical replication changes using the pgoutput plugin — https://medium.com/@film42/getting-postgres-logical-replication-changes-using-pgoutput-plugin-b752e57bfd58 *(blog)*
16. enova/`pgl_ddl_deploy` (DDL propagation via event triggers) — https://github.com/enova/pgl_ddl_deploy *(primary)*
17. `connector_arrow` (Postgres→Arrow reference) — https://docs.rs/connector_arrow *(secondary)*
18. Npgsql — Replication — https://www.npgsql.org/doc/replication.html *(primary)*
19. PostgreSQL — Streaming of Large Transactions for Logical Decoding — https://www.postgresql.org/docs/current/logicaldecoding-streaming.html *(primary)*
20. PostgreSQL — Logical Replication Message Formats (Stream Start/Stop/Commit/Abort, per-message xid) — https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html *(primary)*
21. PostgreSQL — `pgoutput.c` (parses `proto_version`/`streaming` from START_REPLICATION; slot has no such attribute) — https://github.com/postgres/postgres/blob/master/src/backend/replication/pgoutput/pgoutput.c *(primary)*

> **Research caveats to remember:** the per-type Postgres→Arrow mapping (⚠ Open Q1) was not
> source-verified; "event triggers fire on all DDL" was **refuted** (⚠ Open Q2); PeerDB's
> internal connector code was used only to corroborate protocol facts, not verified directly.
> `supabase/etl` [1] is cited as **prior art only** — we hand-roll the consumer
> ([§1.2](#12-replication-consumer--hand-rolled)), so its destination statuses don't affect us.
