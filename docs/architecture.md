# Walrus — Postgres WAL → DuckDB Replication Service (Architecture Sketch)

> **Status: design sketch for review.** This is a thorough, cited design intended to be read
> and critiqued before any code is written. Findings are grounded in a deep-research pass
> (24 sources fetched, 25 claims adversarially verified with 3-vote review, 23 confirmed).
> Inline `[n]` markers reference the **[Sources](#sources)** section. Where research could
> *not* confirm something, it is explicitly flagged as **⚠ unverified / spike needed** rather
> than asserted.

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

**Desired outcome.** Two cleanly separated Rust services on Kubernetes:

- a **Postgres Sink** (`walrus-pg-sink`) that streams the WAL in memory, batches changes,
  converts them to Arrow → Parquet, dumps files to S3, and records each file's location +
  LSN range in a Postgres control table — advancing the slot only after that is durable; and
- a **Data Sink** (`walrus-loader`) that polls the control table on a user-chosen cadence,
  pulls Parquet from S3, **appends each change verbatim into a `<table>_raw` CDC log**, then
  **transforms that log into `<table>`** — the current-state mirror. One `.duckdb` file per
  source table still holds; it now contains **two tables** (the raw log + the derived mirror).

This is **near-real-time, not real-time**: the user picks the cadence. The goal is a fast,
backpressure-safe pipeline that never lets the source WAL run away while the loader catches
up on its own schedule.

### Decisions locked in (from requirements)

| Decision | Choice | Consequence for the design |
|---|---|---|
| DuckDB layout | **One `.duckdb` file per source table** — holding **two tables**: `<table>_raw` (append-only CDC log) + `<table>` (derived mirror) | Per-table single-writer loaders → natural parallelism & isolation; each file carries **two watermarks** (`raw_appended_lsn` ≥ `transformed_lsn`); cross-table point-in-time consistency is *relaxed*. |
| Target semantics | **Current-state mirror (upsert/delete)** | The `<table>` mirror is the current state; the loader **first appends CDC rows verbatim to `<table>_raw`** (no dedup, meta retained), **then derives `<table>`** by dedup-to-latest + `MERGE INTO` on the (possibly composite) PK. Makes PK metadata, `REPLICA IDENTITY`, TOAST handling, and the DDL-audit table load-bearing. |
| Transform primitive | **`MERGE INTO` (DuckDB ≥ 1.4.0 LTS)** | Single-statement insert/update/delete from `<table>_raw` → `<table>`; incremental-from-watermark by default, periodic full-rebuild (`CREATE OR REPLACE … AS SELECT`) for self-heal + compaction. `INSERT … ON CONFLICT` + `DELETE` is the < 1.4.0 fallback. |
| Raw retention | **Append-only, retained behind a rolling window** | `<table>_raw` keeps history behind `transformed_lsn` (default ~7 days / last-K batches) for debug + cheap replay; pruned + compacted below the floor. Not prune-immediately-after-transform. |
| History-table key | **`<table>_raw` PK = source PK + `sink_processed_at` + `lsn`** | The source PK repeats across CDC events, so the history table needs its own composite key; `lsn` guarantees uniqueness and enables `ON CONFLICT DO NOTHING` idempotent appends. |
| Bootstrap | **Consistent snapshot → then stream** | Snapshot exported at the slot's creation LSN; a watermark handoff dedups streamed changes against the snapshot. |
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
- Bounded WAL growth under all conditions via correct slot-feedback checkpointing.
- Handle **large transactions** without buffering them entirely in memory (streaming protocol v2) [3][4].
- Capture **DDL / schema evolution** and apply it to the DuckDB targets.
- **User-configurable cadence** for both flush (sink) and apply (loader). The loader apply is **two operations** — append-to-raw then transform-to-mirror — sharing one poll cadence in v1 (separate transactions, two watermarks), with a slower per-table **compaction/full-rebuild** cadence exposed separately.
- Cloud-native: S3 staging, Kubernetes deployment, horizontal-ish scaling by table sharding.

**Non-goals (v1)**
- Real-time / sub-second latency.
- Bi-directional or multi-master replication.
- Cross-table transactional/point-in-time consistency in the target (relaxed by the one-file-per-table choice).
- Replicating non-table objects (functions, sequences-as-truth, roles). Sequences/`TRUNCATE` are in-scope as metadata only.
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
                   │ pgoutput stream (DML + inline DDL-audit INSERTs)
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

The WAL reader. This is the latency- and correctness-critical service.

### 1.1 Source-side setup (one-time, via migration/Job)

```sql
-- Enable on source: wal_level=logical, sufficient max_replication_slots / max_wal_senders.
CREATE PUBLICATION walrus_pub FOR TABLE orders, customers, ...;   -- or FOR ALL TABLES
SELECT pg_create_logical_replication_slot('walrus_slot', 'pgoutput');
-- ^ a PLAIN pgoutput slot. proto_version / streaming are NOT slot properties and cannot be set
--   here — they are per-connection options on START_REPLICATION (below), re-negotiated on every
--   connect. The same slot can be consumed with or without streaming depending on those options.

-- HARD REQUIREMENT: every replicated table MUST have a PRIMARY KEY.
--   PK present  → REPLICA IDENTITY DEFAULT (the PK) gives UPDATE/DELETE their key. ✅
--   No PK       → REJECTED at preflight; the table is NOT added to the publication.
--                 We do not support keyless tables — the only correct alternative is
--                 REPLICA IDENTITY FULL + recurring full reloads, which is out of scope.
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

- **Protocol version (this is the "large transactions" knob).** The replication *slot* is a
  plain `pgoutput` logical slot — there is **no special slot *type*** for large transactions.
  The capability is negotiated **per connection** via options on `START_REPLICATION`:

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
  `proto_version '2'` **alone does not** give you the large-transaction behavior — you need
  the pair. v3 adds two-phase-commit streaming (PG15+), v4 adds *parallel apply* of a single
  large txn (PG16+); neither is needed since we stage to files, so **v2 + `streaming 'on'` is
  the target**. Because `proto_version` is a connection option (not a slot property), you can
  change it by reconnecting — no slot recreation — so make both configurable.
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

### 1.2 Rust crate choice (recommendation)

Research surfaced two viable, primary-sourced Rust foundations [1][2]:

| Option | What it gives us | Trade-off |
|---|---|---|
| **`supabase/etl`** (formerly `pg_replicate`) [1] | High-level framework *over* logical replication: **initial copy + streaming** of insert/update/delete/truncate/schema events, **batching + memory backpressure** (`BatchConfig{max_fill_ms, memory_budget_ratio, max_bytes}`, `MemoryBackpressureConfig`), parallel table sync, retries, and **pluggable destinations** (incl. a `DuckLake` sink over S3-compatible storage) [1]. | Opinionated; we must confirm it advances the slot **only after** our destination acks durability (critical — see ⚠ Open Q3). |
| **`pgwire-replication`** [2] | Lean tokio crate that speaks `START_REPLICATION ... LOGICAL` + pgoutput **directly**, with explicit **`update_applied_lsn(lsn)`** + monotonic standby status updates — i.e. exact control over `confirmed_flush_lsn` advancement [2]. | Lower-level; we implement snapshot, batching, and the pgoutput message model ourselves (see film42's walkthrough [15]). |

**Recommendation:** **Build the sink on `supabase/etl`, implementing a *custom destination*** that does Arrow→Parquet→S3→manifest, because it already solves snapshot + streaming + batching + backpressure + slot management [1]. Keep `pgwire-replication` [2] as the fallback if etl's slot-advance timing can't be coupled to S3+manifest durability — because that coupling is the linchpin of "don't let the WAL build up." **A ~1-day spike to confirm etl's LSN-advance hook is the top de-risking task.**

### 1.3 In-memory batching & cadence

The sink accumulates decoded changes in memory into per-(table, batch) Arrow builders and
flushes a Parquet file when **any** threshold trips — all user-configurable:

- `max_fill_ms` — wall-clock cadence (the primary "how often do we dump files" knob) [1].
- `max_bytes` / `max_rows` — size caps so batches stay bounded [1].
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
    "lsn": "00000000000019A2B3C",    // zero-padded, sort-comparable form of the change LSN
    "commit_lsn": "000000000001A00",
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
  **Everything lives in this one column** so nothing is lost for later lineage/debug. `lsn` is
  emitted **zero-padded / monotonic-comparable** so it orders correctly as text. The loader
  **persists this column verbatim into `<table>_raw`** (the append-only CDC log) and, at append
  time, **promotes `op`, `lsn`, and `sink_processed_at` (the sink time) to typed columns** there —
  `lsn` for the transform's filter/order, and together with the source PK these form the composite
  primary key of the raw history table (see [§2.1](#21-the-raw-to-mirror-transform-model)). The meta is **dropped from the derived `<table>` mirror by
  default** (it's provenance, not current state); it stays queryable in `<table>_raw` for the
  retention window (and in the staged Parquet under the S3 lifecycle TTL). For deletes, only key
  columns are guaranteed populated in the source columns (identity), the rest null.
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

This gives: memory bounded (we stream to disk/S3), WAL bounded (advance on commit), and
correctness — aborted/uncommitted txns never become visible to the loader, and changes are applied
in **commit order**, exactly as non-streaming mode guarantees.

### 1.7 Snapshot / backfill (bootstrap)

Per the "snapshot → then stream" choice:

1. Create the slot; capture its **consistent snapshot LSN** (the slot's creation point).
2. Export existing rows (`COPY`/`SELECT` at that snapshot, or etl's built-in *initial copy*
   which "backfills the existing rows covered by a publication" [1]) → the *same* Arrow →
   Parquet → S3 → manifest path, marked as `snapshot` files with `lsn_end = snapshot_lsn`.
3. **Watermark handoff:** snapshot files are **appended into `<table>_raw`** (marked
   `kind='snapshot'`) alongside streamed files, so raw may briefly hold both the snapshot tail
   and the stream head for the same PK. The **transform** collapses that overlap: dedup-to-latest
   by `lsn` keeps the winning row (last-writer-by-LSN), so the classic snapshot/stream boundary
   dedup falls out of the raw→mirror transform, not a direct MERGE of staged Parquet.

### 1.8 Single slot for life & total-restart

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

1. The sink **bumps the epoch** and creates a **new slot** (capturing a fresh consistent
   snapshot LSN).
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
| `numeric(p,s)` | `Decimal128(p,s)` (p≤38) / `Decimal256` | ⚠ **unconstrained `numeric`** has no fixed p/s → fall back to `Utf8` or max-precision `Decimal256`. |
| `money` | `Decimal128(19,2)` or `Int64` | locale-sensitive; treat carefully. |
| `char/varchar/text` | `Utf8` (`LargeUtf8` if huge) | |
| `bytea` | `Binary` / `LargeBinary` | |
| `uuid` | `FixedSizeBinary(16)` | |
| `json` / `jsonb` | `Utf8` (canonical text) + field metadata tag | DuckDB re-parses to `JSON`; keeping text is simplest and lossless. |
| `date` | `Date32` | |
| `time` | `Time64(Microsecond)` | |
| `timetz` | `Utf8` or `Int64`+offset | ⚠ Arrow has no tz-aware time. |
| `timestamp` | `Timestamp(Microsecond, None)` | |
| `timestamptz` | `Timestamp(Microsecond, Some("UTC"))` | store normalized to UTC. |
| `interval` | `Interval(MonthDayNano)` | |
| arrays `T[]` | `List<T>` | nested arrays → nested `List`. |
| `enum` | `Dictionary(Int32, Utf8)` or `Utf8` | enum labels come from catalog / `Type` messages. |
| composite/row | `Struct<...>` | |
| `inet/cidr/macaddr`, ranges, `tsvector`, `bit`, geometric, `hstore` | `Utf8` fallback (or `Map` for hstore) | ⚠ lossy-to-text unless we invest per-type. |
| **NULL** (any) | Arrow **validity bitmap** (nullable field) | pgoutput signals null vs unchanged-TOAST distinctly — see below. |

**Two correctness gotchas that must be in v1:**

- **Unchanged TOAST columns.** In an `UPDATE`, pgoutput sends an *"unchanged TOAST value"*
  placeholder for large columns that didn't change (not NULL, not the value). `<table>_raw`
  stores that sentinel **verbatim**; the **carry-forward runs in the transform** (raw → mirror),
  resolved **per column**: for any column named in the winning event's `unchanged_toast`, the
  transform substitutes the current `<table>` value so the `MERGE` writes the real value, not the
  sentinel/null. (The periodic full-rebuild unions the current mirror in as an LSN-floor baseline
  so a TOAST value whose last real write was pruned from raw is never lost.)
- **`REPLICA IDENTITY` (PK required).** Updates/deletes only carry key columns; with a
  `PRIMARY KEY` and default replica identity that key *is* the PK (**all columns of a composite
  PK**) — exactly what the MERGE needs. **Keyless tables are not supported** (rejected at preflight, see §1.1 and Non-goals);
  we never fall back to `REPLICA IDENTITY FULL` + full reloads.

Implementation: the `pg-to-arrow` crate maps the source relation schema (from pgoutput
`Relation`/`Type` messages + `information_schema`) to an Arrow schema once per
`schema_version`, cached in the `schema_registry` table.

---

## DDL capture (schema evolution)

Logical decoding **does not emit DDL** [3][5], so we capture it out-of-band using the
**AWS DMS pattern** [6], adapted:

```sql
-- Fixed-schema audit table (AWS DMS awsdms_ddl_audit shape) [6]
CREATE TABLE walrus.ddl_audit (
  c_key    bigserial PRIMARY KEY,
  c_time   timestamp,
  c_user   varchar(64),
  c_txn    varchar(16),
  c_tag    varchar(24),   -- 'CREATE TABLE' | 'ALTER TABLE' | 'DROP TABLE'
  c_oid    integer,
  c_name   varchar(64),
  c_schema varchar(64),
  c_ddlqry text           -- raw DDL text (current_query())
);

-- SECURITY DEFINER function on ddl_command_end writes current_query() into the audit table [6]
CREATE EVENT TRIGGER walrus_intercept_ddl ON ddl_command_end
  EXECUTE PROCEDURE walrus.intercept_ddl();
```

**Key elegance — DDL is ordered *inline* with DML.** Because `walrus.ddl_audit` is an
ordinary table, **its `INSERT`s flow through the same logical replication slot as regular
DML**. The sink therefore sees "schema changed to X at LSN L" **in commit order relative to
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
event triggers "reliably fire on all DDL events" was **refuted (0-3)**. `ddl_command_end`
does not fire for every command, and drops are best captured via the separate **`sql_drop`**
event. Mitigations:
- Add a `sql_drop` event trigger for `DROP`/column drops.
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
| `TRUNCATE` | n/a (stream op) | transform `DELETE`/`TRUNCATE`s mirror rows up to the truncate LSN. | the `t` op is **appended as a logged row** (raw is **not** truncated). |
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
  id             bigserial PRIMARY KEY,
  epoch          bigint NOT NULL,         -- FK → replication_state; namespaces ALL state
  source_schema  text NOT NULL,
  source_table   text NOT NULL,
  s3_uri         text NOT NULL,
  kind           text NOT NULL,           -- 'snapshot' | 'stream'
  row_count      bigint NOT NULL,
  lsn_start      pg_lsn NOT NULL,
  lsn_end        pg_lsn NOT NULL,         -- per-table watermark: all changes <= lsn_end
  schema_version bigint NOT NULL,         -- FK → schema_registry
  status         text NOT NULL DEFAULT 'ready',  -- ready | failed  (applied rows are DELETED, not kept)
  created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON walrus.file_manifest (epoch, source_schema, source_table, lsn_end)
  WHERE status = 'ready';

CREATE TABLE walrus.loader_checkpoint (
  epoch            bigint NOT NULL,
  source_schema    text NOT NULL,
  source_table     text NOT NULL,
  raw_appended_lsn pg_lsn NOT NULL,     -- Phase A: CDC log durable up to here
  transformed_lsn  pg_lsn NOT NULL,     -- Phase B: mirror derived up to here (<= raw_appended_lsn)
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table),
  CHECK (transformed_lsn <= raw_appended_lsn)
);
-- plus: walrus.ddl_manifest (schema-change events w/ LSN), walrus.schema_registry (versioned Arrow/DuckDB schemas)
```

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
idempotent** — replaying a file would duplicate raw rows — so the append is guarded at **file
granularity**: the loader processes files **ordered by `lsn_end`** and **skips any file with
`lsn_end <= raw_appended_lsn`**, then appends and (per above) advances `raw_appended_lsn` +
deletes the queue rows in one control-DB transaction. A crash between the DuckDB append-commit and
the queue delete just leaves the rows in the queue → the file is re-claimed but **skipped** by the
watermark, so no duplicate raw rows — and because `<table>_raw` carries a real composite PK
(source PK + `sink_processed_at` + `lsn`), an `ON CONFLICT DO NOTHING` append is *also* idempotent
at row granularity as a backstop. The **transform**, by contrast, *is* idempotent: it scans
only `<table>_raw` rows with `lsn > transformed_lsn`, dedups to the latest op per PK by `lsn`, and
`MERGE`s (PK-keyed, last-writer-by-LSN) — so a crash between append and transform just re-runs the
transform harmlessly.

**Staged S3 objects ≠ queue rows.** Don't delete the Parquet inline with the row. Put objects
under an **S3 lifecycle TTL** (e.g. 7–30 days) to keep a bounded replay/debug window after the
queue row is gone (set TTL to `0`/delete-inline if you want no replay window). At high volume,
mitigate delete-driven Postgres bloat by tuning autovacuum or **partitioning `file_manifest`
by day and DROPping old partitions** instead of row-by-row deletes.

---

## Component 2 — Data Sink (`walrus-loader`)

Owns the per-table DuckDB files. **One logical worker owns each `.duckdb` file** (DuckDB is
single-writer) — and that one writer owns **both** tables in the file (`<table>_raw` +
`<table>`), which is exactly why one-file-per-table gives clean parallelism.

**Apply loop (per table, on the user's poll cadence) — two phases, two watermarks:**

*Phase A — append to the raw log (`<table>_raw`):*
1. `SELECT ... FROM file_manifest WHERE table=? AND status='ready' AND lsn_end >
   raw_appended_lsn ORDER BY lsn_end` — claim the next batch of files (the `lsn_end` guard makes
   the non-idempotent append safe against replays).
2. For each file in LSN order: `GET` Parquet from S3 (or let DuckDB read it directly via
   `read_parquet` + `httpfs`/`SET s3_*` [10][11]) and **`APPEND` its rows verbatim** into
   `<table>_raw` — no dedup, `walrus_pg_sink_meta` kept, `op`/`lsn` promoted to typed columns.
3. In **one control-DB transaction** (after the DuckDB append commits), advance
   `loader_checkpoint.raw_appended_lsn` **and `DELETE` the claimed `file_manifest` rows** — queue
   semantics: appended files are removed. A repeatedly-failing file is instead set
   `status='failed'` (dead-letter) so it can't block the queue.

*Phase B — transform the raw log into the mirror (`<table>`):*
4. Apply any pending **structural DDL** whose LSN we're about to cross to the `<table>` mirror
   (and, per the [taxonomy](#per-change-type-handling-schema-evolution-semantics), to
   `<table>_raw`), driven by `ddl_manifest` + `schema_registry`, **before** transforming data
   past that LSN.
5. **Dedup-to-latest per PK** over `<table>_raw` rows with `lsn > transformed_lsn` (window
   `row_number() OVER (PARTITION BY pk ORDER BY lsn DESC)`), into a `TEMP` staging table;
   substitute carried-forward values for any `unchanged_toast` columns from the current mirror.
6. **`MERGE INTO <table>`** from the staging table (DuckDB ≥ 1.4.0): `WHEN MATCHED AND op='d'
   THEN DELETE`, `WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED AND op<>'d' THEN INSERT`; `t`
   (truncate) deletes mirror rows up to the truncate LSN. Commit, then advance `transformed_lsn`
   to the max LSN applied.

A slower, per-table **full-rebuild / compaction** job runs on its own cadence to self-heal drift,
reclaim space, and prune `<table>_raw` below the retention floor — see
[§2.1](#21-the-raw-to-mirror-transform-model).

### 2.1 The raw-to-mirror transform model

Each `.duckdb` file holds **two tables** for one source table:

- **`<table>_raw` — the append-only CDC log / history table (bronze).** The verbatim
  union-superset of every source column ever seen (dropped columns kept nullable, incompatible
  type changes widened to `VARCHAR`, renames tracked by `attnum`), **plus** `walrus_pg_sink_meta`
  stored verbatim, **plus** promoted typed `op` / `lsn` / `sink_processed_at` columns. Rows are
  appended, never updated; snapshot rows land here too (`kind='snapshot'`).
  Its **primary key is composite**: the **source table's PK column(s) + `sink_processed_at` (the
  walrus-pg-sink sink time) + `lsn`**. The source PK alone repeats across events (this *is* a
  history log), so it can't be the key; `sink_processed_at` + the source PK is the natural key,
  and **`lsn` is the deterministic tiebreaker that guarantees uniqueness** — two events for one
  source PK can share a millisecond-resolution sink time, but every WAL change has a distinct LSN
  (and snapshot rows are unique by source PK). This real PK also lets the append use
  `INSERT … ON CONFLICT DO NOTHING` for **row-level idempotency**, backing up the file-level
  watermark. *(If PK-index maintenance on the append-hot path proves costly, demote it to a
  `UNIQUE` constraint or a logical-only key — flagged.)*
- **`<table>` — the derived current-state mirror (silver).** Exactly the current source shape;
  the meta column is dropped. Produced *only* by the transform below.

**Transform — incremental (default).** Each cycle, in one atomic DuckDB transaction:

```sql
BEGIN;
CREATE TEMP TABLE _batch AS
  SELECT * EXCLUDE (rn) FROM (
    SELECT *, row_number() OVER (PARTITION BY <pk> ORDER BY lsn DESC) AS rn
    FROM <table>_raw
    WHERE lsn > :transformed_lsn            -- only the new tail
  ) WHERE rn = 1;                           -- latest op per PK (dedup AFTER ranking)
-- carry unchanged_toast columns forward from the current mirror into _batch here
MERGE INTO <table> t USING _batch s ON t.<pk> = s.<pk>
  WHEN MATCHED AND s.op = 'd'      THEN DELETE
  WHEN MATCHED                     THEN UPDATE SET <every non-key col> = s.<col>
  WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (<cols>) VALUES (s.<cols>);
COMMIT;   -- then advance transformed_lsn = max(lsn) applied
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
PK/UNIQUE and a pre-deduped batch, and must list every non-key column in `SET`.

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
  **raw append** is de-duplicated by the `raw_appended_lsn` **file-level watermark** (a verbatim
  append is not itself idempotent), and (b) the **raw→mirror transform** is idempotent (PK-keyed,
  last-writer-by-LSN dedup + `MERGE`).
- **Per-table ordering** via `lsn_end` monotonic watermark; **cross-table ordering is not
  guaranteed** (consequence of one-file-per-table) — acceptable per goals.
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
3. **Publication** — verify `walrus_pub` exists and covers the configured tables (create if
   `manage_publication=true`, else terminal if absent).
4. **Replication slot** — verify the logical slot exists; **read its `restart_lsn` /
   `confirmed_flush_lsn`** to establish the resume position. Absent → create it (capturing the
   snapshot LSN for the initial-load path, [§1.7](#17-snapshot--backfill-bootstrap)) or terminal, per mode.
5. **DDL capture installed** — verify `walrus.ddl_audit` + the `walrus_intercept_ddl`
   (+ `sql_drop`) event triggers exist on the source; install or terminal.
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
| **Health / probes** | `startupProbe` gates the fail-fast [bootstrap](#startup--bootstrap-fail-fast-preflight); `readinessProbe` holds work until bootstrap completes; `livenessProbe` = replication connection alive + slot lag < cap (sink) / apply loop progressing (loader). |
| **Scaling (single-slot constraint)** | **No multi-slot sharding** — one slot for life. The sink is the single-stream ceiling; scale *out* on the **loader** (one worker per table/DuckDB file) and *up* on the sink pod (CPU/decode). If one sink truly can't keep up, that's a capacity conversation, not more slots. |

---

## Observability

Prometheus metrics: replication lag bytes (`pg_current_wal_lsn − confirmed_flush_lsn`),
slot **retained WAL size** (alarm), batch flush latency & Parquet throughput, files
`ready` per table (loader backlog), **raw-append lag** (`sink lsn_end − raw_appended_lsn`),
**transform lag** (`raw_appended_lsn − transformed_lsn`), `<table>_raw` row-count / file-size
growth, DDL events pending, aborted-txn count, failed-file count.
Structured `tracing` logs keyed by `xid`/`lsn`/`batch_uuid`. Grafana dashboard + alerts on
slot growth and loader backlog.

---

## Proposed Rust workspace layout (for when coding starts)

```
walrus/
├── Cargo.toml                 # workspace (name = walrus; member crates are unprefixed)
├── crates/
│   ├── common/                # config, LSN/types, manifest & registry models, errors
│   ├── pg-to-arrow/           # Postgres → Arrow schema+value mapping (the risky spike)
│   ├── control/               # sqlx models for manifest/checkpoint/ddl/registry
│   ├── pg-sink/               # bin: replication consumer → Arrow → Parquet → S3 → manifest
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

Key deps: `tokio`, **`supabase/etl`** (or `pgwire-replication` + `tokio-postgres`) [1][2],
`arrow` + **`parquet`** [7][8], `object_store` (S3), **`duckdb`** (`appender-arrow`,
`parquet`, `bundled`) [9][10], `sqlx` (control DB), `serde`, `tracing`, `metrics`.

---

## Phased roadmap

0. **Scaffold + control plane.** Workspace, control-table migrations, DDL trigger install, docker-compose (Postgres `wal_level=logical` + MinIO).
1. **Thin end-to-end slice.** One table, simple types, streaming-only → Arrow → Parquet → S3 → manifest → loader **appends into `<tbl>_raw`, then transforms into `<tbl>`** in one DuckDB file. Prove both the append and the transform.
2. **Full type mapping + DML correctness.** The [type table](#data-type-translation-postgres--arrow--parquet), NULLs, updates/deletes with `REPLICA IDENTITY` (incl. **composite PKs**); **the raw→mirror transform** (dedup-to-latest, **unchanged-TOAST** carry-forward) + the two-watermark checkpoint.
3. **Snapshot/backfill** with watermark handoff.
4. **DDL / schema evolution** applied in the loader from the audit stream.
5. **Large-transaction streaming** (proto v2): speculative staging + commit/abort gating.
6. **Scale & ops:** loader table-sharding, K8s manifests, leader election/HA, observability, backpressure tuning.
7. **Resilience:** epoch/total-restart on slot loss/invalidation; orphan-slot cleanup.

---

## Open questions & risks

1. **⚠ Postgres→Arrow per-type mapping (highest effort, unverified).** Research confirmed the
   Arrow/Parquet plumbing [7][8] but **not** a canonical per-type mapping. Spike: `numeric`
   (unconstrained precision), `jsonb`, arrays of composites, `enum`, ranges, domains, custom
   types. Owner of most implementation risk.
2. **⚠ Event triggers aren't exhaustive** (verified caveat, refuted 0-3 claim). Need
   `sql_drop` + `Relation`-message reconciliation + periodic schema-diff to avoid drift.
3. **⚠ Slot-advance coupling in `supabase/etl`.** Must confirm etl advances the slot **only
   after** our custom destination acks S3+manifest durability. If not, drop to
   `pgwire-replication`'s explicit `update_applied_lsn` [2]. *Top de-risking spike.*
4. **⚠ DuckDB single-writer HA + storage + query path.** PVC vs MotherDuck vs
   DuckLake-over-object-store; backup/restore of `.duckdb` files; how end users query them.
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
   ([bootstrap](#startup--bootstrap-fail-fast-preconditions)) is the slowest onboarding step
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
    actually running. Pick the retention-window default and the compaction cadence.
13. **Transform scope & cross-batch ordering (ties to Open Q8).** Incremental scans only
    `lsn > transformed_lsn` but `MERGE`s against the full mirror; a delete + re-insert straddling
    the watermark, or a late lower-LSN op after an applied delete, needs a **per-PK
    max-applied-LSN guard** to avoid silently resurrecting a killed row. The periodic full-rebuild
    is the safety net.

---

## Verification (how we'll prove it works end-to-end, later)

- **Local harness:** docker-compose with Postgres (`wal_level=logical`) + MinIO (S3). Run
  both services; `INSERT/UPDATE/DELETE`, assert Parquet lands in MinIO, the CDC row lands
  **verbatim in `<table>_raw`** (with `walrus_pg_sink_meta` intact), and the derived `<table>`
  row equals the current source row after the transform.
- **Types:** table exercising every mapped type incl. `numeric`, `jsonb`, arrays, `uuid`,
  `timestamptz`, `bytea`, NULLs, and an **unchanged-TOAST** update; assert round-trip fidelity.
- **Large txn:** a multi-million-row transaction with `streaming='on'`; assert **memory and
  slot size stay bounded** and the txn appears atomically after commit; then a large txn that
  **aborts** and assert nothing leaks to DuckDB.
- **WAL-runaway chaos:** pause the loader, keep writing to source; assert slot grows only to
  the safety cap and alerts fire; resume and assert full catch-up with no loss/dupes.
- **Crash safety:** kill the sink mid-batch and the loader mid-MERGE; assert
  effectively-once (no loss, no regressions) via checkpoint replay.
- **DDL:** comment mirrored onto `<table>` (not raw); `ADD COLUMN` / `DROP COLUMN` (physical on
  the mirror, **retained-nullable in `<table>_raw`**); **widening** type change casts existing
  mirror rows in place (raw widened / same `ALTER`); **lossy** type change quarantines + alerts;
  rename column/table (both tables); assert each Parquet file carries exactly one `schema_version`
  (homogeneous-file boundary) and both tables' schemas evolve at the correct LSN relative to data.
- **Slot loss / total-restart:** delete the slot mid-run → assert epoch bump, new slot,
  re-snapshot of all tables, **both tables per `.duckdb` file rebuilt** (raw re-appended, mirror
  re-derived), **both watermarks reset**; and assert a **transient disconnect does NOT** trigger
  total-restart.
- **Provenance column:** assert every Parquet row has a well-formed `walrus_pg_sink_meta` JSON
  with the full field set, that `lsn` orders correctly as text, that the meta is persisted
  **verbatim into `<table>_raw`** (and `op`/`lsn` promoted to typed columns) yet **absent from the
  `<table>` mirror** by default, and that the transform can drive its merge from the raw log.

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
> internal connector code was used only to corroborate protocol facts, not verified directly;
> `supabase/etl` destination statuses (DuckLake "in progress") are volatile as of 2026.
