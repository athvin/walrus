# `walrus-loader` — the raw→mirror reconciler, in depth (work-handoff contract · commit-gating · transform correctness · pod lifecycle · performance)

> **Status: design deep-dive, companion to [architecture.md](./architecture.md).** Where
> [`walrus-pg-sink.md`](./walrus-pg-sink.md) is the authoritative spec for the **producer** side (type
> conversion, DDL capture, pod lifecycle of the WAL consumer), this doc is the authoritative spec for the
> **consumer / reconciler** side — the four things `walrus-loader` lives or dies on:
>
> 1. **The work-handoff contract** — *how and when* the loader gets its work from `walrus-pg-sink`, and when
>    that work is transformed ([§2](#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue), [§4](#4-two-phase-apply--append-then-transform)).
> 2. **The commit-gating guarantee** — why the loader **never** processes a file whose transaction has not
>    finally committed, even though `proto_version '2'` lets the sink pull rows off the WAL *before* commit
>    ([§3](#3-commit-gating--the-loader-only-ever-sees-committed-data-by-contract)).
> 3. **Reconciling to source truth** — the raw→mirror transform that makes `<table>` match the *current* shape
>    of the Postgres table, including the intra-batch `insert → delete → insert` case
>    ([§5](#5-the-rawmirror-transform--correctness-in-depth), [§6](#6-intra-batch-pk-churn--insert--delete--insert-worked-in-full)).
> 4. **Running on Kubernetes** — startup, steady state, and graceful shutdown for a stateful, PVC-backed,
>    single-writer service, and how the processing is made **fast** ([§8](#8-kubernetes-pod-lifecycle), [§9](#9-performance--scaling--making-it-fast)).
>
> This doc **extends** `architecture.md`'s loader design (Component 2, §2.1, the coordination contract, Delivery
> semantics) rather than correcting it — that design is mostly right. It makes **one substantive addition**, the
> per-PK max-applied-commit-LSN guard ([§7](#7-straddling-the-watermark--the-per-pk-max-applied-commit-lsn-guard)),
> and it pins down several operationally under-specified points (the ownership lease, stale-lock recovery, the drain
> sequence). **Every such addition is explicitly flagged** — look for **⚠ Extends architecture.md** callouts — so
> what is settled stays distinct from what is proposed. Inline `[n]` markers reference the **[Sources](#sources)**.

---

## Table of contents

- [1. Mission recap — the accuracy authority](#1-mission-recap--the-accuracy-authority)
- [2. The work-handoff contract — the `file_manifest` as a work queue](#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue)
- [3. Commit-gating — the loader only ever sees committed data, by contract](#3-commit-gating--the-loader-only-ever-sees-committed-data-by-contract)
- [4. Two-phase apply — append, then transform](#4-two-phase-apply--append-then-transform)
- [5. The raw→mirror transform — correctness in depth](#5-the-rawmirror-transform--correctness-in-depth)
  - [5.1 The two tables and the composite raw primary key](#51-the-two-tables-and-the-composite-raw-primary-key)
  - [5.2 Dedup-to-latest per primary key (the window)](#52-dedup-to-latest-per-primary-key-the-window)
  - [5.3 Deletes are filtered AFTER ranking (the resurrection guard)](#53-deletes-are-filtered-after-ranking-the-resurrection-guard)
  - [5.4 The MERGE branches (and composite keys)](#54-the-merge-branches-and-composite-keys)
  - [5.5 TRUNCATE — a wipe keyed on the (commit_lsn, lsn) tuple](#55-truncate--a-wipe-keyed-on-the-commit_lsn-lsn-tuple)
  - [5.6 Unchanged-TOAST resolution — the raw back-scan](#56-unchanged-toast-resolution--the-raw-back-scan)
  - [5.7 DDL at the right LSN, and incremental vs full-rebuild](#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild)
- [6. Intra-batch PK churn — insert → delete → insert, worked in full](#6-intra-batch-pk-churn--insert--delete--insert-worked-in-full)
  - [6.1 The primary case, worked step by step](#61-the-primary-case-worked-step-by-step)
  - [6.2 The variant matrix](#62-the-variant-matrix)
  - [6.3 Why it scales — set-based, single-pass](#63-why-it-scales--set-based-single-pass)
  - [6.4 Why it is unit-testable by construction](#64-why-it-is-unit-testable-by-construction)
- [7. Straddling the watermark — the per-PK max-applied-commit-LSN guard](#7-straddling-the-watermark--the-per-pk-max-applied-commit-lsn-guard)
- [8. Kubernetes pod lifecycle](#8-kubernetes-pod-lifecycle)
  - [8.1 Topology and the single-writer problem](#81-topology-and-the-single-writer-problem)
  - [8.2 Startup — the ordered, fail-fast bootstrap](#82-startup--the-ordered-fail-fast-bootstrap)
  - [8.3 Probes — readiness, liveness, and the catch-up-lag trap](#83-probes--readiness-liveness-and-the-catch-up-lag-trap)
  - [8.4 Steady state — the apply loop, cadences, and the PDB](#84-steady-state--the-apply-loop-cadences-and-the-pdb)
  - [8.5 Graceful shutdown — the SIGTERM drain](#85-graceful-shutdown--the-sigterm-drain)
  - [8.6 Decommission and node drain](#86-decommission-and-node-drain)
- [9. Performance & scaling — making it fast](#9-performance--scaling--making-it-fast)
  - [9.1 The governing constraint](#91-the-governing-constraint)
  - [9.2 The levers](#92-the-levers)
  - [9.3 The cadence dials](#93-the-cadence-dials)
  - [9.4 Retention and compaction realities](#94-retention-and-compaction-realities)
  - [9.5 Deferred — multi-pod horizontal scale-out](#95-deferred--multi-pod-horizontal-scale-out)
- [10. What this doc extends in architecture.md](#10-what-this-doc-extends-in-architecturemd)
- [Sources](#sources)

---

## 1. Mission recap — the accuracy authority

`walrus-loader`'s one job (from [architecture.md §2](./architecture.md#component-2--data-sink-walrus-loader)):
turn the sink's staged stream of change events into a DuckDB `<table>` **mirror that matches the source table as
it currently stands in Postgres** — same rows, same current values, same schema. **Real-time is explicitly not the
goal; correctness is.** The loader may run as far behind the cadence as it needs to, but when it settles, `<table>`
must be a faithful current-state copy. When "fast" and "correct" conflict, the loader chooses **correct** — it is
the accuracy authority of the system, and the periodic full-rebuild ([§5.7](#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild),
[§9.4](#94-retention-and-compaction-realities)) exists precisely to self-heal any drift back to source truth.

Four facts frame everything below:

- **The loader lives downstream of S3 and consumes a *work queue*, not a stream.** It never opens a replication
  connection, never decodes pgoutput, never demultiplexes `xid`s, and — critically — **never sees an uncommitted
  row** ([§3](#3-commit-gating--the-loader-only-ever-sees-committed-data-by-contract)). All of that is the sink's
  job. The loader's entire input is the `walrus.file_manifest` control table plus the Parquet it points at.
- **One `.duckdb` file per source table, holding *two* tables.** DuckDB is single-writer [11], so one logical
  worker owns each `.duckdb` file, and that worker owns **both** tables in it: `<table>_raw` (the append-only CDC
  log, *bronze*) and `<table>` (the derived current-state mirror, *silver*). One-file-per-table is what makes clean
  per-table parallelism possible ([§9.2](#92-the-levers)).
- **Two operations, two watermarks.** Every apply cycle is **Phase A** (append verbatim to `<table>_raw`) then
  **Phase B** (transform `<table>_raw` → `<table>`), tracked by two commit-LSN-valued watermarks,
  `raw_appended_lsn ≥ transformed_lsn` ([§4](#4-two-phase-apply--append-then-transform)).
- **Every restart is a resume.** Durable checkpoints (the two watermarks) plus fail-fast bootstrap make pod churn a
  non-event: a replacement loader picks up exactly where the last one stopped, with no data loss and no manual
  intervention ([§8](#8-kubernetes-pod-lifecycle)).

**What this doc owns vs the sink doc:**

| Concern | Authoritative doc |
|---|---|
| pgoutput decode, `proto_version '2'` / `streaming 'on'`, `Stream Commit`/`Abort`, slot feedback | [`walrus-pg-sink.md`](./walrus-pg-sink.md) + [`proto-version.md`](./proto-version.md) |
| Postgres → Arrow → Parquet type conversion, the type descriptor | [`walrus-pg-sink.md` §2](./walrus-pg-sink.md#2-data-type-conversion-postgres--arrow--parquet--duckdb) |
| DDL capture on the source (event triggers, audit table) | [`walrus-pg-sink.md` §3](./walrus-pg-sink.md#3-ddl-capture--the-sinks-tap-on-the-source) |
| **Work-handoff contract, commit-gating, two-phase apply, raw→mirror transform, loader K8s lifecycle & scaling** | **this doc** |

---

## 2. The work-handoff contract — the `file_manifest` as a work queue

The hand-off from sink to loader is one control-plane table: `walrus.file_manifest`. Understanding it as a **work
queue, not a history** is the key to the whole contract.

### The queue, its lifecycle, and how the loader claims work

A `file_manifest` row is **written `ready` by the sink** (after its Parquet is durable in S3 and only after the
transaction committed — [§3](#3-commit-gating--the-loader-only-ever-sees-committed-data-by-contract)), **claimed by
the loader**, and **DELETED the instant its rows are durably appended to `<table>_raw`** — never flipped to a
"done" terminal state. So what remains in the queue is exactly the un-applied (or crash-window) tail. This is the
opposite of `ddl_manifest` / `schema_registry`, which **are** history and are **never pruned** — they are the
schema record needed to reconstruct a table at any `schema_version`.

The relevant control tables (reproduced from
[architecture.md coordination contract](./architecture.md#coordination-contract-control-plane-tables), annotated for
what the *loader* reads):

```sql
CREATE TABLE walrus.file_manifest (
  id             bigserial PRIMARY KEY,   -- tiebreaker for equal-lsn_end files (esp. snapshot files)
  epoch          bigint NOT NULL,         -- generation; namespaces ALL state (see architecture §1.8)
  source_schema  text NOT NULL,
  source_table   text NOT NULL,
  s3_uri         text NOT NULL,           -- the Parquet the loader GETs / read_parquet()s
  kind           text NOT NULL,           -- 'snapshot' | 'stream'
  row_count      bigint NOT NULL,
  lsn_start      pg_lsn NOT NULL,
  lsn_end        pg_lsn NOT NULL,         -- COMMIT LSN of the file's LAST txn — NOT max row lsn (see below)
  schema_version bigint NOT NULL,         -- gates DDL application (§5.7)
  status         text NOT NULL DEFAULT 'ready',   -- ready | failed  (applied rows are DELETED, not kept)
  created_at     timestamptz NOT NULL DEFAULT now()  -- UTC
);
CREATE INDEX ON walrus.file_manifest (epoch, source_schema, source_table, lsn_end, id)
  WHERE status = 'ready';                 -- the loader's claim index

CREATE TABLE walrus.loader_checkpoint (
  epoch            bigint NOT NULL,
  source_schema    text NOT NULL,
  source_table     text NOT NULL,
  raw_appended_lsn pg_lsn NOT NULL,       -- Phase A: <table>_raw durable up to this COMMIT LSN
  transformed_lsn  pg_lsn NOT NULL,       -- Phase B: <table> mirror derived up to this COMMIT LSN
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table),
  CHECK (transformed_lsn <= raw_appended_lsn)   -- mirror never ahead of the log
);
```

The loader's **claim query** (Phase A, step 1) is load-bearing:

```sql
SELECT id, s3_uri, kind, lsn_end, schema_version
FROM   walrus.file_manifest
WHERE  epoch = :epoch AND source_schema = :schema AND source_table = :table
  AND  status = 'ready'
ORDER  BY lsn_end, id          -- COMMIT-LSN order; id breaks ties (snapshot files share consistent_point)
LIMIT  :max_files_per_cycle;   -- batch several files per cycle to amortize overhead (§9.2)
```

Note what the filter is **not**: it is **not** `lsn_end > raw_appended_lsn`. Such a predicate would wrongly skip
the many **snapshot files that all share `consistent_point`** as their `lsn_end`
([architecture.md §1.7](./architecture.md#17-snapshot--backfill-bootstrap)). What advances the frontier is the
**queue deletion** (an applied file is gone and cannot be re-claimed), backstopped by row-level idempotency
([§4](#4-two-phase-apply--append-then-transform)). The watermarks exist for **ordering and resume**, not dedup.

### Why `lsn_end` is a COMMIT LSN, never a max row LSN (correctness-critical)

Postgres logical decoding delivers changes in **commit order** [1], but each change's *row* LSN is its WAL-write
position. With `streaming 'on'`, a large transaction that opens early and commits late can carry row LSNs
**entirely below** the commit LSN of a smaller transaction that committed first. This is not hypothetical — it is
measured in [`proto-version.md` §10](./proto-version.md#10-interleaving-and-commit-ordering): two concurrent 6000-row
transactions where `xid=862` *started second but committed first*.

If files were ordered or watermarked by **max row LSN**, that late large-txn file (whose max row LSN is *lower*)
would sort **before** — and be **skipped by** — a watermark that the smaller, earlier-committed txn already advanced
past: **the entire transaction silently dropped.** So the contract pins three things together:

| | row LSN | commit LSN (`lsn_end`) |
|---|---|---|
| Txn A: opens first, 5M rows, commits **second** | rows `0/1000`…`0/9000` | `0/E000` |
| Txn B: opens second, small, commits **first** | rows `0/A000`…`0/A0F0` | `0/B000` |
| Order by **max row LSN** | A (`0/9000`) **before** B (`0/A0F0`) → a `0/B000` watermark skips A ❌ | — |
| Order by **commit LSN** | — | B (`0/B000`) **before** A (`0/E000`) ✅ monotonic with delivery |

Therefore: `lsn_end` is defined as the **commit LSN of the file's last transaction**, the loader claims and orders
files by **`(lsn_end, id)`**, and both `loader_checkpoint` watermarks are commit-LSN valued. The per-row `lsn`
survives **only inside `<table>_raw`**, as the per-PK last-writer *tiebreaker* — safe because row-level locking
serializes writes to a single PK, so per-PK row-LSN order equals commit order ([§5.2](#52-dedup-to-latest-per-primary-key-the-window)).

### The handoff, end to end

```
 walrus-pg-sink                         S3                    walrus-loader (per table)
 ──────────────                       ──────                  ─────────────────────────
 decode → Arrow → Parquet ──PUT──▶  <epoch>/<tbl>/*.parquet
 INSERT file_manifest(status='ready', lsn_end=<commitLSN>)
                                                     claim WHERE status='ready' ORDER BY lsn_end,id
                                       ◀────GET/read_parquet────  Phase A: APPEND verbatim → <table>_raw
                                                                  ┌ one control-DB txn:
                                                                  │  UPDATE raw_appended_lsn = max(lsn_end)
                                                                  └  DELETE the claimed manifest rows
                                                                  Phase B: transform → <table>, then
                                                                           UPDATE transformed_lsn
```

Cross-refs: [`proto-version.md` §10](./proto-version.md#10-interleaving-and-commit-ordering) (the empirical proof
that commit order ≠ start order); [architecture.md Delivery semantics](./architecture.md#delivery-semantics-ordering--idempotency)
(per-table ordering by commit LSN, never row LSN).

---

## 3. Commit-gating — the loader only ever sees committed data, by contract

> *"We wouldn't want to process any files that haven't had a final commit sent to them, since we're using
> `proto_version '2'` and can get files off the WAL before the commit has happened."*

This is the correct instinct, and the design honors it fully — but it is important to be precise about **which
service enforces it**, because the answer is the cleanest separation-of-concerns line in the whole system.

### The correct mental model: the sink commit-gates; the loader trusts the contract

Under `streaming 'on'`, the sink **does** receive rows of a large transaction *before* it commits — that is the
entire point of streaming ([`proto-version.md` §8](./proto-version.md#8-streaming-how-a-big-transaction-is-chopped-up),
[architecture.md §1.6](./architecture.md#16-large-transaction-safety)). To bound memory, the sink may even stage
those speculative rows to S3. **But it does not write a `ready` manifest row until it has seen the top-level
`Stream Commit` for that transaction.** Concretely, the sink guarantees ([`proto-version.md` §13](./proto-version.md#13-the-consumer-contract-for-walrus)):

- streamed changes stay invisible until `Stream Commit`;
- on `Stream Abort {sub == top}` (whole-txn rollback), the speculative S3 files are deleted and the buffer dropped;
- on `Stream Abort {sub != top}` (a rolled-back savepoint *inside* a committed txn — the dangerous case in
  [`proto-version.md` §9b](./proto-version.md#9-abort-and-rollback--the-case-that-can-corrupt-a-mirror)), the sink
  **excludes exactly that sub-xid's rows** when it materializes the `ready` file.

**Consequence:** a `ready` `file_manifest` row references Parquet that contains **only committed, surviving rows**.
So the loader's *entire* side of the guarantee is one rule: **read only `status='ready'`.** The loader has no `xid`
demux, no `Stream Abort` handling, no speculative state, and it must never try to re-derive commit fate — it trusts
the contract. The vivid proof: in [`proto-version.md` §9b](./proto-version.md#9-abort-and-rollback--the-case-that-can-corrupt-a-mirror),
a committed transaction with a rolled-back savepoint **streamed 8762 insert messages but only 6000 survive** — the
loader sees exactly the 6000 that landed in `ready` files, never the 2762.

### The loader's defensive posture (checklist)

Even trusting the contract, the loader stays disciplined so it can never reintroduce an uncommitted row:

1. **Never read a non-`ready` row.** Not `failed`, not any absent/other status. The claim query hard-filters
   `status = 'ready'` ([§2](#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue)).
2. **Order strictly by commit LSN** `(lsn_end, id)`, so even correct, committed files apply in commit order — the
   only order that is monotonic with delivery.
3. **Treat `failed` (dead-lettered) files as poison** — alert on them, never apply them. A repeatedly-failing file
   is moved aside as `status='failed'` by the loader itself so a poison file cannot block the queue; it is never
   silently retried into the mirror.
4. **Append verbatim.** Phase A adds no interpretation, filtering, or synthesis that could resurrect a row the sink
   already excluded. The only rows in `<table>_raw` are the ones the sink put in a `ready` file.

### Who owns what (the trust boundary)

```
        COMMIT FATE DECIDED HERE (sink)          │ only committed │   ASSUMES COMMITTED (loader)
 ───────────────────────────────────────────────┤    Parquet     ├──────────────────────────────────
  Stream Start/Stop/Commit/Abort, per-xid demux, │    crosses     │  claim status='ready',
  speculative staging, subtxn-abort exclusion,   │  ───► S3 ───►  │  append verbatim → <table>_raw,
  write `ready` ONLY after Stream Commit          │                │  dedup + MERGE → <table>
```

| Concern | Sink | Loader | Enforced by |
|---|---|---|---|
| Commit-gate visibility (`Stream Commit`) | ✅ | — | pgoutput frames; `ready` written post-commit |
| Whole-txn abort discard (`Stream Abort` sub==top) | ✅ | — | delete speculative S3 files, drop buffer |
| Rolled-back savepoint discard (sub≠top) | ✅ | — | exclude sub-xid rows from the `ready` file |
| Per-`xid` demultiplex | ✅ | — | hand-rolled decoder |
| Never advance `confirmed_flush_lsn` past an open txn | ✅ | — | standby status feedback |
| Write the `ready` manifest row | ✅ | — | durability checkpoint |
| **Read only `ready` rows, in commit order** | — | ✅ | the claim query |
| Append verbatim (no re-interpretation) | — | ✅ | Phase A |
| Dedup-to-latest + `MERGE` to current state | — | ✅ | Phase B |

Cross-refs: [`proto-version.md` §9](./proto-version.md#9-abort-and-rollback--the-case-that-can-corrupt-a-mirror),
[§13](./proto-version.md#13-the-consumer-contract-for-walrus); [architecture.md §1.6](./architecture.md#16-large-transaction-safety).

---

## 4. Two-phase apply — append, then transform

Every apply cycle, per table, is two phases with two independent watermarks. They share one poll cadence in v1
(two transactions), with a slower compaction cadence exposed separately ([§9.3](#93-the-cadence-dials)).

### Phase A — append verbatim to `<table>_raw`

1. **Claim** the next `ready` files in `(lsn_end, id)` order ([§2](#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue)).
2. For each file in LSN order, DuckDB reads the Parquet — either `read_parquet` from S3 directly (`httpfs`/`SET
   s3_*` [8][9]) or via the `duckdb-rs` Appender API [10] ([§9.2](#92-the-levers)) — and **appends its rows verbatim**
   into `<table>_raw`: no dedup, `walrus_pg_sink_meta` kept verbatim, and `op` / `commit_lsn` / `lsn` /
   `sink_processed_at` **promoted to typed columns**. Row-level idempotency comes from `ON CONFLICT DO NOTHING` on
   `<table>_raw`'s composite PK (source PK + `sink_processed_at` + `lsn` — [§5.1](#51-the-two-tables-and-the-composite-raw-primary-key)):

   ```sql
   INSERT INTO <table>_raw
   SELECT *,                                        -- source columns + walrus_pg_sink_meta (verbatim)
          json_extract_string(walrus_pg_sink_meta,'$.op')                AS op,
          json_extract_string(walrus_pg_sink_meta,'$.commit_lsn')        AS commit_lsn,
          json_extract_string(walrus_pg_sink_meta,'$.lsn')               AS lsn,
          json_extract_string(walrus_pg_sink_meta,'$.sink_processed_at') AS sink_processed_at
   FROM   read_parquet(:s3_uri)
   ON CONFLICT DO NOTHING;                          -- row-level idempotency (crash-window safe)
   ```
3. **After the DuckDB append commits**, in **one control-DB transaction**, advance the watermark **and** delete the
   claimed queue rows together — the crash-safe ordering across two databases (DuckDB and control Postgres cannot
   share a transaction):

   ```sql
   BEGIN;
     UPDATE walrus.loader_checkpoint
        SET raw_appended_lsn = :max_claimed_lsn_end, updated_at = now()
      WHERE epoch=:epoch AND source_schema=:schema AND source_table=:table;
     DELETE FROM walrus.file_manifest WHERE id = ANY(:claimed_ids);
   COMMIT;
   ```

### Phase B — transform `<table>_raw` → `<table>`

Runs off `<table>_raw` (it **never re-reads the manifest**), applies pending DDL and the transform
([§5](#5-the-rawmirror-transform--correctness-in-depth)), commits, then advances `transformed_lsn` to the max
commit LSN applied. Because it reads only `commit_lsn > transformed_lsn`, it is naturally idempotent — a crash
between Phase A and Phase B just re-runs it harmlessly.

### Effectively-once = two guarantees, not one MERGE

At-least-once on both hops (WAL→S3 and S3→DuckDB) becomes effectively-once through **two** distinct guards. It
matters to be precise about which does what:

- **Append idempotency** rests on (1) the **work-queue deletion** (an applied file is gone, cannot be re-claimed)
  **plus** (2) the **row-level `ON CONFLICT DO NOTHING`** on `<table>_raw`'s real composite PK. Guard (2) is
  **load-bearing, not a backstop**: in the crash window between the DuckDB append-commit and the control-DB
  txn, the file is still queued and *will* be re-claimed and re-appended, and the file-level watermark does **not**
  cover that window (it was never advanced, precisely because that txn didn't commit). Do **not** demote that PK to
  a non-enforced key without an equivalent row-dedup.
- **Transform idempotency** is natural: it scans only `commit_lsn > transformed_lsn`, dedups by `(commit_lsn DESC,
  lsn DESC)`, and `MERGE`s — so re-running it produces the same winners.

**Crash-window analysis** (every failure point resolves to a re-run that is safe):

| Crash point | State on restart | What re-runs | What keeps it correct |
|---|---|---|---|
| Mid Phase-A append (DuckDB txn open) | DuckDB rolls back; file still `ready` in queue | re-claim + re-append | clean append (nothing partially committed) |
| After DuckDB append-commit, **before** control-DB txn | file still `ready`; `raw_appended_lsn` **not** advanced | re-claim + re-append the same file | **row-level `ON CONFLICT DO NOTHING`** (the watermark can't help here) |
| After control-DB txn, before Phase B | file gone; `transformed_lsn` behind `raw_appended_lsn` | Phase B over the tail | **idempotent transform** (`commit_lsn > transformed_lsn`) |
| Mid Phase-B `MERGE` | `transformed_lsn` not advanced | whole window re-transforms | LWW dedup → same winners |

Cross-refs: [architecture.md coordination contract](./architecture.md#coordination-contract-control-plane-tables)
("that row-level PK is therefore load-bearing, not a mere backstop"),
[Delivery semantics](./architecture.md#delivery-semantics-ordering--idempotency),
[`walrus-pg-sink.md` §4.6](./walrus-pg-sink.md#46-the-loaders-shutdown-differs).

---

## 5. The raw→mirror transform — correctness in depth

This is where accuracy is won. The transform reduces the append-only CDC log into the exact current state of the
source table. It is **pure DuckDB SQL** parameterized only by the table, its primary-key column list, and
`:transformed_lsn` — which is what makes it fast ([§9.2](#92-the-levers)) *and* unit-testable
([§6.4](#64-why-it-is-unit-testable-by-construction)).

### 5.1 The two tables and the composite raw primary key

| | `<table>_raw` — the CDC log (bronze) | `<table>` — the mirror (silver) |
|---|---|---|
| Contents | verbatim union-superset of every source column ever seen (dropped cols kept nullable, incompatible type changes widened to `VARCHAR`, renames tracked by `attnum`) + `walrus_pg_sink_meta` verbatim + promoted `op` / `commit_lsn` / `lsn` / `sink_processed_at` | exactly the current source shape; meta dropped |
| Rows | append-only; snapshot rows too (`kind='snapshot'`); never updated | one row per PK; produced **only** by the transform |
| Primary key | **composite: source PK + `sink_processed_at` + `lsn`** | the source primary key |
| Order/watermark key | `commit_lsn` (delivery order); `lsn` = intra-txn tiebreaker | — |

The raw PK deserves emphasis: the source PK alone repeats across CDC events (this *is* a history log), so it cannot
be the key. `sink_processed_at` + source PK is the natural key, and **`lsn` is the deterministic tiebreaker that
guarantees uniqueness** (every WAL change has a distinct LSN; snapshot rows are unique by source PK). This real PK
is what enables the `ON CONFLICT DO NOTHING` that makes Phase A crash-window-safe
([§4](#4-two-phase-apply--append-then-transform)). If PK-index maintenance on the append-hot path proves costly it
may be a `UNIQUE` constraint instead — but it must stay **enforced**.

The loader rebuilds Tier-2/Tier-3 types (interval, range, enum, bit, char length, …) from the per-column **type
descriptor** the sink wrote into `schema_registry` — see
[`walrus-pg-sink.md` §2.6](./walrus-pg-sink.md#26-the-type-mapping-descriptor-how-the-loader-rebuilds-exactly).

### 5.2 Dedup-to-latest per primary key (the window)

The winner for each PK is the row maximizing the tuple `(commit_lsn, lsn)` among non-truncate rows above the floor.
DuckDB expresses this idiomatically with a window plus `QUALIFY` [4] and `EXCLUDE` [5]:

```sql
-- the un-transformed tail, deduped to the latest op per PK:
CREATE TEMP TABLE _batch AS
  SELECT * EXCLUDE (rn) FROM (
    SELECT *, row_number() OVER (PARTITION BY <pk> ORDER BY commit_lsn DESC, lsn DESC) AS rn
    FROM <table>_raw
    WHERE commit_lsn > :transformed_lsn      -- only the un-transformed tail
      AND op <> 't'                          -- truncate handled separately (§5.5)
  ) WHERE rn = 1;
```

`commit_lsn` orders by **delivery/commit order**; row `lsn` breaks ties within one transaction (and orders
same-commit ops, e.g. a same-txn `TRUNCATE` then `INSERT`). Row `lsn` as the intra-txn tiebreaker is safe because
row-level locking serializes writes to a single PK, so per-PK row-LSN order equals commit order.

### 5.3 Deletes are filtered AFTER ranking (the resurrection guard)

This is the single most important ordering subtlety in the transform. The window **keeps** delete rows (`op='d'`)
while ranking; the delete-vs-keep decision is made on the **winner**, in the `MERGE` ([§5.4](#54-the-merge-branches-and-composite-keys)).
**Pre-filtering `op='d'` *before* the window would let a superseded earlier insert become the winner and resurrect a
deleted key.** The worked proof is [§6.1](#61-the-primary-case-worked-step-by-step). Remember the rule as:
*rank everything, then let the winner's `op` decide.*

### 5.4 The MERGE branches (and composite keys)

The deduped `_batch` is applied to the mirror in a single `MERGE INTO` [3] (DuckDB ≥ 1.4.0 LTS):

```sql
MERGE INTO <table> t USING _batch s ON t.<pk> = s.<pk>
  WHEN MATCHED AND s.op = 'd'      THEN DELETE
  WHEN MATCHED                     THEN UPDATE SET <every non-key col> = s.<col>
  WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (<cols>) VALUES (s.<cols>);
-- COMMIT, then advance transformed_lsn = max(commit_lsn) applied (including any truncate commit_lsn)
```

The three branches encode the **collapse rule**: winner `op ∈ {i,u}` → the key's row becomes the winner tuple
(INSERT if absent, UPDATE all non-key cols if present); winner `op='d'` → the key is absent (DELETE if present,
**no-op if absent** — the `NOT MATCHED AND s.op<>'d'` guard makes a delete-with-no-match do nothing).

**Composite PK generalization.** `<pk>` denotes the *full* primary-key column list, generated from
`schema_registry` — never assuming a single column: `PARTITION BY k1, k2, …`; `ON t.k1=s.k1 AND t.k2=s.k2 …`; and
the `ON CONFLICT (k1, k2, …)` fallback targets the composite constraint. **⚠ Guardrail:** never let a normalized
type into the partition/join key — DuckDB normalizes `INTERVAL` for equality/ordering, so two byte-different
intervals can compare equal ([`walrus-pg-sink.md` §2.4](./walrus-pg-sink.md#24-tier-2-decompositions-column-by-column)).
This is a non-risk since the key is always the source PK, but state it.

**Fallback (DuckDB < 1.4.0).** `INSERT … ON CONFLICT (pk) DO UPDATE` **plus a separate `DELETE`** — it needs a
PK/UNIQUE on the target and a pre-deduped batch, and must list every non-key column in `SET`. The TRUNCATE pre-step
([§5.5](#55-truncate--a-wipe-keyed-on-the-commit_lsn-lsn-tuple)) and the TOAST back-scan
([§5.6](#56-unchanged-toast-resolution--the-raw-back-scan)) apply identically to the fallback.

The transform ships as a **single parameterized SQL template** (`const &str` / `.sql` in `crates/loader`) so the
loader and its unit tests share one source of truth ([§6.4](#64-why-it-is-unit-testable-by-construction)).

### 5.5 TRUNCATE — a wipe keyed on the (commit_lsn, lsn) tuple

A pgoutput `TRUNCATE` carries **no tuple/PK** ([`proto-version.md` §4](./proto-version.md#4-the-message-catalog-decoded-byte-by-byte)),
so it cannot be a `MERGE` join branch. It is handled as a wipe **before** the dedup/MERGE:

```sql
-- (Ct, Lt) := the (commit_lsn, lsn) TUPLE of the LATEST truncate in the tail (both NULL if none):
--   SELECT commit_lsn, lsn FROM <table>_raw
--   WHERE commit_lsn > :transformed_lsn AND op='t' ORDER BY commit_lsn DESC, lsn DESC LIMIT 1;
DELETE FROM <table> WHERE :Ct IS NOT NULL;   -- wipe the mirror as of the truncate; no-op if no truncate
```

Then the dedup window ([§5.2](#52-dedup-to-latest-per-primary-key-the-window)) adds
`AND (:Ct IS NULL OR (commit_lsn, lsn) > (:Ct, :Lt))` so only rows **strictly after the truncate tuple** repopulate
the mirror. **The boundary is the tuple `(commit_lsn, lsn)`, not the scalar `commit_lsn`.** A same-transaction
`TRUNCATE; INSERT …` shares one `commit_lsn`, so a `commit_lsn > Ct` filter would wrongly drop the post-truncate
inserts; `(commit_lsn, lsn) > (Ct, Lt)` keeps them. `transformed_lsn` still advances to include the truncate's
`commit_lsn`. In `<table>_raw`, the `t` op is retained as a logged row — raw is never truncated.

Cross-refs: [architecture.md DDL taxonomy — TRUNCATE row](./architecture.md#per-change-type-handling-schema-evolution-semantics);
[Verification "Same-commit TRUNCATE-then-INSERT"](./architecture.md#verification-how-well-prove-it-works-end-to-end-later).

### 5.6 Unchanged-TOAST resolution — the raw back-scan

pgoutput's `'u'` placeholder means a large (out-of-line TOAST) column **was not modified, so its value is absent
from the wire** — distinct from a real SQL `NULL` (`'n'`) ([`proto-version.md` §5](./proto-version.md#5-tupledata-and-the-unchanged-toast-placeholder)).
`<table>_raw` stores the sentinel verbatim; the **transform resolves it**, per column: for each column named in the
winner's `unchanged_toast` list, substitute the **last non-sentinel value for that PK, found by scanning
`<table>_raw` backward from the winner's `(commit_lsn, lsn)`** — falling back to the current `<table>` value only as
a last resort.

Why the back-scan (not "just read the current mirror"): when the write that set the TOAST value and a later
unchanged-TOAST update land in the **same batch** — `INSERT big='X'` @ `commit_lsn=100`, then `UPDATE …,
big=<sentinel>` @ `commit_lsn=200`, same PK — the mirror has **no row for that PK yet**, so a mirror-only lookup
writes `NULL` and silently drops `'X'`. The back-scan finds `'X'` still sitting in raw at LSN 100. Resolution:
`COALESCE(latest raw value ≤ winner's (commit_lsn, lsn) where the column ≠ sentinel, current mirror value)`. The
periodic full-rebuild additionally unions the current mirror as an LSN-floor baseline, so a TOAST value whose last
real write was already pruned is still never lost.

Cross-refs: [`walrus-pg-sink.md` §2.7](./walrus-pg-sink.md#27-special-values-nulls-and-the-gotchas-we-inherit);
[architecture.md §2.1 TOAST gotcha](./architecture.md#data-type-translation-postgres--arrow--parquet).

### 5.7 DDL at the right LSN, and incremental vs full-rebuild

**DDL is applied before crossing its LSN.** Because the sink cuts a fresh Parquet file at every structural schema
change (the *homogeneous-file rule* — every file carries exactly one `schema_version`,
[`walrus-pg-sink.md` §3.5](./walrus-pg-sink.md#35-how-the-sink-consumes-it)), Phase B applies any pending structural
DDL from `ddl_manifest` + `schema_registry` to `<table>` (and, per the taxonomy — add/rename/lossless-widen — also
to `<table>_raw`) **before** transforming data past that LSN. The mirror is the *exact* current shape (physical
drops, in-place widening casts); `<table>_raw` is the *additive* superset that never destructively drops or casts
history. Full per-change-type behavior:
[architecture.md DDL taxonomy table](./architecture.md#per-change-type-handling-schema-evolution-semantics).

**Incremental (default) vs full-rebuild (self-heal):**

| | Incremental transform | Periodic full-rebuild / compaction |
|---|---|---|
| Cadence | every apply cycle (poll interval) | slower, per-table overridable ([§9.3](#93-the-cadence-dials)) |
| Cost | **O(new events)**; `MERGE`s against the whole mirror so cross-batch state stays correct | O(retained raw + mirror); needs exclusive writer + ~2× transient space |
| Statement | `MERGE INTO <table>` from the deduped `TEMP` batch | `CREATE OR REPLACE TABLE <table> AS SELECT …` (atomic; readers see old table until commit [6]) |
| Baseline | reads only `commit_lsn > transformed_lsn` | window-dedups over **retained raw ∪ current mirror injected as an LSN-floor baseline** (preserves pruned values), drops `op='d'` winners |
| Purpose | keep the mirror fresh | self-heal drift, reclaim space ([§9.4](#94-retention-and-compaction-realities)) |

---

## 6. Intra-batch PK churn — insert → delete → insert, worked in full

> **Normative rule ([architecture.md §2.1, "Intra-batch PK-churn collapse rule"](./architecture.md#21-the-raw-to-mirror-transform-model)):**
> within a single transform window, for **every** primary key that appears, the transform applies **exactly one**
> action, derived from the single **latest** raw event for that key (the `rn=1` winner), chosen **solely by the
> winner's `op`**: winner `op ∈ {i,u}` → the key's mirror row becomes the winner tuple; winner `op='d'` → the key
> is absent. Any number of earlier events for that key — an insert, a delete, another insert, in any mix — are
> discarded before the `MERGE`.

This is the case the user specifically flagged: a primary key that within one micro-batch is inserted, deleted, and
inserted again — and the same pattern happening for many keys at once.

### 6.1 The primary case, worked step by step

Seed `<table>_raw` with **N** keys, each carrying `i → d → i` at distinct row `lsn` (give the two inserts distinct
data so we also exercise PK reuse). Take one representative key `pk=42` (all N behave identically), `:transformed_lsn
= 0/0`:

| pk | op | commit_lsn | lsn | data | `rn` = row_number() OVER (PARTITION BY pk ORDER BY commit_lsn DESC, lsn DESC) | winner? |
|---|---|---|---|---|---|---|
| 42 | i | 0/100 | 0/1001 | `A` |   3 |   |
| 42 | d | 0/200 | 0/2001 | —   |   2 |   |
| 42 | i | 0/300 | 0/3001 | `B` | **1** | ✅ |

Ranking keeps all three rows; `rn=1` lands on the **final insert** `(0/300, 0/3001)` = data `B`. `_batch` therefore
holds one row for `pk=42`: `op='i'`, data `B`. The `MERGE` runs `WHEN NOT MATCHED AND op<>'d' THEN INSERT` (or
`WHEN MATCHED THEN UPDATE` if the mirror already had the key) → mirror row `42 = B`. Across all N keys:
**`COUNT(*) = N`**, every key equals its **last** insert, **zero** delete survivors, **zero** first-insert survivors.

**The counterfactual (why [§5.3](#53-deletes-are-filtered-after-ranking-the-resurrection-guard) matters).** If we
filtered `op='d'` **before** the window, the middle delete vanishes and the window ranks only the two inserts. For a
key whose intended final state is *absence* (`i → d`, no re-insert), pre-filtering would drop the only delete and
leave the first insert as `rn=1` → the key is **resurrected** into the mirror. Ranking first, then deciding on the
winner's `op`, is what prevents this.

### 6.2 The variant matrix

Every sequence collapses to its winner. One transform window; each row is an independent key:

| Sequence (per key) | Winner `op` | Mirror before | Mirror after | MERGE branch exercised |
|---|---|---|---|---|
| `i → d → i(B)` | `i` | absent | **B** | NOT MATCHED → INSERT |
| `i → d` | `d` | absent | **absent** | MATCHED-none → no-op |
| `i → u → d` | `d` | absent | **absent** | MATCHED-none → no-op |
| `d` (phantom, key never seen) | `d` | absent | **absent** | `NOT MATCHED AND op<>'d'` guard → no-op |
| `i(A) → d → i(B)`, `A ≠ B` | `i` | absent | **B** | NOT MATCHED → INSERT (PK reuse) |
| `d → i` on a **pre-seeded** mirror key | `i` | present (old) | **insert data** | MATCHED → UPDATE (last-tuple-wins across a tombstone) |

**PK reuse is resolved by definition.** `i(A) → d → i(B)` resolves to `B`. That is correct: the `<table>` mirror is
a current-state snapshot holding **exactly one row per key** and cannot represent "two logically distinct rows that
reused a PK." The full `i(A) → d → i(B)` history — and the fact that A and B are distinct logical rows — is
preserved only in `<table>_raw`.

### 6.3 Why it scales — set-based, single-pass

Because the winner is picked by `row_number() OVER (PARTITION BY <pk> …)`, **every key's churn collapses
simultaneously in one pass** — the collapse is a natural byproduct of the partitioned window, with **no per-key
loop and no correlated subquery**. It behaves identically whether one key or millions churn in the same batch, and
whether each churns once or "many times" (`i→d→i→d→i→…`): only the single max-`(commit_lsn, lsn)` row per partition
survives. This is the answer to *"what happens when that happens many times in a given micro batch"* — the cost is
**O(new events)**, dominated by one sort/partition, not by the number of times any key flips.

### 6.4 Why it is unit-testable by construction

The dedup + collapse is **pure, self-contained SQL**, parameterized only by `<table>`, the `<pk>` column list, and
`:transformed_lsn`, reading exclusively from `<table>_raw` (plus `<table>` for the final `MERGE`) — no S3, no
Postgres, no network, no Rust state. Because DuckDB is in-process (the `duckdb` crate, `bundled` [10]), a unit test
runs the **exact production transform SQL** against `Connection::open_in_memory()`: seed `<table>_raw` with scripted
`i`/`d`/`i` sequences across many PKs, run the transform, assert the resulting `<table>`. Shipping the transform as
one parameterized SQL template ([§5.4](#54-the-merge-branches-and-composite-keys)) means the test and the loader
share a single source of truth.

The concrete assertions to ship ([architecture.md Verification](./architecture.md#verification-how-well-prove-it-works-end-to-end-later)):
N keys `i → d → i` → every mirror row = its last insert and `COUNT(*) = N`; `i → d` and `i → u → d` and a phantom
`d` → all absent; `i(A) → d → i(B)` (A ≠ B) and `d → i` on a pre-seeded key → mirror = `B` / the insert data.

---

## 7. Straddling the watermark — the per-PK max-applied-commit-LSN guard

> **⚠ Extends architecture.md.** This is the one place the loader's *incremental* path in `architecture.md` is
> under-specified. Per the confirmed decision to recommend-and-flag, this section names the implicit invariant,
> shows how it can break, and specifies a guard — clearly marked as a proposed extension, not settled design. It
> resolves the residual in [Open Q13](./architecture.md#open-questions--risks) and ties [Open Q8](./architecture.md#open-questions--risks).

**The implicit invariant.** The incremental `MERGE` ([§5.4](#54-the-merge-branches-and-composite-keys)) applies the
window winner **unconditionally**. That is correct only if a winner's `(commit_lsn, lsn)` is always **≥** the
`(commit_lsn, lsn)` that last shaped the mirror row it touches. In the happy path this holds because `transformed_lsn`
advances to `max(commit_lsn)` applied and each next window is strictly `> transformed_lsn` — monotonic per PK. Two
faces can break it:

- **Break face A — the equal-`commit_lsn` snapshot straddle.** All snapshot rows carry `commit_lsn =
  consistent_point` ([architecture.md §1.7](./architecture.md#17-snapshot--backfill-bootstrap)). The window's low
  bound is **strict** (`commit_lsn > :transformed_lsn`). If a streamed change for *another* key advances
  `transformed_lsn` to `consistent_point` first, and a **snapshot file for a no-stream key** is appended afterward,
  that key's snapshot row has `commit_lsn = consistent_point` — **not `>`** — so it is **excluded forever** and the
  key is **silently missing** from the mirror. This is exactly the
  [Verification "equal-`lsn_end` snapshot files … none skipped by the watermark"](./architecture.md#verification-how-well-prove-it-works-end-to-end-later) case.
- **Break face B — delete + re-insert straddling the watermark** ([Open Q13](./architecture.md#open-questions--risks)'s
  stated residual): a stale event re-entering a window (via full-rebuild baseline interaction, a manual raw
  backfill, or a boundary landing at an equal commit LSN) could resurrect a killed row if the `MERGE` applies it
  unconditionally.

**Recommended resolution (the extension).** Carry two hidden columns on the mirror — `_applied_commit_lsn` and its
tiebreaker `_applied_lsn` — recording the `(commit_lsn, lsn)` of the event that last shaped each row, and guard
every `MERGE` branch so a stale winner can never overwrite or resurrect:

```sql
MERGE INTO <table> t USING _batch s ON t.<pk> = s.<pk>
  WHEN MATCHED AND s.op='d'
       AND (s.commit_lsn, s.lsn) > (t._applied_commit_lsn, t._applied_lsn) THEN DELETE
  WHEN MATCHED
       AND (s.commit_lsn, s.lsn) > (t._applied_commit_lsn, t._applied_lsn) THEN
       UPDATE SET <non-key cols> = s.<col>, _applied_commit_lsn = s.commit_lsn, _applied_lsn = s.lsn
  WHEN NOT MATCHED AND s.op<>'d' THEN
       INSERT (<cols>, _applied_commit_lsn, _applied_lsn) VALUES (s.<cols>, s.commit_lsn, s.lsn);
```

Pair it with **one** of:

- **(a, primary) a mirror-side guard column** (above) — makes the incremental `MERGE` *self-correcting*: a stale
  event is a no-op regardless of watermark timing. Combine with a `>=` low bound (or a per-PK re-scan) so equal-LSN
  snapshot straddles are re-examined and the guard rejects only genuinely-stale writes. `_applied_*` are internal;
  keep them out of user-facing projections (or in a sibling shadow table if a byte-identical mirror is required).
- **(b, alternative) a Phase-B tuple-boundary watermark** — mirroring the TRUNCATE `(Ct, Lt)` treatment
  ([§5.5](#55-truncate--a-wipe-keyed-on-the-commit_lsn-lsn-tuple)): do not advance `transformed_lsn` past a
  `commit_lsn` until **all** rows at that `commit_lsn` are appended (closes the snapshot `consistent_point` case),
  but does not by itself defend break face B. Prefer (a).

**The full-rebuild is the safety net regardless.** Even without the guard, the periodic
`CREATE OR REPLACE … AS SELECT` over retained-raw ∪ mirror-baseline ([§5.7](#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild),
[§9.4](#94-retention-and-compaction-realities)) heals any drift. The guard makes the **incremental** path correct on
its own rather than eventually-correct — which is why it is worth adding.

Cross-refs: [architecture.md Open Q8/Q13](./architecture.md#open-questions--risks);
[§1.7 step 4](./architecture.md#17-snapshot--backfill-bootstrap);
[Verification "Exported-snapshot backfill boundary" / "Commit-order under streaming"](./architecture.md#verification-how-well-prove-it-works-end-to-end-later).

---

## 8. Kubernetes pod lifecycle

The loader runs on Kubernetes and must survive pod churn: a rescheduled, evicted, or drained pod recovers on its own
and resumes from its two watermarks, with no data loss and no babysitting. This section is the full loader-side
treatment that [`walrus-pg-sink.md` §4.6](./walrus-pg-sink.md#46-the-loaders-shutdown-differs) only summarizes.

### 8.1 Topology and the single-writer problem

**Locked:** `StatefulSet replicas=1` today [12]. One active loader owns **all** `.duckdb` files on its PVC and runs
**one worker thread per table**; that one writer owns **both** `<table>` and `<table>_raw` in each file. Scale it
**up** (CPU/memory / more per-table threads), not out — horizontal scale-out is deferred
([§9.5](#95-deferred--multi-pod-horizontal-scale-out)).

**The thesis of this section.** The sink gets its single-writer guarantee **for free from Postgres**: a second
concurrent `START_REPLICATION` on an active slot simply fails
([`walrus-pg-sink.md` §4.1](./walrus-pg-sink.md#41-topology--the-real-correctness-backstop)). **The loader has no
equivalent database-enforced rule.** Its single-writer guarantee is assembled from weaker, cooperating mechanisms:

| Concern | Sink | Loader |
|---|---|---|
| Enforced by | Postgres active-slot rule (hard, server-side) | `replicas=1` + RWO PVC + ownership lease + DuckDB file lock (cooperative) |
| Two pods run at once | impossible (2nd `START_REPLICATION` fails) | possible in principle → needs a **fencing token** |
| Backstop strength | strong, free | weaker, home-grown |

**Invariant:** no `.duckdb` file is written until its **ownership lease is held** *and* its **DuckDB single-writer
lock is taken**. Bootstrap is that fence ([§8.2](#82-startup--the-ordered-fail-fast-bootstrap)).

> **⚠ Extends architecture.md** — *what an ownership lease is.* `architecture.md` says "acquire table-ownership
> leases … contended → terminal" but never defines the mechanism. **Recommendation:** a `walrus.table_ownership`
> control-Postgres table keyed `(epoch, source_schema, source_table)` carrying `owner_pod`, `lease_expiry`, and a
> monotonic **`fencing_token`** (generation). Acquire via a conditional `UPDATE … WHERE lease_expiry < now() OR
> owner_pod = :self` (or a Postgres advisory lock [18]); renew on an interval well under the TTL; the DuckDB file
> lock [11] is the **second** fence, not the first. Alternatively a `coordination.k8s.io` Lease [14]. The fencing
> token is **inert while `replicas=1`** and becomes load-bearing only under the deferred multi-pod sharding path
> ([Open Q4](./architecture.md#open-questions--risks), [§9.5](#95-deferred--multi-pod-horizontal-scale-out)).

```
              loader pod (StatefulSet replicas=1)
      ┌───────────────────────────────────────────────┐
      │  thread(orders) ─┐                             │
      │  thread(customers)┼─ each = single writer of   │       RWO PVC
      │  thread(items) ───┘  one .duckdb file          │  ┌──────────────┐
      │                                                │──│ orders.duckdb │  (orders + orders_raw)
      │  lease(orders), lease(customers), lease(items) │  │ customers.dd  │  (customers + _raw)
      └───────────────────────────────────────────────┘  │ items.duckdb  │
                                                          └──────────────┘
```

### 8.2 Startup — the ordered, fail-fast bootstrap

An ordered bootstrap runs **before** any file is applied; a non-zero exit → `CrashLoopBackOff`, so a broken deploy
is loud and immediate ([architecture.md Startup & bootstrap](./architecture.md#startup--bootstrap-fail-fast-preflight)).
The loader-specific sequence:

1. **Control Postgres reachable + acquire per-table ownership leases** ([§8.1](#81-topology-and-the-single-writer-problem)).
   A genuinely contended lease → **terminal**.
2. For each owned table: mount-check the PVC → open/create the `.duckdb` file → **take the DuckDB single-writer
   lock** → ensure **both** `<table>` and `<table>_raw` exist (create the raw log if missing) → integrity-check both.
3. **Load both checkpoints** (`raw_appended_lsn`, `transformed_lsn`) so append and transform resume independently
   (respecting `CHECK (transformed_lsn ≤ raw_appended_lsn)`).
4. **Schema-reconcile both tables** to the expected `schema_version` in `schema_registry`: `<table>` = exact source
   shape, `<table>_raw` = additive superset (retained-nullable drops, `VARCHAR`-widened incompatible types,
   `attnum`-tracked renames); apply pending `ddl_manifest` changes **before** the apply loop starts.
5. **Verify the S3 read path** (GET/list the staged prefix).

The fence (lease + lock) is acquired in steps 1–2 **before** checkpoints are read in step 3 — a loader that cannot
prove exclusive ownership must never read-then-write a watermark.

| Condition | Class | Action |
|---|---|---|
| Control PG / S3 not yet up (rollout) | transient | retry with backoff to the startup deadline |
| Lease held by a **live** owner | **terminal** | exit non-zero (distinct code) |
| PVC not mounted | terminal | exit |
| `.duckdb` integrity check fails | terminal | exit; needs restore-from-backup |
| Irreconcilable schema drift | terminal | exit (surfaces the event-trigger-gap risk early) |
| `transformed_lsn > raw_appended_lsn` | terminal | corrupt checkpoint |

> **⚠ Extends architecture.md** — *stale DuckDB-lock recovery.* DuckDB takes a lock on open in read-write mode [11].
> If the prior pod was `SIGKILL`ed, bootstrap must distinguish a **live owner** (lease still renewed by a running
> pod → terminal, retry-with-backoff like the sink's "slot is active" nuance) from a **stale lock left by a dead
> PID** (lease expired, no renewer → recoverable: reclaim the lease, clear the stale lock, open). This is the
> loader's analogue of the sink's transient-vs-fatal slot handling and is not covered in the source docs.

- **`initContainers`** hoist read-only environment checks (control PG reachable, migrations current, S3 reachable)
  so the main container only starts once the world is sane.
- A **generous `startupProbe`** gates the slow bootstrap **and** the initial catch-up (which for the loader means
  draining a potentially large `file_manifest` backlog through Phase A + Phase B). While `startupProbe` is
  unsatisfied, Kubernetes runs neither liveness nor readiness [13], so a long catch-up is never killed mid-progress.
  Size `failureThreshold × periodSeconds` above worst-case initial append+transform of the backlog.

### 8.3 Probes — readiness, liveness, and the catch-up-lag trap

| Probe | Purpose | Loader wiring |
|---|---|---|
| `startupProbe` | gate slow bootstrap + initial catch-up | generous `failureThreshold × periodSeconds`; suppresses the other two [13] |
| `readinessProbe` | keep out of rotation / PDB accounting until working | leases held + DuckDB files open/writable + apply loop live — **NOT** "backlog drained" |
| `livenessProbe` | **true deadlock detection only** | apply-loop thread making progress — **NEVER** lag of any kind |

Two hazards, both corrected here:

- **Readiness ≠ caught up.** A caught-up loader and a badly-behind loader are **both ready**; readiness gates the
  pod into PDB/rotation accounting, not into "zero lag." Gating readiness on backlog=0 would flap a busy loader in
  and out.
- **Liveness must ignore lag — *both* kinds.** Never tie liveness to **slot lag** (the loader does not own the slot)
  and never to **backlog / transform lag / watermark advancement**: a loader legitimately catching up after an
  outage has high lag *by design*, and a healthy but *idle* loader does not advance its watermark at all — either
  would be killed into a restart loop. Define "progress" concretely: the apply loop stamps an in-memory
  `last_poll_completed_at` **every cycle, even on a no-op poll**, and liveness checks that timestamp against a
  multiple of the poll interval.

> **⚠ Extends architecture.md.** `architecture.md` states "never slot lag" but does not name the loader's *own*
> backlog-lag trap distinctly. The metrics `raw-append lag` and `transform lag`
> ([architecture.md Observability](./architecture.md#observability)) are **observability signals, never liveness
> gates.**

```yaml
startupProbe:   { httpGet: { path: /startup,  port: 8080 }, periodSeconds: 10, failureThreshold: 180 }  # ~30m budget
readinessProbe: { httpGet: { path: /ready,    port: 8080 }, periodSeconds: 10, failureThreshold: 3 }    # leases+files+loop
livenessProbe:  { httpGet: { path: /healthz,  port: 8080 }, periodSeconds: 15, failureThreshold: 4 }    # last_poll fresh only
```

### 8.4 Steady state — the apply loop, cadences, and the PDB

- **One apply loop per table**, on the user's poll cadence. **Phase A (append) + Phase B (transform) share one poll
  cadence in v1** (two transactions, two watermarks). A separate, slower **per-table compaction/full-rebuild
  cadence** and a **raw-retention window** are distinct knobs ([§9.3](#93-the-cadence-dials)).
- **Per-table isolation → natural parallelism.** N worker threads, each the single writer of its file; no
  cross-file coordination in the steady loop.
- **PDB:** use **`maxUnavailable: 1`** or **no PDB**. **⚠ Never** a single-replica PDB with **`minAvailable: 1`** —
  it makes the pod unevictable and permanently blocks `kubectl drain` / node upgrades [15], the opposite of
  self-healing-through-node-drain. It bites *harder* for the loader than the sink, because a blocked drain also
  blocks the PVC from detaching and reattaching to a healthy node ([§8.6](#86-decommission-and-node-drain)).

```
per-table apply loop (poll cadence):
  Phase A  claim ready files (lsn_end,id) → GET/read_parquet → APPEND verbatim → <table>_raw
           └ control-DB txn: raw_appended_lsn += ; DELETE claimed manifest rows
  Phase B  apply pending DDL @ LSN → TRUNCATE wipe (Ct,Lt) → dedup window → MERGE → <table>
           └ transformed_lsn = max(commit_lsn)
```

| Knob | Controls | Trade-off |
|---|---|---|
| loader poll interval | how often Phase A+B run | lower = fresher mirror, more S3/DuckDB churn |
| compaction / full-rebuild cadence (per-table overridable) | self-heal + space reclaim | rarer = more bloat; each run needs exclusive writer + ~2× space |
| raw-retention window (~7d / last-K) | history kept behind `transformed_lsn` | wider = more replay/debug + larger file |

> **⚠ Extends architecture.md.** The full-rebuild takes the **same single writer** for that file, so it serializes
> against (blocks) that table's apply loop. **Recommendation:** run it on the **same worker thread**, serialized
> after an apply cycle (simplest, no quiescing dance), and **schedule it low-traffic** ([Open Q12](./architecture.md#open-questions--risks)).

### 8.5 Graceful shutdown — the SIGTERM drain

On `SIGTERM`, each per-table worker drains **in order** before exit:

1. **Stop claiming new manifest files** (stop Phase-A intake).
2. **Finish the in-flight Phase-A append** and **commit** the DuckDB write **and** the control-DB txn that advances
   `raw_appended_lsn` + DELETEs the claimed rows (keep them atomic — [§4](#4-two-phase-apply--append-then-transform)).
3. **Finish the in-flight Phase-B transform** (the current `MERGE`) and **commit `transformed_lsn`**.
4. **Release the table-ownership lease.**
5. **Release the DuckDB single-writer lock / close the file cleanly** (`CHECKPOINT`, then close) so a replacement
   loader takes over with **no stale lock and no half-applied batch**.
6. **Never drop the slot** — the loader doesn't own it; decommission is separate ([§8.6](#86-decommission-and-node-drain)).

Every restart is a **resume** from the two watermarks: an ungraceful `SIGKILL` mid-append is absorbed by the
work-queue + `ON CONFLICT DO NOTHING`, and mid-`MERGE` by the idempotent transform
([§4](#4-two-phase-apply--append-then-transform)). Graceful drain merely minimizes replay and, critically, avoids
leaving a **stale DuckDB lock** for [§8.2](#82-startup--the-ordered-fail-fast-bootstrap) to untangle. Make the Rust
process **PID 1 / under `tini` / exec-form** so `SIGTERM` reaches it and isn't swallowed by a shell entrypoint [16].

> **⚠ Extends architecture.md** — *full-rebuild abort policy.* An in-flight periodic full-rebuild
> (`CREATE OR REPLACE … AS SELECT`, ~2× space, possibly minutes) may not fit the grace budget. Because it is
> **idempotent self-heal**, on `SIGTERM` **abort it** (roll back the transient rebuild; it re-runs next cycle) —
> only the incremental append+transform must complete. This is why grace-period sizing excludes the full-rebuild.

**Sink vs loader drain:**

| Step | Sink ([§4.5](./walrus-pg-sink.md#45-graceful-shutdown--the-missing-piece)) | Loader (this §) |
|---|---|---|
| Stop intake | stop requesting WAL | stop claiming manifest files |
| Flush in-flight | Parquet→S3 + manifest commit | DuckDB append + both watermark commits |
| Final checkpoint | standby status → `confirmed_flush_lsn` | commit `raw_appended_lsn` + `transformed_lsn` |
| Release | `CopyDone`, close replication conn | release lease + DuckDB lock, close file |
| Slot | never drop | n/a — loader never touches the slot |
| In-flight heavy job | (none) | **abort** the periodic full-rebuild |

- **`terminationGracePeriodSeconds`:** size to the **measured incremental worst case** — finish current file append
  (S3 GET + DuckDB append) + current `MERGE` + two commits + lease/lock release + `CHECKPOINT`/close. It can exceed
  the sink's 60–120s if append/`MERGE` batches are large; the full-rebuild is *aborted*, so exclude it. **Skip
  `preStop`** (a non-serving consumer): any preStop time is subtracted from the same grace budget the drain needs
  [16], so let `SIGTERM` arrive at T=0 with the full budget.
- There is **no `wal_sender_timeout` analogue** for the loader — its drain is bounded only by the grace period and
  DuckDB commit latency, not a server-side connection timeout. A genuine simplification vs the sink.

### 8.6 Decommission and node drain

The loader **never** drops the slot (only the sink's explicit decommission job does —
[architecture.md §1.8](./architecture.md#18-single-slot-for-life--total-restart)). Loader decommission = release
leases, close files, optionally snapshot/export each `.duckdb` to S3 for backup. On **node drain**, `replicas=1`
StatefulSet + RWO PVC [17] gives: pod terminates gracefully ([§8.5](#85-graceful-shutdown--the-sigterm-drain)) → PVC
detaches → pod reschedules on a new node → PVC reattaches → bootstrap ([§8.2](#82-startup--the-ordered-fail-fast-bootstrap))
resumes from both watermarks. The PDB must not block this ([§8.4](#84-steady-state--the-apply-loop-cadences-and-the-pdb)).

> **⚠ Extends architecture.md.** The ownership-lease **TTL must exceed the PVC-reattach time** during a node drain,
> or the replacement pod could see the lease as still "contended" by the still-terminating pod. Size lease TTL >
> worst-case (graceful drain + PVC detach/reattach), and prefer `owner_pod`-aware reacquisition so the *same*
> StatefulSet identity reclaims its own lease immediately.

---

## 9. Performance & scaling — making it fast

### 9.1 The governing constraint

Restate the north star ([architecture.md](./architecture.md), Component 2 mission): delivery is **eventually
consistent on a tunable latency budget**, the system scales **vertically today** (a bigger pod / more per-table
threads — *not* more pods), and **correctness wins every tie**. Everything below is "make it as fast as possible
*within* that contract," never "make it real-time."

### 9.2 The levers

| Lever | Mechanism | Phase | Source |
|---|---|---|---|
| Per-table parallel workers | one writer thread per `.duckdb` file; per-table isolation *is* the parallelism model | A+B | [12] |
| DuckDB **Appender API** | `duckdb-rs` `append_record_batch` (`appender-arrow`) writes Arrow batches straight into the persistent `<table>_raw` | A | [10] |
| **`read_parquet` from S3** | `INSERT INTO <table>_raw SELECT … FROM read_parquet(:uri)` reads Parquet natively (`httpfs`/`SET s3_*`), no local staging | A | [8][9] |
| **Set-based single-pass transform** | the partitioned window collapses **all keys at once** — no per-key loop / correlated subquery ([§6.3](#63-why-it-scales--set-based-single-pass)) | B | [4] |
| `TEMP` staging | dedup into a `TEMP` table that never touches the persistent file, then `MERGE` | B | [3] |
| Incremental = O(new events) | scans only `commit_lsn > transformed_lsn`, but `MERGE`s against the whole mirror for cross-batch correctness | B | — |
| Batch files per cycle | claim several `ready` files in one Phase-A pass to amortize S3 round-trips + DuckDB txn overhead | A | — |
| Columnar pushdown | project only needed columns and push predicates so DuckDB reads less from S3 | A | [8] |
| Pinned DuckDB 1.4.x LTS | required for single-statement `MERGE INTO`; note EOL 2026-09-16 → plan the next-LTS bump | B | [3] |

**Appender vs `read_parquet`:** the Appender wins when rows are already in-memory Arrow (e.g. a small file, or reuse
of a decoded batch); `read_parquet` wins for larger files by letting DuckDB stream columnar data straight from S3
with projection/predicate pushdown and no intermediate materialization. Phase B always runs **table-to-table inside
DuckDB** (`MERGE INTO <table>` reading `FROM <table>_raw` via the `TEMP` deduped batch), never from a transient
Parquet view — the transform template is [§5.4](#54-the-merge-branches-and-composite-keys).

### 9.3 The cadence dials

The operator's three latency/footprint controls are the **poll interval** (freshness vs churn), the **compaction /
full-rebuild cadence** (bloat vs contention), and the **raw-retention window** (replay/debug depth vs file size) —
see the table in [§8.4](#84-steady-state--the-apply-loop-cadences-and-the-pdb). Loosening the poll interval and
widening batches yields sooner delivery at the cost of more per-cycle work; these are the knobs meant to be tuned.

### 9.4 Retention and compaction realities

A DuckDB truth the loader must design around: **`DELETE` only tombstones** — `CHECKPOINT` reclaims only
heavily-deleted row groups, and **`VACUUM FULL` is unimplemented** [7]. So the single `.duckdb` file **does not
shrink** on ordinary raw-retention deletes.

> **Raw history under a single-table reload (PR 6.7).** A reload **rebuild** (the `reload` flavor,
> [single-table-reload.md](./single-table-reload.md) H8) `CREATE OR REPLACE`s both `<table>` *and*
> `<table>_raw` at the attempt's schema_version — **discarding that table's raw CDC history in
> DuckDB by design**: the pre-reload raw rows describe the world the clear replaces (replaying
> them would resurrect exactly the drift the reload exists to kill), and the staged Parquet
> persists in S3 per its GC policy for forensic replay. The `resync` flavor never touches raw
> history — its chunks append through Phase A like any file.

| Operation | Reclaims space? |
|---|---|
| `DELETE` (raw-retention prune) | no — tombstones only |
| `CHECKPOINT` | partial — only heavily-deleted row groups |
| `VACUUM FULL` | unimplemented |
| **full-rebuild** (`CREATE OR REPLACE … AS SELECT`) / `COPY FROM DATABASE` | **yes** — fresh, un-bloated table |

Real reclamation therefore **rides the periodic full-rebuild**, which is **atomic** (readers see the old table until
commit [6]) but needs the **exclusive single writer** (it blocks that table's apply loop) and **~2× transient space**
(old + new coexist until commit). That cost is exactly why it is a separate, slower cadence
([§8.4](#84-steady-state--the-apply-loop-cadences-and-the-pdb)) and why it is the job aborted on `SIGTERM`
([§8.5](#85-graceful-shutdown--the-sigterm-drain)). Correctness note the perf story must not omit: the rebuild
**unions the current mirror as an LSN-floor baseline**, so a value whose last real write was already pruned from raw
is never lost ([§5.7](#57-ddl-at-the-right-lsn-and-incremental-vs-full-rebuild),
[§5.6](#56-unchanged-toast-resolution--the-raw-back-scan)). Set the retention window toward `0` only if a file is
severely size-constrained; the transform is unchanged, so the window can widen later.

```
full-rebuild timeline (one table):
  apply loop ──quiesce──▶ CREATE OR REPLACE TABLE <table> AS SELECT …   ──COMMIT(atomic)──▶ resume apply loop
                          └────── exclusive writer, ~2× space ──────┘
                          readers see the OLD table until COMMIT
```

### 9.5 Deferred — multi-pod horizontal scale-out

> **Not yet real ([Deferred design goal 2](./architecture.md#deferred-design-goals-to-solve-later)).** Horizontal
> scale-out = multiple loader replicas each owning a **disjoint** set of tables (consistent hashing, **PVC per
> replica**, exclusive file ownership guarded by the **fencing token** from [§8.1](#81-topology-and-the-single-writer-problem)
> — **never naive HPA**, since file ownership is exclusive [Open Q4](./architecture.md#open-questions--risks)). The
> **sink stays a single consumer** of the one lifelong slot regardless — horizontal scale is a **loader-only** story.

```
DEFERRED (not v1):
  loader-0  owns {orders, customers}  ── PVC-0 ── orders.duckdb, customers.duckdb
  loader-1  owns {items, shipments}   ── PVC-1 ── items.duckdb, shipments.duckdb
  ownership + fencing_token in walrus.table_ownership guards resharding
```

The fencing token designed into the §8.1 lease is the forward-compat hook that makes this safe later; until then it
is inert.

---

## 10. What this doc extends in `architecture.md`

Unlike [`walrus-pg-sink.md` §5](./walrus-pg-sink.md#5-what-this-doc-supersedes-in-architecturemd) — which *corrects*
wrong type/DDL/lifecycle rows — this doc mostly **extends** correct loader design. The one substantive addition is
[§7](#7-straddling-the-watermark--the-per-pk-max-applied-commit-lsn-guard); the rest fills operational gaps.

| `architecture.md` location | Status | Here |
|---|---|---|
| [Coordination contract](./architecture.md#coordination-contract-control-plane-tables) (work-queue semantics, `lsn_end` = commit LSN, two watermarks) | extended, made authoritative | [§2](#2-the-work-handoff-contract--the-file_manifest-as-a-work-queue), [§4](#4-two-phase-apply--append-then-transform) |
| [§1.6](./architecture.md#16-large-transaction-safety) commit-gate | cross-referenced as **sink-owned**; the loader's ready-only side **added** | [§3](#3-commit-gating--the-loader-only-ever-sees-committed-data-by-contract) |
| [Component 2 apply loop](./architecture.md#component-2--data-sink-walrus-loader) + [§2.1 transform](./architecture.md#21-the-raw-to-mirror-transform-model) | extended with worked examples | [§5](#5-the-rawmirror-transform--correctness-in-depth), [§6](#6-intra-batch-pk-churn--insert--delete--insert-worked-in-full) |
| [Delivery semantics](./architecture.md#delivery-semantics-ordering--idempotency) (effectively-once = two guarantees) | restated with the crash-window table | [§4](#4-two-phase-apply--append-then-transform) |
| **[Open Q13](./architecture.md#open-questions--risks)** (delete+reinsert straddle / per-PK guard) | **the genuine ADD — specified** | [§7](#7-straddling-the-watermark--the-per-pk-max-applied-commit-lsn-guard) |
| [Open Q8](./architecture.md#open-questions--risks) (intra-batch PK churn) | fully worked | [§6](#6-intra-batch-pk-churn--insert--delete--insert-worked-in-full) |
| [`walrus-loader` bootstrap](./architecture.md#startup--bootstrap-fail-fast-preflight) (lease mechanism, stale-lock recovery, terminal/transient taxonomy) | pinned down | [§8.1](#81-topology-and-the-single-writer-problem), [§8.2](#82-startup--the-ordered-fail-fast-bootstrap) |
| [Deployment table](./architecture.md#kubernetes-deployment) — probes row (never slot lag) | loader's own **backlog-lag** trap + readiness-not-backlog-drained added | [§8.3](#83-probes--readiness-liveness-and-the-catch-up-lag-trap) |
| Deployment table — PDB / loader row | tied to loader PVC reattach on node drain; drain ordering, grace sizing, full-rebuild-abort added | [§8.4](#84-steady-state--the-apply-loop-cadences-and-the-pdb)–[§8.6](#86-decommission-and-node-drain) |
| [Open Q10](./architecture.md#open-questions--risks) (transform vs load cadence) | same-thread compaction serialization recommended | [§8.4](#84-steady-state--the-apply-loop-cadences-and-the-pdb) |
| [Open Q12](./architecture.md#open-questions--risks) (retention/compaction) | space-reclamation truth + 2×-space / exclusive-writer cost model | [§9.4](#94-retention-and-compaction-realities) |
| [Open Q4](./architecture.md#open-questions--risks) (single-writer HA / fencing) | lease + fencing-token design + deferred-sharding hook | [§8.1](#81-topology-and-the-single-writer-problem), [§9.5](#95-deferred--multi-pod-horizontal-scale-out) |

---

## Sources

Primary sources are PostgreSQL / DuckDB / Kubernetes manuals; the three companion docs carry the cross-referenced
design. Bracketed numbers are local to this doc.

- [1] PostgreSQL — Logical Streaming Replication Protocol (commit order; `proto_version` / `streaming`) — https://www.postgresql.org/docs/current/protocol-logical-replication.html
- [2] PostgreSQL — Logical Replication Message Formats (`Stream Start/Stop/Commit/Abort`, per-message xid) — https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
- [3] DuckDB — `MERGE INTO` (1.4.0 LTS+) — https://duckdb.org/docs/stable/sql/statements/merge_into
- [4] DuckDB — Window functions & `QUALIFY` — https://duckdb.org/docs/stable/sql/query_syntax/qualify
- [5] DuckDB — `SELECT … EXCLUDE` / star expressions — https://duckdb.org/docs/stable/sql/expressions/star
- [6] DuckDB — `CREATE OR REPLACE TABLE … AS SELECT` (atomic replace) — https://duckdb.org/docs/stable/sql/statements/create_table
- [7] DuckDB — `DELETE`, storage tombstones, `CHECKPOINT` (no `VACUUM FULL`) — https://duckdb.org/docs/stable/sql/statements/delete
- [8] DuckDB — Reading Parquet (`read_parquet`, projection & filter pushdown) — https://duckdb.org/docs/stable/data/parquet/overview
- [9] DuckDB — `httpfs` / S3 API — https://duckdb.org/docs/stable/extensions/httpfs/s3api
- [10] `duckdb-rs` — Appender API (`append_record_batch`, `appender-arrow`, `bundled`) — https://github.com/duckdb/duckdb-rs
- [11] DuckDB — Concurrency & the single-writer model / file locking — https://duckdb.org/docs/stable/connect/concurrency
- [12] Kubernetes — StatefulSets — https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/
- [13] Kubernetes — Pod Lifecycle; probes (startup suppresses liveness/readiness) — https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
- [14] Kubernetes — Leases (`coordination.k8s.io`) — https://kubernetes.io/docs/concepts/architecture/leases/
- [15] Kubernetes — Configure a PodDisruptionBudget — https://kubernetes.io/docs/tasks/run-application/configure-pdb/
- [16] Kubernetes — Container Lifecycle Hooks (preStop precedes SIGTERM) — https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/
- [17] Kubernetes — Persistent Volumes & access modes (`ReadWriteOnce`) — https://kubernetes.io/docs/concepts/storage/persistent-volumes/
- [18] PostgreSQL — Explicit Locking / advisory locks — https://www.postgresql.org/docs/current/explicit-locking.html

**Companion docs (primary for the cross-referenced design):**
[`architecture.md`](./architecture.md) (Component 2, §2.1, coordination contract, Delivery semantics, Open Q4/Q8/Q10/Q12/Q13, Verification),
[`proto-version.md`](./proto-version.md) (§9 abort/rollback, §10 interleaving/commit-order, §13 the consumer contract),
[`walrus-pg-sink.md`](./walrus-pg-sink.md) (§2.4/§2.6/§2.7 type descriptor & TOAST, §4 pod lifecycle, §4.6 loader shutdown).
