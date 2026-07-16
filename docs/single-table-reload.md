# Single-table reload — critique of the in-band signal proposal, and a revised shape

> **Status: BUILT (Phase 6).** This design shipped across 12 PRs — see the
> [Phase 6 curriculum](./implementation/phase-6-single-table-reload/) (task files 6.1–6.12). The
> holes below (H1–H11) each map to a task; the anchor use case is proven end to end in
> [PR 6.12](./implementation/phase-6-single-table-reload/pr-6.12-e2e-quarantine-recovery.md).

This note poke-holes a concrete proposal for
[deferred goal §1](./deferred-goals.md#1-single-table-reload--re-sync-while-streaming):
reload **N individual tables** through the one lifelong replication slot
([architecture §1.8](./architecture.md#18-single-slot-for-life--total-restart)), while the
walrus-pg-sink keeps draining the WAL for every other table. It is a design document only — no code
changes accompany it. Grounding: the walrus v1 codebase, Debezium's incremental-snapshot design
(signals + chunking, [DDD-3]), and the Netflix DBLog watermark paper ([arXiv 2010.12597]).

**The verdict up front.** The proposal's two big instincts are right and industry-validated: signal
through the WAL itself, and tolerate snapshot/stream overlap instead of trying to freeze the world.
The two load-bearing holes: (1) a one-shot full-table export has **no consistent point** — "resume
the LSNs after the export" is undefined without a watermark-stamping rule, and getting that rule
backwards silently loses updates; (2) a **single status row in the source database mutated by three
services** breaks walrus layering — the loader has no source-DB connection, and the terminal
"downstream sink" doesn't exist yet. Both have clean fixes that reuse machinery walrus already has.

---

## 1. The proposal, restated

1. A `full_table_reload_state` table lives in the **source** Postgres. Requesting a reload =
   INSERT/UPDATE a row. Because the table is in the source DB, the change flows through the WAL and
   the single slot — the sink sees the request **in-band**, in stream order.
2. On seeing the signal, the sink starts a full export of that table over an ordinary SQL
   connection; the loader stops processing that table's records meanwhile (the sink keeps consuming
   WAL for everything, including the paused table).
3. Export done → sink updates the status row → the loader rebuilds the table's DDL + data file from
   the export, then resumes the WAL records from after the export point, in the same batch; some
   overlap re-processing is acceptable ("eventually correct").
4. The downstream sink updates the status row when it finishes, marking the reload complete.

## 2. What the proposal gets right

**Signal-through-WAL is the validated pattern, not a hack.** Debezium's ad-hoc incremental
snapshots are triggered exactly this way: an operator INSERTs an `execute-snapshot` row into a
source-side *signaling table* that is part of the publication, and the connector notices the row
through its own change stream ([Debezium signalling docs], [incremental snapshots blog]). Netflix
DBLog generates its snapshot watermarks the same way — writes to a dedicated source table whose
change event "is eventually received through the change log" ([Netflix blog]). walrus even has the
pattern in-house already: DDL capture rides the same slot via the event-trigger-populated
`walrus.ddl_audit` table, handled as an internal table in the sink's consume path
([`crates/pg-sink/src/consume.rs`](../crates/pg-sink/src/consume.rs)).

**"Eventually correct with overlap" is not a hope here — it is Phase B.** Debezium documents the
same consumer contract as mandatory: reads are point-in-time state, deletes may arrive for
never-seen rows, and apply must be an idempotent PK upsert ([DDD-3]). walrus already satisfies it
mechanically: Phase B dedups each PK to the latest `(commit_lsn, lsn)` and applies through the
three-branch MERGE with the per-PK applied-guard
([`crates/loader/src/transform.rs`](../crates/loader/src/transform.rs),
[walrus-loader §5](./walrus-loader.md)). This is the same algebra that makes bootstrap's
snapshot↔stream handoff converge ([architecture §1.7](./architecture.md#17-snapshot--backfill-bootstrap)).
Re-applying an overlapped event is a no-op by construction, not by luck.

**Pausing one table's loader is nearly free.** The loader discovers work only through
`file_manifest`, claimed per `(epoch, schema, table)` in `(lsn_end, id)` order
([`crates/control/src/manifest.rs`](../crates/control/src/manifest.rs)). "Stop processing table X"
= stop claiming X's rows; they accumulate as `ready` and the Parquet stays in S3. No record-level
filtering, no buffering, no loss.

**And the feature has a concrete customer.** v1's only terminal state is the quarantine after a
lossy `ALTER COLUMN TYPE` cast failure ([architecture §1.8](./architecture.md#18-single-slot-for-life--total-restart)).
Single-table reload is that state's recovery path; today the only exit is a total-restart of every
table.

## 3. The holes

### H1 — The export has no consistent point (the critical one)

The signal row's commit LSN tells you when the reload was *requested* — it says nothing about what
the export *contains*. The export's MVCC visibility point is wherever the source database happens
to be when the exporting transaction takes its snapshot, which is after the signal by an unknowable
gap. Bootstrap never had this problem because
`CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export')` atomically pairs a snapshot with its
`consistent_point` LSN ([`crates/pg-sink/src/replication.rs`](../crates/pg-sink/src/replication.rs))
— an option that exists **only at slot creation**. For a re-snapshot on the existing slot, Postgres
gives you nothing: "databases typically do not expose the execution position of a select on the
transaction log" is the DBLog paper's founding observation ([arXiv 2010.12597]).

So "the loader should know when to start processing the LSNs that happened after the export" is
exactly the part the proposal leaves undefined. The fix is a stamping rule, and its direction
matters:

> **Snapshot rows must be stamped with an LSN captured *before* the export's visibility point**
> (a conservative *low watermark* `L`).

Why the direction matters, in walrus's dedup algebra (winner = max `(commit_lsn, lsn)` per PK):

- **Stamp early (correct).** Any commit the export might already include also appears in the
  stream at some `C ≥ L`, so the stream event wins the dedup and re-applies the same value — an
  idempotent no-op. Any commit before `L` is in the export and beaten by nothing older. Converges.
- **Stamp late (silent data loss).** E.g. run `SELECT pg_current_wal_lsn()` as the first statement
  *inside* the REPEATABLE READ transaction: the snapshot is taken at the transaction's first query,
  so a commit can land in the gap — invisible to the export, but present in the stream at `C < L`.
  The stale snapshot row stamped `L` then *beats* the newer stream event, and the mirror holds the
  old value until the row happens to change again. No error, no alert.

Deletes ride along for free under the early stamp: a delete committed before `L` means the row is
simply absent from the export; one committed after appears in the stream at `C > L` and tombstones
the snapshot row through the MERGE delete branch. And a delete arriving for a row the rebuilt
mirror never saw falls into the `NOT MATCHED AND op='d'` no-op branch — the exact
"deletes for never-seen rows" case Debezium warns consumers about ([DDD-3]).

**Capturing `L` — two candidate mechanisms, one chosen.**

- **Echo-wait (chosen).** The sink INSERTs the watermark row, then waits until its own insert
  comes back through the replication stream; the decoded **commit LSN** of that transaction is
  `L`. This is the DBLog/Debezium shape.
- **Embedded read-back (noted, not chosen).** The INSERT computes the value itself —
  `INSERT ... VALUES (..., pg_current_wal_insert_lsn()) RETURNING wal_lsn` — and the sink starts
  the export immediately, no stream round-trip.

Both satisfy the stamping rule: both values are captured before the export's visibility point
(the embedded one is slightly *more* conservative, since the insert position precedes the commit
record — the overlap window widens by a hair, harmlessly). Echo-wait wins on reliability, for
three reasons. First, the decode → ship → receive round-trip between `L`'s commit and the chunk
SELECT practically closes the commit-visibility race (§6), which the same-connection `RETURNING`
path leaves at its narrow-but-widest. Second, the stamp is a *real stream position* — every
`commit_lsn` in raw corresponds to a commit the sink actually shipped, so ordering invariants
stay assertable and lag metrics stay honest. Third, it fails loudly on the classic
misconfiguration: a signal table missing from the publication times out the echo instead of
silently exporting anyway (H11). The embedded value's advantages are real — lower per-chunk
latency, simpler sink state, and it would even make the signal table's publication membership
optional — so keep the column anyway as a free cross-check: assert
`embedded wal_lsn < observed commit LSN` when the echo arrives, and it doubles as a diagnostic
if the echo path ever misbehaves.

### H2 — One monolithic export is the wrong unit

Both reference designs reject the single big export deliberately, and their reasons all apply here
([incremental snapshots blog], [arXiv 2010.12597]):

- **Not resumable.** A crash at row 900 M of a billion-row COPY restarts from zero. Debezium chunks
  by primary-key order (default 1,024 rows, continuation predicate
  `WHERE key > last_seen ORDER BY key LIMIT chunk`) and records the last completed chunk, so a
  restart resumes mid-table.
- **A long snapshot pins the xmin horizon.** One REPEATABLE READ transaction held open for an
  hours-long export blocks vacuum **database-wide** on the source — bloat inflicted on the very
  system walrus is supposed to observe politely. Chunked reads are each a single short statement;
  no long transaction exists.
- **N-at-once doesn't scale as N monoliths.** The stated goal is "full reload on n tables". N
  concurrent bulk COPYs are an unbounded load spike; N chunk queues drained round-robin under a
  concurrency cap are boring and fair.

The chunked shape needs a watermark per chunk, which is where the proposal's own signal table comes
back — generalized from one status row to **chunk-start watermark writes**: before each chunk, the
sink INSERTs a watermark row into the signal table, waits until it observes its own insert echo in
the replication stream (that observation *is* how it learns the chunk's low watermark `L_i` — the
insert's commit LSN), then runs the chunk SELECT and stamps every chunk row
`commit_lsn = lsn = L_i`.

**Where walrus gets to be simpler than DBLog.** DBLog brackets each chunk with low *and* high
watermarks, pauses log processing during the bracket, buffers the chunk, and discards buffered rows
whose PKs collide with stream events inside the window — because its consumers apply events in
arrival order and can't reorder ([Netflix blog], [arXiv 2010.12597]). walrus's Phase B applies by
total `(commit_lsn, lsn)` order with the per-PK guard, so the window-dedup happens *at transform
time for free*. No stream pause, no in-memory chunk buffer, no high watermark needed for
correctness: the early stamp does all of it. Chunk Parquet files enter `file_manifest` with
`lsn_end = L_i` and interleave with stream files in the loader's existing `(lsn_end, id)` claim
order — snapshot data literally sorts into the log.

### H3 — "Refresh" and "rebuild" are different operations; the proposal conflates them

A chunk-merge over the *live* mirror (Debezium's incremental snapshot, exactly) repairs stale and
missing rows but **never removes phantom rows** — a row that drifted into the mirror but no longer
exists upstream is in no chunk and gets no delete event. Fine for "re-sync suspected drift"; wrong
for the quarantine case, where the mirror's shape itself is broken. The proposal's own words —
"rebuild the ddl and data file" — require clearing first:

- **`reload` (rebuild).** Pause the table's loader claims, `CREATE OR REPLACE` mirror + raw at the
  current registry schema_version (the compaction path already rebuilds this way), then apply
  chunks + stream in manifest order. This is the quarantine-recovery flavor.
- **`resync` (refresh).** No pause, no clear; chunks merge over live state. Cheaper, keeps the
  table queryable, tolerates phantoms.

Name both in the state machine; they share all machinery except the clear. Note the pause is needed
**only** for rebuild — and "pause" costs nothing (H0 above: unclaimed manifest rows just wait).

> **Which flavor? (the operator decision guide — PR 6.10)**
> - **`resync`** — cheap drift repair. Merges chunks over the *live* mirror, so the table stays
>   queryable throughout; no pause, no rebuild, raw history preserved. Repairs stale and missing
>   rows but **tolerates phantoms** (a row that drifted into the mirror and no longer exists upstream
>   is in no chunk and survives). Reach for it when you suspect the mirror has *fallen behind*.
> - **`reload`** — the truth reset. Clears and rebuilds the mirror at the current schema, so it also
>   removes phantoms and is the quarantine-recovery path (a failed lossy `ALTER … TYPE`, PR 3.9). It
>   pauses the table's claims while exporting (queries see the pre-rebuild mirror until it swaps).
>   Reach for it when the mirror's *shape or content* is wrong, not merely stale.
>
> Rule of thumb: `resync` when you'd tolerate a phantom to keep the table online; `reload` when only
> a byte-for-byte match will do.

### H4 — One status row in the source DB, written by three services, is the wrong state store

Layering problems, in increasing severity:

- The loader has **no source-DB connection or credentials** — by design it talks to control-pg, S3,
  and its own DuckDB files. Granting the loader (and a future downstream sink) write access to the
  customer's production source database so they can flip a status column is a boundary violation
  with real blast radius.
- Status **UPDATEs** in the source require the signal table to have sane REPLICA IDENTITY, generate
  decoded events the sink must ignore-but-ack, and destroy the audit trail (the row is its own
  history).
- Every consumer that "watches the status change" through the WAL is coupled to stream latency for
  what is actually control-plane state.

The split that matches walrus's existing seams:

- **Source DB, `walrus.reload_signal`** — *only* what must be in-band because its **commit LSN is
  the datum**: chunk-start watermark rows. Insert-only, tiny, PK on `(reload_id, chunk_no)`, added
  to the publication, excluded from backfill like the rest of the `walrus` schema
  ([`crates/pg-sink/src/snapshot.rs`](../crates/pg-sink/src/snapshot.rs) already filters
  `schemaname != 'walrus'`).
- **control-pg, `walrus.table_reload`** — the state machine: `reload_id` (UUID, idempotency key),
  table, flavor (`reload`/`resync`), status, last-completed chunk PK bound, watermark LSNs, lease
  fields, timestamps. Operator requests go **here**, not in-band — a request needs no stream
  ordering, and the sink can poll it on its existing heartbeat cadence. Sink and loader both
  already read/write control-pg; nobody new touches the source.

### H5 — Update-in-place signals vs insert-only signals

Follows from H4 but worth its own line, since the proposal says "update or insert": Debezium's
signal table is **insert-only** by design ([Debezium signalling docs]). Each signal is a new row —
no REPLICA IDENTITY concerns, a free audit log, and duplicate-request idempotency reduces to a
unique `reload_id`. Any "update the status on that table" design re-derives these problems one at a
time.

### H6 — Who runs the export, and the stream must never wait for it

If the sink runs the export inline in the replication loop, the single slot stalls for **all**
tables — slot lag, WAL retention growth, exactly the failure the single-slot design exists to
avoid. The export must be a sink-owned side task: `tokio::spawn` + a separate ordinary SQL
connection, the same shape as bootstrap backfill
([`crates/pg-sink/src/snapshot.rs`](../crates/pg-sink/src/snapshot.rs)); the replication loop's
keepalive/feedback machinery already runs independently of durability
([architecture §1.9](./architecture.md)). For N tables: a queue in `table_reload` drained under a
`max_concurrent_reloads` cap, chunks interleaved round-robin.

### H7 — Crash recovery cannot lean on the WAL signal

By the time the sink crashes mid-export, the original signal's LSN is almost certainly behind
`confirmed_flush` — acked, gone, never redelivered. WAL redelivery is therefore **not** a recovery
mechanism for reload state; control-pg is. Recovery = on startup, scan `table_reload` for
non-terminal rows and resume from the last completed chunk PK bound (Debezium stores the same
cursor in its connector offsets). Two sub-gotchas:

- **Echo routing.** The sink sees its own `reload_signal` inserts come back through the stream.
  They must be consumed as internal signals (the `ddl_audit` handling precedent) — never routed to
  a `TableBatcher` as user data, never written to Parquet.
- **Stuck-state detection.** A lease (`lease_expiry` + holder) on the `table_reload` row turns
  "sink died mid-export" from a forever-`exporting` zombie into an alertable, resumable condition.
  With the sink a single StatefulSet this is mostly restart-resume, but the lease also fences the
  future multi-pod world ([deferred goal §2](./deferred-goals.md#2-multi-pod-loader-table-sharding-horizontal-scale-out)).

### H8 — Watermarks, manifest, and the rebuild trigger compose — if you're explicit

Three interactions the proposal doesn't address, all resolvable:

- **Checkpoint monotonicity survives.** `loader_checkpoint` advances via `GREATEST(old, new)` with
  the DB-enforced `transformed_lsn ≤ raw_appended_lsn`
  ([`crates/control/src/checkpoint.rs`](../crates/control/src/checkpoint.rs)). No rewind is ever
  needed: the table's frontier froze at some `W` when claims paused, and every chunk watermark
  `L_i > W` because the chunks are exported after the pause. The watermarks only ever move forward,
  through the reload.
- **Pending pre-reload manifest rows are superseded.** Rows for X with `lsn_end ≤ L_1` describe
  data the first chunk already contains. Drop them at rebuild time (delete the manifest rows; S3
  GC follows normal policy) — or don't, and the dedup algebra eats them harmlessly at the cost of
  wasted appends. Drop for efficiency; algebra as the safety net.
- **The loader needs no new handshake to start the rebuild.** Give reload chunk files a manifest
  `kind='reload'` plus the `reload_id`. The loader claims in order, sees a `reload` file whose
  `reload_id` it hasn't recorded in `_walrus_meta`, and *that* is the trigger: `CREATE OR REPLACE`
  mirror + raw at the file's schema_version, record the reload_id, append, continue. Idempotent by
  the recorded id; zero coordination beyond the manifest row it already reads.

### H9 — DDL landing mid-reload

First, the lock reality. A one-shot COPY holds ACCESS SHARE for the whole export, so most DDL
can't "come in" at all — the `ALTER` queues behind it needing ACCESS EXCLUSIVE, and every *new*
reader and writer then queues behind the waiting `ALTER`: a long export plus one impatient
migration can brown-out the table for the source application. Chunked export inverts this: each
chunk is a short statement, DDL slips in **between** chunks, and mid-reload DDL becomes a real
case needing a policy. Two candidates:

- **Restart-on-DDL (chosen).** Any `ddl_manifest` row for the table with `c_lsn` past the
  reload's first watermark invalidates the reload: bump to a fresh `reload_id`, purge the
  superseded `kind='reload'` manifest rows, re-export from chunk zero at the newest schema.
  Every reload is single-schema **by construction** — the loader never handles version-crossing
  *inside* a rebuild, only in the stream, where that logic already runs and is tested. The
  failure mode is bounded and visible: wasted export work, guarded by a retry cap + alert
  (a migration-heavy week on a huge table could otherwise livelock the reload forever). Restart
  hygiene matters because detection is async: the loader must honor only the *latest* reload_id
  per table, or a stale chunk file claimed late could re-trigger a rebuild.
- **Per-chunk version tolerance (noted, not chosen).** Each chunk file carries the schema_version
  current at its export and interleaves with `ddl_manifest.c_lsn` in claim order, so the loader's
  existing reconciliation *could* in principle apply mid-reload unchanged
  ([walrus-loader](./walrus-loader.md)). Rejected as the default on reliability grounds: it
  exercises the least-hardened corner of the loader (Tier-2 DDL evolution is still open) against
  a half-populated, mid-rebuild table, and its failure mode is subtle mis-reconciliation rather
  than a visible retry. Correct in principle; strictly more states to get wrong. Revisit if
  restart churn on DDL-heavy tables ever becomes a measured problem.

One case to state explicitly either way: in quarantine recovery, the previously-fatal lossy cast
reconciles trivially during the rebuild because it runs against an empty table — the reload
doesn't "retry" the cast on old data, it replaces the data. Restart-on-DDL composes nicely with
this: a restart automatically re-exports at the newest schema, which is exactly where quarantine
recovery wants to land.

### H10 — "The snowflake sink updates the state when it's complete" — there is no snowflake sink

Zero references in the repo; v1's terminus is the per-table `.duckdb` mirror. Design the terminal
transition generically instead: the sink marks `export_complete` carrying the final chunk watermark
`H`; the loader flips `complete` when `transformed_lsn ≥ H` for that table. Any future downstream
consumer gets a documented hook — "observe `table_reload.status='complete'` in control-pg" — rather
than a write path of its own into someone else's state row.

### H11 — Preflight gaps

- The target table must be **in the publication** — publication scope is fixed at sink startup
  (`manage_publication`); a reload request for an unpublished table must fail fast at request time,
  not discover it mid-export.
- `walrus.reload_signal` must itself be added to the publication (and have a PK) or the entire
  mechanism silently never fires — the classic first-run failure of Debezium signal setups.
- PK required on the target table — already a v1 invariant, and it is precisely what makes the
  dedup algebra (and therefore the whole overlap story) sound.

## 4. The rejected alternative: a temporary slot per reload

`CREATE_REPLICATION_SLOT ... TEMPORARY ... (SNAPSHOT 'export')` hands you the atomic
(snapshot, consistent_point) pair and reuses the bootstrap code path nearly verbatim — and it is
how Postgres's own logical replication resyncs a single table (per-table tablesync workers with
short-lived slots). Rejected here because: it consumes a slot per concurrent reload (the constraint
the feature exists to avoid — "I'm not limited to the amount of replication slots"), the slot plus
its exported snapshot pin WAL and xmin for the full export duration, and the walsender session must
sit open the whole time for the snapshot to stay valid. Worth keeping in the back pocket as the
fallback if the chunked path hits an unforeseen wall — it trades the scaling goal for
implementation simplicity, not for correctness.

## 5. The revised shape (recommended)

Rebuild flavor, end to end:

1. **Request.** Operator inserts into control-pg `walrus.table_reload`
   (`reload_id`, table, flavor=`reload`, status=`requested`). Duplicate requests collapse on
   `reload_id`; a second reload of the same table while one is non-terminal is rejected.
2. **Pickup.** Sink polls on heartbeat cadence, takes the lease, flips `exporting`. Loader sees
   the status and stops claiming X's manifest rows; frontier freezes at `W`.
3. **Chunks.** For each PK-ordered chunk: INSERT watermark row into source
   `walrus.reload_signal` (the row also stores `pg_current_wal_insert_lsn()` computed at insert
   time — not the authoritative stamp, a cross-check; see H1) → observe own echo in-stream ⇒
   learn `L_i` from the decoded commit LSN, assert embedded < `L_i` → chunk SELECT on the side
   connection → Parquet with rows stamped `commit_lsn = lsn = L_i`, `kind='reload'`, `reload_id`,
   current schema_version → manifest row (`lsn_end = L_i`) → chunk cursor to `table_reload`.
   Streaming for all tables, including X, never pauses.
4. **Rebuild + drain.** Loader claims X's rows in `(lsn_end, id)` order as always. First
   `kind='reload'` file with an unseen `reload_id` triggers `CREATE OR REPLACE` of mirror + raw and
   the purge of superseded pending rows; then chunk and stream files interleave through ordinary
   Phase A/B — the "same batch" property the proposal wanted falls out of manifest ordering,
   no special batch logic.
5. **DDL invalidation.** If a `ddl_manifest` row for X arrives with `c_lsn` past the reload's
   first watermark while the reload is non-terminal: fresh `reload_id`, purge superseded
   `kind='reload'` manifest rows, restart from chunk zero at the newest schema (retry-capped,
   alert on cap; see H9).
6. **Done.** After the last chunk the sink writes `export_complete` + final watermark `H`; the
   loader flips `complete` when `transformed_lsn ≥ H`. Statuses:
   `requested → exporting → export_complete → complete`, with `failed` reachable from the two
   middle states (lease expiry, poisoned chunk, or DDL-restart cap exhausted), resumable via the
   chunk cursor.

Scale-out for "reload N tables": N rows in `table_reload`, drained under `max_concurrent_reloads`,
chunks interleaved — source load is a tunable, not a spike. For very large tables the chunk SELECT
fan-out can later compose with the CTID-range machinery of
[deferred goal §3](./deferred-goals.md#3-faster-initial-export--backfill--parallel-ctid-range-snapshot-nearest-term).

## 6. Open questions

- **Commit-visibility vs WAL order.** The stamp-early argument assumes a chunk SELECT issued after
  observing `L_i` in-stream sees every transaction with commit LSN ≤ `L_i`. Postgres commit
  *visibility* order can diverge from WAL order in tiny windows (and `synchronous_commit=off`
  widens them). DBLog/Debezium live with the same assumption; verify it holds hard enough for
  walrus, or bound it (e.g. observe one extra echo round-trip before selecting).
- **Read-only watermarking.** Debezium ≥ 2.4 offers a Postgres watermark mode using
  `pg_current_snapshot()` instead of table writes — no signal-table writes on the source at all.
  Worth evaluating before building the write path, though mapping xid-snapshots into walrus's
  LSN-total-order algebra is real design work; the write-based watermark is the conservative
  choice.
- **Vendor precedent for the monolithic shape.** Research verified the chunked designs (Debezium,
  DBLog) to primary sources; whether any production tool ships the *full-export-plus-overlap*
  design (Airbyte resync, PeerDB, Fivetran, AWS DMS reload-table) went unverified. If precedent
  exists it would argue the simpler shape is shippable; absence of evidence isn't absence.
- **Raw history semantics after rebuild.** ✅ **Resolved (PR 6.7 + PR 6.10).** A `reload`'s
  `CREATE OR REPLACE` of `<table>_raw` discards the table's CDC history in DuckDB (S3 Parquet
  persists subject to GC) — acceptable for quarantine recovery. A `resync` takes the **uniform Phase
  A path**: its chunk rows append into `<table>_raw` like any file, so raw history is preserved. One
  path, no special-casing; see [PR 6.7](./implementation/phase-6-single-table-reload/pr-6.7-loader-rebuild-trigger.md)
  and [PR 6.10](./implementation/phase-6-single-table-reload/pr-6.10-resync-flavor.md).
- **Interaction with loader sharding** ([deferred goal §2](./deferred-goals.md#2-multi-pod-loader-table-sharding-horizontal-scale-out)):
  the rebuild trigger runs under the table's ownership lease, so it inherits the fencing story —
  confirm the reload lease and the ownership lease can't deadlock or interleave badly.

## Operating a reload (runbook — PR 6.11)

The `walrus.table_reload` row **is** the operator interface. Metrics/dashboards (PR 6.11) show
aggregate health; this runbook turns a row into a decision.

**Request.** Pick the flavor with the [decision guide](#h3--refresh-and-rebuild-are-different-operations-the-proposal-conflates-them)
above (`resync` = cheap drift repair, keeps the table queryable, tolerates phantoms; `reload` = the
truth reset / quarantine recovery). Then:

```
just reload table='public.orders'                 # rebuild (default)
just reload table='public.orders' flavor='resync' # refresh over the live mirror
```

**Watch.** One SQL query is the whole progress view — the status walk is
`requested → exporting → export_complete → complete` (`failed` is terminal):

```sql
SELECT reload_id, flavor, status, chunk_no, restart_count,
       first_lsn, final_lsn, lease_holder, lease_expiry, left(error, 60) AS error
FROM walrus.table_reload
WHERE source_schema = 'public' AND source_table = 'orders'
ORDER BY reload_id DESC LIMIT 5;
```

`chunk_no` climbs during `exporting`; `final_lsn` (H) is set at `export_complete`; the loader flips
`complete` once `transformed_lsn ≥ H`. On the dashboard, the "Single-table reload" row shows
reloads-in-flight, chunks/rows-per-second, echo p99, and the failure/anomaly panel.

**Unstick.** A reload `exporting` for a long time with a **stale lease** (`lease_expiry < now()`)
means its exporter died and nothing adopted it (the `WalrusReloadLeaseStuck` page / the
`walrus_reload_lease_stale` gauge). Two options:

- **Preferred — restart the sink.** On startup the controller's adoption scan re-acquires its own /
  expired `exporting` leases and resumes from the chunk cursor (PR 6.9). No data is re-exported at
  or before the cursor.
- **Give up on the attempt.** Mark it failed (this also purges its staged `kind='reload'` chunk
  files, so the loader claims nothing stale — the coupling is in `reload::fail`):

```sql
-- Only a reload that is genuinely orphaned — verify lease_expiry < now() first.
UPDATE walrus.table_reload SET status = 'failed', error = 'operator: stuck lease',
       updated_at = now()
WHERE reload_id = :id AND status = 'exporting';
DELETE FROM walrus.file_manifest WHERE reload_id = :id;   -- (reload::fail does both atomically)
```

Then re-request with `just reload` when ready.

**Cross-check violation (page, `WalrusReloadCrosscheckViolation`).** This is the one that means the
watermark *model* is wrong — possible silent data loss. **Do not just restart the pod.** Stop issuing
reloads, capture the `reload_id`/`chunk_no` from the error log lines (`embedded wal_insert_lsn >=
commit LSN`), and open an issue. The cross-check counter should read `0` forever in a healthy system.

**Retention.** The source `walrus.reload_signal` table is insert-only (one row per chunk watermark).
It is tiny and bounded by concurrent reloads, but if you want to reclaim it, deleting rows for a
`complete`/`failed` reload is safe — the echoes were consumed in-stream long ago:

```sql
DELETE FROM walrus.reload_signal s
USING walrus.table_reload r
WHERE s.reload_id = r.reload_id AND r.status IN ('complete', 'failed');
```

## 7. References

- [Debezium: Incremental Snapshots blog (2021)][incremental snapshots blog] — chunking rationale,
  signal flow, dedup window, consumer contract.
- [Debezium signalling documentation][Debezium signalling docs] — signal table shape, insert-only
  semantics, `execute-snapshot` / `stop-snapshot` / `pause-snapshot` lifecycle.
- [Debezium design document DDD-3][DDD-3] — watermark windows, continuation predicate, explicit
  DBLog lineage, downstream ordering contract.
- [DBLog: A Watermark Based Change-Data-Capture Framework][arXiv 2010.12597] — the watermark
  algorithm and the "no LSN for a SELECT" premise.
- [Netflix tech blog: DBLog][Netflix blog] — watermark-table mechanics in production.
- [Debezium: read-only incremental snapshots blog][read-only blog] — the non-write watermarking
  lineage (`pg_current_snapshot()` mode).
- Internal: [architecture §1.7](./architecture.md#17-snapshot--backfill-bootstrap),
  [§1.8](./architecture.md#18-single-slot-for-life--total-restart),
  [walrus-loader](./walrus-loader.md) §4–5,
  [deferred-goals §1](./deferred-goals.md#1-single-table-reload--re-sync-while-streaming).

[incremental snapshots blog]: https://debezium.io/blog/2021/10/07/incremental-snapshots/
[Debezium signalling docs]: https://debezium.io/documentation/reference/stable/configuration/signalling.html
[DDD-3]: https://github.com/debezium/debezium-design-documents/blob/main/DDD-3.md
[arXiv 2010.12597]: https://arxiv.org/abs/2010.12597
[Netflix blog]: https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b
[read-only blog]: https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/
