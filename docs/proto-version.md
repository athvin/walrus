# `proto_version` and the pgoutput stream — how the WAL actually looks on the wire

> **Status: empirical companion to [architecture.md](./architecture.md).** Where the architecture
> *asserts* how the `pgoutput` logical-replication transport behaves (`proto_version '2'` +
> `streaming 'on'`, [§1.1](./architecture.md#11-source-side-setup-one-time-via-migrationjob),
> [§1.6](./architecture.md#16-large-transaction-safety)), this doc **proves it by running it**.
> Every hex dump, message count, and frame below was captured from a real **PostgreSQL 16.14** in
> Docker using the harness in [`examples/proto-version/`](./examples/proto-version/) — nothing here
> is invented. Reproduce it with `docker compose up --wait` and `./03-capture.sh`.
>
> Object IDs (OIDs) and transaction IDs (xids) in the captures are dynamic — they differ run to
> run. The *shapes* do not.

---

## TL;DR — the five things that matter for walrus

1. **`proto_version` is a per-connection option, not a slot property.** The slot is a plain
   `pgoutput` slot; the version and `streaming` are re-negotiated on every `START_REPLICATION`.
   Proven below: the *same* slot consumed at v1 and v2 emits **byte-for-byte identical** output
   for a normal transaction.
2. **`streaming 'on'` (which needs `proto_version >= 2`) is the whole point.** Without it, even at
   v2 a large transaction is buffered on the *server* until commit and delivered in one flood. With
   it, a large in-progress transaction is chopped into **stream blocks** and sent *before* it
   commits.
3. **"Bunched up" = one big transaction split into blocks, and multiple transactions' blocks
   interleaved on the wire — not small transactions merged.** Each streamed message is tagged with
   its transaction's xid so the consumer can demultiplex. Small transactions still arrive **whole,
   at commit**.
4. **Streamed changes are provisional until you're told their fate.** You must hold them invisible
   until `Stream Commit`, and **discard** them on `Stream Abort`. The subtle, dangerous case is a
   **rolled-back savepoint inside a committed transaction**: its rows *are* streamed, and only a
   `Stream Abort` naming that sub-transaction's xid tells you to drop them.
5. **This is the sink's job, not the loader's.** `walrus-pg-sink` is the stream consumer that does
   the buffering / commit-gating / abort-dropping ([§1.6](./architecture.md#16-large-transaction-safety)).
   `walrus-loader` lives downstream of S3 and only ever sees already-committed staged Parquet.

---

## Table of contents

- [1. What `proto_version` is (and isn't)](#1-what-proto_version-is-and-isnt)
- [2. The version matrix](#2-the-version-matrix)
- [3. Two lenses: test_decoding vs pgoutput](#3-two-lenses-test_decoding-vs-pgoutput)
- [4. The message catalog, decoded byte-by-byte](#4-the-message-catalog-decoded-byte-by-byte)
- [5. TupleData and the unchanged-TOAST placeholder](#5-tupledata-and-the-unchanged-toast-placeholder)
- [6. REPLICA IDENTITY: what old-row data you get](#6-replica-identity-what-old-row-data-you-get)
- [7. The per-message xid (v2+): why it exists](#7-the-per-message-xid-v2-why-it-exists)
- [8. Streaming: how a big transaction is chopped up](#8-streaming-how-a-big-transaction-is-chopped-up)
- [9. Abort and rollback — the case that can corrupt a mirror](#9-abort-and-rollback--the-case-that-can-corrupt-a-mirror)
- [10. Interleaving and commit ordering](#10-interleaving-and-commit-ordering)
- [11. Protocol axis, side by side](#11-protocol-axis-side-by-side)
- [12. Two-phase (v3) and parallel apply (v4)](#12-two-phase-v3-and-parallel-apply-v4)
- [13. The consumer contract for walrus](#13-the-consumer-contract-for-walrus)
- [14. Reproduce it yourself](#14-reproduce-it-yourself)
- [Sources](#sources)

---

## 1. What `proto_version` is (and isn't)

`proto_version` is an **output-plugin option** passed to `pgoutput` in the option list of
`START_REPLICATION`. It is **not** baked into the slot, and there is **no special slot type** for
large transactions. A slot is created plugin-only:

```sql
-- the slot knows only its plugin ('pgoutput'); nothing about version or streaming:
SELECT * FROM pg_create_logical_replication_slot('slot_pg', 'pgoutput');
```

…and the version + streaming are chosen **per connection**, and can change on reconnect with no
slot recreation:

```
START_REPLICATION SLOT slot_pg LOGICAL 0/0 (
    proto_version '2',            -- pgoutput protocol version (min for streaming)
    streaming 'on',               -- what actually streams in-progress transactions
    publication_names 'pub'
)
```

**Proof it's per-connection.** We consumed the *same* `slot_pg` for one ordinary insert at
`proto_version '1'` and then `'2'`. The decoded bytes are identical:

```text
# proto_version '1'
BEGIN     final_lsn=0/20114A0 commit_ts=2026-07-05T14:04:41Z xid=771
RELATION  oid=16404 public.customers replident='d' cols=[KEY region, KEY id, name]
INSERT    rel=16404 new=['eu', '7', 'Zeta']
COMMIT    commit_lsn=0/20114A0 end_lsn=0/20114D0

# proto_version '2'  — same slot, same change, SAME BYTES
BEGIN     final_lsn=0/20114A0 commit_ts=2026-07-05T14:04:41Z xid=771
RELATION  oid=16404 public.customers replident='d' cols=[KEY region, KEY id, name]
INSERT    rel=16404 new=['eu', '7', 'Zeta']
COMMIT    commit_lsn=0/20114A0 end_lsn=0/20114D0
```

The takeaway that trips people up: **bumping to v2 does not, by itself, change the wire format.**
v2's additions (the streaming frames and the per-message xid) only appear **when a transaction is
actually streamed** (§7, §8). For everything else, v2 output equals v1 output.

## 2. The version matrix

`proto_version` accepts `1`, `2`, `3`, `4` [3][20]. Each is a strict superset:

| version | server | what it adds | walrus |
|---|---|---|---|
| `1` | PG 10+ | Baseline: `Begin`/`Commit`, `Relation`, `Type`, `Insert`/`Update`/`Delete`, `Truncate`, `Origin`, `Message`. Transactions decoded **only after commit**. | insufficient |
| **`2`** | **PG 14+** | **Streaming of large in-progress transactions**: `Stream Start`/`Stop`/`Commit`/`Abort` frames + a per-message **xid**. | **← target** |
| `3` | PG 15+ | Streaming of **two-phase** (`PREPARE TRANSACTION`) commits: `Begin Prepare`, `Prepare`, `Commit Prepared`, `Rollback Prepared`, `Stream Prepare`. | not needed |
| `4` | PG 16+ | **Parallel apply** (`streaming 'parallel'`): adds abort LSN/timestamp to `Stream Abort` so a subscriber can apply a streamed txn with background workers. | not needed |

We observed the exact server-side guard directly — asking for streaming at v1 is rejected [19]:

```text
$ ... pg_logical_slot_peek_binary_changes('slot_pg', …, 'proto_version','1', 'streaming','on')
ERROR:  requested proto_version=1 does not support streaming, need 2 or higher
```

walrus targets **v2**: it is the *minimum* version that permits streaming, and v3/v4 add features a
file-staging, reconcile-downstream consumer doesn't use (§12).

## 3. Two lenses: test_decoding vs pgoutput

`pgoutput` is **binary** — the format walrus decodes, but unreadable by eye. `test_decoding` is a
**text** plugin that decodes the *same* WAL into human-readable lines. Throughout this doc we show
both, so the binary is legible. They are two lenses on one stream:

| | plugin | how we read it |
|---|---|---|
| readable | `test_decoding` | `pg_logical_slot_peek_changes(...)` → text |
| what walrus decodes | `pgoutput` | `pg_logical_slot_peek_binary_changes(...)` → `bytea` → `encode(data,'hex')` → [`decode_pgoutput.py`](./examples/proto-version/decode_pgoutput.py) |

`peek` is **non-consuming** (the slot doesn't advance — re-running shows the same data); `get`
**consumes** (advances `confirmed_flush_lsn`, lets old WAL recycle). We use `peek` while
experimenting so nothing is lost.

## 4. The message catalog, decoded byte-by-byte

Field primitives [20]: `Int8`=1 byte, `Int16`=2, `Int32`=4 (also OID / xid), `Int64`=8 (LSN, or
`TimestampTz` = µs since 2000-01-01), `String`=null-terminated. Below, every hex string is a real
captured message; brackets are our annotation.

Scenario: on a single-PK table `orders` (`REPLICA IDENTITY DEFAULT`), we ran
`INSERT (id=1)`, `UPDATE`, a **PK-changing** `UPDATE (id 1→100)`, and `DELETE`. test_decoding:

```text
BEGIN 749
table public.orders: INSERT: id[integer]:1 status[text]:'new' amount[numeric]:19.99 feeling[mood]:'happy' note[text]:'first'
table public.orders: UPDATE: id[integer]:1 status[text]:'shipped' ...
table public.orders: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:100 status[text]:'shipped' ...
table public.orders: DELETE: id[integer]:100
COMMIT 749
```

### Begin `'B'` — `Byte1('B')`, Int64 final LSN, Int64 commit ts, Int32 xid

```text
42 000000000199bac8 0002f8dc466a6b4f 000002ed
'B' └final_lsn=0/199BAC8┘ └commit ts──────┘ └xid=749┘
```

### Type `'Y'` — `Byte1('Y')`, Int32 type OID, String namespace, String name
Emitted the first time a non-builtin type is referenced. `mood` is our custom enum:

```text
59 00004006     7075626c6963 00  6d6f6f64 00
'Y' └oid=16390┘ └"public"────┘   └"mood"─┘
```

### Relation `'R'` — table shape; sent once per relation per stream, before its first change

```text
52 0000400d 7075626c6963 00 6f7264657273 00 64  0005  <5 columns…>
'R'└oid=16397┘└"public"─┘   └"orders"───┘   'd' └cols=5┘
```

`'d'` is the replica identity (`relreplident`). Each column is
`Int8 flags` (bit 1 = **key**), `String name`, `Int32 type OID`, `Int32 atttypmod`:

```text
01 6964 00 00000017 ffffffff   → KEY  id      oid=23   (int4)     mod=-1
00 7374617475... 00000019 ...  →      status  oid=25   (text)     mod=-1
00 616d6f756e74 000006a4 000a0006 → amount oid=1700 (numeric) mod=655366  (= numeric(10,2))
00 6665656c696e67 00004006 ... →      feeling oid=16390 (mood)    mod=-1
00 6e6f7465 00000019 ffffffff  →      note    oid=25   (text)     mod=-1
```

### Insert `'I'` — `Byte1('I')`, Int32 relation OID, `Byte1('N')`, TupleData (new tuple)

```text
49 0000400d 4e 0005 74 00000001 31  74 00000003 6e6577  74 00000005 31392e3939 …
'I'└rel────┘ 'N'└c=5┘'t'└len=1┘"1"  't'└len=3┘"new"    't'└len=5┘"19.99"
```
(TupleData column formats — `t`/`n`/`u`/`b` — are §5.)

### Update `'U'` (PK-changing) — carries the old key as a `'K'` submessage first

```text
55 0000400d 4b  0005 74 00000001 31 6e 6e 6e 6e  4e 0005 74 00000003 313030 …
'U'└rel────┘'K' └old key: id="1", rest NULL────┘ 'N' └new tuple: id="100", …
```

Under `REPLICA IDENTITY DEFAULT`, the old image is **key columns only** — here just `id`; the other
four columns are `'n'` (null). The `'K'` submessage appears *only* because a key column changed;
a normal `UPDATE` that leaves the PK alone carries **no** old image at all (§6).

### Delete `'D'` — `Byte1('D')`, Int32 relation OID, `'K'` or `'O'` submessage (the old identity)

```text
44 0000400d 4b 0005 74 00000003 313030 6e 6e 6e 6e
'D'└rel────┘'K'└old key: id="100", rest NULL──────┘
```

### Commit `'C'` — `Byte1('C')`, Int8 flags, Int64 commit LSN, Int64 end LSN, Int64 commit ts

```text
43 00 000000000199bac8 000000000199baf8 0002f8dc466a6b4f
'C'flags└commit_lsn────┘ └end_lsn───────┘ └commit ts─────┘
```

### Truncate `'T'` — `Byte1('T')`, Int32 rel-count, Int8 option bits, then the relation OIDs
Option bits: `1`=CASCADE, `2`=RESTART IDENTITY. Two captures:

```text
TRUNCATE  opts=none              rels=[16595]   ← TRUNCATE orders;
TRUNCATE  opts=CASCADE|RESTART IDENTITY rels=[16609] ← TRUNCATE items RESTART IDENTITY CASCADE;
```
`Truncate` carries **no tuple/PK** — which is exactly why walrus can't treat it as a `MERGE` branch
and handles it as a separate wipe step ([architecture §2.1](./architecture.md#21-the-raw-to-mirror-transform-model)).

### Message `'M'` — logical decoding message (walrus's heartbeat, [§1.9](./architecture.md#19-slot-liveness--heartbeat--keepalive))

```text
MESSAGE  transactional     prefix='walrus' content=b'heartbeat-txn'      ← pg_logical_emit_message(true, …)
MESSAGE  non-transactional prefix='walrus' content=b'heartbeat-nontxn'   ← pg_logical_emit_message(false, …)
```

The distinction is load-bearing for a heartbeat: a **transactional** message is held and delivered
*in commit order inside* its transaction; a **non-transactional** message is emitted **immediately**,
even *ahead of* the enclosing transaction's `Begin`. In our capture the non-transactional heartbeat
appeared **before** `BEGIN`, while the transactional one appeared inside the txn just before
`COMMIT` — proof the non-transactional variant isn't gated on commit, which is what lets it advance
the slot while everything else is idle.

> **walrus's default is the published `walrus.heartbeat` *table*, not this message.** A table gives a
> durable last-beat record and a natural, decodable round-trip — the sink writes it, then observes its
> own `beat_seq` return through the stream (the WAL-consume/slot-advance liveness signal in
> [architecture.md §1.9](./architecture.md#19-slot-liveness--heartbeat--keepalive)).
> `pg_logical_emit_message()` (shown here) is the **table-less alternative**; if used, prefer the
> **non-transactional** form so an idle beat isn't gated on some unrelated open transaction.

## 5. TupleData and the unchanged-TOAST placeholder

A `TupleData` is `Int16 column-count` then, per column, a format byte [20]:

| byte | meaning | followed by |
|---|---|---|
| `n` | SQL NULL | nothing |
| `u` | **unchanged TOAST** — the stored value was not modified, so **it is not sent** | nothing |
| `t` | text value | `Int32` length + bytes |
| `b` | binary value | `Int32` length + bytes (only if `binary 'on'`) |

The `u` case matters to walrus because the value is **absent from the wire** and must be recovered
elsewhere ([architecture §1.4 / §2.1 TOAST handling](./architecture.md#21-the-raw-to-mirror-transform-model)).
To capture it, we forced a column out-of-line (`SET STORAGE EXTERNAL`, so it isn't inlined by
compression), inserted a 5000-char value, then updated a *different* column:

```text
# test_decoding — the UPDATE renders the unchanged column as a marker:
table public.orders: UPDATE: id[integer]:888 status[text]:'changed' ... note[text]:unchanged-toast-datum

# pgoutput — the new-tuple column for `note` is the 'u' byte (no value on the wire):
INSERT  rel=16595 new=['888', 'new', NULL, NULL, 'TTTTTTTT…(5000)']
UPDATE  rel=16595 new=['888', 'changed', NULL, NULL, <unchanged-TOAST>]
```

So an `UPDATE` can arrive with a column whose value you never see. walrus's loader back-scans
`<table>_raw` for the last real value of that PK to fill it in.

## 6. REPLICA IDENTITY: what old-row data you get

The `Relation` message advertises the identity (`'d'` default / `'f'` full / `'n'` nothing /
`'i'` index), and it decides what old-row data `Update`/`Delete` carry [20]:

| identity | UPDATE old image | DELETE old image |
|---|---|---|
| **DEFAULT** (PK) | `'K'` (key columns) **only if a key column changed**, else nothing | `'K'` (key columns) |
| **FULL** | `'O'` = the **entire** old row | `'O'` = the **entire** old row |
| NOTHING | none (publisher errors on update/delete) | none |

Captured contrast — the DEFAULT table `orders` sent key-only (§4); the FULL table `items` sends the
whole old row:

```text
# items has REPLICA IDENTITY FULL:
UPDATE  rel=16609 old[O]=['1', 'widget', '5']  new=['1', 'widget', '9']
DELETE  rel=16609 old[O]=['1', 'widget', '9']
```

This is why walrus **mandates a primary key** and `REPLICA IDENTITY DEFAULT`
([architecture §1.1](./architecture.md#11-source-side-setup-one-time-via-migrationjob)): DEFAULT
gives a stable key for the current-state `MERGE` without the cost of shipping full old rows, and
without needing `FULL` + full reloads.

## 7. The per-message xid (v2+): why it exists

Under v2 the change/schema messages (`Relation`, `Type`, `Insert`, `Update`, `Delete`, `Truncate`,
`Message`) gain a 4-byte xid immediately after the type byte — **but only while streaming** [20].
We proved the "only while streaming" clause directly. Recall the non-streamed `Type` from §4:

```text
59 00004006 …      ← 'Y', then straight to type OID 16390. NO xid.
```

The *same* `Type` message inside a **streamed** transaction gains the prefix:

```text
59 000002f1 00004006 …   ← 'Y', xid=753, THEN type OID 16390.
   └xid───┘
```

Why: with streaming on, blocks of *different* in-progress transactions are **interleaved** on the
wire (§10). The frame-level anchor (`Stream Start`) names the current top-level xid, and the
per-message xid lets the consumer route each individual change to the right transaction's buffer.

A crucial detail we measured (§9): the per-message xid is the **sub-transaction** xid, while
`Stream Start` carries the **top-level** xid. That difference is what makes savepoint rollback
tractable.

## 8. Streaming: how a big transaction is chopped up

Streaming triggers when the **total** decoded size across **all** in-progress transactions exceeds
`logical_decoding_work_mem` (default **64MB**; we set it to **64kB** so a few thousand rows suffice).
At that point Postgres streams the **single largest top-level transaction** [4][19]. Its changes
arrive in one or more `Stream Start … Stream Stop` **blocks**, and the final act is `Stream Commit`
(or `Stream Abort`).

We inserted **8000 rows in one committed transaction**. pgoutput message histogram:

```text
49 (Insert)       × 8000
53 (Stream Start) × 22      45 (Stream Stop) × 22      ← 22 blocks
63 (Stream Commit)× 1
52 (Relation) × 1   59 (Type) × 1
```

So the one transaction was **"bunched up" into 22 blocks** — *this* is what the term means: a large
transaction chopped into pieces, **not** small transactions merged. `test_decoding` narrates the
same thing:

```text
opening a streamed block for transaction TXN 855
streaming change for TXN 855          (× thousands)
closing a streamed block for transaction TXN 855
… (22 open/close cycles) …
committing streamed transaction TXN 855
```

`Stream Start` carries a **first-segment flag**: `1` on the very first block for an xid, `0` on
every continuation. Measured across the 22 blocks:

```text
 1 × Stream Start xid=855 first_segment=1     ← opens the streamed context
21 × Stream Start xid=855 first_segment=0     ← continuations
```

The consumer uses the flag to decide "allocate a new buffer for this xid" vs "append to the existing
one." **Small transactions never stream** — with streaming on, a transaction below the threshold is
still delivered whole, bracketed by ordinary `Begin`/`Commit`, at commit time.

## 9. Abort and rollback — the case that can corrupt a mirror

This is the section that matters most for correctness. Streamed rows are **provisional**: you were
handed them before their fate was decided. Two failure shapes, and they behave differently.

### 9a. Whole transaction aborts — and *when* you decode changes what you see

A large transaction that `ROLLBACK`s. Here the **timing of decoding** matters, and it's a real trap:

- **Decoding after the fact (the SQL functions).** By the time
  `pg_logical_slot_peek_binary_changes` runs, the transaction has already aborted. Postgres detects
  this ("concurrent abort") and **short-circuits** — it opens an empty block and never streams the
  rows, emitting **no `Stream Abort` frame**:
  ```text
  # histogram of an 8000-row ROLLBACK, decoded via the SQL function:
  53 (Stream Start) × 1     45 (Stream Stop) × 1     ← one empty block, nothing else
  ```
- **Decoding live (a real walsender — what walrus uses).** `pg_recvlogical` (i.e.
  `START_REPLICATION`) decodes *concurrently*, so it streams the rows **before** the rollback is
  known, then emits a real `Stream Abort` where **sub-xid == top-xid**:
  ```text
  # same shape of transaction, over a live pg_recvlogical connection:
  5712 × INSERT  (streamed before the rollback)
    16 × Stream Start / Stream Stop blocks
     1 × STREAM ABORT  top_xid=866 sub_xid=866   ← sub == top ⇒ WHOLE-TXN abort
  ```

**Consequence for walrus:** the sink connects with a live walsender, so it lives in the *second*
world — it **will** receive rows for a transaction that ends up aborting, and the `Stream Abort` is
its signal to throw them away. A test harness that only pokes the SQL functions would see the
short-circuit and wrongly conclude "aborted rows never arrive." They do.

### 9b. A rolled-back savepoint inside a *committed* transaction (the dangerous one)

This is the case that silently corrupts a mirror if mishandled. A transaction that **commits**, but
contains a `SAVEPOINT` that was `ROLLBACK`ed. The savepoint's rows **are streamed** — and only a
`Stream Abort` naming that sub-transaction tells you to drop them. We ran:

```sql
BEGIN;
  INSERT … 3000 rows  'kept-A';        -- top-level
  SAVEPOINT sp1;
  INSERT … 3000 rows  'ROLLED-BACK';   -- sub-transaction
  ROLLBACK TO sp1;
  INSERT … 3000 rows  'kept-B';        -- a new sub-transaction after the rollback
COMMIT;
```

The source table ends with **6000** rows (kept-A + kept-B). But pgoutput **streamed 8762 insert
messages** — more than survive. The per-message xids reveal exactly what happened:

```text
INSERT xid=857 : 3000     ← top-level          (kept-A)   → keep
INSERT xid=858 : 2762     ← rolled-back savepoint (ROLLED-BACK, partially streamed) → DISCARD
INSERT xid=859 : 3000     ← post-rollback sub   (kept-B)   → keep
```

and the closing frames:

```text
STREAM ABORT  top_xid=857 sub_xid=858   ← sub (858) != top (857) ⇒ SUBTRANSACTION rollback
STREAM COMMIT xid=857                    ← the top-level transaction still COMMITS
```

Read that carefully, because it's the whole ballgame:

- `Stream Start` blocks all carry the **top-level** xid (857).
- Each streamed change carries **its own sub-transaction xid** (857 / 858 / 859).
- `Stream Abort {top=857, sub=858}` means **"discard the changes tagged 858"**, even though the
  top-level transaction commits.
- `Stream Commit {857}` then makes the survivors (857 + 859 = 6000) visible.

2762 rows were handed to the consumer that must **not** land in the target. The **only** thing
standing between them and the mirror is honoring that sub-transaction `Stream Abort`. The whole-txn
case (§9a) is the coarse version of the same message (sub == top); this is the subtle one.

## 10. Interleaving and commit ordering

With streaming on, only one transaction is "open" between a given `Stream Start`/`Stop`, but across
cycles **different transactions alternate**. Two concurrent 6000-row transactions, block sequence on
the wire:

```text
Stream Start xid=861 first=1
Stream Start xid=862 first=1
Stream Start xid=861 first=0
Stream Start xid=862 first=0
Stream Start xid=861 first=0
Stream Start xid=862 first=0
…                              ← A, B, A, B, … perfectly interleaved
```

The consumer **must** demultiplex by xid to reassemble each transaction. And the punchline for
walrus's ordering model — the commit order is **not** the start order:

```text
STREAM COMMIT xid=862 commit_lsn=0/3B423D8   ← 862 started SECOND but commits FIRST
STREAM COMMIT xid=861 commit_lsn=0/3B626E8   ← 861 commits later (higher commit LSN)
```

This is precisely why walrus orders everything by **commit LSN**, never by row LSN or xid
([architecture Delivery semantics](./architecture.md#delivery-semantics-ordering--idempotency)):
streaming makes row-LSN order non-monotonic with respect to commit/visibility order, but commit LSN
is monotonic with delivery.

## 11. Protocol axis, side by side

Same insert, four ways, to isolate what each knob does:

| connection options | result on the wire |
|---|---|
| `proto_version '1'` | `Begin`/`Relation`/`Insert`/`Commit`, no xid prefix |
| `proto_version '2'`, `streaming 'off'` | **identical bytes** to v1 for a normal txn |
| `proto_version '2'`, `streaming 'off'`, **large txn** | still buffered whole; `Begin … 4000×Insert … Commit`, **no** `Stream *` frames |
| `proto_version '2'`, `streaming 'on'`, **large txn** | `Stream Start/Stop` blocks + per-message xid + `Stream Commit` (§8) |
| `proto_version '1'`, `streaming 'on'` | **`ERROR: … does not support streaming, need 2 or higher`** |

The third row is the one the architecture leans on: at v2 with streaming **off**, a 4000-row
transaction we ran came through as one buffered flood —

```text
42 (Begin) × … 49 (Insert) × 4001 43 (Commit) × …   — zero 53/45/63 frames
```

— proving `proto_version '2'` **alone** does not buy you large-transaction safety. You need the
pair `v2 + streaming 'on'`, which is why the architecture treats it as mandatory, not optional
([§1.1](./architecture.md#why-proto_version-2--streaming-on-and-the-pitfalls-it-avoids)).

## 12. Two-phase (v3) and parallel apply (v4)

For completeness — walrus needs neither:

- **v3 / `two_phase 'on'`** surfaces `PREPARE TRANSACTION` as its own boundary (`Begin Prepare`,
  `Prepare`, then later `Commit Prepared` / `Rollback Prepared`; the streamed variant is
  `Stream Prepare`). walrus stages changes to files and reconciles downstream, so it has no reason
  to distinguish a prepared transaction from an ordinary one — it can treat the eventual commit as
  the boundary. Not enabled.
- **v4 / `streaming 'parallel'`** ships extra info so a *subscriber* can apply one streamed
  transaction with several background workers in parallel (it also adds abort-LSN/timestamp fields
  to `Stream Abort`). walrus is not a Postgres subscriber and does its parallelism per-table in the
  loader, so this is irrelevant. Not enabled.

## 13. The consumer contract for walrus

Everything above distills into the rules `walrus-pg-sink` must implement in its hand-rolled decoder
([architecture §1.2](./architecture.md#12-replication-consumer--hand-rolled),
[§1.6](./architecture.md#16-large-transaction-safety)). **This is the sink, not the loader** — the
loader only ever sees committed, staged Parquet.

1. **Buffer streamed changes; keep them invisible until `Stream Commit`.** A `ready` manifest row
   is written only after the top-level `Stream Commit` for that xid. Streamed sub-batches may be
   staged to S3 to bound memory, but they are not made visible to the loader before commit.
2. **Key the buffer by the per-message (sub-transaction) xid, and track sub-transaction structure.**
   `Stream Start` gives the top-level xid; each change gives its sub-xid. You need both.
3. **On `Stream Abort {top, sub}`, discard the `sub` changes.** If `sub == top`, the whole
   transaction is gone (delete any speculative S3 files, drop the buffer). If `sub != top`, drop
   only that savepoint's rows — the transaction still commits. **Never let a rolled-back
   sub-transaction's rows reach `<table>_raw`** (§9b).
4. **Order by commit LSN.** Files/rows are applied in `(commit_lsn, …)` order, because streaming
   makes row-LSN order non-monotonic and commit order can differ from start order (§10).
5. **Never advance `confirmed_flush_lsn` past an open streamed transaction.** Hold it at the
   begin/first-segment LSN of the oldest still-open streamed xid until its `Stream Commit`, so a
   crash can always re-stream an incomplete-or-aborted transaction from the WAL
   ([architecture §1.5/§1.6](./architecture.md#15-the-durability-checkpoint-wal-bounding-invariant)).
6. **Handle the `u` (unchanged-TOAST) placeholder** — the value isn't on the wire; recover it
   downstream (§5).
7. **Keepalive feedback ≠ slot advance.** Reply to walsender keepalives on a sub-`wal_sender_timeout`
   cadence to stay connected, independent of the durability-gated `confirmed_flush_lsn`
   ([architecture §1.9](./architecture.md#19-slot-liveness--heartbeat--keepalive)).

## 14. Reproduce it yourself

Everything above is in [`examples/proto-version/`](./examples/proto-version/):

```bash
cd docs/examples/proto-version
docker compose up --wait                                          # Postgres 16, healthy
docker exec -i walrus-proto-pg psql -U postgres -d walrus < 01-setup.sql
./03-capture.sh          # runs all 8 scenarios, showing each in test_decoding AND pgoutput
docker compose down -v
```

- `docker-compose.yml` — Postgres 16, `wal_level=logical`, `logical_decoding_work_mem=64kB`.
- `01-setup.sql` — tables (single PK / composite PK / `REPLICA IDENTITY FULL`), a custom enum, the
  publication, and both slots.
- `02-commands.sql` — a copy-paste cheatsheet of the commands + capture queries.
- `03-capture.sh` — the authoritative reproducer of every capture in this doc.
- `decode_pgoutput.py` — a throwaway, stdlib-only pgoutput → readable decoder (not production code;
  it also decodes the live `pg_recvlogical` byte stream with `--stream`).

## Sources

Primary (PostgreSQL manual) — the load-bearing references; blogs corroborate.

- [3] Logical Streaming Replication Protocol (options: `proto_version`, `streaming`, `two_phase`) — https://www.postgresql.org/docs/current/protocol-logical-replication.html
- [20] Logical Replication Message Formats (every byte layout in §4–§7) — https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
- [19] Streaming of Large Transactions for Logical Decoding (trigger, "largest top-level transaction") — https://www.postgresql.org/docs/current/logicaldecoding-streaming.html
- [4] `logical_decoding_work_mem` (default 64MB) — https://www.postgresql.org/docs/current/runtime-config-resource.html
- Replication sub-protocol (`START_REPLICATION`, XLogData `'w'`, keepalive `'k'`, standby status `'r'`) — https://www.postgresql.org/docs/current/protocol-replication.html
- `pg_recvlogical` (the live walsender capture path) — https://www.postgresql.org/docs/current/app-pgrecvlogical.html
- `contrib/test_decoding` (the readable-lens strings: `opening/closing a streamed block`, `aborting streamed (sub)transaction`, `committing streamed transaction`) — https://github.com/postgres/postgres/tree/master/contrib/test_decoding
- PeerDB — Exploring versions of the Postgres logical replication protocol (interleaving, version mapping) — https://blog.peerdb.io/exploring-versions-of-the-postgres-logical-replication-protocol

Bracketed numbers `[n]` match the [architecture.md Sources](./architecture.md#sources) list where they overlap.
