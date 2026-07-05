# Test coverage — pgoutput / proto_version

Two layers of automated tests, plus the narrative captures in
[`../../proto-version.md`](../../proto-version.md). Coverage is driven by an
adversarially-verified edge-case catalog (68 cases across 8 lenses + 9 completeness-critic gaps,
produced by a discovery workflow and empirically checked against PG16).

```bash
# layer 1 — decoder golden vectors (pure; no Docker):
python3 -m unittest test_decode_pgoutput -v          # 24 tests

# layer 2 — live-Postgres wire behavior (needs the container):
docker compose up --wait
docker exec -i walrus-proto-pg psql -U postgres -d walrus < 01-setup.sql
./run-tests.sh                                        # 28 assertions
```

## Layer 1 — `test_decode_pgoutput.py` (decoder golden vectors)

Frozen `{bytes → expected fields}` pairs. Most are real captures; the 2PC and a couple of
streamed vectors are minimally hand-built and cross-checked (noted in the file). These double as
**portable fixtures for the future Rust sink decoder** — the same pairs must hold there.

Covers: every message type (`Begin`/`Commit`/`Origin`/`Relation`/`Type`/`Insert`/`Update`/
`Delete`/`Truncate`/`Message`/`Stream Start`/`Stop`/`Commit`/`Abort` + all four 2PC messages);
and these edge cases from the catalog:

| catalog case | assertion |
|---|---|
| `stream-abort-is-9-bytes-carries-no-lsn` | abort parses to exactly `{top,sub}`; no LSN/ts read under `streaming=on` |
| subtransaction vs whole-txn abort | `whole_txn` flag = `(top==sub)`; **flagship** test |
| `default-identity update, no key change` | **no** old image (`old_kind is None`) |
| PK-changing update | old `K` = key columns only, rest NULL |
| `replica-identity-full-keyflag-expansion` | every Relation column flagged `key`, `replident='f'` |
| `composite-pk-partial-key-change` | old `K` carries **all** key cols even if one changed |
| `null-vs-unchanged-toast-vs-empty-string` | `n` / `u` / `t''` are three distinct column forms |
| `unchanged-toast` | `u` column carries **no value** on the wire |
| `generated-stored-column-omitted` | generated col absent from Relation **and** tuples |
| `add-column-relation-reemit` | re-emitted Relation carries the new column |
| numeric typmod | `numeric(10,2)` → `typmod=655366`; `-1` sentinel normalised |
| `message-byte-collisions-by-context` | `K` as Commit-Prepared msg vs `K` as update key-marker |
| 2PC messages (v3) | `Begin Prepare`/`Prepare`/`Commit Prepared`/`Rollback Prepared` parse (walrus never sees these at v2, but the decoder must not misalign) |
| `logical-message-content-length-prefixed` (critic) | `Message` content read as length-prefixed bytes, not C-string |
| pg_recvlogical `0x0a` framing | `parse_stream` skips separators; streaming context persists across messages |

## Layer 2 — `run-tests.sh` (live wire behavior)

28 pass/fail assertions against a real PG16. Covers the behavioral catalog cases that can't be a
static vector:

| # | catalog case | what it asserts |
|---|---|---|
| 1 | subtransaction rollback | 1 `Stream Abort` (sub≠top) + 1 `Stream Commit`; 3 distinct (sub)xids; aborted rows dropped (source keeps 4000, not 6000) |
| 2 | `nested-savepoint-rollback-emits-one-abort-per-level` | **two** `Stream Abort` frames, same `top_xid`, **descending** `sub_xid` (innermost-first); 1 commit |
| 3 | `release-savepoint-keeps-rows-no-abort` | **zero** aborts; all rows survive (sub_xid≠top **without** an abort ⇒ keep) |
| 4 | `small-abort-emits-nothing` | a small rolled-back txn produces no messages (SQL path) |
| 5 | `same-commit-truncate-then-insert-tuple-boundary` | `TRUNCATE`+`INSERT` in one txn share one `commit_lsn` |
| 6 | `empty-txn-fully-suppressed` | empty transactions emit nothing |
| 7 | interleaving + commit order | two concurrent large txns → 2 commits, alternating stream-start xids |
| 8 | protocol guard | `proto_version=1` + `streaming=on` → server error |
| 9 | `get-is-destructive-no-rewind` | `peek` non-consuming (idempotent); `get` advances the slot |
| 10 | `heartbeat-advances-idle-slot` | a committed heartbeat advances `confirmed_flush_lsn` |
| 11 | `two-slots-independent` | consuming `slot_pg` leaves `slot_test` unaffected |
| 12 | `replica-identity-nothing-publisher-error` | `UPDATE` under `REPLICA IDENTITY NOTHING` is refused |

## Documented (narrative) but not a discrete automated test

Fully captured and explained in [`../../proto-version.md`](../../proto-version.md), not re-asserted
in code: the **SQL-function vs live-walsender abort divergence** (§9a — concurrent-abort
short-circuit vs a real `Stream Abort`), the **per-message xid = sub-xact xid** demultiplexing
(§7), streaming block mechanics + first-segment flag (§8), and REPLICA IDENTITY DEFAULT-vs-FULL
old-image shapes (§6). The live-walsender abort is exercised by `03-capture.sh` scenario 8.

## Deferred (honest gaps — need infra beyond the current harness)

Each is real and worth testing eventually; none is currently a walrus blocker. Priority is for
when the sink is built.

| deferred case | why deferred | priority |
|---|---|---|
| `reconnect-restreams-uncommitted-txn`, `whole-txn-redelivery` | need a walsender kill/reconnect + crash harness | high (correctness on restart) |
| `partition-root-vs-leaf-routing` | needs a partitioned table + `publish_via_partition_root` variants | high (architecture depends on root routing) |
| `row-filter-transforms-update-op`, `column-list-truncates-relation` | need a non-`FOR ALL TABLES` publication (PG15+ filters/column lists) | medium |
| `drop-or-double-consume-active-slot-errors` | needs a concurrently-active walsender to make the slot `active` | medium |
| 2PC live paths (`v2-collapses-prepare-to-ordinary-commit`, `large-prepared-txn-streams`, `rollback-prepared-provisional-rows-discarded`) | 2PC decoding via SQL functions is finicky; **decoder-level** parsing is covered by golden vectors; walrus is v2-only | low |
| `parallel-v4-stream-abort-byte-length-misalign` | needs `streaming=parallel` (a real subscriber); the decoder **documents** the 9-byte-vs-longer risk | low (walrus uses `on`, not `parallel`) |
| `drop-middle-column-positional-shift`, `rename-column-indistinguishable-from-drop-add` | need before/after Relation diffing across a DDL; `add-column` is covered | medium |
| `timestamptz-text-form-depends-on-session-timezone`, `guc-dependent-text-rendering` (critic) | text-format values depend on the decoding session's `timezone`/`DateStyle`; **walrus decodes binary→Arrow**, sidestepping most of this | low |
| `small-nonstreamed-txn-commits-between-stream-blocks` (critic) | needs precise concurrent timing to force a small commit mid-stream | medium |
| type-system breadth (`composite/domain/range` Type messages, `json` vs `jsonb`, unconstrained `numeric` NaN/Inf) | partially covered by the architecture type table; specific wire captures not frozen | medium |

## Catalog artifact

The full 68-case catalog + 9 critic gaps (with per-case reproduce-SQL and expected observations)
was produced by the discovery workflow and is the source for the tables above. The must-priority,
highest-corruption-risk cases (savepoint aborts, same-commit truncate, replica-identity, abort
timing) are all in Layer 1/2 above.
