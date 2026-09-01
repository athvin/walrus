# Deferred design goals — shapes and seams

These are **intended capabilities, deliberately deferred**, plus completed capabilities retained
for context — not permanent
[non-goals](./architecture.md#goals--non-goals) and not open
[unknowns/risks](./architecture.md#open-questions--risks). They are features walrus plans to own,
sequenced after v1. This note pins each to the **exact module/seam** a future contributor extends, so
"not yet" never reads as "never". Canonical list:
[architecture.md → Deferred design goals](./architecture.md#deferred-design-goals-to-solve-later).

The invariant that bounds all of this: **the sink is a single consumer of the one lifelong slot**
([§1.8](./architecture.md#18-single-slot-for-life--total-restart)) — horizontal scale is a **loader**
story only, and there is deliberately **no sink-sharding seam**.

## 1. Single-table reload / re-sync while streaming (completed)

**What.** Re-sync or reload **one** table — e.g. after a quarantined lossy `ALTER COLUMN TYPE`, or on
operator demand — **without a total-restart**, while the single lifelong slot keeps streaming for
every other table.

**Implemented shape.** The sink exports primary-key-ordered chunks with echo-derived LSN watermarks;
the loader either rebuilds the table (`reload`) or merges chunks over the live mirror (`resync`).
Restart-on-DDL, lease adoption, cursor-based crash recovery, and reload metrics preserve progress
without disturbing the slot or other tables.

**Implementation.** The state machine lives in `control::reload`, export in
`pg_sink::reload_export`, orchestration in `pg_sink::reload`, and loader routing/rebuild in
`loader::phase_a`. The operational interface and invariants are documented in
[single-table-reload.md](./single-table-reload.md).

**Design note.** [single-table-reload.md](./single-table-reload.md) critiques an in-band
signal-table proposal for this goal and lands on a chunked, watermark-stamped shape (Debezium/DBLog
lineage) that needs no extra slots and no stream pause.

The anchor use case — a lossy-`ALTER` quarantine recovering via `just reload` while every other
table streams on — is covered by `tests/e2e/tests/reload_quarantine.rs`.

## 2. Multi-pod loader table-sharding (completed with DuckLake)

**What.** Spread tables across **multiple loader replicas**, each owning a disjoint set — consistent
hashing and exclusive per-table writer ownership, while all replicas share one DuckLake catalog and
object-data root.

**Implemented shape.** Rendezvous hashing maps `(epoch, schema, table)` to a stable StatefulSet
ordinal. The existing control-plane lease is the first fence. A dedicated PostgreSQL catalog session
holds one table-keyed advisory lock per assigned table as the second fence; losing that session
cancels every worker before a successor can write. Scale changes are deliberate StatefulSet rollouts,
not a naive HPA.

**Implementation.** Routing and the DuckLake namespace live in `loader::duck`; ordered lease +
catalog-lock acquisition lives in `loader::bootstrap`; catalog-session failure is wired into
`loader::supervisor`. The safe rollout and reshard sequence is documented in
[ducklake-migration.md](./ducklake-migration.md#horizontal-writer-rollout).

## 3. Faster initial export / backfill — parallel CTID-range snapshot (nearest-term)

**What.** Logically partition a large table into disjoint **CTID ranges** and run **multiple `COPY`
streams concurrently** under the single already-exported snapshot, cutting first-time onboarding of a
big database from hours to minutes (cf. PeerDB's ~5× `pg_dump`/`pg_restore` technique).

**Likely shape.** Each worker opens its own `REPEATABLE READ` txn and
`SET TRANSACTION SNAPSHOT '<snapshot_name>'`, so all ranges read the ONE consistent MVCC snapshot; TID-
range scans (`WHERE ctid >= '(lo,0)' AND ctid < '(hi,0)'`) with a server-side cursor per range bound
memory. Every range still emits `snapshot` files at `lsn_end = consistent_point`, disambiguated by
`manifest_id` — a throughput optimisation only, unchanged watermark handoff
([§1.7 step 3](./architecture.md#17-snapshot--backfill-bootstrap), Open Q9).

**Seam.** [`crates/pg-sink/src/backfill.rs`](../crates/pg-sink/src/backfill.rs) — the inert
`CtidRangePlan` / `plan_ctid_ranges` extension point; the fan-out wraps
`snapshot::SourceBackfill::copy_table`. This is the **nearest-term** goal: it needs **no** new slot,
epoch, or ownership machinery — only concurrent `COPY` under the snapshot already exported at bootstrap
— so a future contributor should pick this cheapest win first.

---

*Single-table reload (§1) and DuckLake writer sharding (§2) are implemented. Parallel backfill (§3)
remains deliberately inert and changes no current runtime behavior.*
