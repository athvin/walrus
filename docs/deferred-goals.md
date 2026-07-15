# Deferred design goals — shapes and seams

These are **intended capabilities, deliberately deferred** — not permanent
[non-goals](./architecture.md#goals--non-goals) and not open
[unknowns/risks](./architecture.md#open-questions--risks). They are features walrus plans to own,
sequenced after v1. This note pins each to the **exact module/seam** a future contributor extends, so
"not yet" never reads as "never". Canonical list:
[architecture.md → Deferred design goals](./architecture.md#deferred-design-goals-to-solve-later).

The invariant that bounds all of this: **the sink is a single consumer of the one lifelong slot**
([§1.8](./architecture.md#18-single-slot-for-life--total-restart)) — horizontal scale is a **loader**
story only, and there is deliberately **no sink-sharding seam**.

## 1. Single-table reload / re-sync while streaming

**What.** Re-sync or reload **one** table — e.g. after a quarantined lossy `ALTER COLUMN TYPE`, or on
operator demand — **without a total-restart**, while the single lifelong slot keeps streaming for every
other table. Today the only re-sync is the whole-system
[total-restart](./architecture.md#18-single-slot-for-life--total-restart), which rebuilds *every* table
together; there is no per-table recovery path.

**Likely shape.** Copy the one table under a **fresh exported snapshot**, then reconcile it against the
live stream via a **per-table watermark handoff** ([§1.7](./architecture.md#17-snapshot--backfill-bootstrap)),
all without disturbing the slot or the other tables' loaders.

**Seam.** No code stub in this PR — the future path reuses the existing snapshot/backfill machinery
(`crates/pg-sink/src/snapshot.rs`) plus the per-table checkpoint watermarks
(`crates/control/src/checkpoint.rs`), scoped to one table instead of the whole epoch. It is listed
first by the design but is the **heaviest** of the three (it touches the snapshot↔stream boundary).

**Design note.** [single-table-reload.md](./single-table-reload.md) critiques an in-band
signal-table proposal for this goal and lands on a chunked, watermark-stamped shape (Debezium/DBLog
lineage) that needs no extra slots and no stream pause.

**Planned.** That design is now broken into 12 implementable PRs with Definitions of Done —
[implementation curriculum, Phase 6](./implementation/README.md#the-roadmap)
([task files](./implementation/phase-6-single-table-reload/)).

## 2. Multi-pod loader table-sharding (horizontal scale-out)

**What.** Spread tables across **multiple loader replicas**, each owning a disjoint set — consistent
hashing, one PVC per replica, exclusive file ownership. Today one active loader owns all `.duckdb`
files and scales **up** (CPU/memory, more per-table worker threads within the one pod).

**Likely shape.** A consistent-hash assignment of tables → replicas, with the **fencing token** guarding
against a stale owner's writes after a reshard (never a naive HPA — file ownership is exclusive).

**Seam.** [`crates/loader/src/ownership.rs`](../crates/loader/src/ownership.rs) — the inert
`TableAssignment` placeholder. The forward-compat hook already exists: the `fencing_token` minted in
PR 3.1 (`control::table_ownership`, bumped only when ownership changes hands) is acquired at bootstrap
and carried on every `crate::bootstrap::OwnedTable`, but is **unused for routing** today (dormant at
`replicas=1`). Sharding turns it into the fence; nothing else needs re-plumbing.

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

*v1 curriculum complete (phases 0–4) — these goals are the feature-work finish line
([docs/implementation/README.md](./implementation/README.md); phase 5 there is post-v1 hardening —
benchmarks, hot-path cleanup, CI speed — not new features; phase 6 plans goal §1 above as 12 PRs).
The seams above are marked, not implemented; each changes no v1 runtime behaviour.*
