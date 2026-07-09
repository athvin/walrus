//! DEFERRED-GOAL SEAM — parallel CTID-range snapshotting (PR 4.11). **Inert today.**
//!
//! v1 backfills each published table with a **single** serial `COPY` under the exported snapshot
//! (`snapshot::SourceBackfill::copy_table`). The *nearest-term* deferred goal
//! ([architecture.md → Deferred design goals #3](../../../docs/architecture.md#deferred-design-goals-to-solve-later),
//! [§1.7 step 3 / Open Q9](../../../docs/architecture.md#17-snapshot--backfill-bootstrap)) is to
//! logically partition a large table into disjoint **CTID ranges** and run **multiple `COPY` streams
//! concurrently** — each worker opening its own `REPEATABLE READ` txn and
//! `SET TRANSACTION SNAPSHOT '<snapshot_name>'` so every range reads the ONE consistent MVCC snapshot
//! already exported at bootstrap. It needs **no** new slot, epoch, or ownership machinery — only
//! concurrent `COPY` under the existing snapshot — which is why it is the cheapest of the three
//! deferred goals to pick up first.
//!
//! This module is the marked seam, not an implementation: [`plan_ctid_ranges`] is where a future
//! contributor produces the per-table range plan that [`snapshot::SourceBackfill::copy_table`] would
//! then be fanned out over (each range still emits `snapshot` files at `lsn_end = consistent_point`,
//! disambiguated by `manifest_id`, so the watermark handoff is unchanged — a throughput optimisation
//! only). See `docs/deferred-goals.md`.

/// A per-table plan of disjoint CTID ranges to `COPY` concurrently under the exported snapshot.
/// **Deferred** — nothing constructs or consumes this in v1.
#[allow(dead_code)]
struct CtidRangePlan {
    /// Fully-qualified `schema.table`.
    table: String,
    /// Disjoint `[start_block, end_block)` CTID block ranges, one per concurrent `COPY` worker. A
    /// TID-range scan (`WHERE ctid >= '(lo,0)' AND ctid < '(hi,0)'`) serves each efficiently.
    ranges: Vec<(u64, u64)>,
}

/// **Deferred goal — not implemented.** Would size CTID ranges from table stats (relpages / bloat)
/// and worker count. v1 runs one serial `COPY` per table; see the module docs and
/// `docs/deferred-goals.md`.
#[allow(dead_code)]
fn plan_ctid_ranges(_table: &str) -> CtidRangePlan {
    unimplemented!("deferred goal — parallel CTID-range backfill; see docs/deferred-goals.md")
}
