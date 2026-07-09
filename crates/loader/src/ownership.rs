//! DEFERRED-GOAL SEAM — multi-pod loader table-sharding (PR 4.11). **Inert today.**
//!
//! v1 runs **one active loader** (`StatefulSet replicas=1`) that owns **all** `.duckdb` files and runs
//! one worker thread per table. Horizontal scale-out — **multiple loader replicas each owning a
//! disjoint set of tables** (consistent hashing, PVC per replica, exclusive file ownership guarded by
//! a fencing token) — is a
//! [deferred design goal (#2)](../../../docs/architecture.md#deferred-design-goals-to-solve-later).
//! The **sink** stays a single consumer of the one lifelong slot regardless
//! ([§1.8](../../../docs/architecture.md#18-single-slot-for-life--total-restart)); horizontal scale is
//! a loader-only story. See `docs/deferred-goals.md`.
//!
//! **The forward-compat hook already exists.** The `fencing_token` minted in PR 3.1
//! (`control::table_ownership`, bumped only when ownership changes hands) is acquired at bootstrap and
//! carried on every owned table (`crate::bootstrap::OwnedTable::fencing_token`), but is **unused for
//! routing** today — dormant at `replicas=1`. When sharding lands it becomes the guard that fences a
//! stale owner's writes after a reshard. This module marks where the consistent-hash ownership split
//! will slot in; [`TableAssignment`] is that placeholder.

/// **Deferred** placeholder for a table's shard assignment across loader replicas. Nothing constructs
/// this in v1 — one loader owns every table, so `owner_replica` is always `None`.
#[allow(dead_code)]
struct TableAssignment {
    /// Fully-qualified `schema.table`.
    table: String,
    /// The replica that owns this table once sharding lands. **Always `None`** today (single active
    /// loader owns all tables).
    owner_replica: Option<String>,
    /// The inert forward-compat hook from PR 3.1 (`control::table_ownership::Lease::fencing_token`,
    /// carried on `OwnedTable`). Bumps on ownership change; unused for routing until sharding lands.
    fencing_token: i64,
}
