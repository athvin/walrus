# PR 6.4 — the reload controller: pickup, preflight, lease, concurrency cap

> **Status:** 📋 Planned

> **Phase:** 6 — single-table reload · **Crates touched:** `pg-sink`, `control`, `justfile` ·
> **Est. size:** M · **Depends on:** PR 6.1, PR 6.3 · **Unlocks:** PR 6.5

The export must be a **sink-owned side task that the replication loop never waits for**
([reload H6](../../single-table-reload.md#h6--who-runs-the-export-and-the-stream-must-never-wait-for-it))
— if the stream stalls for one table's export, the single slot lags for *every* table, which is
the exact failure the design exists to avoid. This PR builds the controller: a spawned task that
polls `table_reload` on the heartbeat cadence, preflights each request (fail fast at request time,
not mid-export — H11), takes the lease, flips `requested → exporting`, and schedules per-table
exporters under a `max_concurrent_reloads` cap so "reload N tables" is a queue drained politely,
not N simultaneous load spikes. The exporter itself is a stub here — the chunk engine is PR 6.5.

## Why — learning objectives

By the end of this PR you will have practised:

- **Side-task ownership** — `tokio::spawn` + its own connections, the bootstrap-backfill shape:
  the replication loop and the controller share state but never block each other.
- **Semaphore-bounded fan-out** — `tokio::sync::Semaphore` as the `max_concurrent_reloads` cap;
  permits held for the exporter's lifetime, queued requests waiting their turn.
- **Lease-based liveness** — acquire + periodic renew so a died-mid-export sink is detectable
  (H7's stuck-state story), with the fencing caveat for the future multi-pod world.
- **Fail-fast request validation** — querying `pg_publication_tables` and the PK catalog before
  spending a single chunk on a doomed reload.

## Read first

- `../../single-table-reload.md` — H6 (side task + cap), H7 (lease), H11 (preflight list), §5
  steps 1–2 (request → pickup).
- `crates/pg-sink/src/snapshot.rs` — the backfill side-task shape (own SQL connection, spawned,
  stream keeps flowing) this controller mirrors.
- `crates/pg-sink/src/config.rs` — `SinkConfig::load()` + `validate()`; `max_inflight_bytes` is
  the model for a bounds-checked tunable.
- `crates/control/src/reload.rs` (PR 6.1) — `claim_requested`, `renew_lease`, `fail`.

## Scope

**In scope**

- `ReloadController::spawn` at sink startup: each tick, claim up to *(free permits)* `requested`
  rows via `control::reload::claim_requested`, preflight each, spawn an exporter per survivor.
- Preflight per request: target table is in the publication; target has a PK; flavor is
  implementable (**`resync` is rejected as unimplemented until PR 6.10** — typed reason). Any
  failure ⇒ `control::reload::fail(reload_id, reason)` — the operator sees why in the row.
- Lease renewal on a timer for as long as the exporter runs; renewal failure (lost lease) cancels
  the exporter.
- Config: `max_concurrent_reloads` (default 2, ≥ 1) and `reload_lease_ttl_secs` (default 60,
  bounds-checked against the renewal interval).
- `just reload table='public.orders' flavor='reload'` — INSERTs the request row into control-pg;
  the operator entry point (and the e2e tests' lever).

**Explicitly deferred** (do *not* build these here)

- The real chunk loop → **PR 6.5** (`export_table` is `todo!()`-shaped here, parked behind the
  semaphore so scheduling is testable).
- Crash-recovery startup scan (resume `exporting` rows from the cursor) → **PR 6.9**.
- Restart-on-DDL → **PR 6.8**. The `resync` flavor → **PR 6.10**.

## Files to create / modify

```
crates/pg-sink/src/reload.rs           # new — ReloadController, preflight, scheduling
crates/pg-sink/src/lib.rs              # modify — pub mod reload;
crates/pg-sink/src/main.rs             # modify — spawn the controller after bootstrap
crates/pg-sink/src/config.rs           # modify — max_concurrent_reloads, reload_lease_ttl_secs
justfile                               # modify — the `reload` recipe (psql INSERT)
crates/pg-sink/tests/reload_pickup.rs  # new — compose: pickup, preflight-fail, cap
```

## Skeleton

```rust
// crates/pg-sink/src/reload.rs

/// Sink-owned reload orchestration (H6). Never on the replication loop's path: own control-pg
/// pool, own source SQL connections, communicates with the consume loop only via WatermarkWaiters.
pub struct ReloadController {
    /* control pool · source conn factory · Arc<WatermarkWaiters> · SinkConfig slice ·
       Semaphore(max_concurrent_reloads) · shutdown token */
}

impl ReloadController {
    /// Spawn at startup, next to the heartbeat task. Polls on the heartbeat cadence.
    pub fn spawn(/* … */) -> tokio::task::JoinHandle<()> { todo!() }

    /// One tick: claim ≤ free-permit `requested` rows, preflight, spawn exporters.
    async fn tick(&self) -> Result<(), crate::Error> { todo!() }

    /// H11, fail-fast: target in publication, target has a PK, flavor supported (resync → 6.10).
    /// Rejection reason lands in table_reload.error via control::reload::fail.
    async fn preflight(&self, req: &control::ReloadRow) -> Result<(), PreflightRejection> { todo!() }

    /// Holds a semaphore permit + renews the lease for the exporter's lifetime; lost lease ⇒ cancel.
    async fn run_exporter(&self, req: control::ReloadRow) { todo!() }
}

#[derive(Debug, thiserror::Error)]
pub enum PreflightRejection {
    #[error("table {0}.{1} is not in the publication")] NotPublished(String, String),
    #[error("table {0}.{1} has no primary key")] NoPrimaryKey(String, String),
    #[error("flavor 'resync' lands in PR 6.10")] ResyncNotYetImplemented,
}

/// PR 6.5 replaces this stub with the chunk engine. Here it parks until cancelled so the
/// semaphore's scheduling is observable in tests.
async fn export_table(/* … */) -> Result<(), crate::Error> { todo!() }

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn cap_of_two_schedules_third_request_only_after_a_permit_frees() { todo!() }
    #[test] fn preflight_rejects_unpublished_and_pk_less_tables_with_reasons() { todo!() }
    #[test] fn lost_lease_cancels_the_exporter() { todo!() }
}
```

```just
# justfile (shape)
# Request a single-table reload (flavor: reload | resync). The operator entry point (reload §5.1).
reload table flavor='reload':
    psql $CONTROL_URL -c "INSERT INTO walrus.table_reload …"
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `just reload table='public.orders'` flips the row `requested → exporting` within one
      heartbeat cadence, with `lease_holder`/`lease_expiry` set and `lease_expiry` observably
      advancing while the (stub) exporter runs.
- [ ] A request for an unpublished table and one for a PK-less table both land in `failed` with
      the specific reason in `error` — before any signal row or chunk is attempted.
- [ ] A `resync` request fails with the not-yet-implemented reason (lifted in PR 6.10).
- [ ] With `max_concurrent_reloads=2` and three requests, at most two rows are ever `exporting`
      simultaneously; the third starts when a permit frees (unit-tested with the parking stub).
- [ ] The replication stream never pauses while the controller works — an unrelated table's
      changes keep flowing during pickup (compose-asserted).
- [ ] Config bounds are validated: `max_concurrent_reloads ≥ 1`; lease TTL sanely exceeds the
      renewal interval; violations are terminal at startup.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test reload_pickup -- --ignored`

## What completed looks like

```
$ docker compose up --wait
$ just reload table='public.orders'
$ psql $CONTROL_URL -c "SELECT reload_id, source_table, status, lease_holder,
                               lease_expiry > now() AS lease_live
                        FROM walrus.table_reload ORDER BY reload_id DESC LIMIT 1"
 reload_id | source_table |  status   | lease_holder  | lease_live
-----------+--------------+-----------+---------------+------------
         7 | orders       | exporting | walrus-sink-0 | t

$ just reload table='public.no_pk_table'
$ psql $CONTROL_URL -c "SELECT status, error FROM walrus.table_reload ORDER BY reload_id DESC LIMIT 1"
 status |                    error
--------+---------------------------------------------
 failed | table public.no_pk_table has no primary key
```

## Hints & gotchas

- Publication membership: query `pg_publication_tables WHERE pubname = … AND schemaname = … AND
  tablename = …` on the side connection — the same catalog preflight already reads.
- Hold the `OwnedSemaphorePermit` inside the spawned exporter task, not in `tick` — dropping it on
  task exit is what frees the slot.
- The lease is liveness *and* the future fence: when loader sharding (deferred goal §2) arrives,
  `lease_holder` + the fencing-token pattern from `table_ownership` is how a stale sink is kept
  from double-exporting. Note it in a comment; do not build the fence.
- On graceful SIGTERM (PR 2.28's drain), cancel exporters *without* failing their rows — a
  `requested`/`exporting` row with an expired lease is exactly what PR 6.9's startup scan resumes.
- Pod identity for `lease_holder`: reuse whatever names the sink in the ownership/heartbeat
  machinery (hostname in the StatefulSet) — don't invent a second identity.

## References

- Design: `../../single-table-reload.md` H6, H7, H11, §5 steps 1–2;
  `../../architecture.md#17-snapshot--backfill-bootstrap` (side-task precedent);
  `../../deferred-goals.md#2-multi-pod-loader-table-sharding-horizontal-scale-out` (the fence note).
- Prev: [PR 6.3](./pr-6.3-sink-echo-routing-watermark.md) ·
  Next: [PR 6.5](./pr-6.5-sink-chunk-export-engine.md) · [Roadmap](../README.md)
