# PR 2.27 — Fire an idle heartbeat and track round-trip liveness

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/47

> **Phase:** 2 — walrus-pg-sink · **Crates touched:** `pg-sink`, `common` · **Est. size:** M ·
> **Depends on:** PR 2.26 · **Unlocks:** PR 2.28

An idle *published* set of tables cannot advance `confirmed_flush_lsn`, so `restart_lsn` freezes and
retained WAL grows without bound until the source disk fills or the slot is invalidated — a healthy
sink can still detonate the whole system. This PR adds the mitigation: a **sink-driven,
idle-triggered heartbeat** that writes one row to the published `walrus.heartbeat` table over a
*separate* SQL connection, and treats that beat's **return through the replication stream** as a
round-trip liveness signal surfaced on the readiness/health endpoint (never a liveness kill).

## Why — learning objectives

By the end of this PR you will have practised:

- **Two connections, one source** — driving an *ordinary* `tokio_postgres` SQL connection alongside
  the read-only replication connection, and why the heartbeat must ride the *published* table.
- **Idle detection with a monotonic clock** — `tokio::time::Instant`, `last_activity` vs `last_beat`,
  and suppression under steady write traffic.
- **Round-trip liveness** — stamping a monotonic `beat_seq`, recognising the beat's own relation OID
  when it decodes back, and distinguishing "catching up" (expected staleness) from "wedged".
- **Health semantics that don't self-harm** — a `degraded` field, *never* a `livenessProbe` kill and
  *never* a hard readiness gate.

## Read first

- `../../architecture.md#19-slot-liveness--heartbeat--keepalive` — the heartbeat/keepalive contract:
  fire-only-when-idle, the exact `UPDATE`, `beat_seq` round-trip, the `degraded` field, and why
  `max_slot_wal_keep_size` is a backstop, not a mitigation.
- `../../walrus-pg-sink.md#44-steady-state` and `#43-probes--get-these-exactly-right` — the sink
  itself writes the beat (not a CronJob), and stale round-trip during catch-up feeds *readiness*
  health + an alert, never a liveness kill.
- `../../proto-version.md#7-the-per-message-xid-v2-why-it-exists` and
  `#4-the-message-catalog-decoded-byte-by-byte` — how the beat's `walrus.heartbeat` change decodes.

## Scope

**In scope**

- A `Heartbeat` owning a *separate ordinary SQL connection* to the source; `maybe_beat` fires exactly
  one `UPDATE walrus.heartbeat SET beat_seq = beat_seq + 1, ts = now(), sink_instance = $me WHERE id = 1`
  only when idle on **both** `last_activity` and `last_beat`.
- Recognising the `walrus.heartbeat` relation OID in the decode loop → record the round-trip,
  advance the durable checkpoint's LSN, and **never** stage the beat to S3 / mirror it.
- A `degraded(now)` predicate (`now − last_successful_roundtrip > heartbeat_roundtrip_deadline`) wired
  into the `/ready` health JSON as a non-gating `degraded` field.
- Periodic standby status update advancing the confirmed LSN to the point the beat established.

**Explicitly deferred** (do *not* build these here)

- The unconditional sub-`wal_sender_timeout` keepalive feedback → already landed in **PR 2.20**; this
  PR only adds the *heartbeat* and its round-trip, both distinct from keepalive.
- The Prometheus alert/metric for `beat_seq` gap + round-trip age → **PR 4.10**.
- Chaos proof (WAL-runaway + heartbeat releasing `restart_lsn`) → **PR 4.5**.

## Files to create / modify

```
crates/pg-sink/src/heartbeat.rs      # new — Heartbeat, HeartbeatConfig, InternalTables
crates/pg-sink/src/health.rs         # modify — add `degraded` field to /ready JSON
crates/pg-sink/src/sink.rs           # modify — call maybe_beat on idle; route heartbeat OID
crates/pg-sink/src/config.rs         # modify — heartbeat_idle_after, heartbeat_roundtrip_deadline
crates/pg-sink/src/lib.rs            # modify — `pub mod heartbeat;`
crates/pg-sink/tests/heartbeat_liveness.rs   # new — compose integration test
# no new Cargo deps (tokio-postgres already present from PR 2.19/2.20)
```

## Skeleton

```rust
// crates/pg-sink/src/heartbeat.rs
use std::time::Duration;
use tokio::time::Instant;

/// Bounds-validated in `config.rs`: both must be > 0 and idle_after < roundtrip_deadline.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub idle_after: Duration,
    pub roundtrip_deadline: Duration,
}

/// OIDs of walrus's own published tables — consumed for control, never materialized.
#[derive(Debug, Default, Clone)]
pub struct InternalTables {
    pub heartbeat_oid: Option<u32>,
    // pub ddl_audit_oid: Option<u32>,   // filled in PR 2.33
}

impl InternalTables {
    pub fn is_internal(&self, rel_oid: u32) -> bool { todo!() }
}

/// Owns a *separate* ordinary SQL connection to the source (distinct from replication).
pub struct Heartbeat {
    sql: tokio_postgres::Client,
    instance: String,
    cfg: HeartbeatConfig,
    last_beat: Option<Instant>,
    pending_seq: Option<i64>,          // seq written, awaiting its return through the stream
    last_roundtrip: Option<Instant>,
}

impl Heartbeat {
    pub async fn connect(dsn: &str, instance: String, cfg: HeartbeatConfig) -> Result<Self, crate::Error> { todo!() }

    /// Fire exactly one beat iff idle on BOTH clocks. Returns the seq written, or None (suppressed).
    pub async fn maybe_beat(&mut self, now: Instant, last_activity: Instant) -> Result<Option<i64>, crate::Error> { todo!() }

    /// The heartbeat relation decoded back through the stream: record the round-trip.
    pub fn observe_return(&mut self, beat_seq: i64, now: Instant) { todo!() }

    /// Non-gating health signal: `now − last_successful_roundtrip > roundtrip_deadline`.
    pub fn degraded(&self, now: Instant) -> bool { todo!() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn beat_suppressed_when_active_within_idle_after() { todo!() }
    #[test] fn beat_fires_exactly_once_after_idle_threshold() { todo!() }
    #[test] fn roundtrip_recorded_when_returned_seq_matches_pending() { todo!() }
    #[test] fn degraded_true_only_after_deadline_without_roundtrip() { todo!() }
    #[test] fn internal_heartbeat_oid_is_recognised() { todo!() }
}
```

```rust
// crates/pg-sink/tests/heartbeat_liveness.rs
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn idle_publication_beats_and_advances_confirmed_flush() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] With the published user tables **idle**, a beat fires only **after** `heartbeat_idle_after`, its
      `beat_seq` **returns** through the stream, and `confirmed_flush_lsn` advances as a result.
- [x] Under active user-table writes the beat is **suppressed** (no `walrus.heartbeat` UPDATE issued).
- [x] The `walrus.heartbeat` change is **never** staged to S3 nor written to a manifest row.
- [x] A stale round-trip during a legitimate catch-up sets `/ready`'s `degraded` field **without**
      taking the pod out of readiness (no hard gate) and **without** any liveness kill.
- [x] Docs/comments explain: separate SQL connection, why the table must be in `walrus_pub`, and the
      keepalive-vs-heartbeat-vs-durability three-LSN distinction.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test heartbeat_liveness -- --ignored`
        asserting **`idle_publication_beats_and_advances_confirmed_flush`**.

## Hints & gotchas

- The beat must ride the **published** `walrus.heartbeat` table or `pgoutput` filters it out and you
  get no round-trip — verify it is in `walrus_pub` at preflight.
- Use a **monotonic `Instant`**, not wall-clock, for the idle windows; the `ts` column is UTC `now()`
  purely for lineage.
- `beat_seq` is monotonic — treat "returned seq **≥** pending" as a round-trip so a coalesced/late
  beat still counts; never require exact equality lock-step.
- Resist the temptation to gate readiness on the round-trip: a catching-up sink is stale *by design*
  (same reason liveness is never slot-lag). `degraded` is observ­ability only.
- Keep this separate from PR 2.20's keepalive: three LSNs — keepalive (unconditional), the beat's
  established point, and `confirmed_flush_lsn` (only after durability) — must stay distinct.

## References

- Design: `../../architecture.md#19-slot-liveness--heartbeat--keepalive`;
  `../../walrus-pg-sink.md#44-steady-state`, `#43-probes--get-these-exactly-right`;
  `../../proto-version.md#4-the-message-catalog-decoded-byte-by-byte`.
- Prev: [PR 2.26](./pr-2.26-sink-durability-checkpoint.md) ·
  Next: [PR 2.28](./pr-2.28-sink-graceful-shutdown.md) · [Roadmap](../README.md)
