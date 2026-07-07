# PR 4.5 — End-to-end WAL-runaway, idle heartbeat, and keepalive-vs-durability

> **Phase:** 4 — End-to-end, ops & resilience · **Crates touched:** `tests/e2e` · **Est. size:** L ·
> **Depends on:** PR 4.4 · **Unlocks:** PR 4.6

The slot-liveness chaos suite. Three tests, one per `architecture.md` verification bullet: (1)
**WAL-runaway** — pause the loader and keep writing; the slot grows only to the safety cap, alerts fire,
then resume and assert full catch-up with no loss/dupes; (2) **idle-publication heartbeat** — churn WAL on
an *unpublished* part of the DB while the published tables sit idle, and assert the sink fires a beat only
after `heartbeat_idle_after`, the beat's round-trip is observed, and `restart_lsn`/`confirmed_flush_lsn`
keep advancing so retained WAL stays healthy; (3) **keepalive-vs-durability** — stall the S3 flush past
`wal_sender_timeout` and assert keepalive feedback keeps the walsender **connected** while
`confirmed_flush_lsn` does **not** advance until durable.

## Why — learning objectives

By the end of this PR you will have practised:

- **The single-slot liveness contract, end-to-end** — the heartbeat and keepalive machinery from PR 2.27 /
  2.20, proven to keep the one lifelong slot healthy under idle and stall conditions.
- **The two distinct LSNs** — keepalive/written-flushed LSN (stay connected) vs `confirmed_flush_lsn`
  (advance the slot only when durable) — and asserting they diverge under a stall.
- **Backpressure as bounded WAL** — a paused consumer must cap slot growth (alert), not fill the source
  disk; then catch up cleanly.
- **Round-trip liveness** — a written `beat_seq` returning through the stream and surfacing on the health
  endpoint as *not-degraded*.

## Read first

- `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` — the **"WAL-runaway
  chaos"**, **"Idle-publication heartbeat"**, and **"Keepalive vs durability"** bullets.
- `../../architecture.md#19-slot-liveness--heartbeat--keepalive` — the idle-triggered beat, suppression
  under writes, round-trip=liveness, the two LSNs, and the safety-cap backstop.
- `../../walrus-pg-sink.md#43-probes--get-these-exactly-right` — why a stale round-trip surfaces as
  `degraded`, never a `livenessProbe` kill and never a hard readiness gate (a catching-up sink is stale by
  design).

## Scope

**In scope**

- `wal_runaway_is_bounded_then_catches_up`: pause the loader (or its polling), keep writing to source;
  assert slot retained bytes rise only to the configured cap and the retained-WAL alert condition trips;
  resume; assert full catch-up, mirror == source, no loss/dupes.
- `idle_heartbeat_advances_restart_lsn`: keep published tables idle while an unpublished table churns WAL;
  assert (a) a beat fires **only after** `heartbeat_idle_after`, (b) it is **suppressed** while user-table
  writes are active, (c) the `beat_seq` round-trip is observed on the health endpoint, and (d)
  `restart_lsn` / `confirmed_flush_lsn` advance so `wal_status` stays healthy.
- `stalled_flush_keeps_connection_without_advancing`: stall the S3 flush past `wal_sender_timeout`; assert
  no `terminating walsender` reconnect churn **and** `confirmed_flush_lsn` does not advance until the flush
  completes; also assert a catching-up sink with a stale round-trip is **not** gated out of readiness.

**Explicitly deferred** (do *not* build these here)

- The metric/alert *names* and dashboard JSON → **PR 4.10** (this PR asserts the *behaviour*, using
  whatever alert condition already exists).
- Slot **invalidation** (`wal_status='lost'`) → total-restart in **PR 4.6**; here the cap is a *backstop*
  we alert before, not a trigger we cross.

## Files to create / modify

```
tests/e2e/tests/wal_runaway.rs       # new — pause-loader, bounded slot, alert, catch-up
tests/e2e/tests/heartbeat.rs         # new — idle beat, suppression, round-trip, restart_lsn advance
tests/e2e/tests/keepalive.rs         # new — stalled flush stays connected, confirmed_flush holds
tests/e2e/src/lib.rs                 # modify — pause_loader(), stall_s3(), slot_status(), health_degraded()
# no new deps
```

## Skeleton

```rust
// tests/e2e/tests/wal_runaway.rs
#![cfg(feature = "it")]
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn wal_runaway_is_bounded_then_catches_up() {
    // Pause loader polling; drive a large sustained write workload.
    // Assert slot retained bytes rise toward the cap but stay <= cap and the retained-WAL alert trips.
    // Resume loader; assert full catch-up: mirror == source, no loss/dupes.
    todo!()
}
```

```rust
// tests/e2e/tests/heartbeat.rs
#![cfg(feature = "it")]
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn idle_heartbeat_advances_restart_lsn() {
    // Churn WAL on an UNPUBLISHED table; keep published tables idle.
    // Assert: no beat before heartbeat_idle_after; exactly one beat after; beat_seq round-trip observed
    //         on /ready (not degraded); restart_lsn and confirmed_flush_lsn advance.
    // Then: write to a published table; assert the beat is SUPPRESSED.
    todo!()
}
```

```rust
// tests/e2e/tests/keepalive.rs
#![cfg(feature = "it")]
#[tokio::test]
#[ignore = "requires docker compose up --wait"]
async fn stalled_flush_keeps_connection_without_advancing() {
    // Inject an S3 flush stall > wal_sender_timeout (60s).
    // Assert: walsender stays connected (no reconnect churn) AND confirmed_flush_lsn does not advance;
    //         a catching-up sink with a stale round-trip is NOT gated out of readiness.
    todo!()
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] With the loader paused, slot retained bytes stay ≤ the configured cap and the retained-WAL alert
      condition trips; resuming yields full catch-up (mirror == source, no loss/dupes).
- [ ] An idle published set fires a beat **only after** `heartbeat_idle_after`, the beat is **suppressed**
      under active user-table writes, the `beat_seq` round-trip surfaces on the health endpoint, and
      `restart_lsn` / `confirmed_flush_lsn` advance (retained WAL / `wal_status` stay healthy).
- [ ] A flush stalled past `wal_sender_timeout` keeps the walsender **connected** (no `terminating
      walsender` churn) while `confirmed_flush_lsn` does **not** advance until durable; a catching-up sink
      is **not** gated out of readiness by a stale round-trip.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test --workspace`
  - [ ] `docker compose up --wait` then `cargo test -p e2e --features it -- --ignored` asserting
        **`wal_runaway_is_bounded_then_catches_up`**, **`idle_heartbeat_advances_restart_lsn`**, and
        **`stalled_flush_keeps_connection_without_advancing`**.

## Hints & gotchas

- The heartbeat only advances the slot if `walrus.heartbeat` is in the publication — a beat on an
  *unpublished* table is filtered by `pgoutput` and does nothing. The test's whole point is that an
  **unpublished** table churns WAL (stuck `restart_lsn`) while the **published** beat rescues it.
- Set `heartbeat_idle_after` low (a few seconds) in the test config so the idle window is observable
  without a slow test. Keep it comfortably under `wal_sender_timeout`.
- To exercise keepalive-vs-durability you must stall **the flush**, not the whole process — inject a delay
  in the S3 PUT path (a test-only env knob) so the loop still sends keepalive feedback while durability
  waits. If you stall the whole loop, you'll instead test the timeout severing the connection.
- Do not tie readiness to slot lag or round-trip staleness — a catching-up sink is stale *by design*.
  Assert `/ready` reports `degraded` as a field but does **not** flip to not-ready.
- Alert on retained WAL **well before** the cap — crossing the cap converts bloat into slot-loss →
  total-restart, which is PR 4.6's disaster path, not this one.

## References

- Design: `../../architecture.md#verification-how-well-prove-it-works-end-to-end-later` ("WAL-runaway
  chaos", "Idle-publication heartbeat", "Keepalive vs durability");
  `../../architecture.md#19-slot-liveness--heartbeat--keepalive`;
  `../../walrus-pg-sink.md#43-probes--get-these-exactly-right`.
- Prev: [PR 4.4](./pr-4.4-e2e-crash-safety.md) · Next: [PR 4.6](./pr-4.6-total-restart-epoch.md) ·
  [Roadmap](../README.md)
