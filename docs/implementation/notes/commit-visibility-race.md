# The commit-visibility race under echo-wait watermarking (PR 6.3)

> **Status:** decided 2026-07-15 — **accept the race as practically closed by the echo round-trip;
> keep the embedded-LSN cross-check as the tripwire.** No second signal round-trip, no
> `pg_current_snapshot()` read; revisit triggers below.

## The assumption the reload algebra stands on

> A chunk `SELECT` issued **after** the exporter observes the chunk's watermark `L_i` in-stream
> (the echo, [reload H1](../../single-table-reload.md#h1--the-export-has-no-consistent-point-the-critical-one))
> sees every transaction with commit LSN ≤ `L_i`.

The Phase-B dedup (`(commit_lsn, lsn)` max wins, loader §5.2) makes chunk/stream overlap safe in
exactly one direction:

- **Over-inclusion is free.** If the chunk read sees a transaction `V` with commit LSN `C > L_i`,
  the chunk carries `V`'s effect stamped `L_i` while the stream later delivers `V` itself at `C`.
  `C > L_i` ⇒ the stream event wins the per-PK dedup ⇒ convergence. Nothing to defend.
- **Under-inclusion is the loss.** If a transaction `T` with commit LSN `C < L_i` is *not visible*
  to the chunk read, its stream event (at `C`) **loses** the dedup to the chunk's stale row
  (stamped `L_i ≥ C`) — a silently lost update. This is the only direction that matters.

## Why the round-trip practically closes it

Postgres commits in this order: write the commit record into WAL → (under `synchronous_commit=on`)
wait for the WAL flush → mark the transaction visible in the proc-array. So there is a real window
in which `T`'s commit record exists in WAL — and can be decoded and shipped — while `T` is not yet
MVCC-visible. That window is the race.

But look at what has to happen between `T`'s commit record and the chunk read: the *signal*
transaction's commit record must be written **after** `T`'s (that is what `C < L_i` means), then
flushed, decoded by the walsender, shipped over the network, parsed by the sink, matched to a
waiter, and the exporter must wake and issue its `SELECT` over a separate SQL connection. That
round-trip is milliseconds of network + scheduling; `T`'s WAL-flush→proc-array-visible gap is
microseconds inside a single backend that is actively running the code path. The race is not
*provably* impossible — it is bounded by "an entire replication round-trip outraces one
already-running function call", which is the same practical bound DBLog and Debezium's
incremental snapshots ship on.

`synchronous_commit=off` **helps** rather than hurts: the commit returns and becomes visible
*before* its WAL record is flushed, so visibility *leads* the stream. That widens over-inclusion
(free, above) and narrows under-inclusion to nothing.

## The tripwire

Every echo asserts `embedded wal_insert_lsn < commit LSN`
(`walrus_reload_crosscheck_violations_total`, error-logged, never fatal). The embedded LSN is not
the watermark — it is the canary for exactly this class of model error: any violation means WAL
positions are not ordering the way the argument above requires, and reloads should stop while a
human looks.

## Revisit triggers

Escalate to a stronger mechanism if **any** of:

- `walrus_reload_crosscheck_violations_total` ever ticks;
- the phase-closing convergence e2e (PR 6.12) ever shows a mirror≠source diff attributable to a
  chunk boundary;
- walrus targets a source where the read replica serves the chunk SELECT (replica visibility lags
  arbitrarily — the practical bound evaporates).

Two known escalations, in preference order:

1. **Second echo round-trip (DBLog's lo/hi window):** signal → read chunk → signal again; only
   events between the two commit LSNs need dedup arbitration. Roughly doubles per-chunk latency;
   changes no storage shape.
2. **Read-only watermarking via `pg_current_snapshot()`** (Debezium ≥ 2.4's read-only incremental
   snapshots): replaces the signal-table write with a snapshot read + xid arithmetic against
   decoded `xid`s. Removes the write dependency entirely, at the cost of tracking xid↔LSN
   correspondence in the sink. The right shape if walrus ever needs reloads against a source it
   cannot write to.
