# PR 2.20 — `START_REPLICATION` + standby keepalive feedback (the de-risking spike)

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/40

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib), `common` ·
> **Est. size:** L · **Depends on:** PR 2.19 · **Unlocks:** PR 2.21

The **spike**. Verify the slot, read its resume position, issue
`START_REPLICATION SLOT walrus_slot LOGICAL <lsn> (proto_version '2', streaming 'on', publication_names …)`,
and drive the raw CopyBoth byte stream: parse the `'w'` (XLogData) and `'k'` (primary keepalive)
message headers, and reply with **standby status updates** on a sub-`wal_sender_timeout` interval so the
walsender never drops us. **No pgoutput decoding yet** — this PR proves the transport plumbing end-to-end
and is the explicit **pivot point**: if `tokio-postgres`'s replication surface is too thin, switch to the
`pgwire-replication` crate here, before any decode logic depends on it.

## Why — learning objectives

By the end of this PR you will have practised:

- **The CopyBoth replication sub-protocol** — the `'w'` XLogData header (start/end/clock) and the `'k'`
  keepalive header (walEnd/clock/**replyRequested**), byte-for-byte with `bytes` + `byteorder`.
- **Standby status updates** — the `'r'` reply carrying `write`/`flush`/`apply` LSNs + a timestamp, and
  why keepalive feedback is **unconditional** and *separate* from the durability checkpoint (§1.9).
- **Two distinct LSNs** — the keepalive/received LSN (sent to stay connected) vs `confirmed_flush_lsn`
  (advanced only after durability, PR 2.26). This PR only sends the *keepalive* one.
- **De-risking a dependency** — a spike whose success criterion is "the byte plumbing works," with an
  explicit fallback (`pgwire-replication`) chosen here so nothing downstream re-litigates it.

## Read first

- `../../architecture.md` §1.2 Replication consumer — hand-rolled (`#12-replication-consumer--hand-rolled`) —
  why we own the connection + slot feedback and don't adopt a framework.
- `../../architecture.md` §1.9 (`#19-slot-liveness--heartbeat--keepalive`), the **"Keepalive feedback is
  unconditional"** bullet — the two-LSN model and the `wal_sender_timeout` (60s) severance behaviour.
- `../../architecture.md` "Startup & bootstrap" `walrus-pg-sink` **step 4** — verify slot exists; read
  `restart_lsn` / `confirmed_flush_lsn` to establish the resume LSN.
- `../../proto-version.md` §7 and `../../examples/proto-version/run-tests.sh` — the live-wire harness this
  spike mirrors (a source write yields an XLogData frame).

## Scope

**In scope**

- Verify the slot exists (`pg_replication_slots`); read `restart_lsn` + `confirmed_flush_lsn`; if absent,
  `CREATE_REPLICATION_SLOT walrus_slot LOGICAL pgoutput (SNAPSHOT 'export')` capturing
  `consistent_point` + `snapshot_name` (kept for PR 2.29; not consumed here).
- Issue `START_REPLICATION` at the resume LSN with `proto_version '2'`, `streaming 'on'`,
  `publication_names`.
- Read the CopyBoth stream: parse `'w'` / `'k'` headers; **hand the XLogData payload off as an opaque
  `Bytes`** (decode is PR 2.21).
- Send standby status updates: unconditionally on a `< wal_sender_timeout` interval **and** immediately
  when a `'k'` frame sets `replyRequested = 1`. Only the **keepalive/received** LSN moves here.

**Explicitly deferred** (do *not* build these here)

- pgoutput decode of the XLogData payload → **PR 2.21**.
- Advancing `confirmed_flush_lsn` on durability → **PR 2.26**.
- Consuming the exported snapshot for backfill → **PR 2.29**.
- Idle heartbeat / round-trip liveness → **PR 2.27**.

## Files to create / modify

```
crates/pg-sink/Cargo.toml            # + bytes = "1", byteorder = "1"
crates/pg-sink/src/replication.rs    # new — ReplicationStream + StandbyStatus + wire headers
crates/pg-sink/src/slot.rs           # new — verify/create slot, read resume LSN
crates/pg-sink/tests/replication_spike.rs   # new — compose: a write yields XLogData; survives past wal_sender_timeout
```

## Skeleton

```rust
// crates/pg-sink/src/slot.rs
use common::Lsn;

pub struct SlotInfo { pub restart_lsn: Lsn, pub confirmed_flush_lsn: Lsn }

/// Resume position = confirmed_flush_lsn (server clamps to >= its own value anyway).
pub async fn verify_or_create_slot(
    client: &tokio_postgres::Client,
    slot: &str,
) -> anyhow::Result<SlotResume> { todo!() }

pub enum SlotResume {
    Existing(SlotInfo),
    Created { consistent_point: Lsn, snapshot_name: String }, // kept for PR 2.29
}
```

```rust
// crates/pg-sink/src/replication.rs
use bytes::Bytes;
use common::Lsn;

/// One decoded CopyBoth frame off the wire (payload still opaque pgoutput bytes).
pub enum ReplicationMessage {
    /// 'w' — XLogData
    XLogData { wal_start: Lsn, wal_end: Lsn, server_clock: i64, data: Bytes },
    /// 'k' — primary keepalive
    Keepalive { wal_end: Lsn, server_clock: i64, reply_requested: bool },
}

pub struct ReplicationStream { /* copy_both duplex, feedback deadline, last_received: Lsn */ }

impl ReplicationStream {
    pub async fn start(
        client: &tokio_postgres::Client,
        slot: &str,
        start_lsn: Lsn,
        publication: &str,
    ) -> anyhow::Result<Self> { todo!() } // START_REPLICATION ... (proto_version '2', streaming 'on', ...)

    /// Parse one frame; None on CopyDone/stream end.
    pub async fn next(&mut self) -> anyhow::Result<Option<ReplicationMessage>> { todo!() }

    /// The 'r' standby status update. `flush`/`apply` may lag `write` (that's the point).
    pub async fn send_standby_status(&mut self, s: StandbyStatus) -> anyhow::Result<()> { todo!() }
}

/// write >= flush >= apply. Keepalive path sends the *received* LSN as write; durability (PR 2.26)
/// is the only thing that advances flush/apply (= confirmed_flush_lsn).
#[derive(Clone, Copy)]
pub struct StandbyStatus { pub write: Lsn, pub flush: Lsn, pub apply: Lsn, pub reply_requested: bool }
```

```rust
// crates/pg-sink/tests/replication_spike.rs
#[tokio::test] async fn source_write_yields_at_least_one_xlogdata() { todo!() }
#[tokio::test] async fn connection_survives_past_wal_sender_timeout() { todo!() }
#[tokio::test] async fn reply_requested_keepalive_is_answered_immediately() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] Slot presence is verified and `restart_lsn` / `confirmed_flush_lsn` are read (or the slot is
      created capturing `consistent_point` + `snapshot_name`), establishing the `START_REPLICATION` LSN.
- [x] `START_REPLICATION` is issued with **`proto_version '2'`, `streaming 'on'`, `publication_names`**.
- [x] `'w'` and `'k'` frame headers parse correctly (LSNs are `common::Lsn`, big-endian).
- [x] Standby status updates go out on a sub-`wal_sender_timeout` interval **and** immediately on
      `reply_requested`, advancing only the **received/keepalive** LSN.
- [x] The spike outcome is recorded: `tokio-postgres` is sufficient, **or** the file notes the pivot to
      `pgwire-replication` and the seam (`ReplicationStream`) is unchanged for callers.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [x] `docker compose up --wait` then `cargo test -p pg-sink --test replication_spike`: a source
        `INSERT` produces ≥ 1 `XLogData`, and the connection is still alive **after** `wal_sender_timeout`.

## Hints & gotchas

- **Keepalive ≠ slot advance.** Send feedback *unconditionally* well under 60s; if you only send it
  after S3/manifest durability, a slow flush triggers `terminating walsender process due to replication
  timeout` and a reconnect storm. This is the single most-cited bug in the design — §1.9.
- LSNs on the wire are **8-byte big-endian**; the standby reply also carries an 8-byte microsecond
  timestamp since the Postgres epoch (2000-01-01). Reuse `common::Lsn` for parse/format.
- `START_REPLICATION` at `0/0` means "resume from the slot's `confirmed_flush_lsn`"; passing the read
  resume LSN explicitly is clearer and the server clamps up to its own value anyway.
- Don't consume the exported snapshot on the replication connection now — running any other command on
  it invalidates the snapshot (PR 2.29 needs it). Verify-only.
- Treat *"replication slot … is active"* as **transient / retry-with-backoff** (a replacement pod racing
  the old holder), not fatal — see `../../walrus-pg-sink.md` §4.1.
- Keep the XLogData payload as `Bytes` (zero-copy slice); PR 2.21's decoder consumes it directly.

## References

- Design: `../../architecture.md` §1.2, §1.9 (keepalive), "Startup & bootstrap" step 4;
  `../../proto-version.md` §7; `../../examples/proto-version/run-tests.sh`.
- Prev: [PR 2.19](./pr-2.19-sink-source-preflight.md) · Next: [PR 2.21](./pr-2.21-sink-wire-decoder.md) · [Roadmap](../README.md)
