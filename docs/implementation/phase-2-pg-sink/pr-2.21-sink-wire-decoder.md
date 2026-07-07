# PR 2.21 — Wire the pgoutput decoder to the live stream

> **Phase:** 2 — walrus-pg-sink (2c — the sink binary) · **Crates touched:** `pg-sink` (bin+lib) ·
> **Est. size:** M · **Depends on:** PR 2.20 · **Unlocks:** PR 2.22

Join the two halves already built: feed each `XLogData` payload from the live `ReplicationStream`
(PR 2.20) into `pgoutput::parse_message` (PRs 2.2–2.8), threading the **streaming context** so v2 stream
frames parse correctly, and log every decoded message with structured fields. This is the Rust analogue
of the proof harness's `run-tests.sh`: an `INSERT` into `orders` now decodes to `Begin → Relation →
Insert → Commit` against a real Postgres. No Arrow, no batching, no S3 — just "the bytes become typed
messages, live."

## Why — learning objectives

By the end of this PR you will have practised:

- **Composing a sync pure decoder into an async loop** — the decoder stays `sync + pure`; the loop owns
  I/O and calls it per payload (the seam that kept the decoder unit-testable).
- **Threading decode state across frames** — a `StreamingCtx` (are we inside a `Stream Start`/`Stop`
  block? which `xid`?) carried by the loop, since a v2 sub-xid prefix appears *only inside* a stream.
- **Structured `tracing`** — emit `xid`, `commit_lsn`, `lsn`, `source_table` as fields, never
  interpolated into the message string, so logs are queryable.
- **Reusing a proof harness as a live test** — porting `run-tests.sh` assertions to a Rust compose test.

## Read first

- `../../proto-version.md` §4 The message catalog (`#4-the-message-catalog-decoded-byte-by-byte`) — the
  message model the decoder already produces; this PR just routes live bytes into it.
- `../../architecture.md` §1.2 (`#12-replication-consumer--hand-rolled`) — "parse the pgoutput message
  model ourselves … demultiplexing per `xid`."
- `../../examples/proto-version/run-tests.sh` — the 28 live-wire assertions; the Begin/Relation/Insert/
  Commit sequence this PR reproduces in Rust.
- `../../examples/proto-version/decode_pgoutput.py` — the reference decoder + streaming-context handling.

## Scope

**In scope**

- A decode loop: `ReplicationStream::next()` → on `XLogData`, call `pgoutput::parse_message(payload, &ctx)`.
- Maintain a `StreamingCtx` updated by `Stream Start`/`Stop` (and per-message `xid` under streaming).
- Structured-`tracing` log per decoded message (op, table, `commit_lsn`, `lsn`, `xid`).
- Keepalive frames (`'k'`) still answered (PR 2.20 path) even while decoding.

**Explicitly deferred** (do *not* build these here)

- Relation cache / Arrow schema build → **PR 2.22**.
- Batching, Parquet, S3, manifest, checkpoint → **PRs 2.23–2.26**.
- Per-`xid` demux / speculative staging for large txns → **PR 2.30**.
- Truncate/heartbeat/DDL special handling → **PRs 2.27 / 2.33**.

## Files to create / modify

```
crates/pg-sink/src/consume.rs        # new — decode loop wiring ReplicationStream -> pgoutput
crates/pg-sink/src/main.rs           # modify — spawn the consume loop under the CancellationToken
crates/pg-sink/tests/live_decode.rs  # new — compose: INSERT orders -> Begin/Relation/Insert/Commit
```

## Skeleton

```rust
// crates/pg-sink/src/consume.rs
use crate::pgoutput::{self, StreamingCtx, PgOutputMessage};
use crate::replication::{ReplicationStream, ReplicationMessage};
use tokio_util::sync::CancellationToken;

/// Drive the stream: decode each XLogData, keep keepalives answered, exit on cancel.
pub async fn run_decode_loop(
    stream: &mut ReplicationStream,
    token: CancellationToken,
) -> anyhow::Result<()> { todo!() }

/// Route one live frame. Updates `ctx` on Stream Start/Stop; returns the decoded message (if any).
pub fn on_frame(
    ctx: &mut StreamingCtx,
    frame: ReplicationMessage,
) -> anyhow::Result<Option<PgOutputMessage>> { todo!() }

/// Structured log for one decoded message (fields, not string interpolation).
fn trace_message(msg: &PgOutputMessage) { todo!() }
```

```rust
// crates/pg-sink/tests/live_decode.rs
/// Rust analogue of run-tests.sh: a single INSERT decodes to the canonical 4-message sequence.
#[tokio::test]
async fn insert_into_orders_decodes_begin_relation_insert_commit() { todo!() }

/// Streaming context is threaded: a message's xid is visible only inside a Stream block.
#[tokio::test]
async fn per_message_xid_present_only_within_stream() { todo!() }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Live `XLogData` payloads are decoded by the **existing** `pgoutput::parse_message` (no forked
      copy of the decoder).
- [ ] `StreamingCtx` is updated on `Stream Start`/`Stop` so v2 framing decodes without misalignment.
- [ ] Each decoded message logs via `tracing` with **structured fields** (`op`, `source_table`,
      `commit_lsn`, `lsn`, `xid`) — no `println!`, no interpolation.
- [ ] Keepalive replies still go out while the loop decodes (no regression from PR 2.20).
- [ ] The loop exits cleanly on the `CancellationToken`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)
  - [ ] `docker compose up --wait` then `cargo test -p pg-sink --test live_decode`: an `INSERT INTO
        orders` decodes to `Begin, Relation, Insert, Commit` in order.

## Hints & gotchas

- The decoder is **sync and pure by contract** — do *not* make `parse_message` `async` to fit the loop;
  the loop `.await`s I/O, then calls the decoder synchronously on the returned `Bytes`.
- The **first** change for a relation always arrives preceded by a `Relation` message; the decoder may
  need that `Relation` cached to interpret later tuples — but caching is PR 2.22. Here, just decode and
  log; a later `Insert` without a cached relation is fine to log as "relation <oid>".
- Under `streaming 'on'`, small txns still arrive **whole at commit** (no stream frames) — your
  `StreamingCtx` must handle both the streamed and non-streamed shapes without special-casing at the
  call site.
- Answer `'k'` keepalives inside the same `select!` as decode so a burst of XLogData can't starve the
  feedback deadline.
- Keep the loop's error handling honest: a decode error on a *known* message is a bug (fail loud); a
  transient stream I/O error is retryable (reconnect logic lands later, but don't `unwrap()`).

## References

- Design: `../../proto-version.md` §4; `../../architecture.md` §1.2;
  `../../examples/proto-version/run-tests.sh`, `../../examples/proto-version/decode_pgoutput.py`.
- Prev: [PR 2.20](./pr-2.20-sink-replication-connection-keepalive.md) · Next: [PR 2.22](./pr-2.22-sink-relation-cache.md) · [Roadmap](../README.md)
