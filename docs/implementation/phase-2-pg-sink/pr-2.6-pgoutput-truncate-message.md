# PR 2.6 — `Truncate` and the logical `Message`

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/26

> **Phase:** 2 — walrus-pg-sink (2a: the hand-rolled decoder) · **Crates touched:** `pg-sink` ·
> **Est. size:** S · **Depends on:** PR 2.5 · **Unlocks:** PR 2.7

Two non-tuple messages that complete the v1 catalog. `Truncate ('T')` is `Int32 rel-count`, `Int8`
option bits (`1`=CASCADE, `2`=RESTART IDENTITY), then that many relation OIDs — and crucially **no
tuple, no PK**, which is exactly why the loader can't treat it as a `MERGE` branch and handles it as a
separate wipe step. `Message ('M')` is the logical-decoding message walrus uses for heartbeats; its
`transactional` flag (bit 1 of `Int8 flags`) is load-bearing — a non-transactional message is emitted
immediately, even ahead of its transaction's `Begin`. This PR lights up `truncate_*` and `message_*`.

## Why — learning objectives

- **Fixed-count arrays** — read `Int32 nrel`, then loop `nrel` `Int32` OIDs; the count *is* the length,
  there's no per-element framing.
- **Option-bit decoding** — `cascade = flags & 1`, `restart_identity = flags & 2`; two independent bits
  in one byte.
- **Why Truncate is special downstream** — no PK means it can't be a row-level `MERGE`; the sink emits it
  as a tuple-boundary event and the loader wipes then filters by `(commit_lsn, lsn)` (loader §5.5, PR 3.5).
- **Transactional vs non-transactional messages** — the flag that lets an idle heartbeat advance the slot
  without waiting on some unrelated open transaction (§4 Message note; architecture §1.9).

## Read first

- `../../proto-version.md` §4 "Truncate `'T'`" + "Message `'M'`" — the byte layouts and the option bits;
  note the emphasised "carries **no tuple/PK**" and the transactional-vs-not behaviour.
- `../../examples/proto-version/test_decode_pgoutput.py` — `test_truncate_option_bits`,
  `test_message_transactional_flag`.

## Scope

**In scope**

- `Message::Truncate { xid, cascade, restart_identity, relations }`.
- `Message::Message { xid, transactional, lsn, prefix, content }` — content as `bytes::Bytes`.

**Explicitly deferred** (do *not* build these here)

- The TRUNCATE wipe + `(commit_lsn,lsn)` filter → loader **PR 3.5**.
- Recognising the `walrus`-prefix heartbeat / round-trip liveness → sink **PR 2.27**; DDL-audit message
  handling → sink **PR 2.33**. Here you only decode the message faithfully.

## Files to create / modify

```
crates/pg-sink/src/pgoutput/mod.rs        # + Message::{Truncate, Message}, 'T'/'M' arms
crates/pg-sink/tests/pgoutput_vectors.rs  # + truncate_*/message_* tests + option-bit + txn-flag
```

## Skeleton

```rust
// crates/pg-sink/src/pgoutput/mod.rs  (additions)
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    // … 2.2–2.5 variants …
    /// 'T': Int32 rel-count, Int8 option bits (1=CASCADE, 2=RESTART IDENTITY), then the rel OIDs.
    /// Carries NO tuple / NO PK — handled as a separate wipe step downstream.
    Truncate {
        xid: Option<u32>,
        cascade: bool,
        restart_identity: bool,
        relations: Vec<u32>,
    },
    /// 'M': Int8 flags (bit 1 = transactional), Int64 message LSN, String prefix,
    ///      Int32 content length + content bytes.
    Message {
        xid: Option<u32>,
        transactional: bool,
        lsn: Lsn,
        prefix: String,
        content: Bytes,
    },
}

// 'T' arm shape:
//   let nrel = reader.int32()? as usize;
//   let opt  = reader.byte1()?;
//   let relations = (0..nrel).map(|_| reader.int32()).collect::<Result<_, _>>()?;
//   // cascade = opt & 1 != 0; restart_identity = opt & 2 != 0

// 'M' arm shape:
//   let flags = reader.byte1()?;        // transactional = flags & 1 != 0
//   let lsn = reader.lsn()?;
//   let prefix = reader.string()?;
//   let len = reader.int32()? as usize;
//   let content = reader.take(len)?;
```

```rust
// crates/pg-sink/tests/pgoutput_vectors.rs  (additions)
#[test]
fn truncate_and_message_vectors_render() {
    for name in ["truncate_plain", "truncate_cascade_restart",
                 "message_non_transactional", "message_transactional"] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn truncate_option_bits() {
    // truncate_plain: (cascade, restart_identity) == (false, false)
    // truncate_cascade_restart: == (true, true)
    todo!()
}

#[test]
fn message_transactional_flag() {
    // message_non_transactional.transactional == false; message_transactional == true
    todo!()
}
```

## Definition of Done

- [x] `truncate_plain`, `truncate_cascade_restart`, `message_non_transactional`, `message_transactional`
      all render to their golden lines.
- [x] `truncate_plain` → `(cascade, restart_identity) == (false, false)`, `relations == [16651]`;
      `truncate_cascade_restart` → `(true, true)`, `relations == [16665]`.
- [x] `message_non_transactional.transactional == false`; `message_transactional.transactional == true`.
- [x] Message `content` is preserved as raw bytes (e.g. `b"hb-non"`, `b"hb-txn"`), not utf-8-lossily
      decoded in the decoder.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (workspace stays green)

## Hints & gotchas

- **`Truncate` has no `'N'` and no `TupleData`** — do not call `parse_tuple`. After the option byte it's
  pure `Int32` OIDs. Reaching for `parse_tuple` here is a common muscle-memory slip after 2.4/2.5.
- **Collect fallibly:** `(0..nrel).map(|_| reader.int32()).collect::<Result<Vec<_>, _>>()?` propagates a
  short-buffer error instead of unwrapping mid-loop.
- **Message content is bytes, prefix is a String.** The prefix is null-terminated (`reader.string()`); the
  content is length-prefixed and may be non-utf-8 — keep it `Bytes`. The render helper shows it as a
  byte-string literal (`b'hb-non'`) to match Python.
- The message LSN is a distinct field from any commit/final LSN — it's where the message sits in the WAL;
  store it, the heartbeat round-trip logic (PR 2.27) will read it.

## References

- Design: `../../proto-version.md` §4 (Truncate, Message).
- Prev: [PR 2.5](pr-2.5-pgoutput-update-delete.md) ·
  Next: [PR 2.7](pr-2.7-pgoutput-streaming-frames.md) · [Roadmap](../README.md)
