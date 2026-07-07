# PR 2.8 — Two-phase (v3) messages: parse without misalignment, and disambiguate `'K'`

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/28

> **Phase:** 2 — walrus-pg-sink (2a: the hand-rolled decoder) · **Crates touched:** `pg-sink` ·
> **Est. size:** S · **Depends on:** PR 2.7 · **Unlocks:** PR 2.9

The decoder's completeness insurance. walrus runs at `proto_version '2'` and never enables
`two_phase`, so it will **never** see these messages in production — but a hand-rolled decoder must
still parse them without misaligning the cursor, because a defensive decoder that hits an unknown byte
should fail *loudly at that byte*, not silently corrupt the stream. This PR decodes the five two-phase
frames — `Begin Prepare ('b')`, `Prepare ('P')`, `Commit Prepared ('K')`, `Rollback Prepared ('r')`,
`Stream Prepare ('p')` — and, in doing so, resolves the one genuinely ambiguous byte in the catalog:
`'K'` is **both** the top-level Commit Prepared message id **and** the Update/Delete old-KEY submessage
marker. Context (top-level parse vs. inside a tuple) disambiguates it. This lights up the `TwoPhase`
vectors and closes out the decoder.

## Why — learning objectives

- **Parse-to-not-misalign** — decoding a message you never use is still correct behaviour: consume
  exactly its bytes so `parse_stream` stays aligned on whatever follows.
- **Context-sensitive byte meaning** — `'K'` at the top level (a message tag) vs. `'K'` inside a
  `Update`/`Delete` (an old-image marker) are the same byte with two meanings; the *parser position*,
  not the byte, decides. This is the canonical "why hand-rolled decoders need discipline" lesson.
- **GID + xid fields** — two-phase frames carry a global transaction id `String` (`gid`) and an xid; the
  layouts vary field-by-field (see the reference decoder).

## Read first

- `../../proto-version.md` §12 "Two-phase (v3) and parallel apply (v4)" — why walrus needs neither, and
  that the decoder must still not choke on them.
- `../../examples/proto-version/decode_pgoutput.py` — the `b`/`P`/`K`/`r`/`p` arms are the exact field
  orders (they differ: `rollback_prepared` has two ts fields and two end LSNs).
- `../../examples/proto-version/test_decode_pgoutput.py` — `TwoPhase.test_2pc_messages_parse_with_gid_and_xid`
  and `test_commit_prepared_K_byte_disambiguated_from_update_key_marker`.

## Scope

**In scope**

- `Message::{BeginPrepare, Prepare, CommitPrepared, RollbackPrepared, StreamPrepare}` and their arms.
- The `'K'` disambiguation: top-level `parse_message` dispatch reaches Commit Prepared; the old-KEY `'K'`
  is only ever read *inside* the `Update`/`Delete` arms (from 2.5) — the two never collide.

**Explicitly deferred** (do *not* build these here)

- Nothing further — this is the last decoder PR. The decoder now parses the full pgoutput catalog
  (v1 + v2 streaming + v3 two-phase). Wiring it to a live stream is sink **PR 2.21**.

## Files to create / modify

```
crates/pg-sink/src/pgoutput/mod.rs        # + Message::{BeginPrepare,Prepare,CommitPrepared,RollbackPrepared,StreamPrepare}
crates/pg-sink/tests/pgoutput_vectors.rs  # + 2pc vectors + K-disambiguation test + full-catalog enumerating test (un-ignored)
```

## Skeleton

```rust
// crates/pg-sink/src/pgoutput/mod.rs  (additions)
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    // … 2.2–2.7 variants …
    /// 'b': Int64 prepare LSN, Int64 end LSN, Int64 prepare ts, Int32 xid, String gid.
    BeginPrepare { prepare_lsn: Lsn, end_lsn: Lsn, prepare_ts: i64, xid: u32, gid: String },
    /// 'P': Int8 flags, then prepare LSN, end LSN, prepare ts, xid, gid.
    Prepare { flags: u8, prepare_lsn: Lsn, end_lsn: Lsn, prepare_ts: i64, xid: u32, gid: String },
    /// 'K': Int8 flags, commit LSN, end LSN, commit ts, xid, gid. (Top-level message — NOT old-KEY.)
    CommitPrepared { flags: u8, commit_lsn: Lsn, end_lsn: Lsn, commit_ts: i64, xid: u32, gid: String },
    /// 'r': Int8 flags, end LSN, rollback end LSN, prepare ts, rollback ts, xid, gid.
    RollbackPrepared {
        flags: u8, end_lsn: Lsn, rollback_end_lsn: Lsn,
        prepare_ts: i64, rollback_ts: i64, xid: u32, gid: String,
    },
    /// 'p': the streamed variant of Prepare (same fields as 'P').
    StreamPrepare { flags: u8, prepare_lsn: Lsn, end_lsn: Lsn, prepare_ts: i64, xid: u32, gid: String },
}

// dispatch additions (top-level parse_message):
//   b'b' => todo!("BeginPrepare"),
//   b'P' => todo!("Prepare"),
//   b'K' => todo!("CommitPrepared"),   // top-level: 'K' is Commit Prepared here
//   b'r' => todo!("RollbackPrepared"),
//   b'p' => todo!("StreamPrepare"),
// The OTHER 'K' — old-image marker — is read only inside the Update/Delete arms (PR 2.5),
// never through this dispatch. Same byte, two positions, no collision.
```

```rust
// crates/pg-sink/tests/pgoutput_vectors.rs  (additions)
#[test]
fn two_phase_vectors_render() {
    for name in ["begin_prepare", "prepare", "commit_prepared", "rollback_prepared"] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn two_phase_messages_carry_gid_and_xid() {
    // each of begin_prepare/prepare/commit_prepared/rollback_prepared → xid==100, gid=="gtx"
    todo!()
}

#[test]
fn commit_prepared_K_disambiguated_from_update_key_marker() {
    // top-level 'K' → Message::CommitPrepared;  'K' inside update_pk_change → old_kind == Key
    todo!()
}

/// Now un-ignored: every ported vector round-trips to its golden line.
#[test]
fn all_vectors_render_to_golden() {
    for v in VECTORS {
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{}", v.name);
    }
}
```

## Definition of Done

- [x] `begin_prepare`, `prepare`, `commit_prepared`, `rollback_prepared` all render to their golden lines.
- [x] Each two-phase vector decodes `xid == 100` and `gid == "gtx"` (proves the field order, esp.
      `rollback_prepared`'s two-ts / two-end-LSN layout, is exact — no misalignment).
- [x] `'K'` disambiguation: `commit_prepared` → `Message::CommitPrepared`; the `'K'` in `update_pk_change`
      → `old_kind == Some(Key)` (both decode correctly, same byte, different position).
- [x] The previously-`#[ignore]`d enumerating test from PR 2.1 is **removed/replaced** by
      `all_vectors_render_to_golden`, which passes for **every** vector — the decoder now covers the full
      catalog (v1 + v2 streaming + v3 two-phase).
- [x] A truly unknown top-level byte still yields `DecodeError::UnknownMessage` (regression check that
      adding these arms didn't turn a stray byte into a silent success).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (workspace stays green)

## Hints & gotchas

- **The `'K'` trap is the point of this PR.** There is no ambiguity *in the parser* because the two `'K'`
  sites are reached from different positions: the top-level dispatch never runs while you're mid-tuple,
  and the old-image read never consults the message dispatch. If you ever centralise "handle a `K`" into
  one shared function, you reintroduce the ambiguity — keep them separate.
- **Field orders differ per two-phase message** — do not copy one arm to the next. `rollback_prepared`
  has `end_lsn`, `rollback_end_lsn`, `prepare_ts`, `rollback_ts` (four extra fields vs. the others).
  Cross-check against the reference decoder's `r` arm byte-for-byte; the vector's golden line is your
  oracle.
- These messages **never appear** on walrus's v2 stream, so there's no runtime path to write later — the
  decoder is simply complete and misalignment-proof. Note this explicitly in a code comment so a future
  reader doesn't hunt for the (nonexistent) two-phase runtime handling.
- With the enumerating test now green across all vectors, the decoder is done: PR 2.9 pivots to
  `pg-to-arrow`, consuming the `common::PgRelation`/`TupleValue` this decoder produces — with no
  dependency back on `pg-sink`.

## References

- Design: `../../proto-version.md` §12; §13 "consumer contract".
- Prev: [PR 2.7](pr-2.7-pgoutput-streaming-frames.md) ·
  Next: [PR 2.9](pr-2.9-pgarrow-tier1-schema.md) (phase 2b — `pg-to-arrow`) · [Roadmap](../README.md)
