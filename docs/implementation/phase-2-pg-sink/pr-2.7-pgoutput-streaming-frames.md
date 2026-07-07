# PR 2.7 — v2 stream frames, the per-message xid, and sub-transaction abort (crown jewel)

> **Phase:** 2 — walrus-pg-sink (2a: the hand-rolled decoder) · **Crates touched:** `pg-sink` ·
> **Est. size:** M · **Depends on:** PR 2.6 · **Unlocks:** PR 2.8

The correctness centrepiece of the decoder. `proto_version '2' + streaming 'on'` chops a large
in-progress transaction into `Stream Start ('S')` … `Stream Stop ('E')` blocks, ended by
`Stream Commit ('c')` or `Stream Abort ('A')`. `Stream Start` carries the **top-level** xid and a
first-segment flag; each change *inside* a block carries its **own sub-transaction** xid (the prefix
you wired in 2.3, gated on `ctx.in_stream`). `Stream Abort {top, sub}` with `sub != top` means "discard
exactly that savepoint's rows while the top-level transaction still commits" — the case that silently
corrupts a mirror if ignored (§9b). This PR makes `Stream Start`/`Stop` toggle `ctx.in_stream`, decodes
all four stream frames, and lights up every `stream_*` vector plus the flagship abort test.

## Why — learning objectives

- **Stateful framing** — `Stream Start` sets `ctx.in_stream = true`, `Stream Stop` clears it; that single
  bit decides whether the *next* change reads an xid prefix (§7). This is why `StreamCtx` is `&mut`.
- **Top-level vs sub-transaction xid** — `Stream Start`/`Stream Commit` name the top xid; per-message
  prefixes name the sub xid; `Stream Abort` names *both*. The gap is the whole savepoint story (§9b).
- **`whole_txn` as a derived predicate** — `top == sub` ⇒ whole-transaction abort (drop everything);
  `top != sub` ⇒ discard only the sub-xid's rows (§9a vs §9b).
- **Why commit order ≠ start order** — streamed transactions interleave and can commit out of start order
  (§10); this is *why* walrus keys everything on commit LSN. You don't implement the ordering here, but
  you decode the `(xid, commit_lsn)` that makes it possible.

## Read first

- `../../proto-version.md` §7 "The per-message xid" — the "only while streaming" rule and the top-vs-sub
  distinction (the last paragraph).
- `../../proto-version.md` §8 "Streaming: how a big transaction is chopped up" — Stream Start/Stop/Commit,
  the first-segment flag.
- `../../proto-version.md` §9 "Abort and rollback" — §9a whole-txn (sub==top) vs §9b the dangerous
  sub-transaction rollback (sub!=top).
- `../../proto-version.md` §10 "Interleaving and commit ordering" — why demux-by-xid + commit-LSN order.
- `../../examples/proto-version/test_decode_pgoutput.py` — `Semantics.test_subtransaction_abort_is_not_whole_txn`
  (the flagship), `test_whole_txn_abort_flagged`, and all of `XidPrefixOnlyWhenStreaming` +
  `StreamFraming.test_parse_stream_preserves_streaming_context_across_messages`.

## Scope

**In scope**

- `Message::{StreamStart, StreamStop, StreamCommit, StreamAbort}`; the `S`/`E`/`c`/`A` arms.
- `Stream Start` sets `ctx.in_stream = true`; `Stream Stop` clears it.
- `StreamAbort::whole_txn()` (or a field) = `top_xid == sub_xid`.
- The `streamed_insert_carries_xid` path (a change inside a block reads the sub-xid prefix — already
  wired in 2.3, now *exercised*).

**Explicitly deferred** (do *not* build these here)

- Buffering/commit-gating/abort-dropping to S3 → sink runtime, **PRs 2.30–2.31**. The decoder only
  *reports* the frames; it does not act on them.
- Parallel-apply (`streaming 'parallel'`, v4) abort LSN/ts fields → never (§12). Under `streaming 'on'`,
  `Stream Abort` is exactly `'A' + top_xid + sub_xid`; do **not** read the extra pair.

## Files to create / modify

```
crates/pg-sink/src/pgoutput/mod.rs        # + Message::Stream*, S/E/c/A arms, ctx toggle
crates/pg-sink/tests/pgoutput_vectors.rs  # + stream_* tests, flagship abort test, context-toggle test
```

## Skeleton

```rust
// crates/pg-sink/src/pgoutput/mod.rs  (additions)
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    // … 2.2–2.6 variants …
    /// 'S': Int32 top-level xid, Int8 first-segment flag (1 = first block for this xid).
    StreamStart { xid: u32, first_segment: bool },
    /// 'E': no payload. Closes the current streamed block.
    StreamStop,
    /// 'c': Int32 xid, Int8 flags, Int64 commit LSN, Int64 end LSN, Int64 commit ts.
    StreamCommit { xid: u32, flags: u8, commit_lsn: Lsn, end_lsn: Lsn, commit_ts: i64 },
    /// 'A': Int32 top xid, Int32 sub xid. Under streaming='on' there are NO trailing LSN/ts fields.
    StreamAbort { top_xid: u32, sub_xid: u32 },
}

impl Message {
    /// For a StreamAbort: true when the WHOLE transaction aborted (§9a), false for a rolled-back
    /// savepoint inside a committing txn (§9b). Panics/None for other variants — see the helper below.
    pub fn is_whole_txn_abort(&self) -> Option<bool> {
        match self {
            Message::StreamAbort { top_xid, sub_xid } => Some(top_xid == sub_xid),
            _ => None,
        }
    }
}

// arm shapes:
//   b'S' => { let xid = reader.int32()?; let first = reader.byte1()? != 0;
//             ctx.in_stream = true;  Ok(StreamStart { xid, first_segment: first }) }
//   b'E' => { ctx.in_stream = false; Ok(StreamStop) }
//   b'c' => { /* xid, flags, commit_lsn, end_lsn, commit_ts */ todo!() }
//   b'A' => { let top = reader.int32()?; let sub = reader.int32()?;
//             Ok(StreamAbort { top_xid: top, sub_xid: sub }) }
```

```rust
// crates/pg-sink/tests/pgoutput_vectors.rs  (additions)
#[test]
fn stream_vectors_render() {
    for name in ["stream_start_first", "stream_stop", "stream_commit",
                 "stream_abort_subtransaction", "stream_abort_whole_txn",
                 "streamed_insert_carries_xid"] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

/// FLAGSHIP: a rolled-back savepoint is NOT a whole-txn abort — discard only sub_xid's rows.
#[test]
fn subtransaction_abort_is_not_whole_txn() {
    // stream_abort_subtransaction: top=757, sub=758, is_whole_txn_abort() == Some(false)
    todo!()
}

#[test]
fn whole_txn_abort_flagged() {
    // stream_abort_whole_txn: top==sub==866, is_whole_txn_abort() == Some(true)
    todo!()
}

#[test]
fn stream_start_stop_toggle_context() {
    // Start → ctx.in_stream true → streamed insert reads sub-xid 753 → Stop → ctx.in_stream false.
    todo!()
}

#[test]
fn parse_stream_preserves_streaming_context_across_messages() {
    // 53… \n <streamed insert> \n E \n  → [StreamStart, Insert{xid:Some(753)}, StreamStop]
    todo!()
}
```

## Definition of Done

- [ ] All six `stream_*` vectors render to their golden lines.
- [ ] `stream_abort_subtransaction` → `top_xid=757, sub_xid=758, is_whole_txn_abort() == Some(false)`
      (**the flagship**: a committing txn's rolled-back savepoint).
- [ ] `stream_abort_whole_txn` → `top_xid == sub_xid == 866, is_whole_txn_abort() == Some(true)`.
- [ ] `Stream Start` sets `ctx.in_stream`; the following `streamed_insert_carries_xid` decodes with
      `xid == Some(753)`; `Stream Stop` clears the context and a subsequent change has `xid == None`.
- [ ] `parse_stream` threads the context across `0x0a` separators: `[StreamStart, Insert, StreamStop]`
      with the insert's `xid == Some(753)`.
- [ ] `Stream Abort` reads exactly two `Int32`s (no trailing LSN/ts) — the remaining-bytes guard from 2.2
      confirms nothing is left over.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (workspace stays green)

## Hints & gotchas

- **`ctx.in_stream` is the linchpin.** Set it in the `'S'` arm *before* returning and clear it in `'E'`.
  If you forget, the next change won't read its sub-xid prefix and every field shifts by four bytes —
  and the failure surfaces as a mystery `BadTupleFormat`, not "missing xid".
- **`Stream Abort` under `streaming 'on'` is just two xids.** The abort LSN/timestamp pair exists only
  under `streaming 'parallel'` (v4), which walrus never enables — reading them here would consume 16
  bytes that aren't there. The reference decoder's `'A'` comment spells this out.
- **sub != top is the mirror-corruption case.** Internalise §9b: 2762 rolled-back rows *were* streamed to
  the consumer; only honoring `Stream Abort {top, sub}` keeps them out of the target. The decoder's job
  is to surface `(top, sub)` unambiguously; the sink acts on it in 2.31.
- **Lowercase `'c'` (Stream Commit) ≠ uppercase `'C'` (Commit).** They are different messages with
  different layouts; the match is byte-exact. Likewise `'S'`/`'s'`, `'E'` are distinct — mind the case.
- Do not sort or reorder here; the decoder is a straight-line parser. Commit-LSN ordering (§10) is the
  runtime's concern (PRs 2.30, 3.2+).

## References

- Design: `../../proto-version.md` §7, §8, §9, §10; §13 "consumer contract".
- Prev: [PR 2.6](pr-2.6-pgoutput-truncate-message.md) ·
  Next: [PR 2.8](pr-2.8-pgoutput-two-phase.md) · [Roadmap](../README.md)
