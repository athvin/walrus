# PR 2.2 — Reader primitives, stream framing, and `Begin` / `Commit`

> **Phase:** 2 — walrus-pg-sink (2a: the hand-rolled decoder) · **Crates touched:** `pg-sink` ·
> **Est. size:** M · **Depends on:** PR 2.1 · **Unlocks:** PR 2.3

The first bytes actually decode. This PR builds the cursor (`Reader`) with the big-endian primitives
pgoutput uses, the `DecodeError` taxonomy, the `parse_message` dispatch keyed on the leading type byte,
the `parse_stream` splitter that skips `pg_recvlogical`'s `0x0a` separators, and the two transaction
boundary messages — `Begin ('B')` and `Commit ('C')` — plus `Origin ('O')` for completeness. It lights
up the `begin`, `commit`, and `parse_stream`-framing vectors.

## Why — learning objectives

- **Big-endian binary parsing in safe Rust** — `int16/int32/int64` via `from_be_bytes` over a
  bounds-checked slice; a null-terminated `String`; `Byte1` as a type tag.
- **`thiserror` error modelling in a lib** — a `DecodeError` enum whose variants (`UnexpectedEof`,
  `UnknownMessage`, …) are *structured*, not stringly-typed, so callers can branch on them.
- **A cursor that never panics** — every read is `Result`; running off the end is a modelled error, not
  an index-out-of-bounds.
- **Self-delimiting framing** — pgoutput messages carry their own length implicitly; `parse_stream`
  must skip exactly one `0x0a` between messages and thread `StreamCtx` across them.

## Read first

- `../../proto-version.md` §4 "The message catalog" — the field primitives table (`Int8/16/32/64`,
  `String`) and the exact Begin/Commit/Origin byte layouts.
- `../../proto-version.md` §7 "The per-message xid" — why `StreamCtx` exists (you only *thread* it here;
  the xid prefix logic lands in 2.3+).
- `../../examples/proto-version/decode_pgoutput.py` — `class Reader`, `parse_message` (the `B`/`C`/`O`
  arms), and `parse_stream` (the `0x0a`-skipping loop) are your reference.

## Scope

**In scope**

- `Reader` with `remaining/byte1/int16/int32/int64/string/take` + `lsn()` (→ `common::Lsn`).
- `DecodeError` (thiserror). `parse_message` dispatch on the first byte. `parse_stream`.
- `Message::{Begin, Commit, Origin}`; the test-crate `render()` arms for them.

**Explicitly deferred** (do *not* build these here)

- The xid prefix / `XID_PREFIXED` handling → **PR 2.3** (introduced with the first xid-prefixed message).
- `TupleData`, Relation, DML, streaming, two-phase → their own PRs.

## Files to create / modify

```
crates/pg-sink/src/pgoutput/mod.rs        # + Message::{Begin,Commit,Origin}, parse_message, parse_stream
crates/pg-sink/src/pgoutput/reader.rs     # new — Reader + primitives
crates/pg-sink/src/pgoutput/error.rs      # new — DecodeError
crates/pg-sink/tests/pgoutput_vectors.rs  # + begin/commit tests + parse_stream framing tests + render arms
```

## Skeleton

```rust
// crates/pg-sink/src/pgoutput/error.rs
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("unexpected end of message: needed {needed}B at offset {offset}, {remaining} left")]
    UnexpectedEof { needed: usize, offset: usize, remaining: usize },
    #[error("unknown message type byte {byte:#04x}")]
    UnknownMessage { byte: u8 },
    #[error("bad TupleData format byte {byte:#04x} (misaligned parse?)")]  // used from PR 2.4
    BadTupleFormat { byte: u8 },
    #[error("invalid replica identity byte {byte:#04x}")]                  // used from PR 2.3
    BadReplicaIdentity { byte: u8 },
    #[error("invalid UTF-8 in String field")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("{unconsumed} trailing bytes after a complete message")]
    TrailingBytes { unconsumed: usize },
}
```

```rust
// crates/pg-sink/src/pgoutput/reader.rs
use bytes::Bytes;
use common::Lsn;
use super::error::DecodeError;

/// Cursor over one message's bytes. Every read is bounds-checked → `Result`, never panics.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self { todo!() }
    pub fn remaining(&self) -> usize { todo!() }
    pub fn byte1(&mut self) -> Result<u8, DecodeError> { todo!() }
    pub fn int16(&mut self) -> Result<u16, DecodeError> { todo!() }
    pub fn int32(&mut self) -> Result<u32, DecodeError> { todo!() }
    pub fn int64(&mut self) -> Result<i64, DecodeError> { todo!() }
    /// Null-terminated UTF-8 `String`.
    pub fn string(&mut self) -> Result<String, DecodeError> { todo!() }
    /// Borrow `n` bytes, advancing the cursor (for TOAST-safe value copies from PR 2.4).
    pub fn take(&mut self, n: usize) -> Result<Bytes, DecodeError> { todo!() }
    /// LSN is an unsigned 8-byte value; reuse `int64` and wrap into the `common::Lsn` newtype.
    pub fn lsn(&mut self) -> Result<Lsn, DecodeError> { todo!() }
}
```

```rust
// crates/pg-sink/src/pgoutput/mod.rs  (additions)
pub mod error;
pub mod reader;

pub use error::DecodeError;
pub use reader::Reader;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    /// 'B': Int64 final LSN, Int64 commit ts (µs since 2000-01-01), Int32 xid.
    Begin { final_lsn: Lsn, commit_ts: i64, xid: u32 },
    /// 'C': Int8 flags, Int64 commit LSN, Int64 end LSN, Int64 commit ts.
    Commit { flags: u8, commit_lsn: Lsn, end_lsn: Lsn, commit_ts: i64 },
    /// 'O': Int64 commit LSN, String origin name.
    Origin { commit_lsn: Lsn, name: String },
    // Relation/Type (2.3) · Insert (2.4) · Update/Delete (2.5) · Truncate/Message (2.6)
    // · Stream* (2.7) · two-phase (2.8) …
}

/// Decode exactly one message from `reader`, advancing stream context. Rejects trailing bytes.
pub fn parse_message(reader: &mut Reader<'_>, ctx: &mut StreamCtx) -> Result<Message, DecodeError> {
    let _ = ctx; // consulted from PR 2.3 onward
    let tag = reader.byte1()?;
    match tag {
        b'B' => todo!("Begin"),
        b'C' => todo!("Commit"),
        b'O' => todo!("Origin"),
        other => Err(DecodeError::UnknownMessage { byte: other }),
    }
}

/// Split a raw walsender byte stream into messages, skipping the single `0x0a` `pg_recvlogical`
/// inserts between self-delimiting messages, threading `ctx` across them.
pub fn parse_stream(data: &[u8], ctx: &mut StreamCtx) -> Result<Vec<Message>, DecodeError> {
    todo!()
}
```

```rust
// crates/pg-sink/tests/pgoutput_vectors.rs  (additions)
#[test]
fn begin_decodes() {
    let v = lookup("begin");
    assert_eq!(render(&decode(v.hex, v.streaming)), v.expected);
}

#[test]
fn commit_decodes() {
    let v = lookup("commit");
    assert_eq!(render(&decode(v.hex, v.streaming)), v.expected);
}

#[test]
fn parse_stream_skips_newline_separators() {
    // begin \n commit \n  → [Begin, Commit]; the 0x0a bytes are separators, not data.
    todo!()
}

#[test]
fn unknown_type_byte_errors_not_panics() {
    todo!() // feeding b"Z…" yields DecodeError::UnknownMessage, never a panic
}
```

## Definition of Done

- [ ] `Reader` reads BE ints and null-terminated strings; every primitive returns `Err(UnexpectedEof)`
      (never panics) when the buffer is short.
- [ ] `begin` and `commit` vectors render to their exact golden lines.
- [ ] `parse_stream` splits `begin\ncommit\n` into `[Begin, Commit]` and `Begin.xid == 749`.
- [ ] A message whose leading byte is unrecognised yields `DecodeError::UnknownMessage`, not a panic.
- [ ] `parse_message` rejects a message with trailing unconsumed bytes (`TrailingBytes`).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (workspace stays green)

## Hints & gotchas

- **LSN is unsigned; commit-ts is signed µs.** Read the LSN 8-byte field into `u64` (via `i64` bit-cast
  is fine — it's the same 8 bytes) and wrap in `common::Lsn`; keep `commit_ts` as `i64` micros and let
  the *test* renderer format it (production uses `SinkMeta`'s `Z` stamp, PR 1.1).
- **`String` bounds:** find the `0x00` terminator *within* `remaining()`; a missing terminator is
  `UnexpectedEof`, not a slice panic. Validate UTF-8 with `std::str::from_utf8` → `?` into `Utf8`.
- **`parse_stream` skips exactly one `0x0a`** per gap — pgoutput messages are self-delimiting, so do not
  length-prefix them. Do not skip `0x0a` bytes that fall *inside* a message body (there are none at a
  message boundary, but a value could contain one — which is why you only skip at the top of the loop).
- Render `Begin` with the wide padding the golden line uses (`"BEGIN         "`); diff failures are
  almost always a padding or `X/Y`-hex-casing mismatch, not a parse bug.
- Do **not** consume the xid prefix yet — `Begin`/`Commit`/`Origin` are never xid-prefixed (they are the
  txn frame itself); the prefix machinery arrives with the first xid-prefixed message in 2.3.

## References

- Design: `../../proto-version.md` §4, §7.
- Prev: [PR 2.1](pr-2.1-pgoutput-scaffold-golden-vectors.md) ·
  Next: [PR 2.3](pr-2.3-pgoutput-relation-type.md) · [Roadmap](../README.md)
