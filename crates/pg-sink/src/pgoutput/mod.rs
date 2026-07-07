//! Hand-rolled pgoutput (`proto_version` 2) decoder. Sync + pure: no tokio, no I/O.
//!
//! Shapes only in PR 2.1 — the decoding logic arrives family-by-family in PRs 2.2–2.8, TDD-style
//! against the golden vectors in `tests/pgoutput_vectors.rs`.

/// A cursor over a single pgoutput message's bytes. Big-endian readers land in PR 2.2.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    pub bytes: &'a [u8],
    pub pos: usize,
}

impl<'a> Reader<'a> {
    /// Wrap a byte slice at position 0.
    pub fn new(bytes: &'a [u8]) -> Self {
        Reader { bytes, pos: 0 }
    }
}

/// Whether we are inside a Stream Start..Stop block. The per-message xid prefix (proto §7) exists
/// **only** while this is true; Stream Start/Stop toggle it.
#[derive(Debug, Default, Clone, Copy)]
pub struct StreamCtx {
    pub in_stream: bool,
}

/// One decoded pgoutput message. Variants are added family-by-family in PRs 2.2–2.8:
/// 2.2 Begin/Commit/Origin; 2.3 Relation/Type; 2.4 Insert; 2.5 Update/Delete; 2.6 Truncate/Message;
/// 2.7 Stream*; 2.8 the two-phase family.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {}

/// Decode exactly one message, advancing `ctx` for stream framing. Implemented in PR 2.2+.
pub fn parse_message(_buf: &[u8], _ctx: &mut StreamCtx) -> Message {
    unimplemented!("PR 2.2")
}

/// Decode a `pg_recvlogical` byte stream (messages separated by `0x0a`), preserving stream context
/// across messages. Implemented in PR 2.2+.
pub fn parse_stream(_raw: &[u8]) -> Vec<Message> {
    unimplemented!("PR 2.2")
}
