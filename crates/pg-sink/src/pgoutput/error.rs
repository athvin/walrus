//! The pgoutput decoder's structured error taxonomy.

/// Everything that can go wrong decoding a pgoutput message. Variants are *structured* (not
/// stringly-typed) so callers can branch on them; several are used from later PRs (2.3/2.4).
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("unexpected end of message: needed {needed}B at offset {offset}, {remaining} left")]
    UnexpectedEof {
        needed: usize,
        offset: usize,
        remaining: usize,
    },
    #[error("unknown message type byte {byte:#04x}")]
    UnknownMessage { byte: u8 },
    #[error("bad TupleData format byte {byte:#04x} (misaligned parse?)")]
    BadTupleFormat { byte: u8 },
    #[error("invalid replica identity byte {byte:#04x}")]
    BadReplicaIdentity { byte: u8 },
    #[error("invalid UTF-8 in String field")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("{unconsumed} trailing bytes after a complete message")]
    TrailingBytes { unconsumed: usize },
}
