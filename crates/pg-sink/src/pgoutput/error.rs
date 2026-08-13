//! The pgoutput decoder's structured error taxonomy.

/// Everything that can go wrong decoding a pgoutput message. Variants are *structured* (not
/// stringly-typed) so callers can branch on them; several are used from later PRs (2.3/2.4).
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// Widths match pgoutput's `Int32` frame bound, keeping this per-byte error path compact.
    #[error("unexpected end of message: needed {needed}B at offset {offset}, {remaining} left")]
    UnexpectedEof {
        needed: u32,
        offset: u32,
        remaining: u32,
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
    TrailingBytes { unconsumed: u32 },
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<DecodeError>() == 24,
    "DecodeError crosses the pgoutput frame decode path"
);
