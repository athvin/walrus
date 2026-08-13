#![deny(clippy::indexing_slicing)] // PR 16.2: every hot-path access must carry a bounds proof.

//! The [`Reader`] cursor: big-endian primitives over a bounds-checked byte slice. Every read is a
//! `Result` — running off the end is a modelled [`DecodeError::UnexpectedEof`], never a panic.

use super::error::DecodeError;
use bytes::Bytes;
use common::Lsn;

/// A pgoutput frame is `Int32`-bounded. Saturating keeps malformed-input errors infallible.
pub(super) fn u32c(n: usize) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// Cursor over one message's bytes.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Wrap a byte slice at position 0.
    #[must_use]
    #[inline]
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    /// Bytes left to read.
    #[must_use]
    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// The next byte without consuming it (`None` at end of buffer).
    #[must_use]
    #[inline]
    pub fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    /// The unread tail. Successful reads are the only way `pos` advances, so the fallback is dead.
    fn rest(&self) -> &'a [u8] {
        self.buf.get(self.pos..).unwrap_or_default()
    }

    /// Error unless at least `n` bytes remain, returning exactly the checked head.
    fn need(&self, n: usize) -> Result<&'a [u8], DecodeError> {
        self.rest()
            .get(..n)
            .ok_or_else(|| eof(n, self.pos, self.remaining()))
    }

    /// One byte (a `Byte1` type tag or an `Int8`).
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when no byte remains.
    #[inline]
    pub fn byte1(&mut self) -> Result<u8, DecodeError> {
        // `need(1)` returns exactly one byte, so the total fallback cannot be reached.
        let b = self.need(1)?.first().copied().unwrap_or_default();
        self.pos += 1;
        Ok(b)
    }

    /// Big-endian `Int16`.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than two bytes remain.
    pub fn int16(&mut self) -> Result<u16, DecodeError> {
        let Some(&arr) = self.rest().first_chunk::<2>() else {
            return Err(eof(2, self.pos, self.remaining()));
        };
        self.pos += 2;
        Ok(u16::from_be_bytes(arr))
    }

    /// Big-endian `Int32` (OID / xid).
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than four bytes remain.
    pub fn int32(&mut self) -> Result<u32, DecodeError> {
        let Some(&arr) = self.rest().first_chunk::<4>() else {
            return Err(eof(4, self.pos, self.remaining()));
        };
        self.pos += 4;
        Ok(u32::from_be_bytes(arr))
    }

    /// Big-endian `Int64` (the raw 8-byte field; LSN reads it as unsigned via [`Reader::lsn`],
    /// commit timestamps keep it as signed µs).
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than eight bytes remain.
    pub fn int64(&mut self) -> Result<i64, DecodeError> {
        let Some(&arr) = self.rest().first_chunk::<8>() else {
            return Err(eof(8, self.pos, self.remaining()));
        };
        self.pos += 8;
        Ok(i64::from_be_bytes(arr))
    }

    /// A null-terminated UTF-8 `String`. A missing terminator is `UnexpectedEof`, not a panic.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] if no NUL terminator remains, or
    /// [`DecodeError::Utf8`] if the bytes before it are not valid UTF-8.
    pub fn string(&mut self) -> Result<String, DecodeError> {
        let start = self.pos;
        let tail = self.rest();
        let Some(rel) = tail.iter().position(|&b| b == 0) else {
            return Err(eof(1, start, self.remaining()));
        };
        // `position` proved the split point; the total fallback is unreachable.
        let (text, _) = tail.split_at_checked(rel).unwrap_or_default();
        let s = std::str::from_utf8(text)?.to_string();
        self.pos = start + rel + 1; // consume the NUL terminator
        Ok(s)
    }

    /// Borrow the next `n` bytes without copying, advancing the cursor.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than `n` bytes remain.
    #[inline]
    pub fn slice(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let out = self.need(n)?;
        self.pos += n;
        Ok(out)
    }

    /// Borrow the next `n` bytes as validated UTF-8 without copying.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] for a short read or [`DecodeError::Utf8`] for invalid
    /// UTF-8.
    #[inline]
    pub fn str(&mut self, n: usize) -> Result<&'a str, DecodeError> {
        Ok(std::str::from_utf8(self.slice(n)?)?)
    }

    /// Copy `n` bytes into an owned [`Bytes`], advancing the cursor.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than `n` bytes remain.
    #[inline]
    pub fn take(&mut self, n: usize) -> Result<Bytes, DecodeError> {
        Ok(Bytes::from(self.slice(n)?.to_vec()))
    }

    /// An LSN: an unsigned 8-byte value, wrapped into the `common::Lsn` newtype.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when the eight-byte LSN is truncated.
    #[inline]
    pub fn lsn(&mut self) -> Result<Lsn, DecodeError> {
        Ok(Lsn::new(self.int64()? as u64))
    }
}

/// Build the modelled EOF error for a failed read.
///
/// `#[cold]` marks the branch as unlikely, while `#[inline(never)]` keeps the three-field setup out
/// of the hot reader bodies. `reader_test.rs` pins the exact needed/offset/remaining payload.
#[cold]
#[inline(never)]
fn eof(needed: usize, pos: usize, remaining: usize) -> DecodeError {
    DecodeError::UnexpectedEof {
        needed: u32c(needed),
        offset: u32c(pos),
        remaining: u32c(remaining),
    }
}

#[cfg(test)]
#[path = "reader_test.rs"]
mod tests;
