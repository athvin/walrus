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
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    /// Bytes left to read.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// The next byte without consuming it (`None` at end of buffer).
    #[must_use]
    pub fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    /// Error unless at least `n` bytes remain.
    fn need(&self, n: usize) -> Result<(), DecodeError> {
        if self.remaining() < n {
            Err(DecodeError::UnexpectedEof {
                needed: u32c(n),
                offset: u32c(self.pos),
                remaining: u32c(self.remaining()),
            })
        } else {
            Ok(())
        }
    }

    /// One byte (a `Byte1` type tag or an `Int8`).
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when no byte remains.
    pub fn byte1(&mut self) -> Result<u8, DecodeError> {
        self.need(1)?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// Big-endian `Int16`.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than two bytes remain.
    pub fn int16(&mut self) -> Result<u16, DecodeError> {
        self.need(2)?;
        let arr: [u8; 2] = self.buf[self.pos..self.pos + 2].try_into().map_err(|_| {
            DecodeError::UnexpectedEof {
                needed: 2,
                offset: u32c(self.pos),
                remaining: u32c(self.remaining()),
            }
        })?;
        self.pos += 2;
        Ok(u16::from_be_bytes(arr))
    }

    /// Big-endian `Int32` (OID / xid).
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than four bytes remain.
    pub fn int32(&mut self) -> Result<u32, DecodeError> {
        self.need(4)?;
        let arr: [u8; 4] = self.buf[self.pos..self.pos + 4].try_into().map_err(|_| {
            DecodeError::UnexpectedEof {
                needed: 4,
                offset: u32c(self.pos),
                remaining: u32c(self.remaining()),
            }
        })?;
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
        self.need(8)?;
        let arr: [u8; 8] = self.buf[self.pos..self.pos + 8].try_into().map_err(|_| {
            DecodeError::UnexpectedEof {
                needed: 8,
                offset: u32c(self.pos),
                remaining: u32c(self.remaining()),
            }
        })?;
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
        match self.buf[start..].iter().position(|&b| b == 0) {
            Some(rel) => {
                let s = std::str::from_utf8(&self.buf[start..start + rel])?.to_string();
                self.pos = start + rel + 1; // consume the NUL terminator
                Ok(s)
            }
            None => Err(DecodeError::UnexpectedEof {
                needed: 1,
                offset: u32c(start),
                remaining: u32c(self.remaining()),
            }),
        }
    }

    /// Borrow the next `n` bytes without copying, advancing the cursor.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than `n` bytes remain.
    pub fn slice(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        self.need(n)?;
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    /// Borrow the next `n` bytes as validated UTF-8 without copying.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] for a short read or [`DecodeError::Utf8`] for invalid
    /// UTF-8.
    pub fn str(&mut self, n: usize) -> Result<&'a str, DecodeError> {
        Ok(std::str::from_utf8(self.slice(n)?)?)
    }

    /// Copy `n` bytes into an owned [`Bytes`], advancing the cursor.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when fewer than `n` bytes remain.
    pub fn take(&mut self, n: usize) -> Result<Bytes, DecodeError> {
        Ok(Bytes::from(self.slice(n)?.to_vec()))
    }

    /// An LSN: an unsigned 8-byte value, wrapped into the `common::Lsn` newtype.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::UnexpectedEof`] when the eight-byte LSN is truncated.
    pub fn lsn(&mut self) -> Result<Lsn, DecodeError> {
        Ok(Lsn::new(self.int64()? as u64))
    }
}

#[cfg(test)]
#[path = "reader_test.rs"]
mod tests;
