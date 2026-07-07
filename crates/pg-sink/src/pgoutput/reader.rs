//! The [`Reader`] cursor: big-endian primitives over a bounds-checked byte slice. Every read is a
//! `Result` — running off the end is a modelled [`DecodeError::UnexpectedEof`], never a panic.

use super::error::DecodeError;
use bytes::Bytes;
use common::Lsn;

/// Cursor over one message's bytes.
#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Wrap a byte slice at position 0.
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    /// Bytes left to read.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// The next byte without consuming it (`None` at end of buffer).
    pub fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    /// Error unless at least `n` bytes remain.
    fn need(&self, n: usize) -> Result<(), DecodeError> {
        if self.remaining() < n {
            Err(DecodeError::UnexpectedEof {
                needed: n,
                offset: self.pos,
                remaining: self.remaining(),
            })
        } else {
            Ok(())
        }
    }

    /// One byte (a `Byte1` type tag or an `Int8`).
    pub fn byte1(&mut self) -> Result<u8, DecodeError> {
        self.need(1)?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// Big-endian `Int16`.
    pub fn int16(&mut self) -> Result<u16, DecodeError> {
        self.need(2)?;
        let arr: [u8; 2] = self.buf[self.pos..self.pos + 2].try_into().unwrap();
        self.pos += 2;
        Ok(u16::from_be_bytes(arr))
    }

    /// Big-endian `Int32` (OID / xid).
    pub fn int32(&mut self) -> Result<u32, DecodeError> {
        self.need(4)?;
        let arr: [u8; 4] = self.buf[self.pos..self.pos + 4].try_into().unwrap();
        self.pos += 4;
        Ok(u32::from_be_bytes(arr))
    }

    /// Big-endian `Int64` (the raw 8-byte field; LSN reads it as unsigned via [`Reader::lsn`],
    /// commit timestamps keep it as signed µs).
    pub fn int64(&mut self) -> Result<i64, DecodeError> {
        self.need(8)?;
        let arr: [u8; 8] = self.buf[self.pos..self.pos + 8].try_into().unwrap();
        self.pos += 8;
        Ok(i64::from_be_bytes(arr))
    }

    /// A null-terminated UTF-8 `String`. A missing terminator is `UnexpectedEof`, not a panic.
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
                offset: start,
                remaining: self.remaining(),
            }),
        }
    }

    /// Copy `n` bytes, advancing the cursor (for TOAST-safe value copies from PR 2.4).
    pub fn take(&mut self, n: usize) -> Result<Bytes, DecodeError> {
        self.need(n)?;
        let out = Bytes::copy_from_slice(&self.buf[self.pos..self.pos + n]);
        self.pos += n;
        Ok(out)
    }

    /// An LSN: an unsigned 8-byte value, wrapped into the `common::Lsn` newtype.
    pub fn lsn(&mut self) -> Result<Lsn, DecodeError> {
        Ok(Lsn::new(self.int64()? as u64))
    }
}
