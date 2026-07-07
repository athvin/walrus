//! Hand-rolled pgoutput (`proto_version` 2) decoder. Sync + pure: no tokio, no I/O.
//!
//! The logic arrives family-by-family in PRs 2.2–2.8, TDD-style against the golden vectors in
//! `tests/pgoutput_vectors.rs`. PR 2.2 lands the [`Reader`] primitives, stream framing, and the
//! transaction-boundary messages — Begin / Commit / Origin.

pub mod error;
pub mod reader;
pub mod typmod;

pub use error::DecodeError;
pub use reader::Reader;

use bytes::Bytes;
use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity, TupleValue};

/// Message types that carry a 4-byte per-message xid immediately after the tag — but ONLY inside a
/// streamed block (proto §7): Relation, Type, Insert, Update, Delete, Truncate, Message.
const XID_PREFIXED: &[u8] = b"RYIUDTM";

/// Whether we are inside a Stream Start..Stop block. The per-message xid prefix (proto §7) exists
/// **only** while this is true; Stream Start/Stop toggle it (from PR 2.7). It is threaded through
/// [`parse_stream`] so context carries across messages.
#[derive(Debug, Default, Clone, Copy)]
pub struct StreamCtx {
    pub in_stream: bool,
}

/// The old-image submessage tag: `'K'` = key columns only (DEFAULT identity), `'O'` = the whole old
/// row (FULL identity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldTupleKind {
    Key,
    Full,
}

/// One decoded pgoutput message. Variants are added family-by-family in PRs 2.2–2.8:
/// 2.3 Relation/Type; 2.4 Insert; 2.5 Update/Delete; 2.6 Truncate/Message; 2.7 Stream*;
/// 2.8 the two-phase family.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    /// `'B'`: Int64 final LSN, Int64 commit ts (µs since 2000-01-01), Int32 xid.
    Begin {
        final_lsn: Lsn,
        commit_ts: i64,
        xid: u32,
    },
    /// `'C'`: Int8 flags, Int64 commit LSN, Int64 end LSN, Int64 commit ts.
    Commit {
        flags: u8,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_ts: i64,
    },
    /// `'O'`: Int64 commit LSN, String origin name.
    Origin { commit_lsn: Lsn, name: String },
    /// `'R'`: the table shape (OID, namespace, name, replica identity, columns). `xid` is `Some`
    /// only inside a streamed block.
    Relation {
        xid: Option<u32>,
        relation: PgRelation,
    },
    /// `'Y'`: a non-builtin type announcement (e.g. our `mood` enum).
    Type {
        xid: Option<u32>,
        oid: u32,
        namespace: String,
        name: String,
    },
    /// `'I'`: Int32 relation OID, `Byte1('N')`, then the new TupleData.
    Insert {
        xid: Option<u32>,
        relation_oid: u32,
        new: Vec<TupleValue>,
    },
    /// `'U'`: rel OID, then EITHER a (`'K'`|`'O'`) old tuple + `'N'`, OR straight to `'N'` (no old
    /// image — a non-key UPDATE under DEFAULT identity), then the new tuple.
    Update {
        xid: Option<u32>,
        relation_oid: u32,
        old_kind: Option<OldTupleKind>,
        old: Option<Vec<TupleValue>>,
        new: Vec<TupleValue>,
    },
    /// `'D'`: rel OID, then a (`'K'`|`'O'`) old tuple (always present — how the loader locates the
    /// row to remove).
    Delete {
        xid: Option<u32>,
        relation_oid: u32,
        old_kind: OldTupleKind,
        old: Vec<TupleValue>,
    },
    /// `'T'`: Int32 rel-count, Int8 option bits (`1`=CASCADE, `2`=RESTART IDENTITY), then the rel
    /// OIDs. Carries **no tuple / no PK** — handled as a separate wipe step downstream (loader §5.5).
    Truncate {
        xid: Option<u32>,
        cascade: bool,
        restart_identity: bool,
        relations: Vec<u32>,
    },
    /// `'M'`: Int8 flags (bit 1 = transactional), Int64 message LSN, String prefix, Int32 content
    /// length + content bytes. A non-transactional message is emitted immediately, even ahead of
    /// its transaction's Begin (used for the idle heartbeat).
    Message {
        xid: Option<u32>,
        transactional: bool,
        lsn: Lsn,
        prefix: String,
        content: Bytes,
    },
    /// `'S'`: Int32 **top-level** xid, Int8 first-segment flag (1 = first block for this xid).
    /// Opens a streamed block — sets `ctx.in_stream`.
    StreamStart { xid: u32, first_segment: bool },
    /// `'E'`: no payload. Closes the current streamed block — clears `ctx.in_stream`.
    StreamStop,
    /// `'c'`: Int32 xid, Int8 flags, Int64 commit LSN, Int64 end LSN, Int64 commit ts.
    StreamCommit {
        xid: u32,
        flags: u8,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_ts: i64,
    },
    /// `'A'`: Int32 top xid, Int32 sub xid. Under `streaming 'on'` there are **no** trailing LSN/ts
    /// fields (those exist only under `streaming 'parallel'`, v4, which walrus never enables).
    /// `sub != top` is a rolled-back savepoint inside a *committing* transaction (§9b).
    StreamAbort { top_xid: u32, sub_xid: u32 },

    // ---- two-phase (v3) frames. walrus runs at v2 and never enables `two_phase`, so it NEVER sees
    // these in production — but the decoder must still parse them without misaligning the cursor, so
    // a stray byte fails loudly *at that byte* rather than silently corrupting the stream. There is
    // no runtime handling for these anywhere; the decoder is simply complete. ----
    /// `'b'`: Int64 prepare LSN, Int64 end LSN, Int64 prepare ts, Int32 xid, String gid.
    BeginPrepare {
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_ts: i64,
        xid: u32,
        gid: String,
    },
    /// `'P'`: Int8 flags, then prepare LSN, end LSN, prepare ts, xid, gid.
    Prepare {
        flags: u8,
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_ts: i64,
        xid: u32,
        gid: String,
    },
    /// `'K'`: Int8 flags, commit LSN, end LSN, commit ts, xid, gid. **Top-level message** — NOT the
    /// old-KEY submessage marker (that `'K'` is read only inside the Update/Delete arms; same byte,
    /// different parser position, no collision).
    CommitPrepared {
        flags: u8,
        commit_lsn: Lsn,
        end_lsn: Lsn,
        commit_ts: i64,
        xid: u32,
        gid: String,
    },
    /// `'r'`: Int8 flags, end LSN, rollback end LSN, prepare ts, rollback ts, xid, gid.
    RollbackPrepared {
        flags: u8,
        end_lsn: Lsn,
        rollback_end_lsn: Lsn,
        prepare_ts: i64,
        rollback_ts: i64,
        xid: u32,
        gid: String,
    },
    /// `'p'`: the streamed variant of Prepare (same fields as `'P'`).
    StreamPrepare {
        flags: u8,
        prepare_lsn: Lsn,
        end_lsn: Lsn,
        prepare_ts: i64,
        xid: u32,
        gid: String,
    },
}

impl Message {
    /// For a [`Message::StreamAbort`]: `Some(true)` when the WHOLE transaction aborted (`top == sub`,
    /// §9a — drop everything), `Some(false)` for a rolled-back savepoint inside a committing txn
    /// (`top != sub`, §9b — discard only the sub-xid's rows). `None` for any other message.
    pub fn is_whole_txn_abort(&self) -> Option<bool> {
        match self {
            Message::StreamAbort { top_xid, sub_xid } => Some(top_xid == sub_xid),
            _ => None,
        }
    }
}

/// Consume the fixed `'N'` marker that precedes a new tuple; a mismatch is an upstream framing
/// error (a misaligned parse).
fn expect_n(reader: &mut Reader<'_>) -> Result<(), DecodeError> {
    let b = reader.byte1()?;
    if b != b'N' {
        return Err(DecodeError::BadTupleFormat { byte: b });
    }
    Ok(())
}

/// Decode a `TupleData`: `Int16` column-count, then per column a one-byte format tag —
/// `'n'` → [`TupleValue::Null`], `'u'` → [`TupleValue::UnchangedToast`] (value **not** on the
/// wire), `'t'` → [`TupleValue::Text`] (Int32 length + UTF-8 bytes), `'b'` →
/// [`TupleValue::Binary`] (Int32 length + bytes). An unexpected tag means the cursor misaligned →
/// [`DecodeError::BadTupleFormat`] (fail loud, never guess). Shared by Insert/Update/Delete.
pub fn parse_tuple(reader: &mut Reader<'_>) -> Result<Vec<TupleValue>, DecodeError> {
    let ncols = reader.int16()?;
    let mut cols = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        let value = match reader.byte1()? {
            b'n' => TupleValue::Null,
            b'u' => TupleValue::UnchangedToast, // one byte total — no length, no value
            b't' => {
                let len = reader.int32()? as usize;
                let bytes = reader.take(len)?;
                // `t` is the value's *text* representation; interpreting it (numeric? enum label?)
                // is the type layer's job (pg-to-arrow). Here we only require valid UTF-8.
                TupleValue::Text(std::str::from_utf8(&bytes)?.to_string())
            }
            b'b' => {
                let len = reader.int32()? as usize;
                TupleValue::Binary(reader.take(len)?)
            }
            other => return Err(DecodeError::BadTupleFormat { byte: other }),
        };
        cols.push(value);
    }
    Ok(cols)
}

/// Parse one message off `reader` (advancing it), for use by [`parse_stream`]. Stream context is
/// consulted from PR 2.3 onward (the xid prefix); Begin/Commit/Origin are never xid-prefixed —
/// they *are* the transaction frame.
fn parse_one(reader: &mut Reader<'_>, ctx: &mut StreamCtx) -> Result<Message, DecodeError> {
    let tag = reader.byte1()?;
    // The per-message (sub-transaction) xid prefix exists only while streaming (proto §7/§9b). The
    // same bytes therefore parse differently in vs. out of a stream. Begin/Commit/Origin are the
    // txn frame itself and are never prefixed.
    let xid = if XID_PREFIXED.contains(&tag) && ctx.in_stream {
        Some(reader.int32()?)
    } else {
        None
    };
    match tag {
        b'B' => Ok(Message::Begin {
            final_lsn: reader.lsn()?,
            commit_ts: reader.int64()?,
            xid: reader.int32()?,
        }),
        b'C' => Ok(Message::Commit {
            flags: reader.byte1()?,
            commit_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            commit_ts: reader.int64()?,
        }),
        b'O' => Ok(Message::Origin {
            commit_lsn: reader.lsn()?,
            name: reader.string()?,
        }),
        b'R' => {
            let oid = reader.int32()?;
            let schema = reader.string()?;
            let name = reader.string()?;
            let ident_byte = reader.byte1()?;
            let replica_identity = ReplicaIdentity::from_wire(ident_byte)
                .map_err(|_| DecodeError::BadReplicaIdentity { byte: ident_byte })?;
            let ncols = reader.int16()?;
            let mut columns = Vec::with_capacity(ncols as usize);
            for _ in 0..ncols {
                let flags = reader.byte1()?;
                let col_name = reader.string()?;
                let type_oid = reader.int32()?;
                let type_modifier = typmod::atttypmod(reader.int32()?);
                columns.push(PgColumn {
                    name: col_name,
                    type_oid,
                    type_modifier,
                    is_key: flags & 1 != 0,
                });
            }
            Ok(Message::Relation {
                xid,
                relation: PgRelation {
                    oid,
                    schema,
                    name,
                    replica_identity,
                    columns,
                },
            })
        }
        b'Y' => Ok(Message::Type {
            xid,
            oid: reader.int32()?,
            namespace: reader.string()?,
            name: reader.string()?,
        }),
        b'I' => {
            let relation_oid = reader.int32()?;
            // A fixed `'N'` marker precedes the new tuple; a mismatch is an upstream framing error.
            let marker = reader.byte1()?;
            if marker != b'N' {
                return Err(DecodeError::BadTupleFormat { byte: marker });
            }
            Ok(Message::Insert {
                xid,
                relation_oid,
                new: parse_tuple(reader)?,
            })
        }
        b'U' => {
            let relation_oid = reader.int32()?;
            // Branch on the byte AFTER the OID: 'K'/'O' → an old image (then a 'N' before the new
            // tuple); 'N' → no old image, and the 'N' we just read IS the new-tuple marker.
            let (old_kind, old) = match reader.byte1()? {
                b'K' => {
                    let old = parse_tuple(reader)?;
                    expect_n(reader)?;
                    (Some(OldTupleKind::Key), Some(old))
                }
                b'O' => {
                    let old = parse_tuple(reader)?;
                    expect_n(reader)?;
                    (Some(OldTupleKind::Full), Some(old))
                }
                b'N' => (None, None),
                other => return Err(DecodeError::BadTupleFormat { byte: other }),
            };
            Ok(Message::Update {
                xid,
                relation_oid,
                old_kind,
                old,
                new: parse_tuple(reader)?,
            })
        }
        b'D' => {
            let relation_oid = reader.int32()?;
            let old_kind = match reader.byte1()? {
                b'K' => OldTupleKind::Key,
                b'O' => OldTupleKind::Full,
                other => return Err(DecodeError::BadTupleFormat { byte: other }),
            };
            Ok(Message::Delete {
                xid,
                relation_oid,
                old_kind,
                old: parse_tuple(reader)?,
            })
        }
        b'T' => {
            let nrel = reader.int32()? as usize;
            let opt = reader.byte1()?;
            // Fixed-count array: the count IS the length; no per-element framing, no tuple.
            let relations = (0..nrel)
                .map(|_| reader.int32())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Message::Truncate {
                xid,
                cascade: opt & 1 != 0,
                restart_identity: opt & 2 != 0,
                relations,
            })
        }
        b'M' => {
            let flags = reader.byte1()?;
            let lsn = reader.lsn()?;
            let prefix = reader.string()?;
            let len = reader.int32()? as usize;
            let content = reader.take(len)?;
            Ok(Message::Message {
                xid,
                transactional: flags & 1 != 0,
                lsn,
                prefix,
                content,
            })
        }
        b'S' => {
            let start_xid = reader.int32()?;
            let first_segment = reader.byte1()? != 0;
            ctx.in_stream = true; // opens the streamed block — the next change reads a sub-xid prefix
            Ok(Message::StreamStart {
                xid: start_xid,
                first_segment,
            })
        }
        b'E' => {
            ctx.in_stream = false; // closes the block
            Ok(Message::StreamStop)
        }
        b'c' => Ok(Message::StreamCommit {
            xid: reader.int32()?,
            flags: reader.byte1()?,
            commit_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            commit_ts: reader.int64()?,
        }),
        b'A' => Ok(Message::StreamAbort {
            top_xid: reader.int32()?,
            sub_xid: reader.int32()?,
        }),
        // Two-phase (v3). Field orders differ per message — cross-checked against the reference
        // decoder. `'K'` here is Commit Prepared (top-level), distinct from the old-KEY marker.
        b'b' => Ok(Message::BeginPrepare {
            prepare_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            prepare_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        b'P' => Ok(Message::Prepare {
            flags: reader.byte1()?,
            prepare_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            prepare_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        b'K' => Ok(Message::CommitPrepared {
            flags: reader.byte1()?,
            commit_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            commit_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        b'r' => Ok(Message::RollbackPrepared {
            flags: reader.byte1()?,
            end_lsn: reader.lsn()?,
            rollback_end_lsn: reader.lsn()?,
            prepare_ts: reader.int64()?,
            rollback_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        b'p' => Ok(Message::StreamPrepare {
            flags: reader.byte1()?,
            prepare_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            prepare_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        other => Err(DecodeError::UnknownMessage { byte: other }),
    }
}

/// Decode exactly one **complete** message from `reader`: parse one message, then reject any
/// trailing unconsumed bytes (a truncated or misaligned message).
pub fn parse_message(reader: &mut Reader<'_>, ctx: &mut StreamCtx) -> Result<Message, DecodeError> {
    let msg = parse_one(reader, ctx)?;
    let unconsumed = reader.remaining();
    if unconsumed != 0 {
        return Err(DecodeError::TrailingBytes { unconsumed });
    }
    Ok(msg)
}

/// Split a raw walsender byte stream into messages, skipping the single `0x0a` that
/// `pg_recvlogical` inserts between self-delimiting messages, threading `ctx` across them.
pub fn parse_stream(data: &[u8], ctx: &mut StreamCtx) -> Result<Vec<Message>, DecodeError> {
    let mut reader = Reader::new(data);
    let mut out = Vec::new();
    while reader.remaining() > 0 {
        // Skip exactly one separator, and only at a message boundary (the top of the loop), so a
        // `0x0a` byte *inside* a value is never mistaken for a separator.
        if reader.peek() == Some(0x0a) {
            reader.byte1()?;
            continue;
        }
        out.push(parse_one(&mut reader, ctx)?);
    }
    Ok(out)
}
