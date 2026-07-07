# PR 2.4 — `TupleData` (`n`/`u`/`t`/`b`) and `Insert`

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/24

> **Phase:** 2 — walrus-pg-sink (2a: the hand-rolled decoder) · **Crates touched:** `pg-sink` ·
> **Est. size:** S · **Depends on:** PR 2.3 · **Unlocks:** PR 2.5

The first row values. A `TupleData` is `Int16 column-count` then, per column, a one-byte format tag:
`n` (SQL NULL), `u` (**unchanged TOAST** — value *not on the wire*), `t` (text: Int32 length + bytes),
`b` (binary: Int32 length + bytes). This PR writes the shared `parse_tuple` that every DML message
reuses, mapping each column to a `common::TupleValue`, and the `Insert ('I')` message (relation OID,
the `'N'` marker, then the new tuple). It lights up `insert` and `insert_generated_column_omitted`.

## Why — learning objectives

- **The four-way format tag → `TupleValue`** — and why `Null` and `UnchangedToast` must be *distinct*
  variants (one is a real NULL; the other is "absent, recover it downstream" — §5).
- **Length-prefixed byte slices** — `t`/`b` read an `Int32` length then exactly that many bytes; a wrong
  length silently misaligns the *next* column, so bounds-checking is correctness, not paranoia.
- **Sharing a sub-parser** — `parse_tuple` is written once and consumed by Insert (2.4),
  Update/Delete (2.5); getting it right here pays off five times.

## Read first

- `../../proto-version.md` §5 "TupleData and the unchanged-TOAST placeholder" — the format-byte table
  and why `u` carries no value.
- `../../proto-version.md` §4 "Insert `'I'`" — the `'I'` → rel OID → `'N'` → TupleData layout.
- `../../examples/proto-version/decode_pgoutput.py` — `parse_tuple` and the `I` arm.
- `../../examples/proto-version/test_decode_pgoutput.py` — `test_generated_stored_column_is_omitted`
  (the `insert_generated_column_omitted` vector).

## Scope

**In scope**

- `parse_tuple(reader) -> Result<Vec<TupleValue>, DecodeError>` handling `n`/`u`/`t`/`b`; an unknown tag
  is `BadTupleFormat`.
- `Message::Insert { xid, relation_oid, new }`; consume the mandatory `'N'` marker.
- `t`/`b` value bytes kept as `bytes::Bytes` (zero-copy-ish, cheap clone).

**Explicitly deferred** (do *not* build these here)

- Old-image tuples (`K`/`O`) and the marker-optional logic → **PR 2.5**.
- Interpreting `t` bytes per column type (numeric text, enum label, …) → `pg-to-arrow`, **PR 2.10+**.

## Files to create / modify

```
crates/pg-sink/src/pgoutput/mod.rs        # + parse_tuple, Message::Insert, 'I' arm
crates/pg-sink/tests/pgoutput_vectors.rs  # + insert tests + NULL-vs-value + generated-omitted
```

## Skeleton

```rust
// crates/pg-sink/src/pgoutput/mod.rs  (additions)
use common::TupleValue;   // Null | UnchangedToast | Text(Bytes) | Binary(Bytes)

/// TupleData: Int16 ncols, then per column a format byte + optional Int32-length-prefixed value.
///   'n' → Null · 'u' → UnchangedToast · 't' → Text(bytes) · 'b' → Binary(bytes)
/// An unexpected byte means the cursor misaligned → `BadTupleFormat` (fail loud, don't guess).
pub fn parse_tuple(reader: &mut Reader<'_>) -> Result<Vec<TupleValue>, DecodeError> {
    let ncols = reader.int16()?;
    let mut cols = Vec::with_capacity(ncols as usize);
    for _ in 0..ncols {
        let fmt = reader.byte1()?;
        let value = match fmt {
            b'n' => todo!("TupleValue::Null"),
            b'u' => todo!("TupleValue::UnchangedToast"),
            b't' => { let len = reader.int32()? as usize; todo!("Text(reader.take(len)?)") }
            b'b' => { let len = reader.int32()? as usize; todo!("Binary(reader.take(len)?)") }
            other => return Err(DecodeError::BadTupleFormat { byte: other }),
        };
        cols.push(value);
    }
    Ok(cols)
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    // … 2.2/2.3 variants …
    /// 'I': Int32 relation OID, Byte1('N'), then the new TupleData.
    Insert { xid: Option<u32>, relation_oid: u32, new: Vec<TupleValue> },
}

// 'I' arm shape:
//   let relation_oid = reader.int32()?;
//   let n = reader.byte1()?;            // must be b'N'
//   let new = parse_tuple(reader)?;
```

```rust
// crates/pg-sink/tests/pgoutput_vectors.rs  (additions)
#[test]
fn insert_vectors_render() {
    for name in ["insert", "insert_generated_column_omitted"] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn insert_new_tuple_has_no_null_or_toast_here() {
    // orders insert: all 5 columns are Text; none Null/UnchangedToast.
    todo!()
}

#[test]
fn generated_stored_column_is_omitted_from_tuple() {
    // items insert has 3 columns (id,label,qty) — the GENERATED STORED col is absent.
    todo!()
}
```

## Definition of Done

- [x] `parse_tuple` maps every format byte to the right `TupleValue`; `t`/`b` consume exactly their
      length-prefixed bytes; an unknown byte returns `BadTupleFormat` (no panic, no silent skip).
- [x] `insert` renders to `INSERT        rel=16397 new=['1', 'new', '19.99', 'happy', 'first']`.
- [x] `insert_generated_column_omitted` renders with exactly 3 tuple columns.
- [x] `TupleValue::Null` and `TupleValue::UnchangedToast` are constructed for `n`/`u` and are `!=` each
      other (the distinction proven fully in 2.5's `unchanged_toast_update`).
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (workspace stays green)

## Hints & gotchas

- **`u` reads no length and no bytes.** It is one byte total. Reading a length after `u` is the classic
  misalignment bug — the very next column's tag byte gets eaten and you spiral into `BadTupleFormat`
  three columns later, far from the real cause.
- **Keep `t` as bytes, not `String`, in the decoder.** pgoutput's `t` is the *text representation* of the
  value, but interpreting it (utf-8? numeric? enum label?) is the type layer's job. `TupleValue::Text`
  holding `Bytes` keeps the decoder honest and lossless; the render helper decodes utf-8 for display only.
- The `'N'` after the relation OID in `Insert` is a fixed marker — assert it's `b'N'` (a mismatch means a
  framing error upstream); do not treat it as tuple data.
- `Vec::with_capacity(ncols)` is safe here because `ncols` is bounded by the message length, but never
  pre-allocate on an *unvalidated* length in the length-prefixed `t`/`b` path — let `take` bounds-check.

## References

- Design: `../../proto-version.md` §4, §5.
- Prev: [PR 2.3](pr-2.3-pgoutput-relation-type.md) ·
  Next: [PR 2.5](pr-2.5-pgoutput-update-delete.md) · [Roadmap](../README.md)
