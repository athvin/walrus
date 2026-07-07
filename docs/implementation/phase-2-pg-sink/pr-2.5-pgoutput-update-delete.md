# PR 2.5 — `Update` and `Delete` (the `K`/`O` old image; NULL vs unchanged-TOAST)

> **Phase:** 2 — walrus-pg-sink (2a: the hand-rolled decoder) · **Crates touched:** `pg-sink` ·
> **Est. size:** M · **Depends on:** PR 2.4 · **Unlocks:** PR 2.6

The mutations, and the subtlest REPLICA IDENTITY behaviour in the catalog. `Update ('U')` *optionally*
carries an old image — a `'K'` (key columns only) or `'O'` (the whole old row) submessage — followed by
a `'N'` and the new tuple. `Delete ('D')` always carries a `'K'` or `'O'`. The optional-marker logic is
where a hand-rolled decoder earns its keep: a non-key `UPDATE` under DEFAULT identity carries **no old
image at all**, so the byte after the relation OID is `'N'`, not `'K'`. This PR completes DML decoding
and pins the NULL-vs-unchanged-TOAST distinction. It lights up every `update_*`/`delete_*` vector plus
`unchanged_toast_update`.

## Why — learning objectives

- **Marker-optional parsing** — peek the byte after the relation OID: `K`/`O` → old image + a following
  `N`; `N` → straight to the new tuple, `old = None`. Mis-branch here and you either lose the old key or
  misalign the whole tuple.
- **REPLICA IDENTITY semantics in code** — DEFAULT ships key columns *only if a key changed*; FULL ships
  the entire old row; the decoder faithfully reflects whichever the source sent (§6).
- **NULL ≠ unchanged-TOAST, proven** — the same update carries a real `n` and a `u` in different columns;
  the loader's back-scan (PR 3.6) depends on them staying distinct.

## Read first

- `../../proto-version.md` §4 "Update `'U'`" + "Delete `'D'`" — the `K`/`O`/`N` submessage layouts.
- `../../proto-version.md` §6 "REPLICA IDENTITY" — the old-image matrix (DEFAULT key-only-on-key-change
  vs FULL whole-row).
- `../../proto-version.md` §5 — the `u` placeholder, revisited for `unchanged_toast_update`.
- `../../examples/proto-version/test_decode_pgoutput.py` — `test_pk_changing_update_carries_key_only_old_image`,
  `test_default_identity_update_without_key_change_has_no_old_image`,
  `test_full_identity_carries_whole_old_row`, `test_composite_pk_old_image_carries_all_key_columns`,
  `test_unchanged_toast_value_absent_from_wire`, `test_null_vs_toast_are_distinct`.

## Scope

**In scope**

- `OldTupleKind { Key, Full }` (the `'K'`/`'O'` tag).
- `Message::Update { xid, relation_oid, old_kind, old, new }` with `old`/`old_kind` optional.
- `Message::Delete { xid, relation_oid, old_kind, old }` (old image mandatory).

**Explicitly deferred** (do *not* build these here)

- Recovering the `u` value (raw back-scan) → loader **PR 3.6**. Here it stays `UnchangedToast`.
- Using the old key to locate a row for `MERGE` → loader transform, **PR 3.3**.

## Files to create / modify

```
crates/pg-sink/src/pgoutput/mod.rs        # + OldTupleKind, Message::{Update,Delete}, 'U'/'D' arms
crates/pg-sink/tests/pgoutput_vectors.rs  # + update_*/delete_* tests + NULL-vs-TOAST + composite-pk
```

## Skeleton

```rust
// crates/pg-sink/src/pgoutput/mod.rs  (additions)

/// The old-image submessage tag: 'K' = key columns (DEFAULT identity), 'O' = whole old row (FULL).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldTupleKind { Key, Full }

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    // … 2.2–2.4 variants …
    /// 'U': rel OID, then EITHER ('K'|'O') old-tuple + 'N', OR straight to 'N' (no old image).
    Update {
        xid: Option<u32>,
        relation_oid: u32,
        old_kind: Option<OldTupleKind>,
        old: Option<Vec<TupleValue>>,
        new: Vec<TupleValue>,
    },
    /// 'D': rel OID, then ('K'|'O') old-tuple (always present).
    Delete {
        xid: Option<u32>,
        relation_oid: u32,
        old_kind: OldTupleKind,
        old: Vec<TupleValue>,
    },
}

// 'U' arm shape:
//   let relation_oid = reader.int32()?;
//   let marker = reader.byte1()?;
//   let (old_kind, old) = match marker {
//       b'K' => { let t = parse_tuple(reader)?; expect_n(reader)?; (Some(Key),  Some(t)) }
//       b'O' => { let t = parse_tuple(reader)?; expect_n(reader)?; (Some(Full), Some(t)) }
//       b'N' => (None, None),                       // non-key UPDATE, DEFAULT identity: no old image
//       other => return Err(DecodeError::BadTupleFormat { byte: other }),
//   };
//   let new = parse_tuple(reader)?;

// 'D' arm shape:
//   let relation_oid = reader.int32()?;
//   let old_kind = match reader.byte1()? { b'K' => Key, b'O' => Full, o => return Err(..) };
//   let old = parse_tuple(reader)?;
```

```rust
// crates/pg-sink/tests/pgoutput_vectors.rs  (additions)
#[test]
fn update_delete_vectors_render() {
    for name in ["update_pk_change", "delete_key", "update_no_key_change",
                 "update_full_identity", "delete_full_identity",
                 "update_composite_pk_partial", "unchanged_toast_update"] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn non_key_update_has_no_old_image() {
    // update_no_key_change: old_kind == None, old == None.
    todo!()
}

#[test]
fn pk_changing_update_carries_key_only_old_image() {
    // update_pk_change: old_kind == Key; old[0] == Text("1"); old[1..] all Null.
    todo!()
}

#[test]
fn full_identity_update_carries_whole_old_row() {
    // update_full_identity: old_kind == Full; old == ["7","w","1"].
    todo!()
}

#[test]
fn composite_pk_old_image_carries_all_key_columns() {
    // update_composite_pk_partial: only region changed; old == [Text("us"), Text("1"), Null].
    todo!()
}

#[test]
fn null_and_unchanged_toast_are_distinct() {
    // unchanged_toast_update: new[2] == Null, new[4] == UnchangedToast, and they differ.
    todo!()
}
```

## Definition of Done

- [ ] All seven vectors above render to their exact golden lines.
- [ ] `update_no_key_change` → `old_kind == None` **and** `old == None` (the byte after the OID was
      `'N'`, not `'K'`).
- [ ] `update_pk_change` → `old_kind == Some(Key)`, first old column `Text("1")`, remaining old columns
      `Null` (DEFAULT ships key columns only).
- [ ] `update_full_identity`/`delete_full_identity` → `old_kind == Full` with every column present.
- [ ] `update_composite_pk_partial` → both key columns present in the old image even though only one key
      changed; the non-key column is `Null`.
- [ ] `unchanged_toast_update` → a `Null` column and an `UnchangedToast` column that are **not equal**.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (workspace stays green)

## Hints & gotchas

- **The optional marker is the whole trick.** After the relation OID, branch on the *next byte*: `K`/`O`
  → parse old tuple **then consume a `'N'`** before the new tuple; `N` → no old image, the `'N'` you just
  read *is* the new-tuple marker (don't read another). Getting the "consume the `N`" step wrong shifts
  every subsequent field by one column.
- **`Delete` never omits its old image** — it's how the loader locates the row to remove. If you ever see
  `Delete` with `old_kind == None`, the source's REPLICA IDENTITY is misconfigured (`nothing`), which
  walrus forbids (§6, architecture §1.1). Model it as an error only if you want; the vectors never hit it.
- The old key under DEFAULT is **key columns only, rest NULL** — those `Null`s are structural padding, not
  data. Don't "helpfully" trim them; the loader keys on positional columns.
- Reuse `parse_tuple` untouched; do not fork a "key tuple" parser — `K`/`O`/`N` are all just `TupleData`.

## References

- Design: `../../proto-version.md` §4, §5, §6.
- Prev: [PR 2.4](pr-2.4-pgoutput-tuple-insert.md) ·
  Next: [PR 2.6](pr-2.6-pgoutput-truncate-message.md) · [Roadmap](../README.md)
