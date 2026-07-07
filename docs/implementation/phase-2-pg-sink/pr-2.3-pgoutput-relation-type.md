# PR 2.3 — `Relation` and `Type` (the xid prefix + `typmod` → `numeric(p,s)`)

> **Phase:** 2 — walrus-pg-sink (2a: the hand-rolled decoder) · **Crates touched:** `pg-sink` ·
> **Est. size:** M · **Depends on:** PR 2.2 · **Unlocks:** PR 2.4

The schema messages. `Relation ('R')` is the shape a table advertises before its first change: OID,
namespace, name, replica identity, and per-column `{key, name, type_oid, atttypmod}`. `Type ('Y')`
announces a non-builtin type (our `mood` enum) the first time it's referenced. This PR also introduces
the machinery every remaining change message needs: the **per-message xid prefix that appears only
inside a streamed block** (§7), and the `atttypmod` decode — including turning a `numeric`'s packed
`atttypmod` into `(precision, scale)`. It produces `common::PgRelation` and lights up all `relation_*`
and `type_enum` vectors.

## Why — learning objectives

- **Producing the neutral seam type** — the decoder builds `common::{PgRelation, PgColumn,
  ReplicaIdentity}`; `pg-to-arrow` will consume it (PR 2.9) with no dependency on this crate.
- **Context-sensitive parsing** — `XID_PREFIXED` message types read a 4-byte xid *only when*
  `ctx.in_stream` is set; the same bytes parse differently in vs. out of a stream (§7).
- **Bit-flag + sentinel decoding** — column flag bit 1 = key; `atttypmod == -1` (`0xFFFFFFFF`) means "no
  modifier"; `numeric` packs precision/scale as `((p << 16) | s) + 4`.
- **`relreplident` as an enum** — `'d'/'f'/'n'/'i'` → `ReplicaIdentity`, which later decides what old
  image `Update`/`Delete` carry (PR 2.5).

## Read first

- `../../proto-version.md` §4 "Relation `'R'`" + "Type `'Y'`" — the exact byte layouts and the column
  triple (flags/name/type OID/atttypmod).
- `../../proto-version.md` §6 "REPLICA IDENTITY" — what `'d'/'f'/'n'/'i'` mean (you store it now; it
  bites in 2.5).
- `../../proto-version.md` §7 — the "xid prefix only while streaming" rule you implement here.
- `../../walrus-pg-sink.md` §2.3 "The full type table" (`#23-the-full-type-table`) — the `numeric(p,s)`
  row: why precision/scale must be recovered from `atttypmod` now, not guessed later.
- `../../examples/proto-version/test_decode_pgoutput.py` — `test_numeric_typmod_encodes_precision_scale`,
  `test_relation_marks_key_columns`, `test_full_identity_relation_flags_every_column_key`.

## Scope

**In scope**

- The `XID_PREFIXED` set (`R Y I U D T M`) and `ctx`-gated xid read in `parse_message`.
- `Message::{Relation, Type}`, producing `common::PgRelation` / `PgColumn` / `ReplicaIdentity`.
- `atttypmod` decode: `-1` sentinel + a `numeric` precision/scale helper.

**Explicitly deferred** (do *not* build these here)

- Mapping any type OID → Arrow → that's `pg-to-arrow`, **PR 2.9+**. Here you only capture `type_oid` +
  `atttypmod` faithfully.
- `TupleData`/DML that *reference* a relation → **PR 2.4/2.5**.

## Files to create / modify

```
crates/pg-sink/src/pgoutput/mod.rs        # + XID_PREFIXED, xid-prefix read, Message::{Relation,Type}
crates/pg-sink/src/pgoutput/typmod.rs     # new — atttypmod helpers (numeric p,s)
crates/pg-sink/tests/pgoutput_vectors.rs  # + relation_*/type_enum tests + numeric-typmod + key-flag tests
```

## Skeleton

```rust
// crates/pg-sink/src/pgoutput/typmod.rs
/// Raw `atttypmod` where `-1` means "no modifier". `0xFFFFFFFF` on the wire → `-1`.
pub fn atttypmod(raw: u32) -> i32 { todo!() }

/// `numeric(p,s)` packs `((p << 16) | s) + 4` into atttypmod. `-1` → `None` (unconstrained).
/// e.g. numeric(10,2) → atttypmod 655366 → Some((10, 2)).
pub fn numeric_precision_scale(typmod: i32) -> Option<(u16, u16)> { todo!() }
```

```rust
// crates/pg-sink/src/pgoutput/mod.rs  (additions)
use common::{PgColumn, PgRelation, ReplicaIdentity};

/// Message types that carry a 4-byte xid immediately after the tag — but ONLY inside a
/// streamed block (§7). Relation, Type, Insert, Update, Delete, Truncate, Message.
const XID_PREFIXED: &[u8] = b"RYIUDTM";

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    // … 2.2 variants …
    /// 'R': the table shape. `xid` is Some only inside a stream.
    Relation { xid: Option<u32>, relation: PgRelation },
    /// 'Y': a non-builtin type announcement.
    Type { xid: Option<u32>, oid: u32, namespace: String, name: String },
}

// inside parse_message, right after reading `tag`:
//   let xid = if XID_PREFIXED.contains(&tag) && ctx.in_stream {
//       Some(reader.int32()?)   // the SUB-transaction xid (§7/§9b)
//   } else {
//       None
//   };
```

The decoder maps `'d'/'f'/'n'/'i'` → `ReplicaIdentity` (an unknown byte is `BadReplicaIdentity`), and
each column is `Int8 flags` (bit 1 = key) · `String name` · `Int32 type_oid` · `Int32 atttypmod`:

```rust
// per-column loop shape inside the 'R' arm:
// for _ in 0..ncols {
//     let flags = reader.byte1()?;
//     let name  = reader.string()?;
//     let type_oid = reader.int32()?;
//     let type_modifier = typmod::atttypmod(reader.int32()?);
//     cols.push(PgColumn { is_key: flags & 1 != 0, name, type_oid, type_modifier });
// }
```

```rust
// crates/pg-sink/tests/pgoutput_vectors.rs  (additions)
#[test]
fn relation_and_type_vectors_render() {
    for name in ["type_enum", "relation_orders", "relation_composite_pk",
                 "relation_full_identity_all_keys", "relation_after_add_column"] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn numeric_typmod_encodes_precision_scale() {
    // orders.amount is numeric(10,2): type_oid 1700, atttypmod 655366, (p,s) == (10,2).
    todo!()
}

#[test]
fn relation_marks_only_key_columns() { todo!() }         // orders: keys == ["id"], replident 'd'

#[test]
fn full_identity_flags_every_column_key() { todo!() }    // items: replident 'f', all cols key
```

## Definition of Done

- [ ] `type_enum`, `relation_orders`, `relation_composite_pk`, `relation_full_identity_all_keys`,
      `relation_after_add_column` all render to their golden lines.
- [ ] `atttypmod(0xFFFFFFFF) == -1`; `numeric_precision_scale(655366) == Some((10, 2))`;
      `numeric_precision_scale(-1) == None`.
- [ ] `relation_orders` → key columns are exactly `["id"]`, `replident == Default`.
- [ ] `relation_full_identity_all_keys` → `replident == Full` and every `PgColumn.is_key`.
- [ ] The xid prefix is consumed **only** when `ctx.in_stream`; a non-streamed `Relation`/`Type` has
      `xid == None` (proven directly once streaming lands in 2.7, but the branch exists now).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (workspace stays green)

## Hints & gotchas

- **`atttypmod` is signed once decoded.** On the wire it's an `Int32`; `0xFFFFFFFF` is the `-1` sentinel.
  Store `i32`, not `u32` — a raw `u32` comparison against `-1` will bite you.
- **`numeric` packing is `+4`, not raw.** `((p<<16)|s)+4`: subtract 4 *first*, then shift/mask. Guard the
  `-1`/`< 4` cases → `None`. Do not confuse this with the `Decimal128` vs `VARCHAR` tiering decision —
  that's `pg-to-arrow`'s job (2.9/2.15); here you only recover the numbers.
- **The xid you read is the sub-transaction xid** (§7 final paragraph, §9b). Naming it `xid` is fine, but
  remember `Stream Start` carries the *top-level* xid — the two differ, and that gap is the whole
  savepoint-rollback story in 2.7.
- Generated-always-stored columns simply **don't appear** in the Relation (`relation_after_add_column`
  and the `items` relation prove it) — do not synthesize them.
- Keep producing `common` types verbatim; resist adding Arrow/type-tier fields to `PgColumn` — the seam's
  value is that `pg-to-arrow` unit-tests against hand-built `PgRelation`s with no decoder in sight.

## References

- Design: `../../proto-version.md` §4, §6, §7; `../../walrus-pg-sink.md` §2.3.
- Prev: [PR 2.2](pr-2.2-pgoutput-reader-framing-begin-commit.md) ·
  Next: [PR 2.4](pr-2.4-pgoutput-tuple-insert.md) · [Roadmap](../README.md)
