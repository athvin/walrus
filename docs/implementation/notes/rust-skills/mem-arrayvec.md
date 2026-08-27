# ArrayVec has no hard-capacity home in walrus (PR 11.14; re-audited repo-wide)

> **Status:** decided 2026-08-13 — declined, structurally; re-audited 2026-08-27 against the raw
> rule and still declined. No `arrayvec` dependency. The re-audit did find one exact compile-time
> width (the standby-status frame) and gave it a plain `[u8; N]` stack array — std, not the crate.

## What the rule asks

`ArrayVec<T, N>` is correct only when the domain guarantees a hard `N`. Exceeding it cannot spill to
the heap; it rejects or panics. A typical observed length or a server configuration limit is not a
protocol guarantee.

## The one exact width, and why it is a std array

`build_standby_status` (`crates/pg-sink/src/replication.rs:517`) builds the `'r'` feedback frame:
CopyData tag + self-inclusive Int32 length + a 34-byte payload of one tag byte, three LSNs, one
timestamp, and one reply byte. Every field is fixed by the protocol, so the frame is **exactly** 39
bytes — not "at most". It used to be two heap `Vec`s per feedback send (a 34-byte payload, then the
framed copy of it); it is now `[u8; STANDBY_STATUS_FRAME_BYTES]`, allocating nothing.

`ArrayVec<u8, 39>` would be strictly worse here than the array: an exact width has no length to
track apart from the type, no `try_push` failure mode to handle, and no capacity/len distinction to
get wrong — and it would cost a dependency. `copy_done` (`replication.rs:287`) already wrote its
bare `'c'` frame as an array literal; this brings the sibling builder in line.
`replication_test::standby_status_frame_layout` reads every offset back and checks the wire length
against the frame width, so the `38` length field cannot drift from the array.

## Why nothing else has an `N`

| candidate | length driven by | compile-time bound? |
|---|---|---|
| `parse_tuple` → `Vec<TupleValue>` (`crates/pg-sink/src/pgoutput/mod.rs:258`) | pgoutput's `Int16` TupleData count (`reader.rs:86` returns `u16`) | No; 1,600 is server policy and smaller values reject valid frames |
| `build_startup` / `send_query` (`replication.rs:484`, `:463`) | user, database, and SQL text | No |
| `PgRelation.columns` (`crates/common/src/pg_shape.rs:106`) | user schema | No |
| per-commit `Vec<SealedBatch>` (`crates/pg-sink/src/consume.rs:912`) | transaction rows and configured batching | No |

The arithmetic that kills `parse_tuple`: PR 11.2's layout ratchet caps `TupleValue` at 40 B
(`crates/common/src/pg_shape.rs:155`), so an inline capacity matching PostgreSQL's current
1,600-column ceiling would reserve **64,000 B** of stack for every tuple, and matching the wire's
full `u16` range would be dramatically larger. `Vec::with_capacity(usize::from(ncols))` is one
heap allocation right-sized from the wire count, with no inline slab, spill branch, or artificial
runtime rejection.

## Fixed arity that still keeps its `Vec`

- `parse_line` (`crates/pg-to-arrow/src/geometric.rs:199`) collects exactly three `{A,B,C}`
  coefficients. Genuine fixed arity, but the `Vec` is immediately matched through `as_slice()` and
  dropped, the shape is off every benched shape (`crates/pg-to-arrow/benches/batch.rs:127`), and
  `mem-smallvec.md`'s reversal condition commits this site to "no iterator or inline-vector rewrite"
  until a profile says otherwise. A dependency to save one three-pointer allocation is not that
  profile.
- `hms_to_micros`'s fractional-seconds buffer (`crates/pg-to-arrow/src/tier2.rs:102`) is a ≤ 6-char
  `String` — textbook `ArrayString<6>` shape, and the only candidate on a benched path
  (`arrow/append_row/tier2_fanout`). Still unmeasured, and any hand-rolled byte buffer would have to
  reproduce `i64::from_str`'s acceptance of a leading `+` and its rejection of non-ASCII exactly.
  Left alone pending the same measurement.
- `Lsn`'s `Display` (`crates/common/src/lsn.rs:197`) always writes 16 uppercase hex digits, so
  `ArrayString<16>` is structurally possible. It buys nothing: interpolation sites write straight
  through the `Formatter`, and the two production `to_string()` callers
  (`crates/loader/src/transform.rs:274`, `crates/loader/src/phase_a.rs:230`) render whole SQL
  statements and manifest fields out of dozens of `format!` allocations. PR 11.16 already deferred
  the small-string question on measurement (`mem-compact-string.md`).

## Revisit triggers

Reconsider the `arrayvec` dependency only when a protocol or physical invariant supplies a small
compile-time maximum that is a genuine *range* — a bound where the length still varies, so a plain
array will not do — and exceeding it is impossible by construction. An exact width is an array's
job. A benchmark alone cannot create that invariant; for the two measurable candidates above, the
route is `mem-smallvec.md`'s reversal condition.
