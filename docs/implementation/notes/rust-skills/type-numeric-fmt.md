# Numeric format-specifier parity for the integer newtypes

> **Status:** applied to the domain ids; the remaining integer newtypes are argued exclusions.

## What the rule asks for

`type-numeric-fmt` (C-NUM-FMT) says a newtype over a primitive integer should still answer `{:x}`,
`{:X}`, `{:o}`, and `{:b}`, forwarding the whole formatter to the inner value so `#`, `0`, and
width keep working. `Octal`/`Binary` may be skipped when the domain has no base-8 or base-2
reading; hex is expected for mask, address, and identifier types.

## What was already satisfied

`Lsn` (`crates/common/src/lsn.rs`) — the one genuine address type in the tree — already implements
all four traits by forwarding to the inner `u64`, with `Display` deliberately kept as the canonical
zero-padded 16-hex walrus rendering. `crates/common/src/lsn_test.rs` pins both.

## What the audit changed

`crates/common/src/ids.rs` — `ManifestId`, `EpochNo`, `SchemaVersionNo`, `ReloadId`, and `DdlId`:

- **`Display` now forwards the formatter** (`fmt::Display::fmt(&self.0, f)`) instead of
  re-`write!`ing the value. The old body silently discarded every flag, so `{:>6}` on a
  `ManifestId` printed unpadded. No call site in the tree formats these types with a flag, so no
  rendered output moves; the fix removes a latent trap rather than changing behaviour.
- **`LowerHex`/`UpperHex` added**, forwarding likewise. These types replaced bare `i64`s that
  already answered `{:x}`; wrapping them should not have cost the specifier.
- **`Octal`/`Binary` deliberately omitted** — the rule's own carve-out. A `bigserial` primary key, a
  generation counter, and a schema version have no base-8 or base-2 reading. `Lsn` keeps all four
  because a WAL byte position does.

Tests in `crates/common/src/ids_test.rs` cover flag forwarding, the hex specifiers, and the
two's-complement rendering of a negative id (the bare-`i64` behaviour, recorded so it is a known
consequence of forwarding).

## What deliberately gets no numeric formatting

- **`pg_sink::memory::TableId(u32)`** — a relation OID, and an identifier by the rule's wording, but
  it has no `Display` at all: it exists only as a `HashMap`/`BinaryHeap` key and a `ShedAction`
  payload, all read through the derived `Debug`, and it is never formatted anywhere in the tree.
  Giving it `{:x}` while `{}` still fails to compile would be a worse API than either end, and
  adding `Display` belongs to `type-display-vs-debug`, which explicitly declined to change any
  `Display` behaviour. Postgres also renders OIDs in decimal, so hex would name nothing.
- **`loader::health::InvalidPhase(u8)`, `pg_sink::health::InvalidPhase(u8)`, and
  `common::error::UnknownExitCode(i32)`** — error payloads, not numeric values. Their user-facing
  text is a sentence about a rejected byte or code, not the integer, so the format family does not
  apply.
- **`pg_sink::memory::Ratio(f64)`** — floats have no `LowerHex`/`Octal`/`Binary` to forward to.
- **The pgoutput wire scalars** (relation/type OIDs, `xid`) stay bare integers and already support
  every specifier; see `type-newtype-ids.md` for why they are not newtypes yet.

## What would reverse this

Give `TableId` `Display` plus the two hex traits together the first time an OID is formatted into a
log line or error message. Add `Octal`/`Binary` to an id only if one of them ever becomes a packed
bitfield rather than a counter or a row handle.
