# PR 0.3 — Add the `Lsn` newtype (parse, zero-padded Display, numeric `Ord`)

> **Phase:** 0 — Foundations & CI · **Crates touched:** `common` · **Est. size:** S ·
> **Depends on:** PR 0.2 · **Unlocks:** PR 0.4

Every watermark, manifest bound, and provenance field in walrus is a **Log Sequence Number**.
Postgres shows LSNs two ways — the human `X/Y` (e.g. `0/199BAC8`) and, in walrus's own JSON/control
tables, a **zero-padded 16-hex** string (`00000000019A2B3C`) chosen precisely so a *text* sort equals
a *numeric* sort. This PR makes that a type: a `u64` newtype that parses both forms, prints the padded
form, orders numerically, and serialises as the padded string. Get this wrong and the whole
commit-LSN ordering contract silently breaks.

## Why — learning objectives

By the end of this PR you will have practised:

- **Newtypes over primitives** — wrapping `u64` so an LSN can never be confused with any other number.
- **`FromStr` + `Display` round-tripping** — parsing two input dialects, emitting one canonical form.
- **Deriving vs hand-writing `Ord`** — a `derive(Ord)` on the inner `u64` *is* numeric order; the
  invariant to prove is "canonical text sort == numeric sort".
- **Custom serde** — `serialize`/`deserialize` as the padded string so the on-disk form is comparable.

## Read first

- [`../../architecture.md`](../../architecture.md#14-arrow-conversion--parquet-write) §1.4 (~line 464)
  — the `walrus_pg_sink_meta` JSON: `"lsn": "00000000019A2B3C"` and
  `"commit_lsn": "0000000001B4C000"`, both "zero-padded / monotonic-comparable so they order
  correctly as text". `commit_lsn` is *the* order/watermark key; `lsn` is the per-PK tiebreaker.
- [`../../architecture.md`](../../architecture.md#coordination-contract-control-plane-tables) — where
  `(commit_lsn, lsn)` tuples order everything (skim; the tuple type comes later, this is the scalar).
- Postgres `pg_lsn` text format `X/Y` — both halves are hex; the value is `(X << 32) | Y`.

## Scope

**In scope**

- `common::Lsn(u64)` with `Copy`, `Ord`, `Hash`, `LSN::ZERO`, `as_u64` / `new`.
- `FromStr` accepting **both** `X/Y` (hex/hex) **and** bare 16-hex (with or without leading zeros).
- `Display` as **uppercase, zero-padded, 16-hex** (`{:016X}`).
- `Serialize`/`Deserialize` as the padded string (not a JSON number).
- A dedicated `LsnParseError` (or a `common::Error::Config`-compatible variant — your call, documented).
- Inline unit tests: round-trip, the two parse dialects, and text-sort == numeric-sort.

**Explicitly deferred** (do *not* build these here)

- The `(commit_lsn, lsn)` **tuple** ordering key and `SinkMeta` → **PR 1.1**.
- Any Postgres client that *emits* real LSNs → **PR 2.20**.
- Arithmetic on LSNs (byte deltas for lag metrics) → whenever a metric needs it (Phase 4).

## Files to create / modify

```
crates/common/Cargo.toml         # + serde = { workspace = true, features = ["derive"] }
                                  #   (add serde = "1" and serde_json = "1" (dev) to [workspace.dependencies])
crates/common/src/lsn.rs         # new — Lsn newtype
crates/common/src/lib.rs         # + pub mod lsn; pub use lsn::Lsn;
```

## Skeleton

```rust
// crates/common/src/lsn.rs
use std::fmt;
use std::str::FromStr;

/// A Postgres Log Sequence Number as a single `u64`.
///
/// Canonical text form is **zero-padded 16-hex** (`Display`), chosen so lexical order
/// equals numeric order — the ordering contract the whole pipeline relies on.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);
    pub const fn new(raw: u64) -> Self { Lsn(raw) }
    pub const fn as_u64(self) -> u64 { self.0 }
}

/// Failure to parse either the `X/Y` or the 16-hex form.
#[derive(Debug, thiserror::Error)]
#[error("invalid LSN {input:?}: {reason}")]
pub struct LsnParseError { pub input: String, pub reason: &'static str }

impl FromStr for Lsn {
    type Err = LsnParseError;
    /// Accepts `"0/199BAC8"` (two hex halves) and `"00000000019A2B3C"` (bare ≤16 hex).
    fn from_str(s: &str) -> Result<Self, Self::Err> { todo!() }
}

impl fmt::Display for Lsn {
    /// Always 16 uppercase hex digits, zero-padded.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { todo!() }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { todo!() } // e.g. Lsn(00000000019A2B3C)
}

impl serde::Serialize for Lsn {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { todo!() } // padded string
}

impl<'de> serde::Deserialize<'de> for Lsn {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> { todo!() } // via FromStr
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_x_slash_y_form() { /* "0/199BAC8" -> 0x199BAC8 */ todo!() }

    #[test]
    fn parses_bare_16_hex_form() { todo!() }

    #[test]
    fn display_is_zero_padded_16_upper_hex() { todo!() }

    #[test]
    fn round_trips_through_display_and_from_str() { /* "0/199BAC8" -> Lsn -> parse(Display) == same */ todo!() }

    #[test]
    fn serde_round_trips_as_padded_string() { todo!() }

    /// The load-bearing invariant: sorting the *text* form equals sorting the *numeric* value.
    #[test]
    fn text_sort_equals_numeric_sort() { todo!() }

    #[test]
    fn rejects_garbage_and_overlong_input() { todo!() }
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `"0/199BAC8".parse::<Lsn>()` and `format!("{lsn}")` **round-trip** (parse ∘ display == identity).
- [ ] `Display` always emits exactly 16 uppercase hex digits, zero-padded (`00000000019A2B3C`).
- [ ] `FromStr` accepts both `X/Y` and bare 16-hex; rejects non-hex, empty, and > 16 significant hex.
- [ ] serde emits/consumes the **padded string**, not a JSON number (assert on the JSON text).
- [ ] **Property held:** for a vector of `Lsn`s, sorting by `Display` string == sorting by `as_u64()`.
- [ ] `Ord` is numeric (it derives from the inner `u64`); `Lsn::ZERO < any nonzero`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common` (and `--workspace` stays green)

## Hints & gotchas

- In `X/Y`, **both halves are hexadecimal** and the low half is *not* zero-padded by Postgres. The
  value is `(high << 32) | low`; parse each half with `u32::from_str_radix(_, 16)`.
- `{:016X}` gives uppercase zero-pad; `{:016x}` is lowercase. Pick uppercase to match the design's
  sample meta and be consistent everywhere — the text-sort invariant only holds within one case+width.
- Deriving `Ord` on `Lsn(u64)` is correct *because* the single field is the value. Don't hand-roll it.
- Implement `serde` **by hand** (not `#[derive(Serialize)]`) — a derive on a tuple struct would emit a
  bare number and quietly destroy the "sorts as text" guarantee that manifest/JSON storage depends on.
- Consider rejecting more than 16 significant hex digits explicitly — a 17-digit input can't fit `u64`
  and must be a caller bug, not a silent truncation.

## References

- Design: [`../../architecture.md`](../../architecture.md#14-arrow-conversion--parquet-write) §1.4
  (`walrus_pg_sink_meta` — zero-padded, monotonic-comparable `lsn` / `commit_lsn`).
- Prev: [PR 0.2](./pr-0.2-common-errors-exit-codes.md) · Next:
  [PR 0.4](./pr-0.4-common-telemetry.md) · [Roadmap](../README.md)
