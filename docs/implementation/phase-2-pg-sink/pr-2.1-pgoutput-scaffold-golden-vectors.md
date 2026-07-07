# PR 2.1 — Scaffold `pg-sink` (lib + bin) and port the golden-vector fixtures

> **Status:** ✅ Done — https://github.com/athvin/walrus/pull/21

> **Phase:** 2 — walrus-pg-sink (2a: the hand-rolled decoder) · **Crates touched:** `pg-sink` (new) ·
> **Est. size:** M · **Depends on:** PR 1.6 (phase-1 boundary; also needs `common` from PRs 0.2/0.3/1.2) ·
> **Unlocks:** PR 2.2

This PR stands up the fifth crate — `pg-sink` — as a **library + thin binary** split, so that
`pg-sink/tests/` can import the (still-empty) `pgoutput` decoder module and drive it with the exact
byte vectors we already proved in Python. Nothing decodes yet: the deliverable is the **test scaffold**
— a Rust fixture table that is a faithful port of `test_decode_pgoutput.py::VECTORS`, plus one
enumerating (`#[ignore]`) test that proves all vectors are present and hex-decodable. PRs 2.2–2.8 turn
the vectors on, family by family, TDD-style.

## Why — learning objectives

By the end of this PR you will have practised:

- **The lib+bin crate split** — why `pg-sink` compiles a `lib.rs` (the reusable decoder) *and* a thin
  `main.rs`, so integration tests in `tests/` can `use pg_sink::pgoutput::…` (a `tests/` file cannot see
  a bin-only crate).
- **Table-driven / golden-vector testing** — encoding `(name, hex, streaming_ctx, expected)` rows once
  and asserting them across eight later PRs.
- **`hex` as a dev-dependency** — decoding fixture strings to bytes in tests only, never in the shipped lib.
- **`#[ignore]` as a TDD ratchet** — a test that compiles and lists the work, switched on incrementally.

## Read first

- `../../proto-version.md` §14 "Reproduce it yourself" — where these vectors came from and how to
  regenerate them.
- `../../examples/proto-version/test_decode_pgoutput.py` — the source of truth for `VECTORS`; you are
  porting this dict verbatim (names, hex, streaming flag, expected render line).
- `../../examples/proto-version/decode_pgoutput.py` — the reference decoder whose *shape* the Rust
  `pgoutput` module mirrors (`Reader`, `parse_message`, `render`).
- `../README.md` "Reused assets" + "Golden-vector → PR map" — which vector lights up in which PR.

## Scope

**In scope**

- New crate `crates/pg-sink` wired into the workspace, `lib.rs` + `main.rs`, `#![deny(warnings)]`.
- An empty `pgoutput` module exposing the *names* PR 2.2 will fill (`Reader`, `StreamCtx`, `Message`,
  `parse_message`, `parse_stream`) as `todo!()`/`unimplemented!()` shapes so `tests/` compiles.
- `pg-sink/tests/pgoutput_vectors.rs`: the ported `VECTORS` table + `decode()`/`render()` test helpers +
  one `#[ignore]`d enumerating test asserting the vector count.

**Explicitly deferred** (do *not* build these here)

- Any actual parsing → **PR 2.2** onward. `parse_message` stays `unimplemented!()`.
- The bin's real bootstrap/health/replication → **PR 2.18+**. `main.rs` is a stub.

## Files to create / modify

```
Cargo.toml                                   # + pg-sink to [workspace] members
crates/pg-sink/Cargo.toml                    # new — deps below
crates/pg-sink/src/lib.rs                    # new — `pub mod pgoutput;`
crates/pg-sink/src/main.rs                   # new — thin bin stub
crates/pg-sink/src/pgoutput/mod.rs           # new — module shells (names only)
crates/pg-sink/tests/pgoutput_vectors.rs     # new — ported VECTORS + helpers + enumerating test
```

```toml
# crates/pg-sink/Cargo.toml
[package]
name = "pg-sink"
version = "0.1.0"
edition = "2021"

[lib]
name = "pg_sink"

[[bin]]
name = "walrus-pg-sink"
path = "src/main.rs"

[dependencies]
common   = { path = "../common" }
thiserror = { workspace = true }
anyhow    = { workspace = true }
tokio     = { workspace = true, features = ["macros", "rt-multi-thread", "signal"] }
bytes     = { workspace = true }
tracing   = { workspace = true }

[dev-dependencies]
hex = "0.4"

[lints]
workspace = true
```

## Skeleton

```rust
// crates/pg-sink/src/lib.rs
pub mod pgoutput;
```

```rust
// crates/pg-sink/src/main.rs
fn main() -> std::process::ExitCode {
    // Real bootstrap/health/replication land in PR 2.18+. For now, prove the bin links.
    unimplemented!("walrus-pg-sink bootstrap — PR 2.18")
}
```

```rust
// crates/pg-sink/src/pgoutput/mod.rs
//! Hand-rolled pgoutput (proto_version 2) decoder. Sync + pure: no tokio, no I/O.
//! Shapes only in PR 2.1 — the logic arrives in PRs 2.2–2.8.

/// Mutable "are we inside a Stream Start..Stop block" flag. The per-message xid prefix
/// (§7) exists ONLY when this is true; Stream Start/Stop toggle it.
#[derive(Debug, Default, Clone, Copy)]
pub struct StreamCtx {
    pub in_stream: bool,
}

/// One decoded pgoutput message. Variants are added family-by-family in PRs 2.2–2.8.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    // PR 2.2 adds Begin/Commit/Origin; 2.3 Relation/Type; 2.4 Insert; 2.5 Update/Delete;
    // 2.6 Truncate/Message; 2.7 Stream*; 2.8 the two-phase family.
}

/// Decode exactly one message, advancing `ctx` for stream framing. Implemented in PR 2.2+.
pub fn parse_message(_buf: &[u8], _ctx: &mut StreamCtx) -> Message {
    unimplemented!("PR 2.2")
}
```

```rust
// crates/pg-sink/tests/pgoutput_vectors.rs
use pg_sink::pgoutput::{parse_message, Message, StreamCtx};

/// A ported row of `test_decode_pgoutput.py::VECTORS`.
pub struct Vector {
    pub name: &'static str,
    pub hex: &'static str,
    pub streaming: bool,      // were we inside a Stream Start..Stop block?
    pub expected: &'static str, // the golden render line (verbatim from Python)
}

/// Port EVERY entry of `VECTORS` here — same names, hex, streaming flag, expected line.
pub const VECTORS: &[Vector] = &[
    Vector { name: "begin",
        hex: "42000000000199bac80002f8dc466a6b4f000002ed", streaming: false,
        expected: "BEGIN         final_lsn=0/199BAC8 commit_ts=2026-07-05T13:55:11.294287+00:00 xid=749" },
    Vector { name: "commit",
        hex: "4300000000000199bac8000000000199baf80002f8dc466a6b4f", streaming: false,
        expected: "COMMIT        flags=0 commit_lsn=0/199BAC8 end_lsn=0/199BAF8 commit_ts=2026-07-05T13:55:11.294287+00:00" },
    // … port the remaining vectors (type_enum, relation_*, insert*, update_*, delete_*,
    //     truncate_*, message_*, stream_*, unchanged_toast_update, begin_prepare … ) verbatim.
];

/// Test-only convenience: hex → one `Message`. (Reader/parse signature finalised in PR 2.2.)
pub fn decode(hex: &str, streaming: bool) -> Message {
    let bytes = hex::decode(hex).expect("vector hex is valid");
    let mut ctx = StreamCtx { in_stream: streaming };
    parse_message(&bytes, &mut ctx)
}

/// Test-only renderer that reproduces `decode_pgoutput.py::render` line-for-line.
/// Lives in the test crate, NOT the lib — production stamps time as RFC-3339 `Z` (PR 1.1),
/// this helper must match Python's `+00:00`/`X/Y`-hex output to compare against `expected`.
pub fn render(_m: &Message) -> String {
    todo!("PR 2.2 — implement alongside the first decoded messages")
}

#[test]
#[ignore = "turned on family-by-family in PRs 2.2–2.8"]
fn all_vectors_present_and_hex_decodable() {
    for v in VECTORS {
        assert!(hex::decode(v.hex).is_ok(), "vector {} has bad hex", v.name);
    }
    // Assert the full count so a dropped/renamed vector fails loudly.
    assert_eq!(VECTORS.len(), 30, "expected the full ported VECTORS set");
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [x] `crates/pg-sink` is a workspace member building a `pg_sink` lib **and** a `walrus-pg-sink` bin.
- [x] `pg-sink/tests/pgoutput_vectors.rs` compiles and holds **every** entry of
      `test_decode_pgoutput.py::VECTORS` (same names, hex, streaming flag, expected line) — the
      enumerating test asserts the exact count.
- [x] `#[ignore]`d enumerating test passes when run explicitly
      (`cargo test -p pg-sink -- --ignored all_vectors_present_and_hex_decodable`): all hex decodes.
- [x] `parse_message`/`render` are compilable shapes (`unimplemented!()`/`todo!()`), never invoked by a
      non-ignored test yet.
- [x] **Green locally and in CI:**
  - [x] `cargo fmt --check`
  - [x] `cargo clippy --all-targets --all-features -- -D warnings`
  - [x] `cargo test -p pg-sink` (and `cargo test --workspace` stays green)

## Hints & gotchas

- **Bin vs. lib visibility:** a file in `tests/` links against the crate's **lib** target only. If you
  skip the `[lib]`/`[[bin]]` split you will get "unresolved import `pg_sink`" — the split is the point.
- **Port, don't paraphrase.** Copy the hex and the `expected` strings byte-for-byte from the Python
  file; a single flipped nibble makes a later PR chase a phantom decoder bug.
- **Two OIDs in the vectors differ from §4's prose** (e.g. `relation_orders` is oid 16397, not the
  16404 shown in §1) — OIDs are run-dependent (§4 preamble). Trust the vector, not the narrative.
- Keep the render helper in the **test crate**. The decoder must stay pure and time-format-agnostic;
  the golden `+00:00`/microsecond formatting is a display concern, reproduced only to diff strings.
- The README calls these "the 24 golden vectors"; the Python file has since grown a few edge-case rows.
  Port whatever `VECTORS` actually contains and let the count assertion pin it.

## References

- Design: `../../proto-version.md` §14; `../../examples/proto-version/test_decode_pgoutput.py`;
  `../../examples/proto-version/decode_pgoutput.py`.
- Prev: [PR 1.6](../phase-1-shared-core/pr-1.6-control-schema-registry-ddl-manifest.md) (phase-1
  boundary) · Next: [PR 2.2](pr-2.2-pgoutput-reader-framing-begin-commit.md) · [Roadmap](../README.md)
