<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.7 — Pin compile-time size budgets on the hot decode types and deny the large-value lints

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `common`, `pg-sink` ·
> **Est. size:** M · **Depends on:** PR 9.6 · **Unlocks:** PR 9.8

walrus is already conformant on this rule and has **nothing** stopping it from regressing. Production
contains **0** `size_of` assertions and **0** `Box<[T]>`; all **6** `Box<` uses (every one in
`crates/pg-to-arrow/src/batch.rs`) are `dyn ArrayBuilder` trait objects, not large-value boxes. The
full pedantic + nursery sweep — 51 lints, 1192 sites — reports **no** `large_types_passed_by_value`,
`large_stack_arrays`, `large_stack_frames` or `large_futures` hits anywhere. That is a good position
and an unguarded one: `TupleValue`, `pgoutput::Message`, `common::Error` and `SinkMeta` are moved once
per column / per WAL message / per row, so a future field addition silently multiplies a memcpy
across the hottest loop in the system and no test would notice. This PR converts today's conformance
into a **tripwire**: four `const _: () = assert!(size_of::<T>() <= N);` budgets recording the layout
the compiler reports today, plus the four large-value clippy lints promoted to `deny` (free — all
four are already at 0 sites).

## Why — learning objectives

- **Moves are memcpy** — `size_of::<T>()` is the per-move byte cost; `const {}` assertions make an
  invariant a zero-runtime-cost compile error, and `Box` turns a move into an 8-byte pointer copy.
- **The sink's decode hot path** — one `Message` and N `TupleValue`s are constructed and moved per
  WAL record, and one `SinkMeta` per row, so these four types are where layout actually bites.

## Read first

- [`own-move-large`](../../.claude/skills/rust-skills/rules/own-move-large.md) — take the size/move
  -frequency table (`< 128 B` never box, `> 512 B` always) and the `size_of` measurement habit.
  Ignore the "Pattern: Builder Returns Boxed" section — walrus has no such builder, and this PR
  boxes nothing.
- `crates/common/src/pg_shape.rs:93-111` — `TupleValue`, and the comment explaining why `Null` and
  `UnchangedToast` must stay distinct variants (no collapsing them to shrink the enum).
- `crates/pg-sink/src/pgoutput/mod.rs:37-181` — the 18-variant `Message` enum; note the two-phase
  variants (`BeginPrepare` … `StreamPrepare`) that walrus never sees in production but still decodes.
- `crates/common/src/sink_meta.rs:105-140` — `SinkMeta`, the per-row provenance document, and the
  amortized-serialization note right below it (PR 5.7) explaining why this struct is hot.
- `crates/common/src/error.rs:15-61` — `common::Error`; every variant is a `String` or a
  `{ table: String }`, which is exactly why it is small and must stay that way.
- `Cargo.toml:15-21` — the `[workspace.lints.clippy]` block and the comment explaining why
  restriction/pedantic lints need no `priority` juggling next to `all = "deny"`.

## Scope

**In scope**

- Add four compile-time budgets, each directly beneath the type it guards, each with a doc comment
  saying *why that type is hot* and *what to do when it trips*:
  `TupleValue` (common), `SinkMeta` (common), `common::Error`, `pgoutput::Message` (pg-sink).
- `N` is the size the compiler reports on `main` **today** — no rounding up, no headroom. Use `<=`,
  not `==`, so a future niche optimisation that *shrinks* the type does not fail the build.
- Promote to `deny` in `[workspace.lints.clippy]`: `large_types_passed_by_value`,
  `large_stack_arrays`, `large_stack_frames`, `large_futures` — with a comment recording that all
  four measured 0 sites in the 51-lint sweep, so this is a gate, not a cleanup.
- One sibling test in `crates/common/src/pg_shape_test.rs` mirroring the `TupleValue` bound, so a
  breach prints the actual number instead of a bare const-eval panic.

**Explicitly deferred** (do *not* build these here)

- **Box nothing.** The budgets *record* the current layout so a future field addition trips the
  build — this is a tripwire, not a layout change. No field is reordered, no variant is boxed.
- `clippy::large_enum_variant` and `clippy::result_large_err` are already denied transitively via
  `clippy::all` and need no action here.
- Boxing a large enum variant is Phase 11's `mem-box-large-variant`. If a budget turns out to be
  *uncomfortably* large today, record the number anyway and leave the fix to that PR.
- Do not tighten clippy's thresholds (`pass-by-value-size-limit`, `future-size-threshold`,
  `array-size-threshold`) in `clippy.toml` — a threshold change is a separate, noisier decision.
- No `static_assertions` dependency: `const _: () = assert!(…)` is built-in, and a new dep would
  have to clear `cargo deny` for nothing.

## Files to create / modify

```
Cargo.toml                             # [workspace.lints.clippy] += the 4 large-value lints
crates/common/src/pg_shape.rs          # + size budget under `enum TupleValue`
crates/common/src/pg_shape_test.rs     # + `tuple_value_size_budget` — same bound, readable failure
crates/common/src/sink_meta.rs         # + size budget under `struct SinkMeta`
crates/common/src/error.rs             # + size budget under `enum Error`
crates/pg-sink/src/pgoutput/mod.rs     # + size budget under `enum Message` (no sibling test file
                                       #   here — the const assert is the whole gate)
```

## Skeleton

```rust
// crates/common/src/pg_shape.rs — immediately below `pub enum TupleValue { … }`

/// Move-cost budget for the decode hot path (`own-move-large`).
///
/// One `TupleValue` is built and moved **per column per WAL tuple**, so `size_of` here is a
/// per-row memcpy multiplier. `N` is what the compiler reports today, measured — not a target.
/// If this fails to compile, a field/variant grew the enum: either shrink it, box the offending
/// variant (Phase 11 `mem-box-large-variant`), or raise `N` **deliberately**, in review.
const TUPLE_VALUE_MAX_BYTES: usize = todo!("N — read it off the compiler; recipe in Hints");
const _: () = assert!(size_of::<TupleValue>() <= TUPLE_VALUE_MAX_BYTES);
```

```rust
// crates/common/src/pg_shape_test.rs — the readable mirror of the const assert

#[test]
fn tuple_value_size_budget() {
    // The const assert above is the real gate; this exists so a breach prints the number.
    assert!(
        size_of::<TupleValue>() <= super::TUPLE_VALUE_MAX_BYTES,
        "TupleValue grew to {} bytes (budget {}) — see own-move-large",
        size_of::<TupleValue>(),
        super::TUPLE_VALUE_MAX_BYTES,
    );
}
```

```rust
// crates/pg-sink/src/pgoutput/mod.rs — below `pub enum Message { … }` (ends at :181)

/// One `Message` is moved per decoded WAL record out of `parse_one`/`parse_stream`; the widest
/// variants are `Relation` (a whole `PgRelation`) and the two-phase `*Prepared` frames. Budget as
/// measured — see `own-move-large`.
const MESSAGE_MAX_BYTES: usize = todo!("N — read it off the compiler");
const _: () = assert!(size_of::<Message>() <= MESSAGE_MAX_BYTES);
```

```toml
# Cargo.toml — [workspace.lints.clippy], appended below expect_used
# Large-value gates (PR 9.7, `own-move-large`). All four measured **0 sites** across the 51-lint
# pedantic+nursery sweep, so these cost nothing today and exist purely to stop a regression on the
# per-WAL-message move path. Pedantic/nursery ⇒ not in `clippy::all` ⇒ no `priority` needed.
large_types_passed_by_value = "deny"
large_stack_arrays = "deny"
large_stack_frames = "deny"
large_futures = "deny"
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] Four `const _: () = assert!(size_of::<T>() <= N);` budgets exist — `TupleValue`, `SinkMeta`,
      `common::Error`, `pgoutput::Message` — each directly beneath its type, each with a doc comment
      naming why that type is hot and what to do when the assert trips.
- [ ] Every `N` equals the size the compiler reports on `main` today (no padding, no rounding), and
      a one-line comment records how it was measured.
- [ ] Every budget uses `<=`, never `==`, so a shrink cannot break the build.
- [ ] `crates/common/src/pg_shape_test.rs` gains `tuple_value_size_budget`, asserting the same bound
      with a message that prints the actual size.
- [ ] `Cargo.toml` `[workspace.lints.clippy]` gains `large_types_passed_by_value`,
      `large_stack_arrays`, `large_stack_frames` and `large_futures`, all `= "deny"`, with a comment
      recording the 0-site measurement and why no `priority` field is needed.
- [ ] Nothing is boxed, no field or variant is reordered, and no clippy threshold is changed in
      `clippy.toml` — the diff is comments, four consts, one test, and the lint block.
- [ ] `grep -rn --include='*.rs' --exclude='*_test.rs' 'size_of' crates/*/src | wc -l` is `>= 4`.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common -p pg-sink` (and `--workspace` stays green)

## What completed looks like

```
# BEFORE (on main) — nothing records the layout of the hot decode types
$ grep -rn --include='*.rs' --exclude='*_test.rs' 'size_of' crates/*/src | wc -l
0

# AFTER
$ grep -rn --include='*.rs' --exclude='*_test.rs' 'size_of' crates/*/src | wc -l
8      # >= 4: one budget + one const per guarded type

$ grep -c 'large_types_passed_by_value\|large_stack_arrays\|large_stack_frames\|large_futures' Cargo.toml
4

$ cargo clippy --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
    # green — all four lints were already at 0 sites; they are now a gate, not a cleanup

# And the tripwire actually trips: add a `Vec<u8>` field to SinkMeta and
$ cargo build -p common
error[E0080]: evaluation panicked: assertion failed: size_of::<SinkMeta>() <= SINK_META_MAX_BYTES
```

## Hints & gotchas

- **How to read `N` off the compiler.** Do not hand-compute it from field sizes — enum layout packs
  niches and reorders fields. Drop a throwaway test in the sibling file and let `assert_eq!` print
  both sides:
  ```rust
  #[test] fn sizes() { assert_eq!(size_of::<TupleValue>(), 0); }
  ```
  `cargo test -p common sizes` then prints `left: 40 / right: 0`. Take the left number, delete the
  throwaway. (`-Z print-type-sizes` needs nightly; the pinned toolchain is stable — don't reach for
  it.)
- **`size_of` is in the prelude** since Rust 1.80 and the MSRV is 1.95, so write it bare. Adding
  `use std::mem::size_of;` would be an unused import and `warnings = "deny"` fails the build.
- **`assert!` works in const context** — `const _: () = assert!(…)` is evaluated at compile time and
  emits no code. A failure is `error[E0080]`, terse by design; that terseness is exactly why the
  sibling test mirror is worth the four lines.
- The four new lints are **pedantic/nursery**, i.e. *not* members of `clippy::all`, so they need no
  `priority` field next to `all = "deny"` — the same reasoning already written out at
  `Cargo.toml:17-19` for `unwrap_used`/`expect_used`.
- **`--all-targets` includes benches.** `crates/loader/benches/*` and `crates/pg-sink` benches build
  fixtures; if `large_stack_arrays` or `large_stack_frames` fires there, heap-allocate the fixture in
  the bench. Do **not** blanket-`#[allow]` a lint you just introduced.
- `large_futures` (default threshold 16 KiB) is the one most likely to surprise: it measures the
  generated future, and the sink's `async fn` bodies are long. If it fires, that is real signal —
  `Box::pin` the offending call, and say so in the PR body.
- Do not touch any `.sql` file or the `.sqlx` offline cache — there is no Docker on this machine to
  regenerate it, and nothing here needs it.
- `unwrap`/`expect` are denied in production; the sibling test file is exempt via `clippy.toml`, but
  the test above needs neither.

## References

- Rule: [`own-move-large`](../../.claude/skills/rust-skills/rules/own-move-large.md)
- Design: `docs/architecture.md` §1.2 (hand-rolled replication consumer) and §1.4 (Arrow conversion
  & Parquet write) — the per-message / per-row paths these budgets protect.
- Prev: [PR 9.6](./pr-9.6-own-lifetime-elision.md) · Next: [PR 9.8](./pr-9.8-own-arc-shared.md) · [Phase 9](./README.md) · [Roadmap](../README.md)
