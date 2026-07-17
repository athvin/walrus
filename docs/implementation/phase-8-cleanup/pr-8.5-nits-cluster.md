<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 8.5 — Nits cluster: visibility, an explicit plan tier, and a documented non-change

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 8 — cleanup · **Crates touched:** `loader`, `pg-sink` (doc only) ·
> **Est. size:** S · **Depends on:** PR 7.8 (phase 7 complete) · **Unlocks:** — (phase close)

The genuine nits — each independently trivial, bundled into one small green PR so they don't
each need their own ceremony. Two are code changes; one is a *documented decision not to
change*, recorded because a pedantic audit that silently drops a candidate leaves the next
reader to re-discover it.

## Why — learning objectives

By the end of this PR you will have practised:

- **Right-sizing visibility** — `pub` vs `pub(crate)` vs `#[cfg(test)]` for a helper only
  tests call.
- **Naming an implicit classification** — turning a rule that lives in comments and
  `if len == …` into an `enum` the compiler can see.
- **Auditing to a "keep" verdict** — concluding "this abstraction is correct" and writing
  down *why*, which is as much a result as a refactor.

## Read first

- `crates/loader/src/phase_a.rs:52` (`pause_began`) and its only external caller,
  `crates/loader/src/phase_a_test.rs:26`.
- `crates/loader/src/plan.rs` — the tier classification done implicitly via `emit.len()` and
  `recombine_expr()` (around lines 130–190), and `common::TypeDescriptor::tier` (the
  declared tier that currently isn't cross-checked).
- `crates/pg-sink/src/batch.rs:19-32` — the `Clock` trait + `SystemClock` (the "keep"
  candidate).

## Scope

**In scope**

1. **Visibility:** narrow `phase_a::pause_began` from `pub` to `pub(crate)` (or move it
   behind `#[cfg(test)]` if it is genuinely test-only) — it is not part of the loader's
   public surface.
2. **Explicit plan tier:** add a local `enum PlanTier { … }` in `plan.rs` (or, lighter,
   `debug_assert!` that the inferred tier matches the descriptor's declared
   `TypeDescriptor::tier`) so a corrupt/mismatched registry descriptor fails loudly near the
   plan instead of producing wrong DDL downstream. Keep it low-risk and behaviour-preserving
   on the happy path.
3. **`Clock` — documented non-change:** add a one-line doc note on the `Clock` trait
   recording that the single production impl (`SystemClock`) is intentional: it is a test
   seam that lets `max_fill` cadence be tested without sleeping. No code removal.

**Explicitly deferred** (do *not* build these here)

- Anything from PRs 8.1–8.4.
- Reworking the tier system itself — item 2 is a *guard/label*, not a redesign.

## Files to create / modify

```
crates/loader/src/phase_a.rs       # modify — pause_began visibility
crates/loader/src/plan.rs          # modify — PlanTier enum or descriptor-tier assertion
crates/loader/src/plan_test.rs     # modify/new — a test for the mismatch guard
crates/pg-sink/src/batch.rs        # modify — one doc line on the Clock trait (no logic change)
```

## Skeleton

```rust
// crates/loader/src/phase_a.rs
pub(crate) fn pause_began(logged: &parking_lot::Mutex<Option<i64>>, live: Option<i64>) -> Option<i64> {
    // unchanged body
    todo!()
}
```

```rust
// crates/loader/src/plan.rs   (option A — an explicit label; option B — just the assert)
enum PlanTier { One, TwoRecombine, TwoFlat, Three }

fn classify(/* descriptor, emit, … */) -> PlanTier { todo!() }
// debug_assert_eq!(classify(...) matches descriptor.tier, "registry tier drift for {table}");
```

```rust
// crates/pg-sink/src/batch.rs
/// Injectable clock so `max_fill` is testable without sleeping. Production uses [`SystemClock`];
/// the trait exists **for that test seam** — a single production impl is intentional, not dead
/// generality. (Audited PR 8.5: kept by design.)
pub trait Clock: Send + Sync { /* … */ }
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `pause_began` is no longer `pub` (it's `pub(crate)` or test-scoped); its test still
      compiles and passes.
- [ ] `plan.rs` makes the tier classification explicit **or** guards it against the
      descriptor's declared tier, with a test that a mismatch is caught; happy-path plans are
      unchanged.
- [ ] The `Clock` trait carries the "kept by design" doc note.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader -p pg-sink` (and `--workspace` stays green)
- [ ] **Phase close:** all Phase 8 roadmap boxes reflect reality (this one ticks last).

## What completed looks like

```
$ rg -n 'pub fn pause_began' crates/loader/src/phase_a.rs
$ # (no output — it's pub(crate) now)
$ rg -n 'pub(crate) fn pause_began' crates/loader/src/phase_a.rs
52:    pub(crate) fn pause_began( …

$ cargo test -p loader plan   # tier-guard test passes
```

## Hints & gotchas

- Check `pause_began`'s callers before changing visibility: if *only* `phase_a_test.rs`
  calls it, `pub(crate)` keeps the sibling-test access (the `#[path]` module is still inside
  the crate) — no need for `#[cfg(test)]` unless production never calls it at all.
- Keep item 2 **cheap and non-breaking**: a `debug_assert!` costs nothing in release and
  catches descriptor drift in tests/CI. A full `PlanTier` enum is nicer but only if it
  doesn't churn the plan logic — prefer the assert if the enum grows tentacles.
- Item 3 is the point of a pedantic audit: some candidates end in "leave it, here's why."
  Writing that down *is* the deliverable for that line.

## References

- Design: `docs/architecture.md` (type tiers §2.x) — the tier semantics item 2 guards.
- Prev: [PR 8.4](./pr-8.4-domain-id-newtypes.md) · Next: — *(phase close)* ·
  [Phase 8](./README.md) · [Roadmap](../README.md)
