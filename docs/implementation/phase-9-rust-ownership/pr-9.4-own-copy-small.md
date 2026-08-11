<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.4 — Derive `Copy` on the small value types and gate it with `missing_copy_implementations`

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `common`, `control`, `loader`,
> `pg-sink`, `pg-to-arrow` · **Est. size:** M · **Depends on:** PR 9.3 · **Unlocks:** PR 9.5

**28 production types derive `Copy`; 44 derive `Clone` without it.** At least four of those 44 are
public, hold nothing but `Copy` fields, and force callers into an explicit `.clone()` for no reason:
`DurabilityCheckpoint { confirmed_flush: Lsn, open_txn_floor: Option<Lsn> }`
(`crates/pg-sink/src/checkpoint.rs:19`), the `SlotStatus` and `SlotAction` enums
(`crates/pg-sink/src/epoch.rs:18` and `:31`, whose only payload is an `Lsn`), and
`HeartbeatConfig { idle_after: Duration, roundtrip_deadline: Duration }`
(`crates/pg-sink/src/heartbeat.rs:25`). The rustc lint that finds *all* of them —
`missing_copy_implementations` — is absent: `[workspace.lints.rust]` currently sets only
`warnings = "deny"`, and because the lint is allow-by-default that group never switched it on. This
PR turns it on, works the resulting list, and pairs it with `clippy::trivially_copy_pass_by_ref` so
a newly-`Copy` type does not keep being passed by reference.

## Why — learning objectives

- **`Copy` is an opt-in for small POD types** — the rule's size table is the decision procedure:
  ≤ 16 bytes → derive it; 17–64 bytes → consider it and say why; > 64 bytes → prefer references.
  `Copy` also requires every field `Copy`, no `Drop`, and no heap data (`String`/`Vec`/`Box`).
- **`missing_copy_implementations` is an API-surface lint** — it reports *public* types that could be
  `Copy` but are not, which is exactly the population where the missing `Copy` leaks into callers as
  ceremony. It is allow-by-default, so `warnings = "deny"` alone never enabled it.
- **`Copy` is a commitment, not a free win** — once a type is `Copy`, adding a `String` field later is
  a breaking change, and a type with `&mut self` mutators can be *silently duplicated* by an
  accidental deref. Deriving it is a design decision you record, and `#[allow(...)]` with a stated
  reason is a legitimate outcome.
- **The sink state machines you are editing** — `SlotStatus`/`SlotAction` are the false-positive guard
  that decides "resume vs fresh slot vs retry" (§1.8), and `DurabilityCheckpoint` is the type that
  advances `confirmed_flush_lsn` (§1.5).

## Read first

- [`own-copy-small`](../../.claude/skills/rust-skills/rules/own-copy-small.md) — take the three
  `Copy` requirements and the size table verbatim; they are the triage rubric for every type the lint
  reports.
- `crates/pg-sink/src/checkpoint.rs:15-60` — `DurabilityCheckpoint`, its `#[derive(Debug, Clone)]` at
  :19, and the `&mut self` mutators (`set_open_txn_floor`, `on_batch_durable`) that make `Copy` a
  judgement call rather than an automatic yes.
- `crates/pg-sink/src/epoch.rs:16-52` — `SlotStatus`, `SlotAction`, and
  `pub fn decide(status: &SlotStatus) -> SlotAction`.
- `crates/pg-sink/src/heartbeat.rs:24-31` — `HeartbeatConfig`, and
  `crates/pg-sink/src/config.rs:188-193` — `heartbeat_config()`, which constructs a fresh one.
- `Cargo.toml:10-21` — `[workspace.lints.rust]` (today: `warnings = "deny"` only) and
  `[workspace.lints.clippy]`.

## Scope

**In scope**

- Add `missing_copy_implementations = "deny"` to `[workspace.lints.rust]` and
  `trivially_copy_pass_by_ref = "deny"` to `[workspace.lints.clippy]` (0 sites in the 51-lint sweep
  today, so it is free — it exists to stop a newly-`Copy` type being passed by reference).
- Resolve **every** type the lint reports across all five crates, one of two ways:
  1. add `Copy` to its `derive` (adding `Clone` alongside if it lacks one), or
  2. keep it `Clone`-only with `#[allow(missing_copy_implementations)]` and a one-line comment
     stating *why* (mutation hazard, imminent heap field, deliberate move semantics).
- Fix any `trivially_copy_pass_by_ref` site the new `Copy` derives create, by taking the value.
- Drop only those `.clone()` call sites the compiler now rejects or clippy now flags.

**Explicitly deferred** (do *not* build these here)

- `Copy` on anything holding a `String`/`Vec`/`Bytes`. `PgRelation`, `TypeDescriptor`,
  `WrittenObject` and `TupleValue` stay `Clone`-only — they cannot be `Copy` and must not grow an
  `#[allow]` that pretends otherwise (the lint will not report them anyway).
- A sweep of the remaining `.clone()` call sites. Removing a now-redundant `.clone()` on a `Copy`
  type beyond what the compiler forces is out of scope; `clippy::clone_on_copy` (already inside
  `clippy::all`) will surface any that matter.
- `size_of` budgets — PR 9.7 adds those.

## Files to create / modify

```
Cargo.toml                          # + missing_copy_implementations = "deny"  [workspace.lints.rust]
                                    # + clippy::trivially_copy_pass_by_ref = "deny"
crates/pg-sink/src/checkpoint.rs    # :19  — DurabilityCheckpoint: derive Copy, or allow + reason
crates/pg-sink/src/epoch.rs         # :18, :31 — SlotStatus / SlotAction: derive Copy
crates/pg-sink/src/heartbeat.rs     # :25  — HeartbeatConfig: derive Copy
crates/common/src/*.rs              # whatever else the lint reports — triage each
crates/control/src/*.rs             # "
crates/loader/src/*.rs              # "
crates/pg-to-arrow/src/*.rs         # "
```

## Skeleton

```toml
# Cargo.toml — [workspace.lints.rust]
# rustc lint, ALLOW by default — `warnings = "deny"` above never switched it on. It reports public
# types that could be `Copy` but are not, i.e. exactly the ones whose missing `Copy` leaks into
# callers as ceremony. Both entries are "deny", so no `priority` juggling against the group.
missing_copy_implementations = "deny"
```

```toml
# Cargo.toml — [workspace.lints.clippy]
# Pedantic, 0 sites today: a `Copy` type smaller than a pointer should be taken by value, not by
# reference. Paired with the rustc lint above so adding `Copy` doesn't leave `&SmallThing` behind.
trivially_copy_pass_by_ref = "deny"
```

```rust
// crates/pg-sink/src/checkpoint.rs:19 — every field is Copy (`Lsn` is `Clone, Copy, …`), but the
// type has `&mut self` mutators. Decide, then record the decision in the derive or in an allow.
#[derive(Debug, Clone)] // ← todo!(): + Copy, or #[allow(missing_copy_implementations)] + why not
pub struct DurabilityCheckpoint {
    confirmed_flush: Lsn,
    open_txn_floor: Option<Lsn>,
}
```

```rust
// crates/pg-sink/src/epoch.rs:18 and :31 — payload is `Lsn` only; both are pure classification
// values that are matched on and returned, never mutated.
#[derive(Debug, Clone, PartialEq, Eq)] // ← todo!(): + Copy
pub enum SlotStatus {
    Healthy { confirmed_flush: Lsn },
    Absent,
    Invalidated,
    Unreachable,
}

#[derive(Debug, Clone, PartialEq, Eq)] // ← todo!(): + Copy
pub enum SlotAction {
    Resume { confirmed_flush: Lsn },
    FreshSlot,
    Retry,
}
```

```rust
// crates/pg-sink/src/heartbeat.rs:25 — two `Duration`s. Above the 16-byte band, inside the
// "consider it" band; it is a config value that is read, never mutated in place.
#[derive(Debug, Clone)] // ← todo!(): + Copy
pub struct HeartbeatConfig {
    pub idle_after: Duration,
    pub roundtrip_deadline: Duration,
}
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `[workspace.lints.rust]` contains `missing_copy_implementations = "deny"`, with a comment
      recording that it is allow-by-default and therefore was *not* covered by `warnings = "deny"`.
- [ ] `[workspace.lints.clippy]` contains `trivially_copy_pass_by_ref = "deny"`.
- [ ] `DurabilityCheckpoint`, `SlotStatus`, `SlotAction` and `HeartbeatConfig` each either derive
      `Copy` or carry `#[allow(missing_copy_implementations)]` with a one-line reason — no silent
      third option.
- [ ] Every other type the lint reports across `common`, `control`, `loader`, `pg-sink` and
      `pg-to-arrow` is resolved the same way; there is **no** bare `#[allow]` without a reason
      comment anywhere in the diff.
- [ ] The production `Copy`-derive count is **≥ 32** (28 today), and no type holding a `String`,
      `Vec` or `Bytes` gained `Copy`.
- [ ] Any `trivially_copy_pass_by_ref` site created by the new derives is fixed by taking the value,
      not by an `#[allow]`.
- [ ] Behaviour is unchanged: `cargo test -p pg-sink` is green with no test edits beyond those the
      compiler forces.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p pg-sink` (and `--workspace` stays green)

## What completed looks like

```
# --- on main today ---
$ grep -rn --include='*.rs' --exclude='*_test.rs' '#\[derive(' crates/*/src | grep -c Copy
28
# => 28 (and 44 derive Clone without Copy)

# --- after this PR ---
$ grep -rn --include='*.rs' --exclude='*_test.rs' '#\[derive(' crates/*/src | grep -c Copy
32
# => >= 32, plus `missing_copy_implementations = "deny"` in [workspace.lints.rust] and
#    `clippy::trivially_copy_pass_by_ref = "deny"` in [workspace.lints.clippy] (0 sites in the
#    51-lint sweep, so it is free), with `cargo clippy --all-targets --all-features -- -D warnings`
#    green — every remaining Clone-only type either gains Copy or carries a one-line `#[allow]`
#    saying why not

$ cargo clippy --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

## Hints & gotchas

- **Enumerate before you edit.** Land the lint at `"warn"` first, run
  `cargo clippy --all-targets --all-features 2>&1 | grep -A2 missing_copy_implementations`, write the
  list down, triage it, *then* flip it to `"deny"`. Going straight to `deny` turns the first
  compilation into an unreadable wall.
- **`Copy` on a type with `&mut self` mutators is a real hazard.** `DurabilityCheckpoint` advances
  `confirmed_flush_lsn` — the single most safety-critical value in the sink. If it is `Copy`, an
  accidental `let mut c = *checkpoint;` mutates a *duplicate* and silently loses the advance. That is
  a legitimate reason to choose the `#[allow]` branch; if you choose `Copy` anyway, say in the PR body
  why the call sites (all of which thread `&mut DurabilityCheckpoint`) make it safe.
- Size reality check, so you argue from numbers rather than vibes: `Lsn` is a `u64` (8 bytes),
  `Option<Lsn>` has no niche (16 bytes), `Duration` is 16 bytes. So `SlotStatus`/`SlotAction` land in
  the "≤ 16, derive it" band, `DurabilityCheckpoint` and `HeartbeatConfig` in the "17–64, consider it"
  band. Quote the rule's table in the PR body.
- `Copy` requires `Clone`. If the lint reports a type with no `Clone` at all, add `Clone, Copy`
  together — `#[derive(Copy)]` alone will not compile.
- Adding `Copy` can *create* `trivially_copy_pass_by_ref` sites that did not exist before (any newly
  `Copy` type ≤ 8 bytes that some function takes by `&`). Fix those by taking the value; that is the
  reason the two lints ship in the same PR.
- `clippy::clone_on_copy` is already denied via `clippy::all`, so the moment a type becomes `Copy`
  every `.clone()` on it becomes a hard error. Expect the compiler to hand you the call-site list —
  that is the "beyond what the compiler forces" boundary in Scope.
- `unwrap`/`expect` stay denied in production; `clippy::all` and `warnings` are already `deny`. Do
  not add `#[allow]`s to silence unrelated fallout — fix it or leave the type alone.
- Do not touch any `.sql` file or the committed `.sqlx` offline cache (no Docker locally to
  regenerate it), and add no dependency — anything new must clear `cargo deny`.

## References

- Rule: [`own-copy-small`](../../.claude/skills/rust-skills/rules/own-copy-small.md)
- Design: `docs/architecture.md` § "Component 1 — Postgres Sink (`walrus-pg-sink`)" — the
  durability checkpoint (§1.5) and the slot-loss classification guard (§1.8).
- Prev: [PR 9.3](./pr-9.3-own-clone-explicit.md) · Next: [PR 9.5](./pr-9.5-own-cow-conditional.md) · [Phase 9](./README.md) · [Roadmap](../README.md)
