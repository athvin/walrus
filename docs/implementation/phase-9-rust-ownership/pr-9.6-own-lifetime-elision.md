<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.6 — Replace the two named lifetimes that elision already covers

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Readiness:** audited · **Outcome:** change
> **Gates:** fmt,clippy,test · **Test packages:** common

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `common` ·
> **Est. size:** S · **Depends on:** PR 9.5 · **Unlocks:** PR 9.7

**69 production signatures carry an explicit lifetime**, and `clippy::needless_lifetimes` — already
denied through `clippy::all` — reports **none** of them: nothing in walrus is over-annotated in the
classic "elision would have removed this entirely" sense. What remains is the subtler case, the one
`clippy::elidable_lifetime_names` (nursery, unconfigured) catches: a lifetime that *must* be present
but need not be *named*. There are exactly two, both hand-written sqlx impls in `common`:
`crates/common/src/ids.rs:52` `impl<'q> Encode<'q, Postgres> for ManifestId` and
`crates/common/src/lsn.rs:138` `impl<'q> Encode<'q, Postgres> for Lsn`. Neither uses `'q` anywhere
in its body, so both should read `impl Encode<'_, Postgres> for …`. This PR makes that edit and pins
the lint so the next hand-written trait impl cannot reintroduce a name that carries no information.

## Why — learning objectives

- **The three elision rules** — (1) each input reference gets its own lifetime; (2) with exactly
  **one** input lifetime, the output borrows from it; (3) with `&self`, the output borrows from
  `self`. Rule 2 is the one that fails the moment a second input reference appears, which is why the
  five named lifetimes this PR *leaves alone* genuinely need their names.
- **`'_` is "explicit but inferred"** — it says "a lifetime lives here, the compiler works out which"
  without inventing an identifier a reader then has to track. That is a strictly better default than
  `<'q>` for a parameter used exactly once.
- **walrus's transparent-`int8` newtypes** — `Lsn` and `ManifestId` hand-write `Type`/`Encode`/
  `Decode` (rather than deriving) so `common`'s `sqlx` dependency need not pull the `macros` feature.
  Reading those impls is how you learn where the lifetime actually comes from.

## Read first

- [`own-lifetime-elision`](../../../.claude/skills/rust-skills/rules/own-lifetime-elision.md) — take the
  three rules and the "anonymous lifetime `'_`" section; the "when explicit lifetimes ARE required"
  list is the checklist for what this PR must **not** touch.
- `crates/common/src/ids.rs:35-67` — the whole `#[cfg(feature = "sqlx")] mod sqlx_support`: the
  `Type` impl (no lifetime), the `Encode<'q>` impl at :52 (the target), and the `Decode<'r>` impl at
  :61 (which must keep its name — see below).
- `crates/common/src/lsn.rs:120-155` — the same three impls for `Lsn`, with the `pg_lsn`-by-name
  `type_info`/`compatible` pair and the `Encode<'q>` at :138.
- `crates/common/Cargo.toml` — the `sqlx = ["dep:sqlx"]` **optional feature**. Neither impl is even
  compiled without it; `--all-features` is not optional for this ticket.
- `Cargo.toml:15-21` — `[workspace.lints.clippy]`.

## Baseline contract

- **Precondition:** Confirm `rule-present`, then inspect the immediate predecessor's named paths and
  symbols with `rg`. Historical line coordinates in the audit are navigation hints only; the
  named symbol and stated precondition are authoritative.
- **Allowed files:** The **Files to create / modify** block is exhaustive.

- Any other current-tree mismatch blocks before editing.

## Scope

**Baseline precondition.** Before editing, reproduce the task's authored finding from its named
source paths, symbols, counts, and read-only probes; run the full **Verification commands** block
after implementation. The named sites and allowed paths are the complete task boundary.

**Baseline mismatch.** If the current tree differs from that authored finding, **STOP and request
task re-authoring before editing.** Do not choose another site, implementation, evidence conclusion,
or outcome.

**In scope**

- Add `elidable_lifetime_names = "deny"` to `[workspace.lints.clippy]` in `Cargo.toml`, with a
  comment noting it is nursery (outside `clippy::all`, so no `priority` juggling) and that
  `needless_lifetimes` — the coarser sibling — already arrives through `all`.
- `crates/common/src/ids.rs:52` — `impl<'q> Encode<'q, Postgres> for ManifestId` →
  `impl Encode<'_, Postgres> for ManifestId`.
- `crates/common/src/lsn.rs:138` — the same edit for `Lsn`.

**Explicitly deferred** (do *not* build these here)

- The genuinely-required named lifetimes. Leave all of these exactly as they are:
  `crates/pg-to-arrow/src/batch.rs:568` `struct_field<'a, T>`, `:579` `bool_builder<'a>` and `:770`
  `text<'a>` — each takes two or more input references, so elision rule 2 cannot apply; and the
  structs `Reader<'a>` (`crates/pg-sink/src/pgoutput/reader.rs:10`) and `SourcePreflight<'a>`
  (`crates/pg-sink/src/preflight.rs:124`), which hold references and must name theirs.
- The two `Decode<'r, Postgres>` impls (`ids.rs:61`, `lsn.rs:149`). `'r` appears in the method
  signature (`fn decode(value: PgValueRef<'r>)`), so it is *used* and cannot become `'_`. The lint
  does not report them; do not "make them consistent".
- Any signature change beyond the lifetime spelling. The wire behaviour, the `pg_lsn`-by-name
  matching, and the `as i64` bit-pattern preservation all stay byte-identical.

## Files to create / modify

```
Cargo.toml                  # + clippy::elidable_lifetime_names = "deny"
crates/common/src/ids.rs    # :52  — impl<'q> Encode<'q, …> → impl Encode<'_, …>
crates/common/src/lsn.rs    # :138 — impl<'q> Encode<'q, …> → impl Encode<'_, …>
```

## Skeleton

```toml
# Cargo.toml — [workspace.lints.clippy]
# Nursery (outside `clippy::all`, so no `priority` juggling): flags a lifetime that must EXIST but
# need not be NAMED — `impl<'q> Trait<'q>` where `'q` is used exactly once. Its coarser sibling
# `needless_lifetimes` already arrives via `all` and reports 0 sites; this covers the other half.
elidable_lifetime_names = "deny"
```

```rust
// crates/common/src/ids.rs — inside `#[cfg(feature = "sqlx")] mod sqlx_support`.
// `'q` is mentioned once, in the trait's own parameter list, and never in the body.

impl Encode<'_, Postgres> for ManifestId {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
    }
}

// UNCHANGED — `'r` is used by `PgValueRef<'r>` in the method signature, so it must stay named.
impl<'r> Decode<'r, Postgres> for ManifestId {
    fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(ManifestId(<i64 as Decode<Postgres>>::decode(value)?))
    }
}
```

```rust
// crates/common/src/lsn.rs — the same edit, same reasoning. `Lsn`'s encoder reuses i64's because
// pg_lsn's binary wire format IS an 8-byte big-endian integer.
impl Encode<'_, Postgres> for Lsn {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        todo!("body unchanged: <i64 as Encode<Postgres>>::encode_by_ref(&(self.as_u64() as i64), buf)")
    }
}
```

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-lifetime-elision.md
focused-test = cargo test -p common
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `[workspace.lints.clippy]` in `Cargo.toml` contains `elidable_lifetime_names = "deny"` with the
      nursery / no-`priority` comment.
- [ ] `crates/common/src/ids.rs` and `crates/common/src/lsn.rs` each read
      `impl Encode<'_, Postgres> for …`; `grep -c "impl<'q>" crates/common/src/*.rs` returns 0.
- [ ] Both `Decode<'r, Postgres>` impls are **untouched** and still name `'r`, and the PR body says
      why (the lifetime is used by `PgValueRef<'r>`).
- [ ] None of the five deferred named lifetimes (`struct_field`, `bool_builder`, `text`, `Reader`,
      `SourcePreflight`) changed.
- [ ] `cargo clippy -p common --all-features --all-targets -- -D warnings` is green — run **with**
      `--all-features`, because `mod sqlx_support` is behind the optional `sqlx` feature and is not
      compiled otherwise.
- [ ] Wire behaviour unchanged: `cargo test -p common --all-features` green with no test edits, and
      no `.sql` file or `.sqlx` cache entry touched.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p common` (and `--workspace` stays green)

## What completed looks like

```
# --- on main today ---
$ grep -c 'elidable_lifetime_names' Cargo.toml
0

$ grep -rn "impl<'q>" crates/common/src/
crates/common/src/ids.rs:52:    impl<'q> Encode<'q, Postgres> for ManifestId {
crates/common/src/lsn.rs:138:    impl<'q> Encode<'q, Postgres> for Lsn {

# --- after this PR ---
$ grep -c 'elidable_lifetime_names' Cargo.toml
1
# => 1 (`clippy::elidable_lifetime_names = "deny"`)

$ grep -rn "impl<'q>" crates/common/src/
$ # (no output — both sqlx `Encode` impls now read `impl Encode<'_, Postgres>`)

$ cargo clippy --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

## Hints & gotchas

- **Run clippy with `--all-features` or you will "fix" nothing.** `common`'s sqlx impls sit inside
  `#[cfg(feature = "sqlx")] mod sqlx_support`, and the feature is off by default (`control` and
  `loader` enable it). A default-feature clippy run never even parses the two target lines.
- The `Decode` impls look identical in shape but are **not** elidable: `'r` appears in
  `fn decode(value: PgValueRef<'r>)`. Changing them to `'_` will not compile. If you are tempted to
  "make the module consistent", that instinct is exactly what the lint exists to discipline.
- `clippy::elidable_lifetime_names` is **nursery**; `clippy::needless_lifetimes` is style and already
  arrives via `all = "deny"`. Listing the nursery lint alongside `all` needs no `priority` field —
  same reasoning as the `unwrap_used`/`expect_used` comment at `Cargo.toml:17-19`.
- Nursery lints can move or be renamed between toolchains. The MSRV is pinned at **1.95**
  (`rust-toolchain.toml` + the CI `msrv` job), so verify the lint name resolves under that toolchain
  before committing; an unknown lint name in `[workspace.lints]` is itself a hard error under
  `deny(warnings)`.
- Do not touch any `.sql` file or the committed `.sqlx` offline cache — there is no Docker on this
  machine to regenerate it. This PR is pure Rust type-level spelling; the wire bytes must not move.
- `unwrap`/`expect` are denied in production and `clippy::all` + `warnings` are already `deny`, so
  every member crate picks the new lint up automatically through `[lints] workspace = true`.
- Add no dependency. Anything new must clear `cargo deny` (advisories, the 8-license allow-list,
  bans), which is absurd overhead for a two-character edit.

## References

- Rule: [`own-lifetime-elision`](../../../.claude/skills/rust-skills/rules/own-lifetime-elision.md)
- Design: `docs/implementation/phase-8-cleanup/pr-8.4-domain-id-newtypes.md` — the transparent-`int8`
  newtype pattern whose hand-written `Encode`/`Decode` impls this PR tidies.
- Prev: [PR 9.5](./pr-9.5-own-cow-conditional.md) · Next: [PR 9.7](./pr-9.7-own-move-large.md) · [Roadmap](../README.md)
