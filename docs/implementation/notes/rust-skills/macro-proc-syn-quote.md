# Procedural macros in walrus: declined (PR 24.7)

> **Status:** decided 2026-08-21 — **no direct `syn` / `quote` / `proc-macro2` dependency, and no
> proc-macro crate.** Guarded by `scripts/proc-macro-guard.sh` in the CI `supply-chain` job.
> Reopening trigger below.

## What the rule asks for

If walrus authors a procedural macro, the standard implementation is the ecosystem trio: `syn`
parses a typed Rust AST, `quote` generates readable token streams, and `proc-macro2` makes the
generation logic usable and unit-testable outside the compiler. For a derive, `syn`'s focused
`derive` feature is normally sufficient; enabling `full` would parse all Rust syntax at additional
compile-time cost. These are real benefits, not a reason to add the dependencies before walrus has a
proc-macro problem to solve.

## What is actually true (measured)

The baseline was commit `0e1b3999c9801c8df57276e0c1e86612bc56fb5c`.

- `bash scripts/proc-macro-guard.sh --check` reports `ok: 0 direct syn/quote/proc-macro2
  dependencies across 7 manifests`: the root, five crate manifests, and `tests/e2e/Cargo.toml`.
- `cargo tree --workspace -e normal,build | grep -o '[a-z0-9_-]* v[0-9.]* (proc-macro)' | sort -u
  | wc -l` reports **19** distinct proc-macro crates already compiled transitively.
- `Cargo.lock` carries two `syn` majors, **1.0.109** and **2.0.118**, through consumers including
  `serde_derive`, `thiserror-impl`, `sqlx-macros`, and `async-trait`.
- `deny.toml:12-17` records the concrete maintenance cost of that transitive surface:
  `RUSTSEC-2024-0436` marks the `paste` proc-macro unmaintained. The exception remains justified
  only until the pinned Parquet stack stops depending on it; this PR does not change that policy.

The finding is therefore not “procedural macros are irrelevant.” Walrus already pays for dependency-
owned derives, but none of its seven manifests makes the separate commitment to author compile-time
code.

## Every codegen need walrus has, and what already meets it

The four persisted control string enums — `ManifestKind`, `ManifestStatus`, `ReloadFlavor`, and
`ReloadStatus` — share one declarative `string_enum!` implementation from PRs 24.2–24.6. Their input
is already an explicit variant table, which `macro_rules!` can repeat without inspecting a type.

The transparent-integer SQLx implementations are hand-written on purpose. The comment beside
`ManifestId`'s `sqlx_support` says this avoids enabling SQLx's `macros` feature in `common`; `Lsn`'s
native `pg_lsn` implementation likewise delegates its known wire representation directly. Neither
case needs to discover fields or types from a Rust item.

## Why not “just add it anyway”

A direct procedural-macro dependency is host-compiled before every downstream crate and is paid
again in clean CI and Docker layers. The project already chose the opposite trade in the
`Cargo.toml:71-75` release-profile precedent (the same `[profile.release]` rationale now appears at
lines 196–203 after later workspace-lint additions): it kept default codegen units because a small
runtime gain was not worth roughly doubling the release build when Phase 5's goal was cutting
CI/Docker build time. Adding a derive crate without a derive-shaped need would cut against that
standing decision while also adding a review and advisory surface.

## What would reopen this

Reopen the decision for **a derive whose input is a type definition**: for example, generating a
per-field Arrow schema or a `TupleValue` fan-out directly from a `struct`'s fields. A declarative
`macro_rules!` macro cannot iterate an already-declared struct's fields or read their types; requiring
callers to restate that definition as a second token table would create the duplication a derive is
meant to remove. At that boundary, `syn` with the narrow `derive` feature, `quote`, and
`proc-macro2`-based unit tests would be justified.

## How it is enforced

`scripts/proc-macro-guard.sh --check` scans workspace manifests rather than `Cargo.lock`, so required
transitive proc-macros remain legal. It rejects direct `syn`, `quote`, or `proc-macro2` declarations
written as plain keys, workspace-inherited keys, inline tables, or dependency tables. Its
`--self-test` exercises those forms only in a temporary directory. The CI `supply-chain` job runs the
guard on every push, including docs-only changes, without installing Rust or compiling the tree.

## The two-crate split (PR 24.8)

### What the rule prescribes and why the split is forced

A library that authors a procedural macro needs a dedicated implementation crate such as
`mycrate-derive`, with `[lib] proc-macro = true`, plus an ordinary `mycrate` facade that re-exports
the macro and owns its runtime API. The split is a compiler constraint, not a naming preference: a
proc-macro crate is built for the host and may export procedural macros, but not ordinary public
traits, types, or functions.

The expansion runs in the caller's crate, so its runtime helpers must be reachable by an absolute
facade path such as `::mycrate::__private::helper`. Walrus already solves the declarative version of
that problem: PR 24.4 made `string_enum!` reach its defining crate with `$crate`, and PR 24.5 placed
the helper behind `#[doc(hidden)] pub mod __private`. Those choices adopt the rule's hygiene and
hidden-helper principle without requiring a procedural-macro crate.

### Current workspace and facade evidence

The audited source baseline is `10f40f53f2e19c4e24daf8deca32afef2085bfb6`. Its root manifest
lists six path-only members, for seven manifests including the root. No member is published; only
`tests/e2e/Cargo.toml:7` says so explicitly with `publish = false`, while the other five member
manifests omit the key. No member declares `description`, `repository`, `documentation`, or
`readme`, and all five other workspace packages name `common` as a path dependency.

`common` already provides the workspace-facing facade. It exposes 13 ordinary public modules and
re-exports 25 root items, alongside the exported `string_enum!` macro. The macro's implementation
module remains private while its separate `#[doc(hidden)]` public `__private` namespace exposes the
one helper that downstream expansions must resolve.

### Current candidate census

Walrus has exactly two declarative macro definitions, and each consumes all the tokens its output
requires:

- `string_enum!` has seven invocations. Each supplies the enum name, attributes, error path, column
  literal, and complete variant-to-string table, so no already-declared item needs inspection.
- `typed_reload_row!` has four invocations. It accepts a row expression and expands the one fixed,
  known conversion into `ReloadRow`; it does not discover fields from a type definition.

`num_builder!` was the third until the `trait-blanket-impl` audit retired it: its five invocations
named `PrimitiveBuilder<T>` aliases, so one blanket impl bounded on `T: ArrowPrimitiveType` where
`T::Native: FromStr` emits the same `ArrowNumBuilder` bodies with no tokens to supply
(`crates/pg-to-arrow/src/batch.rs`).

The transparent integer IDs are also current implementations, not deferred candidates.
`ManifestId`, `EpochNo`, `SchemaVersionNo`, and `ReloadId` already have their representation checks,
conversions, formatting, and hand-written SQLx delegation; serde is present where their existing
wire formats require it. If consolidating that repetition ever becomes worthwhile, a declarative
macro can accept the identifier and attributes as input. It does not need a derive to walk an
already-declared type's fields. `Lsn` remains deliberately bespoke because it delegates Postgres's
native `pg_lsn` representation rather than the IDs' `int8` boundary.

### Decision, reopening trigger, and enforcement

There is no current derive-shaped problem and no external facade consumer or implementation/facade
version skew to hide, so walrus does not add a seventh, host-compiled crate. The reopening trigger
from PR 24.7 remains unchanged: when walrus needs a derive that must inspect fields or types from an
already-declared item, use the rule's proc-macro implementation crate plus ordinary facade split.

Guard invariant 2 rejects a `proc-macro` library setting in any workspace manifest. Invariant 3
uses locked, offline `cargo metadata --no-deps` output to require exactly six packages. The existing
always-on `supply-chain` step runs those checks together with PR 24.7's direct-dependency invariant,
so changing the workspace shape requires updating this decision and its guard in the same review.
