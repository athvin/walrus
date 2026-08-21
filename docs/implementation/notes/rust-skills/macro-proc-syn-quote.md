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
