# Portable SIMD declined, SIMD crates banned (PR 16.12)

> **Status:** evaluated — **not adopted**. walrus has no arithmetic-dense float-vector workload,
> and `std::simd` remains outside the pinned stable toolchain. `deny.toml` bans the three proposed
> SIMD crates; the pinned supply-chain CI job enforces the decision.

## What the rule asks for

The `opt-simd-portable` rule offers four approaches:

| Approach | Rule's trade-off | walrus decision |
|---|---|---|
| Stable LLVM autovectorization | Excellent portability, low explicit control | Already adopted where structurally useful: iterator/slice forms are preferred in hot loops. |
| `wide` crate | Stable and portable, with explicit vector types | Declined and banned: no measured vector arithmetic justifies a permanent dependency. |
| `std::simd` portable SIMD | Excellent portability and high control | Declined: it requires nightly, while walrus pins stable Rust 1.95.0. |
| Platform intrinsics | Stable APIs with maximum control and no portability | Declined: they require architecture-specific unsafe code, which walrus forbids. |

The rule is right that SIMD can deliver large gains for repeated arithmetic over contiguous numeric
arrays. It is also right that simple iterator loops can let LLVM vectorize without an explicit SIMD
API. The decision here follows the workload, not a blanket objection to vectorization.

## What walrus actually computes with floats

The audited production tree has zero bare `Vec<f32>`, `Vec<f64>`, `[f32]`, `[f64]`, `[f32; N]`, or
`[f64; N]` workloads. There are 38 `f32`/`f64` token mentions across six files:

| file | token count | actual use |
|---|---:|---|
| `crates/common/src/metrics.rs` | 7 | Scalar histogram/gauge values and integer-to-gauge conversions. |
| `crates/control/src/table_ownership.rs` | 2 | Lease TTL seconds bound as scalar PostgreSQL query parameters. |
| `crates/control/src/reload.rs` | 3 | Reload lease TTL seconds bound as scalar query parameters. |
| `crates/pg-sink/src/memory.rs` | 13 | Validated backpressure ratios and one scalar `total / ceiling` decision. |
| `crates/pg-to-arrow/src/geometric.rs` | 8 | Text parsing into scalar point coordinates, radii, and line coefficients. |
| `crates/pg-to-arrow/src/batch.rs` | 5 | Scalar Arrow float builders plus an optional-float append helper. |

`batch.rs` deserves explicit treatment because it has the only slice-shaped float signature:
`push_doubles` accepts `&[Option<f64>]`. It iterates the small, fixed set of already-parsed geometric
fields and appends each scalar to a distinct Arrow child builder. It performs no add, multiply,
reduction, transform, or other lane-parallel arithmetic. The geometric module likewise parses and
stores Postgres text into nested Arrow `STRUCT`/`LIST<STRUCT>` values; `Pt { x, y }` is data shape,
not a numeric kernel.

## Where walrus's time actually goes

PR 5.4 measured the pgoutput decoder and found per-cell `String` allocation in the text branch to
dominate the wide and text-heavy shapes. PR 5.7 isolated roughly 576 ns/row in `SinkMeta` JSON
serialization and improved `append_row` by amortizing batch-constant JSON work. These are allocation,
parsing, formatting, and branch costs—not dense floating-point arithmetic.

The end-to-end measurements reinforce that result: the loader's DuckDB transform saturates before
the sink, and its hot operators live in the bundled C++ engine. Adding a Rust SIMD abstraction does
not address either measured cost centre.

## The stable/nightly wall

`std::simd` still requires `#![feature(portable_simd)]`. `rust-toolchain.toml` pins the explicit
stable 1.95.0 channel so local and CI behavior cannot drift with a new stable release. Moving the
workspace to nightly for an unused numeric abstraction would weaken that reproducibility contract.

Stable third-party alternatives are not free experiments. Every dependency becomes a recurring
advisory, license, source, and ban-policy obligation in the supply-chain job. Without a benchmarked
kernel, that permanent cost has no corresponding measured benefit.

## The half already delivered

PR 16.2 implemented the rule's stable, autovectorization-friendly advice in the two benchmarked hot
modules. It replaced direct indexing with slice patterns and iterator/slice structure, preserved
sequential traversal, and avoided unchecked access. Those forms give LLVM visible bounds and regular
loops without introducing an explicit SIMD API.

That is the applicable half of the rule. This decision declines new vector types and intrinsics; it
does not undo or dismiss compiler-friendly loop structure.

## The ban

The existing `bans.deny` list gains three extended package entries, each with an ADR-linked reason:

- `wide`: the stable explicit-vector API proposed by the rule, banned until a measured vector
  workload justifies the dependency.
- `packed_simd_2`: a nightly-oriented portable-SIMD alternative that conflicts with the pinned
  stable toolchain decision.
- `simdeez`: an intrinsics abstraction whose intended low-level path conflicts with the workspace's
  `unsafe_code = "forbid"` policy.

The list deliberately does not ban `safe_arch` or unrelated transitive implementation crates. The
architectural boundary applies to the three user-facing APIs evaluated here, not every crate that
might contain architecture-specific internals.

The repository pins cargo-deny 0.19.9 in the `supply-chain` CI job. `cargo deny check bans` rejects
any future dependency path to a banned package, and the full `cargo deny check` continues to apply
the advisory, eight-license allow-list, ban, and source policies. The existing
`multiple-versions = "warn"`, `skip = []`, and all other policy families remain unchanged.

## The re-open trigger

Re-open explicit SIMD only when a named Criterion case identifies a contiguous numeric kernel that
performs repeated arithmetic over at least hundreds of values per call and accounts for more than
half of that benchmark's measured time. An implementation candidate must then show a repeatable
improvement beyond run-to-run variance—non-overlapping confidence intervals or more than a 5%
median gain across at least five runs—on every supported architecture.

For `std::simd` specifically, the feature must also be stable on walrus's pinned Rust channel. For a
third-party crate, remove only its own ban after the same PR records the benchmark, license/advisory
review, and portability result. Scalar gauges, ratios, TTL bindings, or parse-and-store coordinate
fields do not satisfy this trigger.
