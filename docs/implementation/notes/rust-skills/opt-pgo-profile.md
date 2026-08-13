# Profile-guided optimization declined (PR 16.11)

> **Status:** evaluated — **not adopted**. No build surface enables PGO instrumentation or profile
> use. walrus's measured bottleneck is the loader's DuckDB C++ operators, which a Rust profile
> cannot observe; `crates/common/tests/build_profile.rs` guards this decision.

## What the rule asks for

The `opt-pgo-profile` rule proposes an instrument → train → optimize cycle:

1. Build an instrumented release binary with `-Cprofile-generate`.
2. Run representative workloads and merge the raw profiles with `llvm-profdata`.
3. Rebuild with `-Cprofile-use` so LLVM can use observed branch and call frequencies.

It claims a possible 10–30% improvement beyond ordinary release optimization. The rule also
proposes a GitHub Actions job that installs LLVM tools, performs both release builds, trains the
binary, merges the profile, and uploads the optimized artifact. BOLT is offered as a post-link
follow-on, with a further claimed 5–15% from `perf.data`-guided block and function reordering.

Those techniques require a stable, representative training workload and a hot path in code the
instrumenting compiler can actually see.

## Where walrus's time actually goes

The committed measurements point elsewhere:

1. PR 5.6's `mixed` and `wide_text` end-to-end runs identify the **loader** as the first component
   to saturate. The sink sustains roughly 6–7k rows/s with `walrus_sink_inflight_bytes = 0` and no
   spills while loader lag accumulates and later drains.
2. Wide text increases peak loader backlog from 1.72 MB to 11.4 MB—about 6.6×—while the sink still
   remains unblocked. Row width amplifies the loader work rather than exposing a sink CPU ceiling.
3. The 200k-row bulk transaction reaches about 22k rows/s, spills correctly in the sink, and leaves
   no loader lag because one large file amortizes the loader's per-file costs. The bottleneck depends
   on workload shape, not a single stable Rust branch distribution.

PR 5.5's `EXPLAIN ANALYZE` localizes the steady-state loader cost further. The heavy transform is
`WINDOW` → `HASH_GROUP_BY` → `LEFT_DELIM_JOIN` → `HASH_JOIN`; the window/group-by and delimiter join
dominate. `MERGE_INTO` is roughly one seventh of that step, and the TOAST back-scan is already
decorrelated. The measured cost centre is the DuckDB execution plan.

## Why a Rust profile cannot see the bottleneck

`-Cprofile-generate` instruments code emitted by rustc. DuckDB's engine is vendored C++ compiled by
the `duckdb` crate's build script under its `bundled` feature. Its window, hash aggregation, and join
operators are outside rustc's codegen units, so their branch and call frequencies never enter a Rust
profile.

Instrumenting DuckDB would be a separate C/C++ PGO project involving compiler-specific `CFLAGS` and
`CXXFLAGS`, raw-profile compatibility, and profile merging for the bundled engine. This task does
not pretend a Rust flag reaches across that boundary.

## What the Rust side already measured

PR 5.7 measured thin LTO on the `append_row` micro-bench suite as an honest null: −0.8% to +1%,
with mixed signs. That is the same Rust-side suite on which a PGO gain would first be argued. Thin
LTO and PGO are distinct optimizations, but the result gives no evidence that cross-unit Rust
optimization is the missing system-level lever.

The same PR declined an ownership/clone candidate after system measurements showed the sink was not
the limiter. That is the precedent applied here: preserve the measurement and decline an unproven
optimization rather than turn a plausible compiler technique into an unsupported build pipeline.

## What it would cost

PGO needs two differently flagged release builds—instrumented and profile-use—plus workload
execution and profile merging. The loader image is already the slow build because bundled DuckDB is
compiled from source; its Dockerfile records roughly 15–20 minutes for that C++ work when the
cargo-chef layer cannot be reused. Changing `RUSTFLAGS` makes cooked artifacts incompatible across
the PGO passes, as the PR 16.10 target-CPU decision explains.

The closest representative workload is `just bench-e2e`, which boots the compose stack and runs
`mixed`, `wide_text`, or `large_txn`. `docs/benchmarks.md` deliberately marks it local-only and never
a CI job because shared-runner results are hardware-relative and noisy. A durable PGO artifact would
need a stable training workload in CI; walrus has intentionally not made that operational and
maintenance commitment.

No instrumented build, profile data, LLVM tool component, BOLT step, or new Cargo profile is added.

## Answering the rule's own table

| Rule criterion | walrus assessment |
|---|---|
| Use: production deployments | walrus ships production images, but production status alone does not make an unrepresentative profile safe. |
| Use: performance-critical apps | Performance matters, but the measured dominant work is DuckDB C++, outside Rust PGO. |
| Use: stable workload patterns | The measured `mixed`, `wide_text`, and `large_txn` shapes have different saturation behavior; no single CI training distribution is established. |
| Use: sufficient profiling data | No representative Rust profile or committed training fixture exists. |
| Skip: development builds | PGO is not proposed for development, and keeping iteration/build time short is an explicit project goal. |
| Skip: libraries (users can PGO) | The deployables are services, although reusable workspace crates reinforce why a global profile is the wrong policy surface. |
| Skip: highly variable workloads | Table width, text payload, mutation mix, and transaction size materially change where time goes. |
| Skip: quick iteration cycles | Two incompatible release builds, DuckDB compilation risk, and a compose training run conflict with the build-time work from Phase 5. |

The decisive rows are stable workload, sufficient data, and visibility of the performance-critical
code. walrus currently fails all three prerequisites.

## The guard

`crates/common/tests/build_profile.rs` embeds the workspace manifest, the sole CI workflow, both
Dockerfiles, `justfile`, and `scripts/bench-e2e.sh`. It rejects PGO generation, PGO use, and profdata
merging on all six surfaces. Fabricated Dockerfile and justfile inputs prove each diagnostic without
modifying a tracked build file, and the test embeds this ADR so deletion or an empty record fails.

Run it with:

```sh
cargo test -p common --test build_profile
```

## The re-open trigger

Re-open PGO only when end-to-end profiling of the named `just bench-e2e mixed` scenario shows
`pg_to_arrow::BatchBuilder::append_row` accounting for more than half of measured processing time,
while the DuckDB `WINDOW`/`HASH_GROUP_BY`/`LEFT_DELIM_JOIN` transform is no longer the cost centre.
A representative Criterion `append_row` workload must then show a repeatable profile-use win beyond
run-to-run variance—non-overlapping confidence intervals or more than a 5% median improvement across
at least five runs—and the same deterministic training fixture must be practical to run in CI.

That trigger names a Rust hot path rustc can instrument, ties it to a majority of system time, and
requires both a compiler-level delta and an operationally maintainable training workload. Until all
parts hold, PGO optimizes the wrong side of the measured boundary.
