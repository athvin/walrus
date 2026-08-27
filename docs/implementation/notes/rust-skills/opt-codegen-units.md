# `codegen-units = 1` rejected (PR 16.9)

> **Status:** evaluated — **not adopted**. `[profile.release]` keeps Rust's default codegen-unit
> count. The possible runtime gain does not justify roughly doubling walrus's release build time;
> `crates/common/tests/build_profile.rs` now guards the decision and this record.

## What the rule asks for

The `opt-codegen-units` rule describes a legitimate runtime optimization: give LLVM one codegen
unit so it can optimize across the entire crate, accepting less parallel compilation. It claims a
potential 5–20% runtime improvement and proposes this full release profile:

```toml
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
panic = "abort"
strip = true

[profile.release-with-debug]
inherits = "release"
debug = true
strip = false

[profile.bench]
inherits = "release"
```

That trade can be appropriate for a latency-critical final artifact. It is not a free compiler
hint: fewer codegen units reduce LLVM parallelism, and fat LTO lengthens the same release link.

## What walrus already decided

PR 5.7 adopted thin LTO and recorded the codegen-unit decision immediately above the release
profile in `Cargo.toml`. The existing rationale is preserved verbatim:

```text
# Release-artifact tuning (PR 5.7). Thin LTO inlines across crate boundaries — a real win on the
# dispatch-heavy hot paths (Arrow `append_value` downcasts, the decoder match arms) for ~10-20 % extra
# link time, measured on the PR 5.4 suite. `codegen-units` is left at the default: cgu=1 buys only a
# few percent more for ~2× the release build — a bad trade for a project whose Phase-5 north star is
# *cutting* CI/Docker build time (PRs 5.1-5.3), and the bench inherits `release` so the numbers count.
```

The chosen profile therefore remains:

```toml
[profile.release]
lto = "thin"
```

## Why the trade is wrong here

Phase 5's build work (PRs 5.1–5.3) explicitly targeted shorter CI and Docker builds. The loader is
the expensive release artifact: `deploy/docker/Dockerfile.loader` runs both `cargo chef cook
--release -p loader` and `cargo build --release -p loader`, and its `duckdb` `bundled` feature
compiles vendored DuckDB C++ from source. The sink image has the same dependency-cook and release
build shape for its own vendored native dependencies. Serializing more LLVM work would spend time
in the part of the pipeline those PRs set out to reduce.

There is no walrus measurement supporting that expense. PR 5.7 measured thin LTO on the Rust
micro-benches as an honest null: −0.8% to +1%, with mixed signs. That does not prove that one codegen
unit can never help, but it gives no evidence that another, costlier whole-program optimization will
move this workload beyond ordinary run-to-run variance.

## What is adopted and what is rejected

- `lto = "thin"` remains adopted from the rule family. PR 16.1 guards it mechanically.
- `codegen-units = 1` is rejected until the re-open trigger below is met.
- `lto = "fat"` is rejected with the single-unit profile because it compounds the release-link
  cost without a walrus measurement.
- `panic = "abort"` is rejected because walrus relies on unwinding for `#[should_panic]` coverage
  and its end-to-end test harness.
- `opt-level`, `strip`, a release-with-debug profile, and a bench-profile change are outside this
  task. They are not smuggled in as part of the codegen-unit decision.

## The guard

`crates/common/tests/build_profile.rs` scans every `[profile.…]` table in the workspace manifest and
fails when a real `codegen-units` assignment appears in any of them — `[profile.release]`, one of the
`bench` / `release-with-debug` / `production` tables the rule's own snippets propose, or a
`[profile.release.package.…]` override. `bench` inherits `release`, so an override parked there would
quietly de-couple `docs/benchmarks.md`'s numbers from the shipped artifact while the release table
still looked untouched. Comments do not count as assignments, so the rationale above the table keeps
naming the key.

A second check covers what Cargo reads from *outside* the manifest: the CI workflow, both
Dockerfiles, the `justfile` and `scripts/bench-e2e.sh` must contain neither a
`CARGO_PROFILE_<name>_CODEGEN_UNITS` environment override nor a `codegen-units` build flag
(`-C codegen-units=…`, `--config profile.release.codegen-units=…`).

The guard also requires the manifest to retain this ADR path and embeds this file with
`include_str!`, so a missing or empty record fails the same test target. Fabricated manifests and
surfaces prove the override, missing-link and environment-override diagnostics without temporarily
editing the real build files.

Run the focused guard with:

```sh
cargo test -p common --test build_profile
```

## The re-open trigger

Re-open this decision only when an interleaved, same-machine comparison of the default release
profile against `codegen-units = 1` shows a repeatable improvement larger than run-to-run variance:
either non-overlapping Criterion confidence intervals with more than a 5% median gain on a named hot
bench, or more than a 5% median throughput/latency gain in a named `just bench-e2e` scenario across
at least five runs per profile. Record clean release build and link times for both profiles at the
same time. Adopt the override only if the CI/Docker release-build budget can explicitly absorb the
observed increase, including the roughly 2× link-time cost that motivated the current rejection.
