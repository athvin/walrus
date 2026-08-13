# `target-cpu` rejected (PR 16.10)

> **Status:** evaluated — **not adopted**. walrus sets no target CPU and introduces no
> `.cargo/config.toml`. Portable container artifacts remain the policy, guarded by
> `crates/common/tests/build_profile.rs`.

## What the rule asks for

The `opt-target-cpu` rule proposes passing `-C target-cpu=native` through either Cargo
configuration or the `RUSTFLAGS` environment variable. That lets LLVM use the build machine's
instruction set and tune scheduling for its CPU. The rule also offers named CPUs and explicit
`x86-64-v2`, `x86-64-v3`, and `x86-64-v4` ISA floors, plus multi-architecture builds containing a
generic and a feature-specific binary. The expected upside is better autovectorization and use of
instructions such as AVX2, FMA, and BMI on a known deployment target.

This is a real optimization when the build target and runtime hardware are the same known fleet.
That precondition does not hold for walrus's portable images.

## Why it is structurally wrong here

Both binaries are built in one container stage and shipped in another:

- `deploy/docker/Dockerfile.pg-sink:18` uses `rust:1.95.0-slim-bookworm` as the builder,
  line 39 runs the release build, and line 42 starts the `debian:bookworm-slim` runtime stage.
- `deploy/docker/Dockerfile.loader:17` uses the same Rust builder, line 41 runs its release build,
  and line 44 starts its Debian runtime stage.

The builder CPU is therefore not the runtime CPU. `target-cpu=native` would encode the builder
host's ISA in a binary later scheduled elsewhere. An older runtime host would not merely run more
slowly; it could terminate with `SIGILL` on its first unsupported instruction.

There is not even one consistent development meaning of “native.” CI jobs run on
`ubuntu-latest`, while `docs/benchmarks.md` records an Apple M2 reference machine and explicitly
warns that absolute numbers are machine-specific. A native CI binary and a native benchmark binary
would target different architectures and feature sets.

The rule's runtime-dispatch alternative uses `#[target_feature]` and an `unsafe` specialized call.
walrus forbids unsafe code workspace-wide, so that is not a silent fallback for this task.

## The cache cost

The loader Dockerfile's header explains why cargo-chef exists: a cache-tar restore changes mtimes,
reruns the DuckDB build script, and repeats roughly 15–20 minutes of bundled C++ compilation. Its
`cargo chef cook --release -p loader` layer isolates that dependency build so source-only changes
reuse it. The sink uses the same cook/build layering for its vendored native dependencies.

Changing `RUSTFLAGS` changes the compilation environment and invalidates the cooked artifacts. A
target-CPU flag would therefore lose the dependency-layer reuse in addition to making the resulting
image hardware-specific.

## What was measured

At the PR 16.10 baseline, the shipped build surfaces contain zero occurrences of `target-cpu`,
`RUSTFLAGS`, or lower-case `rustflags`: the workspace `Cargo.toml`, the sole workflow
`.github/workflows/ci.yml`, and both Dockerfiles are clean. A repository-root `.cargo/` directory
does not exist, so neither `.cargo/config.toml` nor the legacy `.cargo/config` spelling can inject
the flag.

The source probe is deliberately limited to shipped configuration surfaces; the Rust regression
test and this ADR necessarily name the rejected strings themselves.

## Why not even `x86-64-v2`

A named baseline avoids dependence on the builder's exact CPU, but it still establishes an ISA
floor that walrus has never promised. The images have no hardware compatibility matrix, and an
older host would retain the same illegal-instruction failure mode. Choosing v2, v3, or a named ARM
core without a published deployment contract would replace an accidental baseline with an
unsupported deliberate one.

It would also make benchmark comparisons misleading: the benchmark record says to compare deltas
on one machine, not absolute results across machines. No target-specific delta exists to justify a
new compatibility floor.

## The guard

`crates/common/tests/build_profile.rs` embeds the workspace manifest, the sole CI workflow, and
both Dockerfiles with `include_str!`. It rejects `target-cpu`, upper-case `RUSTFLAGS`, and Cargo's
lower-case `rustflags` form on every surface. A separate CWD-independent check resolves the
workspace root from `CARGO_MANIFEST_DIR` and rejects both Cargo-config filenames. Fabricated inputs
prove the build-flag and file-presence diagnostics without changing any shipped configuration.

Run the focused policy with:

```sh
cargo test -p common --test build_profile
```

The explicit list currently covers the repository's one workflow file. If another shipped workflow
is added, extend `BUILD_SURFACES` in the same change.

## The re-open trigger

Re-open this decision only when **both** prerequisites exist:

1. A per-architecture image matrix builds and labels separate artifacts for every promised runtime
   ISA, with scheduling/deployment guarantees preventing an image from reaching older hardware.
2. A same-machine comparison against a named `docs/benchmarks.md` Criterion case or
   `just bench-e2e` scenario shows a repeatable improvement beyond run-to-run variance—use
   non-overlapping confidence intervals or more than a 5% median gain across at least five runs per
   image variant—and records the cargo-chef/build-time cost.

An image matrix without a measured workload win adds compatibility and cache cost for no benefit;
a benchmark win without a safe per-architecture distribution path still ships a possible `SIGILL`.
