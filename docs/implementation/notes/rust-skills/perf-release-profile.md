# Release-profile knobs audited — `panic` and `strip` rejected

> **Status:** evaluated — **not adopted**. `[profile.release]` keeps `lto = "thin"` and nothing else.
> Of the knobs this rule adds beyond the already-recorded LTO, codegen-unit, target-CPU and PGO
> decisions, two are no-ops, two — `panic = "abort"` and `strip` — are rejected on *behaviour* rather
> than build cost, and the custom profiles, the dev-dependency override and the bench table are
> rejected on top. `crates/common/tests/build_profile.rs` now guards the two behavioural ones
> alongside this record.

## What the rule asks for

The `perf-release-profile` rule proposes a five-key release profile plus a per-package block:

```toml
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
panic = "abort"
strip = true

[profile.release.package."*"]
opt-level = 3
```

and, separately, `release-dev` / `release-prod` / `profiling` custom profiles, a
`[profile.dev.package."*"] opt-level = 3` block, and a `[profile.bench]` table carrying
`debug = true` / `strip = false` / `lto = "fat"`.

## Disposition, knob by knob

| Knob | Disposition | Where it is recorded |
|---|---|---|
| `opt-level = 3` | no-op — already the release default | here |
| `lto` | **adopted** as `"thin"` (PR 5.7); `"fat"` rejected | `opt-codegen-units.md` |
| `codegen-units = 1` | rejected | `opt-codegen-units.md` |
| `panic = "abort"` | rejected — now guarded | here |
| `strip` | rejected — now guarded | here |
| `[profile.release.package."*"]` | no-op — release already optimises every package | here |
| custom profiles | rejected | here |
| `[profile.dev.package."*"]` | rejected | here |
| `[profile.bench]` | rejected — `bench` inherits `release` on purpose | here |

The whole profile is the last two lines of the workspace `Cargo.toml`: `[profile.release]` and
`lto = "thin"`. The comment block above it carries PR 5.7's measurement and now links this record too.

`opt-codegen-units.md` already listed `panic = "abort"` among the settings it declined to smuggle in,
but on a rationale that is not load-bearing: that walrus "relies on unwinding for `#[should_panic]`
coverage and its end-to-end test harness". Neither would notice the key. `.github/workflows/ci.yml`
runs `cargo test --workspace` (`:151`) under the test profile, which inherits `dev`, and the e2e
harness spawns binaries its job builds in the dev profile too (`:445`) — so a `[profile.release]`
key is not applied to either, and `crates/common/src/lsn_test.rs:127`'s
`#[should_panic(expected = …)]` and `crates/pg-sink/src/reload_signal_test.rs:310`'s `catch_unwind`
never see it. `cargo test --release` would not close the gap either: Cargo builds test and bench
units to unwind whatever the profile says. The suite would stay green while the shipped binary
changed behaviour, which is why the knob has to be rejected on production grounds and why the guard
below reads the manifest instead of trusting the tests to notice. This note supersedes that line; the
same file's codegen-unit and `lto = "fat"` decisions stand untouched, and `strip`, `opt-level`, the
custom profiles and the bench table it explicitly left out of scope are settled here.

## The two no-ops

`opt-level = 3` is what `release` already compiles at; writing it down changes no byte of output. The
same is true of `[profile.release.package."*"] opt-level = 3` — the rule's comment ("keep dependencies
optimized even if main crate changes") describes a problem Cargo does not have here. Dependencies are
already built at the profile's `opt-level`, and walrus declares no per-package override for that block
to defend against. Both would be prose pretending to be configuration: a live assignment restating a
default, in the same tables the guard below parses.

## Why `panic = "abort"` is rejected

Unwinding is load-bearing in *production*, not only in tests.

`crates/pg-sink/src/reload.rs:110-131` classifies how each reload-exporter task left the
controller-owned `JoinSet`: the `Err(error) if error.is_panic()` arm at `:116` logs the panic,
returns `ExporterExit::Panicked`, and leaves the lease to expire so PR 6.9's startup adoption resumes
that table's export. It is reached from three live paths — the drain loop at `:137`, the
abort-stragglers loop at `:149`, and the controller's own `join_next` at `:484`. Under
`panic = "abort"` that arm is unreachable in the shipped binary: the first panicking exporter task
takes the whole sink process down, dropping every other in-flight table with it. That contradicts the
policy the workspace lint table states three screens higher — `panic = "deny"` there is annotated
"a service returns a classified error instead of aborting the pod" — and it would silently convert an
isolated, recoverable failure into a pod restart.

## Why `strip` is rejected

The release profile requests no debug info, so `strip = true` removes no DWARF that is already
absent. What it removes is the symbol table: the names in every `perf` sample, flamegraph frame and
panic backtrace taken against a release build.

`bench` inherits `release`, which `docs/benchmarks.md:12` pins as part of the benchmark methodology,
and `:68` states the consequence in the same breath as the profiling workflow — "`[profile.release]`
carries only `lto = "thin"`, so release and `bench` builds compile without debug info". So `strip` in
`[profile.release]` also strips the Criterion binaries `just bench` builds, which are exactly what a
profiler is pointed at when a bench regresses. Keeping that workflow would need a second table
(`[profile.bench] strip = false`) whose only job is to undo the first, and the release binaries
`scripts/bench-e2e.sh:67` builds and then runs (`:82`, `:91`) would stay stripped regardless.

Against that: no measured benefit. No image-size budget is written down anywhere in this repository,
and the runtime layers are Debian slim plus a package set — `deploy/docker/Dockerfile.loader:47` adds
`tini`, `ca-certificates` and `libstdc++6` — not just the binary. Phase 5's stated north star was
build *time*. Trading readable production and benchmark stack frames for an unmeasured number of
megabytes is the trade this repository already declined for `codegen-units`, in the other direction.

## Why the custom profiles and the dev override are rejected

- **`release-dev` / `release-prod` / `profiling`.** No build surface passes `--profile`: both
  Dockerfiles build with `--release` (`Dockerfile.pg-sink:36,39`, `Dockerfile.loader:38,41`), so does
  `scripts/bench-e2e.sh:67`, and `docs/benchmarks.md` describes one release-derived bench profile.
  Named profiles fork the artifact matrix — cargo-chef cooks per profile, so a second release flavour
  means a second cold DuckDB dependency build — and create a way to benchmark one binary and ship
  another. There is one shipped artifact per binary; there should be one profile that builds it.
- **`[profile.dev.package."*"] opt-level = 3`.** This is the actively harmful one. `duckdb` with
  `bundled` (`crates/loader/Cargo.toml:24`, `crates/pg-to-arrow/Cargo.toml:31`) compiles vendored
  DuckDB C++ from a build script, and Cargo hands a build script the `OPT_LEVEL` of the profile it is
  applying to that package. Today a dev build compiles that C++ at the dev profile's level; the
  override would raise it to `-O3` for every dev-profile build — `.github/workflows/ci.yml:151`'s
  `cargo test --workspace` and the e2e job's separate dev-profile binary build (`:445`) — inflating
  exactly the cold-build cost that PRs 5.1-5.3 and the cargo-chef layering exist to cut. walrus's
  dev-build bottleneck is compiling DuckDB, not running it.
- **`[profile.bench]`.** `bench` inheriting `release` untouched is the property that makes
  `docs/benchmarks.md`'s numbers describe the shipped binary. `debug = true` there is a defensible
  one-off for a profiling session, which is why `docs/benchmarks.md:66-77` reaches for it through the
  environment (`CARGO_PROFILE_BENCH_DEBUG=1`, per shell) instead. As a committed default it would
  change what the benchmark record means.

## The guard

`crates/common/tests/build_profile.rs` grew two checks, and its codegen-unit table scan was
generalised into `profile_key_declaration(manifest, key)` to serve both:

1. `release_profile_policy` rejects a real `panic` or `strip` assignment in **any** `[profile.…]`
   table — including `[profile.bench]` and `[profile.release.package.…]` — and requires the manifest
   to keep linking this file. Sharing the scan with the codegen-unit check means the same properties
   hold: comments are not assignments, so the rationale above the table may keep naming the keys, and
   a key that doubles as a lint name — `panic` sits in `[workspace.lints.clippy]` — counts only under
   a `[profile…]` header. A fabricated manifest carrying exactly that lint line proves the second
   point without touching the real `Cargo.toml`.
2. `profile_key_override_policy` rejects the out-of-manifest spellings on `CARGO_BUILD_SURFACES` —
   the renamed shared list of everything that invokes cargo to build a shipped artifact or a
   benchmark: the CI workflow, both Dockerfiles, the `justfile` and `scripts/bench-e2e.sh`. Each key
   gets the same needle pair the codegen-unit check uses, an environment one and a build-flag one:
   `_PANIC` / `_STRIP` for `CARGO_PROFILE_<name>_<KEY>`, and `panic=` / `strip=` for `-C panic=abort`
   or `--config profile.release.strip=true`. The `=` is what makes the second pair safe — unlike
   `codegen-units`, these two key names are ordinary English words, so a bare needle would fail on any
   surface whose comments discussed a panic. The rustflags route is closed twice over: `-C panic=abort`
   trips `panic=` here and `target_cpu_policy` already rejects *any* rustflags variable on all of these
   surfaces (and on `scripts/sink-smoke.sh`). Debug-only `sink-smoke.sh` stays out of this list for
   the reason it is out of the codegen-unit one: a dev-profile override there retunes one local smoke
   run, not an artifact anybody ships or measures.

The rejected keys that are *not* needled are the ones that change no behaviour: `opt-level` and
`debug`. Both are build-cost knobs a future decision may legitimately want (`[profile.dev]
opt-level = 1` for a faster local loop), and pinning `debug` would collide with the per-shell
`CARGO_PROFILE_*_DEBUG` profiling workflow `docs/benchmarks.md` documents. The
`[profile.dev.package."*"]` override this note rejects is therefore left to review rather than to a
needle; so is a
`RUN strip …` layer in an image, which no needle can distinguish from prose but which is a visible
added line in a Dockerfile diff.

Run the focused guard with:

```sh
cargo test -p common --test build_profile
```

## The re-open trigger

- **`panic = "abort"`** re-opens only if the sink stops isolating task panics — that is, if
  `observe_exporter_end`'s `Panicked` arm and the lease-expiry recovery behind it are deliberately
  removed, so that aborting the process becomes the *designed* response to a panicking task rather
  than a silent downgrade of one.
- **`strip`** re-opens only when an image-size budget is written down and a measured breach is
  attributed to the binary's symbol table, and the adopting change also keeps frames readable —
  either an unstripped artifact archived alongside the image, or `strip` scoped so the `bench`
  profile the benchmark record depends on keeps its symbols.
- **The custom profiles and the dev override** re-open only if walrus starts shipping more than one
  flavour of a binary, or if a dev-build measurement shows dependency optimisation paying for the
  DuckDB recompile it forces.
