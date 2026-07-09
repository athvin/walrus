# PR 5.2 — sccache: stop recompiling DuckDB's C++ on every cache miss

> **Phase:** 5 — Performance & CI · **Crates touched:** none (`.github/workflows/ci.yml` only) ·
> **Est. size:** S–M · **Depends on:** PR 5.1 · **Unlocks:** PR 5.3

The single biggest CI cost is the bundled DuckDB C++ compile (~15–20 min) inside `libduckdb-sys`.
`Swatinem/rust-cache` caches the *finished* `target/` artifacts keyed on `Cargo.lock` — so any
dependency bump, however unrelated, throws the whole thing away and every compiling job pays the
DuckDB build again, in its own profile (`gates` dev, `integration`/`conformance` test). sccache fixes
the class of problem: it caches at the *compiler-invocation* level (individual `.o` files and rustc
crate outputs) in the GitHub Actions cache, so identical compilations are hits **across jobs, across
profiles, and across `Cargo.lock` changes** — the DuckDB sources only actually recompile when the
pinned `duckdb` crate version changes.

## Why — learning objectives

By the end of this PR you will have practised:

- **Where build time actually goes** — `cargo build` timings vs the `cc`-crate C++ compile hidden
  inside a `-sys` crate's build script, and why a `target/`-level cache can't help a busted key.
- **Compiler-wrapper caching** — `RUSTC_WRAPPER` for rustc, `CC`/`CXX` wrappers for the `cc` crate,
  and what makes a compilation cache-*hit* (same inputs, flags, compiler) vs silently cache-*miss*
  (absolute paths, `__DATE__`-style nondeterminism, debug prefix maps).
- **Layered caching strategy** — sccache (compile granularity) and Swatinem (registry + incremental
  metadata) are complements, not alternatives.

## Read first

- `.github/workflows/ci.yml` — after PR 5.1: which jobs compile (`gates`, `integration`,
  `conformance`) and their existing `Swatinem/rust-cache@v2` steps.
- External: `mozilla-actions/sccache-action` README (sets `SCCACHE_GHA_ENABLED`, exposes
  `sccache` on PATH); sccache docs on `RUSTC_WRAPPER` and on caching C/C++ via `CC="sccache cc"`;
  the `cc` crate's env-var handling (it honours `CC`/`CXX` per-target).

## Scope

**In scope**

- Add `mozilla-actions/sccache-action` to `gates`, `integration`, and `conformance`, with:
  - `RUSTC_WRAPPER: sccache` (Rust compilation cache),
  - `CC: sccache cc` and `CXX: sccache c++` (the DuckDB C++ objects — this is the payload),
  - `SCCACHE_GHA_ENABLED: "true"` (GHA cache backend).
- Keep `Swatinem/rust-cache@v2` for the registry and target-dir metadata, but consider
  `cache-targets: false` once sccache is proven (the target dir is the bulkiest part of the
  Swatinem cache and sccache makes rebuilding it cheap; measure before deciding).
- A final `sccache --show-stats` step in each compiling job so hit rates are visible in every run.
- Record measurements in the PR description: (a) fully warm run, (b) a run with `Cargo.lock`
  deliberately perturbed (bump any minor dep) — the case that used to cost ~20 min per job.

**Explicitly deferred** (do *not* build these here)

- sccache inside the Docker image builds → **PR 5.3** (different cache plumbing — BuildKit mounts).
- Dropping bundled DuckDB in CI in favour of a prebuilt `libduckdb` — out of scope for the phase;
  only revisit if sccache turns out not to be enough.

## Files to create / modify

```
.github/workflows/ci.yml             # modify — sccache action + env in gates/integration/conformance
docs/implementation/README.md        # modify — CI-grows table row for 5.2
```

## Skeleton

```yaml
# .github/workflows/ci.yml  (per compiling job — shape only)
  gates:
    env:
      RUSTC_WRAPPER: sccache
      CC: sccache cc
      CXX: sccache c++
      SCCACHE_GHA_ENABLED: "true"
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/free-disk-space
      - name: Install pinned Rust toolchain
        run: rustup toolchain install
      - uses: mozilla-actions/sccache-action@v0.0.9   # pin the exact tag
      - uses: Swatinem/rust-cache@v2
      # ... fmt / clippy / test as before ...
      - name: sccache stats
        if: always()
        run: sccache --show-stats
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] All three compiling jobs run under sccache with the env above, and print
      `sccache --show-stats` at the end of every run.
- [ ] **The proof run:** with a warm sccache but a perturbed `Cargo.lock` (Swatinem's worst case),
      each compiling job's compile phase drops from ~15–20 min to low single-digit minutes, and the
      stats show a high C/C++ hit rate (the DuckDB objects). Before/after numbers in the PR
      description.
- [ ] A fully-warm run is not slower than before (sccache overhead on 100% Swatinem hits is noise).
- [ ] The sccache action tag is pinned (not `@main`).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check` / `clippy` / `test --workspace` (unchanged — CI-only PR)
  - [ ] the full workflow green on the PR itself

## Hints & gotchas

- The GHA cache backend has a 10 GB per-repo ceiling with LRU eviction. The DuckDB objects are the
  high-value entries; if eviction becomes a problem, trim what Swatinem stores
  (`cache-targets: false`) rather than fighting for space.
- `CC="sccache cc"` works because the `cc` crate splits the env var on whitespace and treats the
  first token as the wrapper. Set `CXX` too — DuckDB is C++, and forgetting `CXX` silently caches
  nothing (build still green, stats show ~0 C++ requests: check the stats, not just the wall time).
- Incremental compilation (`CARGO_INCREMENTAL`) and sccache don't mix — sccache refuses to cache
  incremental rustc invocations. CI defaults are fine (`cargo` disables incremental in CI when
  `CI=true`? it does **not** — set `CARGO_INCREMENTAL: 0` at the workflow level to be explicit).
- If Rust hit rates are mysteriously low, look for absolute-path differences first
  (`GITHUB_WORKSPACE` is stable across runs on hosted runners, so this usually only bites
  self-hosted).
- Keep the `Free disk space` step: sccache saves *time*, not *disk* — a cold DuckDB build still
  needs the headroom.

## References

- Design: `../README.md` ("CI grows with the phases").
- Prev: [PR 5.1](./pr-5.1-ci-restructure-path-filters.md) ·
  Next: [PR 5.3](./pr-5.3-docker-build-cache.md) · [Roadmap](../README.md)
