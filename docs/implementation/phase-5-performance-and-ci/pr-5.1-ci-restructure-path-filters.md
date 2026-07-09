# PR 5.1 — CI restructure: drop the redundant build, skip heavy jobs on docs-only changes

> **Phase:** 5 — Performance & CI · **Crates touched:** none (`.github/workflows/ci.yml` only) ·
> **Est. size:** S · **Depends on:** PR 4.11 · **Unlocks:** PR 5.2

Cuts pure waste out of the existing workflow before any caching work lands. Two changes: (1) the
`gates` job currently runs `cargo clippy --all-targets --all-features`, then `cargo build --workspace`,
then `cargo test --workspace` — the middle step proves nothing clippy + test don't already prove and
pays a second dev-profile pass; (2) `on: [push, pull_request]` has no path awareness, so a README typo
fix runs all 8 jobs, including three bundled-DuckDB compiles and a Docker image build. After this PR, a
docs-only commit finishes CI in under a minute, and every code push does strictly less work.

## Why — learning objectives

By the end of this PR you will have practised:

- **What each cargo gate actually proves** — `clippy --all-targets` type-checks and lints every
  target (lib, bins, tests, benches); `test` builds and runs them. A separate `build` step is a
  third compile that adds no new signal.
- **Change-detection in GitHub Actions** — a cheap first job that classifies the diff
  (`dorny/paths-filter`), with downstream jobs gated on its outputs while remaining
  required-check-safe (a skipped-because-docs-only job must not leave a PR unmergeable).
- **Workflow hygiene** — de-duplicating repeated step blocks (the free-disk-space reclaim appears
  verbatim in `gates`, `integration`, and `images`).

## Read first

- `.github/workflows/ci.yml` — the whole file; note the three `Free disk space` copies, the
  `gates` step order, and which jobs compile Rust at all (`gates`, `integration`, `conformance`,
  `images`) vs which never do (`compose`, `supply-chain`, `msrv`, `manifests`).
- `../README.md` — "CI grows with the phases": this PR adds a row.
- External: `dorny/paths-filter` README (the `filters:` syntax and job `outputs`); GitHub docs on
  required status checks vs skipped jobs (a job that is `if:`-skipped reports success — unlike a
  workflow-level `paths:` filter, which reports *nothing* and can wedge required checks).

## Scope

**In scope**

- Remove the `cargo build --workspace` step from `gates` (keep fmt → clippy → test, cheapest first).
- Add a `changes` job (runs in seconds) exposing an output like `code: true|false` from a filter
  over `crates/**`, `tests/**`, `migrations/**`, `deploy/**`, `scripts/**`, `Cargo.*`,
  `rust-toolchain.toml`, `justfile`, `deny.toml`, `.github/workflows/**`. Everything else
  (`docs/**`, `*.md`, `LICENSE`) counts as docs-only.
- Gate `gates`, `integration`, `conformance`, `images`, and `compose` on `needs: changes` +
  `if: needs.changes.outputs.code == 'true'`. Leave `supply-chain`, `msrv`, `manifests` ungated
  (they are already ~30 s and `manifests` should still gate on `deploy/**` changes if you prefer).
- Hoist the duplicated free-disk-space block into a single composite action
  (`.github/actions/free-disk-space/action.yml`) used by the three jobs that need it.

**Explicitly deferred** (do *not* build these here)

- sccache / C++ object caching → **PR 5.2**. This PR only removes work; 5.2 makes the remaining
  work cheaper.
- Dockerfile/BuildKit changes → **PR 5.3**.

## Files to create / modify

```
.github/workflows/ci.yml                     # modify — drop build step; add changes job; gate heavy jobs
.github/actions/free-disk-space/action.yml   # new — composite action replacing 3 copies
docs/implementation/README.md                # modify — CI-grows table row for 5.1
```

## Skeleton

```yaml
# .github/workflows/ci.yml  (shapes only)
jobs:
  changes:
    runs-on: ubuntu-latest
    outputs:
      code: ${{ steps.filter.outputs.code }}
    steps:
      - uses: actions/checkout@v4
      - uses: dorny/paths-filter@v3
        id: filter
        with:
          filters: |
            code:
              - 'crates/**'
              - 'tests/**'
              - 'migrations/**'
              - 'deploy/**'
              - 'scripts/**'
              - 'Cargo.*'
              - 'rust-toolchain.toml'
              - 'justfile'
              - 'deny.toml'
              - '.github/**'

  gates:
    needs: changes
    if: needs.changes.outputs.code == 'true'
    steps:
      - uses: ./.github/actions/free-disk-space
      # fmt → clippy → test.  NO `cargo build --workspace` step.
```

```yaml
# .github/actions/free-disk-space/action.yml
name: Free disk space
description: Reclaim runner disk headroom for the bundled-DuckDB build
runs:
  using: composite
  steps:
    - shell: bash
      run: |
        sudo rm -rf /usr/share/dotnet /opt/ghc /usr/local/lib/android \
          /opt/hostedtoolcache/CodeQL /usr/local/share/boost /usr/share/swift || true
        df -h /
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `gates` runs exactly `fmt --check` → `clippy --all-targets --all-features -D warnings` →
      `test --workspace`; the standalone `cargo build --workspace` step is gone.
- [ ] A **docs-only** commit (e.g. touching only this file) runs `changes` + the cheap jobs and
      skips `gates`/`integration`/`conformance`/`images`/`compose` — and the PR is still mergeable
      (skipped gated jobs report as passing, not pending).
- [ ] A **code** commit runs everything, exactly as before.
- [ ] The free-disk-space block exists once, as a composite action, used by all three heavy jobs.
- [ ] The PR description records before/after wall-times for `gates` on a warm cache (evidence the
      dropped step actually saved time).
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check` / `clippy` / `test --workspace` (unchanged — this PR is CI-only)
  - [ ] the full workflow green on the PR itself

## Hints & gotchas

- Use `dorny/paths-filter` (a job-level gate), **not** workflow-level `on.push.paths` — the latter
  makes required checks report *nothing* on filtered pushes, which wedges branch protection.
  An `if:`-skipped job reports success.
- `paths-filter` on `pull_request` diffs against the base branch automatically; on bare `push` it
  needs the checkout first (hence `actions/checkout` before the filter step).
- Keep `.github/**` in the `code` filter — a workflow edit must always run the full pipeline to
  prove itself.
- Composite actions run from the checked-out tree: the `uses: ./.github/actions/...` step must come
  **after** `actions/checkout`.
- Don't gate `compose` on a whim: it's ~2 min and validates `deploy/docker/**`; it belongs in the
  code-gated set, but if you split filters, give it its own `compose:` filter over
  `deploy/docker/**` + `migrations/**`.

## References

- Design: `../README.md` ("CI grows with the phases", "green" definition).
- Prev: [PR 4.11](../phase-4-end-to-end/pr-4.11-deferred-goal-scaffolding.md) ·
  Next: [PR 5.2](./pr-5.2-ci-sccache.md) · [Roadmap](../README.md)
