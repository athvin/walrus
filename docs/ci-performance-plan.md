# CI performance plan

Status: phases 1–4 implemented; three-way e2e matrix enabled, measurements pending
Last audited: 2026-09-01

## Objective

Reduce the elapsed time from a code push to a trustworthy green pull request
without moving any existing correctness gate out of pull-request CI. The work
should target:

- median code-PR latency at or below 15 minutes;
- 95th-percentile code-PR latency at or below 25 minutes;
- docs-only latency at or below 1 minute;
- exactly one CI run per PR revision;
- no loss of test, integration, image, conformance, or policy coverage; and
- no more than a 30% increase in median GitHub-hosted runner minutes.

The latency target has priority over runner cost inside that 30% guardrail.

## Current-state audit

CI is defined in `.github/workflows/ci.yml`. It already has several sound
optimizations: heavy jobs run in parallel, stale runs are cancelled, docs-only
changes are classified before expensive jobs, Rust and DuckDB compilation use
sccache, Cargo dependencies/targets use `rust-cache`, and image builds use
BuildKit's GitHub Actions cache.

Recent successful runs show that scheduling and the heavy jobs still dominate
wall time:

<!-- markdownlint-disable MD013 -->

| Run                                                                      | Event       |   Total |   Gates | Integration |       E2E |  Images | Conformance |
| ------------------------------------------------------------------------ | ----------- | ------: | ------: | ----------: | --------: | ------: | ----------: |
| [33283288256](https://github.com/athvin/walrus/actions/runs/33283288256) | PR          | 19m 37s | 18m 55s |      5m 59s |   14m 40s |  6m 11s |       1m 1s |
| [33296032444](https://github.com/athvin/walrus/actions/runs/33296032444) | `main` push | 27m 44s | 27m 24s |     26m 34s |   27m 33s | 24m 36s |     11m 55s |
| [31630144009](https://github.com/athvin/walrus/actions/runs/31630144009) | PR          | 44m 10s |  25m 4s |     26m 56s |   43m 58s | 25m 47s |     12m 11s |
| [31633839240](https://github.com/athvin/walrus/actions/runs/31633839240) | `main` push | 66m 52s | 22m 40s |     23m 12s | 12m 56s\* | 24m 58s |     11m 24s |

<!-- markdownlint-enable MD013 -->

\* The e2e job in run 31633839240 did not start until approximately 54 minutes
after the workflow started. Its execution was only 13 minutes; runner
availability, not test execution, made that run take more than an hour.

### Findings

1. **PR commits commonly execute twice.** The workflow subscribes to
   unrestricted `push` and `pull_request` events. Runs
   [33283285795](https://github.com/athvin/walrus/actions/runs/33283285795) and
   [33283288256](https://github.com/athvin/walrus/actions/runs/33283288256)
   began three seconds apart for the same branch revision. Their concurrency
   groups differ because `github.ref` is a branch ref for one event and a
   pull-request merge ref for the other, so neither cancels the other. Two
   copies can occupy all twenty jobs and delay useful work.

2. **Clippy and unit tests form one serial critical path.** On a warm run,
   Clippy took 3m 42s and `cargo test --workspace` took 12m 29s. On colder runs
   they each take roughly 10–13 minutes because both configurations compile the
   bundled DuckDB dependency. Running them sequentially makes the
   `fmt · clippy · test` job last 19–28 minutes even though they do not depend
   on one another.

3. **The integration job has unrelated build/test families in one queue.** SQLx
   validation, control tests, sink tests, loader tests, and a process smoke test
   run one after another. Cold examples spend about eight minutes in
   `cargo sqlx prepare --check` and another ten minutes when the first loader
   target compiles DuckDB. The control/sink and loader families can use
   independent Compose stacks and runners while remaining serial within each
   family.

4. **CI misses an e2e optimization already present locally.** The repository
   gate runner invokes `reload_quarantine` and `reload_scale` in one
   `cargo test` command so Cargo resolves and builds their shared graph once. CI
   invokes them in two steps. Logs show `libduckdb-sys` being compiled
   repeatedly as the commands alternate test and binary build graphs.

5. **Image targets build serially.** Cold `walrus-pg-sink` and `walrus-loader`
   builds take about six and sixteen minutes respectively, but the loader starts
   only after the sink completes. Buildx Bake can schedule both independent
   targets concurrently and retain separate cache scopes.

6. **Cargo target caches are job-name scoped.** `Swatinem/rust-cache` includes
   the GitHub job ID in its default key. Renaming or splitting jobs starts new
   cache lines, and equivalent workloads in different jobs do not share a target
   cache. sccache remains valuable, but sampled hit rates ranged from 48% to
   100%; downloading hundreds of DuckDB objects still takes minutes. Each stable
   workload therefore needs an explicit, isolated cache key.

The free-disk action, toolchain installation, Compose startup, and policy-only
jobs are not primary bottlenecks. They generally take seconds to about one
minute and should be changed only where a new job no longer needs the disk
cleanup.

## Target workflow structure

Every code PR continues to run all current gates. The desired dependency graph
is:

```text
changes
├── static checks
├── clippy
├── unit tests
├── compose smoke
├── integration: control + SQLx + sink
├── integration: loader
├── e2e: reload targets
├── conformance
└── images: parallel builds -> PID-1 smoke

roadmap, supply-chain, MSRV, and manifests run independently as they do today
```

No Rust API, schema, binary, container runtime behavior, or test assertion
changes as part of this work. Only workflow triggers, job/check names,
scheduling, and cache configuration change.

## Implementation phases

Each phase should land separately so its effect can be measured and reverted
independently.

### Phase 1: stop duplicate runs and duplicate e2e builds

1. Change workflow triggers to run branch CI through pull requests and run push
   CI only after a change reaches `main`. Keep a manual entry point for
   investigation:

   ```yaml
   on:
     push:
       branches: [main]
     pull_request:
     workflow_dispatch:
   ```

   Retain `cancel-in-progress: true` and key concurrency by workflow plus PR
   number, falling back to the ref for `main`/manual runs:

   ```yaml
   concurrency:
     group:
       ${{ github.workflow }}-${{ github.event.pull_request.number || github.ref
       }}
     cancel-in-progress: true
   ```

   The supported pre-merge path becomes an open PR. A push to a non-`main`
   branch without a PR will intentionally not run CI.

2. Preserve the `changes` job and all existing `needs.changes`/`if` conditions.
   A workflow-level path filter must not replace it because a filtered workflow
   emits no check for branch protection.

3. Replace the two e2e Cargo steps with the command already used by the local
   gate runner:

   ```bash
   cargo test -p e2e --features it \
     --test reload_quarantine --test reload_scale \
     -- --ignored --test-threads=1
   ```

   Keep one Compose startup/teardown pair and preserve test serialization. This
   removes repeated feature resolution and linking before adding more runners.

Expected result: one workflow per PR revision, substantially lower queue
pressure and runner usage, and a shorter/more stable e2e job.

### Phase 2: split the serial Rust critical paths

1. Replace `fmt · clippy · test` with three jobs that all depend only on
   `changes`:
   - `static checks`: lock-choice, unsafe, formatting, macro-fragment, and
     OS-thread guards. It needs checkout and the pinned Rust toolchain for
     rustfmt, but not sccache, the Cargo target cache, or free-disk cleanup.
   - `clippy`: `cargo clippy --all-targets --all-features -- -D warnings`, with
     free-disk cleanup and both Rust caches.
   - `unit tests`: `cargo test --workspace`, followed by the isolated
     `cargo check -p common --no-default-features --all-targets` feature seam,
     with the same disk and cache setup.

   Clippy and unit tests start together. Their duplicated runner usage is
   intentional: expected wall time falls from their sum to their maximum.

2. Split integration into two jobs, each with its own checkout, toolchain/cache
   setup, Compose stack, and unconditional teardown:
   - `integration (control + SQLx + sink)` installs SQLx CLI, applies control
     migrations, runs control integration and descriptor tests, checks the SQLx
     offline cache, runs every existing pg-sink ignored target in its current
     order, and runs `scripts/sink-smoke.sh`.
   - `integration (loader)` runs the existing loader ignored targets in their
     current order. Keep `--test-threads=1` and the current per-target ordering
     because these tests derive fixture state from their shared control
     database.

   The core/sink job does not compile bundled DuckDB and does not need free-disk
   cleanup. The loader job retains it. Independent Compose stacks prevent
   cross-family fixture collisions.

3. Keep the separate 30-second Compose smoke job. Its cheap, early signal is
   useful when a Compose or seed change would otherwise fail only after Rust
   setup/compilation.

Expected result: a warm code PR is bounded by the 12–15 minute unit/e2e jobs
instead of a 19–28 minute combined gate. Cold integration compilation occurs in
parallel instead of serially.

### Phase 3: parallelize the remaining independent work

1. Add a CI-only Buildx Bake definition under `deploy/docker/` with `pg-sink`
   and `loader` targets in one default group. Preserve the current Dockerfiles,
   tags, `load: true`, and independent GHA cache scopes. Replace the two serial
   build actions with one Bake invocation; BuildKit schedules both targets
   concurrently. Run `scripts/image-smoke.sh` only after Bake succeeds. Exclude
   integration and e2e test directories from the release build context so a
   newly auto-discovered test target cannot invalidate cargo-chef's dependency
   recipe and force a cold bundled-DuckDB rebuild.

2. The full-pipeline suite now uses a three-entry matrix (`reload_quarantine`,
   `reload_scale`, and `ddl_transactions`). Each matrix entry gets an isolated
   runner and Compose stack, so the tests execute concurrently without sharing
   ports, databases, buckets, or target directories. Keep the combined command
   locally; the matrix is a CI latency tradeoff only. A tiny aggregate job keeps
   the pre-matrix check name as the stable branch-protection interface and goes
   green only if every matrix child passes.

   The 12-minute rule is mechanical: at or below it, retain the lower-cost
   combined job; above it, retain the matrix because fast feedback is the chosen
   priority. Re-evaluate the 30% runner-minute guardrail after the matrix has
   ten samples. The matrix was enabled explicitly on 2026-09-01; its first ten
   runs supply that decision's post-change sample.

Expected result: cold image latency approaches the slower image build rather
than the sum, and e2e latency has a defined route to the slower test rather than
the sum.

### Phase 4: stabilize caches and expose performance

1. Upgrade `mozilla-actions/sccache-action` from `v0.0.10` to the pinned
   `v0.0.11`. Keep `RUSTC_WRAPPER`, `CC`, `CXX`, `SCCACHE_GHA_ENABLED`, and
   `CARGO_INCREMENTAL=0` unchanged. The action and supported environment are
   documented in the
   [official sccache action](https://github.com/Mozilla-Actions/sccache-action).

2. Give each `Swatinem/rust-cache@v2` workload a stable `shared-key`:

   | Job                   | Shared key               |
   | --------------------- | ------------------------ |
   | Clippy                | `clippy-all-features`    |
   | Unit tests            | `test-workspace`         |
   | Core/sink integration | `integration-core-sink`  |
   | Loader integration    | `integration-loader`     |
   | E2E combined          | `e2e-reload`             |
   | E2E matrix            | `e2e-${{ matrix.test }}` |
   | Conformance           | `conformance`            |

   Do not share one target cache between different Cargo profiles or feature
   sets, and do not let concurrent jobs write the same key. Enable
   `cache-on-failure: true` so a failed first build can still warm dependencies
   for the corrected revision. The relevant inputs are documented by
   [`rust-cache`](https://github.com/Swatinem/rust-cache).

3. Keep `sccache --show-stats` under `if: always()`. Append the hit ratio and
   the job's elapsed build steps to `$GITHUB_STEP_SUMMARY`; this is diagnostic
   and must never fail a job.

4. Do not introduce nextest, upload the full `target/` tree as an artifact,
   change the bundled DuckDB linkage, or move heavy jobs to nightly/main-only CI
   in this effort. Current measurements show compile scheduling and duplication,
   not test-process throughput, as the dominant problem. Those alternatives may
   be reconsidered only with new timing evidence.

## Validation and acceptance

Before merging each workflow change:

1. Run `actionlint` against `.github/workflows/ci.yml` and validate the Bake
   definition with `docker buildx bake --print`.
2. Confirm local commands still match the CI commands documented in the roadmap
   gate runner.
3. Use a docs-only PR to prove that every compile, Compose, integration, e2e,
   conformance, and image job is skipped while the workflow still reports its
   required checks.
4. Use a Rust-only PR to prove that every current gate runs exactly once and
   that Clippy and unit tests overlap in time.
5. Use a loader or dependency change to exercise the cold DuckDB path and verify
   target-cache and sccache statistics remain present.
6. Use a Dockerfile-only change to prove both images build concurrently and both
   are available to the unchanged PID-1 SIGTERM smoke test.
7. On a temporary branch, intentionally fail formatting, Clippy, a unit test,
   one test in each integration family, an e2e target, and an image build. Each
   failure must remain visible and red.

After each phase, collect ten successful code-PR runs using
`gh run list`/`gh run view`, record total elapsed time, queue delay, each heavy
job's elapsed time, total runner minutes, and cache hit rates. Append the
results below. A phase is accepted when coverage is unchanged, reliability does
not regress, and it moves the workflow toward the objective. Revert or revise a
phase if p95 latency worsens or median runner minutes rise more than 30% without
meeting the latency targets.

## Results log

<!-- markdownlint-disable MD013 -->

| Phase    | Sample window                         | PR p50 | PR p95 | Median runner minutes | Notes                                              |
| -------- | ------------------------------------- | -----: | -----: | --------------------: | -------------------------------------------------- |
| Baseline | 2026-08-12 to 2026-08-30 sampled runs |  27m\* |  67m\* |      Not yet recorded | Small sample; includes queue delay and cold builds |

<!-- markdownlint-enable MD013 -->

\* Replace the provisional baseline with a ten-run sample before Phase 1 lands.
Keep the run links in the current-state audit as reproducible examples.
