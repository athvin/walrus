# Green gates

Read by the orchestrator once per session and by every implementer/fixer.
`scripts/run_gate.sh` implements these definitions; this file is the rationale
and the recipes a script cannot encode.

## Contents

1. [Gate names and commands](#1-gate-names-and-commands)
2. [Gate → CI job map](#2-gate--ci-job-map)
3. [What a gate list contains](#3-what-a-gate-list-contains)
4. [Gates that switch on as phases land](#4-gates-that-switch-on-as-phases-land)
5. [The fix-loop and fingerprints](#5-the-fix-loop-and-fingerprints)
6. [Known flakes](#6-known-flakes)
7. [CI polling recipes](#7-ci-polling-recipes)
8. [Testing layers](#8-testing-layers)

## 1. Gate names and commands

`run_gate.sh <gate>[,<gate>…] [--pkgs a,b]` — one `CHECK:<name>=…` line per
check, a 40-line tail per failure, final `GATE=PASS|FAIL`.

| Gate | Runs | Needs |
|---|---|---|
| `fmt` | `cargo fmt --check` | — |
| `clippy` | `cargo clippy --all-targets --all-features -- -D warnings` | — |
| `test` | `cargo test -p …` (from `--pkgs`, fast signal) then `cargo test --workspace` | — |
| `sqlx` | the **guard** (see below) + `cargo sqlx prepare --check --workspace` | guard always; `--check` needs sqlx-cli + control PG |
| `conformance` | `cargo test -p pg-to-arrow --features conformance` | — |
| `deny` | `cargo deny check` | `cargo-deny` |
| `msrv` | `Cargo.toml` `rust-version` == `rust-toolchain.toml` `channel` | — |
| `compose` | `docker compose … up --wait` + the connectivity smoke | docker daemon |
| `integration` | control migrations, `cargo test -p control --features integration`, then `cargo test --workspace -- --ignored --test-threads=1` | docker daemon |
| `e2e` | `cargo test -p e2e --features it --test reload_quarantine\|reload_scale -- --ignored --test-threads=1` | docker daemon |
| `manifests` | `bash scripts/k8s-validate.sh` | kustomize + kubeconform |
| `images` | `bash scripts/image-smoke.sh` | docker daemon |

`baseline` is an alias for `fmt,clippy,test`.

**The sqlx guard is the load-bearing one.** walrus's control queries are
compile-time checked through the committed `.sqlx` offline cache. Regenerating
that cache needs a live control Postgres — so when the docker daemon is down, a
changed query is *unfixable locally* and only fails after a 20-minute CI round
trip. The guard fails fast when `crates/*/sql/**` changed with no `.sqlx` change.
The rule that follows: type the Rust side (enums via `query_file!` + `FromStr`,
transparent newtypes) and keep the SQL text byte-identical — or STOP and ask the
operator to bring the stack up.

A `SKIP:` is not a pass and not a failure — it is a documented hole. Every
`SKIP:` reason must be carried into the PR body's "Green locally" section so a
reader knows which DoD lines only CI proved.

## 2. Gate → CI job map

CI (`.github/workflows/ci.yml`) fires on **both `push` and `pull_request`**, so
every check name appears twice on a PR head; both copies must pass. The `changes`
job classifies the diff and gates the eight compile-heavy jobs on
`code == 'true'`; an `if:`-skipped job still reports success, which is what lets
a docs-only PR go green in ~2 minutes without wedging required checks.

| CI job | Local gate |
|---|---|
| `changes (classify diff)` | — (decides whether the rest run) |
| `gates (fmt · clippy · test)` | `fmt,clippy,test` |
| `compose (source-pg · control-pg · minio)` | `compose` |
| `integration (control migrations + sqlx offline)` | `integration` + `sqlx` |
| `e2e (full pipeline — quarantine recovery + N-table scale)` | `e2e` |
| `conformance (DuckDB read-back)` | `conformance` |
| `supply-chain (cargo-deny …)` | `deny` (always runs in CI — never gated on `code`) |
| `MSRV (declared rust-version == pinned toolchain)` | `msrv` (always runs in CI) |
| `images (build + PID-1 SIGTERM smoke)` | `images` |
| `manifests (kustomize + kubeconform)` | `manifests` (always runs in CI) |

Timing: the jobs that build the bundled DuckDB (`gates`, `integration`, `e2e`,
`conformance`, `images`) cold-build in ~20 minutes and ~3 with a warm sccache —
hence `--wait 2700` for code PRs. Poll patiently; do not read this as a hang.

## 3. What a gate list contains

`next_task.py` derives each task's `gates` from its own **Definition of Done**
text, so the loop runs what that task claims and nothing else. `fmt`, `clippy`
and `test` are always included; the rest appear when the DoD mentions them
(`docker compose`, `.sqlx`, `--features conformance`, `cargo deny`, `--features
it` / `tests/e2e`, `kubeconform`, MSRV, image smoke).

If a DoD line names a gate the list missed, run it anyway and fix the DoD wording
in the mark-done PR — the DoD is the contract, the derived list is a convenience.

## 4. Gates that switch on as phases land

Mirrors the roadmap's "CI grows with the phases" table:

| From PR | Added gate |
|---|---|
| 0.1 | `fmt --check`, `clippy --all-targets -D warnings`, `test --workspace` |
| 0.6 | compose: `up --wait` → smoke → `down -v` |
| 1.3 | integration vs compose (control PG); `cargo sqlx prepare --check` |
| 2.11 | DuckDB conformance (feature `conformance`) |
| 4.1+ | full `tests/e2e` (feature `it`) |
| 4.7 | `cargo-deny`; MSRV guard |
| 4.8–4.9 | image build + PID-1 SIGTERM smoke; `kubeconform` / kustomize |
| 5.1 | docs-only diffs skip the compile-heavy jobs (`changes`) |
| 7.7 | clippy denies `unwrap_used` / `expect_used` in production code |

Phases 8+ are refactors over a finished tree, so the baseline plus whatever the
touched crate's DoD names is usually the whole story. The exception is a task
that *adds* a gate — PR 12.7 lands a Miri job, for instance. When a task's DoD
names a CI job that does not exist yet, that job is part of the deliverable:
`run_gate.sh` will report it as `SKIP:unknown gate name`, and the local proof is
whatever command the task specifies.

## 5. The fix-loop and fingerprints

Run the gate → read the failure → fix the **code** → re-run the *same* gate →
repeat, then move to the next gate. Never edit a gate command, never `#[allow]` a
lint the task did not sanction, never delete a failing test.

**Failure fingerprint** = the sorted `FAILING=` check names plus the first error
line of each failing job's log. Record it in the PR-body ledger. An identical
fingerprint on two consecutive fix rounds is thrash → STOP with the run URL.

`NO_CHECKS` after the grace window means CI did not register a run at all.
Diagnose once, then stop: `gh run list --branch <branch> --limit 5`,
`gh workflow list`, and check that the head commit actually pushed
(`git rev-parse HEAD` vs `gh pr view <N> --json headRefOid`).

## 6. Known flakes

| Check | Symptom | Response |
|---|---|---|
| `e2e (full pipeline …)` | `reload_quarantine` fails on a bootstrap race — the loader's `/ready` 90s timeout loses to a cold DuckDB start | `gh run rerun <RUN_ID> --failed` and re-poll; **not** a regression, do not send a fixer on the first failure |

`ci_status.sh` prints `FLAKE_CANDIDATE=yes` when *every* failing check is a known
flake. Cap the reruns at 2 per PR (ledger `reruns=`); after that treat it as a
real failure. Because CI runs on both `push` and `pull_request`, one bad commit
can show the same flake twice — rerun the failed jobs of both runs before
concluding anything.

Also flake-adjacent: the compose-based integration tests share one control PG and
bootstrap picks the epoch via `read_current_epoch = MAX`. A locally polluted or
out-of-order control database fails them in ways CI does not. Reset the schema
and run them in CI order before believing a local-only failure.

## 7. CI polling recipes

`ci_status.sh <pr> [--wait <s>] [--grace <s>]` — exit 0 `PASS` · 1 `FAIL` ·
2 `PENDING` · 3 `NO_CHECKS` · 4 `ANOMALY`.

- Code PR: `--wait 2700 --grace 300`. Mark-done / docs-only PR: `--wait 900
  --grace 300`.
- Registration grace: `statusCheckRollup` is empty for a couple of minutes after
  a push while GitHub registers both runs; `--grace` treats that as PENDING
  instead of NO_CHECKS.
- On `FAIL` the script prints `FAILING=` names and one `RUN_ID=` line per failing
  run for that head SHA. Hand both to the fixer; the fixer fetches logs itself
  with `gh run view <run-id> --log-failed` — logs never transit the orchestrator.
- Never substitute `gh pr checks`: it exits 1 for "no checks configured" exactly
  as it does for "checks failed".

## 8. Testing layers (prefer the cheapest that proves the thing)

1. **Pure unit** (ms, no Docker): `Lsn`, `SinkMeta`, the pgoutput decoder, the
   loader transform on an in-memory DuckDB. The two hardest correctness stories
   live here — and with the daemon down, this layer is all that runs locally.
2. **Conformance** (feature `conformance`): write Parquet → read back with
   in-process DuckDB; assert both the inferred type and the value.
3. **Integration** (compose): a crate's `tests/` against a real Postgres / MinIO,
   `#[ignore]`d or behind `--features integration`.
4. **End-to-end** (feature `it`, `tests/e2e/`): both binaries wired together
   against the compose stack.

For any compose-based gate, always tear down (`docker compose … down -v`) so the
next task starts from a clean control database — `run_gate.sh` does this unless
`--keep-stack` is passed.
