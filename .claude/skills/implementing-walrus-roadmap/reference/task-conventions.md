# Task conventions

Read by every implementer subagent, once per task. What a walrus task file
contains, which parts are binding, and the repo conventions a PR is rejected for
breaking.

## Contents

1. [Task anatomy](#1-task-anatomy)
2. [The literal-DoD-first rule](#2-the-literal-dod-first-rule)
3. [Scope boundary](#3-scope-boundary)
4. [Repo conventions (binding from PR 0.1)](#4-repo-conventions-binding-from-pr-01)
5. [The `.sqlx` constraint](#5-the-sqlx-constraint)
6. [Slicing a task too big for one PR](#6-slicing-a-task-too-big-for-one-pr)
7. [Done state and drift](#7-done-state-and-drift)
8. [Phases 9+ — the Rust-skills phases](#8-phases-9--the-rust-skills-phases)

## 1. Task anatomy

```
# PR 9.1 — <title>                          ← the H1; the PR title is "PR <id> — <title>"
> **Status:** 📋 Planned                     ← flipped by the mark-done PR, never by you
> **Readiness:** audited · **Outcome:** change|evidence|superseded by PR <id>
> **Gates:** fmt,clippy,test[,extra] · **Test packages:** crate-a,crate-b|—
> **Phase:** … · **Crates touched:** … · **Est. size:** S|M|L ·
> **Depends on:** PR 8.5 · **Unlocks:** PR 9.2

<framing paragraph — the concrete measurement or motivation>

## Why — learning objectives   the point of the exercise; keeps the change honest
## Read first                  exact files + design sections; read ALL of them
## Scope                       In scope / Explicitly deferred — a hard contract
## Files to create / modify    exact file allowlist, including any new check scripts
## Skeleton                    signatures, enum variants, test names, todo!() bodies
## Verification commands      fenced `text`: `lowercase-label = exact command`
## Definition of Done          the literal merge contract
## What completed looks like   the observable end state (commands + expected output)
## Hints & gotchas             the traps the author already hit
## References                  design-doc anchors
```

The **Skeleton** gives shapes, not solutions: fill the `todo!()` bodies, keep the
public signatures and the named tests unless the DoD says otherwise.

For phases 9+, both metadata lines and `Verification commands` are mandatory and
machine parsed. `draft` is allowed only while authoring a scaffold; it can never
be selected. Gates must start with `fmt,clippy,test`, use only names documented
in `green-gates.md`, and list packages by Cargo package name. Use the em dash `—`
for no package pre-check; the resulting gate command omits `--pkgs` entirely.

Verification commands are one read-only command per unique lowercase/hyphen
label. Supported forms are `cargo test/check/clippy/fmt --check/doc/bench/metadata`,
`rg` (or `! rg` for an absence assertion), read-only `git diff/show/status/ls-files`,
`test`, `find` without executing/deleting, named Python validators, and repo
scripts in an explicit `--check`, `--self-test`, or `--dry-run` mode. Do not use
redirects, command substitution, pipelines/chaining, mutating Cargo/Git commands,
or a mutation-and-revert self-test. A script that the task will add is valid only
when its path also appears under `Files to create / modify`.
Because implementation is committed before the final report, diff verification
must name the merge-base range (for example `git diff --check origin/main...HEAD`);
a bare post-commit `git diff --check` is vacuous and rejected.

## 2. The literal-DoD-first rule

The Definition of Done is the contract. Before writing code, restate each DoD
line as something checkable, and before declaring done, walk the list and say how
each is satisfied. A DoD line that names a command (`cargo test -p loader --test
ddl_additive`) must be run as written — not a near approximation.

The "…green in CI" boxes are satisfied by the code PR's CI run, and are ticked
later by the mark-done PR. Never tick a box in the task file yourself.

## 3. Scope boundary

- Build every **In scope** item; build **no** *Explicitly deferred* item. A
  deferred seam belongs to a later task — leaving it unfilled is correct, not
  incomplete.
- Do not "improve" neighbouring code the task did not name. A cleanup that looks
  free is a review surprise and, in a refactor phase, often the next task's PR.
- walrus's permanent non-goals are out of bounds regardless of what a task seems
  to invite: real-time/synchronous delivery, a second coordinating process, more
  than the one replication slot, or a query layer over the mirrors.
- Files: create/modify only the paths under the task's *Files to create / modify*
  (or those its Scope names). Never touch `docs/implementation/README.md` — the
  orchestrator owns it.

## 4. Repo conventions (binding from PR 0.1)

| Area | Rule |
|---|---|
| Errors — libraries | `thiserror` enums; terminal-vs-transient modelled, not stringly-typed |
| Errors — binaries | `anyhow` with context; map to `common::ExitCode` at the top of `main` |
| Logging | `tracing` with structured fields (`xid`, `commit_lsn`, `lsn`, `batch_uuid`); never `println!` |
| Async | `tokio` in the binaries and `control`; the pgoutput decode and the loader transform stay **sync + pure** |
| Config | `serde`-typed, env/file loaded, bounds-validated; invalid config is terminal |
| Time | every walrus-stamped datetime is UTC, RFC-3339, `Z` |
| Ordering | everything keys on `(commit_lsn, lsn)` — never max-row-LSN |
| Identifiers | walrus-authored columns are `lower_snake_case` (`_walrus_op`, `_walrus_commit_lsn`, …); source-derived names are quoted only to mirror the source faithfully |
| Lints | `#![deny(warnings)]` + clippy all/deny via `[workspace.lints]`; `unwrap_used`/`expect_used` denied in production (`clippy.toml` re-allows them in test code) |
| Tests | unit tests in a sibling `foo_test.rs` (`#[cfg(test)] #[path = "foo_test.rs"] mod tests;`); golden-vector/conformance tests in `tests/`; e2e feature-gated |
| SQL location | per-crate `sql/<engine>/{queries,templates,test}/`; control's Postgres queries via `sqlx::query_file!` (offline `.sqlx` cache committed); the loader's DuckDB DDL via `include_str!` templates; migrations under `/migrations/{control,source}/` |
| Commits/PRs | one PR per task file; the PR body links the task and pastes its DoD |

Tests first where the task's Skeleton names tests: commit the failing tests, watch
them fail, then implement. It is the cheapest proof that a test tests anything.

## 5. The `.sqlx` constraint

`crates/control` compiles its queries against the committed `.sqlx` offline cache.
Regenerating it (`cargo sqlx prepare --workspace`) needs a live control Postgres.
On a machine with the docker daemon down that is impossible, and CI's
`cargo sqlx prepare --check --workspace` will fail with no local way to fix it.

So: **keep query text byte-identical** unless the stack is up. Type changes go on
the Rust side — enums via `query_file!` + `FromStr`, transparent newtypes over
`i64`, `AS "col: Type"` casts that already exist. If a task genuinely requires
new or edited SQL and the stack is down, report `STATUS=blocked` with
`BLOCKED_ON=needs cargo sqlx prepare against a live control PG`.

## 6. Slicing a task too big for one PR

Precedent: PR 8.4 shipped as **8.4a — ManifestId newtype (slice 1 of 4)**.

This mechanism is **legacy-only (phases 0–8)**. Phases 9–34 are a fixed,
machine-checked 265-rule/265-task bijection: lettered IDs and extra rows are
rejected. If an audited Rust task cannot land green as written, STOP and
re-author that same task before it is selected again. Do not ship a partial
outcome or leave remaining work outside the index.

For a legacy task whose blast radius cannot land green in one PR:

1. Pick the smallest slice that is independently green and useful.
2. Branch `pr-<id><letter>-<slug>`, PR title `PR <id><letter> — <title> (slice k of n)`.
3. In the task file, add a `> **Shipped (PR #N):**` note recording what landed and
   what remains — that note is what stops the next session from re-doing it.
4. The mark-done PR uses `--note "<what shipped>"` so the marker reads
   `✅ Done — <note>: <url>`, and the remaining slices become follow-up tasks.

Slicing is a judgement call the implementer surfaces and the orchestrator
decides — never a silent partial implementation.

## 7. Done state and drift

Done means **both** signals, written together by the mark-done PR: the task file's
`> **Status:** ✅ Done — <url>` marker (plus ticked DoD boxes) and the `| ✅ |`
roadmap row. `next_task.py` reads both; when they disagree it returns
`VERDICT=DRIFT` and the fix is a docs-only reconcile PR, never a re-implementation.

## 8. Phases 9+ — the Rust-skills phases

Phases 9 and up are the one-rule/one-task Rust curriculum over the finished
tree. They differ from phases 0–8 in these ways:

- Each task's **Read first** cites a rule file under
  `.claude/skills/rust-skills/rules/<rule>.md`. Read the cited rule *and* the
  exact source paths/symbols the task names. Line numbers are orientation only;
  stable symbol/path probes and the task's explicit baseline precondition decide
  whether the authored ticket still applies.
- The author audits applicability and locks the outcome before activation:
  `change` implements the bounded adjustment, `evidence` lands
  `docs/implementation/notes/rust-skills/<rule>.md` plus only any explicitly
  named non-production guard/config artifacts, and `superseded by PR <id>` proves
  the earlier owner and lands the specified evidence note. Record the baseline
  commit, exact commands/results, classified findings, conclusion, and the
  condition that would reverse an evidence conclusion.
- A baseline mismatch is an unconditional STOP for task re-authoring. The
  implementer must not invent a new site, outcome, dependency, test, or fallback.
  Later overlapping rules are residual audits; their earlier task owns shared
  changes.
- Most tasks are behaviour-preserving, but task-specific acceptance is the
  authority. Edition migrations, public compatibility checks, metadata/docs
  changes, and similar rules follow the compatibility and verification contract
  written in that task rather than a blanket "no behavior change" assumption.
- Every task-specific verification command must run exactly and be returned by
  label as `PASS`; the baseline gate alone is insufficient.
