---
name: implementing-walrus-roadmap
description: Ships walrus roadmap PRs autonomously in a continuous loop. Selects the next ☐ task from docs/implementation/README.md, delegates implementation to a fresh subagent, runs the task's own Definition-of-Done gates locally, opens a PR, polls CI with a footgun-free probe until green, squash-merges, then lands the docs-only mark-done PR (Status ✅, DoD ticks, README box) before repeating until the roadmap is complete. Use when asked to build out the walrus Postgres→DuckDB CDC project PR-by-PR, to 'do the next walrus task/PR', 'continue the walrus roadmap', 'keep building walrus', or to resume a task that is mid-implementation, on an open PR, or merged but not yet marked done.
---

# Implementing the walrus roadmap

Run the walrus task loop: one roadmap task per iteration, from selection through
squash-merge and bookkeeping, repeating until `ALL_DONE`.

**Mode:** this skill *writes the Rust and auto-merges* — the deliberate exception
the repo owner chose for unattended runs. Normal interactive walrus work stays
coach-only (the repo is a learn-Rust project: hint, don't implement). Inside this
skill, stay in the safety rails below at all times.

## Terminology

| Term | Meaning |
|---|---|
| task | one `docs/implementation/phase-N-*/pr-<id>-<slug>.md` file |
| id | the PR id (`9.1`, `8.4a`) — also the roadmap row and branch name |
| row | the task's `\| ☐ \|` line in `docs/implementation/README.md` |
| marker | the task file's `> **Status:** ✅ Done — <url>` line |
| done | row **and** marker both set — written together by the mark-done PR |
| readiness | phase 9+ audit state; only `audited` is selectable (`draft` is a scaffold) |
| outcome | author-decided `change`, `evidence`, or `superseded by PR <id>` |
| gate | the explicit phase 9+ check set `run_gate.sh` runs (legacy phases infer it) |
| orchestrator | this session: runs scripts and git/gh, delegates everything else |
| implementer / fixer | fresh per-task subagents that read docs, write code, commit |
| ledger | the `<!-- walrus-loop: … -->` line in the PR body (durable counters) |

## Hard rules — NEVER

1. NEVER use `gh pr checks` — it exits 1 for both "failed" and "no checks", and
   buckets `cancelled` as not-a-failure. `scripts/ci_status.sh` is the only CI
   probe.
2. NEVER merge on `ANOMALY`, with pending checks, or without the pre-merge
   invariants in `reference/pr-and-merge.md` §3.
3. NEVER `gh pr merge --admin`, change branch protection, or alter repo
   settings. Blocked by protection → STOP.
4. NEVER commit on `main` — not code, not the mark-done edits, not a drift
   reconciliation. One task = one branch = one PR (plus its mark-done PR).
5. NEVER `git add -A`, `-a`, or `.` — stage the explicit paths only. The repo
   routinely carries untracked in-progress roadmap directories.
6. NEVER set the marker, tick a DoD box, or flip a roadmap row before the code
   PR is verified `MERGED` — the "…green in CI" boxes are only true afterwards.
   `scripts/mark_done.py` owns all three edits; do not hand-edit them.
7. NEVER change a compile-time-checked query (`crates/*/sql/**`, `query_file!`
   text) without regenerating `.sqlx`. Regenerating needs a live control PG; if
   the docker daemon is down, keep the SQL byte-identical or STOP and ask. CI's
   `cargo sqlx prepare --check` cannot be satisfied any other way.
8. NEVER build a task's *Explicitly deferred* items, and never cross into
   walrus's permanent non-goals (real-time/synchronous delivery, a second
   coordinating process, multi-slot replication, a query layer over the mirrors).
9. NEVER weaken a gate to make it pass: no `#[allow]` for a lint the task did not
   sanction, no `--no-verify`, no deleting a failing test, no editing a gate
   command. Fix the code.
10. NEVER re-implement a task whose PR already merged — that is drift; reconcile
    it with a docs-only PR (step 9b).
11. NEVER reopen or recreate a PR a human closed unmerged — that is a veto. STOP.
12. NEVER force-push except `--force-with-lease` after a deliberate rebase; never
    touch the memory directory or files unrelated to the current task.
13. NEVER read the design docs, task bodies, or CI logs in the orchestrator —
    delegate those to subagents and run scripts for state.
14. NEVER select a phase 9+ task unless full-corpus validation reports all 265
    tasks audited, linked, serially ordered, tracked, and atomically activated.
    `draft`, a partial Rust table, or malformed explicit metadata means STOP.
15. NEVER let an implementer change a task's predetermined outcome. A failed
    baseline precondition blocks for re-authoring; it does not turn a `change`
    into evidence (or vice versa).

## Scripts

All under `.claude/skills/implementing-walrus-roadmap/scripts/`. Run them; do not
reimplement their logic inline. Each prints `KEY=VALUE` lines and a final verdict.

| Script | Invocation | Output |
|---|---|---|
| `next_task.py` | `python3 …/next_task.py` (also `--status`, `--task <id>`, `--validate-all`) | selection verdict + task JSON (`readiness`, `outcome`, explicit `gates`, `test_packages`, `gate_command`, `verification_commands` included); validation prints progress, outcome counts, `NEXT`, `LAST`, and untracked inputs |
| `preflight.sh` | `…/preflight.sh --wait-main <seconds>` (repo mode) | bounded exact-main-SHA CI wait, then `PREFLIGHT=PASS\|DRIFT\|FAIL` + BRANCH/CLEAN/SYNCED/GH_AUTH/MAIN_CI/**DOCKER**/CARGO_DENY/KUBECONFORM/SQLX_CLI; `DRIFT` permits only step 9b |
| `preflight.sh` | `…/preflight.sh <branch> <id>` | task route, including explicit `PUSH_CI` / `PUSH_MARK_DONE` before polling when local commits are ahead; leaves the checkout where the route needs it |
| `preflight.sh` | `…/preflight.sh --reconcile <drift-id>` | idempotent per-drift branch/PR route: `FRESH_RECONCILE\|CONTINUE_RECONCILE\|PUSH_RECONCILE\|POLL_RECONCILE\|RECONCILE_DONE\|STOP_AMBIGUOUS` |
| `run_gate.sh` | `…/run_gate.sh <gates> [--pkgs a,b]` | `CHECK:<name>=PASS\|FAIL\|SKIP:<reason>` + `GATE=PASS\|FAIL`; omit `--pkgs` for an empty list, while a bare flag or unknown gate is an anomaly |
| `ci_status.sh` | `…/ci_status.sh <pr> [--wait s] [--grace s]` | `VERDICT=PASS\|FAIL\|PENDING\|NO_CHECKS\|ANOMALY` + HEAD_SHA/FAILING/**FLAKE_CANDIDATE**/RUN_ID (exit 0/1/2/3/4) |
| `mark_done.py` | `python3 …/mark_done.py <id> --pr <url> [--note …]` | `VERDICT=MARKED\|ALREADY_DONE\|ERROR` + STATUS_LINE/DOD_TICKED/README_BOX/FILES (idempotent) |

## Session startup

1. Read `reference/green-gates.md` and `reference/pr-and-merge.md` once. Do not
   read the other references — they are for subagents.
2. Keep an in-session ledger: one line per finished task
   (`<id> merged PR#n · mark-done PR#m`). Retain nothing else between tasks.

## The loop

Repeat until `ALL_DONE`. Every step is idempotent; on any interruption,
re-invoking this skill re-enters at step 0 and step 2 routes to the right place.

**0 · PREFLIGHT** — run `preflight.sh --wait-main 2700` at the start of
**every iteration**, including the first and the iteration immediately after a
merge. It first runs `next_task.py --validate-all --require-tracked`; require
`RUST_ROWS=265` and `UNTRACKED_ROADMAP_FILES=0` before it probes GitHub or the
checkout. It may switch a clean interrupted loop branch back to main and then
revalidates the synced main tree. It polls the exact `origin/main` SHA's
`ci.yml` push run to a bounded terminal result, so a newly merged SHA may be
queued/running without being mistaken for green. `PREFLIGHT=PASS` → step 1.
`PREFLIGHT=DRIFT` → step 9b and never implementation. `PREFLIGHT=FAIL` →
STOP with the failing keys; red, absent after the wait cap, anomalous, or
unqueryable main CI never proceeds. A push run on main may treat validator exit
5 as green solely so this verified drift-recovery path can run; the same drift
still fails every branch/PR workflow. Note `DOCKER=` — with the daemon down,
compose / integration / e2e / images gates SKIP locally and CI is the proof.

**1 · SELECT** — `python3 scripts/next_task.py`.
`ALL_DONE` → final report (session ledger + `--status`), end.
`DRIFT` → step 9b, then re-select. `NO_ELIGIBLE` / `PARSE_ERROR` → STOP (the
authoritative state is inconsistent; never guess around it).
`SELECTED` → keep the JSON; its fields are `{ID} {TITLE} {PATH} {BRANCH}
{MD_BRANCH} {SIZE} {READINESS} {OUTCOME} {GATE_COMMAND}
{VERIFICATION_COMMANDS} {PR_TITLE}` below. A phase 9+ task with anything other
than `READINESS=audited` is a parser failure, never work to delegate.

**2 · ROUTE** — `scripts/preflight.sh {BRANCH} {ID}`.
`FRESH` → step 3 · `CONTINUE_IMPL` → step 4 with MODE=continue ·
`PUSH_CI` → push the checked-out branch (use `-u origin {BRANCH}` when
`PUSH_SET_UPSTREAM=yes`), then step 7 ·
`POLL_CI` → step 7 (re-read the ledger from the PR body first) ·
`MARK_DONE` → step 9 fresh · `CONTINUE_MARK_DONE` → step 9 resume ·
`PUSH_MARK_DONE` → push (with `-u` when reported), then step 9c ·
`POLL_MARK_DONE` → step 9c · `RECONCILE` (a merged bookkeeping PR left
both done signals unset; half-landed signals were routed by step 0) → step 9b ·
`DONE` / `STOP_AMBIGUOUS` → STOP (a selected task cannot already be done).

**3 · BRANCH** — `git switch -c {BRANCH} && git push -u origin {BRANCH}`.

**4 · IMPLEMENT** — fill the template in `reference/implementer-prompt.md`
(MODE=implement, or continue when routed) strictly from the step-1 JSON; launch a
fresh subagent. On return:
- `STATUS=blocked` → STOP with its `BLOCKED_ON`.
- `STATUS=needs-another-round` → relaunch MODE=continue. Max **3 rounds** per
  task per session; exceeded → STOP.
- `STATUS=complete` with `GATE=PASS`, the same reported outcome, and every JSON
  verification label reported `PASS` → push, then step 5. Use
  `git push -u origin {BRANCH}` when the route printed
  `PUSH_SET_UPSTREAM=yes`; otherwise use `git push`. Missing/failed labels are
  incomplete even if the prose says the DoD is met.
- `GATE=FAIL` claimed complete → one continue round to fix; recurrence → STOP.
- `SIZE=L` and the subagent reports a blast radius it cannot land green in one
  PR → for legacy phases 0–8, slice per `reference/task-conventions.md` §Slicing.
  For the fixed phase-9+ one-rule corpus, `STATUS=blocked`: re-author that same
  task before selection. Never add a lettered Rust row, partially mark it done,
  or defer an unindexed remainder.

**5 · SCOPE CHECK** — only for tasks sized M or L: launch a one-shot reviewer
subagent with ONLY `git diff main...HEAD --stat`, the diff of the key files, the
task's **Scope** section, and hard rule 8's non-goals; it returns `IN_SCOPE` or
`VIOLATION:<what>`. `VIOLATION` → one continue round to remove it, re-check once;
recurrence → STOP. (Cheap insurance against a subagent helpfully implementing the
next task's scope.)

**6 · PR** — if none exists: create per `reference/pr-and-merge.md` §1 (title =
`{PR_TITLE}` verbatim, body template, ledger marker as the last line).

**7 · CI** — `scripts/ci_status.sh <pr> --wait 2700 --grace 300`. The wait budget
is large on purpose: the bundled-DuckDB jobs cold-build in ~20 minutes (~3 with a
warm sccache).
- `PASS` → step 8.
- `PENDING` at cap → one more `--wait 2700` cycle; still pending → STOP with the
  run URL.
- `FAIL` with `FLAKE_CANDIDATE=yes` → `gh run rerun <RUN_ID> --failed`, then
  re-poll. Max **2** reruns per PR; after that treat the failure as real. Known
  flakes are listed in `reference/green-gates.md` → Known flakes.
- `FAIL` otherwise → update the ledger (increment `attempts_ci`, append the
  fingerprint per `reference/green-gates.md` §Fix-loop). Same fingerprint twice
  OR `attempts_ci` > **3** → STOP (thrash guard). Otherwise launch a fixer
  (MODE=ci-fix with `FAILING=`, `RUN_ID=`, prior fingerprints), push its commits,
  repeat step 7.
- `NO_CHECKS` after grace → run the diagnosis recipe in
  `reference/green-gates.md` once; unresolved → STOP.
- `ANOMALY` → STOP.

**8 · MERGE** — follow `reference/pr-and-merge.md` §3 exactly: freshness
invariant (`git merge-base --is-ancestor origin/main {BRANCH}` after fetch; stale
→ rebase → re-gate → `--force-with-lease` → back to step 7), `MERGEABLE=UNKNOWN`
re-poll, then `gh pr merge <N> --squash --delete-branch --match-head-commit
<HEAD_SHA> --subject "{PR_TITLE} (#<N>)"`, one 15s retry on the transient
not-mergeable race. Anything else → STOP. Then `git switch main && git fetch
origin && git merge --ff-only origin/main`.

**9 · MARK DONE — its own docs-only PR** (never a commit on main, never folded
into the code PR). From fresh main:
`git switch -c {MD_BRANCH}` →
`python3 scripts/mark_done.py {ID} --pr <merged PR url>` (add
`--note "<what actually shipped>"` for a slice) →
stage exactly the `FILES=` it printed → commit
`docs: mark PR {ID} done (Status ✅, DoD ticks, README box)` → push →
`gh pr create` per `reference/pr-and-merge.md` §5.

On `CONTINUE_MARK_DONE`, the router has checked out the existing bookkeeping
branch. Re-run `mark_done.py` idempotently, inspect/stage only its `FILES=`,
commit only if changes remain, then push without force. Use
`git push -u origin {MD_BRANCH}` when `PUSH_SET_UPSTREAM=yes`; otherwise use
`git push`. Create the missing PR. Do not recreate or delete the branch.

**9b · RECONCILE (bookkeeping repair only)** — there are exactly two
authorized entries:

1. Step 0 / selection reported `VERDICT=DRIFT`: retain its `TASK`, `BRANCH`,
   and `MERGED_PR` fields.
2. Step 2 reported `RECONCILE` for the currently selected `{ID}` because both
   signals remain unset despite merged code and mark-done PRs: use `{TASK}={ID}`.

Run `preflight.sh --reconcile {TASK}` in either case. For entry 2 it must print
`CONSISTENT_UNSET_PROOF=yes`, `MERGED_CODE_PR=<n>`, and
`MERGED_MARK_DONE_PR=<n>` after independently proving exactly one merged PR for
both branches; any other result → STOP. The reconcile branch is deterministic:
`chore-reconcile-roadmap-{TASK}`.

- `FRESH_RECONCILE` → create that branch from the checked-out fresh main.
- `CONTINUE_RECONCILE` → continue on the branch the router checked out.
- `PUSH_RECONCILE` → push the checked-out local commits (use `-u` when
  reported), then `POLL_RECONCILE`.
- `POLL_RECONCILE` → poll and merge exactly like step 9c.
- `RECONCILE_DONE` → the prior reconcile PR is merged; return to step 0.
- `STOP_AMBIGUOUS` → STOP. A closed-unmerged reconcile PR is a human veto.

For a fresh/continued entry-2 branch, first run `mark_done.py {TASK} --pr
<MERGED_CODE_PR>`. Then reconcile every currently reported drift in the same
docs-only commit. For each `VERDICT=DRIFT`, use its `MERGED_PR` when present. If
it is `-`, query the exact reported task `BRANCH` and require exactly one merged
code PR before passing that URL to `mark_done.py`; zero, multiple, open, or
closed-unmerged matches → STOP. Re-run `next_task.py` after each idempotent
mark until it no longer reports `DRIFT`, stage only the union of the printed
`FILES=`, and commit only when a diff remains. Push with `-u` for a fresh or
`PUSH_SET_UPSTREAM=yes` branch, otherwise push normally; create the reconcile
PR if none exists, then follow `POLL_RECONCILE`. After merge, the router must
explicitly prove the target is `BOX=checked` and `MARKER=done`; merely getting a
selectable/`ALL_DONE` verdict is insufficient, and any remaining `DRIFT` means
the reconcile PR was incomplete and STOPs. Never re-implement and never recreate
an existing branch or PR. → step 0.

**9c · MARK-DONE CI + MERGE** — `scripts/ci_status.sh <pr> --wait 900 --grace
300`. A docs-only diff skips the eight code-gated jobs (they conclude SKIPPED,
which is a pass), so this is a ~2-minute round trip. Then merge with the same
invariants as step 8. On push rejection to the branch, `git pull --rebase` and
retry, twice max.

**10 · VERIFY + LOOP** — `git switch main && git fetch origin && git merge
--ff-only origin/main`; confirm both squashes landed (`git log --oneline -2`);
`python3 scripts/next_task.py --task {ID}` must print `BOX=checked` and
`MARKER=done`. Append the session-ledger line. → step 0, which waits for the
new exact-main push run and re-asserts `MAIN_CI=green`; red main → STOP and
open a `pr-<id>-fix-<slug>` follow-up before continuing.

## Caps

| Limit | Value | On breach |
|---|---|---|
| implement/continue rounds per task per session | 3 | STOP |
| CI-fix rounds per PR (durable in the PR ledger) | 3 | STOP |
| identical CI failure fingerprint | 2 | STOP |
| flake reruns per PR | 2 | treat the next failure as real → fixer |
| CI wait cycles (2700s each) | 2 | STOP |
| merge transient retry | 1 | STOP |
| mark-done push rebase retry | 2 | STOP |

STOP always emits the report template in `reference/pr-and-merge.md` §6 — the
state is always resumable by re-invoking this skill.

## References

| File | Who reads it | When |
|---|---|---|
| `reference/green-gates.md` | orchestrator + subagents | session start / per task |
| `reference/pr-and-merge.md` | orchestrator | session start |
| `reference/task-conventions.md` | implementer | every task |
| `reference/docs-map.md` | implementer | to route to the design sections a task cites |
| `reference/implementer-prompt.md` | orchestrator (as template) | every delegation |

## Operator notes

An unattended run needs these allowlisted (documented here, never self-applied):
`git switch/push/fetch/rebase/merge`, `gh pr create/view/edit/list/merge`,
`gh run list/view/rerun`, `cargo *`, `docker compose *`, `just *`, and
`python3 .claude/skills/implementing-walrus-roadmap/scripts/*` plus the scripts
themselves. Required: `git`, `gh` (authenticated), `python3`, `cargo` with the
pinned toolchain. Optional, and reported by preflight because they decide which
gates can run locally: a live docker daemon (compose / integration / e2e /
images), `sqlx-cli` (`.sqlx` regeneration), `cargo-deny`, `kustomize` +
`kubeconform`.
