# PR, merge & bookkeeping playbook

Read by the orchestrator once per session. Exact command sequences for the
fragile PR / merge / mark-done steps; the loop follows these verbatim.

## Contents

1. [PR creation](#1-pr-creation)
2. [Loop-state ledger](#2-loop-state-ledger)
3. [Merge sequence](#3-merge-sequence)
4. [Post-merge](#4-post-merge)
5. [The mark-done PR](#5-the-mark-done-pr)
6. [Stop-report template](#6-stop-report-template)
7. [Conflict recipe](#7-conflict-recipe)
8. [Follow-up fix PRs](#8-follow-up-fix-prs)

## 1. PR creation

- Branch: the `branch` field from `next_task.py` JSON — the task file's own slug
  (`pr-9.1-own-borrow-over-clone`). Always cut from an up-to-date `main`.
- Title: the `pr_title` field **verbatim** (`PR <id> — <task H1 title>`). It
  becomes the squash subject; never compose it by hand. For a slice, append the
  slice marker: `PR 8.4a — ManifestId newtype (domain-ID newtypes, slice 1 of 4)`.
- Body (strict skeleton, flexible prose inside each section):

```markdown
Implements `docs/implementation/<path to task file>`.

## What changed
<2–5 bullets: what shipped and the decisions behind it>

## Definition of Done
<the task's DoD restated with a status per item; the task FILE's own boxes are
 LEFT UNTOUCHED — the mark-done PR ticks them after this one merges>

## Green locally
<the CHECK: lines from run_gate.sh, including the SKIP:reasons — a reader must
 be able to see which gates only CI can prove on this machine>

## Deviations
<"None." | what deviated from the task and why>

<!-- walrus-loop: phase=ci attempts_impl=1 attempts_ci=0 reruns=0 fingerprints=[] -->
```

- Command: `gh pr create --base main --title "<pr_title>" --body-file <tmpfile>`.

## 2. Loop-state ledger

Durable per-task state lives in an HTML comment as the **last line of the PR
body** (session context dies; the PR body does not):

```
<!-- walrus-loop: phase=<implement|ci|merge> attempts_impl=N attempts_ci=N reruns=N fingerprints=["…"] -->
```

- Update: `gh pr view <N> --json body -q .body` → rewrite **only** the marker
  line → `gh pr edit <N> --body-file <tmpfile>`.
- On resume, re-read the marker before doing anything else: counters only ever
  increment, never reset — that is what keeps the caps monotonic across session
  deaths.
- `reruns` counts flake reruns (`gh run rerun --failed`) separately from
  `attempts_ci`, so a flaky `e2e` job never consumes a real fix round.

## 3. Merge sequence

Pre-merge invariants, all required:

1. `ci_status.sh` verdict `PASS` on the current head.
2. Base freshness after a fresh `git fetch origin`:
   `git merge-base --is-ancestor origin/main <branch>`. If main moved: rebase →
   re-run the task's gates → `git push --force-with-lease` → re-poll CI → retry.
   A stale-but-textually-clean base is the classic semantic-conflict window
   (walrus's shared `common` types make it a real risk); never merge across it.
3. `MERGEABLE=UNKNOWN` (GitHub computes mergeability asynchronously): re-poll
   `gh pr view <N> --json mergeable` every 15s for up to 60s before deciding.

Then, in one command:

```
gh pr merge <N> --squash --delete-branch \
  --match-head-commit <HEAD_SHA from ci_status.sh> \
  --subject "<pr_title> (#<N>)"
```

- `--match-head-commit` closes the race between the green verdict and the merge
  call — the merge fails rather than merging a head that was never verified.
- Explicit `--subject` because GitHub uses the single commit's message, not the
  PR title, for one-commit squashes — without it main's history drifts from the
  roadmap ids.
- Transient failure mentioning "not mergeable" / "clean status": retry **once**
  after 15s.
- Review-required or branch-protection errors: **STOP**. Never `--admin`, never
  change protection settings.

## 4. Post-merge

```
gh pr view <N> --json state,mergedAt      # must show MERGED
git switch main && git fetch origin && git merge --ff-only origin/main
```

## 5. The mark-done PR

walrus records "done" **in the repository**, not only on GitHub: the task file's
`> **Status:** ✅ Done — <url>` marker, its ticked DoD boxes, and the roadmap row
in `docs/implementation/README.md`. All three ship as their own docs-only PR
after the code PR merges.

Why split from the code PR:

- The DoD's "…green in CI" boxes are only genuinely true once the code PR's CI
  went green — ticking them in the same PR asserts something not yet proven.
- A docs-only diff skips the eight code-gated CI jobs (`changes` classifies the
  diff; skipped jobs still report success), so the bookkeeping costs ~2 minutes
  instead of another full bundled-DuckDB build.

```
git switch main && git fetch origin && git merge --ff-only origin/main
git switch -c pr-<id>-mark-done
python3 .claude/skills/implementing-walrus-roadmap/scripts/mark_done.py <id> --pr <url>
git add <exactly the FILES= paths the script printed>
git commit -m "docs: mark PR <id> done (Status ✅, DoD ticks, README box)"
git push -u origin pr-<id>-mark-done
gh pr create --base main --title "PR <id> — mark done (Status ✅, DoD ticks, README box)" \
  --body "Bookkeeping for #<N> (merged). Docs-only: task marker + DoD ticks + roadmap row."
```

Then poll `ci_status.sh <pr> --wait 900 --grace 300` and merge with the §3
invariants. On push rejection (main moved): `git pull --rebase` and retry, twice
max — the edits touch one row and one file section, so the rebase is clean.

Slices and phase closers take a `--note`:

```
mark_done.py 8.4 --pr 123 --note "ManifestId slice only"
```

**Drift** — a ☐ row whose task file already says Done (`next_task.py` returns
`VERDICT=DRIFT`) is a mark-done that only half landed. Fix it exactly like the
above but on the deterministic branch `chore-reconcile-roadmap-<first-drift-id>`,
covering every currently drifted task in one PR. Always route it through
`preflight.sh --reconcile <first-drift-id>` so an existing local/remote branch,
open PR, merged PR, or human-closed PR is resumed rather than duplicated. Never
re-implement a task that already merged.

The same reconcile branch is used when both signals are still unset but the
code and mark-done PRs already merged. That validator-consistent case is allowed
only when reconcile-mode preflight independently proves exactly one merged PR
for each deterministic branch and emits `CONSISTENT_UNSET_PROOF=yes`; pass its
`MERGED_CODE_PR` to `mark_done.py`. A selector verdict alone is never merge
proof, and a merged reconcile is complete only when the target task reports
`BOX=checked` and `MARKER=done`.

## 6. Stop-report template

Every stop emits exactly this block (one line per key):

```
TASK=<id>
PHASE=<select|route|implement|gate|pr|ci|merge|mark-done>
BRANCH=<branch or ->
PR=<number or ->
VERDICT=<last script verdict>
FAILING=<check names or ->
WHY_STOPPED=<one sentence>
HOW_TO_RESUME=re-invoke the skill — preflight routes automatically; <plus any decision only the operator can make>
```

## 7. Conflict recipe

On the task branch: `git fetch origin && git rebase origin/main`.

- Conflicts confined to `docs/implementation/README.md` roadmap rows: take both
  rows and `git rebase --continue`.
- `.sqlx/` conflicts: never hand-merge the cache. If the daemon is up, resolve
  the Rust/SQL side and regenerate with `cargo sqlx prepare --workspace`;
  otherwise `git rebase --abort` and STOP.
- Any other conflict: **one** fixer-subagent attempt with the task context; if it
  cannot resolve, `git rebase --abort` and STOP.
- Force-push only ever with `--force-with-lease`.

## 8. Follow-up fix PRs

If `main` goes red after a merge (preflight's `MAIN_CI=red` at the next
iteration), branch `pr-<id>-fix-<slug>` from main, fix it, and take it through
the same PR → CI → merge cycle. Never force-push a correction onto `main`, and do
not start the next task until main is green again.
