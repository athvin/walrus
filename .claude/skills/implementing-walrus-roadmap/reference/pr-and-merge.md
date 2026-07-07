# PR, merge & verify

The mechanics for turning a green local task into a merged PR on `main`, then confirming the merge.

## Contents
- Branch naming
- Open the PR
- Drive CI green
- Mark the task done (after CI is green)
- Merge (hard-gated on green CI)
- Verify the merge into main
- Reconcile roadmap drift
- Follow-up fix PRs
- Update the PR description

## Branch naming

`pr-<phase>.<n>-<slug>`, matching the task file's slug — e.g. the file
`phase-0-foundations/pr-0.1-workspace-skeleton-and-ci.md` → branch
`pr-0.1-workspace-skeleton-and-ci`. Always branch from an up-to-date `main`:

```
git checkout main && git pull --ff-only
git checkout -b pr-<phase>.<n>-<slug>
```

## Open the PR

```
git push -u origin pr-<phase>.<n>-<slug>
gh pr create --base main --title "PR <phase>.<n> — <task title>" --body-file <tmp>
```

Body template:

```
Implements task `docs/implementation/<path-to-task>.md`.

## What changed
- <bullet per meaningful change / decision>

## Definition of Done
<paste the task's DoD checklist, every box checked>

Green locally: `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
`cargo test --workspace`<, plus any task-specific gates>.
```

## Drive CI green

```
gh pr checks --watch
```

On any failure: `gh run view --log-failed` (or `gh pr checks` for the run URL), reproduce the
failing gate locally, fix, `git push`, and re-watch. Because you ran the same gates locally, most CI
failures are environment-specific (toolchain pin, cache, compose) — read the log before changing
code. If the **same** check fails after 3 fix attempts, stop and ask the human — do not thrash.

## Mark the task done (after CI is green)

Only once CI on the PR is green — so the DoD's "…green in CI" boxes are genuinely true — record
completion **in the repository** so a task's "done" state is provable from code, not only from GitHub.
Make **one** "mark done" commit on the same branch with three edits, then push it:

1. **Task-file status marker** — add this line directly under the task file's H1 title, above the
   `> **Phase:** …` metadata line:

   ```
   > **Status:** ✅ Done — <PR URL>
   ```

   `scripts/next-task.sh` treats the substring `**Status:** ✅ Done` as an authoritative done signal,
   so this marker (together with the README box) is what makes the roadmap self-describing. Keep the
   exact marker text — the script matches it literally.

2. **Definition-of-Done ticks** — in the same task file, change every `- [ ]` in the *Definition of
   Done* to `- [x]`. Every box is genuinely met now: the local ones from steps D–E, and the "…in CI"
   ones because CI just went green.

3. **README roadmap box** — in `docs/implementation/README.md`, change this task's row from `| ☐ |`
   to `| ✅ |`.

This commit is docs-only, so let `gh pr checks --watch` confirm it stays green before merging.

## Merge (hard-gated on green CI)

Gate the merge on a machine check, not a judgement call — run `gh pr checks` and merge only if it
exits `0` (all checks passed, none pending):

```
gh pr checks && gh pr merge --squash --delete-branch
```

Never `--admin`-override a red or pending merge; never force-merge. For defence in depth, enable
branch protection on `main` requiring the CI status checks, so a red/pending PR is blocked
server-side even if the client-side gate is skipped.

## Verify the merge into main

```
git checkout main && git pull --ff-only
git log --oneline -1          # confirm the squash commit landed
```

Then re-run the baseline gates on `main` (`scripts/green.sh`). `main` must be green after the merge.

## Reconcile roadmap drift

The mark-done commit writes the task-file marker, the DoD ticks, and the README box together, so in a
normal autonomous run these never disagree. Drift only appears from outside that flow — a task
finished without this skill, or a mark-done commit that half-landed during a squash conflict:

- **Marker present, README box still `☐`** (what `next-task.sh` warns about): a mark-done that only
  partly landed.
- **PR merged, but neither marker nor box** (a task completed outside the skill): `next-task.sh` will
  return it as the next task, and the `gh pr list --state merged` cross-check in step A is what
  catches it.

Either way the fix is a documentation-only tick, and it **must still go through a branch → PR →
merge — never commit the reconciliation directly to `main`.** Open a short-lived
`chore-reconcile-roadmap` branch, add the missing marker and/or tick the README box for the affected
task(s), and merge it like any other PR. Do **not** re-implement an already-done task.

## Follow-up fix PRs

If `main` is red after a merge (or a later task reveals a defect in a merged one), open a new branch
`pr-<phase>.<n>-fix-<slug>`, fix it, and take it through the same PR → CI → merge → verify cycle. Do
not force-push corrections onto `main`.

## Update the PR description

If the CI-fix loop changed what shipped, update the PR body so the description matches the final
diff:

```
gh pr edit <n> --body-file <tmp>
```
