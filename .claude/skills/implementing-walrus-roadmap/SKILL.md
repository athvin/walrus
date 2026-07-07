---
name: implementing-walrus-roadmap
description: Autonomously implements the next unfinished PR in the walrus docs/implementation roadmap. Reads the design docs and the next task file, writes the Rust to satisfy its Definition of Done, runs cargo fmt/clippy/test (and docker compose where required) until green, opens a PR, drives CI green, squash-merges to main, verifies the merge, then loops to the next task until the roadmap is complete. Use when the user wants to build out the walrus Postgres-to-DuckDB CDC project PR-by-PR, or asks to 'do the next walrus task/PR', 'continue the walrus roadmap', or 'keep building walrus'.
---

# Implementing the walrus roadmap

Build walrus (a Postgres → DuckDB CDC pipeline) by working through `docs/implementation/`
one PR at a time, fully autonomously: pick the next task → implement the Rust → get it green
→ open a PR → drive CI green → merge to `main` → verify the merge → repeat until the roadmap
is done.

**Mode:** this skill *writes the code and auto-merges* — a deliberate exception the repo owner
chose for unattended runs. (Normal interactive walrus work stays coach-only.) Stay inside the
safety rails below at all times.

## Orientation (once per fresh run)

1. Start from a clean slate: work from the docs, not from stale context left by an earlier task.
   "Clean slate" means **re-read the canonical material** — it does **not** mean deleting
   anything. Never touch the memory directory.
2. Read the four design docs in full so you hold the whole picture:
   `docs/architecture.md`, `docs/proto-version.md`, `docs/walrus-loader.md`,
   `docs/walrus-pg-sink.md`. On later tasks *within the same run* you already have this — re-orient
   cheaply by reading only the sections a task cites (use `reference/docs-map.md` to locate them).

## Per-task workflow

Copy this checklist into your working notes and tick as you go:

```
Walrus task progress:
- [ ] A. Sync main, select the next undone task (next-task.sh + README + gh cross-check)
- [ ] B. Branch from fresh main (assert no branch/PR already exists for this task)
- [ ] C. Implement to the Skeleton, within Scope
- [ ] D. Green locally — fmt/clippy/test + task gates (stop & ask after 3 failed attempts)
- [ ] E. Every DoD item provable-now is satisfied (the "in CI" boxes wait for step H)
- [ ] F. Open PR (body links task file + pastes DoD + summary)
- [ ] G. Drive CI green (stop & ask after 3 failed attempts)
- [ ] H. Only after CI is green: commit the "mark done" edits, re-confirm CI, hard-gate + squash-merge, verify main
- [ ] I. Loop to next task (or report DONE)
```

**A — Sync & select.** First `git checkout main && git pull --ff-only`, so selection reflects the
true merged state — never select from a leftover feature branch. Run `scripts/next-task.sh`; it
prints the first *undone* task (README box `☐` **and** no `**Status:** ✅ Done` marker), or `DONE`.
Sanity-check against the roadmap in `docs/implementation/README.md`, then cross-check reality with
`gh pr list --state merged` and `git log --oneline`:
- If the selected task's PR is **already merged**, or `next-task.sh` warns of drift (a Status marker
  present while the README box is still `☐`), do **not** implement it — the roadmap is just out of
  sync. Reconcile it via a small PR (`reference/pr-and-merge.md` → "Reconcile roadmap drift"), then
  re-select.
- If `next-task.sh` prints `DONE` → report complete and **stop the loop**.

**B — Branch.** Assert nothing already exists for this task's slug — no local branch, no open or
merged PR (`git branch --list 'pr-<...>'`, `gh pr list --search <slug> --state all`). If something
does, resume/inspect it instead of re-implementing. Then `git checkout -b pr-<phase>.<n>-<slug>`
(matching the task's filename slug, e.g. `pr-0.1-workspace-skeleton-and-ci`). **Never commit on
`main`.**

**C — Implement.** Read the full task file. Create/modify exactly the paths under *Files to
create / modify*; fill the *Skeleton*'s `todo!()` bodies. Honour *Scope*: build the *In scope*
items and **never** the *Explicitly deferred* ones. Follow the repo *Conventions* (thiserror in
libs, anyhow in bins, `tracing` not `println!`, UTC-`Z` time, `(commit_lsn, lsn)` ordering) and the
task's *Hints & gotchas*. Write the tests named in the Skeleton.

**D — Green locally (bounded).** Run the gates and fix until each passes: baseline `scripts/green.sh`
(`cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`,
`cargo test --workspace`) plus the task-specific gates its DoD lists (compose smoke, sqlx offline,
conformance, cargo-deny, kubeconform). **If any gate still fails after 3 fix attempts, stop and ask
the human** — surface the output; do not spin. Details + per-phase gate growth: `reference/green-gates.md`.

**E — Definition of Done.** Verify every DoD item you can prove now (all the local ones). The boxes
worded "…green in CI" are only truly satisfied once CI runs, so you tick them in step H — not here.

**F — Open PR.** Push the branch and `gh pr create` (title `PR <phase>.<n> — <title>`; body links the
task file, pastes the DoD, and summarizes what changed and why). Do **not** mark the task done yet.

**G — Drive CI green (bounded).** `gh pr checks --watch`; on failure `gh run view --log-failed`,
reproduce locally, fix, push, repeat. **If the same check fails after 3 fix attempts, stop and ask the
human.**

**H — Mark done, then merge (only after CI is green).** Now that CI is green the "…in CI" DoD boxes
are genuinely true, so record completion in **one** commit on the branch: add `> **Status:** ✅ Done —
<PR URL>` under the task file's H1 (above its `> **Phase:** …` line), tick that task's DoD boxes
`- [ ]`→`- [x]`, and tick its README roadmap row `☐`→`✅`. Push it; let `gh pr checks --watch` confirm
green on that docs-only commit. Then **hard-gate the merge**: `gh pr checks && gh pr merge --squash
--delete-branch` — merge only if `gh pr checks` exits `0` (all passed, none pending); never
`--admin`-override. Finally `git checkout main && git pull --ff-only`, confirm the squash landed, and
re-run the baseline gates on `main`; if `main` is red, open a `pr-<...>-fix-...` follow-up before
continuing. Full detail: `reference/pr-and-merge.md`.

**I — Loop.** Update the PR description if the CI-fix loop changed anything, then return to **A**.
Continue until `next-task.sh` reports `DONE`.

## Reference (read as needed)

- **`reference/docs-map.md`** — the four design docs, their section index, and which to consult for
  a given task.
- **`reference/green-gates.md`** — exact gate commands, how gates grow per phase, and the fix-loop.
- **`reference/pr-and-merge.md`** — branch naming, PR body template, CI watch, merge, and verifying
  the merge into `main`.

## Helper scripts

- **`scripts/next-task.sh`** — prints the next *undone* task file path (absolute), or `DONE`. Run it
  on an up-to-date `main`.
- **`scripts/green.sh`** — runs the baseline `fmt`/`clippy`/`test` gates from the repo root.

## Safety rails (do not cross)

- Never work on `main`; one task = one branch = one PR — including roadmap-drift reconciliations.
- Stay strictly within the task's *Scope* — never build *Explicitly deferred* items.
- Never mark a Definition-of-Done box green that is not actually green — the "…in CI" boxes wait for
  CI (step H).
- Merge only when a machine check confirms green: gate `gh pr merge` on `gh pr checks` exiting `0`;
  never merge a pending or red PR, never `--admin`-override, never force-push.
- Do not touch the memory directory or files unrelated to the current task.

## Stop and ask the human when

- Any gate — local (step D) or CI (step G) — still fails after **3** fix attempts (surface the logs;
  do not thrash).
- Task selection is ambiguous — `next-task.sh` disagrees with merged PRs in a way you can't
  reconcile safely.
- A merge leaves `main` red and one follow-up fix PR does not restore it.
- A task's Scope is unclear or seems to conflict with a design doc.
