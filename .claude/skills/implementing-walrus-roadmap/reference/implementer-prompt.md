# Implementer prompt template

The verbatim prompt the orchestrator hands to every per-task subagent.
**ALWAYS use this exact structure; fill the placeholders only from
`next_task.py` JSON and `ci_status.sh` output.** Never paraphrase, never add
context the placeholders don't carry — the point is that the orchestrator itself
never has to read the task body or the design docs.

## 1. Placeholders

| Placeholder | Source |
|---|---|
| `{MODE}` | orchestrator: `implement` \| `continue` \| `ci-fix` |
| `{ID}` `{TITLE}` `{PATH}` `{BRANCH}` `{SIZE}` `{CRATES}` | `next_task.py` JSON (`id`, `title`, `path`, `branch`, `size`, `crates`) |
| `{READINESS}` `{OUTCOME}` | `next_task.py` JSON `readiness` and `outcome_text` |
| `{GATE_COMMAND}` | `next_task.py` JSON `gate_command` (already omits `--pkgs` when the package list is empty) |
| `{VERIFICATION_COMMANDS}` | `next_task.py` JSON `verification_commands`, rendered one `label = command` per line; `none` for legacy tasks |
| `{DEPS}` | `next_task.py` JSON `depends_on_text` |
| `{DOCKER}` | `preflight.sh` repo-mode `DOCKER=` value |
| `{FAILURE_CONTEXT}` | ci-fix only: `FAILING=` names + `RUN_ID=` lines from `ci_status.sh`, plus prior fingerprints from the PR-body ledger |

## 2. The template

```text
You are implementing walrus task PR {ID} — {TITLE} in MODE={MODE}.
Branch: {BRANCH} · Size: {SIZE} · Crates touched: {CRATES} · Depends on: {DEPS}
Readiness: {READINESS} · Predetermined outcome: {OUTCOME}
Local docker daemon: {DOCKER}

READ, in this order, before writing anything:
1. {PATH} — the task file, IN FULL (framing, Why, Read first, Scope,
   Skeleton, Definition of Done, What completed looks like, Hints & gotchas).
2. Every file and design section its "Read first" names. Use
   .claude/skills/implementing-walrus-roadmap/reference/docs-map.md to locate a
   cited section instead of reading a whole design doc.
3. .claude/skills/implementing-walrus-roadmap/reference/task-conventions.md.
4. .claude/skills/implementing-walrus-roadmap/reference/green-gates.md
   (sections 1, 5 and 6 — the gate semantics, the fix-loop, the known flakes).

PROCESS (hard rules, no deviation):
- Work ONLY on branch {BRANCH}. Verify first:
  `git rev-parse --abbrev-ref HEAD`. Wrong branch = stop and report.
- For phase 9+, Readiness must be `audited`. Execute the predetermined outcome
  exactly: `change` changes the named sites, `evidence` lands the task's evidence
  note plus only the explicitly listed non-production guard/config artifacts,
  and `superseded by PR <id>` proves the earlier owner and lands the specified
  note. Evidence must not invent a production change. Never substitute a
  different outcome because the baseline moved.
- Run every baseline precondition in the task before editing. A mismatch is
  `STATUS=blocked`: do not reinterpret the ticket or choose a fallback outcome.
- Where the Skeleton names tests, commit the failing tests FIRST
  (`test(PR {ID}): failing tests for <short description>`), watch them fail,
  then implement until green in separate commits.
- Stage explicit paths only. NEVER `git add -A`, `git add .`, or `-a`.
- NEVER push. The orchestrator owns push, PR, and merge.
- NEVER edit docs/implementation/README.md, the task file's Status marker, or
  its Definition-of-Done checkboxes — the orchestrator's mark-done PR owns all
  three, after this PR merges green.
- The task's "Explicitly deferred" list and walrus's permanent non-goals are
  hard constraints. Do not fill a seam that belongs to a later task.
- Do not change compile-time-checked SQL (crates/*/sql/**, query_file! text)
  unless the docker daemon is up and you regenerate .sqlx with
  `cargo sqlx prepare --workspace`. Otherwise keep the query text
  byte-identical, or report STATUS=blocked.
- Follow the repo conventions in task-conventions.md §4 (thiserror in libs,
  anyhow in bins, tracing not println!, UTC-Z, (commit_lsn, lsn) ordering,
  sibling _test.rs files, no unwrap/expect in production code).
- Before declaring done, run this JSON-provided command verbatim from the repo root:
  `{GATE_COMMAND}`
  Iterate until it prints GATE=PASS. Report every
  SKIP:<reason> — a skipped gate is a hole CI will have to prove, not a pass.
- Also run every task-specific command below exactly as written and require a
  zero exit status from each. Do not replace, weaken, combine, or omit one:
  {VERIFICATION_COMMANDS}
- If the task's Definition of Done cannot be met without a decision the task
  does not make, stop and report BLOCKED_ON instead of choosing.

RETURN FORMAT (strict):
STATUS=complete|blocked|needs-another-round
READINESS={READINESS}
OUTCOME={OUTCOME}
GATE=PASS|FAIL
SKIPPED_GATES=<name:reason, … or none>
VERIFICATION_COMMANDS=<every JSON label=PASS, or label=FAIL; `none` only for legacy tasks>
COMMITS=<one line per commit: short-sha + subject>
TESTS_FIRST_SHA=<sha of the failing-tests commit, or n/a>
DOD=<one line per Definition-of-Done item: met | met-in-CI | not-met + why>
DEVIATIONS=none|<what deviated from the task and why>
NOTES=<≤3 lines>
BLOCKED_ON=<one line — only if STATUS=blocked>
```

`STATUS=complete` is invalid unless `GATE=PASS` and every label from
`{VERIFICATION_COMMANDS}` is returned as `PASS`. The orchestrator must reject an
incomplete/missing label report and launch a continue round; it must never infer
success from prose in `NOTES` or `DOD`.

## 3. Mode deltas

Append the matching block to the template.

**`continue`** (the branch already has commits; do not redo committed work):

```text
MODE=continue: this branch already has work. First run
`git log --oneline main..HEAD` and diff the branch state against the task's
Definition of Done. Report one extra line:
ASSESSMENT=<what is done / what remains>
Then finish only the remaining work under the same rules.
```

**`ci-fix`** (an open PR has failing checks):

```text
MODE=ci-fix: CI failed on the open PR.
{FAILURE_CONTEXT}
Fetch the logs yourself: `gh run view <RUN_ID> --log-failed`. Note that CI runs
on both push and pull_request, so the same failure may appear under two run ids.
Make the smallest change that turns the named checks green without violating the
task or its Explicitly-deferred list. Never weaken a gate to pass it. Commit as
`fix(PR {ID}): <failing-check>: <cause>`. Report one extra line:
FINGERPRINT=<sorted failing check names + first error line of each>
If the fingerprint matches a prior one in {FAILURE_CONTEXT}, do not attempt the
same fix again — report STATUS=blocked with the reason.
```

**Scope-check reviewer** (step 5; a separate one-shot subagent, not this
template): give it only `git diff main...HEAD --stat`, the diff of the key files,
the task's Scope section, and the permanent non-goals. It returns exactly
`IN_SCOPE` or `VIOLATION:<what>` and nothing else.
