---
name: applying-walrus-rust-rules
description: Sequentially audit the entire Walrus repository against every raw rust-skills rule, checkpoint each rule locally, run the complete local gate, and open one green pull request. Use only when explicitly asked for the full repository-wide Rust rule loop; do not use for ordinary Rust edits or reviews.
---

# Apply every Rust rule to Walrus

Use the deterministic driver for this long-running, repository-wide audit. It
processes exactly one raw rule at a time and can resume from its Git trailers.

## Preconditions

- Work only on `rust-roadmap-remainder-batch` unless the operator explicitly
  chose another non-main branch.
- Require a clean worktree. Never discard unrelated user changes.
- Read rules from `../rust-skills/SKILL.md` in its declared priority order.
  The driver verifies that this index names every Markdown file under
  `../rust-skills/rules/` exactly once.
- Keep the audited implementation-roadmap completion state separate. A raw-rule
  review does not prove a roadmap task's predetermined acceptance contract.

## Run

First inspect the manifest without invoking an agent:

```bash
python3 .claude/skills/applying-walrus-rust-rules/scripts/run.py --dry-run
```

Then start or resume the complete workflow:

```bash
python3 .claude/skills/applying-walrus-rust-rules/scripts/run.py
```

The driver owns rule selection, Claude Code invocation, checkpoint commits,
failure recovery, cleanup passes, the full local gate, the initial push, PR
creation, and CI polling/fixes. Do not run another copy concurrently and do not
manually mark a rule complete while it is active.

Use `--status` for a non-mutating progress report. Optional `--model` and
`--max-budget-usd` values are forwarded to each Claude invocation; otherwise
the installed Claude Code defaults apply.

## Invariants

- One fresh Claude session audits one rule across the whole repository.
- `applied` and justified `no-change` results receive one `Rust-Rule` trailer;
  failed attempts receive `Rust-Rule-Attempt` and are retried only in the three
  cleanup passes.
- A failed agent's tracked diff and untracked files are archived beneath the
  worktree Git directory before the clean tree is restored.
- Rule sources, this driver, and roadmap completion files are protected from
  rule-agent edits.
- No testing, push, or PR begins while any rule remains failed.
- The final local gate is the complete `run_gate.sh` gate with no skips. A gate
  or real CI failure gets at most three focused repair rounds.
- The driver opens but never merges the final PR.

Every stop is resumable by invoking the same command again.
