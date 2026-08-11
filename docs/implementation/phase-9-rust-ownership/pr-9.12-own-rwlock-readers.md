<!--
  Canonical task-file template for the walrus implementation curriculum.
  Copy this file when adding a new PR task. Keep every section — a missing
  "Definition of Done" is the one thing a reviewer will always reject.
-->

# PR 9.12 — Record why walrus has no `RwLock` and guard the lock-choice decision

> **Status:** 📋 Planned <!-- flip to "✅ Done — <PR url>" when it merges -->

> **Readiness:** audited · **Outcome:** evidence
> **Gates:** fmt,clippy,test · **Test packages:** loader,pg-sink

> **Phase:** 9 — Rust ownership & borrowing · **Crates touched:** `loader`, `pg-sink` ·
> **Est. size:** S · **Depends on:** PR 9.11 · **Unlocks:** PR 10.1

`RwLock` appears **0** times across `crates` + `tests`, and after PRs 9.10–9.11 exactly **two** locks
remain — neither of them read-dominated. `crates/pg-sink/src/reload_signal.rs:45`
`waiters: Mutex<HashMap<(i64,i64), oneshot::Sender<Echo>>>` is touched by exactly two methods:
`subscribe` inserts (`:57`) and `resolve` removes (`:80`) — **every single access is a write**.
`crates/loader/src/health.rs:28` `last_poll_completed_at: Mutex<Option<Instant>>` is written by every
apply worker at the end of every poll cycle (`stamp_poll`, `:73`) and read only by the kubelet's
`/healthz` probe (`is_live`, `:78`) — writes dominate there too, by the ratio of poll interval to
probe interval. Swapping either for `RwLock` would add reader-tracking cost to a workload with no
concurrent readers. This PR does **not** adopt the rule: it records the measurement as an ADR and
installs a mechanical guard so the decision is re-argued, in-tree, the next time anyone declares a
lock.

## Why — learning objectives

- **`Mutex` vs `RwLock` vs atomics** — reader-tracking overhead, writer starvation, and poisoning as
  an *API-design* consequence rather than a footnote.
- **walrus's real concurrency shape** — `!Send` `LocalSet` workers, one axum health task, one decode
  loop, and side exporter tasks; knowing the shape is what makes the lock choice decidable.

## Read first

- [`own-rwlock-readers`](../../../.claude/skills/rust-skills/rules/own-rwlock-readers.md) — read it in
  full, and read the **When RwLock Hurts** table hardest: "writes >20 % of operations ⇒ `Mutex`" and
  "lock held very briefly ⇒ `Mutex`" are the two rows that decide this ticket. Note also that its
  `std::sync::RwLock` examples all end in `.unwrap()`.
- `crates/pg-sink/src/reload_signal.rs:36-59,67-99` — the registry, and the module header explaining
  the subscribe-then-insert ordering that makes lock *availability* (not read throughput) the
  property that matters.
- `crates/loader/src/health.rs:20-29,70-79` — `LoaderState`, `stamp_poll`, `is_live`, and the
  module doc explaining that liveness is *progress*, never lag.
- `Cargo.toml:38-41` — the recorded reason `parking_lot` is a direct dependency.
- `docs/implementation/notes/duckdb-lts-bump.md` — the house format for a "we evaluated and declined"
  ADR: what the task expected, what is actually true, the measurement, the go-forward clock.
- `.github/workflows/ci.yml:53-104` — the `gates` job your new step joins; and
  `scripts/k8s-validate.sh` for the house shell-guard style (`set -euo pipefail`,
  `cd "$(git rev-parse --show-toplevel)"`).

## Baseline contract

- **Precondition:** Confirm `rule-present`, then reproduce the audited non-applicability or rejected
  tradeoff on the immediate predecessor by named path and symbol. Historical line coordinates are
  navigation hints only.
- **Locked outcome:** This is an evidence task. Land only the decision note and mechanical guards
  explicitly listed in the allowlist; do not adopt the rejected optimization or API. The canonical
  artifact is `docs/implementation/notes/rust-skills/own-rwlock-readers.md`.
- **Allowed files:** The **Files to create / modify** block is exhaustive. Any applicable production
  change, ambiguous mapping, or newly required path blocks; do not convert the task to `change`.

## Scope

**Baseline precondition.** Before editing, reproduce the task's authored finding from its named
source paths, symbols, counts, and read-only probes; run the full **Verification commands** block
after implementation. The named sites and allowed paths are the complete task boundary.

**Baseline mismatch.** If the current tree differs from that authored finding, **STOP and request
task re-authoring before editing.** Do not choose another site, implementation, evidence conclusion,
or outcome.

**The conflict, and how to resolve it** — the corpus rule says *"reads significantly outnumber
writes → `RwLock`"*. **Measured, walrus has no such structure.** Zero `RwLock`s exist and both
surviving `Mutex`es are write-dominated (per-site analysis above). Worse, `std::sync::RwLock` would
reintroduce the poisoning / `unwrap()` shape that `[workspace.lints.clippy] unwrap_used = "deny"`
(PR 7.7) exists to forbid — which is exactly why PR 7.6 promoted `parking_lot` to a direct
dependency in the first place. **Do not apply the rule.** The deliverable is a *documented
rejection* carrying the per-site read/write analysis, plus a mechanical guard — not an adoption.

**In scope**

- `docs/implementation/notes/rust-skills/own-rwlock-readers.md` — the ADR: what the rule asks, the per-site read/write
  counts for both locks, why `RwLock` loses on both, why `parking_lot::Mutex` is the workspace
  default, and the criteria that would make a future `RwLock` legitimate.
- `scripts/check-lock-choice.sh` — fails if any `Mutex<` / `RwLock<` **field declaration** under
  `crates/*/src` (excluding `*_test.rs`) lacks a `// LOCK-CHOICE:` line directly above it. Its
  `--self-test` uses a disposable source root to prove both branches.
- A step in the CI `gates` job (`.github/workflows/ci.yml:53`) running that script.
- The two `// LOCK-CHOICE:` comments the guard requires, each naming the access pattern that picked
  `Mutex`.

**Explicitly deferred** (do *not* build these here)

- **No `RwLock` is introduced anywhere.** The tree-wide count stays 0.
- No dependency is added or removed (`parking_lot` stays; nothing new needs `cargo deny` clearance).
- No lock's runtime behaviour changes — the only Rust edit in this PR is the two comment lines.
- The guard does **not** try to police lock *scope*; `clippy::significant_drop_in_scrutinee` /
  `significant_drop_tightening` already do that as of PR 9.11.
- The guard does not scan `crates/*/tests/**` or `tests/e2e` — a test-local lock is not an
  architectural decision.

## Files to create / modify

```
docs/implementation/notes/rust-skills/own-rwlock-readers.md    # new — the ADR (per-site analysis + rejection)
scripts/check-lock-choice.sh                # new — the mechanical guard
.github/workflows/ci.yml                    # + a step in the `gates` job (after checkout)
crates/loader/src/health.rs                 # + // LOCK-CHOICE: above :28 last_poll_completed_at
crates/pg-sink/src/reload_signal.rs         # + // LOCK-CHOICE: above :45 waiters
```

## Skeleton

```bash
#!/usr/bin/env bash
# check-lock-choice.sh — PR 9.12 guard (`own-rwlock-readers`). Every `Mutex<` / `RwLock<` FIELD
# declaration in production code must carry a `// LOCK-CHOICE:` justification on the line directly
# above it, naming the access pattern (read/write ratio, hold time) that picked this primitive.
# walrus deliberately has zero RwLocks — see docs/implementation/notes/rust-skills/own-rwlock-readers.md — and a future
# one must be argued, not assumed.
#
#   bash scripts/check-lock-choice.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Field declarations only: `name: Mutex<…>` / `name: parking_lot::RwLock<…>`. A `&`-taking function
# parameter or a `use` line never matches, because a path segment must follow the colon directly.
PATTERN='^[[:space:]]*(pub[[:space:]]+)?[a-z_][a-z0-9_]*:[[:space:]]*((parking_lot|std::sync|tokio::sync)::)?(Mutex|RwLock)<'

fail=0
while IFS=: read -r file line _rest; do
  prev=$((line - 1))
  if ! sed -n "${prev}p" "$file" | grep -q 'LOCK-CHOICE:'; then
    echo "::error file=${file},line=${line}::lock field declared without a '// LOCK-CHOICE:' justification"
    fail=1
  fi
done < <(grep -rnE --include='*.rs' --exclude='*_test.rs' "$PATTERN" crates/*/src)

if [ "$fail" -eq 0 ]; then
  echo "OK: every Mutex/RwLock field under crates/*/src carries a // LOCK-CHOICE: justification"
fi
exit "$fail"
```

```rust
// crates/loader/src/health.rs — above line 28
    /// The end of the last poll cycle — liveness proof, NOT a lag metric. `None` until bootstrap ends.
    // LOCK-CHOICE: Mutex, not RwLock — write-dominated. Written by every apply worker at the end of
    // every poll cycle (`stamp_poll`); read only by the kubelet's /healthz probe (`is_live`). Both
    // hold the lock for a single expression, so reader-tracking would be pure overhead.
    // See docs/implementation/notes/rust-skills/own-rwlock-readers.md.
    last_poll_completed_at: Mutex<Option<Instant>>,
```

```rust
// crates/pg-sink/src/reload_signal.rs — above line 45
    // LOCK-CHOICE: Mutex, not RwLock — 100 % writes. `subscribe` inserts and `resolve` removes;
    // there is no read-only access path at all. Held for exactly one map operation (PR 9.11).
    // See docs/implementation/notes/rust-skills/own-rwlock-readers.md.
    waiters: Mutex<HashMap<(i64, i64), oneshot::Sender<Echo>>>,
```

```yaml
# .github/workflows/ci.yml — in the `gates` job (line 53), directly after `- uses: actions/checkout@v4`
      # Seconds, no toolchain needed — fail before paying for the DuckDB build (PR 9.12).
      - name: Lock-choice guard
        run: bash scripts/check-lock-choice.sh
```

The ADR at `docs/implementation/notes/rust-skills/own-rwlock-readers.md` follows `duckdb-lts-bump.md`: an H1
`Lock choice: why walrus has no RwLock (PR 9.12)`, then a status blockquote —
*"evaluated — `own-rwlock-readers` deliberately **not** adopted; two locks remain, both
write-dominated, `parking_lot::Mutex` correct for both"* — then six H2 sections:

| section | must contain |
|---|---|
| What the rule asks | the corpus claim, quoted, and its `> 20 % writes ⇒ Mutex` escape hatch |
| The two locks, measured | a row per lock: field, `file:line`, each method, read-or-write, hold time |
| Why RwLock loses (twice) | reader-tracking cost with zero concurrent readers; guards held for one op |
| Why parking_lot, not std::sync | poisoning ⇒ `Result` ⇒ `unwrap` ⇒ the PR 7.7 deny (PR 7.6's reason) |
| The guard | what `scripts/check-lock-choice.sh` enforces and where it runs in CI |
| When to revisit | the concrete conditions that would make a future `RwLock` legitimate |

## Verification commands

```text
rule-present = test -f .claude/skills/rust-skills/rules/own-rwlock-readers.md
focused-test = cargo test -p loader -p pg-sink
guard-self-test = bash scripts/check-lock-choice.sh --self-test
evidence-note = test -s docs/implementation/notes/rust-skills/own-rwlock-readers.md
diff-check = git diff --check origin/main...HEAD
```

## Definition of Done

A reviewer merges this PR when **all** of the following hold:

- [ ] `docs/implementation/notes/rust-skills/own-rwlock-readers.md` exists and contains, per lock: the field, the
      methods that touch it, whether each access is a read or a write, and how long the guard is
      held — plus the explicit rejection of `RwLock` and the `parking_lot`-vs-`std::sync`
      poisoning/`unwrap_used` argument.
- [ ] The note states the criteria that would justify a future `RwLock` (a genuinely read-dominated
      structure, held long enough that reader-tracking pays for itself), so the decision is
      revisitable rather than dogma.
- [ ] `scripts/check-lock-choice.sh` exists, starts `set -euo pipefail`, and exits **0** on this
      branch.
- [ ] `bash scripts/check-lock-choice.sh --self-test` proves a clean temporary fixture passes and a
      fixture lacking `// LOCK-CHOICE:` fails with its temporary `file:line`; tracked comments are
      never removed.
- [ ] `.github/workflows/ci.yml`'s `gates` job runs `bash scripts/check-lock-choice.sh`.
- [ ] `crates/loader/src/health.rs:28` and `crates/pg-sink/src/reload_signal.rs:45` each carry a
      `// LOCK-CHOICE:` comment naming the access pattern and linking the note.
- [ ] `grep -rn --include='*.rs' 'RwLock' crates tests | wc -l` is still `0`, and no dependency was
      added — the only Rust change in the diff is the two comments.
- [ ] **Green locally and in CI:**
  - [ ] `cargo fmt --check`
  - [ ] `cargo clippy --all-targets --all-features -- -D warnings`
  - [ ] `cargo test -p loader -p pg-sink` (and `--workspace` stays green)

## What completed looks like

```
# BEFORE (on main) — no guard, and the lock choice lives only in reviewers' heads
$ ls scripts/check-lock-choice.sh
ls: scripts/check-lock-choice.sh: No such file or directory
$ grep -rn --include='*.rs' 'RwLock' crates tests | wc -l
0

# AFTER
$ bash scripts/check-lock-choice.sh
OK: every Mutex/RwLock field under crates/*/src carries a // LOCK-CHOICE: justification
$ echo $?
0

# …and its isolated fixture proves the rejection path:
$ bash scripts/check-lock-choice.sh --self-test
ok: justified temporary lock field passes
ok: unjustified temporary lock field is rejected with its file and line
check-lock-choice self-test: PASS

$ grep -rn --include='*.rs' 'RwLock' crates tests | wc -l
0      # unchanged — this PR rejects the rule, it does not adopt it
```

## Hints & gotchas

- **This is a "declined, with evidence" ticket.** A reviewer will reject "not relevant". Every claim
  in the note must be a countable fact from the tree: which methods touch the lock, whether each is
  a read or a write, and what is held across the guard. Cite `file:line`.
- The `parking_lot`-vs-`std::sync` half of the argument is the interesting half:
  `std::sync::RwLock::read()` returns a `Result` because of poisoning, so adopting it would force
  `unwrap()`/`expect()` — denied workspace-wide since PR 7.7. Write that down; it is the reason a
  future contributor cannot "just use the std one".
- **Guard the declaration, not the type name.** A regex on the bare word `Mutex` would flag
  `use parking_lot::Mutex;` and the doc comments you are adding. Anchor on `name:` immediately
  followed by an optional path and `Mutex<`/`RwLock<`, as in the skeleton.
- `set -euo pipefail` + `while … done < <(grep …)`: grep's exit status is **not** checked in a
  process substitution, so an empty match set will not abort the script. Do not pipe `grep | while`
  instead — the loop body would run in a subshell and `fail=1` would be lost.
- Put the CI step **before** the toolchain install and the DuckDB build: it costs seconds, and a
  fast failure is the point. `gates` only runs when the `changes` job classifies the diff as code
  (`crates/**`, `scripts/**`, `.github/**` are all in that filter), which is exactly when a new lock
  could appear.
- `chmod +x` the script if you like, but invoke it as `bash scripts/check-lock-choice.sh` in CI —
  that is what `scripts/k8s-validate.sh` does and it avoids a mode-bit-only diff mattering.
- Don't "improve" either lock while you are in the file: no scope tightening (PR 9.11 did that), no
  swap to atomics for `last_poll_completed_at` (an `Instant` is not `AtomicU64`-shaped, and the
  `Option` matters).
- `clippy::all` + `warnings` are already `deny` and comments cannot trip them — but a stray unused
  import while editing will fail the build.
- No `.sql` file and no `.sqlx` cache entry is involved.

## References

- Rule: [`own-rwlock-readers`](../../../.claude/skills/rust-skills/rules/own-rwlock-readers.md)
- Design: `docs/implementation/notes/rust-skills/own-rwlock-readers.md` (this PR's ADR) and `docs/architecture.md`
  §1.9 / "Kubernetes wiring" — the probe cadence that makes `/healthz` the *only* reader of
  `last_poll_completed_at`.
- Prev: [PR 9.11](./pr-9.11-own-mutex-interior.md) · Next: [PR 10.1](../phase-10-rust-errors/pr-10.1-err-from-impl.md) *(phase boundary → Phase 10 Rust error handling)* · [Roadmap](../README.md)
