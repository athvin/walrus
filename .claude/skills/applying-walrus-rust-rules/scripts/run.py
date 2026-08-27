#!/usr/bin/env python3
"""Sequentially apply every raw rust-skills rule to the Walrus repository."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

RULE_LINK_RE = re.compile(r"\[`(?P<slug>[a-z0-9-]+)`\]\(rules/(?P=slug)\.md\)")
TRAILER_RE = re.compile(
    r"^(?P<key>Rust-Rule|Rust-Rule-Attempt|Rust-Rule-Result|Rust-Rule-Pass|"
    r"Rust-Rule-Loop|Rust-Rule-Manifest|Rust-Rule-Count):\s*(?P<value>.+?)\s*$",
    re.MULTILINE,
)
FULL_GATES = (
    "fmt,clippy,test,sqlx,conformance,deny,msrv,compose,integration,e2e,"
    "manifests,images"
)
MAX_CLEANUP_PASSES = 3
MAX_REPAIR_ROUNDS = 3
MAX_CI_WAIT_CYCLES = 2
MAX_FLAKE_RERUNS = 2
EXPECTED_BRANCH = "rust-roadmap-remainder-batch"
SKILL_REL = Path(".claude/skills/applying-walrus-rust-rules")
RULE_SKILL_REL = Path(".claude/skills/rust-skills/SKILL.md")
RULE_DIR_REL = Path(".claude/skills/rust-skills/rules")

RESULT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["applied", "no_change", "failed"]},
        "summary": {"type": "string"},
        "validation": {"type": "array", "items": {"type": "string"}},
        "blocked_on": {"type": "string"},
    },
    "required": ["status", "summary", "validation", "blocked_on"],
    "additionalProperties": False,
}


class LoopError(RuntimeError):
    """A safe, resumable workflow stop."""


@dataclass(frozen=True)
class Rule:
    slug: str
    path: Path
    digest: str


@dataclass(frozen=True)
class Record:
    slug: str
    result: str
    pass_name: str
    commit: str
    completed: bool


@dataclass(frozen=True)
class Progress:
    records: tuple[Record, ...]
    initial_attempted: frozenset[str]
    completed: frozenset[str]
    failed: frozenset[str]
    cleanup_attempts: dict[str, frozenset[int]]


def run(
    args: Sequence[str],
    *,
    cwd: Path,
    check: bool = True,
    capture: bool = True,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args),
        cwd=cwd,
        check=check,
        capture_output=capture,
        text=True,
        env=env,
    )


def repo_root() -> Path:
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=True,
        capture_output=True,
        text=True,
    )
    return Path(result.stdout.strip()).resolve()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def discover_rules(root: Path) -> list[Rule]:
    skill = root / RULE_SKILL_REL
    rule_dir = root / RULE_DIR_REL
    slugs = [match.group("slug") for match in RULE_LINK_RE.finditer(skill.read_text())]
    duplicates = sorted({slug for slug in slugs if slugs.count(slug) > 1})
    if duplicates:
        raise LoopError("duplicate rule link(s): " + ",".join(duplicates))
    files = {path.stem: path for path in rule_dir.glob("*.md")}
    indexed = set(slugs)
    missing = sorted(set(files) - indexed)
    unknown = sorted(indexed - set(files))
    if missing or unknown or len(slugs) != 265:
        raise LoopError(
            "rule manifest mismatch: "
            f"indexed={len(slugs)} files={len(files)} "
            f"missing={','.join(missing) or '-'} unknown={','.join(unknown) or '-'}"
        )
    return [Rule(slug, files[slug].relative_to(root), sha256(files[slug])) for slug in slugs]


def manifest_digest(rules: Sequence[Rule]) -> str:
    payload = "".join(f"{rule.slug}\0{rule.path}\0{rule.digest}\n" for rule in rules)
    return hashlib.sha256(payload.encode()).hexdigest()


def git_output(root: Path, *args: str) -> str:
    return run(["git", *args], cwd=root).stdout.strip()


def current_branch(root: Path) -> str:
    return git_output(root, "branch", "--show-current")


def require_clean(root: Path) -> None:
    status = git_output(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        raise LoopError("worktree is not clean:\n" + status)


def loop_start(root: Path) -> tuple[str, dict[str, str]] | None:
    raw = git_output(root, "log", "--format=%H%x1f%B%x1e")
    starts: list[tuple[str, dict[str, str]]] = []
    for item in raw.split("\x1e"):
        if not item.strip() or "\x1f" not in item:
            continue
        commit, body = item.strip().split("\x1f", 1)
        trailers = dict(TRAILER_RE.findall(body))
        if trailers.get("Rust-Rule-Loop") == "start":
            starts.append((commit.strip(), trailers))
    if len(starts) > 1:
        raise LoopError("multiple Rust rule loop start commits exist on this branch")
    return starts[0] if starts else None


def create_loop_start(root: Path, rules: Sequence[Rule]) -> str:
    digest = manifest_digest(rules)
    run(
        [
            "git",
            "commit",
            "--allow-empty",
            "-m",
            "chore(rust-rules): start sequential repository audit",
            "-m",
            f"Rust-Rule-Loop: start\nRust-Rule-Manifest: {digest}\nRust-Rule-Count: {len(rules)}",
        ],
        cwd=root,
    )
    return git_output(root, "rev-parse", "HEAD")


def validate_start(start: tuple[str, dict[str, str]], rules: Sequence[Rule]) -> str:
    commit, trailers = start
    expected = manifest_digest(rules)
    if trailers.get("Rust-Rule-Manifest") != expected:
        raise LoopError("raw rule files or their declared order changed after the loop started")
    if trailers.get("Rust-Rule-Count") != str(len(rules)):
        raise LoopError("loop start commit has an invalid rule count")
    return commit


def records_since(root: Path, start_commit: str, known: set[str]) -> tuple[Record, ...]:
    raw = git_output(root, "log", "--reverse", "--format=%H%x1f%B%x1e", f"{start_commit}..HEAD")
    records: list[Record] = []
    for item in raw.split("\x1e"):
        if not item.strip() or "\x1f" not in item:
            continue
        commit, body = item.strip().split("\x1f", 1)
        pairs = TRAILER_RE.findall(body)
        trailers: dict[str, list[str]] = {}
        for key, value in pairs:
            trailers.setdefault(key, []).append(value)
        completed = trailers.get("Rust-Rule", [])
        attempted = trailers.get("Rust-Rule-Attempt", [])
        if len(completed) + len(attempted) == 0:
            continue
        if len(completed) + len(attempted) != 1:
            raise LoopError(f"commit {commit[:12]} has invalid Rust rule trailers")
        slug = (completed or attempted)[0]
        if slug not in known:
            raise LoopError(f"commit {commit[:12]} names unknown rule {slug}")
        results = trailers.get("Rust-Rule-Result", [])
        passes = trailers.get("Rust-Rule-Pass", [])
        if len(results) != 1 or len(passes) != 1:
            raise LoopError(f"commit {commit[:12]} lacks one result/pass trailer")
        is_completed = bool(completed)
        if is_completed and results[0] not in {"applied", "no-change"}:
            raise LoopError(f"commit {commit[:12]} has inconsistent completion result")
        if not is_completed and results[0] != "failed":
            raise LoopError(f"commit {commit[:12]} has inconsistent attempt result")
        records.append(Record(slug, results[0], passes[0], commit.strip(), is_completed))
    return tuple(records)


def progress(records: Sequence[Record]) -> Progress:
    completed = {record.slug for record in records if record.completed}
    initial = {record.slug for record in records if record.pass_name == "initial"}
    cleanup: dict[str, set[int]] = {}
    for record in records:
        match = re.fullmatch(r"cleanup-(\d+)", record.pass_name)
        if match:
            cleanup.setdefault(record.slug, set()).add(int(match.group(1)))
    attempted = {record.slug for record in records}
    return Progress(
        records=tuple(records),
        initial_attempted=frozenset(initial),
        completed=frozenset(completed),
        failed=frozenset(attempted - completed),
        cleanup_attempts={slug: frozenset(values) for slug, values in cleanup.items()},
    )


def next_rule(rules: Sequence[Rule], state: Progress) -> tuple[Rule, str] | None:
    for rule in rules:
        if rule.slug not in state.initial_attempted:
            return rule, "initial"
    failures = [rule for rule in rules if rule.slug not in state.completed]
    if not failures:
        return None
    for cleanup_pass in range(1, MAX_CLEANUP_PASSES + 1):
        for rule in failures:
            if cleanup_pass not in state.cleanup_attempts.get(rule.slug, frozenset()):
                return rule, f"cleanup-{cleanup_pass}"
    raise LoopError(
        "rules still failed after three cleanup passes: "
        + ",".join(rule.slug for rule in failures)
    )


def state_dir(root: Path, start_commit: str) -> Path:
    git_path = git_output(root, "rev-parse", "--git-path", "walrus-rust-rule-loop")
    path = Path(git_path)
    if not path.is_absolute():
        path = root / path
    target = path / start_commit[:12]
    target.mkdir(parents=True, exist_ok=True)
    return target


def changed_paths(root: Path) -> list[Path]:
    tracked = run(
        ["git", "diff", "--name-only", "-z", "HEAD"], cwd=root
    ).stdout.split("\0")
    untracked = run(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=root
    ).stdout.split("\0")
    return sorted({Path(item) for item in tracked + untracked if item})


def is_protected(path: Path) -> bool:
    value = path.as_posix()
    if value.startswith(RULE_DIR_REL.as_posix() + "/"):
        return True
    if value == RULE_SKILL_REL.as_posix() or value.startswith(SKILL_REL.as_posix() + "/"):
        return True
    if value == "docs/implementation/README.md":
        return True
    return bool(re.fullmatch(r"docs/implementation/phase-[^/]+/pr-[^/]+\.md", value))


def archive_and_restore(root: Path, archive: Path, paths: Sequence[Path]) -> None:
    archive.mkdir(parents=True, exist_ok=True)
    patch = run(["git", "diff", "--binary", "HEAD"], cwd=root).stdout
    (archive / "changes.patch").write_text(patch)
    untracked_root = archive / "untracked"
    tracked: list[str] = []
    untracked: list[Path] = []
    for path in paths:
        result = run(
            ["git", "ls-files", "--error-unmatch", "--", path.as_posix()],
            cwd=root,
            check=False,
        )
        if result.returncode == 0:
            tracked.append(path.as_posix())
        else:
            untracked.append(path)
            source = root / path
            if source.exists() or source.is_symlink():
                destination = untracked_root / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                if source.is_dir():
                    shutil.copytree(source, destination, dirs_exist_ok=True)
                else:
                    shutil.copy2(source, destination, follow_symlinks=False)
    if tracked:
        run(["git", "restore", "--source=HEAD", "--staged", "--worktree", "--", *tracked], cwd=root)
    for path in sorted(untracked, key=lambda item: len(item.parts), reverse=True):
        target = root / path
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
        elif target.exists() or target.is_symlink():
            target.unlink()
        parent = target.parent
        while parent != root:
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent
    require_clean(root)


def parse_claude_result(stdout: str) -> dict[str, Any]:
    data = json.loads(stdout)
    candidate: Any = data.get("structured_output") if isinstance(data, dict) else None
    if candidate is None and isinstance(data, dict):
        candidate = data.get("result")
        if isinstance(candidate, str):
            candidate = json.loads(candidate)
    if not isinstance(candidate, dict):
        raise ValueError("Claude response has no structured output")
    required = {"status", "summary", "validation", "blocked_on"}
    if set(candidate) != required or candidate["status"] not in {"applied", "no_change", "failed"}:
        raise ValueError("Claude structured output does not match the required schema")
    return candidate


def infrastructure_failure(process: subprocess.CompletedProcess[str]) -> str | None:
    """Identify failures that retrying against hundreds of rules cannot fix."""
    if process.returncode == 0:
        return None
    combined = (process.stdout + "\n" + process.stderr).lower()
    markers = (
        "unknown option",
        "not logged in",
        "authentication failed",
        "authentication_error",
        "rate limit",
        "rate_limit",
        "credit balance",
        "invalid api key",
    )
    return next((marker for marker in markers if marker in combined), None)


def rule_prompt(root: Path, rule: Rule, pass_name: str, archive: Path) -> str:
    prior = ""
    if pass_name != "initial":
        prior = (
            f"\nThis is {pass_name}. Prior failure artifacts are under {archive.relative_to(root.parent) if root.parent in archive.parents else archive}. "
            "Use them as diagnostics, but work from the current clean tree.\n"
        )
    return f"""Audit the entire Walrus repository against exactly one Rust guideline:
{rule.path}

Read that Markdown file completely, then inspect the whole repository for relevant sites. The raw
rule is authoritative for this run; do not read or implement its matching audited roadmap task.
Apply focused, idiomatic improvements where the rule genuinely benefits this codebase. A justified
no-change result is valid when the rule is already satisfied, inapplicable, conditional, or would
encourage speculative dependencies/optimization. Preserve behavior unless the rule requires a
correctness fix, and add or update focused tests when behavior changes.

Do not delegate. Do not edit .claude/skills/rust-skills, this loop skill, docs/implementation/README.md,
or docs/implementation/phase-*/pr-*.md. Do not commit, push, create/update PRs, switch branches,
reset/restore/clean the tree, or inspect target/ or other ignored/generated artifacts. Do not run
Cargo, rustc, Docker, tests, builds, formatters, linters, or the repository-wide final gate during
the rule pass; all executable validation is deliberately deferred until every rule has been applied.
Use source inspection and focused static searches to validate the rule-specific result. Return
status=failed if you cannot leave a coherent rule-specific result.
{prior}
Report only the required structured result. In validation, list exact commands and outcomes.
Use blocked_on as an empty string unless status is failed.
"""


def claude_command(options: argparse.Namespace, *, allow_bash: bool = False) -> list[str]:
    command = [
        "claude",
        "-p",
        "--output-format",
        "json",
        "--json-schema",
        json.dumps(RESULT_SCHEMA, separators=(",", ":")),
        "--permission-mode",
        "auto",
        "--no-session-persistence",
        "--tools",
        "Read,Grep,Glob,Edit,Write,Bash" if allow_bash else "Read,Grep,Glob,Edit,Write",
    ]
    if allow_bash:
        command.extend(
            [
                "--disallowedTools",
                "Bash(git push *),Bash(gh *),Bash(git commit *),Bash(git reset *),"
                "Bash(git clean *),Bash(git checkout *),Bash(git switch *),"
                "Bash(git restore *),Bash(rm *)",
            ]
        )
    if options.model:
        command.extend(["--model", options.model])
    if options.max_budget_usd is not None:
        command.extend(["--max-budget-usd", str(options.max_budget_usd)])
    return command


def run_claude(
    command: Sequence[str], *, prompt: str, cwd: Path, timeout: int
) -> subprocess.CompletedProcess[str]:
    """Run Claude in its own process group so timeouts/interruption stop descendants."""
    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(input=prompt, timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt):
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        raise
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def commit_record(
    root: Path,
    rule: Rule,
    result: str,
    pass_name: str,
    summary: str,
    paths: Sequence[Path],
) -> str:
    if paths:
        run(["git", "add", "--", *(path.as_posix() for path in paths)], cwd=root)
    if result == "failed":
        subject = f"chore(rust-rule): record failed {rule.slug} audit"
        key = "Rust-Rule-Attempt"
    else:
        subject = f"chore(rust-rule): audit {rule.slug}"
        key = "Rust-Rule"
    clean_summary = " ".join(summary.split())[:500] or "No summary returned."
    trailer_result = "no-change" if result == "no_change" else result
    run(
        [
            "git",
            "commit",
            "--allow-empty",
            "-m",
            subject,
            "-m",
            clean_summary,
            "-m",
            f"{key}: {rule.slug}\nRust-Rule-Result: {trailer_result}\nRust-Rule-Pass: {pass_name}",
        ],
        cwd=root,
    )
    require_clean(root)
    return git_output(root, "rev-parse", "HEAD")


def invoke_rule(
    root: Path,
    start_commit: str,
    rule: Rule,
    pass_name: str,
    options: argparse.Namespace,
) -> None:
    before = git_output(root, "rev-parse", "HEAD")
    archive = state_dir(root, start_commit) / "rules" / rule.slug / pass_name
    archive.mkdir(parents=True, exist_ok=True)
    prompt = rule_prompt(root, rule, pass_name, archive)
    print(f"RULE_START={rule.slug} PASS={pass_name}", flush=True)
    try:
        process = run_claude(
            claude_command(options),
            prompt=prompt,
            cwd=root,
            timeout=options.agent_timeout_seconds,
        )
        (archive / "stdout.json").write_text(process.stdout)
        (archive / "stderr.log").write_text(process.stderr)
        if git_output(root, "rev-parse", "HEAD") != before:
            raise LoopError(f"rule agent {rule.slug} changed Git history; refusing automatic recovery")
        paths = changed_paths(root)
        infrastructure = infrastructure_failure(process)
        if infrastructure:
            archive_and_restore(root, archive, paths)
            raise LoopError(
                f"Claude infrastructure failure while processing {rule.slug}: {infrastructure}; "
                f"see {archive}"
            )
        protected = [path for path in paths if is_protected(path)]
        try:
            result = parse_claude_result(process.stdout)
        except (ValueError, json.JSONDecodeError) as error:
            result = {
                "status": "failed",
                "summary": f"invalid Claude result: {error}",
                "validation": [],
                "blocked_on": str(error),
            }
        if process.returncode != 0:
            result["status"] = "failed"
            result["summary"] = f"Claude exited {process.returncode}: {result['summary']}"
        if protected:
            result["status"] = "failed"
            result["summary"] = "agent edited protected paths: " + ",".join(map(str, protected))
        if result["status"] == "failed":
            archive_and_restore(root, archive, paths)
            commit = commit_record(root, rule, "failed", pass_name, result["summary"], [])
            print(f"RULE_END={rule.slug} RESULT=failed COMMIT={commit[:12]}", flush=True)
            return
        normalized = "applied" if paths else "no_change"
        commit = commit_record(root, rule, normalized, pass_name, result["summary"], paths)
        print(
            f"RULE_END={rule.slug} RESULT={'no-change' if normalized == 'no_change' else normalized} "
            f"FILES={len(paths)} COMMIT={commit[:12]}",
            flush=True,
        )
    except subprocess.TimeoutExpired as error:
        paths = changed_paths(root)
        archive_and_restore(root, archive, paths)
        (archive / "timeout.log").write_text(str(error))
        commit = commit_record(
            root,
            rule,
            "failed",
            pass_name,
            f"Claude exceeded {options.agent_timeout_seconds}s timeout",
            [],
        )
        print(f"RULE_END={rule.slug} RESULT=failed COMMIT={commit[:12]} REASON=timeout", flush=True)
    except KeyboardInterrupt:
        paths = changed_paths(root)
        archive_and_restore(root, archive, paths)
        print(f"RULE_INTERRUPTED={rule.slug} RECOVERY={archive}", flush=True)
        raise


def print_status(rules: Sequence[Rule], state: Progress) -> None:
    applied = {record.slug for record in state.records if record.result == "applied" and record.completed}
    no_change = {
        record.slug for record in state.records if record.result == "no-change" and record.completed
    }
    print("VERDICT=RUST_RULE_STATUS")
    print(f"TOTAL={len(rules)}")
    print(f"COMPLETED={len(state.completed)}")
    print(f"APPLIED={len(applied)}")
    print(f"NO_CHANGE={len(no_change)}")
    print(f"FAILED={len(state.failed)}")
    print(f"FAILED_IDS={','.join(rule.slug for rule in rules if rule.slug in state.failed) or '-'}")
    selection = next_rule(rules, state)
    print(f"NEXT={selection[0].slug if selection else '-'}")
    print(f"NEXT_PASS={selection[1] if selection else '-'}")


def prerequisite_errors(root: Path) -> list[str]:
    checks = [
        (["docker", "info"], "Docker daemon"),
        (["cargo", "sqlx", "--version"], "cargo-sqlx"),
        (["cargo", "deny", "--version"], "cargo-deny"),
        (["kustomize", "version"], "kustomize"),
        (["kubeconform", "-v"], "kubeconform"),
    ]
    errors: list[str] = []
    for command, label in checks:
        try:
            process = run(command, cwd=root, check=False)
        except FileNotFoundError:
            errors.append(f"{label} is not installed")
            continue
        if process.returncode != 0:
            errors.append(f"{label} is unavailable")
    return errors


def stream_command(command: Sequence[str], *, root: Path, log: Path) -> int:
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as output:
        process = subprocess.Popen(
            list(command), cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            output.write(line)
        return process.wait()


def repair_prompt(kind: str, log: Path) -> str:
    return f"""Repair the current Walrus branch so its {kind} passes.

Read the failure log at {log}. Inspect the current branch and make the smallest coherent fixes for
the reported failures. Do not revisit unrelated Rust rules. Do not edit .claude/skills/rust-skills,
the applying-walrus-rust-rules skill, docs/implementation/README.md, or roadmap task completion
files. Do not commit, push, create/update PRs, switch branches, or use reset/restore/clean. Run
focused validation for the repair, but do not run the entire full gate. Do not delegate.

Return the required structured result. status=no_change is valid only when the failure is proven
transient. Use blocked_on as an empty string unless status=failed.
"""


def invoke_repair(
    root: Path,
    start_commit: str,
    kind: str,
    log: Path,
    round_number: int,
    options: argparse.Namespace,
) -> bool:
    before = git_output(root, "rev-parse", "HEAD")
    archive = state_dir(root, start_commit) / "repairs" / kind.replace(" ", "-") / str(round_number)
    archive.mkdir(parents=True, exist_ok=True)
    process = run_claude(
        claude_command(options, allow_bash=True),
        prompt=repair_prompt(kind, log),
        cwd=root,
        timeout=options.agent_timeout_seconds,
    )
    (archive / "stdout.json").write_text(process.stdout)
    (archive / "stderr.log").write_text(process.stderr)
    if git_output(root, "rev-parse", "HEAD") != before:
        raise LoopError(f"{kind} repair agent changed Git history")
    paths = changed_paths(root)
    protected = [path for path in paths if is_protected(path)]
    try:
        result = parse_claude_result(process.stdout)
    except (ValueError, json.JSONDecodeError) as error:
        result = {"status": "failed", "summary": str(error)}
    if process.returncode != 0 or result["status"] == "failed" or protected or not paths:
        archive_and_restore(root, archive, paths)
        return False
    run(["git", "add", "--", *(path.as_posix() for path in paths)], cwd=root)
    run(
        [
            "git",
            "commit",
            "-m",
            f"fix(rust-rules): clear {kind} failure (round {round_number})",
        ],
        cwd=root,
    )
    require_clean(root)
    return True


def full_gate(root: Path, start_commit: str, options: argparse.Namespace) -> Path:
    errors = prerequisite_errors(root)
    if errors:
        raise LoopError("full local gate prerequisites failed: " + "; ".join(errors))
    gate = root / ".claude/skills/implementing-walrus-roadmap/scripts/run_gate.sh"
    latest_log = state_dir(root, start_commit) / "full-gate-0.log"
    for attempt in range(MAX_REPAIR_ROUNDS + 1):
        latest_log = state_dir(root, start_commit) / f"full-gate-{attempt}.log"
        rc = stream_command([str(gate), FULL_GATES], root=root, log=latest_log)
        content = latest_log.read_text()
        if rc == 0 and "GATE=PASS" in content and "=SKIP:" not in content:
            return latest_log
        if attempt == MAX_REPAIR_ROUNDS:
            break
        print(f"FULL_GATE_REPAIR={attempt + 1}", flush=True)
        invoke_repair(root, start_commit, "full local gate", latest_log, attempt + 1, options)
    raise LoopError(f"full local gate failed after {MAX_REPAIR_ROUNDS} repair rounds: {latest_log}")


def result_counts(state: Progress) -> tuple[int, int]:
    latest: dict[str, str] = {}
    for record in state.records:
        if record.completed:
            latest[record.slug] = record.result
    return sum(value == "applied" for value in latest.values()), sum(
        value == "no-change" for value in latest.values()
    )


def ensure_publishable(root: Path) -> None:
    branch = current_branch(root)
    if branch == "main" or not branch:
        raise LoopError("refusing to publish from main or detached HEAD")
    run(["git", "fetch", "origin"], cwd=root, capture=False)
    ancestor = run(
        ["git", "merge-base", "--is-ancestor", "origin/main", "HEAD"],
        cwd=root,
        check=False,
    )
    if ancestor.returncode != 0:
        raise LoopError("origin/main moved; integrate it and rerun the full local gate before publishing")
    remote = run(
        ["git", "ls-remote", "--exit-code", "--heads", "origin", branch],
        cwd=root,
        check=False,
    )
    if remote.returncode == 0:
        remote_sha = remote.stdout.split()[0]
        local_sha = git_output(root, "rev-parse", "HEAD")
        if remote_sha != local_sha:
            raise LoopError(f"remote branch origin/{branch} already exists at a different commit")


def create_or_find_pr(
    root: Path,
    start_commit: str,
    rules: Sequence[Rule],
    state: Progress,
    gate_log: Path,
) -> tuple[int, str]:
    branch = current_branch(root)
    existing = run(
        ["gh", "pr", "list", "--state", "open", "--head", branch, "--json", "number,url"],
        cwd=root,
    )
    rows = json.loads(existing.stdout)
    if len(rows) > 1:
        raise LoopError(f"multiple open PRs use branch {branch}")
    if rows:
        return int(rows[0]["number"]), str(rows[0]["url"])
    remote = run(
        ["git", "ls-remote", "--exit-code", "--heads", "origin", branch],
        cwd=root,
        check=False,
    )
    if remote.returncode != 0:
        run(["git", "push", "-u", "origin", branch], cwd=root, capture=False)
    applied, no_change = result_counts(state)
    body_path = state_dir(root, start_commit) / "pr-body.md"
    body_path.write_text(
        "## Summary\n\n"
        f"- audited all {len(rules)} raw rust-skills rules sequentially across Walrus\n"
        f"- applied focused changes for {applied} rules; {no_change} rules required no additional change\n"
        "- preserved the existing audited-roadmap completion state as a separate contract\n\n"
        "## Validation\n\n"
        f"- `{FULL_GATES}`: PASS with no skips\n"
        f"- local gate log: `{gate_log.name}` (worktree-local execution artifact)\n\n"
        f"<!-- walrus-rust-rule-audit: manifest={manifest_digest(rules)} count={len(rules)} -->\n"
    )
    created = run(
        [
            "gh",
            "pr",
            "create",
            "--base",
            "main",
            "--head",
            branch,
            "--title",
            "Apply Rust guidelines across Walrus",
            "--body-file",
            str(body_path),
        ],
        cwd=root,
    )
    url = created.stdout.strip().splitlines()[-1]
    view = run(["gh", "pr", "view", url, "--json", "number,url"], cwd=root)
    data = json.loads(view.stdout)
    return int(data["number"]), str(data["url"])


def parse_key_values(output: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in output.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            if re.fullmatch(r"[A-Z_]+", key):
                values[key] = value
    return values


def get_ci_failure_log(root: Path, run_id: str, destination: Path) -> None:
    process = run(
        ["gh", "run", "view", run_id, "--log-failed"], cwd=root, check=False
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(process.stdout + "\n" + process.stderr)


def wait_for_green_pr(
    root: Path,
    start_commit: str,
    pr: int,
    options: argparse.Namespace,
) -> None:
    checker = root / ".claude/skills/implementing-walrus-roadmap/scripts/ci_status.sh"
    wait_cycles = 0
    flake_reruns = 0
    repairs = 0
    poll_number = 0
    while True:
        poll_number += 1
        poll_log = state_dir(root, start_commit) / "ci" / f"poll-{poll_number}.log"
        stream_command(
            [str(checker), str(pr), "--wait", "2700", "--grace", "300"],
            root=root,
            log=poll_log,
        )
        values = parse_key_values(poll_log.read_text())
        verdict = values.get("VERDICT", "ANOMALY")
        if verdict == "PASS":
            return
        if verdict == "PENDING":
            wait_cycles += 1
            if wait_cycles >= MAX_CI_WAIT_CYCLES:
                raise LoopError("PR CI remained pending after two wait cycles")
            continue
        if verdict != "FAIL":
            raise LoopError(f"PR CI returned {verdict}")
        run_id = values.get("RUN_ID", "")
        if values.get("FLAKE_CANDIDATE") == "yes" and run_id and flake_reruns < MAX_FLAKE_RERUNS:
            flake_reruns += 1
            run(["gh", "run", "rerun", run_id, "--failed"], cwd=root, capture=False)
            continue
        if repairs >= MAX_REPAIR_ROUNDS:
            raise LoopError("PR CI failed after three repair rounds")
        repairs += 1
        log = state_dir(root, start_commit) / "ci" / f"failure-{repairs}.log"
        get_ci_failure_log(root, run_id, log)
        if not invoke_repair(root, start_commit, "pull request CI", log, repairs, options):
            raise LoopError(f"CI repair round {repairs} produced no valid fix")
        full_gate(root, start_commit, options)
        run(["git", "push"], cwd=root, capture=False)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--status", action="store_true")
    parser.add_argument("--model")
    parser.add_argument("--max-budget-usd", type=float)
    parser.add_argument("--agent-timeout-seconds", type=int, default=900)
    parser.add_argument("--allow-other-branch", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_args(argv)
    root = repo_root()
    try:
        rules = discover_rules(root)
        if options.dry_run:
            print("VERDICT=DRY_RUN")
            print(f"TOTAL={len(rules)}")
            print(f"MANIFEST={manifest_digest(rules)}")
            for index, rule in enumerate(rules, 1):
                print(f"RULE={index:03d} {rule.slug} {rule.path}")
            return 0
        branch = current_branch(root)
        if branch == "main" or (branch != EXPECTED_BRANCH and not options.allow_other_branch):
            raise LoopError(f"expected branch {EXPECTED_BRANCH}, found {branch or '<detached>'}")
        require_clean(root)
        start = loop_start(root)
        if start is None:
            if options.status:
                print("VERDICT=NOT_STARTED")
                print(f"TOTAL={len(rules)}")
                print(f"NEXT={rules[0].slug}")
                return 0
            start_commit = create_loop_start(root, rules)
        else:
            start_commit = validate_start(start, rules)
        while True:
            state = progress(records_since(root, start_commit, {rule.slug for rule in rules}))
            if options.status:
                print_status(rules, state)
                return 0
            selection = next_rule(rules, state)
            if selection is None:
                break
            invoke_rule(root, start_commit, selection[0], selection[1], options)
        state = progress(records_since(root, start_commit, {rule.slug for rule in rules}))
        if len(state.completed) != len(rules):
            raise LoopError("not every rule is complete")
        print_status(rules, state)
        gate_log = full_gate(root, start_commit, options)
        require_clean(root)
        ensure_publishable(root)
        pr, url = create_or_find_pr(root, start_commit, rules, state, gate_log)
        print(f"PR={url}", flush=True)
        wait_for_green_pr(root, start_commit, pr, options)
        print("VERDICT=COMPLETE")
        print(f"PR={url}")
        return 0
    except (LoopError, subprocess.CalledProcessError, FileNotFoundError) as error:
        print("VERDICT=STOP")
        print(f"ERROR={error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
