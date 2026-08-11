#!/usr/bin/env python3
"""Validate and select work from the walrus implementation roadmap.

Phases 0-8 predate the machine-readable task contract, so their gate/package
metadata is inferred for compatibility.  Every phase 9+ task is parsed from
explicit audited metadata.  The Rust curriculum is deliberately fail-closed:
its 265 rows may be absent while preparation is in progress, or all present,
but a partial activation is never selectable.

Usage:
  next_task.py                    validate, then select the next task
  next_task.py --status           validate, then print progress counts
  next_task.py --task <id>        validate, then print one task as JSON
  next_task.py --validate-all     validate the complete task/roadmap corpus
  ... --readme <path>             alternate roadmap file (test hook)
  --validate-all --require-tracked  clean-checkout/loop-start validation

Exit codes:
  0  selected/status/task/validation success
  2  ALL_DONE
  3  NO_ELIGIBLE
  4  PARSE_ERROR or validation failure
  5  DRIFT between a roadmap box and task status marker
"""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional


RUST_PHASE_FIRST = 9
RUST_PHASE_LAST = 34
RUST_TASK_COUNT = 265
LEGACY_TASK_COUNT = 102
RUST_FIRST_ID = "9.1"
RUST_LAST_ID = "34.15"
RUST_PHASE_SPECS = {
    9: ("phase-9-rust-ownership", "own", 12),
    10: ("phase-10-rust-errors", "err", 12),
    11: ("phase-11-rust-memory", "mem", 17),
    12: ("phase-12-rust-unsafe", "unsafe", 7),
    13: ("phase-13-rust-api-design", "api", 17),
    14: ("phase-14-rust-async", "async", 18),
    15: ("phase-15-rust-concurrency", "conc", 4),
    16: ("phase-16-rust-codegen-opt", "opt", 12),
    17: ("phase-17-rust-numeric", "num", 5),
    18: ("phase-18-rust-type-safety", "type", 13),
    19: ("phase-19-rust-traits", "trait", 6),
    20: ("phase-20-rust-conversions", "conv", 3),
    21: ("phase-21-rust-const", "const", 4),
    22: ("phase-22-rust-serde", "serde", 8),
    23: ("phase-23-rust-patterns", "pat", 5),
    24: ("phase-24-rust-macros", "macro", 8),
    25: ("phase-25-rust-closures", "closure", 5),
    26: ("phase-26-rust-collections", "coll", 4),
    27: ("phase-27-rust-naming", "name", 16),
    28: ("phase-28-rust-testing", "test", 15),
    29: ("phase-29-rust-documentation", "doc", 12),
    30: ("phase-30-rust-observability", "obs", 7),
    31: ("phase-31-rust-performance", "perf", 13),
    32: ("phase-32-rust-project-structure", "proj", 14),
    33: ("phase-33-rust-linting", "lint", 13),
    34: ("phase-34-rust-anti-patterns", "anti", 15),
}
SUPPORTED_GATES = (
    "fmt",
    "clippy",
    "test",
    "sqlx",
    "conformance",
    "deny",
    "msrv",
    "compose",
    "integration",
    "e2e",
    "manifests",
    "images",
)
BASELINE_GATES = ("fmt", "clippy", "test")

# Roadmap row, for example:
# | ☐ | [9.1](./phase-9-rust-ownership/pr-9.1-own-borrow-over-clone.md) | ... |
ROW_RE = re.compile(
    r"^\|\s*(?P<box>[☐✅])\s*\|\s*\[(?P<id>\d+\.\d+[a-z]?)\]\((?P<file>[^)]+)\)\s*\|"
    r"(?P<delivers>[^|]*)\|(?P<design>[^|]*)\|\s*$"
)
HEADER_ROW_RE = re.compile(r"^\|\s*[☐✅]\s*\|\s*PR\s*\|")
TASK_ID_RE = re.compile(r"^(?P<phase>\d+)\.(?P<number>\d+)(?P<suffix>[a-z]?)$")
TASK_FILENAME_RE = re.compile(
    r"^pr-(?P<id>\d+\.\d+[a-z]?)-(?P<rule>[a-z0-9][a-z0-9-]*)\.md$"
)
PHASE_DIR_RE = re.compile(r"^phase-(?P<phase>\d+)-rust-[a-z0-9-]+$")

H1_RE = re.compile(r"^#\s+PR\s+(?P<id>\d+\.\d+[a-z]?)\s+[\u2014-]\s+(?P<title>.+?)\s*$")
STATUS_RE = re.compile(r"^>\s*\*\*Status:\*\*\s*(?P<status>.+?)\s*$")
DONE_MARKER = "**Status:** ✅ Done"
SIZE_RE = re.compile(r"\*\*Est\. size:\*\*\s*([SML])")
CRATES_RE = re.compile(r"\*\*Crates touched:\*\*\s*(.+?)\s*·\s*\*\*Est\. size")
DEPS_RE = re.compile(r"\*\*Depends on:\*\*\s*(.+?)\s*(?:·\s*\*\*Unlocks|$)")
UNLOCKS_RE = re.compile(r"\*\*Unlocks:\*\*\s*(.+?)\s*$")
DEP_ID_RE = re.compile(
    r"PR\s*(?P<lo>\d+\.\d+[a-z]?)(?:\s*[–—-]\s*(?:PR\s*)?(?P<hi>\d+\.\d+[a-z]?))?"
)

READINESS_OUTCOME_RE = re.compile(
    r"^> \*\*Readiness:\*\* (?P<readiness>draft|audited) · "
    r"\*\*Outcome:\*\* (?P<outcome>change|evidence|superseded by PR (?P<superseded>\d+\.\d+[a-z]?))$"
)
GATES_PACKAGES_RE = re.compile(
    r"^> \*\*Gates:\*\* (?P<gates>[^\s]+) · \*\*Test packages:\*\* (?P<packages>[^\s]+)$"
)
VERIFICATION_LABEL_RE = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
MARKDOWN_LINK_RE = re.compile(r"!?\[[^\]]*\]\((?P<target>[^)]+)\)")

# Legacy-only inference.  Phase 9+ prose is intentionally never scanned: a
# negative/deferred mention must not silently enable a gate.
GATE_PATTERNS = (
    ("compose", r"docker compose|up --wait|just up\b|compose stack"),
    ("sqlx", r"sqlx prepare|\.sqlx\b|query_file!"),
    ("conformance", r"features?[ =\"']*conformance"),
    ("e2e", r"--features it\b|tests/e2e|\be2e\b"),
    ("deny", r"cargo[ -]deny|deny\.toml"),
    ("manifests", r"kubeconform|kustomize"),
    ("images", r"image smoke|docker build|deploy/docker/Dockerfile"),
    ("msrv", r"\bmsrv\b|rust-version"),
)
PKG_RE = re.compile(r"-p\s+([a-z0-9][a-z0-9-]*)")


@dataclass
class ValidationResult:
    rows: list[dict] = field(default_factory=list)
    tasks: dict[str, dict] = field(default_factory=dict)
    rust_tasks: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    drifts: list[dict] = field(default_factory=list)
    untracked: list[str] = field(default_factory=list)


def repo_root() -> Path:
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(out.stdout.strip()).resolve()


def id_key(task_id: str) -> tuple[int, int, str]:
    """Sortable task key: 9.10 follows 9.2 and 8.4a follows 8.4."""
    m = TASK_ID_RE.match(task_id)
    if not m:
        return (0, 0, "")
    return (int(m.group("phase")), int(m.group("number")), m.group("suffix"))


def phase_of(task_id: str) -> int:
    return id_key(task_id)[0]


def parse_readme(readme: Path) -> tuple[list[dict], list[str]]:
    rows: list[dict] = []
    errors: list[str] = []
    for lineno, line in enumerate(readme.read_text().splitlines(), 1):
        if not line.startswith("|") or not re.match(r"^\|\s*[☐✅]", line):
            continue
        if HEADER_ROW_RE.match(line):
            continue
        match = ROW_RE.match(line)
        if not match:
            errors.append(f"line {lineno}: unparseable roadmap row: {line}")
            continue
        rows.append(
            {
                "id": match.group("id"),
                "file": match.group("file").lstrip("./"),
                "checked": match.group("box") == "✅",
                "delivers": match.group("delivers").strip(),
                "design": match.group("design").strip(),
                "lineno": lineno,
            }
        )
    return rows, errors


def section(text: str, heading: str) -> str:
    match = re.search(rf"^##\s+{re.escape(heading)}\s*$", text, re.M)
    if not match:
        return ""
    rest = text[match.end() :]
    next_heading = re.search(r"^##\s+", rest, re.M)
    return rest[: next_heading.start()] if next_heading else rest


def _single_header_match(
    lines: list[str], pattern: re.Pattern[str], description: str, errors: list[str]
) -> Optional[re.Match[str]]:
    matches = [match for line in lines if (match := pattern.match(line))]
    if len(matches) != 1:
        errors.append(f"expected exactly one {description} header, found {len(matches)}")
        return None
    return matches[0]


def validate_verification_command(
    command: str, root: Path, declared_paths: Optional[set[str]] = None
) -> Optional[str]:
    """Return an error for a command outside the read-only verification allowlist."""
    if command != command.strip() or not command:
        return "command must be non-empty with no leading/trailing whitespace"
    if any(token in command for token in ("`", "\n", "\r")):
        return "backticks and newlines are forbidden"
    if "$(" in command:
        return "command substitution is forbidden"
    try:
        argv = shlex.split(command)
    except ValueError as exc:
        return f"cannot parse command: {exc}"
    if not argv:
        return "empty command"

    if argv[0] == "!":
        argv = argv[1:]
        if not argv:
            return "`!` must precede an allowed command"

    shell_ops = {";", "&&", "||", "|", "&", "<", ">", ">>", "2>", "2>>", "&>"}
    if any(arg in shell_ops or re.match(r"^(?:\d*>>?|\d*<|&>)", arg) for arg in argv):
        return "shell chaining, pipes, and redirects are forbidden; use one labeled command per line"
    if any("../" in arg or arg == ".." for arg in argv):
        return "parent-directory traversal is forbidden"

    program = argv[0]
    if program == "cargo":
        if len(argv) < 2 or argv[1] not in {
            "test",
            "check",
            "clippy",
            "fmt",
            "doc",
            "bench",
            "metadata",
        }:
            return "cargo subcommand is not an allowed read-only verification"
        if argv[1] == "fmt" and "--check" not in argv[2:]:
            return "cargo fmt must include --check"
        if any(arg in {"--fix", "--allow-dirty", "--allow-staged"} for arg in argv[2:]):
            return "mutating cargo flags are forbidden"
        return None

    if program == "git":
        if len(argv) < 2 or argv[1] not in {"diff", "show", "status", "ls-files"}:
            return "git command is not an allowed read-only verification"
        if any(arg == "--output" or arg.startswith("--output=") for arg in argv[2:]):
            return "git output files are forbidden"
        if any(arg in {"--ext-diff", "--textconv"} for arg in argv[2:]):
            return "external diff/textconv execution is forbidden"
        if argv[1] == "diff" and "--check" in argv[2:] and not any(
            "..." in arg for arg in argv[2:]
        ):
            return "git diff --check must be merge-base-relative (for example origin/main...HEAD)"
        return None

    if program == "rg":
        if any(arg == "--pre" or arg.startswith("--pre=") for arg in argv[1:]):
            return "rg --pre command execution is forbidden"
        return None

    if program == "test":
        return None

    if program == "find":
        forbidden_find = {"-delete", "-exec", "-execdir", "-ok", "-okdir", "-fprint", "-fprintf"}
        if any(
            arg in forbidden_find or arg.startswith(("-fprint", "-fprintf"))
            for arg in argv[1:]
        ):
            return "mutating/executing find actions are forbidden"
        if len(argv) > 1 and (argv[1].startswith("/") or argv[1].startswith("~")):
            return "find must stay within a repository-relative path"
        return None

    if program == "bash":
        script_index = 1
        if len(argv) > 1 and argv[1] == "-n":
            script_index = 2
        if len(argv) <= script_index:
            return "bash verification must name a repository script"
        script = argv[script_index]
        if not re.fullmatch(r"scripts/[a-zA-Z0-9_./-]+\.sh", script):
            return "bash verification must target scripts/*.sh"
        mode_args = set(argv[script_index + 1 :])
        if script_index == 1 and not mode_args.intersection({"--check", "--self-test", "--dry-run"}):
            return "bash script must use an explicit --check, --self-test, or --dry-run mode"
        if not (root / script).is_file() and script not in (declared_paths or set()):
            return f"repository script does not exist: {script}"
        return None

    if program == "python3":
        if len(argv) < 2:
            return "python3 verification must name a repository validator"
        script = argv[1]
        if Path(script).is_absolute():
            return "python3 validator path must be repository-relative"
        path = root / script
        name = Path(script).name
        named_validator = bool(re.match(r"^(?:validate|check|audit)[a-zA-Z0-9_-]*\.py$", name))
        is_next_task = name == "next_task.py" and "--validate-all" in argv[2:]
        if not (named_validator or is_next_task):
            return "python3 may run only a named repository validator"
        if not path.is_file():
            return f"repository validator does not exist: {script}"
        return None

    return f"unsupported verification command prefix: {program}"


def parse_verification_commands(text: str, root: Path) -> tuple[list[dict], list[str]]:
    errors: list[str] = []
    body = section(text, "Verification commands")
    if not body:
        return [], ["missing `## Verification commands` section"]
    blocks = list(re.finditer(r"```text\s*\n(?P<body>.*?)^```\s*$", body, re.M | re.S))
    if len(blocks) != 1:
        return [], [f"Verification commands must contain exactly one fenced `text` block, found {len(blocks)}"]
    outside = body[: blocks[0].start()] + body[blocks[0].end() :]
    if outside.strip():
        errors.append("Verification commands section may contain only its fenced `text` block")

    declared_paths = set(
        re.findall(
            r"(?<![a-zA-Z0-9_./-])(scripts/[a-zA-Z0-9_./-]+\.sh)",
            section(text, "Files to create / modify"),
        )
    )
    commands: list[dict] = []
    labels: set[str] = set()
    for line_number, line in enumerate(blocks[0].group("body").splitlines(), 1):
        if not line.strip():
            continue
        if " = " not in line:
            errors.append(f"verification line {line_number} must be `label = exact command`")
            continue
        label, command = line.split(" = ", 1)
        if not VERIFICATION_LABEL_RE.fullmatch(label):
            errors.append(f"invalid verification label `{label}`")
            continue
        if label in labels:
            errors.append(f"duplicate verification label `{label}`")
            continue
        labels.add(label)
        command_error = validate_verification_command(command, root, declared_paths)
        if command_error:
            errors.append(f"verification `{label}`: {command_error}")
            continue
        commands.append({"label": label, "command": command})
    if not commands:
        errors.append("Verification commands must contain at least one valid command")
    return commands, errors


def build_gate_command(gates: Iterable[str], packages: Iterable[str]) -> str:
    command = (
        ".claude/skills/implementing-walrus-roadmap/scripts/run_gate.sh "
        + ",".join(gates)
    )
    package_list = list(packages)
    if package_list:
        command += " --pkgs " + ",".join(package_list)
    return command


def _outside_fenced_blocks(text: str) -> str:
    """Preserve prose/line structure while blanking fenced code blocks."""
    output: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if re.match(r"^\s*(?:///+\s*)?(?:```|~~~)", line):
            in_fence = not in_fence
            output.append("")
        elif in_fence:
            output.append("")
        else:
            output.append(line)
    return "\n".join(output)


def validate_rust_task_structure(text: str) -> list[str]:
    errors: list[str] = []
    heading_matches = list(re.finditer(r"^## (?P<heading>.+?)\s*$", text, re.M))
    headings = {match.group("heading"): match.start() for match in heading_matches}
    heading_counts: dict[str, int] = {}
    for match in heading_matches:
        name = match.group("heading")
        heading_counts[name] = heading_counts.get(name, 0) + 1
    required = (
        "Why — learning objectives",
        "Read first",
        "Files to create / modify",
        "Skeleton",
        "Verification commands",
        "Definition of Done",
        "What completed looks like",
        "References",
    )
    for heading in required:
        count = heading_counts.get(heading, 0)
        if count != 1:
            errors.append(f"expected exactly one canonical `## {heading}` section, found {count}")
    for heading in ("Scope", "Baseline and decision", "Implementation contract"):
        if heading_counts.get(heading, 0) > 1:
            errors.append(f"duplicate canonical `## {heading}` section")

    if heading_counts.get("Scope") == 1:
        contract_heading = "Scope"
    elif (
        heading_counts.get("Baseline and decision") == 1
        and heading_counts.get("Implementation contract") == 1
    ):
        contract_heading = "Baseline and decision"
        if headings["Implementation contract"] < headings["Baseline and decision"]:
            errors.append("Implementation contract must follow Baseline and decision")
    else:
        contract_heading = ""
        errors.append(
            "missing canonical `## Scope` or the `## Baseline and decision` + "
            "`## Implementation contract` contract pair"
        )

    ordered = [
        "Why — learning objectives",
        "Read first",
        contract_heading,
        "Files to create / modify",
        "Skeleton",
        "Verification commands",
        "Definition of Done",
        "What completed looks like",
        "References",
    ]
    positions = [headings[name] for name in ordered if name and name in headings]
    if positions != sorted(positions):
        errors.append("canonical task sections are out of order")

    files = section(text, "Files to create / modify")
    file_paths = re.findall(
        r"(?<![a-zA-Z0-9_./-])((?:\.?[a-zA-Z0-9_-]+/)+[a-zA-Z0-9_.*{}-]+(?:\.[a-zA-Z0-9_-]+)?|"
        r"(?:Cargo|README|LICENSE|clippy|deny|rust-toolchain)\.[a-zA-Z0-9_-]+)",
        files,
    )
    if not files.strip() or not file_paths:
        errors.append("Files to create / modify must contain at least one concrete path")

    prose = _outside_fenced_blocks(text)
    placeholder_patterns = (
        (r"<command\s*\+\s*count>", "scaffold placeholder `<command + count>`"),
        (r"<file:line>", "scaffold placeholder `<file:line>`"),
        (r"(?<!`)``(?!`)", "empty inline-code marker ``"),
        (r"\bthe named expression\b", "damaged prose marker `the named expression`"),
    )
    for pattern, description in placeholder_patterns:
        if re.search(pattern, prose, re.I):
            errors.append(f"contains {description} outside a fenced block")

    lower_prose = prose.lower()
    if "baseline precondition" not in lower_prose:
        errors.append("missing an explicit baseline precondition")
    mismatch_index = lower_prose.find("baseline mismatch")
    if mismatch_index < 0:
        errors.append("missing an explicit baseline-mismatch contract")
    else:
        mismatch_contract = lower_prose[mismatch_index : mismatch_index + 1000]
        blocks = re.search(r"\b(?:stop|block(?:s|ed|ing)?)\b", mismatch_contract)
        reauthors = re.search(r"re-?author", mismatch_contract)
        if not blocks or not reauthors:
            errors.append(
                "baseline mismatch must block implementation and require task re-authoring"
            )

    dynamic_fallback_patterns = (
        r"\bpredecessor fallback\b",
        r"\bfollow an alternate branch\b",
        r"\bnone-or-predecessor-fallback\b",
    )
    if any(re.search(pattern, lower_prose) for pattern in dynamic_fallback_patterns):
        errors.append(
            "contains a dynamic predecessor fallback; a baseline mismatch must stop for re-authoring"
        )

    # A negative control must use a temporary fixture or a script self-test.
    # Editing a tracked file and later reverting it is unsafe in a dirty tree
    # and becomes especially misleading once an implementer has committed.
    mutating_control_patterns = (
        (r"\bgit\s+(?:checkout\s+--|restore\b|reset\b)", "Git mutation/revert instruction"),
        (r"(?m)^\s*(?:\$\s*)?sed\s+-i(?:\s|$)", "in-place sed instruction"),
        (r"(?m)^\s*(?:\$\s*)?cargo\s+add(?:\s|$)", "temporary cargo-add instruction"),
        (
            r"(?m)^\s*(?:\$\s*)?(?:printf|echo)\b[^\n]*>>\s*"
            r"(?:Cargo\.toml|justfile|crates/|tests/|deploy/|docs/|\.github/)",
            "append-to-tracked-file instruction",
        ),
    )
    for pattern, description in mutating_control_patterns:
        if re.search(pattern, text, re.I):
            errors.append(
                f"contains {description}; use a temporary fixture or explicit self-test mode"
            )

    for line_number, line in enumerate(text.splitlines(), 1):
        if re.search(r"\bgit\s+diff\b", line) and "origin/main...HEAD" not in line:
            errors.append(
                f"line {line_number}: git diff proof must use the merge-base range "
                "`origin/main...HEAD`"
            )

    skeleton = section(text, "Skeleton")
    generic_steps = (
        "Capture the complete baseline command and result",
        "Restrict the diff to the allowed files",
        "Apply exactly this operation",
        "Prove this postcondition",
        "Run every labeled verification command",
    )
    if all(step in skeleton for step in generic_steps):
        errors.append("Skeleton is the generated generic audit 1..5 scaffold")
    return errors


def parse_explicit_metadata(text: str, root: Path) -> tuple[dict, list[str]]:
    errors: list[str] = []
    header_lines = text.split("\n## ", 1)[0].splitlines()
    readiness_match = _single_header_match(
        header_lines, READINESS_OUTCOME_RE, "Readiness/Outcome", errors
    )
    gates_match = _single_header_match(
        header_lines, GATES_PACKAGES_RE, "Gates/Test packages", errors
    )

    metadata: dict = {
        "readiness": "missing",
        "outcome": "missing",
        "outcome_text": "missing",
        "superseded_by": None,
        "gates": [],
        "test_packages": [],
        "verification_commands": [],
    }

    if readiness_match:
        readiness = readiness_match.group("readiness")
        raw_outcome = readiness_match.group("outcome")
        metadata["readiness"] = readiness
        metadata["outcome_text"] = raw_outcome
        metadata["outcome"] = "superseded" if raw_outcome.startswith("superseded") else raw_outcome
        metadata["superseded_by"] = readiness_match.group("superseded")
        if readiness != "audited":
            errors.append(f"Readiness must be `audited`, found `{readiness}`")

    if gates_match:
        raw_gates = gates_match.group("gates")
        gates = raw_gates.split(",")
        metadata["gates"] = gates
        if raw_gates != ",".join(gates) or any(not gate for gate in gates):
            errors.append("Gates must be a comma-separated list without whitespace or empty entries")
        if len(gates) != len(set(gates)):
            errors.append("Gates must not contain duplicates")
        unknown = [gate for gate in gates if gate not in SUPPORTED_GATES]
        if unknown:
            errors.append("unsupported gate(s): " + ", ".join(unknown))
        if tuple(gates[:3]) != BASELINE_GATES:
            errors.append("Gates must start with exactly `fmt,clippy,test`")

        raw_packages = gates_match.group("packages")
        if raw_packages == "—":
            packages: list[str] = []
        else:
            packages = raw_packages.split(",")
            if raw_packages != ",".join(packages) or any(
                not re.fullmatch(r"[a-z0-9][a-z0-9-]*", package) for package in packages
            ):
                errors.append(
                    "Test packages must be `—` or a comma-separated package list without whitespace"
                )
            if len(packages) != len(set(packages)):
                errors.append("Test packages must not contain duplicates")
        metadata["test_packages"] = packages

    commands, command_errors = parse_verification_commands(text, root)
    metadata["verification_commands"] = commands
    errors.extend(command_errors)
    errors.extend(validate_rust_task_structure(text))
    metadata["gate_command"] = build_gate_command(
        metadata["gates"], metadata["test_packages"]
    )
    return metadata, errors


def read_task(root: Path, row: dict) -> dict:
    path = root / "docs" / "implementation" / row["file"]
    task = dict(row)
    task["path"] = str(path.relative_to(root)) if path.is_relative_to(root) else str(path)
    task["errors"] = []
    if not path.exists():
        task["errors"].append(f"task file missing: {task['path']}")
        return task

    text = path.read_text()
    lines = text.splitlines()
    h1_matches = [match for line in lines[:40] if (match := H1_RE.match(line))]
    if len(h1_matches) != 1:
        task["errors"].append(
            f"expected exactly one `# PR <id> — <title>` H1 in {path.name}, found {len(h1_matches)}"
        )
    else:
        task["title"] = h1_matches[0].group("title")
        if h1_matches[0].group("id") != row["id"]:
            task["errors"].append(
                f"row/filename says PR {row['id']} but H1 says PR {h1_matches[0].group('id')}"
            )

    status_matches = [match for line in lines[:40] if (match := STATUS_RE.match(line))]
    task["marker"] = "absent"
    if len(status_matches) != 1:
        task["errors"].append(f"expected exactly one Status header, found {len(status_matches)}")
    else:
        status_line = status_matches[0].group(0)
        task["marker"] = "done" if DONE_MARKER in status_line else "planned"
        task["status_text"] = status_matches[0].group("status")
        if task["marker"] == "planned" and "📋 Planned" not in status_line:
            task["errors"].append("Status must be `📋 Planned` or `✅ Done — <PR url>`")

    header = " ".join(re.sub(r"^>\s?", "", line) for line in lines[:40])
    size_match = SIZE_RE.search(header)
    task["size"] = size_match.group(1) if size_match else "?"
    if not size_match:
        task["errors"].append("missing or invalid Est. size metadata")
    crates_match = CRATES_RE.search(header)
    task["crates"] = crates_match.group(1).strip() if crates_match else ""
    if not crates_match:
        task["errors"].append("missing Crates touched metadata")
    deps_match = DEPS_RE.search(header)
    task["depends_on_text"] = deps_match.group(1).strip() if deps_match else ""
    if not deps_match:
        task["errors"].append("missing Depends on metadata")
    unlock_matches = []
    for line in lines[:40]:
        if match := UNLOCKS_RE.search(re.sub(r"^>\s?", "", line)):
            unlock_matches.append(match)
    task["unlocks_text"] = unlock_matches[0].group(1).strip() if len(unlock_matches) == 1 else ""
    if len(unlock_matches) != 1:
        task["errors"].append(f"expected exactly one Unlocks metadata value, found {len(unlock_matches)}")

    dod = section(text, "Definition of Done")
    dod_states = re.findall(r"^\s*- \[([ xX])\]", dod, re.M)
    task["dod_lines"] = len(dod_states)
    task["dod_checked"] = sum(state.lower() == "x" for state in dod_states)
    if not dod_states:
        task["errors"].append("Definition of Done has no checklist items")

    if phase_of(row["id"]) >= RUST_PHASE_FIRST:
        metadata, metadata_errors = parse_explicit_metadata(text, root)
        task.update(metadata)
        task["errors"].extend(metadata_errors)
    else:
        haystack = (dod + section(text, "What completed looks like")).lower()
        gates = list(BASELINE_GATES)
        for name, pattern in GATE_PATTERNS:
            if re.search(pattern, haystack, re.I):
                gates.append(name)
        packages = sorted(
            set(
                PKG_RE.findall(
                    " ".join(line for line in haystack.splitlines() if "cargo test" in line)
                )
            )
        )
        task.update(
            {
                "readiness": "legacy",
                "outcome": "change",
                "outcome_text": "change",
                "superseded_by": None,
                "gates": gates,
                "test_packages": packages,
                "verification_commands": [],
                "gate_command": build_gate_command(gates, packages),
            }
        )

    slug = path.stem
    task["slug"] = slug
    task["branch"] = slug
    task["mark_done_branch"] = f"pr-{row['id']}-mark-done"
    title = task.get("title", "<invalid title>")
    task["pr_title"] = f"PR {row['id']} — {title}"
    task["mark_done_pr_title"] = (
        f"PR {row['id']} — mark done (Status ✅, DoD ticks, README box)"
    )
    task["_text"] = text
    return task


def parse_dependencies(deps_text: str, known_ids: list[str]) -> tuple[list[str], Optional[str]]:
    """Parse every dependency token; reject text the parser would otherwise ignore."""
    no_annotations = re.sub(r"\([^)]*\)", "", deps_text).strip()
    if no_annotations in {"—", "-", "none", "None"}:
        return [], None
    matches = list(DEP_ID_RE.finditer(deps_text))
    remainder = DEP_ID_RE.sub("", deps_text)
    remainder = re.sub(r"\([^)]*\)", "", remainder)
    remainder = re.sub(r"\band\b|[\s,;&+/\u2014-]", "", remainder, flags=re.I)
    if remainder or not matches:
        return [], f"malformed dependency text `{deps_text}`"

    dependencies: list[str] = []
    for match in matches:
        low, high = match.group("lo"), match.group("hi")
        if not high:
            dependencies.append(low)
            continue
        if low not in known_ids or high not in known_ids or id_key(low) > id_key(high):
            return [], f"invalid dependency range `PR {low}–{high}`"
        dependencies.extend(
            task_id
            for task_id in known_ids
            if id_key(low) <= id_key(task_id) <= id_key(high)
        )
    if len(dependencies) != len(set(dependencies)):
        return dependencies, f"duplicate dependency in `{deps_text}`"
    return dependencies, None


def _local_link_errors(task: dict, root: Path) -> list[str]:
    errors: list[str] = []
    path = root / task["path"]
    in_fence = False
    for line_number, line in enumerate(task.get("_text", "").splitlines(), 1):
        if re.match(r"^\s*(?:///+\s*)?(?:```|~~~)", line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        for match in MARKDOWN_LINK_RE.finditer(line):
            raw = match.group("target").strip()
            # Optional markdown link title follows whitespace; task paths do not
            # contain spaces. Angle brackets are permitted by Markdown.
            raw = raw.split(maxsplit=1)[0].strip("<>")
            if raw.startswith(("https://", "http://", "mailto:", "#")):
                continue
            target = urllib.parse.unquote(raw.split("#", 1)[0].split("?", 1)[0])
            # Rustdoc/type links such as `[Self::validate]` are not filesystem
            # links. Only targets with an unmistakable path shape are checked.
            if "/" not in target and not target.endswith(".md") and not target.startswith("."):
                continue
            if target and not (path.parent / target).resolve().exists():
                errors.append(f"{task['path']}:{line_number}: broken local link `{raw}`")
    return errors


def _workspace_packages(root: Path) -> tuple[set[str], Optional[str]]:
    """Read member package names without invoking the pinned Rust toolchain.

    Roadmap validation runs in the cheap, docs-only CI job before Rust is
    installed.  Every workspace member lives in one of these two declared
    locations, so reading each manifest's package name is both sufficient for
    task-package validation and independent of rustup/network state.
    """
    manifests = sorted((root / "crates").glob("*/Cargo.toml"))
    manifests.append(root / "tests/e2e/Cargo.toml")
    packages: set[str] = set()
    for manifest in manifests:
        if not manifest.is_file():
            return set(), f"workspace member manifest missing: {manifest.relative_to(root)}"
        match = re.search(
            r'^\s*name\s*=\s*"([a-z0-9][a-z0-9-]*)"\s*$',
            manifest.read_text(),
            re.M,
        )
        if not match:
            return set(), f"cannot parse package name from {manifest.relative_to(root)}"
        packages.add(match.group(1))
    return packages, None


def _tracked_paths(root: Path) -> set[str]:
    process = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True
    )
    return {
        item.decode("utf-8")
        for item in process.stdout.split(b"\0")
        if item
    }


def validate_repository(
    root: Path, readme: Path, *, allow_untracked: bool = False
) -> ValidationResult:
    result = ValidationResult()
    if not readme.exists():
        result.errors.append(f"roadmap not found at {readme}")
        return result

    rows, row_errors = parse_readme(readme)
    result.rows = rows
    result.errors.extend(row_errors)
    if not rows:
        result.errors.append("no roadmap rows found")
        return result

    ids = [row["id"] for row in rows]
    files = [row["file"] for row in rows]
    for task_id in sorted({task_id for task_id in ids if ids.count(task_id) > 1}, key=id_key):
        result.errors.append(f"duplicate roadmap id {task_id}")
    for task_file in sorted({task_file for task_file in files if files.count(task_file) > 1}):
        result.errors.append(f"duplicate roadmap task path {task_file}")
    if ids != sorted(ids, key=id_key):
        result.errors.append("roadmap rows are not in task-id order")

    by_id = {row["id"]: row for row in rows}
    legacy_rows = [row for row in rows if phase_of(row["id"]) < RUST_PHASE_FIRST]
    rust_rows = [row for row in rows if phase_of(row["id"]) >= RUST_PHASE_FIRST]
    if len(legacy_rows) != LEGACY_TASK_COUNT:
        result.errors.append(
            f"expected {LEGACY_TASK_COUNT} phase 0-8 roadmap rows, found {len(legacy_rows)}"
        )
    if any(not row["checked"] for row in legacy_rows):
        result.errors.append("all phase 0-8 roadmap rows must remain completed")
    if len(rust_rows) not in {0, RUST_TASK_COUNT}:
        result.errors.append(
            f"partial Rust-roadmap activation: expected 0 or {RUST_TASK_COUNT} phase 9+ rows, found {len(rust_rows)}"
        )

    rust_skill = root / ".claude/skills/rust-skills/SKILL.md"
    rust_rules = sorted((root / ".claude/skills/rust-skills/rules").glob("*.md"))
    all_rust_paths = sorted(
        (root / "docs/implementation").glob("phase-*-rust-*/pr-*.md")
    )
    rust_paths = sorted(
        (
            path
            for path in all_rust_paths
            if (phase_match := PHASE_DIR_RE.match(path.parent.name))
            and RUST_PHASE_FIRST <= int(phase_match.group("phase")) <= RUST_PHASE_LAST
        ),
        key=lambda path: id_key(TASK_FILENAME_RE.match(path.name).group("id"))
        if TASK_FILENAME_RE.match(path.name)
        else (0, 0, ""),
    )
    if len(rust_rules) != RUST_TASK_COUNT:
        result.errors.append(f"expected {RUST_TASK_COUNT} Rust rule files, found {len(rust_rules)}")
    if len(rust_paths) != RUST_TASK_COUNT:
        result.errors.append(f"expected {RUST_TASK_COUNT} Rust task files, found {len(rust_paths)}")
    for path in sorted(set(all_rust_paths) - set(rust_paths)):
        result.errors.append(f"Rust task is outside canonical phases 9-34: {path.relative_to(root)}")

    rule_names = [path.name for path in rust_rules]
    if len(rule_names) != len(set(rule_names)):
        result.errors.append("duplicate Rust rule filenames")
    if not rust_skill.is_file():
        result.errors.append("missing .claude/skills/rust-skills/SKILL.md")
    else:
        skill_rule_links = re.findall(
            r"\]\(rules/([a-z0-9][a-z0-9-]*\.md)(?:#[^)]+)?\)",
            rust_skill.read_text(),
        )
        if len(skill_rule_links) != RUST_TASK_COUNT:
            result.errors.append(
                f"Rust skill must contain exactly {RUST_TASK_COUNT} rule links, found {len(skill_rule_links)}"
            )
        if len(set(skill_rule_links)) != RUST_TASK_COUNT:
            result.errors.append(
                f"Rust skill must contain {RUST_TASK_COUNT} unique rule links, found {len(set(skill_rule_links))}"
            )
        missing_skill_links = sorted(set(rule_names) - set(skill_rule_links))
        extra_skill_links = sorted(set(skill_rule_links) - set(rule_names))
        if missing_skill_links or extra_skill_links:
            result.errors.append(
                "Rust skill/rules link-set drift: missing="
                + ",".join(missing_skill_links)
                + " extra="
                + ",".join(extra_skill_links)
            )

    for phase, (directory, prefix, expected_count) in RUST_PHASE_SPECS.items():
        phase_paths = [
            path
            for path in rust_paths
            if (match := TASK_FILENAME_RE.match(path.name))
            and phase_of(match.group("id")) == phase
        ]
        if len(phase_paths) != expected_count:
            result.errors.append(
                f"phase {phase} must contain {expected_count} `{prefix}-*` tasks, found {len(phase_paths)}"
            )
        wrong_directories = sorted(
            str(path.relative_to(root)) for path in phase_paths if path.parent.name != directory
        )
        if wrong_directories:
            result.errors.append(
                f"phase {phase} tasks must live in `{directory}`: " + ", ".join(wrong_directories)
            )
        numbers: list[int] = []
        for path in phase_paths:
            match = TASK_FILENAME_RE.match(path.name)
            assert match is not None
            task_key = id_key(match.group("id"))
            numbers.append(task_key[1])
            if task_key[2]:
                result.errors.append(f"{path.relative_to(root)}: Rust task ids may not use letter suffixes")
            if not match.group("rule").startswith(prefix + "-"):
                result.errors.append(
                    f"{path.relative_to(root)}: phase {phase} rule slug must start `{prefix}-`"
                )
        if sorted(numbers) != list(range(1, expected_count + 1)):
            result.errors.append(
                f"phase {phase} ticket numbers must be contiguous 1..{expected_count}; found "
                + ",".join(map(str, sorted(numbers)))
            )
        phase_rule_names = [name for name in rule_names if name.startswith(prefix + "-")]
        if len(phase_rule_names) != expected_count:
            result.errors.append(
                f"Rust rule category `{prefix}-*` must contain {expected_count} files, found {len(phase_rule_names)}"
            )
    rust_ids: list[str] = []
    rust_task_files: list[str] = []
    rule_to_task: dict[str, str] = {}
    parsed_rust: list[dict] = []
    for path in rust_paths:
        filename_match = TASK_FILENAME_RE.match(path.name)
        if not filename_match:
            result.errors.append(f"malformed Rust task filename: {path.relative_to(root)}")
            continue
        task_id = filename_match.group("id")
        rule_name = filename_match.group("rule") + ".md"
        phase_match = PHASE_DIR_RE.match(path.parent.name)
        assert phase_match is not None
        if phase_of(task_id) != int(phase_match.group("phase")):
            result.errors.append(
                f"{path.relative_to(root)}: task id phase does not match its directory"
            )
        if rule_name not in rule_names:
            result.errors.append(f"{path.relative_to(root)}: no Rust rule `{rule_name}`")
        if rule_name in rule_to_task:
            result.errors.append(
                f"Rust rule `{rule_name}` maps to both {rule_to_task[rule_name]} and {path.name}"
            )
        else:
            rule_to_task[rule_name] = path.name
        rust_ids.append(task_id)
        relative_to_docs = str(path.relative_to(root / "docs/implementation"))
        rust_task_files.append(relative_to_docs)
        row = by_id.get(
            task_id,
            {
                "id": task_id,
                "file": relative_to_docs,
                "checked": False,
                "delivers": "",
                "design": "",
                "lineno": 0,
            },
        )
        task = read_task(root, row)
        task["rule"] = filename_match.group("rule")
        task["rule_path"] = f".claude/skills/rust-skills/rules/{rule_name}"
        parsed_rust.append(task)
        for error in task.get("errors", []):
            result.errors.append(f"{task['path']}: {error}")
        primary_link = f".claude/skills/rust-skills/rules/{rule_name}"
        prose = _outside_fenced_blocks(task.get("_text", ""))
        if not re.search(
            r"\]\([^)]*" + re.escape(primary_link) + r"(?:#[^)]*)?\)", prose
        ):
            result.errors.append(f"{task['path']}: missing primary rule link `{primary_link}`")
        if task_id in by_id:
            if row.get("delivers") != task.get("title"):
                result.errors.append(
                    f"{task['path']}: roadmap Delivers must equal H1 title exactly"
                )
            if row.get("design", "").strip("`") != filename_match.group("rule"):
                result.errors.append(
                    f"{task['path']}: roadmap Design must equal rule slug `{filename_match.group('rule')}`"
                )
        if task.get("outcome") in {"evidence", "superseded"}:
            note_path = (
                "docs/implementation/notes/rust-skills/"
                + filename_match.group("rule")
                + ".md"
            )
            if note_path not in section(task.get("_text", ""), "Files to create / modify"):
                result.errors.append(
                    f"{task['path']}: {task.get('outcome')} outcome must create `{note_path}`"
                )
        result.errors.extend(_local_link_errors(task, root))

    if len(rust_ids) != len(set(rust_ids)):
        result.errors.append("duplicate Rust task ids")
    if len(rust_task_files) != len(set(rust_task_files)):
        result.errors.append("duplicate Rust task paths/branch slugs")
    missing_mappings = sorted(set(rule_names) - set(rule_to_task))
    if missing_mappings:
        result.errors.append("Rust rules without tasks: " + ", ".join(missing_mappings))
    if rust_ids and (rust_ids[0] != RUST_FIRST_ID or rust_ids[-1] != RUST_LAST_ID):
        result.errors.append(
            f"Rust task bounds must be {RUST_FIRST_ID}…{RUST_LAST_ID}, found {rust_ids[0]}…{rust_ids[-1]}"
        )
    present_phases = {phase_of(task_id) for task_id in rust_ids}
    expected_phases = set(range(RUST_PHASE_FIRST, RUST_PHASE_LAST + 1))
    if present_phases != expected_phases:
        result.errors.append(
            "Rust phases must be exactly 9-34; missing="
            + ",".join(map(str, sorted(expected_phases - present_phases)))
            + " extra="
            + ",".join(map(str, sorted(present_phases - expected_phases)))
        )

    if len(rust_rows) == RUST_TASK_COUNT:
        if set(row["id"] for row in rust_rows) != set(rust_ids):
            result.errors.append("Rust roadmap ids are not a bijection with Rust task ids")
        if set(row["file"] for row in rust_rows) != set(rust_task_files):
            result.errors.append("Rust roadmap links are not a bijection with Rust task paths")

    # Parse every active roadmap task, including the legacy corpus.  Reuse the
    # richer Rust parse above so errors and link checks are not duplicated.
    rust_by_id = {task["id"]: task for task in parsed_rust}
    for row in rows:
        task = rust_by_id.get(row["id"]) or read_task(root, row)
        result.tasks[row["id"]] = task
        if row["id"] not in rust_by_id:
            for error in task.get("errors", []):
                result.errors.append(f"{task['path']}: {error}")
            result.errors.extend(_local_link_errors(task, root))
        expected_marker = "done" if row["checked"] else "planned"
        if task.get("marker") != expected_marker:
            drift = {
                "id": row["id"],
                "path": task.get("path", row["file"]),
                "checked": row["checked"],
                "marker": task.get("marker", "absent"),
                "lineno": row["lineno"],
            }
            result.drifts.append(drift)
            result.errors.append(
                f"state drift for PR {row['id']}: roadmap={'checked' if row['checked'] else 'unchecked'} "
                f"but marker={task.get('marker', 'absent')}"
            )
        if phase_of(row["id"]) >= RUST_PHASE_FIRST:
            if row["checked"] and task.get("dod_checked") != task.get("dod_lines"):
                result.errors.append(f"{task['path']}: checked Rust task has unchecked DoD items")
            if not row["checked"] and task.get("dod_checked", 0):
                result.errors.append(f"{task['path']}: planned Rust task has checked DoD items")

    # An inactive Rust corpus must still be wholly planned and audited.
    if not rust_rows:
        for task in parsed_rust:
            if task.get("marker") != "planned":
                result.errors.append(f"{task['path']}: inactive Rust task must remain planned")
            if task.get("dod_checked", 0):
                result.errors.append(f"{task['path']}: inactive Rust task has checked DoD items")

    known_ids = sorted(set(ids) | set(rust_ids), key=id_key)
    all_tasks_for_deps = {
        **{task_id: task for task_id, task in result.tasks.items()},
        **rust_by_id,
    }
    for task_id, task in all_tasks_for_deps.items():
        dependencies, dependency_error = parse_dependencies(task.get("depends_on_text", ""), known_ids)
        task["depends_on"] = dependencies
        if dependency_error:
            result.errors.append(f"{task['path']}: {dependency_error}")
            continue
        for dependency in dependencies:
            if dependency not in known_ids:
                result.errors.append(f"{task['path']}: dependency PR {dependency} is not known")
            elif id_key(dependency) >= id_key(task_id):
                result.errors.append(f"{task['path']}: dependency PR {dependency} is not earlier than PR {task_id}")

    for index, task in enumerate(parsed_rust):
        expected_previous = "8.5" if index == 0 else parsed_rust[index - 1]["id"]
        expected_next = "—" if index == len(parsed_rust) - 1 else f"PR {parsed_rust[index + 1]['id']}"
        if task.get("depends_on_text") != f"PR {expected_previous}":
            result.errors.append(
                f"{task['path']}: exact serial dependency must be `PR {expected_previous}`"
            )
        if task.get("unlocks_text") != expected_next:
            result.errors.append(f"{task['path']}: Unlocks must be `{expected_next}`")

        superseded_by = task.get("superseded_by")
        if superseded_by:
            if superseded_by not in known_ids:
                result.errors.append(f"{task['path']}: superseding PR {superseded_by} is not known")
            elif id_key(superseded_by) >= id_key(task["id"]):
                result.errors.append(f"{task['path']}: superseding PR must precede this task")

    # Logical, non-mutating selection simulation for the full Rust sequence.
    simulated_done = {row["id"] for row in legacy_rows if row["checked"]}
    for task in parsed_rust:
        unmet = [dependency for dependency in task.get("depends_on", []) if dependency not in simulated_done]
        if unmet:
            result.errors.append(
                f"selection simulation stops at PR {task['id']}; unmet=" + ",".join(unmet)
            )
            break
        simulated_done.add(task["id"])

    packages, packages_error = _workspace_packages(root)
    if packages_error:
        result.errors.append(packages_error)
    else:
        for task in parsed_rust:
            unknown_packages = sorted(set(task.get("test_packages", [])) - packages)
            if unknown_packages:
                result.errors.append(
                    f"{task['path']}: unknown workspace test package(s): "
                    + ", ".join(unknown_packages)
                )

    try:
        tracked = _tracked_paths(root)
        required_paths: list[str] = []
        for path in [readme, rust_skill, *rust_rules, *rust_paths]:
            try:
                required_paths.append(str(path.resolve().relative_to(root)))
            except ValueError:
                if path != readme:
                    result.errors.append(f"validated input is outside the repository: {path}")
        result.untracked = sorted(path for path in required_paths if path not in tracked)
        if not allow_untracked:
            for path in result.untracked:
                result.errors.append(f"required roadmap input is not Git-tracked: {path}")
    except subprocess.CalledProcessError as exc:
        result.errors.append(f"git ls-files failed with exit {exc.returncode}")

    # A completed Rust row may only appear after all prior Rust rows.  This is
    # redundant with dependencies but gives a precise state-drift diagnostic.
    saw_unchecked = False
    for row in rust_rows:
        if not row["checked"]:
            saw_unchecked = True
        elif saw_unchecked:
            result.errors.append(f"Rust completion is not a contiguous prefix at PR {row['id']}")

    result.rust_tasks = parsed_rust
    # Stable output: one copy of each diagnostic, in discovery order.
    result.errors = list(dict.fromkeys(result.errors))
    return result


def emit(task: dict) -> None:
    public = {key: value for key, value in task.items() if not key.startswith("_")}
    print(json.dumps(public, indent=2, ensure_ascii=False))


def errors_are_drift_only(result: ValidationResult) -> bool:
    """Return whether every validation error is a done-state reconciliation."""
    return bool(result.drifts) and all(
        error.startswith("state drift for PR ")
        or error == "all phase 0-8 roadmap rows must remain completed"
        or error.endswith("checked Rust task has unchecked DoD items")
        or error.endswith("planned Rust task has checked DoD items")
        for error in result.errors
    )


def print_validation(result: ValidationResult) -> int:
    rust_rows = [row for row in result.rows if phase_of(row["id"]) >= RUST_PHASE_FIRST]
    todo = [row for row in result.rows if not row["checked"]]
    done = [row for row in result.rows if row["checked"]]
    outcome_counts = {
        outcome: sum(task.get("outcome") == outcome for task in result.rust_tasks)
        for outcome in ("change", "evidence", "superseded")
    }
    if result.errors:
        drift_only = errors_are_drift_only(result)
        print("VALIDATION=" + ("DRIFT" if drift_only else "FAIL"))
        for error in result.errors:
            print(f"ERROR={error}")
        print(f"TOTAL={len(result.rows)}")
        print(f"DONE={len(done)}")
        print(f"REMAINING={len(todo)}")
        print(f"NEXT={todo[0]['id'] if todo else '-'}")
        print(f"LAST={RUST_LAST_ID}")
        print(f"RUST_ROWS={len(rust_rows)}")
        print(
            "OUTCOMES="
            + ",".join(f"{outcome}:{count}" for outcome, count in outcome_counts.items())
        )
        print(f"UNTRACKED_ROADMAP_FILES={len(result.untracked)}")
        return 5 if drift_only else 4
    print("VALIDATION=PASS")
    print(f"TOTAL={len(result.rows)}")
    print(f"DONE={len(done)}")
    print(f"REMAINING={len(todo)}")
    print(f"NEXT={todo[0]['id'] if todo else '-'}")
    print(f"LAST={RUST_LAST_ID}")
    print(f"RUST_ROWS={len(rust_rows)}")
    print(
        "OUTCOMES="
        + ",".join(f"{outcome}:{count}" for outcome, count in outcome_counts.items())
    )
    print(f"UNTRACKED_ROADMAP_FILES={len(result.untracked)}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--task")
    mode.add_argument("--validate-all", action="store_true")
    parser.add_argument("--readme", type=Path, help="alternate roadmap path (tests only)")
    parser.add_argument(
        "--require-tracked",
        action="store_true",
        help="fail when a present roadmap input is not in the Git index (preflight/CI)",
    )
    args = parser.parse_args(argv)

    if args.require_tracked and not args.validate_all:
        parser.error("--require-tracked is only valid with --validate-all")

    root = repo_root()
    readme = args.readme or root / "docs/implementation/README.md"
    # Plain --validate-all is authoring-friendly: it validates present content
    # and reports UNTRACKED_ROADMAP_FILES without forcing authors to stage it.
    # Selection and preflight remain fail-closed; preflight uses
    # --validate-all --require-tracked.
    allow_untracked = args.validate_all and not args.require_tracked
    result = validate_repository(root, readme, allow_untracked=allow_untracked)
    if args.validate_all:
        return print_validation(result)

    if result.errors:
        # Keep the established drift verdict for direct selection when drift is
        # the only inconsistency.  --validate-all and preflight still fail it.
        if not args.status and not args.task and errors_are_drift_only(result):
            drift = result.drifts[0]
            task = result.tasks[drift["id"]]
            print(f"VERDICT=DRIFT\nTASK={drift['id']}\nPATH={drift['path']}")
            print(f"BRANCH={task.get('branch', '-')}")
            print(f"MARK_DONE_BRANCH={task.get('mark_done_branch', '-')}")
            status_text = task.get("status_text", "")
            merged_url = re.search(r"https://github\.com/[^\s)]+/pull/\d+", status_text)
            print(f"MERGED_PR={merged_url.group(0) if merged_url else '-'}")
            print(
                "REASON=roadmap box and task Status marker disagree "
                f"(box={'checked' if drift['checked'] else 'unchecked'}, marker={drift['marker']})"
            )
            print("FIX=reconcile both done signals via a docs-only PR, then re-select")
            return 5
        print("VERDICT=PARSE_ERROR")
        for error in result.errors:
            print(f"ERROR={error}")
        return 4

    rows = result.rows
    by_id = {row["id"]: row for row in rows}
    if args.task:
        if args.task not in by_id:
            print(f"VERDICT=PARSE_ERROR\nERROR=no roadmap row for task {args.task}")
            return 4
        task = result.tasks[args.task]
        print("VERDICT=TASK")
        print(f"BOX={'checked' if task['checked'] else 'unchecked'}")
        print(f"MARKER={task.get('marker', 'absent')}")
        emit(task)
        return 0

    todo = [row for row in rows if not row["checked"]]
    done = [row for row in rows if row["checked"]]
    if args.status:
        print(f"VERDICT=STATUS\nTOTAL={len(rows)}\nDONE={len(done)}\nREMAINING={len(todo)}")
        if todo:
            print(f"NEXT={todo[0]['id']}")
        return 0
    if not todo:
        print("VERDICT=ALL_DONE")
        return 2

    task = result.tasks[todo[0]["id"]]
    unmet = []
    for dependency in task.get("depends_on", []):
        row = by_id.get(dependency)
        if row is None:
            unmet.append(f"PR {dependency} (not in the active roadmap)")
        elif not row["checked"]:
            unmet.append(f"PR {dependency} (still ☐)")
    if unmet:
        print(f"VERDICT=NO_ELIGIBLE\nTASK={task['id']}")
        for dependency in unmet:
            print(f"UNMET_DEP={dependency}")
        return 3

    print("VERDICT=SELECTED")
    emit(task)
    return 0


if __name__ == "__main__":
    sys.exit(main())
