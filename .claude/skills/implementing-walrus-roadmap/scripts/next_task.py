#!/usr/bin/env python3
"""Select the next unfinished walrus roadmap task from docs/implementation/README.md.

The roadmap table in that README is the sole done-tracker: one row per task,
first cell `☐` (todo) or `✅` (done). Each row links the task file, whose header
carries a second done signal (`> **Status:** ✅ Done — <url>`). Both are written
together by the mark-done PR, so a row where they disagree is *drift* — a
mark-done that only half landed — never a task to re-implement.

This script is the single source of truth for selection, task-header extraction,
dependency checking, and the gate list a task needs, so the loop never parses
markdown itself.

Usage:
  next_task.py                select the next task, print JSON to stdout
  next_task.py --status       print progress counts and the would-be selection
  next_task.py --task <id>    print JSON for one task (any state), e.g. 9.1
  …any mode plus --readme <path>   read an alternate roadmap file (tests only)

Exit codes:
  0  task selected / status printed
  2  ALL_DONE      every roadmap row is ✅
  3  NO_ELIGIBLE   the next ☐ task has an unmet dependency (only possible if the
                   README was hand-edited out of order — the chain is printed)
  4  PARSE_ERROR   a roadmap row or task header failed to parse
  5  DRIFT         a ☐ row whose task file already says Done (half-landed
                   mark-done) — reconcile it, do not implement it
"""

import json
import re
import subprocess
import sys
from pathlib import Path

# Roadmap row, e.g.:
# | ☐ | [9.1](./phase-9-rust-ownership/pr-9.1-own-borrow-over-clone.md) | Delivers | Design |
# The box chars are U+2610 / U+2705 and the id cell must be a markdown link —
# that alone excludes the per-table header rows (`| ✅ | PR | Delivers | Design |`).
ROW_RE = re.compile(
    r"^\|\s*(?P<box>[☐✅])\s*\|\s*\[(?P<id>\d+\.\d+[a-z]?)\]\((?P<file>[^)]+)\)\s*\|"
    r"(?P<delivers>[^|]*)\|(?P<design>[^|]*)\|\s*$"
)
HEADER_ROW_RE = re.compile(r"^\|\s*[☐✅]\s*\|\s*PR\s*\|")

# Task-file header, e.g.
#   # PR 9.1 — Delete the redundant and implicit clones …
#   > **Status:** ✅ Done — https://github.com/athvin/walrus/pull/125
#   > **Phase:** 9 — … · **Crates touched:** `loader` · **Est. size:** S ·
#   > **Depends on:** PR 8.5 · **Unlocks:** PR 9.2
H1_RE = re.compile(r"^#\s+PR\s+(?P<id>\d+\.\d+[a-z]?)\s+[—-]\s+(?P<title>.+?)\s*$")
STATUS_RE = re.compile(r"^>\s*\*\*Status:\*\*\s*(?P<status>.+?)\s*$")
DONE_MARKER = "**Status:** ✅ Done"  # keep in sync with mark_done.py + SKILL.md
SIZE_RE = re.compile(r"\*\*Est\. size:\*\*\s*([SML])")
CRATES_RE = re.compile(r"\*\*Crates touched:\*\*\s*(.+?)\s*·\s*\*\*Est\. size")
DEPS_RE = re.compile(r"\*\*Depends on:\*\*\s*(.+?)\s*(?:·\s*\*\*Unlocks|$)")
# "PR 8.5", "PR 7.1–7.6", "PR 2.9, PR 2.10". The dash may be en/em/hyphen.
DEP_ID_RE = re.compile(
    r"PR\s*(?P<lo>\d+\.\d+[a-z]?)(?:\s*[–—-]\s*(?:PR\s*)?(?P<hi>\d+\.\d+[a-z]?))?"
)

# Which extra gate a Definition of Done implies. Matched case-insensitively over
# the task's DoD + "What completed looks like" text. fmt/clippy/test are implicit
# in every task and always prepended.
GATE_PATTERNS = [
    ("compose", r"docker compose|up --wait|just up\b|compose stack"),
    ("sqlx", r"sqlx prepare|\.sqlx\b|query_file!"),
    ("conformance", r"features?[ =\"']*conformance"),
    ("e2e", r"--features it\b|tests/e2e|\be2e\b"),
    ("deny", r"cargo[ -]deny|deny\.toml"),
    ("manifests", r"kubeconform|kustomize"),
    ("images", r"image smoke|docker build|deploy/docker/Dockerfile"),
    ("msrv", r"\bmsrv\b|rust-version"),
]
PKG_RE = re.compile(r"-p\s+([a-z0-9][a-z0-9-]*)")


def repo_root() -> Path:
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True, text=True, check=True,
    )
    return Path(out.stdout.strip())


def id_key(task_id: str):
    """Sortable key for a task id: 9.10 sorts after 9.2, and 8.4a after 8.4."""
    m = re.match(r"^(\d+)\.(\d+)([a-z]?)$", task_id)
    if not m:
        return (0, 0, "")
    return (int(m.group(1)), int(m.group(2)), m.group(3))


def parse_readme(readme: Path):
    """Return (ordered list of rows, list of parse errors)."""
    rows, errors = [], []
    for lineno, line in enumerate(readme.read_text().splitlines(), 1):
        if not line.startswith("|"):
            continue
        if not re.match(r"^\|\s*[☐✅]", line):
            continue          # separator rows, other tables
        if HEADER_ROW_RE.match(line):
            continue          # per-phase table header
        m = ROW_RE.match(line)
        if not m:
            errors.append(f"line {lineno}: unparseable roadmap row: {line}")
            continue
        rows.append({
            "id": m.group("id"),
            "file": m.group("file").lstrip("./"),
            "checked": m.group("box") == "✅",
            "delivers": m.group("delivers").strip(),
            "design": m.group("design").strip(),
            "lineno": lineno,
        })
    return rows, errors


def section(text: str, heading: str) -> str:
    """Text of one `## heading` section, up to the next `## ` heading."""
    m = re.search(rf"^##\s+{re.escape(heading)}\s*$", text, re.M)
    if not m:
        return ""
    rest = text[m.end():]
    nxt = re.search(r"^##\s+", rest, re.M)
    return rest[: nxt.start()] if nxt else rest


def read_task(root: Path, row: dict) -> dict:
    path = root / "docs" / "implementation" / row["file"]
    t = dict(row)
    t["path"] = str(path.relative_to(root))
    if not path.exists():
        t["header_error"] = f"task file missing: {t['path']}"
        return t

    text = path.read_text()
    lines = text.splitlines()

    for line in lines[:20]:
        m = H1_RE.match(line)
        if m:
            t["title"] = m.group("title")
            if m.group("id") != row["id"]:
                t["header_error"] = (
                    f"README row says PR {row['id']} but {path.name}'s H1 says "
                    f"PR {m.group('id')}"
                )
            break
    if "title" not in t:
        t["header_error"] = f"no `# PR <id> — <title>` H1 in {path.name}"
        return t

    t["marker"] = "absent"
    for line in lines[:20]:
        m = STATUS_RE.match(line)
        if m:
            t["marker"] = "done" if DONE_MARKER in line else "planned"
            t["status_text"] = m.group("status")
            break

    # The metadata blockquote wraps across lines; strip the `> ` prefixes before
    # joining so a wrapped `**Est. size:**` still follows its `·` separator.
    header = " ".join(re.sub(r"^>\s?", "", l) for l in lines[:40])
    sm = SIZE_RE.search(header)
    t["size"] = sm.group(1) if sm else "?"
    cm = CRATES_RE.search(header)
    t["crates"] = cm.group(1).strip() if cm else ""
    dm = DEPS_RE.search(header)
    t["depends_on_text"] = dm.group(1).strip() if dm else "—"

    dod = section(text, "Definition of Done")
    t["dod_lines"] = len([l for l in dod.splitlines() if l.strip().startswith("- [")])
    haystack = (dod + section(text, "What completed looks like")).lower()
    gates = ["fmt", "clippy", "test"]
    for name, pattern in GATE_PATTERNS:
        if re.search(pattern, haystack, re.I):
            gates.append(name)
    t["gates"] = gates
    t["test_packages"] = sorted(
        set(PKG_RE.findall(" ".join(
            l for l in haystack.splitlines() if "cargo test" in l
        )))
    )

    # Everything the loop needs to name branches, PRs and files, computed once
    # here so no two steps can disagree about them.
    slug = Path(row["file"]).stem                       # pr-9.1-own-borrow-over-clone
    t["slug"] = slug
    t["branch"] = slug
    t["mark_done_branch"] = f"pr-{row['id']}-mark-done"
    t["pr_title"] = f"PR {row['id']} — {t['title']}"
    t["mark_done_pr_title"] = f"PR {row['id']} — mark done (Status ✅, DoD ticks, README box)"
    return t


def expand_deps(deps_text: str, known_ids: list) -> list:
    """Explicit dep ids, with `PR 7.1–7.6` ranges expanded against the roadmap."""
    out = []
    for m in DEP_ID_RE.finditer(deps_text):
        lo, hi = m.group("lo"), m.group("hi")
        if not hi:
            out.append(lo)
            continue
        lo_k, hi_k = id_key(lo), id_key(hi)
        if lo_k > hi_k:                                  # inverted: leave as-is
            out.extend([lo, hi])
            continue
        out.extend([i for i in known_ids if lo_k <= id_key(i) <= hi_k])
    return out


def emit(t: dict) -> None:
    print(json.dumps(t, indent=2, ensure_ascii=False))


def main() -> int:
    root = repo_root()
    argv = sys.argv[1:]
    if "--readme" in argv:                               # test hook
        i = argv.index("--readme")
        readme = Path(argv[i + 1])
        del argv[i:i + 2]
    else:
        readme = root / "docs" / "implementation" / "README.md"
    if not readme.exists():
        print(f"VERDICT=PARSE_ERROR\nERROR=roadmap not found at {readme}")
        return 4

    rows, errors = parse_readme(readme)
    if errors:
        print("VERDICT=PARSE_ERROR")
        for e in errors:
            print(f"ERROR={e}")
        return 4
    if not rows:
        print("VERDICT=PARSE_ERROR\nERROR=no roadmap rows found in docs/implementation/README.md")
        return 4

    by_id = {r["id"]: r for r in rows}
    known_ids = [r["id"] for r in rows]

    if argv[0:1] == ["--task"]:
        if len(argv) < 2 or argv[1] not in by_id:
            print(f"VERDICT=PARSE_ERROR\nERROR=no roadmap row for task {argv[1:2]}")
            return 4
        t = read_task(root, by_id[argv[1]])
        print("VERDICT=TASK")
        print(f"BOX={'checked' if t['checked'] else 'unchecked'}")
        print(f"MARKER={t.get('marker', 'absent')}")
        emit(t)
        return 0

    todo = [r for r in rows if not r["checked"]]
    done = [r for r in rows if r["checked"]]

    if argv[0:1] == ["--status"]:
        print(f"VERDICT=STATUS\nTOTAL={len(rows)}\nDONE={len(done)}\nREMAINING={len(todo)}")
        if todo:
            print(f"NEXT={todo[0]['id']}")
        return 0

    if not todo:
        print("VERDICT=ALL_DONE")
        return 2

    t = read_task(root, todo[0])
    if "header_error" in t:
        print(f"VERDICT=PARSE_ERROR\nERROR={t['header_error']}")
        return 4

    # Drift: the row is ☐ but the file already says Done — a mark-done PR that
    # only half landed. Reconciling is a docs-only PR, never a re-implementation.
    if t["marker"] == "done":
        print(f"VERDICT=DRIFT\nTASK={t['id']}\nPATH={t['path']}")
        print(f"REASON=task file says Done but its README row (line {t['lineno']}) is still ☐")
        print("FIX=reconcile the README box via a docs-only PR, then re-select")
        return 5

    unmet = []
    for dep in expand_deps(t["depends_on_text"], known_ids):
        row = by_id.get(dep)
        if row is None:
            unmet.append(f"PR {dep} (not in the roadmap)")
        elif not row["checked"]:
            unmet.append(f"PR {dep} (still ☐)")
    if unmet:
        print(f"VERDICT=NO_ELIGIBLE\nTASK={t['id']}")
        for u in unmet:
            print(f"UNMET_DEP={u}")
        return 3

    print("VERDICT=SELECTED")
    emit(t)
    return 0


if __name__ == "__main__":
    sys.exit(main())
