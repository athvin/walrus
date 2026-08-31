#!/usr/bin/env python3
"""Record a merged walrus task as done — the three edits, in one idempotent step.

walrus tracks "done" in two places that must always agree:
  1. the task file's status marker  `> **Status:** ✅ Done — <PR url>`
     (+ every `- [ ]` in its Definition of Done ticked to `- [x]`), and
  2. the roadmap row in docs/implementation/README.md  `| ☐ |` → `| ✅ |`.

next_task.py reads both; a disagreement is drift it refuses to guess around.
Doing all three edits from one script is what keeps them in lockstep, and being
idempotent is what lets an interrupted mark-done PR simply be re-run.

These edits ship as their own docs-only PR, AFTER the code PR merged green — a
docs-only diff skips the compile-heavy CI jobs, so the bookkeeping costs ~2
minutes instead of another full DuckDB build.

Usage:
  mark_done.py <id> --pr <url|number> [--note "<text>"] [--readme <path>]

Exit codes:
  0  VERDICT=MARKED or ALREADY_DONE (idempotent success either way)
  1  VERDICT=ERROR — nothing was written; the reason is printed
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_URL = "https://github.com/athvin/walrus"
DONE_MARKER = "**Status:** ✅ Done"     # keep in sync with next_task.py + SKILL.md
STATUS_RE = re.compile(r"^>\s*\*\*Status:\*\*")
H1_RE = re.compile(r"^#\s+PR\s+\d+\.\d+[a-z]?\s+[—-]\s+")
DOD_HEADING_RE = re.compile(r"^##\s+Definition of Done\s*$")
NEXT_HEADING_RE = re.compile(r"^##\s+")
UNCHECKED_RE = re.compile(r"^(\s*)- \[ \]")


def repo_root() -> Path:
    out = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                         capture_output=True, text=True, check=True)
    return Path(out.stdout.strip())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("id", help="task id, e.g. 9.1")
    ap.add_argument("--pr", required=True, help="PR url or number of the merged code PR")
    ap.add_argument("--note", default="", help="short qualifier, e.g. 'ManifestId slice only'")
    ap.add_argument("--readme", default=None, help="alternate roadmap path (tests only)")
    a = ap.parse_args()

    root = repo_root()
    readme = Path(a.readme) if a.readme else root / "docs" / "implementation" / "README.md"
    if not readme.exists():
        print(f"VERDICT=ERROR\nERROR=roadmap not found at {readme}")
        return 1

    pr_url = a.pr if a.pr.startswith("http") else f"{REPO_URL}/pull/{a.pr.lstrip('#')}"
    marker = f"> {DONE_MARKER} — " + (f"{a.note}: {pr_url}" if a.note else pr_url)

    # --- locate the row and its task file -----------------------------------
    row_re = re.compile(
        r"^(?P<head>\|\s*)(?P<box>[☐✅])(?P<mid>\s*\|\s*\[" + re.escape(a.id) + r"\]\((?P<file>[^)]+)\))"
    )
    lines = readme.read_text().splitlines(keepends=True)
    hits = [(i, m) for i, l in enumerate(lines) if (m := row_re.match(l))]
    if len(hits) != 1:
        print(f"VERDICT=ERROR\nERROR=expected exactly one roadmap row for PR {a.id}, found {len(hits)}")
        return 1
    idx, m = hits[0]
    task_path = root / "docs" / "implementation" / m.group("file").lstrip("./")
    if not task_path.exists():
        print(f"VERDICT=ERROR\nERROR=task file missing: {task_path}")
        return 1

    changed = []

    # --- 1 + 2: the task file (status marker, then the DoD ticks) -----------
    tlines = task_path.read_text().splitlines(keepends=True)
    status_idx = next((i for i, l in enumerate(tlines) if STATUS_RE.match(l)), None)
    if status_idx is None:
        h1 = next((i for i, l in enumerate(tlines) if H1_RE.match(l)), None)
        if h1 is None:
            print(f"VERDICT=ERROR\nERROR=no `# PR <id> — <title>` H1 in {task_path.name}")
            return 1
        tlines.insert(h1 + 1, "\n" + marker + "\n")
        print("STATUS_LINE=inserted")
    elif DONE_MARKER in tlines[status_idx]:
        print("STATUS_LINE=already")
    else:
        tlines[status_idx] = marker + "\n"
        print("STATUS_LINE=set")

    ticked = 0
    in_dod = False
    for i, line in enumerate(tlines):
        if DOD_HEADING_RE.match(line):
            in_dod = True
            continue
        if in_dod and NEXT_HEADING_RE.match(line):
            break
        if in_dod and UNCHECKED_RE.match(line):
            tlines[i] = UNCHECKED_RE.sub(r"\1- [x]", line, count=1)
            ticked += 1
    print(f"DOD_TICKED={ticked}")

    new_task = "".join(tlines)
    if new_task != task_path.read_text():
        task_path.write_text(new_task)
        changed.append(str(task_path.relative_to(root)))

    # --- 3: the roadmap box -------------------------------------------------
    if m.group("box") == "✅":
        print("README_BOX=already")
    else:
        lines[idx] = m.group("head") + "✅" + m.group("mid") + lines[idx][m.end():]
        readme.write_text("".join(lines))
        print("README_BOX=flipped")
        try:
            changed.append(str(readme.relative_to(root)))
        except ValueError:
            changed.append(str(readme))

    print(f"FILES={' '.join(changed) if changed else '-'}")
    print(f"TASK_FILE={task_path.relative_to(root)}")
    print("VERDICT=" + ("MARKED" if changed else "ALREADY_DONE"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
