#!/usr/bin/env python3
"""Classify a walrus PR's statusCheckRollup JSON (stdin) into one CI verdict.

Called by ci_status.sh with the output of
`gh pr view --json statusCheckRollup,headRefOid,headRefName,state,mergeable`.

walrus specifics this encodes:
  * PR CI fires once through `pull_request` (push CI is main-only), so every
    workflow job must appear once on a PR head. The job set is read from ci.yml;
    a missing job is PENDING, never PASS, which closes the registration race
    where the cheapest job finishes first.
  * The `changes` job gates the nine code-heavy jobs, and an `if:`-skipped
    job concludes SKIPPED — which is a PASS. That is what makes a docs-only PR
    (the mark-done PR) go green in ~2 minutes.

Every status/conclusion value is classified exhaustively; an unrecognized value
yields ANOMALY so the loop stops instead of silently merging.

Exit codes: 0 PASS · 1 FAIL · 2 PENDING · 3 NO_CHECKS · 4 ANOMALY
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path

# CheckRun conclusions and StatusContext states.
PASS = {"SUCCESS", "NEUTRAL", "SKIPPED"}
FAIL = {"FAILURE", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED",
        "STARTUP_FAILURE", "ERROR", "STALE"}
PENDING_STATES = {"QUEUED", "IN_PROGRESS", "WAITING", "PENDING",
                  "REQUESTED", "EXPECTED"}

# Jobs with a known, documented flake mode. `e2e` spawns the real sink+loader
# binaries and races the loader's 90s /ready bootstrap timeout; a rerun of the
# failed job is the correct first response, not a code fix. See
# reference/green-gates.md → "Known flakes".
FLAKY_PREFIXES = ("e2e",)
EXPECTED_COPIES_PER_HEAD = 1


def expected_check_names() -> tuple[str, ...]:
    """Read the job display names from the checked-out CI workflow.

    Walrus runs PR CI once through ``pull_request``. Requiring one copy of every
    declared job prevents the first cheap job to finish from making a
    still-registering head look green. Parsing just the top-level job/name shape
    keeps this check independent of PyYAML on the orchestration host.
    """
    root = Path(__file__).resolve().parents[4]
    workflow = root / ".github/workflows/ci.yml"
    names: dict[str, str] = {}
    in_jobs = False
    current: str | None = None
    for line in workflow.read_text().splitlines():
        if line == "jobs:":
            in_jobs = True
            continue
        if not in_jobs:
            continue
        if line and not line.startswith(" "):
            break
        job_match = re.match(r"^  ([a-zA-Z0-9_-]+):\s*(?:#.*)?$", line)
        if job_match:
            current = job_match.group(1)
            names[current] = current
            continue
        name_match = re.match(r"^    name:\s*(.+?)\s*$", line)
        if current and name_match:
            names[current] = name_match.group(1).strip("'\"")
    if not names:
        raise ValueError(f"no CI jobs parsed from {workflow}")
    return tuple(names.values())


def main() -> int:
    d = json.load(sys.stdin)
    print(f"PR_STATE={d.get('state')}")
    print(f"HEAD_SHA={d.get('headRefOid')}")
    print(f"HEAD_REF={d.get('headRefName')}")
    print(f"MERGEABLE={d.get('mergeable')}")

    if d.get("state") != "OPEN":
        print("VERDICT=ANOMALY")
        return 4

    rollup = d.get("statusCheckRollup") or []
    print(f"CHECKS={len(rollup)}")
    if not rollup:
        print("VERDICT=NO_CHECKS")
        return 3

    failing, pending, unknown, skipped = [], [], [], []
    observed_names: list[str] = []
    for c in rollup:
        name = c.get("name") or c.get("context") or "?"
        observed_names.append(name)
        if c.get("__typename") == "StatusContext" or "state" in c:
            val = c.get("state", "")
            if val in PASS:
                continue
            if val in FAIL:
                failing.append(name)
            elif val in PENDING_STATES:
                pending.append(name)
            else:
                unknown.append(f"{name}:{val}")
        else:
            status, concl = c.get("status", ""), c.get("conclusion", "")
            if status != "COMPLETED":
                if status in PENDING_STATES:
                    pending.append(name)
                else:
                    unknown.append(f"{name}:{status}")
            elif concl == "SKIPPED":
                skipped.append(name)
            elif concl in PASS:
                continue
            elif concl in FAIL:
                failing.append(name)
            else:
                unknown.append(f"{name}:{concl}")

    if skipped:
        # Expected on a docs-only PR: the code-gated jobs skip and still report
        # success, so required checks stay satisfied.
        print(f"SKIPPED_JOBS={len(skipped)}")

    try:
        expected = expected_check_names()
    except (OSError, ValueError) as exc:
        print(f"REGISTRATION_ERROR={exc}")
        print("VERDICT=ANOMALY")
        return 4
    counts = Counter(observed_names)
    registration_gaps = [
        (name, counts[name])
        for name in expected
        if counts[name] < EXPECTED_COPIES_PER_HEAD
    ]
    print(f"EXPECTED_CHECKS={len(expected)}")
    print(f"EXPECTED_COPIES_PER_CHECK={EXPECTED_COPIES_PER_HEAD}")
    for name, count in registration_gaps:
        print(f"UNDER_REGISTERED={name}:{count}/{EXPECTED_COPIES_PER_HEAD}")

    if unknown:
        for u in unknown:
            print(f"UNKNOWN={u}")
        print("VERDICT=ANOMALY")
        return 4
    if failing:
        for f in sorted(set(failing)):
            print(f"FAILING={f}")
        flaky = all(f.startswith(FLAKY_PREFIXES) for f in failing)
        print(f"FLAKE_CANDIDATE={'yes' if flaky else 'no'}")
        print("VERDICT=FAIL")
        return 1
    if pending:
        for p in sorted(set(pending)):
            print(f"PENDING_CHECK={p}")
        print("VERDICT=PENDING")
        return 2
    if registration_gaps:
        print("VERDICT=PENDING")
        return 2
    print("VERDICT=PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
