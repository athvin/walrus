#!/usr/bin/env python3
"""Route-level regressions for preflight's resumable branch state machine."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


SKILL_ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT = SKILL_ROOT / "scripts/preflight.sh"


class PreflightRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.scripts = self.root / "scripts"
        self.bin = self.root / "bin"
        self.scripts.mkdir()
        self.bin.mkdir()
        shutil.copy2(PREFLIGHT, self.scripts / "preflight.sh")

        (self.scripts / "next_task.py").write_text(
            textwrap.dedent(
                """\
                import os
                import sys

                validation = os.environ.get("FAKE_VALIDATION", "PASS")
                if "--validate-all" in sys.argv:
                    print(f"VALIDATION={validation}")
                    if validation == "DRIFT":
                        print("ERROR=state drift for PR 9.1: roadmap=unchecked but marker=done")
                    print("RUST_ROWS=265")
                    print("UNTRACKED_ROADMAP_FILES=0")
                    raise SystemExit(5 if validation == "DRIFT" else 0)
                if "--task" in sys.argv:
                    print("VERDICT=TASK")
                    print("BOX=" + os.environ.get("FAKE_BOX", "unchecked"))
                    print("MARKER=" + os.environ.get("FAKE_MARKER", "planned"))
                    print("{")
                    print('  "branch": "' + os.environ.get("FAKE_CODE_BRANCH", "pr-9.1-fixture") + '",')
                    print("}")
                    raise SystemExit(0)
                selector = os.environ.get("FAKE_SELECTOR", "drift" if validation == "DRIFT" else "selected")
                if selector == "all-done":
                    print("VERDICT=ALL_DONE")
                    raise SystemExit(2)
                if selector == "drift":
                    print("VERDICT=DRIFT")
                    print("TASK=" + os.environ.get("FAKE_DRIFT_TASK", "9.1"))
                    raise SystemExit(5)
                print("VERDICT=SELECTED")
                raise SystemExit(0)
                """
            )
        )
        self._write_executable(
            "git",
            r"""
            #!/usr/bin/env bash
            command=$1
            shift
            case "$command" in
              fetch|switch|merge|merge-base) exit 0 ;;
              rev-parse)
                if [ "${1:-}" = "--abbrev-ref" ]; then
                  printf 'main\n'
                else
                  printf 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'
                fi ;;
              status) exit 0 ;;
              show-ref)
                ref=${!#}
                case "$ref" in
                  refs/heads/*)
                    branch=${ref#refs/heads/}
                    refs=",${FAKE_LOCAL_REFS:-}," ;;
                  refs/remotes/origin/*)
                    branch=${ref#refs/remotes/origin/}
                    refs=",${FAKE_REMOTE_REFS:-}," ;;
                  *) exit 1 ;;
                esac
                case "$refs" in *",$branch,"*) exit 0 ;; *) exit 1 ;; esac ;;
              rev-list)
                printf '%s %s\n' "${FAKE_BEHIND:-0}" "${FAKE_AHEAD:-0}" ;;
              *) exit 0 ;;
            esac
            """,
        )
        self._write_executable(
            "gh",
            r"""
            #!/usr/bin/env bash
            if [ "$1 $2" = "auth status" ]; then exit 0; fi
            if [ "$1 $2" = "run list" ]; then
              case " $* " in
                *" --event push "*) ;;
                *) exit 65 ;;
              esac
              printf '%s\n' "${FAKE_MAIN_RUN:-}"
              exit 0
            fi
            if [ "$1 $2" != "pr list" ]; then exit 64; fi
            shift 2
            head_ref=
            while [ $# -gt 0 ]; do
              if [ "$1" = "--head" ]; then head_ref=$2; break; fi
              shift
            done
            if [ "$head_ref" = "${FAKE_CODE_BRANCH:-}" ]; then
              printf '%s\n' "${FAKE_CODE_PR:-}"
            elif [ "$head_ref" = "${FAKE_MD_BRANCH:-}" ]; then
              printf '%s\n' "${FAKE_MD_PR:-}"
            elif [ "$head_ref" = "${FAKE_RECONCILE_BRANCH:-}" ]; then
              printf '%s\n' "${FAKE_RECONCILE_PR:-}"
            fi
            """,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_executable(self, name: str, source: str) -> None:
        path = self.bin / name
        path.write_text(textwrap.dedent(source).lstrip())
        path.chmod(0o755)

    def run_preflight(
        self, *arguments: str, **overrides: str
    ) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.update(
            {
                "PATH": str(self.bin) + os.pathsep + environment["PATH"],
                "FAKE_VALIDATION": "PASS",
                "FAKE_CODE_BRANCH": "pr-9.1-fixture",
                "FAKE_MD_BRANCH": "pr-9.1-mark-done",
                "FAKE_RECONCILE_BRANCH": "chore-reconcile-roadmap-9.1",
            }
        )
        environment.update(overrides)
        return subprocess.run(
            [str(self.scripts / "preflight.sh"), *arguments],
            cwd=self.root,
            env=environment,
            capture_output=True,
            text=True,
        )

    def test_open_code_pr_pushes_local_ahead_commit_before_poll(self) -> None:
        result = self.run_preflight(
            "pr-9.1-fixture",
            "9.1",
            FAKE_CODE_PR="101:OPEN",
            FAKE_LOCAL_REFS="pr-9.1-fixture",
            FAKE_REMOTE_REFS="pr-9.1-fixture",
            FAKE_AHEAD="1",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("PUSH_NEEDED=yes", result.stdout)
        self.assertIn("ROUTE=PUSH_CI", result.stdout)
        self.assertNotIn("ROUTE=POLL_CI", result.stdout)

    def test_open_mark_done_pr_pushes_local_ahead_commit_before_poll(self) -> None:
        result = self.run_preflight(
            "pr-9.1-fixture",
            "9.1",
            FAKE_CODE_PR="101:MERGED",
            FAKE_MD_PR="102:OPEN",
            FAKE_LOCAL_REFS="pr-9.1-mark-done",
            FAKE_REMOTE_REFS="pr-9.1-mark-done",
            FAKE_AHEAD="1",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("ROUTE=PUSH_MARK_DONE", result.stdout)
        self.assertNotIn("ROUTE=POLL_MARK_DONE", result.stdout)

    def test_local_only_code_branch_reports_set_upstream(self) -> None:
        result = self.run_preflight(
            "pr-9.1-fixture",
            "9.1",
            FAKE_LOCAL_REFS="pr-9.1-fixture",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("PUSH_SET_UPSTREAM=yes", result.stdout)
        self.assertIn("ROUTE=CONTINUE_IMPL", result.stdout)

    def test_open_reconcile_pr_pushes_local_ahead_commit_before_poll(self) -> None:
        result = self.run_preflight(
            "--reconcile",
            "9.1",
            FAKE_VALIDATION="DRIFT",
            FAKE_RECONCILE_PR="103:OPEN",
            FAKE_LOCAL_REFS="chore-reconcile-roadmap-9.1",
            FAKE_REMOTE_REFS="chore-reconcile-roadmap-9.1",
            FAKE_AHEAD="1",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("ROUTE=PUSH_RECONCILE", result.stdout)
        self.assertNotIn("ROUTE=POLL_RECONCILE", result.stdout)

    def test_local_only_reconcile_branch_resumes_with_set_upstream(self) -> None:
        result = self.run_preflight(
            "--reconcile",
            "9.1",
            FAKE_VALIDATION="DRIFT",
            FAKE_LOCAL_REFS="chore-reconcile-roadmap-9.1",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("PUSH_SET_UPSTREAM=yes", result.stdout)
        self.assertIn("ROUTE=CONTINUE_RECONCILE", result.stdout)

    def test_closed_reconcile_pr_is_a_veto(self) -> None:
        result = self.run_preflight(
            "--reconcile",
            "9.1",
            FAKE_VALIDATION="DRIFT",
            FAKE_RECONCILE_PR="103:CLOSED",
        )
        self.assertEqual(1, result.returncode)
        self.assertIn("ROUTE=STOP_AMBIGUOUS", result.stdout)
        self.assertIn("human veto", result.stdout)

    def test_merged_final_reconcile_accepts_all_done_selector(self) -> None:
        result = self.run_preflight(
            "--reconcile",
            "9.1",
            FAKE_VALIDATION="DRIFT",
            FAKE_RECONCILE_PR="103:MERGED",
            FAKE_SELECTOR="all-done",
            FAKE_BOX="checked",
            FAKE_MARKER="done",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("ROADMAP=ALL_DONE", result.stdout)
        self.assertIn("ROUTE=RECONCILE_DONE", result.stdout)

    def test_merged_reconcile_must_leave_target_explicitly_done(self) -> None:
        result = self.run_preflight(
            "--reconcile",
            "9.1",
            FAKE_VALIDATION="DRIFT",
            FAKE_RECONCILE_PR="103:MERGED",
            FAKE_SELECTOR="selected",
            FAKE_BOX="unchecked",
            FAKE_MARKER="planned",
        )
        self.assertEqual(1, result.returncode)
        self.assertIn("ROUTE=STOP_AMBIGUOUS", result.stdout)
        self.assertIn("left task 9.1 BOX=unchecked MARKER=planned", result.stdout)

    def test_merged_reconcile_rejects_any_remaining_drift(self) -> None:
        result = self.run_preflight(
            "--reconcile",
            "9.1",
            FAKE_VALIDATION="DRIFT",
            FAKE_RECONCILE_PR="103:MERGED",
            FAKE_SELECTOR="drift",
            FAKE_DRIFT_TASK="9.2",
        )
        self.assertEqual(1, result.returncode)
        self.assertIn("ROUTE=STOP_AMBIGUOUS", result.stdout)
        self.assertIn("left roadmap drift at task 9.2", result.stdout)

    def test_consistent_unset_state_is_recoverable_with_two_merge_proofs(self) -> None:
        result = self.run_preflight(
            "--reconcile",
            "9.1",
            FAKE_CODE_PR="101:MERGED",
            FAKE_MD_PR="102:MERGED",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("CONSISTENT_UNSET_PROOF=yes", result.stdout)
        self.assertIn("MERGED_CODE_PR=101", result.stdout)
        self.assertIn("MERGED_MARK_DONE_PR=102", result.stdout)
        self.assertIn("ROUTE=FRESH_RECONCILE", result.stdout)

    def test_consistent_unset_state_rejects_unmerged_code_pr(self) -> None:
        result = self.run_preflight(
            "--reconcile",
            "9.1",
            FAKE_CODE_PR="101:OPEN",
            FAKE_MD_PR="102:MERGED",
        )
        self.assertEqual(1, result.returncode)
        self.assertIn("ROUTE=STOP_AMBIGUOUS", result.stdout)
        self.assertIn("exactly one merged code PR", result.stdout)

    def test_repo_mode_requires_exact_main_success(self) -> None:
        result = self.run_preflight(
            "--wait-main",
            "0",
            FAKE_MAIN_RUN="completed:success",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("POST_SYNC_VALIDATION=PASS", result.stdout)
        self.assertIn("MAIN_CI=green", result.stdout)
        self.assertIn("PREFLIGHT=PASS", result.stdout)

    def test_repo_mode_does_not_accept_running_main_at_wait_cap(self) -> None:
        result = self.run_preflight(
            "--wait-main",
            "0",
            FAKE_MAIN_RUN="in_progress:",
        )
        self.assertEqual(1, result.returncode)
        self.assertIn("MAIN_CI=running", result.stdout)
        self.assertIn("PREFLIGHT=FAIL", result.stdout)

    def test_repo_mode_can_route_verified_drift_after_green_main(self) -> None:
        result = self.run_preflight(
            "--wait-main",
            "0",
            FAKE_VALIDATION="DRIFT",
            FAKE_MAIN_RUN="completed:success",
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn("POST_SYNC_VALIDATION=DRIFT", result.stdout)
        self.assertIn("MAIN_CI=green", result.stdout)
        self.assertIn("PREFLIGHT=DRIFT", result.stdout)


if __name__ == "__main__":
    unittest.main()
