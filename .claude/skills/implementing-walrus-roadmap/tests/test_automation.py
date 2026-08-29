#!/usr/bin/env python3
"""Focused, read-only regressions for the roadmap automation boundary."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SKILL_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = SKILL_ROOT / "scripts/next_task.py"
CLASSIFIER_PATH = SKILL_ROOT / "scripts/classify_checks.py"
CI_STATUS_PATH = SKILL_ROOT / "scripts/ci_status.sh"
SPEC = importlib.util.spec_from_file_location("walrus_next_task", MODULE_PATH)
assert SPEC and SPEC.loader
next_task = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = next_task
SPEC.loader.exec_module(next_task)


def task_text(
    *,
    readiness: str = "audited",
    outcome: str = "change",
    gates: str = "fmt,clippy,test",
    packages: str = "—",
    extra_prose: str = "",
    verification: str = "format = cargo fmt --check",
    files: str = "Cargo.toml",
) -> str:
    return f"""# PR 9.1 — Fixture

> **Status:** 📋 Planned
> **Readiness:** {readiness} · **Outcome:** {outcome}
> **Gates:** {gates} · **Test packages:** {packages}
> **Phase:** 9 — Rust · **Crates touched:** `common` · **Est. size:** S ·
> **Depends on:** PR 8.5 · **Unlocks:** PR 9.2

## Why — learning objectives

Fixture. {extra_prose}

## Read first

- `Cargo.toml`

## Scope

Bounded fixture.

**Baseline precondition:** the named fixture exists before editing.

**Baseline mismatch:** STOP and request task re-authoring before editing.

## Files to create / modify

```text
{files}
```

## Skeleton

Concrete fixture skeleton.

## Verification commands

```text
{verification}
```

## Definition of Done

- [ ] The fixture is proven.

## What completed looks like

Fixture passes.

## References

- Fixture reference.
"""


class MetadataTests(unittest.TestCase):
    def test_explicit_integration_ignores_negative_gate_prose(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            text = task_text(
                gates="fmt,clippy,test,integration",
                extra_prose="SQLx, e2e, Compose, and images are explicitly deferred.",
            )
            metadata, errors = next_task.parse_explicit_metadata(text, root)
        self.assertEqual([], errors)
        self.assertEqual(["fmt", "clippy", "test", "integration"], metadata["gates"])
        self.assertEqual([], metadata["test_packages"])
        self.assertNotIn("--pkgs", metadata["gate_command"])

    def test_populated_packages_are_rendered_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            metadata, errors = next_task.parse_explicit_metadata(
                task_text(packages="loader,pg-sink"), Path(directory)
            )
        self.assertEqual([], errors)
        self.assertEqual(["loader", "pg-sink"], metadata["test_packages"])
        self.assertTrue(metadata["gate_command"].endswith(" --pkgs loader,pg-sink"))

    def test_draft_and_unknown_gate_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, errors = next_task.parse_explicit_metadata(
                task_text(readiness="draft", gates="fmt,clippy,test,typo"), Path(directory)
            )
        joined = "\n".join(errors)
        self.assertIn("Readiness must be `audited`", joined)
        self.assertIn("unsupported gate(s): typo", joined)

    def test_declared_future_check_script_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            metadata, errors = next_task.parse_explicit_metadata(
                task_text(
                    files="scripts/check-naming.sh",
                    verification="naming = bash scripts/check-naming.sh --check",
                ),
                Path(directory),
            )
        self.assertEqual([], errors)
        self.assertEqual("naming", metadata["verification_commands"][0]["label"])


class CommandSafetyTests(unittest.TestCase):
    def test_read_only_allowlist(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            allowed = (
                "cargo test --workspace",
                "cargo fmt --check",
                "git diff --check origin/main...HEAD",
                "git show origin/main:Cargo.toml",
                "rg -n unwrap crates",
                "! rg -n placeholder docs",
                "find crates -name '*.rs'",
                "test -f Cargo.toml",
            )
            for command in allowed:
                self.assertIsNone(
                    next_task.validate_verification_command(command, root), command
                )

    def test_mutating_or_shell_composed_commands_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rejected = (
                "cargo add serde",
                "cargo fmt",
                "git reset --hard",
                "git diff --check",
                "rg foo > result.txt",
                "test -f Cargo.toml && git show HEAD:Cargo.toml",
                "git show $(git merge-base main HEAD):Cargo.toml",
            )
            for command in rejected:
                self.assertIsNotNone(
                    next_task.validate_verification_command(command, root), command
                )


class StructureAndDependencyTests(unittest.TestCase):
    def test_legacy_first_pr_dependency_annotation_is_valid(self) -> None:
        dependencies, error = next_task.parse_dependencies("— (first PR)", ["0.1"])
        self.assertEqual([], dependencies)
        self.assertIsNone(error)

    def test_malformed_dependency_is_rejected(self) -> None:
        _, error = next_task.parse_dependencies("PR banana", ["8.5", "9.1"])
        self.assertIsNotNone(error)

    def test_partial_mark_done_errors_are_drift_only(self) -> None:
        result = next_task.ValidationResult(
            errors=[
                "state drift for PR 9.1: roadmap=unchecked but marker=done",
                "docs/implementation/task.md: planned Rust task has checked DoD items",
            ],
            drifts=[{"id": "9.1"}],
        )
        self.assertTrue(next_task.errors_are_drift_only(result))

    def test_placeholders_and_generic_scaffold_are_rejected(self) -> None:
        text = task_text(extra_prose="Use <file:line> at the named expression and ``.")
        text = text.replace(
            "Concrete fixture skeleton.",
            "\n".join(
                (
                    "1. Capture the complete baseline command and result.",
                    "2. Restrict the diff to the allowed files.",
                    "3. Apply exactly this operation.",
                    "4. Prove this postcondition.",
                    "5. Run every labeled verification command.",
                )
            ),
        )
        errors = "\n".join(next_task.validate_rust_task_structure(text))
        self.assertIn("<file:line>", errors)
        self.assertIn("the named expression", errors)
        self.assertIn("empty inline-code", errors)
        self.assertIn("generic audit 1..5", errors)

    def test_fenced_markers_do_not_damage_prose_validation(self) -> None:
        text = task_text().replace(
            "Concrete fixture skeleton.", "```text\n<file:line> and the named expression and ``\n```"
        )
        errors = "\n".join(next_task.validate_rust_task_structure(text))
        self.assertNotIn("damaged prose marker", errors)
        self.assertNotIn("empty inline-code", errors)

    def test_mutating_negative_controls_are_rejected(self) -> None:
        instructions = (
            "$ git checkout -- Cargo.toml",
            "$ git restore Cargo.toml",
            "$ git reset Cargo.toml",
            "$ sed -i 's/old/new/' Cargo.toml",
            "$ cargo add serde",
            "$ printf 'fixture' >> Cargo.toml",
        )
        for instruction in instructions:
            with self.subTest(instruction=instruction):
                text = task_text().replace(
                    "Concrete fixture skeleton.",
                    "Run this negative control and then clean up:\n\n" + instruction,
                )
                errors = "\n".join(next_task.validate_rust_task_structure(text))
                self.assertIn("temporary fixture or explicit self-test mode", errors)

    def test_missing_fail_closed_baseline_contract_is_rejected(self) -> None:
        text = task_text()
        text = text.replace(
            "**Baseline precondition:** the named fixture exists before editing.\n\n",
            "",
        ).replace(
            "**Baseline mismatch:** STOP and request task re-authoring before editing.\n\n",
            "",
        )
        errors = "\n".join(next_task.validate_rust_task_structure(text))
        self.assertIn("explicit baseline precondition", errors)
        self.assertIn("explicit baseline-mismatch contract", errors)

    def test_post_commit_diff_proof_requires_merge_base_range(self) -> None:
        text = task_text().replace(
            "The fixture is proven.",
            "The fixture is proven and `git diff --stat` names only allowed files.",
        )
        errors = "\n".join(next_task.validate_rust_task_structure(text))
        self.assertIn("git diff proof must use the merge-base range", errors)

    def test_dynamic_predecessor_fallback_is_rejected(self) -> None:
        text = task_text().replace(
            "Bounded fixture.",
            "Bounded fixture. Follow an alternate branch as a predecessor fallback.",
        )
        errors = "\n".join(next_task.validate_rust_task_structure(text))
        self.assertIn("dynamic predecessor fallback", errors)


class CorpusCoverageTests(unittest.TestCase):
    def test_workspace_packages_are_read_without_cargo(self) -> None:
        repository = SKILL_ROOT.parents[2]
        packages, error = next_task._workspace_packages(repository)
        self.assertIsNone(error)
        self.assertEqual(
            {"common", "control", "e2e", "loader", "pg-sink", "pg-to-arrow"},
            packages,
        )

    def test_rust_skill_links_are_a_rule_file_bijection(self) -> None:
        repository = SKILL_ROOT.parents[2]
        rule_names = {
            path.name for path in (repository / ".claude/skills/rust-skills/rules").glob("*.md")
        }
        links = re.findall(
            r"\]\(rules/([a-z0-9][a-z0-9-]*\.md)(?:#[^)]+)?\)",
            (repository / ".claude/skills/rust-skills/SKILL.md").read_text(),
        )
        self.assertEqual(265, len(links))
        self.assertEqual(265, len(set(links)))
        self.assertEqual(rule_names, set(links))

    def test_fixed_phase_counts_and_contiguous_numbers(self) -> None:
        repository = SKILL_ROOT.parents[2]
        self.assertEqual(265, sum(spec[2] for spec in next_task.RUST_PHASE_SPECS.values()))
        for phase, (directory, prefix, expected_count) in next_task.RUST_PHASE_SPECS.items():
            paths = list((repository / "docs/implementation" / directory).glob("pr-*.md"))
            numbers = []
            for path in paths:
                match = next_task.TASK_FILENAME_RE.match(path.name)
                self.assertIsNotNone(match, path)
                assert match is not None
                self.assertEqual(phase, next_task.phase_of(match.group("id")), path)
                self.assertTrue(match.group("rule").startswith(prefix + "-"), path)
                numbers.append(next_task.id_key(match.group("id"))[1])
            self.assertEqual(expected_count, len(paths), directory)
            self.assertEqual(list(range(1, expected_count + 1)), sorted(numbers), directory)


class GateRunnerTests(unittest.TestCase):
    def run_gate(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(SKILL_ROOT / "scripts/run_gate.sh"), *arguments],
            cwd=SKILL_ROOT.parents[2],
            capture_output=True,
            text=True,
        )

    def test_bare_packages_is_controlled_anomaly(self) -> None:
        result = self.run_gate("fmt", "--pkgs")
        self.assertEqual(2, result.returncode)
        self.assertIn("GATE=FAIL", result.stdout)
        self.assertIn("ANOMALY=--pkgs requires", result.stdout)

    def test_unknown_gate_is_failure_not_skip(self) -> None:
        result = self.run_gate("not-a-gate")
        self.assertEqual(2, result.returncode)
        self.assertIn("CHECK:not-a-gate=FAIL", result.stdout)
        self.assertNotIn("SKIP", result.stdout)

    def test_digit_in_supported_e2e_gate_passes_list_validation(self) -> None:
        result = self.run_gate("e2e,not-a-gate")
        self.assertEqual(2, result.returncode)
        self.assertIn("CHECK:not-a-gate=FAIL", result.stdout)
        self.assertNotIn("invalid comma-separated gate list", result.stdout)

    def test_sqlx_gate_migrates_before_prepare_check(self) -> None:
        script = (SKILL_ROOT / "scripts/run_gate.sh").read_text()
        sqlx_case = script[script.index("    sqlx)") : script.index("    conformance)")]
        self.assertLess(
            sqlx_case.index("sqlx migrate run --source migrations/control"),
            sqlx_case.index("run_check sqlx-prepare cargo sqlx prepare --check --workspace"),
        )

    def test_ignored_integration_gate_runs_only_discovered_test_targets(self) -> None:
        script = (SKILL_ROOT / "scripts/run_gate.sh").read_text()
        helper = script[
            script.index("run_ignored_integration_tests()") : script.index("docker_up()")
        ]
        integration_case = script[
            script.index("    integration)") : script.index("    e2e)")
        ]
        self.assertIn("rg -l '#\\[ignore'", helper)
        self.assertIn('cargo test -p "$package" --test "$target"', helper)
        self.assertIn(
            "run_check integration-ignored run_ignored_integration_tests",
            integration_case,
        )
        self.assertNotIn("cargo test --workspace -- --ignored", integration_case)

    def test_control_integration_gate_serializes_the_shared_database(self) -> None:
        script = (SKILL_ROOT / "scripts/run_gate.sh").read_text()
        integration_case = script[
            script.index("    integration)") : script.index("    e2e)")
        ]
        self.assertIn(
            "cargo test -p control --features integration -- --test-threads=1",
            integration_case,
        )


class CheckClassifierTests(unittest.TestCase):
    def run_classifier(self, checks: list[dict]) -> subprocess.CompletedProcess[str]:
        payload = {
            "state": "OPEN",
            "headRefOid": "a" * 40,
            "headRefName": "pr-9.1-fixture",
            "mergeable": "MERGEABLE",
            "statusCheckRollup": checks,
        }
        return subprocess.run(
            [sys.executable, str(CLASSIFIER_PATH)],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
        )

    @staticmethod
    def completed(name: str, conclusion: str = "SUCCESS") -> dict:
        return {
            "__typename": "CheckRun",
            "name": name,
            "status": "COMPLETED",
            "conclusion": conclusion,
        }

    def expected_names(self) -> tuple[str, ...]:
        spec = importlib.util.spec_from_file_location(
            "walrus_classify_checks", CLASSIFIER_PATH
        )
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.expected_check_names()

    def test_one_registered_workflow_copy_is_pending(self) -> None:
        names = self.expected_names()
        self.assertEqual(11, len(names))
        self.assertIn("implementation roadmap contract", names)
        result = self.run_classifier([self.completed(name) for name in names])
        self.assertEqual(2, result.returncode)
        self.assertIn("UNDER_REGISTERED=", result.stdout)
        self.assertIn("VERDICT=PENDING", result.stdout)

    def test_two_complete_workflow_copies_pass(self) -> None:
        names = self.expected_names()
        checks = [self.completed(name) for name in names for _ in range(2)]
        result = self.run_classifier(checks)
        self.assertEqual(0, result.returncode)
        self.assertIn("EXPECTED_COPIES_PER_CHECK=2", result.stdout)
        self.assertIn("VERDICT=PASS", result.stdout)

    def test_cheap_roadmap_check_alone_cannot_false_green(self) -> None:
        result = self.run_classifier(
            [self.completed("implementation roadmap contract")]
        )
        self.assertEqual(2, result.returncode)
        self.assertIn("VERDICT=PENDING", result.stdout)

    def test_terminal_failure_wins_over_incomplete_registration(self) -> None:
        result = self.run_classifier(
            [self.completed("implementation roadmap contract", "TIMED_OUT")]
        )
        self.assertEqual(1, result.returncode)
        self.assertIn("VERDICT=FAIL", result.stdout)


class CiStatusTests(unittest.TestCase):
    @staticmethod
    def failed_check() -> dict:
        return {
            "__typename": "CheckRun",
            "name": "implementation roadmap contract",
            "status": "COMPLETED",
            "conclusion": "FAILURE",
        }

    def run_ci_status(
        self, run_lines: str
    ) -> subprocess.CompletedProcess[str]:
        payload = {
            "state": "OPEN",
            "headRefOid": "a" * 40,
            "headRefName": "pr-9.1-fixture",
            "mergeable": "MERGEABLE",
            "statusCheckRollup": [self.failed_check()],
        }
        environment = os.environ.copy()
        environment["WALRUS_TEST_PR_JSON"] = json.dumps(payload)
        environment["WALRUS_TEST_RUN_LINES"] = run_lines
        fake_gh = r'''
gh() {
  case "$1 $2" in
    "pr view") printf '%s\n' "$WALRUS_TEST_PR_JSON" ;;
    "run list") printf '%s\n' "$WALRUS_TEST_RUN_LINES" ;;
    *) return 64 ;;
  esac
}
export -f gh
exec "$1" 999
'''
        return subprocess.run(
            ["bash", "-c", fake_gh, "walrus-ci-status-test", str(CI_STATUS_PATH)],
            cwd=SKILL_ROOT.parents[2],
            env=environment,
            capture_output=True,
            text=True,
        )

    def test_failed_check_waits_for_nonterminal_workflow_run(self) -> None:
        result = self.run_ci_status(
            "RUN_ID=101 EVENT=pull_request STATUS=in_progress CONCLUSION=-"
        )
        self.assertEqual(2, result.returncode)
        self.assertIn("WAITING_RUN_ID=101", result.stdout)
        self.assertIn("VERDICT=PENDING", result.stdout)
        self.assertNotIn("VERDICT=FAIL", result.stdout)

    def test_terminal_failure_returns_fail_with_run_id(self) -> None:
        result = self.run_ci_status(
            "RUN_ID=202 EVENT=push STATUS=completed CONCLUSION=timed_out"
        )
        self.assertEqual(1, result.returncode)
        self.assertIn("RUN_ID=202", result.stdout)
        self.assertIn("CONCLUSION=timed_out", result.stdout)
        self.assertIn("VERDICT=FAIL", result.stdout)


if __name__ == "__main__":
    unittest.main()
