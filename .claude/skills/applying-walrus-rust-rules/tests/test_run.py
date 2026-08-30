#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module():
    path = ROOT / "scripts/run.py"
    spec = importlib.util.spec_from_file_location("walrus_rust_rule_loop_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


loop = load_module()


def rules(*slugs: str):
    return [loop.Rule(slug, Path(f"rules/{slug}.md"), slug * 4) for slug in slugs]


def record(slug: str, result: str, pass_name: str, completed: bool = True):
    return loop.Record(slug, result, pass_name, slug, completed)


class SelectionTests(unittest.TestCase):
    def test_initial_pass_continues_past_failure(self) -> None:
        items = rules("one", "two", "three")
        state = loop.progress(
            [
                record("one", "applied", "initial"),
                record("two", "failed", "initial", completed=False),
            ]
        )
        self.assertEqual(loop.next_rule(items, state), (items[2], "initial"))

    def test_cleanup_retries_only_failed_rules(self) -> None:
        items = rules("one", "two")
        state = loop.progress(
            [
                record("one", "applied", "initial"),
                record("two", "failed", "initial", completed=False),
            ]
        )
        self.assertEqual(loop.next_rule(items, state), (items[1], "cleanup-1"))

    def test_cleanup_advances_pass_after_each_failed_rule_was_tried(self) -> None:
        items = rules("one", "two")
        state = loop.progress(
            [
                record("one", "failed", "initial", completed=False),
                record("two", "failed", "initial", completed=False),
                record("one", "failed", "cleanup-1", completed=False),
                record("two", "failed", "cleanup-1", completed=False),
            ]
        )
        self.assertEqual(loop.next_rule(items, state), (items[0], "cleanup-2"))

    def test_successful_cleanup_removes_rule_from_failure_queue(self) -> None:
        items = rules("one", "two")
        state = loop.progress(
            [
                record("one", "failed", "initial", completed=False),
                record("two", "no-change", "initial"),
                record("one", "applied", "cleanup-1"),
            ]
        )
        self.assertIsNone(loop.next_rule(items, state))

    def test_stops_after_three_failed_cleanup_passes(self) -> None:
        items = rules("one")
        records = [record("one", "failed", "initial", completed=False)]
        records.extend(
            record("one", "failed", f"cleanup-{number}", completed=False)
            for number in range(1, 4)
        )
        with self.assertRaises(loop.LoopError):
            loop.next_rule(items, loop.progress(records))


class ManifestTests(unittest.TestCase):
    def test_repository_manifest_has_exactly_265_rules(self) -> None:
        repo = ROOT.parents[2]
        items = loop.discover_rules(repo)
        self.assertEqual(len(items), 265)
        self.assertEqual(len({item.slug for item in items}), 265)
        self.assertEqual(items[0].slug, "own-borrow-over-clone")
        self.assertEqual(items[-1].slug, "anti-stringly-typed")

    def test_manifest_digest_changes_with_content_or_order(self) -> None:
        first = rules("one", "two")
        second = rules("two", "one")
        self.assertNotEqual(loop.manifest_digest(first), loop.manifest_digest(second))


class SafetyTests(unittest.TestCase):
    def test_default_agent_timeout_allows_opus_source_synthesis(self) -> None:
        self.assertEqual(loop.parse_args([]).agent_timeout_seconds, 1800)

    def test_protects_rule_sources_runner_and_roadmap_completion_files(self) -> None:
        protected = [
            Path(".claude/skills/rust-skills/SKILL.md"),
            Path(".claude/skills/rust-skills/rules/own-copy-small.md"),
            Path(".claude/skills/applying-walrus-rust-rules/scripts/run.py"),
            Path("docs/implementation/README.md"),
            Path("docs/implementation/phase-9-rust-ownership/pr-9.1-own-borrow-over-clone.md"),
        ]
        self.assertTrue(all(loop.is_protected(path) for path in protected))
        self.assertFalse(loop.is_protected(Path("crates/common/src/lib.rs")))
        self.assertFalse(loop.is_protected(Path("docs/architecture.md")))

    def test_parses_structured_claude_wrapper(self) -> None:
        payload = {
            "structured_output": {
                "status": "no_change",
                "summary": "already satisfied",
                "validation": [],
                "blocked_on": "",
            }
        }
        self.assertEqual(loop.parse_claude_result(loop.json.dumps(payload))["status"], "no_change")

    def test_detects_non_retryable_claude_infrastructure_failure(self) -> None:
        process = subprocess.CompletedProcess(
            ["claude"], 1, stdout="", stderr="Authentication failed: not logged in"
        )
        self.assertEqual(loop.infrastructure_failure(process), "not logged in")

    def test_claude_prompt_is_supplied_via_stdin_not_variadic_tool_args(self) -> None:
        options = loop.argparse.Namespace(model=None, max_budget_usd=None)
        command = loop.claude_command(options)
        self.assertNotIn("--", command)
        self.assertEqual(command[-1], "Read,Grep,Glob,Edit,Write")
        self.assertNotIn("Bash", command[-1])

    def test_repair_agents_receive_bash_with_mutation_denies(self) -> None:
        options = loop.argparse.Namespace(model=None, max_budget_usd=None)
        command = loop.claude_command(options, allow_bash=True)
        self.assertIn("Read,Grep,Glob,Edit,Write,Bash", command)
        self.assertIn("--disallowedTools", command)

    def test_archive_restore_keeps_untracked_recovery_copy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            loop.run(["git", "init", "-q"], cwd=root)
            loop.run(["git", "config", "user.email", "test@example.com"], cwd=root)
            loop.run(["git", "config", "user.name", "Test"], cwd=root)
            tracked = root / "tracked.txt"
            tracked.write_text("before\n")
            loop.run(["git", "add", "tracked.txt"], cwd=root)
            loop.run(["git", "commit", "-q", "-m", "base"], cwd=root)
            tracked.write_text("after\n")
            untracked = root / "new" / "file.txt"
            untracked.parent.mkdir()
            untracked.write_text("recover me\n")
            archive = root / ".git" / "archive"
            loop.archive_and_restore(
                root, archive, [Path("tracked.txt"), Path("new/file.txt")]
            )
            self.assertEqual(tracked.read_text(), "before\n")
            self.assertFalse(untracked.exists())
            self.assertEqual(
                (archive / "untracked" / "new" / "file.txt").read_text(),
                "recover me\n",
            )


if __name__ == "__main__":
    unittest.main()
