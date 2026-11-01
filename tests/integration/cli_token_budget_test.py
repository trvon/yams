#!/usr/bin/env python3
"""Isolated CLI acceptance lane: python3 this_file.py --binary build/debug/tools/yams-cli/yams-cli.

Uses one temp corpus for the lane, never the operator's daemon or data directory.
The rename and shared-worktree cases deliberately exercise path aliases of one CAS hash.
"""
import argparse
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

BINARY = None


class CliTokenBudgetAcceptance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="yams-cli-budget-")
        cls.root = Path(cls.temp.name)
        cls.repo = cls.root / "repo"
        cls.repo.mkdir()
        cls.data = cls.root / "data"
        cls.config = cls.root / "config.toml"
        cls.config.write_text(
            "[version]\nconfig_version = 3\n[core]\ndata_dir = " + json.dumps(str(cls.data)) +
            "\n[search]\nexclude_paths = []\n", encoding="utf-8")
        cls.env = os.environ.copy()
        for name in ("YAMS_CONFIG_PATH", "YAMS_IN_DAEMON", "YAMS_DAEMON_SOCKET", "YAMS_DAEMON_SOCKET_PATH"):
            cls.env.pop(name, None)
        cls.env.update({
            "YAMS_CONFIG": str(cls.config), "YAMS_DATA_DIR": str(cls.data),
            "YAMS_STORAGE": str(cls.data), "YAMS_EMBEDDED": "1",
            "YAMS_DISABLE_VECTORS": "1", "YAMS_SKIP_MODEL_LOADING": "1",
            "YAMS_DISABLE_SESSION_WATCHER": "1", "YAMS_CLI_DISABLE_DAEMON_AUTOSTART": "1",
        })
        cls.git("init", "-q")
        cls.files = []
        for index in range(12):
            path = cls.repo / f"handoff-{index:02}.txt"
            path.write_text("".join(f"file {index} line {line} budgetneedle\n" for line in range(1, 81)), encoding="utf-8")
            cls.files.append(path)
        cls.git("add", "--", ".")
        result = cls.cli("add", *(str(path) for path in cls.files), "--sync", "--no-embeddings",
                         "--no-session", "-m", "source=code,task=cli-token-budget")
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors="replace"))
        cls.sizes = {}

    @classmethod
    def tearDownClass(cls):
        print("\nMeasured acceptance stdout sizes:", json.dumps(cls.sizes, sort_keys=True))
        cls.temp.cleanup()

    @classmethod
    def git(cls, *args):
        return subprocess.run(["git", "-C", str(cls.repo), *args], env=cls.env,
                              capture_output=True, check=True, timeout=30)

    @classmethod
    def cli(cls, *args, cwd=None):
        return subprocess.run([str(BINARY), "--data-dir", str(cls.data), *args], env=cls.env,
                              cwd=cwd or cls.repo, capture_output=True, timeout=120)

    def ok(self, *args, cwd=None):
        result = self.cli(*args, cwd=cwd)
        self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
        return result.stdout

    def test_result_caps(self):
        recent = self.ok("list", "--recent", "3")
        self.assertLessEqual(len(recent.splitlines()), 4)
        paths = self.ok("grep", "-F", "budgetneedle", "--paths-only", "--max-results", "10", "--no-live")
        self.assertLessEqual(len(paths.splitlines()), 10)
        self.assertEqual(len(paths.splitlines()), len(set(paths.splitlines())))
        self.sizes.update(recent_3=len(recent), grep_paths_10=len(paths))

    def test_read_slices_and_byte_caps(self):
        target = str(self.files[3])
        for command in ("get", "cat"):
            selector = ("--name", target) if command == "get" else (target,)
            output = self.ok(command, *selector, "--lines", "10:30")
            rows = output.decode().splitlines()
            self.assertEqual(len(rows), 21)
            self.assertTrue(rows[0].startswith("10: "))
            self.assertTrue(rows[-1].startswith("30: "))
            self.assertLessEqual(len(self.ok(command, *selector, "--max-bytes", "19")), 19)
            around = self.ok(command, *selector, "--around", "line 20 ", "--context", "2")
            self.assertEqual(len(around.splitlines()), 5)
            self.sizes[f"{command}_lines_10_30"] = len(output)

    def test_budgets_on_all_six_reads(self):
        commands = {
            "search": ("search", "budgetneedle", "--type", "keyword", "--limit", "10"),
            "grep": ("grep", "-F", "budgetneedle", "--no-live"),
            "list": ("list", "--limit", "10"),
            "get": ("get", "--name", str(self.files[3])),
            "cat": ("cat", str(self.files[3])),
            "graph": ("graph", "--list-types"),
        }
        for name, args in commands.items():
            with self.subTest(command=name):
                full = self.ok(*args)
                bounded = self.ok(*args, "--budget-chars", "35")
                self.assertLessEqual(len(bounded), 235)
                if len(full) > 35:
                    self.assertRegex(bounded.decode(), r"\[truncated: \d+ more chars, \d+ more results;")
                self.sizes[f"{name}_before_budget"] = len(full)
                self.sizes[f"{name}_budget_35"] = len(bounded)

    def test_compact_and_quiet(self):
        output = self.ok("search", "budgetneedle", "--type", "keyword", "--compact", "--limit", "5")
        self.assertLess(len(output), 900)
        self.assertLessEqual(len(output.splitlines()), 5)
        for row in output.decode().splitlines():
            self.assertRegex(row, r"^.+:\d+: ")
        self.sizes["search_compact_5"] = len(output)
        for command in ("search", "grep"):
            args = ("--type", "keyword") if command == "search" else ("--no-live", "-F")
            result = self.cli(command, "unique-no-hit-f01c5513", *args, "--quiet")
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout, b"(no results)\n")
            self.assertNotIn(b"Tip:", result.stderr)
            self.assertNotIn(b"Diagnostics:", result.stderr)

    def test_list_names_and_metadata(self):
        named = json.loads(self.ok("list", "*handoff*", "--format", "json"))
        self.assertTrue(named)
        filtered = self.ok("list", "--metadata", "task=cli-token-budget", "--paths-only", "--limit", "100")
        self.assertGreaterEqual(len(filtered.splitlines()), 10)

    def test_freshness_edit_and_incremental_add(self):
        self.ok("status", "--repo", str(self.repo))
        self.files[0].write_text("new_cli_symbol_67ca\n", encoding="utf-8")
        stale = self.cli("status", "--repo", str(self.repo), "--json")
        self.assertEqual(stale.returncode, 3, stale.stderr.decode(errors="replace"))
        self.assertEqual(json.loads(stale.stdout)["stale"], 1)
        self.ok("add", "--changed", str(self.repo), "--no-embeddings")
        fresh = json.loads(self.ok("status", "--repo", str(self.repo), "--json"))
        self.assertTrue(fresh["fresh"])
        self.assertIn(b"new_cli_symbol_67ca", self.ok("grep", "-F", "new_cli_symbol_67ca", "--compact", "--no-live"))

    def test_live_and_ephemeral(self):
        if not shutil.which("rg"):
            self.skipTest("rg not installed")
        output = self.ok("grep", "-F", "budgetneedle", "--live", "--compact", "--max-results", "2")
        self.assertEqual(len(output.splitlines()), 2)
        scratch = self.root / "scratchpad"
        scratch.mkdir(exist_ok=True)
        (scratch / "copy.txt").write_text("ephemeral_probe\n", encoding="utf-8")
        original = self.config.read_text()
        try:
            self.config.write_text(original.replace("exclude_paths = []", 'exclude_paths = ["**/scratchpad/**"]'))
            absent = self.ok("grep", "-F", "ephemeral_probe", "--cwd", str(scratch), "--live", "--compact")
            self.assertEqual(absent, b"(no results)\n")
            present = self.ok("grep", "-F", "ephemeral_probe", "--cwd", str(scratch), "--live", "--compact", "--include-ephemeral")
            self.assertIn(b"ephemeral_probe", present)
        finally:
            self.config.write_text(original)

    def test_rename_keeps_shared_content(self):
        old = self.files[1]
        renamed = self.repo / "renamed.txt"
        self.git("mv", "--", old.name, renamed.name)
        self.ok("add", "--changed", str(self.repo), "--no-embeddings")
        status = json.loads(self.ok("status", "--repo", str(self.repo), "--json"))
        self.assertTrue(status["fresh"])
        self.assertIn(b"file 1 line 1", self.ok("cat", str(renamed)))

    def test_worktree_shared_hash_scoping(self):
        self.git("add", "--all")
        self.git("-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "acceptance fixture")
        other = self.root / "other-worktree"
        self.git("worktree", "add", "-b", "budget-lane", str(other))
        self.ok("add", "--changed", str(other), "--no-embeddings", cwd=other)
        self.ok("status", "--repo", str(other))
        found = self.ok("grep", "-F", "budgetneedle", "--paths-only", "--no-live", "--worktree", other.name)
        self.assertTrue(found.strip())
        self.assertTrue(all(str(other).encode() in path for path in found.splitlines()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    args, remaining = parser.parse_known_args()
    BINARY = args.binary.resolve(strict=True)
    unittest.main(argv=[__file__, *remaining])
