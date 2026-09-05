#!/usr/bin/env python3
"""Unit tests for the stable-release candidate preflight."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "ci" / "validate_release_candidate.py"
SPEC = importlib.util.spec_from_file_location("validate_release_candidate", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
candidate_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = candidate_module
SPEC.loader.exec_module(candidate_module)

CandidateError = candidate_module.CandidateError


class ReleaseCandidateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.repo = Path(self.tempdir.name)
        self.git("init", "-q")
        self.git("config", "user.name", "Release Test")
        self.git("config", "user.email", "release-test@example.com")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        self.git("config", "core.hooksPath", "/dev/null")
        (self.repo / "README.md").write_text("fixture\n", encoding="utf-8")
        self.git("add", "README.md")
        self.git("commit", "-q", "-m", "chore: initialize fixture")
        self.write_versions("0.19.0")
        self.git("add", ".")
        self.git("commit", "-q", "-m", "chore(main): release 0.19.0")
        self.base = self.git("rev-parse", "HEAD")
        self.git("tag", "v0.19.0")
        (self.repo / "feature.txt").write_text("candidate\n", encoding="utf-8")
        self.git("add", "feature.txt")
        self.git("commit", "-q", "-m", "feat: candidate")
        self.candidate = self.git("rev-parse", "HEAD")

    def git(self, *args: str) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=self.repo,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def write_versions(self, version: str) -> None:
        (self.repo / ".release-please-manifest.json").write_text(
            json.dumps({".": version}) + "\n", encoding="utf-8"
        )
        (self.repo / "meson.build").write_text(
            f"project('yams', 'cpp', version: '{version}')\n", encoding="utf-8"
        )
        (self.repo / "conanfile.py").write_text(
            f'class YamsConan:\n    version = "{version}"\n', encoding="utf-8"
        )
        (self.repo / "CITATION.cff").write_text(
            f'cff-version: 1.2.0\nversion: "{version}"\n', encoding="utf-8"
        )

    def test_accepts_descendant_with_aligned_unbumped_versions(self) -> None:
        report = candidate_module.validate_candidate(
            self.repo, self.base, self.candidate
        )

        self.assertEqual(report.current_version, "0.19.0")
        self.assertEqual(report.latest_stable_tag, "v0.19.0")
        self.assertEqual(report.commit_count, 1)
        self.assertEqual(report.base_sha, self.base)
        self.assertEqual(report.candidate_sha, self.candidate)

    def test_rejects_divergent_candidate(self) -> None:
        self.git("checkout", "-q", "--detach", self.base + "^")
        (self.repo / "other.txt").write_text("divergent\n", encoding="utf-8")
        self.git("add", "other.txt")
        self.git("commit", "-q", "-m", "feat: divergent")
        divergent = self.git("rev-parse", "HEAD")

        with self.assertRaisesRegex(CandidateError, "must contain the complete base"):
            candidate_module.validate_candidate(self.repo, self.base, divergent)

    def test_rejects_candidate_version_prebump(self) -> None:
        self.write_versions("0.20.0")
        self.git("add", ".")
        self.git("commit", "-q", "-m", "chore: pre-bump candidate")

        with self.assertRaisesRegex(CandidateError, "must remain at 0.19.0"):
            candidate_module.validate_candidate(
                self.repo, self.base, self.git("rev-parse", "HEAD")
            )

    def test_rejects_misaligned_candidate_version_surfaces(self) -> None:
        (self.repo / "conanfile.py").write_text(
            'class YamsConan:\n    version = "0.18.0"\n', encoding="utf-8"
        )
        self.git("add", "conanfile.py")
        self.git("commit", "-q", "-m", "fix: drift conan version")

        with self.assertRaisesRegex(CandidateError, "version surfaces disagree"):
            candidate_module.validate_candidate(
                self.repo, self.base, self.git("rev-parse", "HEAD")
            )

    def test_rejects_base_manifest_that_does_not_match_stable_tag(self) -> None:
        self.git("checkout", "-q", "--detach", self.base)
        self.write_versions("0.18.0")
        self.git("add", ".")
        self.git("commit", "-q", "-m", "fix: stale base version")
        stale_base = self.git("rev-parse", "HEAD")
        (self.repo / "next.txt").write_text("next\n", encoding="utf-8")
        self.git("add", "next.txt")
        self.git("commit", "-q", "-m", "feat: next")

        with self.assertRaisesRegex(CandidateError, "does not match latest stable tag"):
            candidate_module.validate_candidate(
                self.repo, stale_base, self.git("rev-parse", "HEAD")
            )

    def test_workflow_is_read_only_and_uses_pinned_release_please_dry_run(self) -> None:
        workflow = (ROOT / ".github/workflows/release-candidate.yml").read_text(
            encoding="utf-8"
        )

        self.assertIn("contents: read", workflow)
        self.assertIn("pull-requests: read", workflow)
        self.assertNotIn("contents: write", workflow)
        self.assertNotIn("pull-requests: write", workflow)
        self.assertIn("release-please@17.11.2", workflow)
        self.assertIn("--dry-run", workflow)
        self.assertIn("--target-branch=experimental", workflow)
        self.assertIn("validate_release_candidate.py", workflow)
        self.assertIn("github.head_ref == 'experimental'", workflow)
        self.assertIn(
            "github.event.pull_request.head.repo.full_name == github.repository",
            workflow,
        )


if __name__ == "__main__":
    unittest.main()
