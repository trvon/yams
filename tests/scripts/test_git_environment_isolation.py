#!/usr/bin/env python3
"""Exercise hostile hook environments using disposable repositories only."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class GitEnvironmentIsolationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        # Bootstrap must not depend on the implementation under test: even a red
        # regression can only damage the disposable victim, never the calling repo.
        self.clean = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self.clean.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        for key in list(self.clean):
            if key.startswith(("YAMS_PREPUSH_", "YAMS_SKIP_PREPUSH_")):
                self.clean.pop(key)
        self.victim = self.root / "victim"
        self.target = self.root / "target"
        for repo in (self.victim, self.target):
            repo.mkdir()
            self.git(repo, "init", "-q")
            self.git(repo, "config", "user.name", "Disposable owner")
            self.git(repo, "config", "user.email", "fixture@example.invalid")
            self.git(repo, "config", "commit.gpgsign", "false")
            self.git(repo, "config", "tag.gpgsign", "false")
            self.git(repo, "config", "core.hooksPath", os.devnull)
            (repo / "README.md").write_text(repo.name + "\n")
            self.git(repo, "add", "README.md")
            self.git(repo, "commit", "-q", "-m", "fix: protect " + repo.name)
        self.git(self.victim, "tag", "v0.19.0")
        self.linked = self.root / "linked"
        self.git(self.victim, "worktree", "add", "-q", "-b", "linked", str(self.linked))
        self.gitdir = Path(self.git(self.linked, "rev-parse", "--absolute-git-dir"))
        self.hook_env = dict(self.clean, GIT_DIR=str(self.gitdir), GIT_PREFIX="")
        self.before = self.snapshot()

    def git(self, repo, *args):
        return subprocess.run(
            ["git", *args],
            cwd=repo,
            env=self.clean,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def snapshot(self):
        return {
            "config": (self.victim / ".git/config").read_bytes(),
            "head": self.git(self.victim, "rev-parse", "HEAD"),
            "linked_head": self.git(self.linked, "rev-parse", "HEAD"),
            "index": (self.gitdir / "index").read_bytes(),
            "readme": (self.linked / "README.md").read_bytes(),
        }

    def test_candidate_fixture_cannot_mutate_hook_repository(self):
        child = subprocess.run(
            [
                sys.executable,
                str(ROOT / "tests/scripts/test_validate_release_candidate.py"),
                "ReleaseCandidateTests.test_accepts_descendant_with_aligned_unbumped_versions",
            ],
            cwd=self.linked,
            env=self.hook_env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        after = self.snapshot()
        # Avoid dumping whole config/index bytes on failure.
        self.assertEqual([k for k in self.before if self.before[k] != after[k]], [])
        self.assertEqual(child.returncode, 0, child.stderr)

    def test_validator_respects_explicit_root_despite_git_overrides(self):
        for extra in (
            {},
            {
                "GIT_WORK_TREE": str(self.linked),
                "GIT_COMMON_DIR": str(self.victim / ".git"),
                "GIT_INDEX_FILE": str(self.gitdir / "index"),
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "user.name",
                "GIT_CONFIG_VALUE_0": "Injected identity",
            },
        ):
            with self.subTest(override_keys=list(extra)):
                child = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        (
                            "import runpy,sys; from pathlib import Path; "
                            "ns=runpy.run_path(sys.argv[1]); "
                            "print(ns['resolve_commit'](Path(sys.argv[2]), 'HEAD'))"
                        ),
                        str(ROOT / "scripts/ci/validate_release_candidate.py"),
                        str(self.target),
                    ],
                    cwd=self.linked,
                    env={**self.hook_env, **extra},
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(child.returncode, 0, child.stderr)
                self.assertEqual(
                    child.stdout.strip(), self.git(self.target, "rev-parse", "HEAD")
                )
                self.assertEqual(self.snapshot(), self.before)

    def test_pre_push_removes_repository_overrides_before_running_gate(self):
        gate = self.linked / "scripts/local-ci/pre-push-ci-gate.sh"
        gate.parent.mkdir(parents=True)
        # A hook dispatch unit test, NOT a substitute for the actual sanitizer gate.
        gate.write_text("""#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import os
from pathlib import Path
for key in ('GIT_DIR', 'GIT_WORK_TREE', 'GIT_COMMON_DIR', 'GIT_INDEX_FILE', 'GIT_PREFIX',
            'GIT_CONFIG_PARAMETERS', 'GIT_CONFIG_COUNT'):
    assert key not in os.environ, 'Leaked hook variable: ' + key
Path('gate.ran').write_text('yes')
PY
""")
        gate.chmod(0o755)
        hook = self.linked / ".githooks/pre-push"
        hook.parent.mkdir()
        shutil.copyfile(ROOT / ".githooks/pre-push", hook)
        child = subprocess.run(
            ["bash", str(hook)],
            cwd=self.linked,
            env=self.hook_env,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(child.returncode, 0, child.stderr)
        self.assertTrue((self.linked / "gate.ran").is_file())
        self.assertEqual(self.snapshot(), self.before)


if __name__ == "__main__":
    unittest.main()
