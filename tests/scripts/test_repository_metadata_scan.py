import contextlib
import importlib.util
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "metadata_policy", Path(__file__).with_name("check_repository_metadata.py")
)
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


class RepositoryScanTests(unittest.TestCase):
    def run_policy(self, root):
        output = io.StringIO()
        with mock.patch.object(sys, "argv", ["check", "--root", str(root)]):
            with contextlib.redirect_stdout(output):
                result = policy.main()
        return result, output.getvalue()

    def seed(self, root):
        for relative, required in policy.EXPECTED_TEXT.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(required), encoding="utf-8")

    def test_excluded_directories_are_not_traversed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            self.seed(root)
            excluded = {root / name for name in policy.SKIP_PARTS}
            for path in excluded:
                path.mkdir()
                (path / "nested").mkdir()
            scandir = os.scandir

            def guarded(path):
                self.assertNotIn(Path(path), excluded, "traversed an excluded directory")
                return scandir(path)

            with mock.patch("os.scandir", side_effect=guarded):
                result, output = self.run_policy(root)
            self.assertEqual(result, 0, output)

    def test_ci_builddir_is_not_entered(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            self.seed(root)
            builddir = root / "builddir"
            builddir.mkdir()
            (builddir / "artifact.txt").write_text(policy.SOURCEHUT_MARKERS[0])
            original = os.scandir

            def guarded(path):
                self.assertNotEqual(Path(path), builddir, "entered CI build artifacts")
                return original(path)

            with mock.patch("os.scandir", side_effect=guarded):
                result, output = self.run_policy(root)
            self.assertEqual(result, 0, output)

    def test_large_source_is_not_read_whole(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            self.seed(root)
            source = root / "large.txt"
            source.write_text("x" * 131072 + policy.SOURCEHUT_MARKERS[0])
            original = Path.open
            case = self

            class BoundedReader:
                def __init__(self, stream):
                    self.stream = stream

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    self.stream.close()

                def read(self, size=-1):
                    case.assertGreater(size, 0, "unbounded source read")
                    case.assertLessEqual(size, 65536, "oversized source read")
                    return self.stream.read(size)

            def guarded(path, *args, **kwargs):
                stream = original(path, *args, **kwargs)
                return BoundedReader(stream) if path == source else stream

            with mock.patch.object(Path, "open", guarded):
                result, output = self.run_policy(root)
            self.assertEqual(result, 1)
            self.assertIn("large.txt", output)

    def test_marker_crossing_chunk_boundary_and_late_invalid_utf8(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            self.seed(root)
            source = root / "boundary.txt"
            marker = policy.SOURCEHUT_MARKERS[0]
            source.write_text("x" * (65536 - 5) + marker + "é" * 65536, encoding="utf-8")
            result, output = self.run_policy(root)
            self.assertEqual(result, 1, output)
            self.assertIn("boundary.txt", output)
            # Match before invalid UTF-8 must still be ignored, like read_text did.
            with source.open("ab") as stream:
                stream.write(b"\xff")
            result, output = self.run_policy(root)
            self.assertEqual(result, 0, output)

    def test_unicode_fixture_is_independent_of_locale_encoding(self):
        original = Path.write_text

        def windows_default(path, data, encoding=None, **kwargs):
            return original(path, data, encoding=encoding or "cp1252", **kwargs)

        with mock.patch.object(Path, "write_text", windows_default):
            self.test_marker_crossing_chunk_boundary_and_late_invalid_utf8()

    def test_real_source_marker_is_still_detected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            self.seed(root)
            source = root / "src" / "nested" / "example.txt"
            source.parent.mkdir(parents=True)
            source.write_text(policy.SOURCEHUT_MARKERS[0], encoding="utf-8")
            result, output = self.run_policy(root)
            self.assertEqual(result, 1)
            self.assertIn("src/nested/example.txt", output)

    def git(self, root, *args):
        env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        subprocess.run(["git", "-C", str(root), *args], env=env, check=True,
                       stdin=subprocess.DEVNULL, capture_output=True)

    @unittest.skipUnless(shutil.which("git"), "git is not installed")
    def test_checkout_scans_tracked_files_only(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            self.seed(root)
            marker = policy.SOURCEHUT_MARKERS[0]
            # An untracked copy of the tree, as an agent worktree leaves under .claude/.
            stale = root / ".claude" / "worktrees" / "agent" / "README.md"
            stale.parent.mkdir(parents=True)
            stale.write_text(marker, encoding="utf-8")
            (root / "untracked.txt").write_text(marker, encoding="utf-8")
            self.git(root, "init", "-q")
            self.git(root, "add", "--", *policy.EXPECTED_TEXT)
            self.assertIsNotNone(policy.tracked_files(root))
            result, output = self.run_policy(root)
            self.assertEqual(result, 0, output)

            tracked = root / "src" / "tracked.txt"
            tracked.parent.mkdir()
            tracked.write_text(marker, encoding="utf-8")
            self.git(root, "add", "--", "src/tracked.txt")
            result, output = self.run_policy(root)
            self.assertEqual(result, 1, output)
            self.assertIn("src/tracked.txt", output)
            self.assertNotIn(".claude", output)
            self.assertNotIn("untracked.txt", output)

    @unittest.skipUnless(shutil.which("git"), "git is not installed")
    def test_checkout_scan_ignores_hook_git_environment(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve() / "repo"
            other = Path(temp).resolve() / "other"
            for repo in (root, other):
                repo.mkdir()
                self.git(repo, "init", "-q")
            self.seed(root)
            self.git(root, "add", "--", *policy.EXPECTED_TEXT)
            (other / "leak.txt").write_text(policy.SOURCEHUT_MARKERS[0], encoding="utf-8")
            self.git(other, "add", "--", "leak.txt")
            with mock.patch.dict(os.environ, {"GIT_DIR": str(other / ".git")}):
                tracked = policy.tracked_files(root)
            self.assertIsNotNone(tracked)
            self.assertNotIn("leak.txt", tracked)

    @unittest.skipUnless(shutil.which("git"), "git is not installed")
    def test_directory_nested_in_another_checkout_is_walked(self):
        with tempfile.TemporaryDirectory() as temp:
            outer = Path(temp).resolve()
            self.git(outer, "init", "-q")
            root = outer / "export"
            root.mkdir()
            self.seed(root)
            (root / "loose.txt").write_text(policy.SOURCEHUT_MARKERS[0], encoding="utf-8")
            self.assertIsNone(policy.tracked_files(root))
            result, output = self.run_policy(root)
            self.assertEqual(result, 1, output)
            self.assertIn("loose.txt", output)

    def test_walk_skips_gitignored_dot_directories_without_git(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            self.seed(root)
            marker = policy.SOURCEHUT_MARKERS[0]
            (root / ".gitignore").write_text(
                "# local state\n.claude/\n/.cache\n.yams*\n!.keep\nbuild-*/\n", encoding="utf-8"
            )
            for relative in (".claude/worktrees/agent/README.md", ".cache/copy.txt",
                             "docs/.claude/copy.txt"):
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(marker, encoding="utf-8")
            with mock.patch.object(policy, "tracked_files", return_value=None):
                result, output = self.run_policy(root)
                self.assertEqual(result, 0, output)

                # An anchored entry applies at the root only; other dot-directories are scanned.
                for relative in ("docs/.cache/copy.txt", ".github/workflows/ci.yml"):
                    path = root / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(marker, encoding="utf-8")
                result, output = self.run_policy(root)
            self.assertEqual(result, 1, output)
            self.assertIn("docs/.cache/copy.txt", output)
            self.assertIn(".github/workflows/ci.yml", output)
            self.assertNotIn(".claude", output)


if __name__ == "__main__":
    unittest.main()
