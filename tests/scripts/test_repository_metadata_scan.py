import contextlib
import importlib.util
import io
import os
from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
