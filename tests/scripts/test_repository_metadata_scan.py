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
