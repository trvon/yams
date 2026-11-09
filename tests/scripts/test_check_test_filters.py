#!/usr/bin/env python3
"""Tests for check_test_filters.py."""

from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import check_test_filters as checker  # noqa: E402

# A stand-in Catch2 binary: lists one case per argument that is not an option, or one
# case for an empty spec; a spec named "nothing" matches zero cases.
FAKE_CATCH2 = """#!{python}
import sys
specs = [a for a in sys.argv[1:] if not a.startswith("-") and a != "xml"]
names = [] if "nothing" in specs else (specs or ["default case"])
print('<?xml version="1.0" encoding="UTF-8"?>')
print("<MatchingTests>")
for name in names:
    print("  <TestCase><Name>" + name + "</Name></TestCase>")
print("</MatchingTests>")
"""


def entry(name: str, cmd: list[str], suite: list[str] | None = None) -> dict:
    return {"name": name, "cmd": cmd, "suite": suite or ["yams:catch2"], "env": {}}


class RegistrationTest(unittest.TestCase):
    def test_non_catch2_suite_is_unchecked_quietly(self) -> None:
        result = checker.registration_from_entry(entry("t", ["/usr/bin/tool"], ["unit"]))
        self.assertIsInstance(result, checker.Unchecked)
        self.assertFalse(result.warn)

    def test_catch2_looking_binary_outside_suite_warns(self) -> None:
        by_name = checker.registration_from_entry(entry("t", ["/b/catch2_x"], ["unit"]))
        self.assertIsInstance(by_name, checker.Unchecked)
        self.assertTrue(by_name.warn)
        by_dir = checker.registration_from_entry(
            entry("t", ["/build/tests/unit/x_test"], ["unit"]), Path("/build")
        )
        self.assertIsInstance(by_dir, checker.Unchecked)
        self.assertTrue(by_dir.warn)

    def test_unknown_wrapper_in_catch2_suite_warns(self) -> None:
        result = checker.registration_from_entry(entry("t", ["/usr/bin/python3", "x.py"]))
        self.assertIsInstance(result, checker.Unchecked)
        self.assertTrue(result.warn)

    def test_allow_all_skipped_wrapper_checks_wrapped_binary(self) -> None:
        reg = checker.registration_from_entry(
            entry("t", ["/usr/bin/python3", "/s/catch2_allow_all_skipped.py", "/b", "[x]"])
        )
        self.assertIsInstance(reg, checker.Registration)
        self.assertEqual(reg.binary, Path("/b"))
        self.assertEqual(reg.args, ["[x]"])

    def test_windows_listing_puts_extra_paths_on_path(self) -> None:
        e = entry("t", ["/b/catch2_t.exe"])
        e["env"] = {"A": "1"}
        e["extra_paths"] = ["/dlls/one", "/dlls/two"]
        reg = checker.registration_from_entry(e)
        self.assertIsInstance(reg, checker.Registration)
        base = {"PATH": "/usr/bin"}
        windows = checker.listing_env(reg, base, windows=True)
        self.assertEqual(windows["PATH"], os.pathsep.join(["/dlls/one", "/dlls/two", "/usr/bin"]))
        self.assertEqual(windows["A"], "1")
        # meson test leaves PATH alone elsewhere (shared libraries load through rpath).
        self.assertEqual(checker.listing_env(reg, base, windows=False)["PATH"], "/usr/bin")

    def test_listing_args_drop_reporter_and_keep_specs(self) -> None:
        self.assertEqual(
            checker.listing_args(["[a]~[slow]", "-r", "junit", "--out=x", "--allow-running-no-tests"]),
            ["[a]~[slow]", "--allow-running-no-tests", "--list-tests", "--reporter", "xml"],
        )

    def test_count_listed(self) -> None:
        xml = "<MatchingTests><TestCase><Name>a</Name></TestCase>\n<TestCase/></MatchingTests>"
        self.assertEqual(checker.count_listed(xml), 2)
        self.assertEqual(checker.count_listed("<MatchingTests/>"), 0)


@unittest.skipIf(os.name == "nt", "fake binary relies on a POSIX shebang")
class EndToEndTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.build = Path(self._tmp.name)
        self.binary = self.build / "fake_catch2"
        self.binary.write_text(FAKE_CATCH2.format(python=sys.executable), encoding="utf-8")
        self.binary.chmod(self.binary.stat().st_mode | stat.S_IXUSR)
        (self.build / "meson-info").mkdir()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def write_intro(self, entries: list[dict]) -> None:
        (self.build / "meson-info" / "intro-tests.json").write_text(
            json.dumps(entries), encoding="utf-8"
        )

    def run_check(self, *extra: str) -> int:
        return checker.main(["--build-dir", str(self.build), "--jobs", "1", *extra])

    def test_selecting_registrations_pass(self) -> None:
        self.write_intro([
            entry("named", [str(self.binary), "Some case"]),
            entry("default", [str(self.binary)]),
        ])
        self.assertEqual(self.run_check(), 0)

    def test_empty_filter_fails_even_with_allow_flag(self) -> None:
        self.write_intro([
            entry("ok", [str(self.binary), "Some case"]),
            entry("empty", [str(self.binary), "nothing", "--allow-running-no-tests"]),
        ])
        self.assertEqual(self.run_check(), 1)

    def test_missing_binary_fails_unless_allowed(self) -> None:
        self.write_intro([
            entry("ok", [str(self.binary), "Some case"]),
            entry("unbuilt", [str(self.build / "absent"), "x"]),
        ])
        self.assertEqual(self.run_check(), 1)
        self.assertEqual(self.run_check("--allow-missing"), 0)
        self.write_intro([entry("unbuilt", [str(self.build / "absent"), "x"])])
        self.assertEqual(self.run_check("--allow-missing"), 1)

    def test_unchecked_registrations_do_not_fail(self) -> None:
        self.write_intro([
            entry("ok", [str(self.binary), "Some case"]),
            entry("policy", ["/usr/bin/python3", "check.py"], ["unit", "policy"]),
            entry("odd", ["/b/catch2_odd"], ["unit"]),
        ])
        self.assertEqual(self.run_check("-v"), 0)


if __name__ == "__main__":
    unittest.main()
