#!/usr/bin/env python3
"""Pure harness coverage; never launches the YAMS CLI or a daemon."""

from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

import test_serve_delegation as harness


class ServeRuntimeEnvironmentTests(unittest.TestCase):
    def test_explicit_directories_prepend_the_platform_loader_path(self) -> None:
        directories = [Path("/build/runtime one"), Path("/build/runtime-two")]
        for system, key in (
            ("Darwin", "DYLD_LIBRARY_PATH"),
            ("Linux", "LD_LIBRARY_PATH"),
        ):
            with self.subTest(system=system):
                inherited = {
                    "PATH": "",
                    "DYLD_LIBRARY_PATH": "/inherited/darwin:/other/darwin",
                    "LD_LIBRARY_PATH": "/inherited/linux:/other/linux",
                    "YAMS_CLI_DISABLE_DAEMON_AUTOSTART": "1",
                }
                original = inherited.copy()
                result = harness._runtime_environment(inherited, directories, system)
                expected = dict(original)
                expected[key] = f"{directories[0]}:{directories[1]}:" + original[key]
                self.assertEqual(result, expected)
                self.assertEqual(inherited, original)
                self.assertIsNot(result, inherited)
                self.assertEqual(directories, [Path("/build/runtime one"), Path("/build/runtime-two")])

    def test_absent_or_empty_loader_value_has_no_trailing_separator(self) -> None:
        for system, key in (("Darwin", "DYLD_LIBRARY_PATH"), ("Linux", "LD_LIBRARY_PATH")):
            for inherited in ({"PATH": ""}, {"PATH": "", key: ""}):
                with self.subTest(system=system, inherited=inherited):
                    original = inherited.copy()
                    result = harness._runtime_environment(inherited, [Path("/runtime")], system)
                    self.assertEqual(result, dict(original, **{key: str(Path('/runtime'))}))
                    self.assertEqual(inherited, original)

    def test_windows_leaves_environment_unchanged_but_returns_copy(self) -> None:
        inherited = {"PATH": "C:\\test-bin", "LD_LIBRARY_PATH": "old", "DYLD_LIBRARY_PATH": "old"}
        result = harness._runtime_environment(inherited, [Path("/runtime")], "Windows")
        self.assertEqual(result, inherited)
        self.assertIsNot(result, inherited)

    def test_empty_directories_leave_environment_unchanged_but_return_copy(self) -> None:
        for system in ("Darwin", "Linux", "Windows"):
            with self.subTest(system=system):
                inherited = {"PATH": "", "LD_LIBRARY_PATH": "old", "DYLD_LIBRARY_PATH": "old"}
                result = harness._runtime_environment(inherited, [], system)
                self.assertEqual(result, inherited)
                self.assertIsNot(result, inherited)

    def test_loader_configuration_does_not_interfere_with_cli_path_resolution(self) -> None:
        for system in ("Darwin", "Linux", "Windows"):
            for inherited in ({}, {"PATH": ""}, {"PATH": "/test/path-only-bin"}):
                with self.subTest(system=system, inherited=inherited):
                    original = inherited.copy()
                    result = harness._runtime_environment(inherited, [Path("/runtime")], system)
                    self.assertEqual("PATH" in result, "PATH" in original)
                    self.assertEqual(result.get("PATH"), original.get("PATH"))
                    result["copy-only"] = "value"
                    self.assertEqual(inherited, original)


class ServeHarnessArgumentsTests(unittest.TestCase):
    def test_runtime_directories_are_optional_and_unittest_args_are_preserved(self) -> None:
        cli = Path(__file__).resolve()
        self.assertEqual(
            harness._split_harness_arguments(["--cli", str(cli), "-v", "ServeDelegationTests"]),
            (cli, [], ["-v", "ServeDelegationTests"]),
        )

    def test_repeated_runtime_directories_preserve_order_and_unittest_args(self) -> None:
        cli = Path(__file__).resolve()
        with tempfile.TemporaryDirectory() as root:
            first = Path(root).resolve() / "runtime one"
            second = Path(root).resolve() / "runtime-two"
            first.mkdir()
            second.mkdir()
            argv = [
                "--cli", str(cli), "--runtime-library-dir", str(first), "-v",
                "--runtime-library-dir", str(second), "ServeDelegationTests",
            ]
            original = argv.copy()
            self.assertEqual(
                harness._split_harness_arguments(argv),
                (cli, [first, second], ["-v", "ServeDelegationTests"]),
            )
            self.assertEqual(argv, original)

    def test_invalid_runtime_directories_are_rejected(self) -> None:
        cli = Path(__file__).resolve()
        with tempfile.TemporaryDirectory() as root:
            invalid = (
                (Path("relative-runtime"), "--runtime-library-dir must be an absolute path"),
                (cli, "--runtime-library-dir is not a directory"),
                (Path(root).resolve() / "missing", "--runtime-library-dir is not a directory"),
            )
            for directory, message in invalid:
                with self.subTest(directory=directory):
                    stderr = io.StringIO()
                    with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
                        harness._split_harness_arguments(
                            ["--cli", str(cli), "--runtime-library-dir", str(directory)]
                        )
                    self.assertEqual(raised.exception.code, 2)
                    self.assertIn(message, stderr.getvalue())

    def test_cli_validation_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            for cli in (Path("relative-cli"), Path(root).resolve(), Path(root).resolve() / "missing"):
                with self.subTest(cli=cli):
                    with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                        harness._split_harness_arguments(["--cli", str(cli)])
                    self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
