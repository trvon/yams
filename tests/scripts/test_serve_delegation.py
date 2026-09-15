#!/usr/bin/env python3
"""Regression coverage for ``yams serve`` executable delegation."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

CLI_UNDER_TEST: Path | None = None
RUNTIME_LIBRARY_DIRS: list[Path] = []


def _runtime_environment(
    inherited: dict[str, str], directories: list[Path], system: str
) -> dict[str, str]:
    """Give a copied CLI its build libraries without changing executable lookup."""
    env = inherited.copy()
    loader_key = {"Darwin": "DYLD_LIBRARY_PATH", "Linux": "LD_LIBRARY_PATH"}.get(system)
    if loader_key and directories:
        entries = [str(directory) for directory in directories]
        if env.get(loader_key):
            entries.append(env[loader_key])
        env[loader_key] = ":".join(entries)
    return env


def _split_harness_arguments(argv: list[str]) -> tuple[Path, list[Path], list[str]]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--cli", required=True, type=Path)
    parser.add_argument("--runtime-library-dir", action="append", default=[], type=Path)
    args, unittest_args = parser.parse_known_args(argv)
    if not args.cli.is_absolute():
        parser.error("--cli must be an absolute path")
    if not args.cli.is_file():
        parser.error(f"--cli is not a file: {args.cli}")
    for directory in args.runtime_library_dir:
        if not directory.is_absolute():
            parser.error("--runtime-library-dir must be an absolute path")
        if not directory.is_dir():
            parser.error(f"--runtime-library-dir is not a directory: {directory}")
    return args.cli, args.runtime_library_dir, unittest_args


@unittest.skipIf(sys.platform == "win32", "POSIX exec delegation tests")
class ServeDelegationTests(unittest.TestCase):
    def setUp(self) -> None:
        if CLI_UNDER_TEST is None:
            self.fail("test harness requires --cli /absolute/path/to/yams-cli")

        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.bin_dir = self.root / "bin"
        self.bin_dir.mkdir()

        # Deliberately copy rather than symlink: currentExecutablePath() canonicalizes
        # the executable path on macOS, and /proc/self/exe resolves it on Linux.
        self.cli = self.bin_dir / "yams"
        shutil.copy2(CLI_UNDER_TEST, self.cli)
        self.cli.chmod(self.cli.stat().st_mode | 0o111)

        self.home = self.root / "home"
        self.data_dir = self.root / "data"
        self.config_dir = self.root / "config"
        self.cache_dir = self.root / "cache"
        self.state_dir = self.root / "state"
        self.runtime_dir = self.root / "runtime"
        self.work_dir = self.root / "work"
        for directory in (
            self.home,
            self.data_dir,
            self.config_dir,
            self.cache_dir,
            self.state_dir,
            self.runtime_dir,
            self.work_dir,
        ):
            directory.mkdir()

        self.socket = self.runtime_dir / "never-connect-to-a-real-daemon.sock"
        self.config = self.config_dir / "missing-test-config.toml"
        self.env = _runtime_environment(
            dict(os.environ), RUNTIME_LIBRARY_DIRS, platform.system()
        )
        for name in (
            "HOME",
            "PATH",
            "XDG_CACHE_HOME",
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "XDG_RUNTIME_DIR",
            "XDG_STATE_HOME",
            "YAMS_CONFIG",
            "YAMS_CONFIG_PATH",
            "YAMS_DAEMON_BIN",
            "YAMS_DAEMON_SOCKET",
            "YAMS_DAEMON_SOCKET_PATH",
            "YAMS_DATA_DIR",
            "YAMS_EMBEDDED",
            "YAMS_IN_DAEMON",
            "YAMS_STORAGE",
        ):
            self.env.pop(name, None)
        self.env.update(
            {
                "HOME": str(self.home),
                "PATH": "",
                "XDG_CACHE_HOME": str(self.cache_dir),
                "XDG_CONFIG_HOME": str(self.config_dir),
                "XDG_DATA_HOME": str(self.data_dir),
                "XDG_RUNTIME_DIR": str(self.runtime_dir),
                "XDG_STATE_HOME": str(self.state_dir),
                "YAMS_CONFIG": str(self.config),
                "YAMS_CONFIG_PATH": str(self.config),
                "YAMS_CLI_DISABLE_DAEMON_AUTOSTART": "1",
                "YAMS_DAEMON_KILL_OTHERS": "0",
                "YAMS_DAEMON_SOCKET": str(self.socket),
                "YAMS_DAEMON_SOCKET_PATH": str(self.socket),
                "YAMS_DATA_DIR": str(self.data_dir),
                "YAMS_DISABLE_MODEL_DOWNLOAD": "1",
                "YAMS_NON_INTERACTIVE": "1",
                "YAMS_STORAGE": str(self.data_dir),
            }
        )

    def make_stub(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        interpreter = Path(sys.executable)
        self.assertTrue(interpreter.is_absolute(), sys.executable)
        path.write_text(
            f"#!{interpreter}\n"
            "import json\n"
            "import os\n"
            "import pathlib\n"
            "import sys\n"
            "marker = os.environ.get('YAMS_TEST_STUB_MARKER')\n"
            "if marker:\n"
            "    pathlib.Path(marker).write_text('started\\n', encoding='utf-8')\n"
            "print(json.dumps(sys.argv))\n",
            encoding="utf-8",
        )
        path.chmod(0o755)
        return path

    def run_serve(
        self, *serve_args: str, env: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(self.cli), "serve", *serve_args],
            cwd=self.work_dir,
            env=self.env if env is None else env,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )

    def assert_stub_argv(
        self,
        result: subprocess.CompletedProcess[str],
        stub: Path,
        expected_forwarded: list[str],
    ) -> None:
        self.assertEqual(result.returncode, 0, result.stderr)
        try:
            argv = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            self.fail(f"stub did not emit JSON argv: {error}; stdout={result.stdout!r}")
        self.assertIsInstance(argv, list)
        self.assertEqual(Path(argv[0]).resolve(), stub.resolve())
        self.assertEqual(argv[1:], expected_forwarded)

    def test_installed_sibling_resolves_with_empty_path_and_forwards_warn(self) -> None:
        stub = self.make_stub(self.bin_dir / "yams-mcp-server")

        result = self.run_serve()

        self.assert_stub_argv(
            result,
            stub,
            ["--log-level", "warn", "--daemon-socket", str(self.socket)],
        )

    def test_verbose_forwards_info_and_preserves_socket_with_spaces(self) -> None:
        stub = self.make_stub(self.bin_dir / "yams-mcp-server")
        spaced_socket = self.root / "runtime with spaces" / "daemon socket.sock"

        result = self.run_serve(
            "--verbose", "--daemon-socket", str(spaced_socket)
        )

        self.assert_stub_argv(
            result,
            stub,
            ["--log-level", "info", "--daemon-socket", str(spaced_socket)],
        )

    def test_build_tree_fallback_resolves_without_path(self) -> None:
        stub = self.make_stub(self.root / "yams-mcp" / "yams-mcp-server")

        result = self.run_serve()

        self.assert_stub_argv(
            result,
            stub,
            ["--log-level", "warn", "--daemon-socket", str(self.socket)],
        )

    def test_path_fallback_resolves_after_sibling_locations_miss(self) -> None:
        path_dir = self.root / "path-only-bin"
        stub = self.make_stub(path_dir / "yams-mcp-server")
        env = dict(self.env, PATH=str(path_dir))

        result = self.run_serve(env=env)

        self.assert_stub_argv(
            result,
            stub,
            ["--log-level", "warn", "--daemon-socket", str(self.socket)],
        )

    def test_missing_server_is_a_yams_127_diagnostic_not_outer_enoent(self) -> None:
        try:
            result = self.run_serve()
        except FileNotFoundError as error:
            self.fail(f"outer subprocess spawn unexpectedly failed: {error}")

        self.assertEqual(result.returncode, 127, result.stderr)
        self.assertIn("Failed to exec", result.stderr)
        self.assertIn("yams-mcp-server", result.stderr)

    def test_literal_cli_plus_serve_is_outer_enoent_before_process_start(self) -> None:
        self.make_stub(self.bin_dir / "yams-mcp-server")
        marker = self.root / "stub-started"
        env = dict(self.env, YAMS_TEST_STUB_MARKER=str(marker))
        malformed_executable = f"{self.cli} serve"

        with self.assertRaises(FileNotFoundError) as raised:
            subprocess.run(
                [malformed_executable],
                cwd=self.work_dir,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )

        self.assertEqual(raised.exception.filename, malformed_executable)
        self.assertFalse(marker.exists(), "the malformed outer spawn started a process")


if __name__ == "__main__":
    CLI_UNDER_TEST, RUNTIME_LIBRARY_DIRS, unittest_arguments = _split_harness_arguments(
        sys.argv[1:]
    )
    unittest.main(argv=[sys.argv[0], *unittest_arguments])
