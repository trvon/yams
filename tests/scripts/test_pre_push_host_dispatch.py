#!/usr/bin/env python3
"""Dispatch-only tests with disposable lanes; never evidence of sanitizer success."""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class PrePushHostDispatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.scripts = self.root / "scripts/local-ci"
        self.scripts.mkdir(parents=True)
        self.gate = self.scripts / "pre-push-ci-gate.sh"
        shutil.copyfile(ROOT / "scripts/local-ci/pre-push-ci-gate.sh", self.gate)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        uname = self.bin / "uname"
        uname.write_text('#!/bin/sh\ncase "$1" in -s) echo "$TEST_HOST";; *) echo x86_64;; esac\n')
        uname.chmod(0o755)
        self.calls = self.root / "calls.txt"
        for lane in ("cross-compile-smoke", "native-macos-lane", "native-linux-lane"):
            (self.scripts / (lane + ".sh")).write_text(
                '#!/bin/bash\nset -eu\n'
                'printf "%s %s\\n" "$(basename "$0")" "$*" >> "$TEST_CALLS"\n'
                'case " $* " in *" --profile asan "*) exit "${TEST_ASAN_EXIT:-0}";; '
                '*" --profile tsan "*) exit "${TEST_TSAN_EXIT:-0}";; esac\n'
            )
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                        TEST_CALLS=str(self.calls))

    def run_gate(self, host, *args, **extra):
        result = subprocess.run(
            ["bash", str(self.gate), "--log-root", str(self.root / "logs"), *args],
            env=dict(self.env, TEST_HOST=host, **extra), capture_output=True, text=True,
            timeout=10, check=False,
        )
        calls = self.calls.read_text().splitlines() if self.calls.exists() else []
        return result, calls

    def assert_default_lanes(self, host, lane):
        result, calls = self.run_gate(host)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(calls), 3, calls)
        self.assertTrue(calls[0].startswith("cross-compile-smoke.sh "), calls)
        self.assertTrue(calls[1].startswith(lane + " --profile asan "), calls)
        self.assertTrue(calls[2].startswith(lane + " --profile tsan "), calls)

    def test_linux_runs_both_required_native_profiles(self):
        self.assert_default_lanes("Linux", "native-linux-lane.sh")

    def test_darwin_keeps_both_required_native_profiles(self):
        self.assert_default_lanes("Darwin", "native-macos-lane.sh")

    def test_linux_asan_failure_fails_gate_and_still_runs_tsan(self):
        result, calls = self.run_gate("Linux", TEST_ASAN_EXIT="13")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(calls), 3, calls)
        self.assertIn("--profile tsan", calls[-1])
        self.assertIn("failed (13)", result.stderr)

    def test_linux_tsan_failure_fails_gate(self):
        result, calls = self.run_gate("Linux", TEST_TSAN_EXIT="17")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(calls), 3, calls)
        self.assertIn("failed (17)", result.stderr)

    def test_unsupported_host_fails_closed(self):
        result, calls = self.run_gate("FreeBSD")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(calls), 1, calls)

    def test_mutually_exclusive_profiles_do_not_execute_lanes(self):
        result, calls = self.run_gate("Linux", "--asan-only", "--tsan-only")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
