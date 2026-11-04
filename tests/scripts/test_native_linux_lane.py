#!/usr/bin/env python3
"""Fake Meson checks command policy, NOT compiler/runtime correctness."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class NativeLinuxLaneTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        scripts = self.root / "scripts/local-ci"
        scripts.mkdir(parents=True)
        self.lane = scripts / "native-linux-lane.sh"
        shutil.copyfile(ROOT / "scripts/local-ci/native-linux-lane.sh", self.lane)
        (self.root / "meson.build").write_text("# disposable command-policy fixture\n")
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "calls.jsonl"
        self.options = {
            "b_sanitize": ["address", "undefined"], "buildtype": "debug",
            "build-tests": True, "wrap_mode": "nofallback",
        }
        self.data = {
            "buildsystem-files": [str(self.root / "meson.build")],
            "machines": {x: {"system": "linux", "cpu_family": "x86_64", "endian": "little"}
                         for x in ("host", "build")},
            "tests": [{"suite": ["yams:unit"]}, {"suite": ["yams:integration"]}],
        }
        self.data_file = self.root / "info.json"
        fake = self.bin / "meson"
        fake.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
with Path(os.environ["TEST_CALLS"]).open("a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
if sys.argv[1] == "introspect":
    print(json.dumps(json.loads(Path(os.environ["TEST_INFO"]).read_text())[sys.argv[-1][2:]]))
else:
    sys.exit(int(os.environ.get("TEST_" + sys.argv[1].upper() + "_EXIT", "0")))
''')
        fake.chmod(0o755)
        setup = self.root / "setup.sh"
        setup.write_text('''#!/bin/bash
set -eu
printf '%s\\n' "$*" >> "$TEST_SETUP_CALLS"
exit "${TEST_SETUP_EXIT:-0}"
''')
        setup.chmod(0o755)
        uname = self.bin / "uname"
        uname.write_text('#!/bin/sh\necho Linux\n')
        uname.chmod(0o755)
        self.env = {k: v for k, v in os.environ.items() if k not in (
            "LD_PRELOAD", "ASAN_OPTIONS", "UBSAN_OPTIONS", "TSAN_OPTIONS", "TSAN_SYMBOLIZER_PATH")}
        self.env.update(PATH=str(self.bin) + os.pathsep + self.env["PATH"],
                        TEST_CALLS=str(self.calls), TEST_INFO=str(self.data_file),
                        TEST_SETUP_CALLS=str(self.root / "setup-calls.txt"))

    def run_lane(self, profile="asan", **extra):
        self.data["buildoptions"] = [{"name": k, "value": v} for k, v in self.options.items()]
        self.data_file.write_text(json.dumps(self.data))
        result = subprocess.run(
            ["bash", str(self.lane), "--profile", profile], cwd=self.root,
            env=dict(self.env, **extra), capture_output=True, text=True, timeout=10, check=False,
        )
        calls = [json.loads(s) for s in self.calls.read_text().splitlines()] if self.calls.exists() else []
        return result, calls

    def test_asan_uses_offline_system_setup_and_both_test_suites(self):
        result, calls = self.run_lane()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        setup = (self.root / "setup-calls.txt").read_text()
        for arg in ("--offline", "--system-deps", "--asan", "--no-tsan"):
            self.assertIn(arg, setup)
        self.assertTrue(any(c[0] == "compile" and "-j4" in c for c in calls), calls)
        tests = next(c for c in calls if c[0] == "test")
        self.assertIn("unit", tests)
        self.assertIn("integration", tests)
        self.assertIn("--print-errorlogs", tests)

    def test_tsan_is_required_and_runs_tests_serially(self):
        self.options["b_sanitize"] = ["thread"]
        result, calls = self.run_lane("tsan")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        tests = next(c for c in calls if c[0] == "test")
        self.assertEqual(tests[tests.index("--num-processes") + 1], "1")
        setup = (self.root / "setup-calls.txt").read_text()
        self.assertIn("--tsan", setup)
        self.assertIn("--no-asan", setup)

    def assert_preflight_rejected_before_compile(self):
        result, calls = self.run_lane()
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("native Linux preflight failed:", result.stdout + result.stderr)
        self.assertNotIn("Traceback", result.stdout + result.stderr)
        self.assertFalse(any(c[0] in ("compile", "test") for c in calls), calls)

    def test_asan_without_ubsan_is_not_sufficient(self):
        self.options["b_sanitize"] = ["address"]
        self.assert_preflight_rejected_before_compile()

    def test_unsanitized_directory_is_rejected(self):
        self.options["b_sanitize"] = []
        self.assert_preflight_rejected_before_compile()

    def test_missing_integration_suite_is_rejected(self):
        self.data["tests"] = [{"suite": ["yams:unit"]}]
        self.assert_preflight_rejected_before_compile()

    def test_other_source_checkout_is_rejected(self):
        self.data["buildsystem-files"] = [str(self.root / "other/meson.build")]
        self.assert_preflight_rejected_before_compile()

    def test_cross_build_is_not_native_validation(self):
        self.data["machines"]["host"]["cpu_family"] = "aarch64"
        self.assert_preflight_rejected_before_compile()

    def test_setup_failure_stops_before_compile(self):
        result, calls = self.run_lane(TEST_SETUP_EXIT="13")
        self.assertEqual(result.returncode, 13, result.stdout + result.stderr)
        self.assertEqual(calls, [])

    def test_compile_failure_does_not_run_tests(self):
        result, calls = self.run_lane(TEST_COMPILE_EXIT="7")
        self.assertEqual(result.returncode, 7, result.stdout + result.stderr)
        self.assertFalse(any(c[0] == "test" for c in calls), calls)

    def test_test_failure_propagates(self):
        result, calls = self.run_lane(TEST_TEST_EXIT="19")
        self.assertEqual(result.returncode, 19, result.stdout + result.stderr)
        self.assertTrue(any(c[0] == "test" for c in calls), calls)

    def test_inherited_runtime_override_is_rejected(self):
        result, calls = self.run_lane(UBSAN_OPTIONS="halt_on_error=0")
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertEqual(calls, [])

    def test_self_test_does_not_build_or_claim_sanitizer_success(self):
        result, calls = self.run_lane("self-test")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(calls, [])
        self.assertIn("no sanitizer build/tests run", result.stdout)


if __name__ == "__main__":
    unittest.main()
