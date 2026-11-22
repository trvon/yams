#!/usr/bin/env python3
# Copyright (c) 2026 YAMS Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).with_name("rerun_failed_tests.py")
SPEC = importlib.util.spec_from_file_location("rerun_failed_tests", SCRIPT)
assert SPEC and SPEC.loader
RERUN = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = RERUN
SPEC.loader.exec_module(RERUN)

# Stands in for `meson test`: writes the rerun's testlog.json and records its arguments.
FAKE_MESON = r'''
import json, sys
from pathlib import Path
logs, results, code = Path(sys.argv[1]), json.loads(sys.argv[2]), int(sys.argv[3])
(logs / "argv.json").write_text(json.dumps(sys.argv[4:]))
(logs / "testlog.json").write_text("".join(
    json.dumps({"name": f"yams:integration+daemon / {n}", "result": r}) + "\n"
    for n, r in results.items()))
sys.exit(code)
'''


class RerunFailedTestsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.build = root / "build"
        self.logs = self.build / "meson-logs"
        self.logs.mkdir(parents=True)
        self.fake = root / "fake_meson.py"
        self.fake.write_text(FAKE_MESON)
        self.summary = root / "summary.md"
        self.output = root / "output.txt"

    def first_run(self, **results: str) -> None:
        (self.logs / "testlog.json").write_text("".join(
            json.dumps({"name": f"yams:integration+daemon / {n}", "result": r}) + "\n"
            for n, r in results.items()))
        (self.logs / "testlog.txt").write_text("first run output\n")

    def run_script(self, rerun_results: dict[str, str], code: int = 0,
                   max_reruns: int | None = None) -> tuple[int, str, str]:
        args = [str(self.build)]
        if max_reruns is not None:
            args += ["--max-reruns", str(max_reruns)]
        args += ["--", sys.executable, "-I", str(self.fake), str(self.logs),
                 json.dumps(rerun_results), str(code)]
        out, err = io.StringIO(), io.StringIO()
        env = {"GITHUB_STEP_SUMMARY": str(self.summary), "GITHUB_OUTPUT": str(self.output)}
        with mock.patch.dict(os.environ, env), redirect_stdout(out), redirect_stderr(err):
            rc = RERUN.main(args)
        return rc, out.getvalue(), err.getvalue()

    def rerun_argv(self) -> list[str]:
        return json.loads((self.logs / "argv.json").read_text())

    def test_failures_that_pass_alone_are_flaky_and_do_not_fail_the_job(self) -> None:
        self.first_run(client_timeout_recovery="FAIL", other="OK")
        rc, out, _ = self.run_script({"client_timeout_recovery": "OK"})
        self.assertEqual(rc, 0)
        self.assertEqual(self.rerun_argv(), ["--num-processes", "1", "client_timeout_recovery"])
        self.assertIn("::warning title=Flaky test::client_timeout_recovery", out)
        self.assertEqual((self.logs / "flaky-tests.txt").read_text(), "client_timeout_recovery\n")
        self.assertIn("`client_timeout_recovery`", self.summary.read_text())
        self.assertEqual(self.output.read_text(), "flaky=true\n")
        # The first run's failure output survives the rerun.
        self.assertIn("FAIL", (self.logs / "testlog.first-pass.json").read_text())
        self.assertEqual((self.logs / "testlog.first-pass.txt").read_text(), "first run output\n")

    def test_a_failure_that_repeats_alone_fails_the_job(self) -> None:
        self.first_run(a="FAIL", b="TIMEOUT")
        rc, out, err = self.run_script({"a": "OK", "b": "TIMEOUT"}, code=1)
        self.assertEqual(rc, 1)
        self.assertEqual(self.rerun_argv(), ["--num-processes", "1", "a", "b"])
        self.assertIn("still failing alone: b", err)
        self.assertIn("Flaky test::a", out)
        self.assertEqual((self.logs / "flaky-tests.txt").read_text(), "a\n")

    def test_too_many_failures_are_not_rerun(self) -> None:
        self.first_run(a="FAIL", b="FAIL", c="FAIL")
        rc, _, err = self.run_script({}, max_reruns=2)
        self.assertEqual(rc, 1)
        self.assertIn("more than 2", err)
        self.assertFalse((self.logs / "argv.json").exists())

    def test_no_failed_test_record_is_not_called_flaky(self) -> None:
        self.first_run(a="OK")
        rc, _, err = self.run_script({})
        self.assertEqual(rc, 1)
        self.assertIn("no test did", err)
        self.assertFalse((self.logs / "argv.json").exists())

    def test_missing_log_fails(self) -> None:
        rc, _, err = self.run_script({})
        self.assertEqual(rc, 1)
        self.assertIn("no readable testlog.json", err)

    def test_rerun_that_fails_without_a_failing_record_fails(self) -> None:
        self.first_run(a="FAIL")
        rc, out, err = self.run_script({"a": "OK"}, code=1)
        self.assertEqual(rc, 1)
        self.assertIn("without a failing test record", err)
        self.assertNotIn("Flaky test", out)


if __name__ == "__main__":
    unittest.main()
