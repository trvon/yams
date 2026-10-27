#!/usr/bin/env python3
# Copyright (c) 2026 YAMS Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("report_meson_failures.py")
SPEC = importlib.util.spec_from_file_location("report_meson_failures", SCRIPT)
assert SPEC and SPEC.loader
REPORTER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = REPORTER
SPEC.loader.exec_module(REPORTER)


class MesonFailureReporterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.build_dir = Path(self.temp.name) / "build"
        self.log_dir = self.build_dir / "meson-logs"
        self.log_dir.mkdir(parents=True)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_jsonl(self, *records: object) -> None:
        (self.log_dir / "testlog.json").write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )

    def render(self) -> str:
        output = io.StringIO()
        REPORTER.report(self.build_dir, output)
        return output.getvalue()

    def test_excludes_nonfail_results_and_reports_multiple_failures(self) -> None:
        self.write_jsonl(
            {"name": "passing", "result": "OK", "returncode": 0, "stdout": "pass"},
            {"name": "skipped", "result": "SKIP", "returncode": 0},
            {"name": "expected", "result": "EXPECTEDFAIL", "returncode": 1},
            {
                "name": "surprising-pass",
                "result": "UNEXPECTEDPASS",
                "returncode": 0,
                "stdout": "expected failure did not happen",
            },
            {
                "name": "hung-test",
                "result": "TIMEOUT",
                "returncode": -15,
                "stderr": "timed out waiting for worker",
            },
        )

        output = self.render()

        self.assertNotIn("name=passing", output)
        self.assertNotIn("name=skipped", output)
        self.assertNotIn("name=expected ", output)
        self.assertIn(
            "name=surprising-pass status=UNEXPECTEDPASS returncode=0", output
        )
        self.assertIn("name=hung-test status=TIMEOUT returncode=-15", output)
        self.assertIn("expected failure did not happen", output)
        self.assertIn("timed out waiting for worker", output)

    def test_missing_json_uses_sanitized_text_fallback(self) -> None:
        (self.log_dir / "testlog.txt").write_text(
            "Meson test log\n"
            ">>> TOKEN=do-not-print /tmp/private-test --secret argument\n"
            "environment: PASSWORD=also-do-not-print\n"
            "Inherited environment: TOKEN=inherited-secret\n"
            "failing-case: assertion at useful_file.cpp:42\n",
            encoding="utf-8",
        )

        output = self.render()

        self.assertIn("testlog.json is missing", output)
        self.assertIn("bounded fallback tail", output)
        self.assertIn("failing-case: assertion at useful_file.cpp:42", output)
        self.assertIn("command/environment line omitted", output)
        self.assertNotIn("do-not-print", output)
        self.assertNotIn("also-do-not-print", output)
        self.assertNotIn("private-test", output)
        self.assertNotIn("inherited-secret", output)

    def test_malformed_json_reports_problem_and_uses_text_fallback(self) -> None:
        (self.log_dir / "testlog.json").write_text("{not-json}\n", encoding="utf-8")
        (self.log_dir / "testlog.txt").write_text(
            "timeout-test TIMEOUT after 30 seconds\n", encoding="utf-8"
        )

        output = self.render()

        self.assertIn("malformed record at line 1", output)
        self.assertIn("contains no usable records", output)
        self.assertIn("using text fallback", output)
        self.assertIn("timeout-test TIMEOUT after 30 seconds", output)

    def test_partial_jsonl_keeps_valid_failure_records(self) -> None:
        (self.log_dir / "testlog.json").write_text(
            json.dumps(
                {
                    "name": "valid-failure",
                    "result": "ERROR",
                    "returncode": 2,
                    "stdout": "preserve this diagnostic",
                }
            )
            + "\n{truncated\n",
            encoding="utf-8",
        )
        (self.log_dir / "testlog.txt").write_text(
            "fallback context for truncated record\n", encoding="utf-8"
        )

        output = self.render()

        self.assertIn("name=valid-failure status=ERROR returncode=2", output)
        self.assertIn("preserve this diagnostic", output)
        self.assertIn("retained 1 valid record(s)", output)
        self.assertIn("fallback context for truncated record", output)

    def test_bounds_failure_output_and_ignores_json_env_and_command(self) -> None:
        self.write_jsonl(
            {
                "name": "large-failure",
                "result": "FAIL",
                "returncode": 1,
                "stdout": "A" * (REPORTER.MAX_TEST_OUTPUT_CHARS + 100),
                "env": {"TOKEN": "json-env-secret"},
                "command": ["private-binary", "--token=json-command-secret"],
            }
        )

        output = self.render()

        self.assertIn("stdout for large-failure truncated", output)
        self.assertLess(len(output), REPORTER.MAX_TEST_OUTPUT_CHARS + 1_000)
        self.assertNotIn("json-env-secret", output)
        self.assertNotIn("json-command-secret", output)
        self.assertNotIn("private-binary", output)

    def test_truncated_environment_line_is_not_emitted(self) -> None:
        (self.log_dir / "testlog.txt").write_text(
            "Inherited environment: TOKEN=" + "secret" * REPORTER.MAX_TEXT_READ_BYTES,
            encoding="utf-8",
        )
        output = self.render()
        self.assertNotIn("secret", output)

    def test_missing_json_and_text_are_reported_honestly(self) -> None:
        output = self.render()

        self.assertIn("testlog.json is missing", output)
        self.assertIn("testlog.txt is missing", output)


if __name__ == "__main__":
    unittest.main()
