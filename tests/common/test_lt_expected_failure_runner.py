#!/usr/bin/env python3

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from lt_expected_failure_runner import (
    ValidationError,
    main,
    output_tail,
    validate_execution,
    validate_xml,
)

CASE = "lt_red_case"
EXPRESSION = "contractHeld"


def report(
    *,
    case: str = CASE,
    expression: str = EXPRESSION,
    assertion_type: str = "CHECK",
    failures: int = 1,
    event: str = "",
) -> str:
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<Catch2TestRun name="synthetic" xml-format-version="3" catch2-version="3.5.2">
  <TestCase name="{case}" tags="[lt-red]" filename="synthetic.cpp" line="1">
    {event}
    <Expression success="false" type="{assertion_type}" filename="synthetic.cpp" line="2">
      <Original>{expression}</Original><Expanded>false</Expanded>
    </Expression>
    <OverallResult success="false" skips="0"/>
  </TestCase>
  <OverallResults successes="0" failures="{failures}" expectedFailures="0" skips="0"/>
  <OverallResultsCases successes="0" failures="1" expectedFailures="0" skips="0"/>
</Catch2TestRun>'''


class RunnerValidationTests(unittest.TestCase):
    def validate_report(self, contents: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.xml"
            path.write_text(contents, encoding="utf-8")
            validate_xml(path, CASE, EXPRESSION)

    def test_accepts_only_matching_ordinary_red_assertion(self) -> None:
        validate_execution(returncode=1, timed_out=False, stdout="", stderr="[rusage] ok")
        self.validate_report(report())

    def test_rejects_unexpected_pass_exit_signal_and_timeout(self) -> None:
        scenarios = [
            {"returncode": 0, "timed_out": False, "stdout": "", "stderr": ""},
            {"returncode": 2, "timed_out": False, "stdout": "", "stderr": ""},
            {"returncode": -11, "timed_out": False, "stdout": "", "stderr": ""},
            {"returncode": None, "timed_out": True, "stdout": "", "stderr": ""},
        ]
        for scenario in scenarios:
            with self.subTest(scenario=scenario), self.assertRaises(ValidationError):
                validate_execution(**scenario)

    def test_rejects_sanitizer_and_allocator_diagnostics(self) -> None:
        diagnostics = [
            "ThreadSanitizer: data race",
            "LeakSanitizer: detected memory leaks",
            "MemorySanitizer: use-of-uninitialized-value",
            "malloc(): corrupted top size",
            "double free or corruption (out)",
            "free(): double free detected in tcache 2",
            "free(): invalid pointer",
        ]
        for diagnostic in diagnostics:
            with self.subTest(diagnostic=diagnostic), self.assertRaises(ValidationError):
                validate_execution(
                    returncode=1,
                    timed_out=False,
                    stdout=diagnostic,
                    stderr="",
                )

    def test_cli_rejects_allocator_diagnostic_even_with_expected_xml(self) -> None:
        def fake_catch2(command, **_kwargs):
            Path(command[command.index("-o") + 1]).write_text(report(), encoding="utf-8")
            return mock.Mock(returncode=1, stdout="", stderr="free(): double free detected in tcache 2")

        with mock.patch("lt_expected_failure_runner.subprocess.run", side_effect=fake_catch2):
            self.assertEqual(main(["--binary", "fake-catch2", "--case", CASE,
                                   "--expression", EXPRESSION]), 1)

    def test_cli_prints_child_output_tail_on_rejection(self) -> None:
        child_stderr = "\n".join(f"line {index}" for index in range(80)) + "\nSUMMARY: boom"

        def fake_catch2(command, **_kwargs):
            Path(command[command.index("-o") + 1]).write_text(report(), encoding="utf-8")
            return mock.Mock(returncode=-6, stdout="", stderr=child_stderr)

        captured = io.StringIO()
        with mock.patch("lt_expected_failure_runner.subprocess.run", side_effect=fake_catch2), \
                contextlib.redirect_stderr(captured):
            self.assertEqual(main(["--binary", "fake-catch2", "--case", CASE,
                                   "--expression", EXPRESSION]), 1)
        text = captured.getvalue()
        self.assertIn("terminated by signal 6", text)
        self.assertIn("SUMMARY: boom", text)
        self.assertIn("line 79", text)
        self.assertNotIn("line 10\n", text)
        self.assertEqual(output_tail("a\nb\nc\n", lines=2), "b\nc")

    def test_rejects_setup_failure_exception_wrong_identity_and_counts(self) -> None:
        invalid_reports = [
            report(assertion_type="REQUIRE"),
            report(event="<Exception>boom</Exception>"),
            report(case="other_case"),
            report(expression="unrelatedFailure"),
            report(failures=2),
            "not xml",
        ]
        for contents in invalid_reports:
            with self.subTest(contents=contents[:60]), self.assertRaises(ValidationError):
                self.validate_report(contents)

    def test_rejects_missing_report(self) -> None:
        with self.assertRaises(ValidationError):
            validate_xml(Path("/definitely/missing/lt-report.xml"), CASE, EXPRESSION)


if __name__ == "__main__":
    unittest.main()
