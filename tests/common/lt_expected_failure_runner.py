#!/usr/bin/env python3
"""Validate one known-red Catch2 assertion without accepting unrelated failures."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

_RUNTIME_DIAGNOSTIC = re.compile(
    r"addresssanitizer|threadsanitizer|leaksanitizer|memorysanitizer|"
    r"undefinedbehaviorsanitizer|runtime error:|terminate called|segmentation fault|"
    r"core dumped|fatal error|uncaught exception|malloc\(\):[^\n]*(?:corrupt|invalid|unaligned)|"
    r"free\(\):[^\n]*(?:corrupt|invalid)|double free|"
    r"corrupted size vs\. prev_size|malloc_consolidate\(\):|"
    r"munmap_chunk\(\): invalid pointer",
    re.IGNORECASE,
)


class ValidationError(RuntimeError):
    """A rejected run. `output` carries the Catch2 child's combined output when it is known."""

    def __init__(self, message: str, output: str = "") -> None:
        super().__init__(message)
        self.output = output


_OUTPUT_TAIL_LINES = 60


def _text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def output_tail(output: str, lines: int = _OUTPUT_TAIL_LINES) -> str:
    """Last `lines` lines of the child's output, for the Meson/CI log on rejection."""
    kept = output.rstrip().splitlines()[-lines:]
    return "\n".join(kept)


def _integer(element: ET.Element, attribute: str) -> int:
    value = element.get(attribute)
    if value is None:
        raise ValidationError(f"missing {element.tag}/@{attribute}")
    try:
        return int(value)
    except ValueError as error:
        raise ValidationError(f"invalid {element.tag}/@{attribute}: {value!r}") from error


def validate_execution(
    *, returncode: int | None, timed_out: bool, stdout: str, stderr: str
) -> None:
    if timed_out:
        raise ValidationError("Catch2 case timed out")
    if returncode is None:
        raise ValidationError("Catch2 process did not produce an exit status")
    if returncode < 0:
        raise ValidationError(f"Catch2 case terminated by signal {-returncode}")
    if returncode != 1:
        reason = "unexpectedly passed" if returncode == 0 else "unexpected exit status"
        raise ValidationError(f"Catch2 case {reason}: {returncode}")
    diagnostics = "\n".join((stdout, stderr))
    match = _RUNTIME_DIAGNOSTIC.search(diagnostics)
    if match:
        raise ValidationError(f"unrelated runtime diagnostic: {match.group(0)}")


def validate_xml(path: Path, expected_case: str, expected_expression: str) -> None:
    try:
        root = ET.parse(path).getroot()  # nosec B314: report is from the owned Catch2 child
    except (OSError, ET.ParseError) as error:
        raise ValidationError(f"missing or malformed Catch2 XML: {error}") from error

    if root.tag != "Catch2TestRun" or root.get("xml-format-version") != "3":
        raise ValidationError("unsupported Catch2 XML report")

    test_cases = root.findall("TestCase")
    if len(test_cases) != 1 or test_cases[0].get("name") != expected_case:
        names = [case.get("name") for case in test_cases]
        raise ValidationError(f"unexpected testcase identity/count: {names!r}")
    test_case = test_cases[0]

    forbidden = test_case.findall(".//Exception") + test_case.findall(".//FatalErrorCondition")
    if forbidden:
        raise ValidationError(f"unexpected Catch2 fatal/exception event: {forbidden[0].tag}")

    failed_expressions = [
        expression
        for expression in test_case.findall(".//Expression")
        if expression.get("success") == "false"
    ]
    if len(failed_expressions) != 1:
        raise ValidationError(f"expected one failed assertion, found {len(failed_expressions)}")
    expression = failed_expressions[0]
    original = "".join(expression.findtext("Original", default="").split())
    expected = "".join(expected_expression.split())
    if expression.get("type") != "CHECK" or original != expected:
        raise ValidationError(
            f"unexpected failed assertion: type={expression.get('type')!r}, expression={original!r}"
        )

    case_result = test_case.find("OverallResult")
    totals = root.find("OverallResults")
    case_totals = root.find("OverallResultsCases")
    if case_result is None or case_result.get("success") != "false" or _integer(case_result, "skips") != 0:
        raise ValidationError("testcase did not report one ordinary non-skipped failure")
    if totals is None or _integer(totals, "failures") != 1 or _integer(totals, "expectedFailures") != 0 or _integer(totals, "skips") != 0:
        raise ValidationError("unexpected assertion totals")
    if case_totals is None or _integer(case_totals, "successes") != 0 or _integer(case_totals, "failures") != 1 or _integer(case_totals, "expectedFailures") != 0 or _integer(case_totals, "skips") != 0:
        raise ValidationError("unexpected testcase totals")


def run_case(binary: Path, case: str, expression: str, timeout: float) -> None:
    with tempfile.TemporaryDirectory(prefix="lt-red-") as directory:
        report = Path(directory) / "report.xml"
        command = [str(binary), case, "-r", "xml", "-o", str(report)]
        try:
            completed = subprocess.run(
                command, capture_output=True, text=True, check=False, timeout=timeout
            )
        except subprocess.TimeoutExpired as error:
            raise ValidationError(
                "Catch2 case timed out",
                output="\n".join((_text(error.stdout), _text(error.stderr))),
            ) from error
        output = "\n".join((completed.stdout, completed.stderr))
        try:
            validate_execution(
                returncode=completed.returncode,
                timed_out=False,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
            validate_xml(report, case, expression)
        except ValidationError as error:
            error.output = output
            raise


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--case", required=True)
    parser.add_argument("--expression", required=True)
    parser.add_argument("--timeout", type=float, default=170.0)
    args = parser.parse_args(argv)
    try:
        run_case(args.binary, args.case, args.expression, args.timeout)
    except (OSError, ValidationError) as error:
        print(f"lt expected-failure validation rejected: {error}", file=sys.stderr)
        tail = output_tail(getattr(error, "output", ""))
        if tail:
            print(f"--- Catch2 output, last {_OUTPUT_TAIL_LINES} lines ---", file=sys.stderr)
            print(tail, file=sys.stderr)
        return 1
    print(f"validated expected red assertion: {args.case}: {args.expression}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
