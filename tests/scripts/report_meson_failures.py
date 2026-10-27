#!/usr/bin/env python3
# Copyright (c) 2026 YAMS Contributors
# SPDX-License-Identifier: GPL-3.0-or-later
"""Print bounded diagnostics for failed tests in a Meson build directory."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import TextIO

NONFAIL_RESULTS = {"OK", "SKIP", "EXPECTEDFAIL"}
MAX_TEST_OUTPUT_CHARS = 16_000
MAX_TEXT_OUTPUT_CHARS = 16_000
MAX_TEXT_READ_BYTES = 64_000
_SENSITIVE_TEXT_LINE = re.compile(
    r"^\s*(?:>>>|command\s*[:=]|(?:inherited\s+)?env(?:ironment)?\s*[:=])", re.IGNORECASE
)


def _display_scalar(value: object, default: str) -> str:
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return default


def _bounded_tail(text: str, limit: int, label: str) -> str:
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return f"[... {label} truncated: {omitted} leading characters omitted ...]\n{text[-limit:]}"


def _print_failure(record: dict[str, object], output: TextIO) -> None:
    name = _display_scalar(record.get("name"), "<unnamed>")
    status = _display_scalar(record.get("result"), "UNKNOWN").strip().upper() or "UNKNOWN"
    returncode = _display_scalar(record.get("returncode"), "<unknown>")
    print(f"Meson test failure: name={name} status={status} returncode={returncode}", file=output)

    sections: list[tuple[str, str]] = []
    for key in ("stdout", "stderr"):
        value = record.get(key)
        if isinstance(value, str) and value:
            sections.append((key, value))

    if not sections:
        print("  (no stdout/stderr captured)", file=output)
        return

    section_limit = MAX_TEST_OUTPUT_CHARS // len(sections)
    for label, text in sections:
        print(f"--- {label} (bounded) ---", file=output)
        print(_bounded_tail(text, section_limit, f"{label} for {name}"), file=output)


def _read_json_log(path: Path, output: TextIO) -> tuple[list[dict[str, object]], bool]:
    records: list[dict[str, object]] = []
    malformed = 0
    try:
        with path.open("r", encoding="utf-8", errors="replace") as log:
            for line_number, line in enumerate(log, start=1):
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except (json.JSONDecodeError, ValueError) as error:
                    malformed += 1
                    print(
                        f"Meson JSONL diagnostic: malformed record at line {line_number}: "
                        f"{error.msg if isinstance(error, json.JSONDecodeError) else error}",
                        file=output,
                    )
                    continue
                if not isinstance(value, dict):
                    malformed += 1
                    print(
                        f"Meson JSONL diagnostic: non-object record at line {line_number}; ignored.",
                        file=output,
                    )
                    continue
                records.append(value)
    except OSError as error:
        print(f"Meson JSONL diagnostic: cannot read {path}: {error}", file=output)
        return [], True

    if malformed:
        print(
            f"Meson JSONL diagnostic: retained {len(records)} valid record(s); "
            f"ignored {malformed} malformed record(s).",
            file=output,
        )
    if not records:
        print(f"Meson JSONL diagnostic: {path} contains no usable records.", file=output)
    return records, malformed > 0 or not records


def _sanitized_text_tail(path: Path) -> tuple[str, bool]:
    with path.open("rb") as log:
        log.seek(0, 2)
        size = log.tell()
        start = max(0, size - MAX_TEXT_READ_BYTES)
        log.seek(start)
        raw = log.read(MAX_TEXT_READ_BYTES)

    text = raw.decode("utf-8", errors="replace")
    if start:
        # Never emit a partial leading line: it may be the tail of an environment header.
        text = text.partition("\n")[2]

    lines: list[str] = []
    redacted = False
    for line in text.splitlines():
        if _SENSITIVE_TEXT_LINE.match(line):
            if not redacted or not lines or lines[-1] != "[command/environment line omitted]":
                lines.append("[command/environment line omitted]")
            redacted = True
        else:
            lines.append(line)

    sanitized = "\n".join(lines)
    if start:
        sanitized = (
            f"[... testlog.txt prefix omitted; showing final {len(raw)} bytes ...]\n"
            f"{sanitized}"
        )
    return _bounded_tail(sanitized, MAX_TEXT_OUTPUT_CHARS, "testlog.txt fallback"), redacted


def _print_text_fallback(path: Path, output: TextIO) -> None:
    if not path.is_file():
        print(f"Meson text fallback unavailable: {path} is missing.", file=output)
        return
    try:
        tail, redacted = _sanitized_text_tail(path)
    except OSError as error:
        print(f"Meson text fallback unreadable: {path}: {error}", file=output)
        return

    print(f"--- bounded fallback tail from {path} ---", file=output)
    if tail:
        print(tail, file=output)
    else:
        print("(testlog.txt is empty after filtering)", file=output)
    if redacted:
        print("Meson text fallback omitted command/environment lines.", file=output)


def report(build_dir: Path, output: TextIO = sys.stdout) -> None:
    log_dir = build_dir / "meson-logs"
    json_path = log_dir / "testlog.json"
    text_path = log_dir / "testlog.txt"

    if not json_path.is_file():
        print(f"Meson JSONL diagnostic: {json_path} is missing.", file=output)
        _print_text_fallback(text_path, output)
        return

    records, needs_fallback = _read_json_log(json_path, output)
    failures = []
    for record in records:
        status = _display_scalar(record.get("result"), "UNKNOWN").strip().upper()
        if status not in NONFAIL_RESULTS:
            failures.append(record)

    if failures:
        for record in failures:
            _print_failure(record, output)
    elif records:
        print("Meson JSONL diagnostic: no failed test records found.", file=output)

    if needs_fallback:
        print(
            "Meson JSONL diagnostic: using text fallback because JSONL was incomplete or unusable.",
            file=output,
        )
        _print_text_fallback(text_path, output)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Report Meson test failures without deciding the CI gate result."
    )
    parser.add_argument("build_dir", type=Path, help="Meson build directory")
    args = parser.parse_args(argv)
    report(args.build_dir)
    return 0


if __name__ == "__main__":
    # Windows CI consoles default to cp1252; test output with non-ASCII text (e.g. "❌") made the
    # reporter crash and hid the failing assertions.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    raise SystemExit(main())
