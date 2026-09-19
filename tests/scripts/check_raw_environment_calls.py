#!/usr/bin/env python3
"""Reject unreviewed raw process-environment mutations in first-party C/C++ code."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hpp", ".m", ".mm"}
DEFAULT_ROOTS = ("src", "include/yams", "plugins", "tools", "tests")
EXCLUDED_PARTS = {"build", "builddir", "subprojects", "third_party", "vendor"}
CENTRAL_BOUNDARY = "src/config/config_helpers.cpp"
RAW_MUTATION_PATTERN = re.compile(
    r"(?<![A-Za-z0-9_.>:])(?:(?:std\s*::|::)\s*)?"
    r"(?P<name>SetEnvironmentVariableA|SetEnvironmentVariableW|SetEnvironmentVariable|"
    r"_wputenv_s|_wputenv|_putenv_s|_putenv|clearenv|unsetenv|setenv|putenv)\b"
)


@dataclass(frozen=True, order=True)
class Finding:
    path: str
    line: int
    column: int
    name: str

    @property
    def allowlist_key(self) -> tuple[str, int, str]:
        return (self.path, self.line, self.name)


def mask_cpp(text: str) -> str:
    """Mask comments and literals while preserving byte positions and newlines."""
    out = list(text)
    length = len(text)
    index = 0

    def blank(start: int, end: int) -> None:
        for position in range(start, end):
            if out[position] != "\n":
                out[position] = " "

    while index < length:
        if text.startswith("//", index):
            end = text.find("\n", index + 2)
            if end < 0:
                end = length
            blank(index, end)
            index = end
            continue
        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            end = length if end < 0 else end + 2
            blank(index, end)
            index = end
            continue
        if text.startswith('R"', index):
            delimiter_end = text.find("(", index + 2)
            if delimiter_end >= 0:
                delimiter = text[index + 2 : delimiter_end]
                terminator = ")" + delimiter + '"'
                end = text.find(terminator, delimiter_end + 1)
                end = length if end < 0 else end + len(terminator)
                blank(index, end)
                index = end
                continue
        if text[index] in {'"', "'"}:
            quote = text[index]
            end = index + 1
            while end < length:
                if text[end] == "\\":
                    end += 2
                    continue
                if text[end] == quote:
                    end += 1
                    break
                end += 1
            blank(index, min(end, length))
            index = min(end, length)
            continue
        index += 1

    return "".join(out)


def source_files(root: Path, roots: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    for relative_root in roots:
        candidate_root = root / relative_root
        if not candidate_root.exists():
            continue
        for path in candidate_root.rglob("*"):
            if not path.is_file() or path.suffix not in SOURCE_SUFFIXES:
                continue
            relative = path.relative_to(root)
            if any(part in EXCLUDED_PARTS for part in relative.parts):
                continue
            files.append(path)
    return sorted(set(files), key=lambda path: path.relative_to(root).as_posix())


def scan(root: Path, roots: tuple[str, ...]) -> list[Finding]:
    findings: list[Finding] = []
    for path in source_files(root, roots):
        relative = path.relative_to(root).as_posix()
        if relative == CENTRAL_BOUNDARY:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        masked = mask_cpp(text)
        for match in RAW_MUTATION_PATTERN.finditer(masked):
            line = masked.count("\n", 0, match.start()) + 1
            line_start = masked.rfind("\n", 0, match.start()) + 1
            findings.append(
                Finding(
                    relative, line, match.start() - line_start + 1, match.group("name")
                )
            )
    return sorted(findings)


def load_allowlist(path: Path) -> dict[tuple[str, int, str], str]:
    entries: dict[tuple[str, int, str], str] = {}
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        entry, separator, reason = line.partition("#")
        if not separator or not reason.strip():
            raise ValueError(
                f"{path}:{number}: allowlist entry requires a non-empty reason"
            )
        parts = entry.strip().rsplit(":", 2)
        if len(parts) != 3:
            raise ValueError(f"{path}:{number}: expected path:line:raw-env/name")
        file_path, line_number, rule = parts
        if not rule.startswith("raw-env/"):
            raise ValueError(f"{path}:{number}: expected raw-env/<name> rule")
        try:
            source_line = int(line_number)
        except ValueError as error:
            raise ValueError(
                f"{path}:{number}: invalid source line {line_number!r}"
            ) from error
        key = (file_path, source_line, rule.removeprefix("raw-env/"))
        if key in entries:
            raise ValueError(f"{path}:{number}: duplicate allowlist entry {key}")
        entries[key] = reason.strip()
    return entries


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--allowlist", type=Path, required=True)
    parser.add_argument("--scan-root", action="append", dest="roots")
    args = parser.parse_args()

    root = args.root.resolve()
    allowlist_path = args.allowlist.resolve()
    try:
        allowlist = load_allowlist(allowlist_path)
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 2

    findings = scan(root, tuple(args.roots or DEFAULT_ROOTS))
    finding_keys = {finding.allowlist_key for finding in findings}
    unapproved = [
        finding for finding in findings if finding.allowlist_key not in allowlist
    ]
    stale = sorted(set(allowlist) - finding_keys)

    for finding in unapproved:
        replacement = (
            "yams::config::set_environment() or yams::test::ScopedEnvVar"
            if finding.path.startswith("tests/")
            else "yams::config::set_environment()"
        )
        print(
            f"{finding.path}:{finding.line}:{finding.column}: raw-env/{finding.name}: "
            f"use {replacement}",
            file=sys.stderr,
        )
    for path, line, name in stale:
        print(
            f"{allowlist_path}:{path}:{line}: stale raw-env/{name} allowlist entry",
            file=sys.stderr,
        )

    if unapproved or stale:
        return 1
    print(
        f"raw environment mutation policy: {len(findings)} reviewed occurrence(s), 0 new"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
