#!/usr/bin/env python3
# Copyright (c) 2025 YAMS Contributors
# SPDX-License-Identifier: GPL-3.0-or-later
"""Reject benchmark-prefixed controls from first-party production C/C++ code."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hpp", ".m", ".mm"}
DEFAULT_ROOTS = ("src", "include/yams", "plugins", "tools")
EXCLUDED_PARTS = {"build", "builddir", "subprojects", "third_party", "vendor"}
BENCHMARK_ONLY_PREFIXES = ("src/benchmarks/",)
BENCHMARK_ENV_PATTERN = re.compile(r"YAMS_BENCH_[A-Z0-9_]+")


@dataclass(frozen=True, order=True)
class Finding:
    path: str
    line: int
    column: int
    name: str


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
            relative_name = relative.as_posix()
            if any(part in EXCLUDED_PARTS for part in relative.parts):
                continue
            if relative_name.startswith(BENCHMARK_ONLY_PREFIXES):
                continue
            files.append(path)
    return sorted(set(files), key=lambda path: path.relative_to(root).as_posix())


def scan(root: Path, roots: tuple[str, ...] = DEFAULT_ROOTS) -> list[Finding]:
    findings: list[Finding] = []
    for path in source_files(root, roots):
        relative = path.relative_to(root).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in BENCHMARK_ENV_PATTERN.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            line_start = text.rfind("\n", 0, match.start()) + 1
            findings.append(
                Finding(relative, line, match.start() - line_start + 1, match.group(0))
            )
    return sorted(findings)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--scan-root", action="append", dest="roots")
    args = parser.parse_args()

    root = args.root.resolve()
    findings = scan(root, tuple(args.roots or DEFAULT_ROOTS))
    for finding in findings:
        print(
            f"{finding.path}:{finding.line}:{finding.column}: benchmark-env/{finding.name}: "
            "pass a typed harness/config option instead of changing production behavior",
            file=sys.stderr,
        )
    if findings:
        return 1
    print("benchmark environment policy: 0 production leaks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
