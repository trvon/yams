#!/usr/bin/env python3
"""Reject Tracy zones that stay open across a coroutine suspension point.

A Tracy zone must begin and end on the same thread, in stack order. A scoped zone
whose scope contains ``co_await``/``co_yield`` can end on another thread after the
coroutine resumes; the profiler reports "Zone is ended twice" and drops the capture.

Scoped zones must cover synchronous code only. ``YAMS_ZONE_SYNC_N(var, name)`` may
precede a suspension point if ``YAMS_ZONE_END(var)`` comes first at the zone's own
scope level (an end inside a branch does not count). Bodies of nested lambdas are
separate functions and are skipped.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

SOURCE_SUFFIXES = {".cc", ".cpp", ".cxx", ".h", ".hpp", ".mm"}
DEFAULT_ROOTS = ("src", "include/yams", "plugins", "tools")
EXCLUDED_PARTS = {"build", "builddir", "subprojects", "third_party", "vendor"}
EXCLUDED_FILES = {"include/yams/profiling.h"}

SCOPED_ZONE = re.compile(
    r"\b(?P<macro>YAMS_ZONE_SCOPED(?:_N|_C|_NC|_DYN)?|YAMS_EMBEDDING_ZONE_BATCH|"
    r"YAMS_VECTOR_SEARCH_ZONE|YAMS_ONNX_ZONE|YAMS_STORAGE_ZONE|"
    r"ZoneScoped(?:N|C|NC|S|NS|CS|NCS)?|ZoneNamed(?:N|C|NC|S|NS|CS|NCS)?)\s*\("
    r"|\b(?P<bare>ZoneScoped)\b"
)
SYNC_ZONE = re.compile(r"\bYAMS_ZONE_SYNC_N\s*\(\s*(?P<var>[A-Za-z_]\w*)\s*,")
SUSPEND = re.compile(r"\bco_(?:await|yield)\b")
LAMBDA_HEAD = re.compile(
    r"\[[^\[\]]*\]\s*(?:\([^()]*(?:\([^()]*\)[^()]*)*\))?\s*(?:mutable\s*)?"
    r"(?:noexcept\s*)?(?:->\s*[^{;]*)?$"
)


@dataclass(frozen=True, order=True)
class Finding:
    path: str
    line: int
    zone: str
    suspend_line: int

    def render(self) -> str:
        return (f"{self.path}:{self.line}: zone {self.zone} is open across a "
                f"suspension point at line {self.suspend_line}")


def mask_cpp(text: str) -> str:
    """Blank comments and literals, keeping offsets and newlines."""
    out = list(text)
    n = len(text)
    i = 0

    def blank(a: int, b: int) -> None:
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        if text.startswith("//", i):
            end = text.find("\n", i + 2)
            end = n if end < 0 else end
            blank(i, end)
            i = end
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            end = n if end < 0 else end + 2
            blank(i, end)
            i = end
            continue
        if text.startswith('R"', i):
            open_paren = text.find("(", i + 2)
            if open_paren >= 0:
                term = ")" + text[i + 2:open_paren] + '"'
                end = text.find(term, open_paren + 1)
                end = n if end < 0 else end + len(term)
                blank(i, end)
                i = end
                continue
        if text[i] in "\"'":
            quote = text[i]
            end = i + 1
            while end < n:
                if text[end] == "\\":
                    end += 2
                    continue
                if text[end] == quote:
                    end += 1
                    break
                end += 1
            # Keep the quotes so zone names stay attributable; blank the body only.
            blank(i + 1, max(i + 1, min(end, n) - 1))
            i = min(end, n)
            continue
        i += 1
    return "".join(out)


def _matching_brace(masked: str, open_index: int) -> int:
    depth = 0
    for k in range(open_index, len(masked)):
        if masked[k] == "{":
            depth += 1
        elif masked[k] == "}":
            depth -= 1
            if depth == 0:
                return k
    return len(masked)


def _first_suspend(masked: str, start: int, stop_pattern: re.Pattern[str] | None) -> int | None:
    """Offset of the first suspension point in the enclosing scope after ``start``."""
    depth = 0
    k = start
    n = len(masked)
    while k < n:
        # An early end inside a branch does not close the zone on every path.
        if stop_pattern is not None and depth == 0 and stop_pattern.match(masked, k):
            return None
        ch = masked[k]
        if ch == "{":
            head_start = max(masked.rfind(";", 0, k), masked.rfind("{", 0, k),
                             masked.rfind("}", 0, k)) + 1
            head = masked[head_start:k].strip()
            if LAMBDA_HEAD.search(head):
                k = _matching_brace(masked, k) + 1
                continue
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth < 0:
                return None
        elif ch == "c":
            m = SUSPEND.match(masked, k)
            if m and (k == 0 or not (masked[k - 1].isalnum() or masked[k - 1] == "_")):
                return k
        k += 1
    return None


def _zone_name(original: str, start: int) -> str:
    line_end = original.find("\n", start)
    snippet = original[start:line_end if line_end >= 0 else len(original)]
    m = re.search(r'"([^"]*)"', snippet)
    return m.group(1) if m else snippet.strip().rstrip(";")[:60]


def scan_text(relative: str, text: str) -> list[Finding]:
    masked = mask_cpp(text)
    findings: list[Finding] = []

    def line_of(offset: int) -> int:
        return masked.count("\n", 0, offset) + 1

    for m in SCOPED_ZONE.finditer(masked):
        if m.group("bare") and masked[m.end():].lstrip().startswith("("):
            continue
        line_start = masked.rfind("\n", 0, m.start()) + 1
        if masked[line_start:m.start()].lstrip().startswith("#"):
            continue
        suspend = _first_suspend(masked, m.end(), None)
        if suspend is not None:
            findings.append(Finding(relative, line_of(m.start()), _zone_name(text, m.start()),
                                    line_of(suspend)))
    for m in SYNC_ZONE.finditer(masked):
        end = re.compile(r"\bYAMS_ZONE_END\s*\(\s*" + re.escape(m.group("var")) + r"\s*\)")
        suspend = _first_suspend(masked, m.end(), end)
        if suspend is not None:
            findings.append(Finding(relative, line_of(m.start()), _zone_name(text, m.start()),
                                    line_of(suspend)))
    return findings


def source_files(root: Path, roots: tuple[str, ...]) -> list[Path]:
    files: set[Path] = set()
    for relative_root in roots:
        base = root / relative_root
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix not in SOURCE_SUFFIXES:
                continue
            rel = path.relative_to(root)
            if any(part in EXCLUDED_PARTS for part in rel.parts):
                continue
            if rel.as_posix() in EXCLUDED_FILES:
                continue
            files.add(path)
    return sorted(files, key=lambda p: p.relative_to(root).as_posix())


def scan(root: Path, roots: tuple[str, ...] = DEFAULT_ROOTS) -> list[Finding]:
    findings: list[Finding] = []
    for path in source_files(root, roots):
        text = path.read_text(encoding="utf-8", errors="replace")
        if "co_await" not in text and "co_yield" not in text:
            continue
        findings.extend(scan_text(path.relative_to(root).as_posix(), text))
    return sorted(findings)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args(argv)
    findings = scan(args.root.resolve())
    for finding in findings:
        print(finding.render())
    if findings:
        print(f"{len(findings)} Tracy zone(s) open across co_await/co_yield. Scope the zone to "
              "synchronous code, or use YAMS_ZONE_SYNC_N + YAMS_ZONE_END before suspending.",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
