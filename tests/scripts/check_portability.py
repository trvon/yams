#!/usr/bin/env python3
"""Reject unreviewed cross-platform portability hazards in first-party C/C++ code.

Catches the Windows/Linux/macOS-only defect classes that only surface in GitHub
CI, not the macOS-host pre-push sanitizer lanes:

  * portability/jthread     raw ``std::jthread`` (use ``yams::compat::jthread``)
  * portability/path-cstr   ``sqlite3_open*`` filename via ``.c_str()`` on a
                            ``std::filesystem::path`` (use ``.string().c_str()``)
  * portability/env-read    raw ``getenv`` read outside the config boundary
                            (use ``yams::config::getenv_optional``)

Reviewed existing occurrences live in ``portability_allowlist.txt``; new sites
fail the gate.
"""

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

RULES = {
    "jthread": re.compile(r"\bstd\s*::\s*jthread\b"),
    "path-cstr": re.compile(r"sqlite3_open(?:16|_v2)?\s*\([^,]*\.c_str\s*\(\s*\)"),
    "env-read": re.compile(
        r"(?<![A-Za-z0-9_.>:])(?:(?:std\s*::|::)\s*)?(?P<name>getenv|_getenv|_wgetenv)\b"
    ),
}


@dataclass(frozen=True, order=True)
class Finding:
    path: str
    line: int
    column: int
    rule: str

    @property
    def allowlist_key(self) -> tuple[str, int, str]:
        return (self.path, self.line, self.rule)


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
        if text[index] == "'":
            # A C++14 digit separator (1'000'000, 0xFFFF'FFFF) is not a character literal.
            prev = text[index - 1] if index > 0 else ""
            nxt = text[index + 1] if index + 1 < length else ""
            if prev in "0123456789abcdefABCDEF" and nxt in "0123456789abcdefABCDEF":
                index += 1
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


def has_unsafe_path_cstr(text: str) -> bool:
    """True when `text` has a ``.c_str()`` that is not the correct ``.string().c_str()``."""
    for match in re.finditer(r"\.c_str\s*\(\s*\)", text):
        prefix = text[: match.start()]
        if not re.search(r"\.string\s*\(\s*\)\s*$", prefix):
            return True
    return False


def scan(root: Path, roots: tuple[str, ...]) -> list[Finding]:
    findings: list[Finding] = []
    for path in source_files(root, roots):
        relative = path.relative_to(root).as_posix()
        if relative == CENTRAL_BOUNDARY:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        masked = mask_cpp(text)
        for rule, pattern in RULES.items():
            for match in pattern.finditer(masked):
                if rule == "path-cstr" and not has_unsafe_path_cstr(match.group(0)):
                    continue
                line = masked.count("\n", 0, match.start()) + 1
                line_start = masked.rfind("\n", 0, match.start()) + 1
                findings.append(
                    Finding(relative, line, match.start() - line_start + 1, rule)
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
            raise ValueError(f"{path}:{number}: expected path:line:portability/<rule>")
        file_path, line_number, rule = parts
        if not rule.startswith("portability/"):
            raise ValueError(f"{path}:{number}: expected portability/<rule> rule")
        try:
            source_line = int(line_number)
        except ValueError as error:
            raise ValueError(
                f"{path}:{number}: invalid source line {line_number!r}"
            ) from error
        key = (file_path, source_line, rule.removeprefix("portability/"))
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

    hint = {
        "jthread": "yams::compat::jthread",
        "path-cstr": "path.string().c_str()",
        "env-read": "yams::config::getenv_optional()",
    }
    for finding in unapproved:
        print(
            f"{finding.path}:{finding.line}:{finding.column}: "
            f"portability/{finding.rule}: use {hint[finding.rule]}",
            file=sys.stderr,
        )
    for path, line, rule in stale:
        print(
            f"{allowlist_path}:{path}:{line}: stale portability/{rule} allowlist entry",
            file=sys.stderr,
        )

    if unapproved or stale:
        return 1
    print(f"portability policy: {len(findings)} reviewed occurrence(s), 0 new")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
