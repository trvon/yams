#!/usr/bin/env python3
"""Fail on new production YAMS_* surfaces or duplicate TOML reader definitions."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SOURCE_ROOTS = ("src", "include", "tools")
SOURCE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp"}
ENV_LITERAL_RE = re.compile(r"[\"'](YAMS_[A-Z0-9_]+)[\"']")
TOML_DEFINITION_RE = re.compile(
    r"(?P<name>[A-Za-z_]\w*(?:[Tt][Oo][Mm][Ll])\w*)\s*"
    r"\([^;{}]*\)\s*(?:const\s*)?\{",
    re.MULTILINE,
)


def iter_sources(root: Path):
    for relative_root in SOURCE_ROOTS:
        source_root = root / relative_root
        if not source_root.is_dir():
            continue
        for path in source_root.rglob("*"):
            if path.is_file() and path.suffix.lower() in SOURCE_SUFFIXES:
                yield path


def read_allowlist(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def production_environment_keys(root: Path) -> set[str]:
    keys: set[str] = set()
    for path in iter_sources(root):
        keys.update(ENV_LITERAL_RE.findall(path.read_text(encoding="utf-8", errors="ignore")))
    return keys


def toml_reader_definitions(root: Path) -> set[str]:
    definitions: set[str] = set()
    for path in iter_sources(root):
        if path.suffix.lower() in {".h", ".hh", ".hpp"}:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in TOML_DEFINITION_RE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            relative = path.relative_to(root).as_posix()
            definitions.add(f"{relative}:{match.group('name')}:{line}")
    return definitions


def normalized_definition(entry: str) -> str:
    path, name, _line = entry.rsplit(":", 2)
    return f"{path}:{name}"


def run(root: Path, environment_allowlist: Path, reader_allowlist: Path) -> int:
    allowed_keys = read_allowlist(environment_allowlist)
    current_keys = production_environment_keys(root)
    unknown_keys = sorted(current_keys - allowed_keys)

    allowed_readers = read_allowlist(reader_allowlist)
    definitions = sorted(toml_reader_definitions(root))
    unknown_readers = [
        definition
        for definition in definitions
        if normalized_definition(definition) not in allowed_readers
    ]

    if unknown_keys:
        print("new production YAMS_* literals require typed-config review:", file=sys.stderr)
        for key in unknown_keys:
            print(f"  {key}", file=sys.stderr)
    if unknown_readers:
        print("new TOML reader definitions must use parse_simple_toml:", file=sys.stderr)
        for definition in unknown_readers:
            print(f"  {definition}", file=sys.stderr)
    if unknown_keys or unknown_readers:
        return 1

    print(
        "configuration authority policy: "
        f"{len(current_keys)} reviewed YAMS_* literal(s), "
        f"{len(definitions)} reviewed TOML reader definition(s), 0 new"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--environment-allowlist", type=Path, required=True)
    parser.add_argument("--reader-allowlist", type=Path, required=True)
    args = parser.parse_args()
    return run(
        args.root.resolve(),
        args.environment_allowlist.resolve(),
        args.reader_allowlist.resolve(),
    )


if __name__ == "__main__":
    raise SystemExit(main())
