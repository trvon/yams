#!/usr/bin/env python3
"""Validate canonical and mirrored repository metadata."""

from __future__ import annotations

import argparse
from pathlib import Path

FORGEJO_URL = "https://git.trevon.dev/trevon/yams"
GITHUB_URL = "https://github.com/trvon/yams"
SOURCEHUT_MARKERS = ("https://sr.ht/~trvon/yams", "https://git.sr.ht/~trevon/yams")

EXPECTED_TEXT = {
    "README.md": (GITHUB_URL, FORGEJO_URL),
    "docs/index.md": (GITHUB_URL, FORGEJO_URL),
    "CITATION.cff": (
        f'repository-code: "{GITHUB_URL}"',
        f'repository-artifact: "{FORGEJO_URL}"',
    ),
    "mkdocs.yml": (
        f"repo_url: {FORGEJO_URL}",
        f"link: {GITHUB_URL}",
        f"link: {FORGEJO_URL}",
    ),
    "scripts/build-deb.sh": (
        f"Homepage: {FORGEJO_URL}",
        f"URL: {FORGEJO_URL}",
    ),
}

# These references describe historical releases or the still-active newsletter list.
SOURCEHUT_ALLOWLIST = {
    "CHANGELOG.md",
    "docs/newsletter.md",
    "tests/scripts/check_repository_metadata.py",
}
SKIP_PARTS = {".git", "build", "node_modules", "subprojects", "third_party"}
RETIRED_PATHS = (".build.yml", "scripts/srht-collect-artifacts.sh")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    root = args.root.resolve()
    failures: list[str] = []

    for relative, required in EXPECTED_TEXT.items():
        path = root / relative
        if not path.is_file():
            failures.append(f"missing metadata file: {relative}")
            continue
        text = path.read_text(encoding="utf-8")
        for value in required:
            if value not in text:
                failures.append(f"{relative}: missing {value!r}")

    for relative in RETIRED_PATHS:
        if (root / relative).exists():
            failures.append(f"retired SourceHut path remains: {relative}")

    for path in root.rglob("*"):
        if not path.is_file() or any(part in SKIP_PARTS for part in path.parts):
            continue
        relative = path.relative_to(root).as_posix()
        if relative in SOURCEHUT_ALLOWLIST:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for marker in SOURCEHUT_MARKERS:
            if marker in text:
                failures.append(f"{relative}: retired repository URL remains: {marker}")

    if failures:
        for failure in failures:
            print(failure)
        return 1

    print("repository metadata policy: clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
