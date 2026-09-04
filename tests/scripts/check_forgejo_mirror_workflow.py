#!/usr/bin/env python3
"""Static safety policy for the GitHub-to-Forgejo mirror workflow."""

from __future__ import annotations

import argparse
from pathlib import Path

REQUIRED = (
    "contents: read",
    "group: forgejo-source-mirror",
    "cancel-in-progress: false",
    "refs/heads/main",
    "refs/heads/experimental",
    "refs/tags/v[0-9]*",
    "refs/tags/yams-v[0-9]*",
    "FORGEJO_MIRROR_SSH_KEY",
    "FORGEJO_MIRROR_KNOWN_HOSTS",
    "StrictHostKeyChecking=yes",
    'git push forgejo "refs/mirror/source:${SOURCE_REF}"',
    'git ls-remote --exit-code forgejo "${SOURCE_REF}"',
    '"${actual_sha}" != "${EXPECTED_SHA}"',
)

FORBIDDEN = (
    "-" + "-mirror",
    "-" + "-prune",
    "-" + "-force",
    "git push " + "-f ",
    "git push " + "--delete",
    "ssh-keyscan",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    workflow = args.root.resolve() / ".github/workflows/mirror-forgejo.yml"

    if not workflow.is_file():
        print(f"missing workflow: {workflow}")
        return 1

    text = workflow.read_text(encoding="utf-8")
    failures = [
        f"missing required control: {value}" for value in REQUIRED if value not in text
    ]
    failures.extend(
        f"forbidden mirror operation: {value}" for value in FORBIDDEN if value in text
    )

    if text.count("git push forgejo") != 1:
        failures.append("workflow must contain exactly one explicit Forgejo push")
    if "pull_request:" in text:
        failures.append("mirror workflow must not receive credentials on pull requests")

    if failures:
        print("\n".join(failures))
        return 1

    print("Forgejo mirror workflow policy: clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
