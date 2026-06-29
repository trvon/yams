#!/usr/bin/env python3
"""Run a Catch2 executable, treating all-skipped/no-tests exit code as success.

Catch2 returns 4 when every discovered test case is skipped. Meson treats that as
failure, but optional-plugin suites should pass when the plugin is unavailable and
all cases SKIP intentionally.
"""

import subprocess
import sys


CATCH2_ALL_SKIPPED_OR_NO_TESTS = 4


def main(argv: list[str]) -> int:
    if not argv:
        print(
            "usage: catch2_allow_all_skipped.py <catch2-exe> [args...]", file=sys.stderr
        )
        return 2

    completed = subprocess.run(argv, check=False)
    if completed.returncode == CATCH2_ALL_SKIPPED_OR_NO_TESTS:
        print(
            "Catch2 reported all tests skipped/no tests; treating optional suite as skipped."
        )
        return 0
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
