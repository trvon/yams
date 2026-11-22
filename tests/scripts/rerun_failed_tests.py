#!/usr/bin/env python3
# Copyright (c) 2026 YAMS Contributors
# SPDX-License-Identifier: GPL-3.0-or-later
"""Rerun the tests a Meson run failed, one at a time, to tell flaky tests from broken ones.

A full-suite run shares the runner with every other test: CPU, the temp dir, daemons that are
still stopping. A test that fails there and passes alone is flaky under load. This script runs
only the failed tests, serially, with nothing else running. It exits 0 when all of them pass
alone and reports each as flaky (a GitHub warning, the step summary and
meson-logs/flaky-tests.txt). It exits 1 when any still fails, when too many failed for a rerun
to be credible, or when the first run left no record of what failed.

    rerun_failed_tests.py BUILD_DIR -- meson test -C BUILD_DIR --no-rebuild ...

The command after "--" is the suite's own `meson test` invocation (with any wrapper such as
`arch -x86_64`); the script appends `--num-processes 1` and the failed test names.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

NONFAIL_RESULTS = {"OK", "SKIP", "EXPECTEDFAIL"}
DEFAULT_MAX_RERUNS = 3


def read_results(log: Path) -> dict[str, str] | None:
    """Map each test's short name to its result; None when the log is missing or unreadable."""
    if not log.is_file():
        return None
    results: dict[str, str] = {}
    try:
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            name = str(record.get("name", ""))
            # Meson records "project:suite+suite / name"; `meson test` selects by the bare name.
            short = name.rsplit(" / ", 1)[-1].split(":", 1)[-1]
            results[short] = str(record.get("result", "")).upper()
    except (json.JSONDecodeError, OSError, AttributeError):
        return None
    return results


def failed_tests(results: dict[str, str]) -> list[str]:
    return sorted(name for name, result in results.items() if result not in NONFAIL_RESULTS)


def report(lines: list[str], github_file: str | None) -> None:
    if github_file:
        with open(github_file, "a", encoding="utf-8") as out:
            out.write("\n".join(lines) + "\n")


def main(argv: list[str]) -> int:
    if "--" not in argv:
        print("usage: rerun_failed_tests.py BUILD_DIR [--max-reruns N] -- MESON_TEST_COMMAND...",
              file=sys.stderr)
        return 2
    split = argv.index("--")
    parser = argparse.ArgumentParser()
    parser.add_argument("build_dir", type=Path)
    parser.add_argument("--max-reruns", type=int, default=DEFAULT_MAX_RERUNS)
    args = parser.parse_args(argv[:split])
    command = argv[split + 1:]
    if not command:
        print("rerun: no meson test command after --", file=sys.stderr)
        return 2

    logs = args.build_dir / "meson-logs"
    first = read_results(logs / "testlog.json")
    if first is None:
        print("rerun: the first run left no readable testlog.json; not rerunning.", file=sys.stderr)
        return 1
    failed = failed_tests(first)
    if not failed:
        print("rerun: meson failed but no test did (setup or build error); not rerunning.",
              file=sys.stderr)
        return 1
    if len(failed) > args.max_reruns:
        print(f"rerun: {len(failed)} tests failed, more than {args.max_reruns}; "
              "treating this as a real failure.", file=sys.stderr)
        return 1

    # Keep the first run's logs: they hold the failure output the rerun would overwrite.
    for name in ("testlog.json", "testlog.txt", "testlog.junit.xml"):
        source = logs / name
        if source.is_file():
            shutil.copyfile(source, logs / name.replace("testlog", "testlog.first-pass"))

    print(f"rerun: running {len(failed)} failed test(s) alone: {', '.join(failed)}", flush=True)
    rerun = subprocess.run([*command, "--num-processes", "1", *failed], check=False)
    second = read_results(logs / "testlog.json") or {}

    flaky = [name for name in failed if second.get(name) in NONFAIL_RESULTS]
    broken = [name for name in failed if name not in flaky]
    if rerun.returncode != 0 and not broken:
        # Meson failed for a reason the log does not show; do not call anything flaky.
        print("rerun: the rerun failed without a failing test record.", file=sys.stderr)
        return 1

    if flaky:
        (logs / "flaky-tests.txt").write_text("".join(f"{name}\n" for name in flaky),
                                              encoding="utf-8")
        for name in flaky:
            print(f"::warning title=Flaky test::{name} failed in the full suite and passed "
                  "when rerun alone")
        report(["### Flaky tests", "",
                "Failed in the full suite, passed when rerun alone "
                "(first-run output: `meson-logs/testlog.first-pass.txt`):", "",
                *(f"- `{name}`" for name in flaky)],
               os.environ.get("GITHUB_STEP_SUMMARY"))
        report(["flaky=true"], os.environ.get("GITHUB_OUTPUT"))
    if broken:
        print(f"rerun: still failing alone: {', '.join(broken)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
