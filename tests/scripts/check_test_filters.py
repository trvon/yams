#!/usr/bin/env python3
"""Fail when a Catch2 test registration selects zero test cases.

Every meson test() registration in the build directory is read from Meson's
introspection data (meson-info/intro-tests.json, the same data
`meson introspect --tests` prints), so there is no hand-maintained list to drift.
For each registration in a 'catch2' suite, the registered binary is asked to list
the cases its registered arguments select:

    <binary> <registered args> --list-tests --reporter xml

A registration that selects nothing is a test that can only "pass" by running
nothing; with --allow-running-no-tests it even exits 0. That hid a deleted test
case and a '[tag]~[slow]' filter whose cases were all tagged [slow].

Binaries that are not built yet are reported and skipped (a partial build).
The check fails when no registration could be checked at all.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

CATCH2_SUITE = "catch2"
# Wrapper that runs a Catch2 binary and maps exit 4 to success (optional plugin suites).
ALLOW_ALL_SKIPPED_WRAPPER = "catch2_allow_all_skipped.py"
# Registered options that would fight the listing invocation; dropped with their value.
_OPTIONS_WITH_VALUE = {"-r", "--reporter", "-o", "--out"}
_TEST_CASE_RE = re.compile(r"<TestCase\b")


@dataclass
class Registration:
    name: str
    binary: Path
    args: list[str]
    env: dict[str, str] = field(default_factory=dict)
    workdir: str | None = None


@dataclass
class Outcome:
    registration: Registration
    status: str  # "ok", "empty", "missing", "error"
    count: int = 0
    detail: str = ""


def _suite_names(entry: dict) -> set[str]:
    # Meson may qualify suites as "<project>:<suite>".
    return {str(s).split(":")[-1] for s in entry.get("suite", [])}


def registration_from_entry(entry: dict) -> Registration | None:
    """Return the Catch2 binary invocation for one intro-tests entry, or None."""
    if CATCH2_SUITE not in _suite_names(entry):
        return None
    cmd = [str(c) for c in entry.get("cmd", [])]
    if not cmd:
        return None
    # python <wrapper> <binary> args...: check the wrapped binary.
    if len(cmd) >= 3 and Path(cmd[1]).name == ALLOW_ALL_SKIPPED_WRAPPER:
        cmd = cmd[2:]
    elif Path(cmd[0]).name.startswith("python") or cmd[0].endswith((".py", ".sh")):
        return None
    return Registration(
        name=str(entry.get("name", "")),
        binary=Path(cmd[0]),
        args=cmd[1:],
        env={str(k): str(v) for k, v in (entry.get("env") or {}).items()},
        workdir=entry.get("workdir"),
    )


def listing_args(args: list[str]) -> list[str]:
    """Registered args minus reporter/output options, plus the XML listing request."""
    kept: list[str] = []
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg in _OPTIONS_WITH_VALUE:
            skip_next = True
            continue
        if any(arg.startswith(opt + "=") for opt in _OPTIONS_WITH_VALUE if opt.startswith("--")):
            continue
        kept.append(arg)
    return kept + ["--list-tests", "--reporter", "xml"]


def count_listed(xml_output: str) -> int:
    return len(_TEST_CASE_RE.findall(xml_output))


def check(reg: Registration, timeout: float) -> Outcome:
    if not reg.binary.is_file():
        return Outcome(reg, "missing", detail=str(reg.binary))
    env = dict(os.environ)
    env.update(reg.env)
    try:
        completed = subprocess.run(
            [str(reg.binary), *listing_args(reg.args)],
            cwd=reg.workdir or None,
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return Outcome(reg, "error", detail=str(error))
    if completed.returncode != 0:
        tail = (completed.stderr or completed.stdout).strip().splitlines()[-5:]
        return Outcome(
            reg, "error", detail=f"exit {completed.returncode}: " + " | ".join(tail)
        )
    count = count_listed(completed.stdout)
    return Outcome(reg, "ok" if count > 0 else "empty", count=count)


def load_registrations(build_dir: Path) -> list[Registration]:
    intro = build_dir / "meson-info" / "intro-tests.json"
    if intro.is_file():
        entries = json.loads(intro.read_text(encoding="utf-8"))
    else:
        completed = subprocess.run(
            ["meson", "introspect", "--tests", str(build_dir)],
            capture_output=True,
            text=True,
            check=True,
        )
        entries = json.loads(completed.stdout)
    regs = [registration_from_entry(entry) for entry in entries]
    return [reg for reg in regs if reg is not None]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--build-dir", required=True, type=Path)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=60.0, help="per-binary seconds")
    options = parser.parse_args(argv)

    registrations = load_registrations(options.build_dir)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, options.jobs)) as pool:
        outcomes = list(pool.map(lambda r: check(r, options.timeout), registrations))

    empty = [o for o in outcomes if o.status == "empty"]
    errors = [o for o in outcomes if o.status == "error"]
    missing = [o for o in outcomes if o.status == "missing"]
    checked = [o for o in outcomes if o.status in ("ok", "empty")]

    for outcome in missing:
        print(f"SKIP {outcome.registration.name}: binary not built ({outcome.detail})")
    for outcome in errors:
        reg = outcome.registration
        print(f"ERROR {reg.name}: {reg.binary.name} {reg.args!r} could not list tests: "
              f"{outcome.detail}")
    for outcome in empty:
        reg = outcome.registration
        print(f"EMPTY {reg.name}: {reg.binary.name} {reg.args!r} selects zero test cases")

    print(f"checked {len(checked)} Catch2 registrations "
          f"({len(empty)} empty, {len(errors)} errors, {len(missing)} not built)")
    if not checked and not errors:
        print("no Catch2 registration could be checked; is the build directory built?")
        return 1
    return 1 if empty or errors else 0


if __name__ == "__main__":
    sys.exit(main())
