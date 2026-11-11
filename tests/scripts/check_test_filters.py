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

Registrations outside a 'catch2' suite, or run through a wrapper other than
catch2_allow_all_skipped.py, are not checked; they are counted in the summary
(named with -v), and one that looks like a Catch2 test binary (name contains
"catch2", or built under <build>/tests) gets a WARN line.

A registered binary that is not built is a failure: under `meson test` the
build already exists. Pass --allow-missing for ad-hoc runs on a partial build.

Separately, every registration (any suite) that passes --allow-running-no-tests
must be named in ALLOW_NO_TESTS_ALLOWLIST below with a reason. In Catch2 v3 that
flag also turns "every selected case skipped" into exit 0, so a registration
whose cases can never run (an env var no lane sets, a daemon that never starts)
reports green. Prefer gating the test() in meson on the capability. An
allowlisted name that is registered without the flag is reported as stale, so
the list cannot outlive its entries.

Environment: each listing gets this process's environment plus the
registration's own env. On Windows the registration's extra_paths (the
directories of the DLLs it links) go in front of PATH, as `meson test` does;
without them every binary that links a project DLL exits 0xC0000135. Meson does not expose add_test_setup() environments in
its introspection data, so a setup's env (here, the default 'tsan' setup's
TSAN_OPTIONS) applies only when this script itself runs under `meson test`,
which exports it to this process and so to every listing. Ad-hoc runs do not
get it; listing does not depend on it.
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

ALLOW_NO_TESTS_FLAG = "--allow-running-no-tests"
_BENCH_REASON = (
    "suite 'bench', declared only with -Denable-bench-tests and outside the unit/integration "
    "lanes; the flag predates this guard and has not been audited"
)
# Registration name -> why an all-skipped run may pass. Keep this short: every entry is a
# registration that can report green without running a single case.
ALLOW_NO_TESTS_ALLOWLIST: dict[str, str] = {
    "hnsw_retrieval_diagnostic": (
        "declared only when the ONNX plugin is built; its one case still SKIPs when the host "
        "has no ONNX models_root / embedding model, which meson cannot know at configure time"
    ),
    "write_coordinator_bench": _BENCH_REASON,
    "simeon_lexical_score_bench": _BENCH_REASON,
    "metadata_contention_bench": _BENCH_REASON,
    "hnsw_entity_search_bench": _BENCH_REASON,
    "multi_client_ingestion_bench": _BENCH_REASON,
    "topology_ablation_bench": _BENCH_REASON,
}


@dataclass
class Registration:
    name: str
    binary: Path
    args: list[str]
    env: dict[str, str] = field(default_factory=dict)
    workdir: str | None = None
    extra_paths: list[str] = field(default_factory=list)


@dataclass
class Unchecked:
    """A registration this check does not list, and why."""

    name: str
    reason: str
    warn: bool = False


@dataclass
class Outcome:
    registration: Registration
    status: str  # "ok", "empty", "missing", "error"
    count: int = 0
    detail: str = ""


def _suite_names(entry: dict) -> set[str]:
    # Meson may qualify suites as "<project>:<suite>".
    return {str(s).split(":")[-1] for s in entry.get("suite", [])}


def _is_script(path: str) -> bool:
    return Path(path).name.startswith("python") or path.endswith((".py", ".sh"))


def _looks_like_catch2_binary(path: str, build_dir: Path | None) -> bool:
    if _is_script(path):
        return False
    if "catch2" in Path(path).name:
        return True
    if build_dir is None:
        return False
    try:
        Path(path).resolve().relative_to((build_dir / "tests").resolve())
    except ValueError:
        return False
    return True


def registration_from_entry(entry: dict,
                            build_dir: Path | None = None) -> Registration | Unchecked:
    """Return the Catch2 binary invocation for one intro-tests entry, or why it is unchecked."""
    name = str(entry.get("name", ""))
    cmd = [str(c) for c in entry.get("cmd", [])]
    if not cmd:
        return Unchecked(name, "no command")
    if CATCH2_SUITE not in _suite_names(entry):
        if _looks_like_catch2_binary(cmd[0], build_dir):
            return Unchecked(name, f"looks like a Catch2 test ({Path(cmd[0]).name}) but is not "
                             "in a 'catch2' suite", warn=True)
        return Unchecked(name, "not in a 'catch2' suite")
    # python <wrapper> <binary> args...: check the wrapped binary.
    if len(cmd) >= 3 and Path(cmd[1]).name == ALLOW_ALL_SKIPPED_WRAPPER:
        cmd = cmd[2:]
    elif _is_script(cmd[0]):
        wrapper = Path(cmd[1]).name if len(cmd) > 1 else Path(cmd[0]).name
        return Unchecked(name, f"'catch2' suite but runs through unknown wrapper {wrapper}",
                         warn=True)
    return Registration(
        name=name,
        binary=Path(cmd[0]),
        args=cmd[1:],
        env={str(k): str(v) for k, v in (entry.get("env") or {}).items()},
        workdir=entry.get("workdir"),
        extra_paths=[str(p) for p in entry.get("extra_paths") or []],
    )


def allow_no_tests_findings(
    entries: list[dict], allowlist: dict[str, str] | None = None
) -> tuple[list[str], list[str]]:
    """Return (unlisted, stale) registration names for the --allow-running-no-tests policy.

    unlisted: passes the flag without an allowlist entry.
    stale: allowlisted, registered in this build, and no registration of that name passes it.
    """
    allowed = ALLOW_NO_TESTS_ALLOWLIST if allowlist is None else allowlist
    with_flag: set[str] = set()
    registered: set[str] = set()
    for entry in entries:
        name = str(entry.get("name", ""))
        registered.add(name)
        if ALLOW_NO_TESTS_FLAG in [str(c) for c in entry.get("cmd", [])]:
            with_flag.add(name)
    unlisted = sorted(with_flag - set(allowed))
    stale = sorted((set(allowed) & registered) - with_flag)
    return unlisted, stale


def listing_env(reg: Registration, base: dict[str, str], windows: bool) -> dict[str, str]:
    """The environment `meson test` would give this registration."""
    env = dict(base)
    env.update(reg.env)
    if windows and reg.extra_paths:
        env["PATH"] = os.pathsep.join([*reg.extra_paths, env.get("PATH", "")])
    return env


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
    env = listing_env(reg, dict(os.environ), windows=os.name == "nt")
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


def load_entries(build_dir: Path) -> list[dict]:
    intro = build_dir / "meson-info" / "intro-tests.json"
    if intro.is_file():
        return json.loads(intro.read_text(encoding="utf-8"))
    completed = subprocess.run(
        ["meson", "introspect", "--tests", str(build_dir)],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(completed.stdout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--build-dir", required=True, type=Path)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=60.0, help="per-binary seconds")
    parser.add_argument("--allow-missing", action="store_true",
                        help="report unbuilt binaries instead of failing (partial builds)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="name every registration that is not checked")
    options = parser.parse_args(argv)

    registrations: list[Registration] = []
    unchecked: list[Unchecked] = []
    entries = load_entries(options.build_dir)
    unlisted_flag, stale_flag = allow_no_tests_findings(entries)
    for entry in entries:
        result = registration_from_entry(entry, options.build_dir)
        (registrations if isinstance(result, Registration) else unchecked).append(result)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, options.jobs)) as pool:
        outcomes = list(pool.map(lambda r: check(r, options.timeout), registrations))

    empty = [o for o in outcomes if o.status == "empty"]
    errors = [o for o in outcomes if o.status == "error"]
    missing = [o for o in outcomes if o.status == "missing"]
    checked = [o for o in outcomes if o.status in ("ok", "empty")]
    warned = [u for u in unchecked if u.warn]

    for item in unchecked:
        if item.warn:
            print(f"WARN {item.name}: not checked: {item.reason}")
        elif options.verbose:
            print(f"UNCHECKED {item.name}: {item.reason}")
    missing_label = "SKIP" if options.allow_missing else "MISSING"
    for outcome in missing:
        print(f"{missing_label} {outcome.registration.name}: binary not built ({outcome.detail})")
    for outcome in errors:
        reg = outcome.registration
        print(f"ERROR {reg.name}: {reg.binary.name} {reg.args!r} could not list tests: "
              f"{outcome.detail}")
    for outcome in empty:
        reg = outcome.registration
        print(f"EMPTY {reg.name}: {reg.binary.name} {reg.args!r} selects zero test cases")
    for name in unlisted_flag:
        print(f"ALLOW-NO-TESTS {name}: passes {ALLOW_NO_TESTS_FLAG}, which also passes a run "
              "whose every case skipped; gate the registration in meson, or add it to "
              "ALLOW_NO_TESTS_ALLOWLIST with a reason")
    for name in stale_flag:
        print(f"STALE-ALLOW {name}: on ALLOW_NO_TESTS_ALLOWLIST but registered without "
              f"{ALLOW_NO_TESTS_FLAG}; remove the entry")

    flag_failed = bool(unlisted_flag or stale_flag)
    print(f"checked {len(checked)} Catch2 registrations "
          f"({len(empty)} empty, {len(errors)} errors, {len(missing)} not built, "
          f"{len(unlisted_flag)} unlisted {ALLOW_NO_TESTS_FLAG}, "
          f"{len(stale_flag)} stale allowlist entries); "
          f"{len(unchecked)} registrations not checked ({len(warned)} warnings"
          f"{'' if options.verbose else '; -v names them'})")
    if not checked and not errors:
        print("no Catch2 registration could be checked; is the build directory built?")
        return 1
    if missing and not options.allow_missing:
        return 1
    return 1 if empty or errors or flag_failed else 0


if __name__ == "__main__":
    sys.exit(main())
