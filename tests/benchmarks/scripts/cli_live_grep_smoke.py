#!/usr/bin/env python3
"""Compare one live-grep workload against rg and an optional pre-change CLI.

Creates a deterministic, isolated corpus and config; never ingests the corpus.
Writes every measured process duration and command before enforcing the ratio.
This measures process wall time, including startup, rather than engine-only time.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

PATTERN = "cli_live_probe_only"
FILE_RE = re.compile(rb"file-[0-9]+\.txt")


def run_once(argv: list[str], env: dict[str, str], expected: set[bytes]) -> dict:
    start = time.perf_counter_ns()
    result = subprocess.run(argv, env=env, capture_output=True, timeout=60, check=False)
    elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
    paths = set(FILE_RE.findall(result.stdout))
    if result.returncode != 0 or paths != expected:
        raise RuntimeError(
            f"workload correctness failed: {argv[0]}, exit={result.returncode}, "
            f"paths={sorted(paths)!r}, stderr={result.stderr[:1000]!r}"
        )
    return {
        "wall_ms": elapsed_ms,
        "stdout_bytes": len(result.stdout),
        "stderr_bytes": len(result.stderr),
        "return_code": result.returncode,
    }


def benchmark(args: argparse.Namespace, root: Path) -> dict:
    corpus = root / "corpus"
    corpus.mkdir()
    for index in range(args.files):
        text = f"ordinary test file {index}\n"
        if index == args.files - 1:
            text += PATTERN + "\n"
        (corpus / f"file-{index:04}.txt").write_text(text, encoding="utf-8")
    data = root / "data"
    config = root / "config.toml"
    config.write_text(
        "[version]\nconfig_version = 3\n[core]\ndata_dir = "
        + json.dumps(str(data))
        + "\n[search]\nexclude_paths = []\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    for name in (
        "YAMS_CONFIG_PATH", "YAMS_IN_DAEMON", "YAMS_DAEMON_SOCKET",
        "YAMS_DAEMON_SOCKET_PATH", "YAMS_BUDGET_CHARS",
    ):
        env.pop(name, None)
    overrides = {
        "YAMS_CONFIG": str(config),
        "YAMS_DATA_DIR": str(data),
        "YAMS_STORAGE": str(data),
        "YAMS_EMBEDDED": "1",
        "YAMS_DISABLE_VECTORS": "1",
        "YAMS_SKIP_MODEL_LOADING": "1",
        "YAMS_DISABLE_SESSION_WATCHER": "1",
        "YAMS_CLI_DISABLE_DAEMON_AUTOSTART": "1",
    }
    env.update(overrides)
    rg = shutil.which("rg")
    if rg is None:
        raise RuntimeError("rg is required for the paired control")
    commands = {
        "rg": [rg, "--files-with-matches", "--no-ignore-parent", "--regexp", PATTERN,
               "--", str(corpus)],
    }
    binaries = {"after": args.after}
    if args.before is not None:
        binaries["before"] = args.before
    for name, binary in binaries.items():
        commands[name] = [
            str(binary.resolve()), "--data-dir", str(data), "grep", PATTERN,
            "--live", "--paths-only", "--no-session", str(corpus),
        ]
    expected = {f"file-{args.files - 1:04}.txt".encode()}
    # Preflight warms each process path and verifies comparable results, not just exit codes.
    for command in commands.values():
        run_once(command, env, expected)
    samples = {name: [] for name in commands}
    names = list(commands)
    for index in range(args.runs):
        # Rotate ordering so one implementation does not always get the warmest caches.
        order = names[index % len(names):] + names[:index % len(names)]
        for name in order:
            samples[name].append(run_once(commands[name], env, expected))
    cases = {
        name: {
            "argv": commands[name],
            "median_ms": statistics.median(item["wall_ms"] for item in runs),
            "runs": runs,
        }
        for name, runs in samples.items()
    }
    ratio = cases["after"]["median_ms"] / cases["rg"]["median_ms"]
    return {
        "schema_version": "yams_cli_live_grep_smoke_v1",
        "files": args.files,
        "pattern": PATTERN,
        "expected_files": [value.decode() for value in sorted(expected)],
        "warmups": 1,
        "measured_rounds": args.runs,
        "ordering": "rotating",
        "env_overrides": overrides,
        "cases": cases,
        "after_over_rg": ratio,
        "maximum_ratio": args.max_ratio,
        "meets_ratio": ratio <= args.max_ratio,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--after", required=True, type=Path)
    parser.add_argument("--before", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--files", type=int, default=5000)
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--max-ratio", type=float, default=3.0)
    args = parser.parse_args()
    if args.files < 1 or args.runs < 3 or args.max_ratio <= 0:
        parser.error("files must be positive, runs >= 3 and max-ratio > 0")
    with tempfile.TemporaryDirectory(prefix="yams-cli-live-bench-") as temporary:
        report = benchmark(args, Path(temporary))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    for name, case in report["cases"].items():
        print(f"{name}: median {case['median_ms']:.3f} ms")
    print(f"after/rg: {report['after_over_rg']:.3f}x; goal <= {args.max_ratio:.1f}x")
    print(f"artifact: {args.output}")
    return 0 if report["meets_ratio"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
