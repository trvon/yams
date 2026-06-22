#!/usr/bin/env python3
"""Run a small YAMS CLI overhead smoke and emit JSON.

This is intentionally lightweight and read-oriented. It enables
YAMS_CLI_PERF_TRACE, disables daemon autostart, and records both process wall
clock and emitted trace phases for common user-facing commands.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

TRACE_RE = re.compile(
    r"\[yams-cli-perf\]\s+stage=(?P<stage>\S+)\s+elapsed_us=(?P<elapsed>\d+)(?:\s+note=(?P<note>.*))?"
)


def default_commands() -> list[tuple[str, list[str]]]:
    sentinel = "__yams_cli_overhead_smoke_nohit__"
    return [
        ("version", ["--version"]),
        ("help", ["--help"]),
        ("status_json", ["status", "--json"]),
        ("grep_nohit", ["grep", sentinel, "--limit", "1", "--no-live"]),
        (
            "search_nohit",
            [
                "search",
                sentinel,
                "--limit",
                "1",
                "--no-streaming",
                "--no-group-versions",
                "--json",
            ],
        ),
    ]


def to_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def parse_traces(stderr: str) -> list[dict[str, object]]:
    traces: list[dict[str, object]] = []
    for line in stderr.splitlines():
        match = TRACE_RE.search(line)
        if not match:
            continue
        item: dict[str, object] = {
            "stage": match.group("stage"),
            "elapsed_us": int(match.group("elapsed")),
        }
        if match.group("note"):
            item["note"] = match.group("note")
        traces.append(item)
    return traces


def run_case(
    cli: Path, name: str, args: list[str], timeout_s: float
) -> dict[str, object]:
    env = os.environ.copy()
    env["YAMS_CLI_PERF_TRACE"] = "1"
    env.setdefault("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1")
    env.setdefault("YAMS_DISABLE_MODEL_DOWNLOAD", "1")

    data_dir = env.get("YAMS_CLI_SMOKE_DATA_DIR")
    cmd = [str(cli)]
    if data_dir:
        cmd.extend(["--data-dir", data_dir])
    cmd.extend(args)

    start = time.perf_counter_ns()
    try:
        proc = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            timeout=timeout_s,
            env=env,
            check=False,
        )
        timed_out = False
        stdout = proc.stdout
        stderr = proc.stderr
        return_code = proc.returncode
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = to_text(exc.stdout)
        stderr = to_text(exc.stderr)
        return_code = None
    end = time.perf_counter_ns()

    traces = parse_traces(stderr)
    return {
        "name": name,
        "argv": args,
        "return_code": return_code,
        "timed_out": timed_out,
        "wall_us": (end - start) // 1000,
        "trace_phases": traces,
        "stdout_bytes": len(stdout.encode("utf-8", errors="replace")),
        "stderr_bytes": len(stderr.encode("utf-8", errors="replace")),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cli", default="builddir/tools/yams-cli/yams-cli", help="yams-cli path"
    )
    parser.add_argument("--output", default="bench_results/cli_overhead_smoke.json")
    parser.add_argument(
        "--timeout", type=float, default=20.0, help="per-command timeout seconds"
    )
    args = parser.parse_args()

    cli = Path(args.cli)
    if not cli.exists():
        print(f"CLI not found: {cli}", file=sys.stderr)
        return 2

    cases = [
        run_case(cli, name, cmd_args, args.timeout)
        for name, cmd_args in default_commands()
    ]
    output = {
        "schema_version": "yams_cli_overhead_smoke_v1",
        "cli": str(cli),
        "env": {
            "YAMS_CLI_DISABLE_DAEMON_AUTOSTART": os.environ.get(
                "YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1"
            ),
            "YAMS_CLI_SMOKE_DATA_DIR": os.environ.get("YAMS_CLI_SMOKE_DATA_DIR", ""),
        },
        "cases": cases,
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(str(out_path))

    if any(case["timed_out"] for case in cases):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
