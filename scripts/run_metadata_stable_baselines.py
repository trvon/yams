#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class CommandResult:
    name: str
    command: str
    duration_sec: float
    status: str
    exit_code: int | None
    stdout_log: str
    stderr_log: str
    output_path: str = ""
    env_overrides: dict[str, str] = field(default_factory=dict)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def text_from_timeout(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def run_command(
    name: str,
    command: list[str],
    outdir: Path,
    env: dict[str, str],
    timeout_sec: int,
    output_path: Path | None = None,
    env_overrides: dict[str, str] | None = None,
) -> CommandResult:
    safe_name = name.replace("/", "_").replace(" ", "_")
    stdout_log = outdir / f"{safe_name}.stdout.log"
    stderr_log = outdir / f"{safe_name}.stderr.log"
    start = time.monotonic()
    status = "unknown"
    exit_code: int | None = None
    try:
        proc = subprocess.run(
            command,
            cwd=Path.cwd(),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
        )
        status = "ok" if proc.returncode == 0 else "failed"
        exit_code = proc.returncode
        stdout_log.write_text(proc.stdout, encoding="utf-8", errors="replace")
        stderr_log.write_text(proc.stderr, encoding="utf-8", errors="replace")
    except subprocess.TimeoutExpired as exc:
        status = "timeout"
        stdout_log.write_text(
            text_from_timeout(exc.stdout), encoding="utf-8", errors="replace"
        )
        stderr_log.write_text(
            text_from_timeout(exc.stderr), encoding="utf-8", errors="replace"
        )
    return CommandResult(
        name=name,
        command=" ".join(shlex.quote(part) for part in command),
        duration_sec=round(time.monotonic() - start, 3),
        status=status,
        exit_code=exit_code,
        stdout_log=str(stdout_log),
        stderr_log=str(stderr_log),
        output_path=str(output_path or ""),
        env_overrides=env_overrides or {},
    )


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        records.append(json.loads(line))
    return records


def latency_summary(record: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, value in record.items():
        if not isinstance(value, dict):
            continue
        if "latency_p95_us" not in value or not value.get("attempts"):
            continue
        rows.append(
            {
                "operation": key,
                "attempts": value.get("attempts", 0),
                "mean_us": value.get("latency_mean_us", 0.0),
                "p95_us": value.get("latency_p95_us", 0.0),
                "p99_us": value.get("latency_p99_us", 0.0),
                "max_us": value.get("latency_max_us", 0.0),
                "errors": value.get("errors", 0),
                "lock_errors": value.get("lock_errors", 0),
            }
        )
    rows.sort(key=lambda row: float(row["p95_us"]), reverse=True)
    return rows


def summarize_api(path: Path) -> list[dict[str, Any]]:
    rows = []
    for record in load_jsonl(path):
        rows.append(
            {
                "name": record.get("name", ""),
                "duration_ms": record.get("duration_ms", 0.0),
                "duration_ms_median": record.get("duration_ms_median", 0.0),
                "duration_ms_p95": record.get("duration_ms_p95", 0.0),
                "operations": record.get("operations", 0),
                "ops_per_sec": record.get("ops_per_sec", 0.0),
            }
        )
    rows.sort(key=lambda row: float(row["duration_ms_p95"]), reverse=True)
    return rows


def write_summary(
    outdir: Path, results: list[CommandResult], analysis: dict[str, Any]
) -> None:
    lines = [
        "# Stable metadata baseline run",
        "",
        f"Run directory: `{outdir}`",
        "",
        "## Commands",
        "",
        "| Name | Status | Duration (s) | Exit | Output | Logs |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
    for result in results:
        logs = f"[`stdout`]({Path(result.stdout_log).name}) / [`stderr`]({Path(result.stderr_log).name})"
        exit_code = "" if result.exit_code is None else str(result.exit_code)
        lines.append(
            f"| `{result.name}` | {result.status} | {result.duration_sec:.3f} | "
            f"{exit_code} | `{result.output_path}` | {logs} |"
        )
    lines.extend(["", "## API repeated baseline", ""])
    for row in analysis.get("api", []):
        lines.append(
            f"- `{row['name']}`: p95 `{row['duration_ms_p95']:.3f} ms`, "
            f"median `{row['duration_ms_median']:.3f} ms`, operations `{row['operations']}`"
        )
    lines.extend(["", "## Metadata contention matrix top p95 rows", ""])
    for matrix in analysis.get("metadata_matrix", []):
        lines.append(
            f"### mode={matrix['write_mode']} checkpoint={matrix['checkpoint_every_ms']}ms"
        )
        for row in matrix.get("top_latencies", [])[:5]:
            lines.append(
                f"- `{row['operation']}`: attempts `{row['attempts']}`, "
                f"p95 `{row['p95_us']:.0f} us`, p99 `{row['p99_us']:.0f} us`, "
                f"max `{row['max_us']:.0f} us`, lock_errors `{row['lock_errors']}`"
            )
        lines.append("")
    lines.extend(
        [
            "## Notes",
            "",
            "- These runs strengthen the prior smoke data but still share one local workstation environment.",
            "- Use identical commands for before/after comparisons when implementing a patch.",
        ]
    )
    (outdir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run stable metadata/API optimization baselines"
    )
    parser.add_argument("--build-dir", default="build/release")
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--runtime-sec", type=int, default=8)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--api-iterations", type=int, default=10)
    parser.add_argument("--timeout-sec", type=int, default=420)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    build_dir = Path(args.build_dir).resolve()
    outdir = (
        Path(args.out_dir)
        if args.out_dir
        else Path("audit_artifacts/benchmark_runs") / f"metadata_stable_{utc_stamp()}"
    ).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    data_dir = outdir / "json"
    data_dir.mkdir(exist_ok=True)

    base_env = os.environ.copy()
    base_env.update(
        {
            "YAMS_TESTING": "1",
            "YAMS_DISABLE_VECTORS": "1",
            "YAMS_DISABLE_SESSION_WATCHER": "1",
            "YAMS_SKIP_MODEL_LOADING": "1",
            "YAMS_TEST_SAFE_SINGLE_INSTANCE": "1",
        }
    )

    results: list[CommandResult] = []
    if not args.skip_build:
        results.append(
            run_command(
                "build_metadata_targets",
                [
                    "meson",
                    "compile",
                    "-C",
                    str(build_dir),
                    "-j4",
                    "yams_api_benchmarks",
                    "metadata_contention_bench",
                ],
                outdir,
                base_env,
                timeout_sec=900,
            )
        )
        if results[-1].status != "ok":
            (outdir / "results.json").write_text(
                json.dumps([asdict(r) for r in results], indent=2), encoding="utf-8"
            )
            write_summary(outdir, results, {"api": [], "metadata_matrix": []})
            print(outdir)
            return 1

    api_exe = build_dir / "tests/benchmarks/yams_api_benchmarks"
    metadata_exe = build_dir / "tests/benchmarks/metadata_contention_bench"

    results.append(
        run_command(
            "smoke_api_benchmarks",
            [
                str(api_exe),
                "--quiet",
                "--warmup",
                "0",
                "--iterations",
                "1",
                "--out-dir",
                str(data_dir / "api_smoke"),
            ],
            outdir,
            base_env,
            timeout_sec=120,
            output_path=data_dir / "api_smoke",
        )
    )

    smoke_output = data_dir / "metadata_smoke.jsonl"
    smoke_env = dict(base_env)
    smoke_overrides = {
        "YAMS_BENCH_WRITE_MODE": "mixed",
        "YAMS_BENCH_SEED_DOCS": "100",
        "YAMS_BENCH_WRITER_THREADS": "1",
        "YAMS_BENCH_READER_THREADS": "2",
        "YAMS_BENCH_RUNTIME_S": "1",
        "YAMS_BENCH_REPEAT": "1",
        "YAMS_BENCH_CHECKPOINT_EVERY_MS": "500",
        "YAMS_BENCH_OUTPUT": str(smoke_output),
    }
    smoke_env.update(smoke_overrides)
    results.append(
        run_command(
            "smoke_metadata_contention",
            [str(metadata_exe)],
            outdir,
            smoke_env,
            timeout_sec=120,
            output_path=smoke_output,
            env_overrides=smoke_overrides,
        )
    )

    api_out_dir = data_dir / "api_repeated"
    results.append(
        run_command(
            "api_repeated_iterations",
            [
                str(api_exe),
                "--quiet",
                "--warmup",
                "2",
                "--iterations",
                str(args.api_iterations),
                "--out-dir",
                str(api_out_dir),
            ],
            outdir,
            base_env,
            timeout_sec=args.timeout_sec,
            output_path=api_out_dir,
        )
    )

    matrix: list[dict[str, Any]] = []
    for mode in ["batch_content", "repair_status", "metadata_batch"]:
        for checkpoint in [0, 500, 2000]:
            output = data_dir / f"metadata_{mode}_checkpoint_{checkpoint}.jsonl"
            overrides = {
                "YAMS_BENCH_WRITE_MODE": mode,
                "YAMS_BENCH_SEED_DOCS": "2000",
                "YAMS_BENCH_WRITER_THREADS": "2",
                "YAMS_BENCH_READER_THREADS": "8",
                "YAMS_BENCH_LONG_READERS": "0",
                "YAMS_BENCH_RUNTIME_S": str(args.runtime_sec),
                "YAMS_BENCH_REPEAT": str(args.repeat),
                "YAMS_BENCH_CHECKPOINT_EVERY_MS": str(checkpoint),
                "YAMS_BENCH_OUTPUT": str(output),
            }
            env = dict(base_env)
            env.update(overrides)
            name = f"metadata_{mode}_checkpoint_{checkpoint}"
            results.append(
                run_command(
                    name,
                    [str(metadata_exe)],
                    outdir,
                    env,
                    timeout_sec=args.timeout_sec,
                    output_path=output,
                    env_overrides=overrides,
                )
            )
            records = load_jsonl(output)
            latest = records[-1] if records else {}
            matrix.append(
                {
                    "write_mode": mode,
                    "checkpoint_every_ms": checkpoint,
                    "output": str(output),
                    "top_latencies": latency_summary(latest),
                }
            )

    api_jsonl = api_out_dir / "api_benchmarks.jsonl"
    analysis = {"api": summarize_api(api_jsonl), "metadata_matrix": matrix}
    (outdir / "analysis.json").write_text(
        json.dumps(analysis, indent=2), encoding="utf-8"
    )
    (outdir / "results.json").write_text(
        json.dumps([asdict(r) for r in results], indent=2), encoding="utf-8"
    )
    write_summary(outdir, results, analysis)
    print(outdir)
    failures = [result for result in results if result.status != "ok"]
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
