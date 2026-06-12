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


@dataclass
class BenchRun:
    name: str
    group: str
    command: list[str]
    env: dict[str, str] = field(default_factory=dict)
    timeout_sec: int = 300
    output_hint: str = ""


@dataclass
class BenchResult:
    name: str
    group: str
    command: str
    timeout_sec: int
    duration_sec: float
    exit_code: int | None
    status: str
    stdout_log: str
    stderr_log: str
    output_hint: str = ""


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run_command(run: BenchRun, outdir: Path, base_env: dict[str, str]) -> BenchResult:
    safe = run.name.replace("/", "_").replace(" ", "_")
    stdout_path = outdir / f"{safe}.stdout.log"
    stderr_path = outdir / f"{safe}.stderr.log"
    env = dict(base_env)
    env.update(run.env)
    start = time.monotonic()
    status = "unknown"
    code: int | None = None
    try:
        proc = subprocess.run(
            run.command,
            cwd=Path.cwd(),
            env=env,
            text=True,
            capture_output=True,
            timeout=run.timeout_sec,
        )
        code = proc.returncode
        status = "ok" if code == 0 else "failed"
        stdout_path.write_text(proc.stdout, encoding="utf-8", errors="replace")
        stderr_path.write_text(proc.stderr, encoding="utf-8", errors="replace")
    except subprocess.TimeoutExpired as exc:
        status = "timeout"
        code = None
        stdout = (
            exc.stdout.decode("utf-8", errors="replace")
            if isinstance(exc.stdout, bytes)
            else (exc.stdout or "")
        )
        stderr = (
            exc.stderr.decode("utf-8", errors="replace")
            if isinstance(exc.stderr, bytes)
            else (exc.stderr or "")
        )
        stdout_path.write_text(stdout, encoding="utf-8", errors="replace")
        stderr_path.write_text(stderr, encoding="utf-8", errors="replace")
    dur = time.monotonic() - start
    return BenchResult(
        name=run.name,
        group=run.group,
        command=" ".join(shlex.quote(x) for x in run.command),
        timeout_sec=run.timeout_sec,
        duration_sec=round(dur, 3),
        exit_code=code,
        status=status,
        stdout_log=str(stdout_path),
        stderr_log=str(stderr_path),
        output_hint=run.output_hint,
    )


def write_markdown(
    results: list[BenchResult], outdir: Path, tracy_status: Path
) -> None:
    lines = [
        "# Benchmark profiling suite run",
        "",
        f"- Run directory: `{outdir}`",
        f"- Tracy status: `{tracy_status}`",
        "",
        "## Results",
        "",
        "| Group | Name | Status | Duration (s) | Exit | Output | Logs |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for r in results:
        logs = f"[`stdout`]({Path(r.stdout_log).name}) / [`stderr`]({Path(r.stderr_log).name})"
        lines.append(
            f"| {r.group} | `{r.name}` | {r.status} | {r.duration_sec:.3f} | "
            f"{'' if r.exit_code is None else r.exit_code} | `{r.output_hint}` | {logs} |"
        )
    lines += [
        "",
        "## Notes",
        "",
        "- `timeout` means the command was bounded by the suite runner, not necessarily broken.",
        "- This smoke suite is for coverage of benchmark tooling and artifact plumbing; use larger repeat counts for optimization decisions.",
        "- If Tracy status is `gui-only`, no `.tracy` file is expected from this automated run.",
    ]
    (outdir / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run bounded YAMS benchmark profiling smoke suite"
    )
    parser.add_argument("--build-dir", default="build/release")
    parser.add_argument("--out-dir", default="")
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()

    build_dir = Path(args.build_dir).resolve()
    outdir = (
        Path(args.out_dir)
        if args.out_dir
        else Path("audit_artifacts/benchmark_runs") / stamp()
    ).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

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

    tracy_status = outdir / "tracy_capture_status.json"
    subprocess.run(
        ["scripts/check_tracy_capture.sh", str(tracy_status)], text=True, check=False
    )

    targets = [
        "bench_write_coordinator",
        "metadata_contention_bench",
        "symspell_bench",
        "ipc_stream_bench",
        "yams_core_benchmarks",
        "yams_search_benchmarks",
        "yams_api_benchmarks",
        "multi_client_ingestion_bench",
        "symbol_extraction_bench",
    ]
    if not args.skip_build:
        build_cmd = ["meson", "compile", "-C", str(build_dir), "-j4", *targets]
        build_run = BenchRun(
            "build_selected_benchmarks", "setup", build_cmd, timeout_sec=900
        )
        build_result = run_command(build_run, outdir, base_env)
        results: list[BenchResult] = [build_result]
        if build_result.status != "ok":
            (outdir / "results.json").write_text(
                json.dumps([asdict(x) for x in results], indent=2)
            )
            write_markdown(results, outdir, tracy_status)
            print(outdir)
            return 1
    else:
        results = []

    bench_results = outdir / "json"
    bench_results.mkdir(exist_ok=True)

    runs = [
        BenchRun(
            "write_coordinator_bench",
            "metadata/write-path",
            ["scripts/run_tracy_benchmark.sh", "write_coordinator_bench"],
            env={
                "YAMS_BENCH_NUM_FILES": "2",
                "YAMS_BENCH_FILE_SIZE_BYTES": "128",
                "YAMS_BENCH_VERSION_ITERATIONS": "1",
                "YAMS_BENCH_REPEAT": "1",
                "YAMS_BENCH_OUTPUT": str(bench_results / "write_coordinator.jsonl"),
            },
            timeout_sec=args.timeout,
            output_hint=str(bench_results / "write_coordinator.jsonl"),
        ),
        BenchRun(
            "metadata_contention_bench",
            "metadata/write-path",
            [
                "meson",
                "test",
                "-C",
                str(build_dir),
                "metadata_contention_bench",
                "--print-errorlogs",
            ],
            env={
                "YAMS_BENCH_SEED_DOCS": "50",
                "YAMS_BENCH_WRITER_THREADS": "1",
                "YAMS_BENCH_READER_THREADS": "2",
                "YAMS_BENCH_LONG_READERS": "0",
                "YAMS_BENCH_RUNTIME_S": "1",
                "YAMS_BENCH_REPEAT": "1",
                "YAMS_BENCH_OUTPUT": str(bench_results / "metadata_contention.jsonl"),
            },
            timeout_sec=args.timeout,
            output_hint=str(bench_results / "metadata_contention.jsonl"),
        ),
        BenchRun(
            "symspell_bench",
            "vector/fuzzy",
            [
                "meson",
                "test",
                "-C",
                str(build_dir),
                "symspell_bench",
                "--print-errorlogs",
            ],
            timeout_sec=args.timeout,
        ),
        BenchRun(
            "ipc_stream_bench",
            "core/ipc/storage",
            [
                str(build_dir / "tests/benchmarks/ipc_stream_bench"),
                "--quiet",
                "--iterations",
                "1",
                "--output",
                str(bench_results / "ipc_stream.json"),
            ],
            timeout_sec=args.timeout,
            output_hint=str(bench_results / "ipc_stream.json"),
        ),
        BenchRun(
            "yams_core_benchmarks",
            "other/core",
            [
                str(build_dir / "tests/benchmarks/yams_core_benchmarks"),
                "--quiet",
                "--warmup",
                "0",
                "--iterations",
                "1",
                "--out-dir",
                str(bench_results / "core"),
            ],
            timeout_sec=args.timeout,
            output_hint=str(bench_results / "core"),
        ),
        BenchRun(
            "yams_search_benchmarks",
            "search/retrieval",
            [
                str(build_dir / "tests/benchmarks/yams_search_benchmarks"),
                "--quiet",
                "--warmup",
                "0",
                "--iterations",
                "1",
                "--out-dir",
                str(bench_results / "search"),
            ],
            timeout_sec=args.timeout,
            output_hint=str(bench_results / "search"),
        ),
        BenchRun(
            "yams_api_benchmarks",
            "other/api",
            [
                str(build_dir / "tests/benchmarks/yams_api_benchmarks"),
                "--quiet",
                "--warmup",
                "0",
                "--iterations",
                "1",
                "--out-dir",
                str(bench_results / "api"),
            ],
            timeout_sec=args.timeout,
            output_hint=str(bench_results / "api"),
        ),
        BenchRun(
            "multi_client_ingestion_bench",
            "ingestion/pipeline",
            [
                "meson",
                "test",
                "-C",
                str(build_dir),
                "multi_client_ingestion_bench",
                "--print-errorlogs",
            ],
            timeout_sec=args.timeout,
        ),
        BenchRun(
            "symbol_extraction_bench",
            "extraction/plugins",
            [
                "meson",
                "test",
                "-C",
                str(build_dir),
                "symbol_extraction_bench",
                "--print-errorlogs",
            ],
            timeout_sec=args.timeout,
        ),
    ]

    for run in runs:
        print(f"running {run.name}...", flush=True)
        results.append(run_command(run, outdir, base_env))

    (outdir / "results.json").write_text(
        json.dumps([asdict(x) for x in results], indent=2), encoding="utf-8"
    )
    write_markdown(results, outdir, tracy_status)
    print(outdir)
    failures = [r for r in results if r.status not in {"ok"}]
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
