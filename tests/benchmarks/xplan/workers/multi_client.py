"""Adapter around multi_client_ingestion_bench (Catch2)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from artifacts import raw_worker_output_path, write_json
from workers.ablation import apply_ablation
from workers.base import WorkerContext, WorkerResult
from workers.util import (
    last_jsonl_matching,
    latency_ms_from_us_block,
    maybe_meson_compile,
    nested_get,
    resolve_binary,
    run_captured,
)


def _binary(ctx: WorkerContext) -> Path:
    explicit = ctx.params.get("binary") or os.environ.get("YAMS_BENCH_MULTI_CLIENT_BINARY")
    return resolve_binary(
        ctx.build_dir,
        "tests",
        "benchmarks",
        "multi_client_ingestion_bench",
        explicit=str(explicit) if explicit else None,
    )


def run_multi_client(
    ctx: WorkerContext,
    *,
    catch_filter: str,
    test_name: str,
    env_extra: dict[str, str] | None = None,
) -> WorkerResult:
    """Run a Catch2-filtered multi_client case and return raw JSONL record + status."""
    binary = _binary(ctx)
    raw_path = raw_worker_output_path(ctx.arm_dir, ctx.step_index, "multi_client")
    jsonl_path = ctx.arm_dir / f"step{ctx.step_index:02d}_multi_client.jsonl"
    stdout_path = ctx.arm_dir / f"step{ctx.step_index:02d}_multi_client.stdout.log"
    stderr_path = ctx.arm_dir / f"step{ctx.step_index:02d}_multi_client.stderr.log"

    if ctx.dry_run:
        write_json(
            raw_path,
            {"dry_run": True, "binary": str(binary), "catch_filter": catch_filter},
        )
        return WorkerResult(
            status="ok",
            exit_code=0,
            metrics={},
            attributes={"dry_run": True, "binary": str(binary), "catch_filter": catch_filter},
            message=f"dry-run multi_client filter={catch_filter}",
            raw_path=str(raw_path),
        )

    skip_build = bool(ctx.params.get("skip_build")) or os.environ.get("YAMS_BENCH_SKIP_BUILD") == "1"
    force_build = bool(ctx.params.get("force_build")) or os.environ.get("YAMS_BENCH_FORCE_BUILD") == "1"
    maybe_meson_compile(
        ctx.repo_root,
        ctx.build_dir,
        "bench_multi_client",
        skip=skip_build,
        binary=binary,
        force=force_build,
    )
    if not binary.exists():
        return WorkerResult(
            status="failed",
            exit_code=2,
            metrics={},
            attributes={"binary": str(binary)},
            message=f"multi_client binary missing: {binary}",
        )

    env = os.environ.copy()
    env.update(ctx.env)
    env.setdefault("YAMS_TESTING", "1")
    env.setdefault("YAMS_TEST_SAFE_SINGLE_INSTANCE", "1")
    env.setdefault("YAMS_DISABLE_SESSION_WATCHER", "1")
    env.setdefault("YAMS_SKIP_MODEL_LOADING", "1")
    # Default vectors off for load speed unless ablation sets vectors=on.
    env.setdefault("YAMS_DISABLE_VECTORS", "1")
    env["YAMS_BENCH_OUTPUT"] = str(jsonl_path)

    arm_factors = getattr(getattr(ctx.arm, "arm", None), "factors", None) or {}
    ablation = apply_ablation(env, factors=arm_factors, params=ctx.params)
    # If ablation wants vectors on, clear the default disable.
    if ablation.get("axes", {}).get("vectors") == "on":
        env.pop("YAMS_DISABLE_VECTORS", None)

    # Workload knobs from plan params / factor values.
    clients = ctx.params.get("search_clients") or ctx.params.get("num_clients") or 4
    docs = ctx.params.get("docs_per_client") or ctx.params.get("corpus_size") or 25
    doc_size = ctx.params.get("doc_size") or ctx.params.get("doc_size_bytes") or 1024
    search_ratio = ctx.params.get("search_ratio", 0.5)
    warmup = ctx.params.get("warmup_docs", 20)
    search_type = (
        env.get("YAMS_BENCH_SEARCH_TYPE")
        or ctx.params.get("search_type")
        or "hybrid"
    )

    env["YAMS_BENCH_NUM_CLIENTS"] = str(int(clients))
    env["YAMS_BENCH_DOCS_PER_CLIENT"] = str(int(docs))
    env["YAMS_BENCH_DOC_SIZE_BYTES"] = str(int(doc_size))
    env["YAMS_BENCH_SEARCH_RATIO"] = str(float(search_ratio))
    env["YAMS_BENCH_WARMUP_DOCS"] = str(int(warmup))
    env["YAMS_BENCH_SEARCH_TYPE"] = str(search_type)
    env["YAMS_BENCH_MIXED_SEARCHERS"] = str(int(clients))
    env["YAMS_BENCH_MIXED_WRITERS"] = str(max(1, int(int(clients) // 2)))
    env["YAMS_BENCH_MIXED_READER_OPS"] = str(int(ctx.params.get("mixed_reader_ops") or 50))
    env["YAMS_BENCH_IDLE_PROBE"] = str(ctx.params.get("idle_probe", "1"))

    if env_extra:
        env.update({k: str(v) for k, v in env_extra.items()})

    timeout = ctx.step.timeout_sec or int(ctx.params.get("timeout_sec") or ctx.arm.plan.timeout_sec)
    # Catch2: run only the requested tag/name; allow benchmark-tagged cases.
    cmd = [str(binary), catch_filter, "--reporter", "compact"]

    try:
        proc = run_captured(
            cmd,
            cwd=ctx.repo_root,
            env=env,
            timeout=timeout,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )
    except Exception as exc:  # noqa: BLE001
        return WorkerResult(
            status="failed",
            exit_code=124,
            metrics={},
            attributes={"binary": str(binary), "catch_filter": catch_filter},
            message=f"multi_client timed out/failed: {exc}",
        )

    record = last_jsonl_matching(jsonl_path, test_name=test_name)
    if record is None:
        # Fall back to any record
        record = last_jsonl_matching(jsonl_path)
    write_json(raw_path, record or {"exit_code": proc.returncode, "stdout": stdout_path.name})

    if proc.returncode != 0 and record is None:
        return WorkerResult(
            status="failed",
            exit_code=proc.returncode,
            metrics={},
            attributes={"binary": str(binary), "catch_filter": catch_filter},
            message=f"multi_client exited {proc.returncode} without JSONL metrics",
            raw_path=str(raw_path),
        )

    return WorkerResult(
        status="ok" if proc.returncode == 0 else "failed",
        exit_code=proc.returncode,
        metrics=_metrics_from_record(record or {}),
        attributes={
            "binary": str(binary),
            "catch_filter": catch_filter,
            "test": (record or {}).get("test"),
            "jsonl": str(jsonl_path),
            "ablation": ablation,
            "search_type": search_type,
        },
        message="multi_client completed" if proc.returncode == 0 else f"exit {proc.returncode}",
        raw_path=str(raw_path),
    )


def _metrics_from_record(record: dict[str, Any]) -> dict[str, Any]:
    search = record.get("search_latency") or nested_get(record, "search", "latency") or {}
    add = record.get("add_latency") or {}
    idle = record.get("idle_probe") or {}
    idle_base = idle.get("idle_baseline") if isinstance(idle, dict) else {}
    drain = record.get("drain_metrics") or {}
    peaks = record.get("resource_peaks") or {}

    metrics: dict[str, Any] = {
        "elapsed_seconds": float(record.get("elapsed_seconds") or 0),
        "docs_per_s": float(record.get("throughput_docs_per_sec") or 0),
        "total_searches": float(record.get("total_searches") or 0),
        "total_adds": float(record.get("total_adds") or 0),
        "total_failures": float(record.get("total_failures") or 0),
        "num_clients": float(record.get("num_clients") or 0),
        "drained": 1.0 if record.get("drained") else 0.0,
    }

    p50 = latency_ms_from_us_block(search if isinstance(search, dict) else None, "p50_us")
    p90 = latency_ms_from_us_block(search if isinstance(search, dict) else None, "p90_us")
    p95 = latency_ms_from_us_block(search if isinstance(search, dict) else None, "p95_us")
    if p50 is not None:
        metrics["search_p50_ms"] = p50
    if p90 is not None:
        metrics["search_p90_ms"] = p90
    if p95 is not None:
        metrics["search_p95_ms"] = p95
    elif p90 is not None:
        # multi_client emits p90; map to p95 slot when p95 absent
        metrics["search_p95_ms"] = p90

    add_p50 = latency_ms_from_us_block(add if isinstance(add, dict) else None, "p50_us")
    if add_p50 is not None:
        metrics["add_p50_ms"] = add_p50

    metrics["search_rejected"] = float(record.get("search_rejected") or 0)
    if metrics["total_searches"] > 0 and metrics["elapsed_seconds"] > 0:
        metrics["qps"] = metrics["total_searches"] / metrics["elapsed_seconds"]
    else:
        metrics["qps"] = 0.0

    # Idle / ops timeline fields
    if isinstance(idle_base, dict):
        metrics["idle_mean_cpu_pct"] = float(idle_base.get("meanCpuPercent") or idle_base.get("mean_cpu_percent") or 0)
        metrics["idle_peak_cpu_pct"] = float(idle_base.get("peakCpuPercent") or idle_base.get("peak_cpu_percent") or 0)
        samples = float(idle_base.get("samples") or 0)
        metrics["sample_count"] = samples
        # Proxy idle fraction: inverse of mean CPU vs a 100% busy baseline (clamped).
        mean_cpu = metrics["idle_mean_cpu_pct"]
        metrics["idle_fraction"] = max(0.0, min(1.0, 1.0 - (mean_cpu / 100.0)))
    if isinstance(idle, dict):
        metrics["idle_work_visible_us"] = float(idle.get("work_visible_us") or idle.get("workVisibleUs") or 0)
        metrics["idle_add_latency_us"] = float(idle.get("add_latency_us") or idle.get("addLatencyUs") or 0)
        metrics["idle_probe_attempted"] = 1.0 if idle.get("attempted") else 0.0
    if isinstance(drain, dict):
        metrics["backlog_peak"] = float(
            drain.get("peakPostIngestQueued") or drain.get("peak_post_ingest_queued") or 0
        )
        metrics["drain_elapsed_ms"] = float(drain.get("elapsedMs") or drain.get("elapsed_ms") or 0)
    if isinstance(peaks, dict):
        metrics["backlog_peak"] = max(
            metrics.get("backlog_peak", 0.0),
            float(peaks.get("peak_post_ingest_queued") or 0),
        )
        metrics["post_ingest_inflight_peak"] = float(
            peaks.get("peak_post_ingest_inflight") or 0
        )
        metrics["write_queue_depth_peak"] = float(
            peaks.get("peak_write_queue_depth") or 0
        )
        metrics["write_in_flight_peak"] = float(peaks.get("peak_write_in_flight") or 0)
        metrics["write_max_batch_apply_ms_peak"] = float(
            peaks.get("peak_write_max_batch_apply_ms") or 0
        )
        metrics["write_max_batch_queue_wait_ms_peak"] = float(
            peaks.get("peak_write_max_batch_queue_wait_ms") or 0
        )
        metrics["write_max_batch_excess_queue_wait_ms_peak"] = float(
            peaks.get("peak_write_max_batch_excess_queue_wait_ms") or 0
        )
        metrics["pressure_level_peak"] = float(peaks.get("max_pressure_level") or 0)

    # Work-share proxies from final snapshot if present
    snap = record.get("daemon_snapshot_final") or record.get("daemon_snapshot") or {}
    if isinstance(snap, dict):
        metrics["work_share_post_ingest"] = float(
            snap.get("postIngestQueued") or snap.get("post_ingest_queued") or 0
        )
        metrics["work_share_rpc"] = float(snap.get("searchActive") or snap.get("search_active") or 0)
        metrics["work_share_repair"] = float(
            snap.get("repairQueueDepth") or snap.get("repair_queue_depth") or 0
        )
        metrics["work_share_background"] = float(
            snap.get("fts5Queued") or snap.get("fts5_queued") or 0
        )

    metrics["fanout_stage_ms"] = float(record.get("fanout_stage_ms") or 0)
    return metrics


def write_timeline_from_record(arm_dir: Path, record: dict[str, Any]) -> Path | None:
    """Materialize a coarse timeline.jsonl from multi_client time series if present."""
    series = record.get("time_series") or record.get("timeseries") or []
    path = arm_dir / "timeline.jsonl"
    if not isinstance(series, list) or not series:
        # Synthesize a short timeline from idle probe + drain if available.
        rows = []
        idle = record.get("idle_probe") or {}
        if isinstance(idle, dict) and idle.get("attempted"):
            base = idle.get("idle_baseline") or {}
            rows.append(
                {
                    "t_ms": 0,
                    "phase": "idle_baseline",
                    "mean_cpu_pct": base.get("meanCpuPercent") or base.get("mean_cpu_percent"),
                    "peak_cpu_pct": base.get("peakCpuPercent") or base.get("peak_cpu_percent"),
                }
            )
            rows.append(
                {
                    "t_ms": idle.get("work_visible_us", 0),
                    "phase": "work_visible",
                    "add_latency_us": idle.get("add_latency_us"),
                }
            )
        if not rows:
            return None
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json_dumps(row) + "\n")
        return path

    with path.open("w", encoding="utf-8") as fh:
        for row in series:
            if isinstance(row, dict):
                fh.write(json_dumps(row) + "\n")
    return path


def json_dumps(obj: Any) -> str:
    import json

    return json.dumps(obj, sort_keys=True)
