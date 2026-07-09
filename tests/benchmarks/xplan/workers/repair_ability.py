"""KPI 4 — repair ability via repair_ability_bench binary."""

from __future__ import annotations

import os
from pathlib import Path

from artifacts import raw_worker_output_path, write_json
from workers.ablation import apply_ablation
from workers.base import WorkerContext, WorkerResult
from workers.util import maybe_meson_compile, read_json_file, resolve_binary, run_captured


def run_repair_ability(ctx: WorkerContext) -> WorkerResult:
    explicit = ctx.params.get("binary") or os.environ.get("YAMS_BENCH_REPAIR_BINARY")
    binary = resolve_binary(
        ctx.build_dir,
        "tests",
        "benchmarks",
        "repair_ability_bench",
        explicit=str(explicit) if explicit else None,
    )

    raw_path = raw_worker_output_path(ctx.arm_dir, ctx.step_index, "repair_ability")
    stdout_path = ctx.arm_dir / f"step{ctx.step_index:02d}_repair_ability.stdout.log"
    stderr_path = ctx.arm_dir / f"step{ctx.step_index:02d}_repair_ability.stderr.log"

    fault_kind = str(ctx.params.get("fault_kind") or "embed")
    fault_docs = int(ctx.params.get("fault_docs") or ctx.params.get("corpus_size") or 20)
    doc_size = int(ctx.params.get("doc_size") or 512)

    if ctx.dry_run:
        write_json(
            raw_path,
            {
                "dry_run": True,
                "binary": str(binary),
                "fault_kind": fault_kind,
                "fault_docs": fault_docs,
            },
        )
        return WorkerResult(
            status="ok",
            exit_code=0,
            metrics={
                "faults_injected": float(fault_docs),
                "faults_repaired": 0.0,
                "residual_faults": float(fault_docs),
                "repair_docs_per_s": 0.0,
                "detect_latency_ms": 0.0,
                "repair_wall_ms": 0.0,
            },
            attributes={"dry_run": True, "fault_kind": fault_kind},
            message="dry-run repair_ability",
            raw_path=str(raw_path),
        )

    skip_build = bool(ctx.params.get("skip_build")) or os.environ.get("YAMS_BENCH_SKIP_BUILD") == "1"
    force_build = bool(ctx.params.get("force_build")) or os.environ.get("YAMS_BENCH_FORCE_BUILD") == "1"
    maybe_meson_compile(
        ctx.repo_root,
        ctx.build_dir,
        "bench_repair_ability",
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
            message=f"repair_ability_bench missing: {binary} (meson compile bench_repair_ability)",
        )

    env = os.environ.copy()
    env.update(ctx.env)
    env.setdefault("YAMS_TESTING", "1")
    env.setdefault("YAMS_TEST_SAFE_SINGLE_INSTANCE", "1")
    env.setdefault("YAMS_DISABLE_SESSION_WATCHER", "1")
    env.setdefault("YAMS_SKIP_MODEL_LOADING", "1")
    arm_factors = getattr(getattr(ctx.arm, "arm", None), "factors", None) or {}
    ablation = apply_ablation(env, factors=arm_factors, params=ctx.params)
    env["YAMS_BENCH_FAULT_KIND"] = fault_kind
    env["YAMS_BENCH_FAULT_DOCS"] = str(fault_docs)
    env["YAMS_BENCH_DOC_SIZE"] = str(doc_size)
    env["YAMS_BENCH_OUTPUT"] = str(raw_path)

    timeout = ctx.step.timeout_sec or int(ctx.params.get("timeout_sec") or ctx.arm.plan.timeout_sec)
    try:
        proc = run_captured(
            [str(binary)],
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
            message=f"repair_ability timed out: {exc}",
        )

    raw = read_json_file(raw_path)
    if raw is None and proc.stdout:
        # try parse stdout
        try:
            import json

            raw = json.loads(proc.stdout)
            if isinstance(raw, dict):
                write_json(raw_path, raw)
        except Exception:
            raw = None

    if raw is None:
        return WorkerResult(
            status="failed",
            exit_code=proc.returncode or 1,
            metrics={},
            message="repair_ability produced no JSON metrics",
            raw_path=str(raw_path) if raw_path.exists() else None,
        )

    metrics = {
        "faults_injected": float(raw.get("faults_injected") or 0),
        "faults_repaired": float(raw.get("faults_repaired") or 0),
        "residual_faults": float(raw.get("residual_faults") or 0),
        "repair_docs_per_s": float(raw.get("repair_docs_per_s") or 0),
        "detect_latency_ms": float(raw.get("detect_latency_ms") or raw.get("repair_wall_ms") or 0),
        "repair_wall_ms": float(raw.get("repair_wall_ms") or 0),
        "inject_ms": float(raw.get("inject_ms") or 0),
        "total_succeeded": float(raw.get("total_succeeded") or 0),
        "total_failed": float(raw.get("total_failed") or 0),
        "total_skipped": float(raw.get("total_skipped") or 0),
    }
    status = "ok" if raw.get("status") == "ok" and proc.returncode == 0 else "failed"
    return WorkerResult(
        status=status,
        exit_code=proc.returncode,
        metrics=metrics,
        attributes={
            "binary": str(binary),
            "fault_kind": fault_kind,
            "ablation": ablation,
            "error": raw.get("error"),
            "operation_results": raw.get("operation_results"),
        },
        message=str(raw.get("error") or "repair_ability completed"),
        raw_path=str(raw_path),
    )
