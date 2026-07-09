"""Run a legacy shell script as an xplan arm (migration bridge)."""

from __future__ import annotations

import os
import shlex
from pathlib import Path

from artifacts import write_json
from workers.base import WorkerContext, WorkerResult
from workers.util import run_captured


def run_external_script(ctx: WorkerContext) -> WorkerResult:
    script = ctx.params.get("script")
    if not script:
        return WorkerResult(
            status="failed",
            exit_code=2,
            metrics={},
            message="external_script worker requires params.script",
        )

    script_path = Path(str(script))
    if not script_path.is_absolute():
        script_path = (ctx.repo_root / script_path).resolve()

    if ctx.dry_run:
        write_json(
            ctx.arm_dir / "metrics_external.json",
            {"dry_run": True, "script": str(script_path)},
        )
        return WorkerResult(
            status="ok",
            exit_code=0,
            metrics={"script_ran": 0.0},
            attributes={"dry_run": True, "script": str(script_path)},
            message=f"dry-run external_script {script_path.name}",
        )

    if not script_path.is_file():
        return WorkerResult(
            status="failed",
            exit_code=2,
            metrics={},
            message=f"script not found: {script_path}",
        )

    env = os.environ.copy()
    env.update(ctx.env)
    # Point legacy scripts at this arm directory when they honor OUT / YAMS_*_OUT.
    env.setdefault("YAMS_XPLAN_ARM_DIR", str(ctx.arm_dir))
    env.setdefault("YAMS_BENCH_OUT_DIR", str(ctx.arm_dir))
    for key in (
        "YAMS_TOPOLOGY_EXPANSION_OUT",
        "YAMS_TOPOLOGY_SOURCE_OUT",
        "YAMS_TOPOLOGY_CLUSTER_OUT",
        "YAMS_TOPOLOGY_ROUTE_OUT",
    ):
        env.setdefault(key, str(ctx.arm_dir))

    # Map common factor names onto harness env expected by legacy scripts.
    factor_env_map = {
        "topology_source": "YAMS_BENCH_TOPOLOGY_SOURCE",
        "search_engine_variant": "YAMS_SEARCH_ENGINE_VARIANT",
        "expansion_arms": "YAMS_TOPOLOGY_EXPANSION_ARMS",
        "expansion_arm": "YAMS_TOPOLOGY_EXPANSION_ARMS",
    }
    for factor_key, env_key in factor_env_map.items():
        if factor_key in ctx.params and env_key not in env:
            env[env_key] = str(ctx.params[factor_key])
    if "topology_source" in ctx.params:
        env.setdefault("YAMS_BENCH_SEED_SEMANTIC_NEIGHBORS", "1")

    extra_args = ctx.params.get("args") or []
    if isinstance(extra_args, str):
        extra_args = shlex.split(extra_args)

    cmd = ["bash", str(script_path), *[str(a) for a in extra_args]]
    stdout_path = ctx.arm_dir / "external_script.stdout.log"
    stderr_path = ctx.arm_dir / "external_script.stderr.log"
    timeout = ctx.step.timeout_sec or int(ctx.params.get("timeout_sec") or ctx.arm.plan.timeout_sec)

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
            message=f"external_script timed out: {exc}",
        )

    # Best-effort: if script wrote summary.txt or metrics.json, note it.
    summary = ctx.arm_dir / "summary.txt"
    metrics = {
        "script_ran": 1.0,
        "exit_code": float(proc.returncode),
        "has_summary_txt": 1.0 if summary.is_file() else 0.0,
    }
    return WorkerResult(
        status="ok" if proc.returncode == 0 else "failed",
        exit_code=proc.returncode,
        metrics=metrics,
        attributes={"script": str(script_path), "stdout": str(stdout_path)},
        message=f"external_script {script_path.name} exit={proc.returncode}",
    )
