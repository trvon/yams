"""Local Docker/MinIO memory-sync audit worker."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

from artifacts import write_json
from workers.base import WorkerContext, WorkerResult
from workers.util import run_captured

_ALLOWED_LANES = {"filesystem", "s3-persistent", "s3-temporary"}


def _convergence_metrics(evidence_dir: Path) -> dict[str, float]:
    path = evidence_dir / "convergence-lag.jsonl"
    """Convergence metrics from evidence; absent evidence yields no metrics (not 0 ms lag)."""
    if not path.is_file():
        return {}
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        return {"convergence_samples": 0.0}
    return {
        "convergence_samples": float(len(rows)),
        "convergence_lag_max_ms": float(max(row["lag_ms"] for row in rows)),
    }


def run_p2p_memory_sync(ctx: WorkerContext) -> WorkerResult:
    lane = str(ctx.params.get("lane", "filesystem"))
    if lane not in _ALLOWED_LANES:
        return WorkerResult(
            status="failed",
            exit_code=2,
            message=f"unsupported P2P lane: {lane}",
        )

    harness = ctx.repo_root / "tests/benchmarks/xplan/fixtures/p2p/validate_p2p_sync.sh"
    evidence_dir = ctx.arm_dir / "evidence"
    stdout_path = ctx.arm_dir / "p2p.stdout.log"
    stderr_path = ctx.arm_dir / "p2p.stderr.log"

    if ctx.dry_run:
        write_json(
            ctx.arm_dir / "p2p_dry_run.json",
            {"dry_run": True, "harness": str(harness), "lane": lane},
        )
        return WorkerResult(
            status="stub",
            exit_code=0,
            metrics={},
            attributes={"dry_run": True, "lane": lane},
            message=f"dry-run P2P memory-sync lane {lane} (no measurements)",
        )

    if not harness.is_file():
        return WorkerResult(
            status="failed", exit_code=2, message=f"P2P fixture missing: {harness}"
        )
    if shutil.which("docker") is None:
        return WorkerResult(
            status="skipped",
            exit_code=0,
            metrics={},
            attributes={"lane": lane, "prerequisite": "docker"},
            message="Docker is unavailable; local P2P lane not executed",
        )
    compose = subprocess.run(
        ["docker", "compose", "version"],
        cwd=ctx.repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if compose.returncode != 0:
        return WorkerResult(
            status="skipped",
            exit_code=0,
            metrics={},
            attributes={"lane": lane, "prerequisite": "docker compose"},
            message=f"Docker Compose unavailable: {compose.stderr.strip()}",
        )

    evidence_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(ctx.env)
    env["YAMS_P2P_EVIDENCE_DIR"] = str(evidence_dir)
    timeout = ctx.step.timeout_sec or ctx.arm.plan.timeout_sec
    try:
        proc = run_captured(
            ["bash", str(harness), lane],
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
            metrics={"lane_pass": 0.0, **_convergence_metrics(evidence_dir)},
            attributes={"lane": lane, "evidence": str(evidence_dir)},
            message=f"P2P lane timed out: {exc}",
        )

    convergence = _convergence_metrics(evidence_dir)
    metrics = {"lane_pass": 1.0 if proc.returncode == 0 else 0.0, **convergence}
    missing = [
        key
        for key in ("convergence_samples", "convergence_lag_max_ms")
        if key not in convergence
    ]
    return WorkerResult(
        status="ok" if proc.returncode == 0 else "failed",
        exit_code=proc.returncode,
        metrics=metrics,
        attributes={
            "lane": lane,
            "evidence": str(evidence_dir),
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
            **({"missing_metrics": missing} if missing else {}),
        },
        message=f"P2P memory-sync lane {lane} exit={proc.returncode}",
        raw_path=str(stdout_path),
    )
