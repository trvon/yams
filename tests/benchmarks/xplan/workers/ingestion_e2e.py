"""Adapter worker for tests/benchmarks/search/ingestion_e2e_bench."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from artifacts import raw_worker_output_path, write_json
from workers.ablation import apply_ablation
from workers.base import WorkerContext, WorkerResult


def _phase_total_ms(block: dict[str, Any] | None, key: str) -> float:
    if not isinstance(block, dict):
        return 0.0
    entry = block.get(key) or {}
    if not isinstance(entry, dict):
        return 0.0
    return float(entry.get("total_ms") or 0.0)


def _probe_map(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    probes = raw.get("search_impact") or []
    out: dict[str, dict[str, Any]] = {}
    if isinstance(probes, list):
        for probe in probes:
            if isinstance(probe, dict) and probe.get("name"):
                out[str(probe["name"])] = probe
    return out


def extract_metrics(raw: dict[str, Any]) -> dict[str, Any]:
    cfg = raw.get("test_config") or {}
    status = raw.get("pipeline_status") or {}
    queues = raw.get("queues") or {}
    post = raw.get("post_ingest_phase_timings") or {}
    doc_store = raw.get("document_store_phase_timings") or {}
    probes = _probe_map(raw)
    keyword = probes.get("keyword") or {}
    semantic = probes.get("semantic") or {}
    graph = probes.get("graph_rerank_hybrid") or {}

    return {
        "total_duration_ms": float(raw.get("total_duration_ms") or 0),
        "docs_per_s": float(raw.get("throughput_docs_per_sec") or 0),
        "pipeline_complete": 1.0 if status.get("complete") else 0.0,
        "kg_enabled": 1.0 if cfg.get("kg_enabled") else 0.0,
        "expected_kg": float(status.get("expected_kg") or 0),
        "observed_kg": float(status.get("observed_kg") or 0),
        "expected_embed": float(status.get("expected_embed") or 0),
        "observed_embed": float(status.get("observed_embed") or 0),
        "expected_post": float(status.get("expected_post") or 0),
        "observed_post": float(status.get("observed_post") or 0),
        "dropped_batches": float(queues.get("dropped_batches") or 0),
        "extract_ms": _phase_total_ms(post, "prepare_metadata_entry"),
        "kg_ms": _phase_total_ms(post, "kg_graph_component"),
        "dispatch_ms": _phase_total_ms(post, "dispatch_successes"),
        "embed_prepare_ms": _phase_total_ms(post, "dispatch_embed_prepare"),
        "document_store_total_ms": _phase_total_ms(doc_store, "document_store_total")
        if "document_store_total" in (doc_store or {})
        else float(
            sum(
                float((v or {}).get("total_ms") or 0)
                for v in (doc_store or {}).values()
                if isinstance(v, dict)
            )
        ),
        "keyword_ms": float(keyword.get("wall_ms") or 0),
        "keyword_total": float(keyword.get("total_count") or 0),
        "semantic_ms": float(semantic.get("wall_ms") or 0),
        "semantic_total": float(semantic.get("total_count") or 0),
        "graph_ms": float(graph.get("wall_ms") or 0),
        "graph_total": float(graph.get("total_count") or 0),
    }


def _resolve_binary(ctx: WorkerContext) -> Path:
    explicit = ctx.params.get("binary") or os.environ.get("YAMS_BENCH_BINARY")
    if explicit:
        return Path(str(explicit))
    return ctx.build_dir / "tests" / "benchmarks" / "ingestion_e2e_bench"


def _maybe_build(ctx: WorkerContext, binary: Path) -> None:
    if binary.exists() and os.access(binary, os.X_OK):
        return
    if ctx.params.get("skip_build") or os.environ.get("YAMS_BENCH_SKIP_BUILD") == "1":
        return
    target = str(ctx.params.get("meson_target") or "bench_ingestion_e2e")
    subprocess.run(
        ["meson", "compile", "-C", str(ctx.build_dir), "-j4", target],
        check=False,
        cwd=str(ctx.repo_root),
    )


def run_ingestion_e2e(ctx: WorkerContext) -> WorkerResult:
    binary = _resolve_binary(ctx)
    raw_path = raw_worker_output_path(ctx.arm_dir, ctx.step_index, "ingestion_e2e")

    if ctx.dry_run:
        metrics = {
            "total_duration_ms": 0.0,
            "docs_per_s": 0.0,
            "pipeline_complete": 0.0,
            "dropped_batches": 0.0,
        }
        write_json(
            raw_path,
            {"dry_run": True, "binary": str(binary), "params": ctx.params},
        )
        return WorkerResult(
            status="ok",
            exit_code=0,
            metrics=metrics,
            attributes={"dry_run": True, "binary": str(binary)},
            message="dry-run: skipped ingestion_e2e execution",
            raw_path=str(raw_path),
        )

    _maybe_build(ctx, binary)
    if not binary.exists():
        return WorkerResult(
            status="failed",
            exit_code=2,
            metrics={},
            attributes={"binary": str(binary)},
            message=f"ingestion_e2e binary not found: {binary}",
        )

    env = os.environ.copy()
    env.update(ctx.env)
    env.setdefault("YAMS_TEST_SAFE_SINGLE_INSTANCE", "1")

    arm_factors = getattr(getattr(ctx.arm, "arm", None), "factors", None) or {}
    ablation = apply_ablation(env, factors=arm_factors, params=ctx.params)

    # Prefer explicit harness env (shell wrappers), then plan params.
    corpus_size = int(
        env.get("YAMS_BENCH_CORPUS_SIZE") or ctx.params.get("corpus_size") or 100
    )
    doc_size = int(env.get("YAMS_BENCH_DOC_SIZE") or ctx.params.get("doc_size") or 1000)
    poll_ms = int(
        env.get("YAMS_BENCH_POLL_INTERVAL_MS") or ctx.params.get("poll_interval_ms") or 100
    )

    env["YAMS_BENCH_CORPUS_SIZE"] = str(corpus_size)
    env["YAMS_BENCH_DOC_SIZE"] = str(doc_size)
    env["YAMS_BENCH_POLL_INTERVAL_MS"] = str(poll_ms)
    env["YAMS_BENCH_OUTPUT"] = str(raw_path)

    timeout = ctx.step.timeout_sec
    if timeout is None:
        timeout = int(ctx.params.get("timeout_sec") or ctx.arm.plan.timeout_sec)

    stdout_path = ctx.arm_dir / f"step{ctx.step_index:02d}_ingestion_e2e.stdout.log"
    stderr_path = ctx.arm_dir / f"step{ctx.step_index:02d}_ingestion_e2e.stderr.log"

    try:
        proc = subprocess.run(
            [str(binary)],
            cwd=str(ctx.repo_root),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout_path.write_text(exc.stdout or "", encoding="utf-8", errors="replace")
        stderr_path.write_text(exc.stderr or "", encoding="utf-8", errors="replace")
        return WorkerResult(
            status="failed",
            exit_code=124,
            metrics={},
            attributes={"binary": str(binary), "timeout_sec": timeout},
            message=f"ingestion_e2e timed out after {timeout}s",
            raw_path=str(raw_path) if raw_path.exists() else None,
        )

    stdout_path.write_text(proc.stdout or "", encoding="utf-8", errors="replace")
    stderr_path.write_text(proc.stderr or "", encoding="utf-8", errors="replace")

    if proc.returncode != 0:
        return WorkerResult(
            status="failed",
            exit_code=proc.returncode,
            metrics={},
            attributes={"binary": str(binary)},
            message=f"ingestion_e2e exited {proc.returncode}",
            raw_path=str(raw_path) if raw_path.exists() else None,
        )

    if not raw_path.exists():
        # Some older builds printed JSON to stdout only.
        if proc.stdout and proc.stdout.strip().startswith("{"):
            raw_path.write_text(proc.stdout, encoding="utf-8")
        else:
            return WorkerResult(
                status="failed",
                exit_code=1,
                metrics={},
                attributes={"binary": str(binary)},
                message=f"missing worker raw output: {raw_path}",
            )

    try:
        raw = json.loads(raw_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return WorkerResult(
            status="failed",
            exit_code=1,
            metrics={},
            message=f"invalid JSON from ingestion_e2e: {exc}",
            raw_path=str(raw_path),
        )

    metrics = extract_metrics(raw if isinstance(raw, dict) else {})
    # Compatibility copy for older ablation summary path names.
    legacy_copy = ctx.arm_dir / f"{ctx.arm.arm.safe_name}.json"
    try:
        shutil.copyfile(raw_path, legacy_copy)
    except OSError:
        pass

    return WorkerResult(
        status="ok",
        exit_code=0,
        metrics=metrics,
        attributes={
            "binary": str(binary),
            "corpus_size": corpus_size,
            "doc_size": doc_size,
            "ablation": ablation,
            "semantic_skip": (_probe_map(raw).get("semantic") or {}).get("skip_reason"),
            "graph_skip": (_probe_map(raw).get("graph_rerank_hybrid") or {}).get(
                "skip_reason"
            ),
        },
        message="ingestion_e2e completed",
        raw_path=str(raw_path),
    )
