"""KPI 5 — ops-over-time / idle mapping via multi_client idle probe + drain metrics."""

from __future__ import annotations

import json
from pathlib import Path

from workers.base import WorkerContext, WorkerResult
from workers.multi_client import run_multi_client, write_timeline_from_record
from workers.util import read_json_file


def run_ops_timeline(ctx: WorkerContext) -> WorkerResult:
    # Baseline single-client path always runs idle probe after drain — good for idle mapping.
    # Filter by name fragment; Catch2 accepts substring filters.
    result = run_multi_client(
        ctx,
        # Catch2 name filter (wildcard); tags alone collide with other [ingestion] cases.
        catch_filter="*baseline single client*",
        test_name="baseline_single_client",
        env_extra={
            "YAMS_BENCH_NUM_CLIENTS": "1",
            "YAMS_BENCH_IDLE_PROBE": "1",
            "YAMS_BENCH_DOCS_PER_CLIENT": str(
                int(ctx.params.get("load_docs") or ctx.params.get("docs_per_client") or 30)
            ),
        },
    )

    if ctx.dry_run:
        result.metrics = {
            "idle_fraction": 0.0,
            "work_share_rpc": 0.0,
            "work_share_post_ingest": 0.0,
            "work_share_repair": 0.0,
            "work_share_background": 0.0,
            "backlog_peak": 0.0,
            "sample_count": 0.0,
            "idle_work_visible_us": 0.0,
            "drain_elapsed_ms": 0.0,
        }
        # Empty timeline for structure
        (ctx.arm_dir / "timeline.jsonl").write_text(
            json.dumps({"t_ms": 0, "phase": "dry_run"}) + "\n", encoding="utf-8"
        )
        result.status = "ok"
        result.message = "dry-run ops_timeline"
        return result

    result.metrics.setdefault("idle_fraction", 0.0)
    result.metrics.setdefault("sample_count", 0.0)
    result.metrics.setdefault("work_share_rpc", 0.0)
    result.metrics.setdefault("work_share_post_ingest", 0.0)
    result.metrics.setdefault("work_share_repair", 0.0)
    result.metrics.setdefault("work_share_background", 0.0)
    result.metrics.setdefault("backlog_peak", 0.0)

    raw = read_json_file(Path(result.raw_path)) if result.raw_path else None
    if isinstance(raw, dict):
        write_timeline_from_record(ctx.arm_dir, raw)

    return result
