"""KPI 5 — ops-over-time / idle mapping via multi_client idle probe + drain metrics."""

from __future__ import annotations

import json
from pathlib import Path

from workers.base import WorkerContext, WorkerResult
from workers.multi_client import run_multi_client, write_timeline_from_record
from workers.util import read_json_file


REQUIRED_METRICS = (
    "idle_fraction",
    "sample_count",
    "work_share_rpc",
    "work_share_post_ingest",
    "work_share_repair",
    "work_share_background",
    "backlog_peak",
)


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
        # A dry run measured nothing: report no metrics rather than a fabricated idle/zero row.
        (ctx.arm_dir / "timeline.jsonl").write_text(
            json.dumps({"t_ms": 0, "phase": "dry_run"}) + "\n", encoding="utf-8"
        )
        result.metrics = {}
        result.status = "stub"
        result.message = "dry-run ops_timeline (no measurements)"
        return result

    # Never default a missing measurement to 0.0: a zero idle_fraction / backlog_peak is a
    # real (and flattering) observation. Missing means the probe did not report it.
    missing = [key for key in REQUIRED_METRICS if key not in result.metrics]
    if missing:
        result.attributes["missing_metrics"] = missing
        if result.status == "ok" and result.exit_code == 0:
            result.status = "failed"
            result.exit_code = 1
        result.message = (
            f"{result.message} | missing metrics: {', '.join(missing)}".lstrip(" |")
        )

    raw = read_json_file(Path(result.raw_path)) if result.raw_path else None
    if isinstance(raw, dict):
        write_timeline_from_record(ctx.arm_dir, raw)

    return result
