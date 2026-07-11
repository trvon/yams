"""KPI 3 — concurrent retrieval load via multi_client mixed workload."""

from __future__ import annotations

from workers.base import WorkerContext, WorkerResult
from workers.multi_client import run_multi_client


def run_retrieval_load(ctx: WorkerContext) -> WorkerResult:
    # Prefer mixed read/write Catch2 case; it emits search_latency percentiles.
    # Catch2 filter matches the [mixed] tag.
    result = run_multi_client(
        ctx,
        catch_filter="[mixed]",
        test_name="mixed_read_write",
        env_extra={
            # Bias toward search pressure for this KPI.
            "YAMS_BENCH_SEARCH_RATIO": str(ctx.params.get("search_ratio", 0.7)),
        },
    )
    if ctx.dry_run:
        result.metrics = {
            "search_p50_ms": 0.0,
            "search_p95_ms": 0.0,
            "search_rejected": 0.0,
            "fanout_stage_ms": 0.0,
            "qps": 0.0,
            "total_searches": 0.0,
            "backlog_peak": 0.0,
            "post_ingest_inflight_peak": 0.0,
            "write_queue_depth_peak": 0.0,
            "write_in_flight_peak": 0.0,
            "write_max_batch_apply_ms_peak": 0.0,
            "write_max_batch_queue_wait_ms_peak": 0.0,
            "write_max_batch_excess_queue_wait_ms_peak": 0.0,
            "pressure_level_peak": 0.0,
        }
        result.status = "ok"
        result.message = "dry-run retrieval_load"
        return result

    # Ensure required KPI keys exist even if JSONL shape drifts.
    result.metrics.setdefault("search_p50_ms", 0.0)
    result.metrics.setdefault("search_p95_ms", result.metrics.get("search_p90_ms", 0.0))
    result.metrics.setdefault("search_rejected", 0.0)
    result.metrics.setdefault("fanout_stage_ms", 0.0)
    result.metrics.setdefault("qps", 0.0)
    result.metrics.setdefault("backlog_peak", 0.0)
    result.metrics.setdefault("post_ingest_inflight_peak", 0.0)
    result.metrics.setdefault("write_queue_depth_peak", 0.0)
    result.metrics.setdefault("write_in_flight_peak", 0.0)
    result.metrics.setdefault("write_max_batch_apply_ms_peak", 0.0)
    result.metrics.setdefault("write_max_batch_queue_wait_ms_peak", 0.0)
    result.metrics.setdefault("write_max_batch_excess_queue_wait_ms_peak", 0.0)
    result.metrics.setdefault("pressure_level_peak", 0.0)
    return result
