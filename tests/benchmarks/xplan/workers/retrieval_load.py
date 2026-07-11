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
            "search_p99_ms": 0.0,
            "add_p50_ms": 0.0,
            "add_p95_ms": 0.0,
            "add_p99_ms": 0.0,
            "list_p50_ms": 0.0,
            "list_p95_ms": 0.0,
            "list_p99_ms": 0.0,
            "search_rejected": 0.0,
            "fanout_stage_ms": 0.0,
            "qps": 0.0,
            "add_ops_per_s": 0.0,
            "search_ops_per_s": 0.0,
            "list_ops_per_s": 0.0,
            "total_ops_per_s": 0.0,
            "total_searches": 0.0,
            "backlog_peak": 0.0,
            "post_ingest_inflight_peak": 0.0,
            "write_queue_depth_peak": 0.0,
            "write_in_flight_peak": 0.0,
            "write_max_batch_apply_ms_peak": 0.0,
            "write_max_batch_queue_wait_ms_peak": 0.0,
            "write_max_batch_excess_queue_wait_ms_peak": 0.0,
            "pressure_level_peak": 0.0,
            "db_write_pool_waiting_peak": 0.0,
            "db_read_pool_waiting_peak": 0.0,
            "db_write_pool_slow_holders_delta": 0.0,
            "db_read_pool_slow_holders_delta": 0.0,
            "db_write_pool_max_holder_high_water_ms": 0.0,
            "db_read_pool_max_holder_high_water_ms": 0.0,
            "write_queue_capacity": 0.0,
            "write_queue_depth_max": 0.0,
            "write_queue_capacity_rejections": 0.0,
            "write_queue_forced_over_capacity": 0.0,
            "metadata_wal_bytes_peak": 0.0,
            "write_queue_depth_high_water_delta": 0.0,
            "write_queue_capacity_rejections_delta": 0.0,
            "write_queue_forced_over_capacity_delta": 0.0,
            "metadata_wal_growth_bytes": 0.0,
        }
        result.status = "ok"
        result.message = "dry-run retrieval_load"
        return result

    # Ensure required KPI keys exist even if JSONL shape drifts.
    result.metrics.setdefault("search_p50_ms", 0.0)
    result.metrics.setdefault("search_p95_ms", result.metrics.get("search_p90_ms", 0.0))
    result.metrics.setdefault("search_p99_ms", result.metrics.get("search_p95_ms", 0.0))
    result.metrics.setdefault("add_p50_ms", 0.0)
    result.metrics.setdefault("add_p95_ms", result.metrics.get("add_p50_ms", 0.0))
    result.metrics.setdefault("add_p99_ms", result.metrics.get("add_p95_ms", 0.0))
    result.metrics.setdefault("list_p50_ms", 0.0)
    result.metrics.setdefault("list_p95_ms", result.metrics.get("list_p50_ms", 0.0))
    result.metrics.setdefault("list_p99_ms", result.metrics.get("list_p95_ms", 0.0))
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
    result.metrics.setdefault("db_write_pool_waiting_peak", 0.0)
    result.metrics.setdefault("db_read_pool_waiting_peak", 0.0)
    result.metrics.setdefault("db_write_pool_slow_holders_delta", 0.0)
    result.metrics.setdefault("db_read_pool_slow_holders_delta", 0.0)
    result.metrics.setdefault("db_write_pool_max_holder_high_water_ms", 0.0)
    result.metrics.setdefault("db_read_pool_max_holder_high_water_ms", 0.0)
    result.metrics.setdefault("write_queue_capacity", 0.0)
    result.metrics.setdefault("write_queue_depth_max", 0.0)
    result.metrics.setdefault("write_queue_capacity_rejections", 0.0)
    result.metrics.setdefault("write_queue_forced_over_capacity", 0.0)
    result.metrics.setdefault("metadata_wal_bytes_peak", 0.0)
    result.metrics.setdefault("write_queue_depth_high_water_delta", 0.0)
    result.metrics.setdefault("write_queue_capacity_rejections_delta", 0.0)
    result.metrics.setdefault("write_queue_forced_over_capacity_delta", 0.0)
    result.metrics.setdefault("metadata_wal_growth_bytes", 0.0)
    return result
