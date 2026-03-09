#!/usr/bin/env python3
"""
Summarize multi-client benchmark JSONL artifacts.

Input records are produced by tests/benchmarks/multi_client_ingestion_bench.cpp.
This tool computes KPI aggregates needed for iterative optimization loops.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.fmean(values))


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    idx = min(len(sorted_values) - 1, math.floor(pct * len(sorted_values)))
    return float(sorted_values[idx])


def _to_summary(values: list[float], unit: str) -> dict[str, Any]:
    if not values:
        return {
            "count": 0,
            "unit": unit,
            "mean": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    return {
        "count": len(values),
        "unit": unit,
        "mean": _mean(values),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "min": float(min(values)),
        "max": float(max(values)),
    }


def _rate(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_records(paths: list[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Input JSONL not found: {path}")
        with path.open("r", encoding="utf-8") as handle:
            for lineno, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{lineno}: invalid JSON: {exc}") from exc
                if isinstance(row, dict):
                    row["_source_file"] = str(path)
                    records.append(row)
    return records


def summarize(records: list[dict[str, Any]], input_paths: list[Path]) -> dict[str, Any]:
    large_corpus: dict[str, dict[str, list[float]]] = {}
    large_corpus_churn: dict[str, dict[str, list[float]]] = {}
    large_usage_profiles: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    large_churn_usage_profiles: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    contention_metrics: dict[str, list[float]] = {
        "throughput_ops_per_sec": [],
        "fail_rate": [],
        "retry_after_rate": [],
        "latency_p95_ms": [],
        "success_rate": [],
    }

    case_buckets: dict[
        tuple[str, str, str, str, str, str, int], dict[str, list[float]]
    ] = {}

    def get_large_bucket(
        target: dict[str, dict[str, list[float]]], request_path: str
    ) -> dict[str, list[float]]:
        if request_path not in target:
            target[request_path] = {
                "ops_per_sec": [],
                "fail_rate": [],
                "connection_drop_rate": [],
                "document_not_found_rate": [],
                "content_not_found_rate": [],
                "invalid_hash_quarantined": [],
                "search_p95_ms": [],
                "list_p95_ms": [],
                "grep_p95_ms": [],
                "status_p95_ms": [],
                "get_p95_ms": [],
                "cat_p95_ms": [],
                "search_fail_rate": [],
                "list_fail_rate": [],
                "grep_fail_rate": [],
                "status_fail_rate": [],
                "get_fail_rate": [],
                "cat_fail_rate": [],
                "search_ops_per_sec": [],
                "list_ops_per_sec": [],
                "grep_ops_per_sec": [],
                "status_ops_per_sec": [],
                "get_ops_per_sec": [],
                "cat_ops_per_sec": [],
                "op_trace_count": [],
                "grep_server_execution_time_ms_p95": [],
                "grep_server_phase_worker_scan_ms_p95": [],
                "grep_server_content_retrieval_ms_p95": [],
                "grep_server_worker_critical_total_ms_p95": [],
                "grep_server_worker_critical_content_retrieval_ms_p95": [],
                "grep_server_worker_critical_regex_scan_ms_p95": [],
                "grep_server_worker_critical_other_ms_p95": [],
                "grep_server_docs_scanned_mean": [],
                "grep_server_lines_scanned_mean": [],
                "grep_server_bytes_scanned_mean": [],
                "search_server_execution_time_ms_p95": [],
                "search_server_phase_dispatch_service_ms_p95": [],
                "search_server_phase_dispatch_service_us_p95": [],
                "search_server_phase_dispatch_map_sort_ms_p95": [],
                "search_server_phase_dispatch_map_sort_us_p95": [],
                "search_server_phase_dispatch_feedback_ms_p95": [],
                "search_server_phase_dispatch_feedback_us_p95": [],
                "search_server_phase_dispatch_response_ms_p95": [],
                "search_server_phase_dispatch_response_us_p95": [],
                "search_server_phase_dispatch_total_ms_p95": [],
                "search_server_phase_dispatch_total_us_p95": [],
                "list_server_service_execution_time_ms_p95": [],
                "list_server_phase_dispatch_service_ms_p95": [],
                "list_server_phase_dispatch_service_us_p95": [],
                "list_server_phase_dispatch_map_ms_p95": [],
                "list_server_phase_dispatch_map_us_p95": [],
                "list_server_phase_dispatch_total_ms_p95": [],
                "list_server_phase_dispatch_total_us_p95": [],
            }
        return target[request_path]

    def get_case_bucket(
        key: tuple[str, str, str, str, str, str, int],
    ) -> dict[str, list[float]]:
        if key not in case_buckets:
            case_buckets[key] = {
                "ops_per_sec": [],
                "fail_rate": [],
                "latency_p95_ms": [],
            }
        return case_buckets[key]

    for row in records:
        test_name = str(row.get("test", ""))
        run_phase = str(row.get("run_phase", "unknown"))
        run_transport = str(
            row.get("run_transport", row.get("request_path", "unknown"))
        )
        usage_profile = str(row.get("layout", {}).get("usage_profile", "unknown"))
        backend = str(row.get("run_backend", row.get("backend", "unknown")))
        layout = (
            row.get("layout", {}) if isinstance(row.get("layout", {}), dict) else {}
        )
        num_clients = int(
            _safe_float(
                row.get(
                    "num_clients",
                    _safe_float(layout.get("search_clients", 0.0))
                    + _safe_float(layout.get("list_clients", 0.0))
                    + _safe_float(layout.get("grep_clients", 0.0))
                    + _safe_float(layout.get("status_get_clients", 0.0)),
                ),
                0.0,
            )
        )

        if test_name in {"large_corpus_reads", "large_corpus_reads_churn"}:
            is_churn = test_name == "large_corpus_reads_churn"
            request_path = str(row.get("request_path", "unknown"))
            bucket = get_large_bucket(
                large_corpus_churn if is_churn else large_corpus, request_path
            )

            elapsed_seconds = _safe_float(row.get("elapsed_seconds", 0.0))
            total_ops = _safe_float(row.get("total_ops", 0.0))
            total_failures = _safe_float(row.get("total_failures", 0.0))
            error_categories = row.get("error_categories", {})
            connection_drop = _safe_float(error_categories.get("connection_drop", 0.0))
            document_not_found = _safe_float(
                error_categories.get("document_not_found", 0.0)
            )
            content_not_found = _safe_float(
                error_categories.get("content_not_found", 0.0)
            )
            hash_pool = row.get("hash_pool", {})

            bucket["ops_per_sec"].append(_safe_float(row.get("ops_per_sec", 0.0)))
            bucket["fail_rate"].append(_rate(total_failures, total_ops))
            bucket["connection_drop_rate"].append(_rate(connection_drop, total_ops))
            bucket["document_not_found_rate"].append(
                _rate(document_not_found, total_ops)
            )
            bucket["content_not_found_rate"].append(_rate(content_not_found, total_ops))
            bucket["invalid_hash_quarantined"].append(
                _safe_float(hash_pool.get("invalid_quarantined", 0.0))
            )

            for op in ("search", "list", "grep", "status", "get", "cat"):
                op_obj = row.get(op, {})
                op_ops = _safe_float(op_obj.get("ops", 0.0))
                op_fails = _safe_float(op_obj.get("fails", 0.0))
                op_p95_ms = (
                    _safe_float(op_obj.get("latency", {}).get("p95_us", 0.0)) / 1000.0
                )
                bucket[f"{op}_p95_ms"].append(op_p95_ms)
                bucket[f"{op}_fail_rate"].append(_rate(op_fails, op_ops))
                bucket[f"{op}_ops_per_sec"].append(_rate(op_ops, elapsed_seconds))

            traces = row.get("op_traces", [])
            bucket["op_trace_count"].append(
                float(len(traces)) if isinstance(traces, list) else 0.0
            )

            grep_server = row.get("grep_server", {})
            bucket["grep_server_execution_time_ms_p95"].append(
                _safe_float(grep_server.get("execution_time_ms", {}).get("p95", 0.0))
            )
            bucket["grep_server_phase_worker_scan_ms_p95"].append(
                _safe_float(grep_server.get("phase_worker_scan_ms", {}).get("p95", 0.0))
            )
            bucket["grep_server_content_retrieval_ms_p95"].append(
                _safe_float(grep_server.get("content_retrieval_ms", {}).get("p95", 0.0))
            )
            bucket["grep_server_worker_critical_total_ms_p95"].append(
                _safe_float(
                    grep_server.get("worker_critical_total_ms", {}).get("p95", 0.0)
                )
            )
            bucket["grep_server_worker_critical_content_retrieval_ms_p95"].append(
                _safe_float(
                    grep_server.get("worker_critical_content_retrieval_ms", {}).get(
                        "p95", 0.0
                    )
                )
            )
            bucket["grep_server_worker_critical_regex_scan_ms_p95"].append(
                _safe_float(
                    grep_server.get("worker_critical_regex_scan_ms", {}).get("p95", 0.0)
                )
            )
            bucket["grep_server_worker_critical_other_ms_p95"].append(
                _safe_float(
                    grep_server.get("worker_critical_other_ms", {}).get("p95", 0.0)
                )
            )
            bucket["grep_server_docs_scanned_mean"].append(
                _safe_float(grep_server.get("docs_scanned", {}).get("mean", 0.0))
            )
            bucket["grep_server_lines_scanned_mean"].append(
                _safe_float(grep_server.get("lines_scanned", {}).get("mean", 0.0))
            )
            bucket["grep_server_bytes_scanned_mean"].append(
                _safe_float(grep_server.get("bytes_scanned", {}).get("mean", 0.0))
            )

            search_server = row.get("search_server", {})
            bucket["search_server_execution_time_ms_p95"].append(
                _safe_float(search_server.get("execution_time_ms", {}).get("p95", 0.0))
            )
            bucket["search_server_phase_dispatch_service_ms_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_service_ms", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_service_us_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_service_us", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_map_sort_ms_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_map_sort_ms", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_map_sort_us_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_map_sort_us", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_feedback_ms_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_feedback_ms", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_feedback_us_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_feedback_us", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_response_ms_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_response_ms", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_response_us_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_response_us", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_total_ms_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_total_ms", {}).get("p95", 0.0)
                )
            )
            bucket["search_server_phase_dispatch_total_us_p95"].append(
                _safe_float(
                    search_server.get("phase_dispatch_total_us", {}).get("p95", 0.0)
                )
            )

            list_server = row.get("list_server", {})
            bucket["list_server_service_execution_time_ms_p95"].append(
                _safe_float(
                    list_server.get("service_execution_time_ms", {}).get("p95", 0.0)
                )
            )
            bucket["list_server_phase_dispatch_service_ms_p95"].append(
                _safe_float(
                    list_server.get("phase_dispatch_service_ms", {}).get("p95", 0.0)
                )
            )
            bucket["list_server_phase_dispatch_service_us_p95"].append(
                _safe_float(
                    list_server.get("phase_dispatch_service_us", {}).get("p95", 0.0)
                )
            )
            bucket["list_server_phase_dispatch_map_ms_p95"].append(
                _safe_float(
                    list_server.get("phase_dispatch_map_ms", {}).get("p95", 0.0)
                )
            )
            bucket["list_server_phase_dispatch_map_us_p95"].append(
                _safe_float(
                    list_server.get("phase_dispatch_map_us", {}).get("p95", 0.0)
                )
            )
            bucket["list_server_phase_dispatch_total_ms_p95"].append(
                _safe_float(
                    list_server.get("phase_dispatch_total_ms", {}).get("p95", 0.0)
                )
            )
            bucket["list_server_phase_dispatch_total_us_p95"].append(
                _safe_float(
                    list_server.get("phase_dispatch_total_us", {}).get("p95", 0.0)
                )
            )

            if is_churn:
                large_churn_usage_profiles[request_path][usage_profile] += 1
            else:
                large_usage_profiles[request_path][usage_profile] += 1

            case_key = (
                run_phase,
                test_name,
                request_path,
                run_transport,
                usage_profile,
                backend,
                num_clients,
            )
            c_bucket = get_case_bucket(case_key)
            c_bucket["ops_per_sec"].append(_safe_float(row.get("ops_per_sec", 0.0)))
            c_bucket["fail_rate"].append(_rate(total_failures, total_ops))
            c_bucket["latency_p95_ms"].append(
                _safe_float(row.get("search", {}).get("latency", {}).get("p95_us", 0.0))
                / 1000.0
            )

        elif test_name == "connection_contention":
            total_ops = _safe_float(row.get("total_ops", 0.0))
            fail_count = _safe_float(row.get("fail_count", 0.0))
            retry_after_count = _safe_float(row.get("retry_after_count", 0.0))
            success_count = _safe_float(row.get("success_count", 0.0))

            throughput = _safe_float(row.get("throughput_ops_per_sec", 0.0))
            fail_rate = _rate(fail_count, total_ops)
            retry_after_rate = _rate(retry_after_count, total_ops)
            success_rate = _rate(success_count, total_ops)
            latency_p95_ms = (
                _safe_float(row.get("latency", {}).get("p95_us", 0.0)) / 1000.0
            )

            contention_metrics["throughput_ops_per_sec"].append(throughput)
            contention_metrics["fail_rate"].append(fail_rate)
            contention_metrics["retry_after_rate"].append(retry_after_rate)
            contention_metrics["latency_p95_ms"].append(latency_p95_ms)
            contention_metrics["success_rate"].append(success_rate)

            case_key = (
                run_phase,
                test_name,
                "n/a",
                run_transport,
                usage_profile,
                backend,
                num_clients,
            )
            c_bucket = get_case_bucket(case_key)
            c_bucket["ops_per_sec"].append(throughput)
            c_bucket["fail_rate"].append(fail_rate)
            c_bucket["latency_p95_ms"].append(latency_p95_ms)

    # Backward-compatible summaries
    def _build_large_compat(
        source: dict[str, dict[str, list[float]]],
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for request_path, metrics in source.items():
            out[request_path] = {
                "ops_per_sec": _to_summary(metrics["ops_per_sec"], "ops/s"),
                "fail_rate": _to_summary(metrics["fail_rate"], "ratio"),
                "connection_drop_rate": _to_summary(
                    metrics["connection_drop_rate"], "ratio"
                ),
                "document_not_found_rate": _to_summary(
                    metrics["document_not_found_rate"], "ratio"
                ),
                "content_not_found_rate": _to_summary(
                    metrics["content_not_found_rate"], "ratio"
                ),
                "search_p95_ms": _to_summary(metrics["search_p95_ms"], "ms"),
                "list_p95_ms": _to_summary(metrics["list_p95_ms"], "ms"),
            }
        return out

    def _build_large_v2(
        source: dict[str, dict[str, list[float]]],
        usage_profiles: dict[str, dict[str, int]],
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for request_path, metrics in source.items():
            out[request_path] = {
                "ops_per_sec": _to_summary(metrics["ops_per_sec"], "ops/s"),
                "fail_rate": _to_summary(metrics["fail_rate"], "ratio"),
                "connection_drop_rate": _to_summary(
                    metrics["connection_drop_rate"], "ratio"
                ),
                "document_not_found_rate": _to_summary(
                    metrics["document_not_found_rate"], "ratio"
                ),
                "content_not_found_rate": _to_summary(
                    metrics["content_not_found_rate"], "ratio"
                ),
                "invalid_hash_quarantined": _to_summary(
                    metrics["invalid_hash_quarantined"], "count"
                ),
                "usage_profiles": dict(sorted(usage_profiles[request_path].items())),
                "search": {
                    "p95_ms": _to_summary(metrics["search_p95_ms"], "ms"),
                    "fail_rate": _to_summary(metrics["search_fail_rate"], "ratio"),
                    "ops_per_sec": _to_summary(metrics["search_ops_per_sec"], "ops/s"),
                },
                "list": {
                    "p95_ms": _to_summary(metrics["list_p95_ms"], "ms"),
                    "fail_rate": _to_summary(metrics["list_fail_rate"], "ratio"),
                    "ops_per_sec": _to_summary(metrics["list_ops_per_sec"], "ops/s"),
                },
                "grep": {
                    "p95_ms": _to_summary(metrics["grep_p95_ms"], "ms"),
                    "fail_rate": _to_summary(metrics["grep_fail_rate"], "ratio"),
                    "ops_per_sec": _to_summary(metrics["grep_ops_per_sec"], "ops/s"),
                },
                "status": {
                    "p95_ms": _to_summary(metrics["status_p95_ms"], "ms"),
                    "fail_rate": _to_summary(metrics["status_fail_rate"], "ratio"),
                    "ops_per_sec": _to_summary(metrics["status_ops_per_sec"], "ops/s"),
                },
                "get": {
                    "p95_ms": _to_summary(metrics["get_p95_ms"], "ms"),
                    "fail_rate": _to_summary(metrics["get_fail_rate"], "ratio"),
                    "ops_per_sec": _to_summary(metrics["get_ops_per_sec"], "ops/s"),
                },
                "cat": {
                    "p95_ms": _to_summary(metrics["cat_p95_ms"], "ms"),
                    "fail_rate": _to_summary(metrics["cat_fail_rate"], "ratio"),
                    "ops_per_sec": _to_summary(metrics["cat_ops_per_sec"], "ops/s"),
                },
                "op_trace_count": _to_summary(metrics["op_trace_count"], "count"),
                "grep_server": {
                    "execution_time_ms_p95": _to_summary(
                        metrics["grep_server_execution_time_ms_p95"], "ms"
                    ),
                    "phase_worker_scan_ms_p95": _to_summary(
                        metrics["grep_server_phase_worker_scan_ms_p95"], "ms"
                    ),
                    "content_retrieval_ms_p95": _to_summary(
                        metrics["grep_server_content_retrieval_ms_p95"], "ms"
                    ),
                    "worker_critical_total_ms_p95": _to_summary(
                        metrics["grep_server_worker_critical_total_ms_p95"], "ms"
                    ),
                    "worker_critical_content_retrieval_ms_p95": _to_summary(
                        metrics["grep_server_worker_critical_content_retrieval_ms_p95"],
                        "ms",
                    ),
                    "worker_critical_regex_scan_ms_p95": _to_summary(
                        metrics["grep_server_worker_critical_regex_scan_ms_p95"], "ms"
                    ),
                    "worker_critical_other_ms_p95": _to_summary(
                        metrics["grep_server_worker_critical_other_ms_p95"], "ms"
                    ),
                    "docs_scanned_mean": _to_summary(
                        metrics["grep_server_docs_scanned_mean"], "count"
                    ),
                    "lines_scanned_mean": _to_summary(
                        metrics["grep_server_lines_scanned_mean"], "count"
                    ),
                    "bytes_scanned_mean": _to_summary(
                        metrics["grep_server_bytes_scanned_mean"], "bytes"
                    ),
                },
                "search_server": {
                    "execution_time_ms_p95": _to_summary(
                        metrics["search_server_execution_time_ms_p95"], "ms"
                    ),
                    "phase_dispatch_service_ms_p95": _to_summary(
                        metrics["search_server_phase_dispatch_service_ms_p95"], "ms"
                    ),
                    "phase_dispatch_service_us_p95": _to_summary(
                        metrics["search_server_phase_dispatch_service_us_p95"], "us"
                    ),
                    "phase_dispatch_map_sort_ms_p95": _to_summary(
                        metrics["search_server_phase_dispatch_map_sort_ms_p95"], "ms"
                    ),
                    "phase_dispatch_map_sort_us_p95": _to_summary(
                        metrics["search_server_phase_dispatch_map_sort_us_p95"], "us"
                    ),
                    "phase_dispatch_feedback_ms_p95": _to_summary(
                        metrics["search_server_phase_dispatch_feedback_ms_p95"], "ms"
                    ),
                    "phase_dispatch_feedback_us_p95": _to_summary(
                        metrics["search_server_phase_dispatch_feedback_us_p95"], "us"
                    ),
                    "phase_dispatch_response_ms_p95": _to_summary(
                        metrics["search_server_phase_dispatch_response_ms_p95"], "ms"
                    ),
                    "phase_dispatch_response_us_p95": _to_summary(
                        metrics["search_server_phase_dispatch_response_us_p95"], "us"
                    ),
                    "phase_dispatch_total_ms_p95": _to_summary(
                        metrics["search_server_phase_dispatch_total_ms_p95"], "ms"
                    ),
                    "phase_dispatch_total_us_p95": _to_summary(
                        metrics["search_server_phase_dispatch_total_us_p95"], "us"
                    ),
                },
                "list_server": {
                    "service_execution_time_ms_p95": _to_summary(
                        metrics["list_server_service_execution_time_ms_p95"], "ms"
                    ),
                    "phase_dispatch_service_ms_p95": _to_summary(
                        metrics["list_server_phase_dispatch_service_ms_p95"], "ms"
                    ),
                    "phase_dispatch_service_us_p95": _to_summary(
                        metrics["list_server_phase_dispatch_service_us_p95"], "us"
                    ),
                    "phase_dispatch_map_ms_p95": _to_summary(
                        metrics["list_server_phase_dispatch_map_ms_p95"], "ms"
                    ),
                    "phase_dispatch_map_us_p95": _to_summary(
                        metrics["list_server_phase_dispatch_map_us_p95"], "us"
                    ),
                    "phase_dispatch_total_ms_p95": _to_summary(
                        metrics["list_server_phase_dispatch_total_ms_p95"], "ms"
                    ),
                    "phase_dispatch_total_us_p95": _to_summary(
                        metrics["list_server_phase_dispatch_total_us_p95"], "us"
                    ),
                },
            }
        return out

    large_corpus_compat = _build_large_compat(large_corpus)
    large_corpus_v2 = _build_large_v2(large_corpus, large_usage_profiles)
    large_corpus_churn_compat = _build_large_compat(large_corpus_churn)
    large_corpus_churn_v2 = _build_large_v2(
        large_corpus_churn, large_churn_usage_profiles
    )

    contention_compat = {
        "throughput_ops_per_sec": _to_summary(
            contention_metrics["throughput_ops_per_sec"], "ops/s"
        ),
        "fail_rate": _to_summary(contention_metrics["fail_rate"], "ratio"),
        "retry_after_rate": _to_summary(
            contention_metrics["retry_after_rate"], "ratio"
        ),
        "latency_p95_ms": _to_summary(contention_metrics["latency_p95_ms"], "ms"),
    }

    contention_v2 = {
        **contention_compat,
        "success_rate": _to_summary(contention_metrics["success_rate"], "ratio"),
    }

    case_breakdown: list[dict[str, Any]] = []
    for key in sorted(case_buckets.keys()):
        (
            run_phase,
            test_name,
            request_path,
            run_transport,
            usage_profile,
            backend,
            num_clients,
        ) = key
        metrics = case_buckets[key]
        case_breakdown.append(
            {
                "phase": run_phase,
                "test": test_name,
                "request_path": request_path,
                "transport": run_transport,
                "usage_profile": usage_profile,
                "backend": backend,
                "num_clients": num_clients,
                "ops_per_sec": _to_summary(metrics["ops_per_sec"], "ops/s"),
                "fail_rate": _to_summary(metrics["fail_rate"], "ratio"),
                "latency_p95_ms": _to_summary(metrics["latency_p95_ms"], "ms"),
            }
        )

    out = {
        "schema_version": "multi_client_summary_v2",
        "record_count": len(records),
        "inputs": [str(p) for p in input_paths],
        "large_corpus_reads": large_corpus_compat,
        "large_corpus_reads_churn": large_corpus_churn_compat,
        "connection_contention": contention_compat,
        "large_corpus_reads_v2": large_corpus_v2,
        "large_corpus_reads_churn_v2": large_corpus_churn_v2,
        "connection_contention_v2": contention_v2,
        "case_breakdown": case_breakdown,
    }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize multi-client benchmark JSONL runs"
    )
    parser.add_argument(
        "--input",
        type=Path,
        action="append",
        required=True,
        help="JSONL file path (repeatable)",
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="Summary JSON output path"
    )
    args = parser.parse_args()

    records = _load_records(args.input)
    summary = summarize(records, args.input)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    print(f"Wrote summary: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
