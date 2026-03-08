#!/usr/bin/env python3
"""
Regression checker for summarized multi-client benchmark KPIs.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in: {path}")
    return data


def _metric_mean(obj: dict[str, Any], key: str) -> float:
    return float(obj.get(key, {}).get("mean", 0.0))


def _nested_metric_mean(obj: dict[str, Any], keys: list[str]) -> float:
    cur: Any = obj
    for key in keys:
        if not isinstance(cur, dict) or key not in cur:
            return 0.0
        cur = cur[key]
    if isinstance(cur, dict):
        return float(cur.get("mean", 0.0))
    try:
        return float(cur)
    except (TypeError, ValueError):
        return 0.0


def _compare_ratio_change(current: float, baseline: float) -> float:
    if baseline == 0.0:
        return 0.0
    return (current - baseline) / baseline


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare multi-client KPI summaries")
    parser.add_argument("current", type=Path, help="Current summary JSON")
    parser.add_argument("baseline", type=Path, help="Baseline summary JSON")
    parser.add_argument(
        "--throughput-regression",
        type=float,
        default=0.10,
        help="Throughput drop threshold as ratio (default: 0.10 = 10%%)",
    )
    parser.add_argument(
        "--failure-regression",
        type=float,
        default=0.01,
        help="Allowed absolute fail-rate increase (default: 0.01 = +1pp)",
    )
    parser.add_argument(
        "--latency-regression",
        type=float,
        default=0.15,
        help="Allowed p95 latency increase ratio (default: 0.15 = +15%%)",
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="Comparison report JSON"
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit non-zero when regressions are detected",
    )
    args = parser.parse_args()

    current = _load_json(args.current)
    baseline = _load_json(args.baseline)

    regressions: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []

    def check(
        metric_name: str,
        current_value: float,
        baseline_value: float,
        is_regression: bool,
        reason: str = "",
    ):
        row = {
            "metric": metric_name,
            "current": current_value,
            "baseline": baseline_value,
            "delta": current_value - baseline_value,
            "reason": reason,
        }
        checks.append(row)
        if is_regression:
            regressions.append(row)

    # ------------------------------------------------------------------
    # Backward-compatible large corpus checks
    # ------------------------------------------------------------------
    current_large = current.get("large_corpus_reads", {})
    baseline_large = baseline.get("large_corpus_reads", {})

    for request_path in sorted(
        set(current_large.keys()).intersection(baseline_large.keys())
    ):
        c_bucket = current_large.get(request_path, {})
        b_bucket = baseline_large.get(request_path, {})

        c_ops = _metric_mean(c_bucket, "ops_per_sec")
        b_ops = _metric_mean(b_bucket, "ops_per_sec")
        ops_change = _compare_ratio_change(c_ops, b_ops)
        check(
            f"large_corpus_reads.{request_path}.ops_per_sec",
            c_ops,
            b_ops,
            ops_change < -args.throughput_regression,
            "throughput drop beyond threshold",
        )

        c_fail = _metric_mean(c_bucket, "fail_rate")
        b_fail = _metric_mean(b_bucket, "fail_rate")
        check(
            f"large_corpus_reads.{request_path}.fail_rate",
            c_fail,
            b_fail,
            (c_fail - b_fail) > args.failure_regression,
            "fail-rate increase beyond threshold",
        )

        c_drop = _metric_mean(c_bucket, "connection_drop_rate")
        b_drop = _metric_mean(b_bucket, "connection_drop_rate")
        check(
            f"large_corpus_reads.{request_path}.connection_drop_rate",
            c_drop,
            b_drop,
            (c_drop - b_drop) > args.failure_regression,
            "connection-drop increase beyond threshold",
        )

        c_search = _metric_mean(c_bucket, "search_p95_ms")
        b_search = _metric_mean(b_bucket, "search_p95_ms")
        search_change = _compare_ratio_change(c_search, b_search)
        check(
            f"large_corpus_reads.{request_path}.search_p95_ms",
            c_search,
            b_search,
            search_change > args.latency_regression,
            "search p95 increase beyond threshold",
        )

        c_list = _metric_mean(c_bucket, "list_p95_ms")
        b_list = _metric_mean(b_bucket, "list_p95_ms")
        list_change = _compare_ratio_change(c_list, b_list)
        check(
            f"large_corpus_reads.{request_path}.list_p95_ms",
            c_list,
            b_list,
            list_change > args.latency_regression,
            "list p95 increase beyond threshold",
        )

    # ------------------------------------------------------------------
    # v2 large corpus checks (op-specific + grep server stats)
    # ------------------------------------------------------------------
    current_large_v2 = current.get("large_corpus_reads_v2", {})
    baseline_large_v2 = baseline.get("large_corpus_reads_v2", {})
    for request_path in sorted(
        set(current_large_v2.keys()).intersection(baseline_large_v2.keys())
    ):
        c_bucket = current_large_v2.get(request_path, {})
        b_bucket = baseline_large_v2.get(request_path, {})

        for op in ("search", "list", "grep", "status", "get", "cat"):
            c_ops = _nested_metric_mean(c_bucket, [op, "ops_per_sec"])
            b_ops = _nested_metric_mean(b_bucket, [op, "ops_per_sec"])
            ops_change = _compare_ratio_change(c_ops, b_ops)
            check(
                f"large_corpus_reads_v2.{request_path}.{op}.ops_per_sec",
                c_ops,
                b_ops,
                ops_change < -args.throughput_regression,
                "op throughput drop beyond threshold",
            )

            c_fail = _nested_metric_mean(c_bucket, [op, "fail_rate"])
            b_fail = _nested_metric_mean(b_bucket, [op, "fail_rate"])
            check(
                f"large_corpus_reads_v2.{request_path}.{op}.fail_rate",
                c_fail,
                b_fail,
                (c_fail - b_fail) > args.failure_regression,
                "op fail-rate increase beyond threshold",
            )

            c_p95 = _nested_metric_mean(c_bucket, [op, "p95_ms"])
            b_p95 = _nested_metric_mean(b_bucket, [op, "p95_ms"])
            p95_change = _compare_ratio_change(c_p95, b_p95)
            check(
                f"large_corpus_reads_v2.{request_path}.{op}.p95_ms",
                c_p95,
                b_p95,
                p95_change > args.latency_regression,
                "op p95 increase beyond threshold",
            )

        for metric in (
            "execution_time_ms_p95",
            "phase_worker_scan_ms_p95",
            "content_retrieval_ms_p95",
            "worker_critical_total_ms_p95",
            "worker_critical_content_retrieval_ms_p95",
            "worker_critical_regex_scan_ms_p95",
            "worker_critical_other_ms_p95",
        ):
            c_val = _nested_metric_mean(c_bucket, ["grep_server", metric])
            b_val = _nested_metric_mean(b_bucket, ["grep_server", metric])
            change = _compare_ratio_change(c_val, b_val)
            check(
                f"large_corpus_reads_v2.{request_path}.grep_server.{metric}",
                c_val,
                b_val,
                change > args.latency_regression,
                "grep server-phase latency increase beyond threshold",
            )

    # ------------------------------------------------------------------
    # External-agent churn profile checks (legacy + v2)
    # ------------------------------------------------------------------
    current_churn = current.get("large_corpus_reads_churn", {})
    baseline_churn = baseline.get("large_corpus_reads_churn", {})

    for request_path in sorted(
        set(current_churn.keys()).intersection(baseline_churn.keys())
    ):
        c_bucket = current_churn.get(request_path, {})
        b_bucket = baseline_churn.get(request_path, {})

        c_ops = _metric_mean(c_bucket, "ops_per_sec")
        b_ops = _metric_mean(b_bucket, "ops_per_sec")
        ops_change = _compare_ratio_change(c_ops, b_ops)
        check(
            f"large_corpus_reads_churn.{request_path}.ops_per_sec",
            c_ops,
            b_ops,
            ops_change < -args.throughput_regression,
            "throughput drop beyond threshold",
        )

        c_fail = _metric_mean(c_bucket, "fail_rate")
        b_fail = _metric_mean(b_bucket, "fail_rate")
        check(
            f"large_corpus_reads_churn.{request_path}.fail_rate",
            c_fail,
            b_fail,
            (c_fail - b_fail) > args.failure_regression,
            "fail-rate increase beyond threshold",
        )

        c_drop = _metric_mean(c_bucket, "connection_drop_rate")
        b_drop = _metric_mean(b_bucket, "connection_drop_rate")
        check(
            f"large_corpus_reads_churn.{request_path}.connection_drop_rate",
            c_drop,
            b_drop,
            (c_drop - b_drop) > args.failure_regression,
            "connection-drop increase beyond threshold",
        )

        c_search = _metric_mean(c_bucket, "search_p95_ms")
        b_search = _metric_mean(b_bucket, "search_p95_ms")
        search_change = _compare_ratio_change(c_search, b_search)
        check(
            f"large_corpus_reads_churn.{request_path}.search_p95_ms",
            c_search,
            b_search,
            search_change > args.latency_regression,
            "search p95 increase beyond threshold",
        )

        c_list = _metric_mean(c_bucket, "list_p95_ms")
        b_list = _metric_mean(b_bucket, "list_p95_ms")
        list_change = _compare_ratio_change(c_list, b_list)
        check(
            f"large_corpus_reads_churn.{request_path}.list_p95_ms",
            c_list,
            b_list,
            list_change > args.latency_regression,
            "list p95 increase beyond threshold",
        )

    current_churn_v2 = current.get("large_corpus_reads_churn_v2", {})
    baseline_churn_v2 = baseline.get("large_corpus_reads_churn_v2", {})
    for request_path in sorted(
        set(current_churn_v2.keys()).intersection(baseline_churn_v2.keys())
    ):
        c_bucket = current_churn_v2.get(request_path, {})
        b_bucket = baseline_churn_v2.get(request_path, {})

        for op in ("search", "list", "grep", "status", "get", "cat"):
            c_ops = _nested_metric_mean(c_bucket, [op, "ops_per_sec"])
            b_ops = _nested_metric_mean(b_bucket, [op, "ops_per_sec"])
            ops_change = _compare_ratio_change(c_ops, b_ops)
            check(
                f"large_corpus_reads_churn_v2.{request_path}.{op}.ops_per_sec",
                c_ops,
                b_ops,
                ops_change < -args.throughput_regression,
                "op throughput drop beyond threshold",
            )

            c_fail = _nested_metric_mean(c_bucket, [op, "fail_rate"])
            b_fail = _nested_metric_mean(b_bucket, [op, "fail_rate"])
            check(
                f"large_corpus_reads_churn_v2.{request_path}.{op}.fail_rate",
                c_fail,
                b_fail,
                (c_fail - b_fail) > args.failure_regression,
                "op fail-rate increase beyond threshold",
            )

            c_p95 = _nested_metric_mean(c_bucket, [op, "p95_ms"])
            b_p95 = _nested_metric_mean(b_bucket, [op, "p95_ms"])
            p95_change = _compare_ratio_change(c_p95, b_p95)
            check(
                f"large_corpus_reads_churn_v2.{request_path}.{op}.p95_ms",
                c_p95,
                b_p95,
                p95_change > args.latency_regression,
                "op p95 increase beyond threshold",
            )

    # ------------------------------------------------------------------
    # Connection contention checks
    # ------------------------------------------------------------------
    current_contention = current.get("connection_contention", {})
    baseline_contention = baseline.get("connection_contention", {})
    if current_contention and baseline_contention:
        c_thr = _metric_mean(current_contention, "throughput_ops_per_sec")
        b_thr = _metric_mean(baseline_contention, "throughput_ops_per_sec")
        thr_change = _compare_ratio_change(c_thr, b_thr)
        check(
            "connection_contention.throughput_ops_per_sec",
            c_thr,
            b_thr,
            thr_change < -args.throughput_regression,
            "throughput drop beyond threshold",
        )

        c_fail = _metric_mean(current_contention, "fail_rate")
        b_fail = _metric_mean(baseline_contention, "fail_rate")
        check(
            "connection_contention.fail_rate",
            c_fail,
            b_fail,
            (c_fail - b_fail) > args.failure_regression,
            "fail-rate increase beyond threshold",
        )

        c_retry_after = _metric_mean(current_contention, "retry_after_rate")
        b_retry_after = _metric_mean(baseline_contention, "retry_after_rate")
        check(
            "connection_contention.retry_after_rate",
            c_retry_after,
            b_retry_after,
            (c_retry_after - b_retry_after) > args.failure_regression,
            "retry-after increase beyond threshold",
        )

    current_contention_v2 = current.get("connection_contention_v2", {})
    baseline_contention_v2 = baseline.get("connection_contention_v2", {})
    if current_contention_v2 and baseline_contention_v2:
        c_success = _metric_mean(current_contention_v2, "success_rate")
        b_success = _metric_mean(baseline_contention_v2, "success_rate")
        success_change = _compare_ratio_change(c_success, b_success)
        check(
            "connection_contention_v2.success_rate",
            c_success,
            b_success,
            success_change < -args.throughput_regression,
            "success-rate drop beyond threshold",
        )

    report = {
        "current": str(args.current),
        "baseline": str(args.baseline),
        "thresholds": {
            "throughput_regression": args.throughput_regression,
            "failure_regression": args.failure_regression,
            "latency_regression": args.latency_regression,
        },
        "checks": checks,
        "regressions": regressions,
        "has_regression": len(regressions) > 0,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    if regressions:
        print(f"Regressions detected: {len(regressions)}")
        for row in regressions:
            print(
                f"  - {row['metric']}: baseline={row['baseline']:.6f} current={row['current']:.6f}"
            )
        return 1 if args.fail_on_regression else 0

    print("No regressions detected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
