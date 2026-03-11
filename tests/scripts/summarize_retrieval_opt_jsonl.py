#!/usr/bin/env python3
"""Summarize retrieval optimization JSONL artifacts.

This script consumes one or more JSONL files produced by
`src/search/benchmarks/retrieval_quality_bench.cpp` (optimization loop mode)
and writes a normalized JSON summary focused on baseline selection.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class RunRecord:
    source_file: str
    line_number: int
    candidate: str
    success: bool
    objective: float
    raw: dict[str, Any]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _is_success(row: dict[str, Any]) -> bool:
    if isinstance(row.get("success"), bool):
        return bool(row.get("success"))
    status = str(row.get("status", "")).strip().lower()
    return status == "ok"


def _objective_value(row: dict[str, Any]) -> float:
    if "objective" in row:
        return _safe_float(row.get("objective"), 0.0)
    if "objective_score" in row:
        return _safe_float(row.get("objective_score"), 0.0)
    return 0.0


def _load_records(inputs: list[Path]) -> list[RunRecord]:
    records: list[RunRecord] = []
    for path in inputs:
        if not path.exists():
            raise FileNotFoundError(f"Input JSONL not found: {path}")

        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{line_no}: invalid JSON: {exc}") from exc

                if not isinstance(row, dict):
                    continue

                candidate = str(row.get("candidate", "")).strip()
                if not candidate:
                    continue

                records.append(
                    RunRecord(
                        source_file=str(path),
                        line_number=line_no,
                        candidate=candidate,
                        success=_is_success(row),
                        objective=_objective_value(row),
                        raw=row,
                    )
                )
    return records


def _metric_triplet(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "count": 0,
            "mean": 0.0,
            "stddev": 0.0,
            "min": 0.0,
            "max": 0.0,
        }

    stddev = statistics.pstdev(values) if len(values) > 1 else 0.0
    return {
        "count": len(values),
        "mean": float(statistics.fmean(values)),
        "stddev": float(stddev),
        "min": float(min(values)),
        "max": float(max(values)),
    }


def _nested(data: dict[str, Any], path: list[str], default: Any = None) -> Any:
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict):
            return default
        if key not in cur:
            return default
        cur = cur[key]
    return cur


def summarize(records: list[RunRecord], inputs: list[Path]) -> dict[str, Any]:
    latest_success_by_candidate: dict[str, RunRecord] = {}
    objective_by_candidate: dict[str, list[float]] = {}
    debug_metric_by_candidate: dict[str, dict[str, list[float]]] = {}

    debug_metric_paths: dict[str, list[str]] = {
        "hybrid_keyword_mrr_delta": ["hybrid_keyword_mrr_delta"],
        "hybrid_duplicate_rate_at_k": ["hybrid", "duplicate_rate_at_k"],
        "objective_penalty_hybrid_regression": [
            "objective_penalties",
            "hybrid_regression",
        ],
        "objective_penalty_duplicate_rate": ["objective_penalties", "duplicate_rate"],
        "trace_coverage": ["hybrid_debug_summary", "trace_coverage"],
        "graph_rerank_apply_rate": ["hybrid_debug_summary", "graph_rerank_apply_rate"],
        "semantic_rescue_rate_mean": [
            "hybrid_debug_summary",
            "semantic_rescue_rate",
            "mean",
        ],
        "vector_only_below_threshold_mean": [
            "hybrid_debug_summary",
            "vector_only_below_threshold",
            "mean",
        ],
        "query_without_relevant_hit_rate": [
            "hybrid_debug_summary",
            "query_without_relevant_hit_rate",
        ],
    }

    for record in records:
        if record.success:
            latest_success_by_candidate[record.candidate] = record
            objective_by_candidate.setdefault(record.candidate, []).append(
                record.objective
            )
            metric_store = debug_metric_by_candidate.setdefault(record.candidate, {})
            for metric_name, path in debug_metric_paths.items():
                value = _nested(record.raw, path, 0.0)
                metric_store.setdefault(metric_name, []).append(_safe_float(value, 0.0))

    latest_rows: list[dict[str, Any]] = []
    for candidate, record in latest_success_by_candidate.items():
        row = {
            "candidate": candidate,
            "objective": record.objective,
            "source_file": record.source_file,
            "line_number": record.line_number,
            "env_overrides": record.raw.get("env_overrides", {}),
            "hybrid": record.raw.get("hybrid", {}),
            "keyword": record.raw.get("keyword", {}),
            "hybrid_debug_summary": record.raw.get("hybrid_debug_summary", {}),
            "keyword_debug_summary": record.raw.get("keyword_debug_summary", {}),
            "tuning_state": record.raw.get("tuning_state", ""),
            "tuning_reason": record.raw.get("tuning_reason", ""),
            "run_id": record.raw.get("run_id", ""),
            "debug_file": record.raw.get("debug_file", ""),
            "trace_top_n": int(record.raw.get("trace_top_n", 0) or 0),
            "trace_component_top_n": int(
                record.raw.get("trace_component_top_n", 0) or 0
            ),
            "stage_trace_enabled": bool(record.raw.get("stage_trace_enabled", False)),
        }
        latest_rows.append(row)

    latest_rows.sort(key=lambda row: row["objective"], reverse=True)

    aggregates: list[dict[str, Any]] = []
    for candidate, values in objective_by_candidate.items():
        stats = _metric_triplet(values)
        debug_metrics: dict[str, Any] = {}
        for metric_name, metric_values in debug_metric_by_candidate.get(
            candidate, {}
        ).items():
            debug_metrics[metric_name] = _metric_triplet(metric_values)
        aggregates.append(
            {"candidate": candidate, **stats, "debug_metrics": debug_metrics}
        )
    aggregates.sort(key=lambda row: row["mean"], reverse=True)

    winner_latest = latest_rows[0] if latest_rows else None
    winner_mean = aggregates[0] if aggregates else None

    return {
        "schema_version": "retrieval_opt_summary_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "inputs": [str(path) for path in inputs],
        "record_count": len(records),
        "successful_record_count": sum(1 for r in records if r.success),
        "failed_record_count": sum(1 for r in records if not r.success),
        "candidates_with_success": len(latest_rows),
        "latest_success_by_candidate": latest_rows,
        "aggregates_by_candidate": aggregates,
        "winner": {
            "basis": "latest_success",
            "latest_success": winner_latest,
            "best_mean_objective": winner_mean,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        action="append",
        required=True,
        help="Input optimization JSONL (repeatable)",
    )
    parser.add_argument("--output", required=True, help="Output summary JSON path")
    args = parser.parse_args()

    input_paths = [Path(path) for path in args.input]
    output_path = Path(args.output)

    records = _load_records(input_paths)
    summary = summarize(records, input_paths)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)

    latest = summary["winner"]["latest_success"]
    if latest:
        print(
            "winner_latest="
            f"{latest['candidate']} objective={latest['objective']:.6f} "
            f"source={latest['source_file']}:{latest['line_number']}"
        )
    else:
        print("winner_latest=none")

    print(
        "records="
        f"{summary['record_count']} success={summary['successful_record_count']} "
        f"failed={summary['failed_record_count']}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
