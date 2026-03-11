#!/usr/bin/env python3
"""Compute per-candidate metric deltas versus a baseline from matrix JSON."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def _index_by_candidate(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        candidate = str(row.get("candidate", "")).strip()
        if candidate:
            out[candidate] = row
    return out


def _f(row: dict[str, Any], key: str) -> float:
    value = row.get(key, 0.0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_delta_rows(
    rows: list[dict[str, Any]], baseline_candidate: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    by_candidate = _index_by_candidate(rows)
    if baseline_candidate not in by_candidate:
        raise ValueError(f"Baseline candidate not found: {baseline_candidate}")

    baseline = by_candidate[baseline_candidate]
    metric_keys = [
        "objective",
        "precision_at_k",
        "mrr",
        "ndcg_at_k",
        "recall_at_k",
        "map",
        "hybrid_eval_ms",
        "keyword_eval_ms",
        "hybrid_keyword_mrr_delta",
        "duplicate_rate_at_k",
        "objective_penalty_hybrid_regression",
        "objective_penalty_duplicate_rate",
        "trace_coverage",
        "graph_rerank_apply_rate",
        "semantic_rescue_rate_mean",
        "vector_only_below_threshold_mean",
        "query_without_relevant_hit_rate",
        "trace_top_n",
        "trace_component_top_n",
    ]

    deltas: list[dict[str, Any]] = []
    for row in rows:
        candidate = str(row.get("candidate", "")).strip()
        out = {
            "candidate": candidate,
            "line_number": row.get("line_number"),
            "source_file": row.get("source_file"),
        }
        for key in metric_keys:
            out[key] = _f(row, key)
            out[f"delta_{key}"] = _f(row, key) - _f(baseline, key)
        deltas.append(out)

    deltas.sort(
        key=lambda r: (
            r["delta_precision_at_k"],
            r["delta_ndcg_at_k"],
            r["delta_mrr"],
            r["delta_objective"],
        ),
        reverse=True,
    )
    return baseline, deltas


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = [
        "candidate",
        "line_number",
        "source_file",
        "objective",
        "delta_objective",
        "precision_at_k",
        "delta_precision_at_k",
        "mrr",
        "delta_mrr",
        "ndcg_at_k",
        "delta_ndcg_at_k",
        "recall_at_k",
        "delta_recall_at_k",
        "map",
        "delta_map",
        "hybrid_eval_ms",
        "delta_hybrid_eval_ms",
        "keyword_eval_ms",
        "delta_keyword_eval_ms",
        "hybrid_keyword_mrr_delta",
        "delta_hybrid_keyword_mrr_delta",
        "duplicate_rate_at_k",
        "delta_duplicate_rate_at_k",
        "objective_penalty_hybrid_regression",
        "delta_objective_penalty_hybrid_regression",
        "objective_penalty_duplicate_rate",
        "delta_objective_penalty_duplicate_rate",
        "trace_coverage",
        "delta_trace_coverage",
        "graph_rerank_apply_rate",
        "delta_graph_rerank_apply_rate",
        "semantic_rescue_rate_mean",
        "delta_semantic_rescue_rate_mean",
        "vector_only_below_threshold_mean",
        "delta_vector_only_below_threshold_mean",
        "query_without_relevant_hit_rate",
        "delta_query_without_relevant_hit_rate",
        "trace_top_n",
        "delta_trace_top_n",
        "trace_component_top_n",
        "delta_trace_component_top_n",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input matrix JSON path")
    parser.add_argument("--baseline", required=True, help="Baseline candidate name")
    parser.add_argument("--json-out", required=True, help="Output delta JSON path")
    parser.add_argument("--csv-out", required=True, help="Output delta CSV path")
    args = parser.parse_args()

    input_path = Path(args.input)
    data = json.loads(input_path.read_text(encoding="utf-8"))
    matrix_rows = data.get("matrix", [])
    if not isinstance(matrix_rows, list):
        raise ValueError("Invalid matrix JSON: 'matrix' must be a list")

    baseline, deltas = _build_delta_rows(matrix_rows, args.baseline)

    output = {
        "schema_version": "retrieval_opt_delta_v1",
        "input": str(input_path),
        "baseline_candidate": args.baseline,
        "baseline": baseline,
        "rows": deltas,
    }

    json_out = Path(args.json_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(Path(args.csv_out), deltas)

    print(
        f"baseline={args.baseline} rows={len(deltas)} "
        f"top_delta_candidate={(deltas[0]['candidate'] if deltas else 'none')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
