#!/usr/bin/env python3
"""Build a candidate matrix from retrieval optimization JSONL artifacts."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class Row:
    candidate: str
    source_file: str
    line_number: int
    success: bool
    objective: float
    hybrid_precision: float
    hybrid_mrr: float
    hybrid_ndcg: float
    hybrid_recall: float
    hybrid_map: float
    hybrid_eval_ms: float
    keyword_eval_ms: float
    tuning_state: str
    tuning_reason: str
    env_overrides: dict[str, Any]


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


def _objective(row: dict[str, Any]) -> float:
    if "objective" in row:
        return _safe_float(row.get("objective"), 0.0)
    if "objective_score" in row:
        return _safe_float(row.get("objective_score"), 0.0)
    return 0.0


def _load_rows(inputs: list[Path]) -> list[Row]:
    out: list[Row] = []
    for path in inputs:
        if not path.exists():
            raise FileNotFoundError(f"Input JSONL not found: {path}")

        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                if not isinstance(data, dict):
                    continue

                candidate = str(data.get("candidate", "")).strip()
                if not candidate:
                    continue

                hybrid = data.get("hybrid", {})
                out.append(
                    Row(
                        candidate=candidate,
                        source_file=str(path),
                        line_number=line_number,
                        success=_is_success(data),
                        objective=_objective(data),
                        hybrid_precision=_safe_float(hybrid.get("precision_at_k"), 0.0),
                        hybrid_mrr=_safe_float(hybrid.get("mrr"), 0.0),
                        hybrid_ndcg=_safe_float(hybrid.get("ndcg_at_k"), 0.0),
                        hybrid_recall=_safe_float(hybrid.get("recall_at_k"), 0.0),
                        hybrid_map=_safe_float(hybrid.get("map"), 0.0),
                        hybrid_eval_ms=_safe_float(data.get("hybrid_eval_ms"), 0.0),
                        keyword_eval_ms=_safe_float(data.get("keyword_eval_ms"), 0.0),
                        tuning_state=str(data.get("tuning_state", "")),
                        tuning_reason=str(data.get("tuning_reason", "")),
                        env_overrides=data.get("env_overrides", {}),
                    )
                )
    return out


def _latest_success(rows: list[Row]) -> dict[str, Row]:
    latest: dict[str, Row] = {}
    for row in rows:
        if row.success:
            latest[row.candidate] = row
    return latest


def _matrix_rows(latest_success: dict[str, Row]) -> list[dict[str, Any]]:
    matrix: list[dict[str, Any]] = []
    for candidate, row in latest_success.items():
        matrix.append(
            {
                "candidate": candidate,
                "objective": row.objective,
                "precision_at_k": row.hybrid_precision,
                "mrr": row.hybrid_mrr,
                "ndcg_at_k": row.hybrid_ndcg,
                "recall_at_k": row.hybrid_recall,
                "map": row.hybrid_map,
                "hybrid_eval_ms": row.hybrid_eval_ms,
                "keyword_eval_ms": row.keyword_eval_ms,
                "source_file": row.source_file,
                "line_number": row.line_number,
                "tuning_state": row.tuning_state,
                "tuning_reason": row.tuning_reason,
            }
        )

    matrix.sort(
        key=lambda r: (r["precision_at_k"], r["ndcg_at_k"], r["mrr"], r["objective"]),
        reverse=True,
    )
    return matrix


def _write_csv(path: Path, matrix: list[dict[str, Any]]) -> None:
    fieldnames = [
        "candidate",
        "objective",
        "precision_at_k",
        "mrr",
        "ndcg_at_k",
        "recall_at_k",
        "map",
        "hybrid_eval_ms",
        "keyword_eval_ms",
        "source_file",
        "line_number",
        "tuning_state",
        "tuning_reason",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(matrix)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", action="append", required=True, help="Input JSONL file"
    )
    parser.add_argument("--json-out", required=True, help="Output matrix JSON path")
    parser.add_argument("--csv-out", required=True, help="Output matrix CSV path")
    args = parser.parse_args()

    input_paths = [Path(v) for v in args.input]
    rows = _load_rows(input_paths)
    latest = _latest_success(rows)
    matrix = _matrix_rows(latest)

    perfect = [r for r in matrix if r["precision_at_k"] >= 1.0]
    best = matrix[0] if matrix else None

    output = {
        "schema_version": "retrieval_opt_matrix_v1",
        "inputs": [str(p) for p in input_paths],
        "record_count": len(rows),
        "successful_latest_candidates": len(matrix),
        "perfect_precision_candidates": perfect,
        "best_precision_candidate": best,
        "matrix": matrix,
    }

    json_path = Path(args.json_out)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")

    _write_csv(Path(args.csv_out), matrix)

    if perfect:
        names = ",".join(entry["candidate"] for entry in perfect)
        print(f"perfect_precision_candidates={names}")
    elif best is not None:
        print(
            "best_precision_candidate="
            f"{best['candidate']} precision_at_k={best['precision_at_k']:.6f} "
            f"location={best['source_file']}:{best['line_number']}"
        )
    else:
        print("best_precision_candidate=none")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
