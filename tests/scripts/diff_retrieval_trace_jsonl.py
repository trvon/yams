#!/usr/bin/env python3
"""Diff per-query retrieval trace JSONL between baseline and candidate."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class QuerySnapshot:
    query_index: int
    query: str
    relevant_doc_ids: set[str]
    returned_doc_ids: list[str]
    first_relevant_rank: int
    hit: bool
    duplicate_topk: bool
    semantic_rescue_rate: float
    fusion_dropped_count: float
    vector_only_below_threshold: float
    vector_only_docs: float


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_relevant_rank(
    returned_doc_ids: list[str], relevant_doc_ids: set[str]
) -> int:
    if not returned_doc_ids or not relevant_doc_ids:
        return -1
    for idx, doc_id in enumerate(returned_doc_ids, start=1):
        if doc_id in relevant_doc_ids:
            return idx
    return -1


def _load_trace(path: Path, search_type: str) -> dict[int, QuerySnapshot]:
    snapshots: dict[int, QuerySnapshot] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                continue
            if row.get("search_type") != search_type:
                continue
            if "query_index" not in row:
                continue
            query_index = int(row.get("query_index", -1))
            if query_index < 0:
                continue
            query = str(row.get("query", ""))
            if query.startswith("__diag_"):
                continue

            relevant_doc_ids = {
                str(v).strip()
                for v in row.get("relevant_doc_ids", [])
                if isinstance(v, str) and str(v).strip()
            }
            returned_doc_ids = [
                str(v).strip()
                for v in row.get("returned_doc_ids", [])
                if isinstance(v, str) and str(v).strip()
            ]

            first_rank = _first_relevant_rank(returned_doc_ids, relevant_doc_ids)
            duplicate_topk = len(returned_doc_ids) != len(set(returned_doc_ids))

            stats = row.get("search_stats", {})
            if not isinstance(stats, dict):
                stats = {}

            prefusion = {}
            prefusion_raw = stats.get("trace_prefusion_signal_summary_json")
            if isinstance(prefusion_raw, str) and prefusion_raw:
                try:
                    parsed = json.loads(prefusion_raw)
                    if isinstance(parsed, dict):
                        prefusion = parsed
                except json.JSONDecodeError:
                    prefusion = {}

            snapshots[query_index] = QuerySnapshot(
                query_index=query_index,
                query=query,
                relevant_doc_ids=relevant_doc_ids,
                returned_doc_ids=returned_doc_ids,
                first_relevant_rank=first_rank,
                hit=first_rank > 0,
                duplicate_topk=duplicate_topk,
                semantic_rescue_rate=_as_float(stats.get("semantic_rescue_rate"), 0.0),
                fusion_dropped_count=_as_float(
                    stats.get("trace_fusion_dropped_count"), 0.0
                ),
                vector_only_below_threshold=_as_float(
                    prefusion.get("vector_only_below_threshold"), 0.0
                ),
                vector_only_docs=_as_float(prefusion.get("vector_only_docs"), 0.0),
            )
    return snapshots


def _classify(baseline: QuerySnapshot, candidate: QuerySnapshot) -> str:
    if baseline.hit and not candidate.hit:
        return "lost_hit"
    if not baseline.hit and candidate.hit:
        return "recovered_hit"
    if not baseline.hit and not candidate.hit:
        return "both_miss"
    if candidate.first_relevant_rank < baseline.first_relevant_rank:
        return "rank_improved"
    if candidate.first_relevant_rank > baseline.first_relevant_rank:
        return "rank_regressed"
    return "rank_unchanged"


def _build_rows(
    baseline_by_query: dict[int, QuerySnapshot],
    candidate_by_query: dict[int, QuerySnapshot],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for query_index in sorted(set(baseline_by_query) & set(candidate_by_query)):
        baseline = baseline_by_query[query_index]
        candidate = candidate_by_query[query_index]
        outcome = _classify(baseline, candidate)
        row = {
            "query_index": query_index,
            "query": candidate.query or baseline.query,
            "outcome": outcome,
            "baseline_hit": baseline.hit,
            "candidate_hit": candidate.hit,
            "baseline_first_relevant_rank": baseline.first_relevant_rank,
            "candidate_first_relevant_rank": candidate.first_relevant_rank,
            "delta_first_relevant_rank": (
                baseline.first_relevant_rank - candidate.first_relevant_rank
                if baseline.first_relevant_rank > 0
                and candidate.first_relevant_rank > 0
                else 0
            ),
            "baseline_duplicate_topk": baseline.duplicate_topk,
            "candidate_duplicate_topk": candidate.duplicate_topk,
            "delta_semantic_rescue_rate": (
                candidate.semantic_rescue_rate - baseline.semantic_rescue_rate
            ),
            "delta_fusion_dropped_count": (
                candidate.fusion_dropped_count - baseline.fusion_dropped_count
            ),
            "delta_vector_only_below_threshold": (
                candidate.vector_only_below_threshold
                - baseline.vector_only_below_threshold
            ),
            "delta_vector_only_docs": candidate.vector_only_docs
            - baseline.vector_only_docs,
        }
        rows.append(row)
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "query_index",
        "query",
        "outcome",
        "baseline_hit",
        "candidate_hit",
        "baseline_first_relevant_rank",
        "candidate_first_relevant_rank",
        "delta_first_relevant_rank",
        "baseline_duplicate_topk",
        "candidate_duplicate_topk",
        "delta_semantic_rescue_rate",
        "delta_fusion_dropped_count",
        "delta_vector_only_below_threshold",
        "delta_vector_only_docs",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-debug", required=True, help="Baseline trace JSONL")
    parser.add_argument(
        "--candidate-debug", required=True, help="Candidate trace JSONL"
    )
    parser.add_argument(
        "--search-type", default="hybrid", help="Search type to compare"
    )
    parser.add_argument("--json-out", required=True, help="Output JSON path")
    parser.add_argument("--csv-out", required=True, help="Output CSV path")
    args = parser.parse_args()

    baseline_path = Path(args.baseline_debug)
    candidate_path = Path(args.candidate_debug)

    baseline = _load_trace(baseline_path, args.search_type)
    candidate = _load_trace(candidate_path, args.search_type)
    rows = _build_rows(baseline, candidate)
    counts = Counter(row["outcome"] for row in rows)

    output = {
        "schema_version": "retrieval_trace_delta_v1",
        "baseline_debug": str(baseline_path),
        "candidate_debug": str(candidate_path),
        "search_type": args.search_type,
        "query_pairs_compared": len(rows),
        "outcome_counts": dict(counts),
        "rows": rows,
    }

    json_out = Path(args.json_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(Path(args.csv_out), rows)

    print(
        f"query_pairs={len(rows)} outcomes="
        f"{','.join(f'{k}:{v}' for k, v in sorted(counts.items())) or 'none'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
