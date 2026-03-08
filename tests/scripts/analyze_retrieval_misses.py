#!/usr/bin/env python3
"""Analyze retrieval misses and compare candidate runs against a baseline."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path


def _parse_candidate_spec(raw: str) -> tuple[str, Path]:
    if "=" not in raw:
        raise ValueError(
            f"Invalid --input '{raw}'. Expected format: name=/path/to/debug.jsonl"
        )
    name, path = raw.split("=", 1)
    name = name.strip()
    path = path.strip()
    if not name:
        raise ValueError(f"Invalid --input '{raw}': empty candidate name")
    if not path:
        raise ValueError(f"Invalid --input '{raw}': empty path")
    return name, Path(path)


def _normalize_doc_id(raw: str) -> str:
    value = (raw or "").strip()
    if value.endswith(".txt"):
        return value[:-4]
    return value


def _parse_tab_list(raw: str) -> list[str]:
    if not raw:
        return []
    return [_normalize_doc_id(item) for item in raw.split("\t") if item.strip()]


def _has_overlap(left: set[str], right: set[str]) -> bool:
    if not left or not right:
        return False
    return any(item in right for item in left)


@dataclass
class QueryEval:
    query: str
    hit_top_k: bool
    hit_any_final: bool
    miss_type: str
    relevant_count: int
    top_k_returned: list[str]
    relevant_ids: list[str]


def _classify_miss(entry: dict, k: int) -> QueryEval:
    query = str(entry.get("query", "")).strip()
    relevant = {
        _normalize_doc_id(v)
        for v in entry.get("relevant_doc_ids", [])
        if isinstance(v, str) and v.strip()
    }
    if not relevant:
        relevant = {
            _normalize_doc_id(v)
            for v in entry.get("relevant_files", [])
            if isinstance(v, str) and v.strip()
        }

    returned_doc_ids = [_normalize_doc_id(v) for v in entry.get("returned_doc_ids", [])]
    top_k_returned = returned_doc_ids[:k]

    stats = entry.get("search_stats", {})
    if not isinstance(stats, dict):
        stats = {}

    has_stage_trace = str(stats.get("trace_enabled", "0")) == "1"
    pre_fusion = _parse_tab_list(str(stats.get("trace_pre_fusion_doc_ids", "")))
    post_fusion = _parse_tab_list(str(stats.get("trace_post_fusion_doc_ids", "")))
    post_fusion_top = _parse_tab_list(
        str(stats.get("trace_post_fusion_top_doc_ids", ""))
    )
    post_graph_top = _parse_tab_list(str(stats.get("trace_post_graph_top_doc_ids", "")))
    final_all = _parse_tab_list(str(stats.get("trace_final_doc_ids", "")))
    final_top = _parse_tab_list(str(stats.get("trace_final_top_doc_ids", "")))

    if not final_all:
        final_all = returned_doc_ids
    if not final_top:
        final_top = top_k_returned
    if not post_fusion:
        post_fusion = final_all
    if not post_fusion_top:
        post_fusion_top = post_fusion[:k]
    if not post_graph_top:
        post_graph_top = post_fusion_top

    relevant_in_final_top = _has_overlap(set(final_top[:k]), relevant)
    relevant_in_final_any = _has_overlap(set(final_all), relevant)

    if relevant_in_final_top:
        miss_type = "hit"
    elif not has_stage_trace:
        miss_type = "cutoff_miss" if relevant_in_final_any else "no_relevant_returned"
    else:
        relevant_in_pre = _has_overlap(set(pre_fusion), relevant)
        relevant_in_post_fusion_any = _has_overlap(set(post_fusion), relevant)
        relevant_in_post_fusion_top = _has_overlap(set(post_fusion_top[:k]), relevant)
        relevant_in_post_graph_top = _has_overlap(set(post_graph_top[:k]), relevant)

        if not relevant_in_pre:
            miss_type = "candidate_generation_miss"
        elif not relevant_in_post_fusion_any:
            miss_type = "fusion_drop_miss"
        elif relevant_in_post_fusion_top and not relevant_in_post_graph_top:
            miss_type = "graph_rerank_miss"
        elif relevant_in_post_graph_top and not relevant_in_final_top:
            miss_type = "cross_rerank_miss"
        elif relevant_in_final_any:
            miss_type = "cutoff_miss"
        else:
            miss_type = "post_fusion_ranking_miss"

    return QueryEval(
        query=query,
        hit_top_k=relevant_in_final_top,
        hit_any_final=relevant_in_final_any,
        miss_type=miss_type,
        relevant_count=len(relevant),
        top_k_returned=top_k_returned,
        relevant_ids=sorted(relevant),
    )


def _load_debug_run(path: Path, search_type: str, k: int) -> dict[str, QueryEval]:
    out: dict[str, QueryEval] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            query = str(entry.get("query", "")).strip()
            if not query or query.startswith("__"):
                continue
            if search_type:
                if (
                    str(entry.get("search_type", "")).strip().lower()
                    != search_type.lower()
                ):
                    continue
            out[query] = _classify_miss(entry, k)
    return out


def _compare_runs(
    baseline_name: str,
    baseline: dict[str, QueryEval],
    candidate_name: str,
    candidate: dict[str, QueryEval],
) -> dict:
    shared_queries = sorted(set(baseline.keys()) & set(candidate.keys()))
    baseline_miss = Counter()
    candidate_miss = Counter()
    recovered = 0
    regressed = 0
    recovered_by_baseline_miss = Counter()
    regressed_by_candidate_miss = Counter()
    rows = []

    baseline_hits = 0
    candidate_hits = 0

    for query in shared_queries:
        b = baseline[query]
        c = candidate[query]

        if b.hit_top_k:
            baseline_hits += 1
        if c.hit_top_k:
            candidate_hits += 1

        baseline_miss[b.miss_type] += 1
        candidate_miss[c.miss_type] += 1

        status = "unchanged"
        if (not b.hit_top_k) and c.hit_top_k:
            recovered += 1
            status = "recovered"
            recovered_by_baseline_miss[b.miss_type] += 1
        elif b.hit_top_k and (not c.hit_top_k):
            regressed += 1
            status = "regressed"
            regressed_by_candidate_miss[c.miss_type] += 1

        rows.append(
            {
                "query": query,
                "status": status,
                "baseline_hit_top_k": b.hit_top_k,
                "candidate_hit_top_k": c.hit_top_k,
                "baseline_miss_type": b.miss_type,
                "candidate_miss_type": c.miss_type,
                "baseline_relevant_count": b.relevant_count,
                "candidate_relevant_count": c.relevant_count,
            }
        )

    total = len(shared_queries)
    baseline_hit_rate = (baseline_hits / total) if total else 0.0
    candidate_hit_rate = (candidate_hits / total) if total else 0.0

    return {
        "baseline": baseline_name,
        "candidate": candidate_name,
        "num_queries": total,
        "baseline_hit_rate": baseline_hit_rate,
        "candidate_hit_rate": candidate_hit_rate,
        "recovered_queries": recovered,
        "regressed_queries": regressed,
        "net_hit_gain": recovered - regressed,
        "baseline_miss_breakdown": dict(sorted(baseline_miss.items())),
        "candidate_miss_breakdown": dict(sorted(candidate_miss.items())),
        "recovered_by_baseline_miss": dict(sorted(recovered_by_baseline_miss.items())),
        "regressed_by_candidate_miss": dict(
            sorted(regressed_by_candidate_miss.items())
        ),
        "query_rows": rows,
    }


def _write_csv(path: Path, comparisons: list[dict]) -> None:
    fieldnames = [
        "baseline",
        "candidate",
        "num_queries",
        "baseline_hit_rate",
        "candidate_hit_rate",
        "recovered_queries",
        "regressed_queries",
        "net_hit_gain",
        "baseline_candidate_generation_miss",
        "baseline_fusion_drop_miss",
        "baseline_graph_rerank_miss",
        "baseline_cross_rerank_miss",
        "baseline_cutoff_miss",
        "candidate_candidate_generation_miss",
        "candidate_fusion_drop_miss",
        "candidate_graph_rerank_miss",
        "candidate_cross_rerank_miss",
        "candidate_cutoff_miss",
    ]

    def _count(source: dict[str, int], key: str) -> int:
        try:
            return int(source.get(key, 0))
        except (TypeError, ValueError):
            return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for comp in comparisons:
            b = comp.get("baseline_miss_breakdown", {})
            c = comp.get("candidate_miss_breakdown", {})
            writer.writerow(
                {
                    "baseline": comp.get("baseline", ""),
                    "candidate": comp.get("candidate", ""),
                    "num_queries": comp.get("num_queries", 0),
                    "baseline_hit_rate": comp.get("baseline_hit_rate", 0.0),
                    "candidate_hit_rate": comp.get("candidate_hit_rate", 0.0),
                    "recovered_queries": comp.get("recovered_queries", 0),
                    "regressed_queries": comp.get("regressed_queries", 0),
                    "net_hit_gain": comp.get("net_hit_gain", 0),
                    "baseline_candidate_generation_miss": _count(
                        b, "candidate_generation_miss"
                    ),
                    "baseline_fusion_drop_miss": _count(b, "fusion_drop_miss"),
                    "baseline_graph_rerank_miss": _count(b, "graph_rerank_miss"),
                    "baseline_cross_rerank_miss": _count(b, "cross_rerank_miss"),
                    "baseline_cutoff_miss": _count(b, "cutoff_miss"),
                    "candidate_candidate_generation_miss": _count(
                        c, "candidate_generation_miss"
                    ),
                    "candidate_fusion_drop_miss": _count(c, "fusion_drop_miss"),
                    "candidate_graph_rerank_miss": _count(c, "graph_rerank_miss"),
                    "candidate_cross_rerank_miss": _count(c, "cross_rerank_miss"),
                    "candidate_cutoff_miss": _count(c, "cutoff_miss"),
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        action="append",
        required=True,
        help="Candidate debug file as name=/path/to/debug.jsonl (repeatable)",
    )
    parser.add_argument("--baseline", required=True, help="Baseline candidate name")
    parser.add_argument(
        "--k", type=int, default=10, help="Top-k cutoff used for scoring"
    )
    parser.add_argument(
        "--search-type",
        default="hybrid",
        help="Filter debug rows by search_type (default: hybrid)",
    )
    parser.add_argument("--json-out", required=True, help="Output JSON report")
    parser.add_argument("--csv-out", required=True, help="Output CSV summary")
    args = parser.parse_args()

    run_specs = dict(_parse_candidate_spec(item) for item in args.input)
    if args.baseline not in run_specs:
        raise ValueError(f"Baseline '{args.baseline}' not present in --input list")

    runs = {
        name: _load_debug_run(path, args.search_type, args.k)
        for name, path in run_specs.items()
    }
    baseline_run = runs[args.baseline]

    comparisons = []
    for candidate_name, candidate_run in runs.items():
        if candidate_name == args.baseline:
            continue
        comparisons.append(
            _compare_runs(args.baseline, baseline_run, candidate_name, candidate_run)
        )

    comparisons.sort(key=lambda item: item.get("net_hit_gain", 0), reverse=True)

    output = {
        "schema_version": "retrieval_miss_analysis_v1",
        "baseline": args.baseline,
        "k": args.k,
        "search_type": args.search_type,
        "inputs": {name: str(path) for name, path in run_specs.items()},
        "comparisons": comparisons,
    }

    json_out = Path(args.json_out)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(Path(args.csv_out), comparisons)

    print(
        f"baseline={args.baseline} candidates={len(comparisons)} "
        f"best={(comparisons[0]['candidate'] if comparisons else 'none')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
