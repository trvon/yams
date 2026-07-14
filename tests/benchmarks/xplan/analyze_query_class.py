#!/usr/bin/env python3
"""Offline per-query RR analysis from xplan retrieval_quality debug.jsonl.

Usage:
  python3 tests/benchmarks/xplan/analyze_query_class.py \\
      build/benchmarks/search_vector_weight_0_20_multicorp/STAMP \\
      --compare scifact_v030 scifact_v020

  python3 tests/benchmarks/xplan/analyze_query_class.py ARM_DIR --hybrid-vs-keyword

  python3 tests/benchmarks/xplan/analyze_query_class.py RUN_DIR \
      --route-calibration global_ann_c32 routed_margin020_min1

  python3 tests/benchmarks/xplan/analyze_query_class.py RUN_DIR \
      --shadow-calibration shadow_margin020_min1
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from collections.abc import Iterator
from pathlib import Path
from typing import Any


def _as_int(value: Any) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return 0.0


def _corpus_from_relevant_docs(relevant_docs: set[str]) -> str:
    corpora = {doc.split("__", 1)[0] for doc in relevant_docs if "__" in doc}
    return next(iter(corpora)) if len(corpora) == 1 else "unknown"


def _tab_set(value: Any) -> set[str]:
    return {part for part in str(value or "").split("\t") if part}


def _read_lines(path: Path) -> Iterator[str]:
    with path.open(encoding="utf-8") as handle:
        yield from handle


def load_by_type(debug_path: Path) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    if not debug_path.is_file():
        return out
    for line in _read_lines(debug_path):
        o = json.loads(line)
        st = o.get("search_type") or ""
        if st not in {"hybrid", "keyword", "grep"}:
            continue
        rel = set(map(str, o.get("relevant_doc_ids") or []))
        ret = [str(x) for x in (o.get("returned_doc_ids") or [])]
        rr = 0.0
        rank = None
        for i, d in enumerate(ret, 1):
            if d in rel:
                rr = 1.0 / i
                rank = i
                break
        q = o.get("query") or ""
        tokens = re.findall(r"[A-Za-z0-9\-]+", q.lower())
        trace = o.get("relevant_decision_trace") or {}
        trace_docs = trace.get("relevant_docs") if isinstance(trace, dict) else []
        pre_fusion_relevant = {
            str(doc.get("doc_id"))
            for doc in trace_docs or []
            if isinstance(doc, dict) and doc.get("in_pre_fusion") is True
        }
        vector_unique_relevant = {
            str(doc.get("doc_id"))
            for doc in trace_docs or []
            if isinstance(doc, dict)
            and set(doc.get("component_top_hits") or []) == {"vector"}
        }
        stats = o.get("search_stats") or {}
        if not isinstance(stats, dict):
            stats = {}
        exact_work = _as_int(stats.get("vector_search_exact_distance_evaluations_actual"))
        out.setdefault(st, []).append(
            {
                "query": q,
                "corpus": _corpus_from_relevant_docs(rel),
                "rr": rr,
                "rank": rank,
                "hit": rr > 0,
                "n_tok": len(tokens),
                "has_digit": any(any(c.isdigit() for c in t) for t in tokens),
                "pre_fusion_relevant": pre_fusion_relevant,
                "vector_unique_relevant": vector_unique_relevant,
                "narrow_applied": str(
                    stats.get("topology_weak_query_narrow_applied", "0")
                ).lower()
                in {"1", "true", "yes", "on"},
                "route_margin": _as_float(
                    stats.get("topology_route_boundary_score_margin")
                ),
                "seed_hits": _as_int(stats.get("topology_seed_coverage_count")),
                "exact_work": exact_work,
                "shadow_evaluated": str(
                    stats.get("topology_shadow_evaluated", "0")
                ).lower()
                in {"1", "true", "yes", "on"},
                "shadow_proposed_action": str(
                    stats.get("topology_shadow_proposed_action", "global")
                ),
                "shadow_retained_doc_ids": _tab_set(
                    stats.get("topology_shadow_retained_candidate_doc_ids")
                ),
                "shadow_removed_doc_ids": _tab_set(
                    stats.get("topology_shadow_removed_candidate_doc_ids")
                ),
            }
        )
    return out


def first_rep_debug(arm_dir: Path) -> Path | None:
    reps = sorted(arm_dir.glob("rep*"))
    if reps:
        p = reps[0] / "debug.jsonl"
        return p if p.exists() else None
    p = arm_dir / "debug.jsonl"
    return p if p.exists() else None


def all_rep_debugs(arm_dir: Path) -> dict[str, Path]:
    reps = {
        rep.name: rep / "debug.jsonl"
        for rep in sorted(arm_dir.glob("rep*"))
        if (rep / "debug.jsonl").exists()
    }
    if reps:
        return reps
    debug = arm_dir / "debug.jsonl"
    return {"single": debug} if debug.exists() else {}


def mean(xs: list[float]) -> float:
    return statistics.mean(xs) if xs else float("nan")


def _summarize_route_rows(rows: list[dict[str, Any]]) -> dict[str, float | int]:
    protected = sum(len(row["protected"]) for row in rows)
    missed = sum(len(row["missed"]) for row in rows)
    if not rows:
        return {
            "calibration_queries": 0,
            "protected_candidates": 0,
            "missed_protected_candidates": 0,
            "misses_per_thousand": 0.0,
            "mean_rr_delta": 0.0,
            "mean_exact_work_delta": 0.0,
            "mean_route_margin": 0.0,
            "mean_seed_hits": 0.0,
        }
    return {
        "calibration_queries": len(rows),
        "protected_candidates": protected,
        "missed_protected_candidates": missed,
        "misses_per_thousand": (1000.0 * missed / protected) if protected else 0.0,
        "mean_rr_delta": mean([float(row["rr_delta"]) for row in rows]),
        "mean_exact_work_delta": mean([float(row["exact_work_delta"]) for row in rows]),
        "mean_route_margin": mean([float(row["route_margin"]) for row in rows]),
        "mean_seed_hits": mean([float(row["seed_hits"]) for row in rows]),
    }


def paired_route_risk(
    baseline: list[dict[str, Any]], routed: list[dict[str, Any]]
) -> dict[str, dict[str, float | int]]:
    """Summarize admitted narrowing loss relative to paired global ANN queries.

    Judged-relevant vector candidates that no other global component produced are
    the protected set. Queries without a vector-unique global rescue cannot
    calibrate route miss risk and are excluded. A routed candidate retained by
    any component is safe because lexical/NF fallback remains intact. The output
    fields map directly to the Lean
    `RouteRiskObservation` counts.
    """

    baseline_by_key = {
        (row.get("repeat"), row["corpus"], row["query"]): row for row in baseline
    }
    rows: list[dict[str, Any]] = []
    for current in routed:
        if not current.get("narrow_applied"):
            continue
        key = (current.get("repeat"), current["corpus"], current["query"])
        before = baseline_by_key.get(key)
        if before is None:
            continue
        protected = set(before.get("vector_unique_relevant") or set())
        if not protected:
            continue
        retained = set(current.get("pre_fusion_relevant") or set())
        rows.append(
            {
                "corpus": current["corpus"],
                "protected": protected,
                "missed": protected - retained,
                "rr_delta": float(current["rr"]) - float(before["rr"]),
                "exact_work_delta": int(current["exact_work"])
                - int(before["exact_work"]),
                "route_margin": float(current["route_margin"]),
                "seed_hits": int(current["seed_hits"]),
            }
        )

    summaries = {"all": _summarize_route_rows(rows)}
    for corpus in sorted({str(row["corpus"]) for row in rows}):
        summaries[corpus] = _summarize_route_rows(
            [row for row in rows if row["corpus"] == corpus]
        )
    return summaries


def _summarize_shadow_rows(rows: list[dict[str, Any]]) -> dict[str, float | int]:
    protected = sum(len(row["protected"]) for row in rows)
    missed = sum(len(row["missed"]) for row in rows)
    if not rows:
        return {
            "calibration_queries": 0,
            "protected_candidates": 0,
            "missed_protected_candidates": 0,
            "misses_per_thousand": 0.0,
            "mean_global_rr": 0.0,
            "mean_exact_work": 0.0,
            "mean_projected_removed_candidates": 0.0,
            "mean_route_margin": 0.0,
            "mean_seed_hits": 0.0,
        }
    return {
        "calibration_queries": len(rows),
        "protected_candidates": protected,
        "missed_protected_candidates": missed,
        "misses_per_thousand": (1000.0 * missed / protected) if protected else 0.0,
        "mean_global_rr": mean([float(row["global_rr"]) for row in rows]),
        "mean_exact_work": mean([float(row["exact_work"]) for row in rows]),
        "mean_projected_removed_candidates": mean(
            [float(row["projected_removed_candidates"]) for row in rows]
        ),
        "mean_route_margin": mean([float(row["route_margin"]) for row in rows]),
        "mean_seed_hits": mean([float(row["seed_hits"]) for row in rows]),
    }


def shadow_route_risk(
    records: list[dict[str, Any]],
) -> dict[str, dict[str, float | int]]:
    """Calibrate proposed narrowing from one global-result shadow arm."""

    rows: list[dict[str, Any]] = []
    for record in records:
        if (
            not record.get("shadow_evaluated")
            or record.get("shadow_proposed_action") != "narrow"
        ):
            continue
        protected = set(record.get("vector_unique_relevant") or set())
        if not protected:
            continue
        removed = set(record.get("shadow_removed_doc_ids") or set())
        rows.append(
            {
                "corpus": record["corpus"],
                "protected": protected,
                "missed": protected & removed,
                "global_rr": float(record["rr"]),
                "exact_work": int(record["exact_work"]),
                "projected_removed_candidates": len(removed),
                "route_margin": float(record["route_margin"]),
                "seed_hits": int(record["seed_hits"]),
            }
        )

    summaries = {"all": _summarize_shadow_rows(rows)}
    for corpus in sorted({str(row["corpus"]) for row in rows}):
        summaries[corpus] = _summarize_shadow_rows(
            [row for row in rows if row["corpus"] == corpus]
        )
    return summaries


def compare_arms(a_dir: Path, b_dir: Path) -> None:
    a_debugs = all_rep_debugs(a_dir)
    b_debugs = all_rep_debugs(b_dir)
    common_repeats = sorted(set(a_debugs) & set(b_debugs))
    if not common_repeats:
        missing = a_dir if not a_debugs else b_dir
        print(f"no debug.jsonl under {missing}")
        return
    ha: dict[tuple[str, str], dict[str, Any]] = {}
    hb: dict[tuple[str, str], dict[str, Any]] = {}
    for repeat in common_repeats:
        for row in load_by_type(a_debugs[repeat]).get("hybrid", []):
            ha[(repeat, row["query"])] = row
        for row in load_by_type(b_debugs[repeat]).get("hybrid", []):
            hb[(repeat, row["query"])] = row
    common = sorted(set(ha) & set(hb))
    deltas = [
        (hb[key]["rr"] - ha[key]["rr"], key[1], ha[key], hb[key])
        for key in common
    ]
    deltas.sort(key=lambda item: (item[0], item[1]))
    print(
        f"compare {a_dir.name} -> {b_dir.name}  n={len(common)} "
        f"meanΔrr={mean([d[0] for d in deltas]):.4f} "
        f"win={sum(1 for d in deltas if d[0] > 0)} "
        f"lose={sum(1 for d in deltas if d[0] < 0)} "
        f"tie={sum(1 for d in deltas if d[0] == 0)}"
    )
    print("  hurts most (B worse):")
    for da, q, qa, qb in deltas[:5]:
        print(
            f"    Δrr={da:+.3f} rA={qa['rank']} rB={qb['rank']} "
            f"ntok={qa['n_tok']} digit={qa['has_digit']} | {q[:90]}"
        )
    print("  helps most (B better):")
    for da, q, qa, qb in deltas[-5:][::-1]:
        print(
            f"    Δrr={da:+.3f} rA={qa['rank']} rB={qb['rank']} "
            f"ntok={qa['n_tok']} digit={qa['has_digit']} | {q[:90]}"
        )
    for name, pred in [
        ("short<=8", lambda x: x["n_tok"] <= 8),
        ("long>8", lambda x: x["n_tok"] > 8),
        ("has_digit", lambda x: x["has_digit"]),
        ("no_digit", lambda x: not x["has_digit"]),
    ]:
        bucket = [d for d in deltas if pred(d[2])]
        if not bucket:
            continue
        print(
            f"  bucket {name}: n={len(bucket)} meanΔrr={mean([d[0] for d in bucket]):.4f} "
            f"hitA={mean([1.0 if d[2]['hit'] else 0.0 for d in bucket]):.2f} "
            f"hitB={mean([1.0 if d[3]['hit'] else 0.0 for d in bucket]):.2f}"
        )


def hybrid_vs_keyword(arm_dir: Path) -> None:
    dbg = first_rep_debug(arm_dir)
    if not dbg:
        print(f"no debug.jsonl under {arm_dir}")
        return
    by = load_by_type(dbg)
    hy = {x["query"]: x for x in by.get("hybrid", [])}
    kw = {x["query"]: x for x in by.get("keyword", [])}
    common = sorted(set(hy) & set(kw))
    deltas = [(hy[q]["rr"] - kw[q]["rr"], q, hy[q], kw[q]) for q in common]
    print(
        f"hybrid vs keyword {arm_dir.name}: n={len(common)} meanΔrr={mean([d[0] for d in deltas]):.4f} "
        f"win={sum(1 for d in deltas if d[0] > 0)} lose={sum(1 for d in deltas if d[0] < 0)} "
        f"tie={sum(1 for d in deltas if d[0] == 0)}"
    )
    print("  hybrid hurts most:")
    for da, q, h, k in sorted(deltas)[:5]:
        print(f"    Δ={da:+.3f} hy={h['rr']:.3f} kw={k['rr']:.3f} | {q[:90]}")
    print("  hybrid helps most:")
    for da, q, h, k in sorted(deltas)[-5:][::-1]:
        print(f"    Δ={da:+.3f} hy={h['rr']:.3f} kw={k['rr']:.3f} | {q[:90]}")


def route_calibration(baseline_dir: Path, routed_dir: Path) -> None:
    baseline_debugs = all_rep_debugs(baseline_dir)
    routed_debugs = all_rep_debugs(routed_dir)
    common_repeats = sorted(set(baseline_debugs) & set(routed_debugs))
    if not common_repeats:
        missing = baseline_dir if not baseline_debugs else routed_dir
        print(f"no debug.jsonl under {missing}")
        return
    baseline: list[dict[str, Any]] = []
    routed: list[dict[str, Any]] = []
    for repeat in common_repeats:
        for row in load_by_type(baseline_debugs[repeat]).get("hybrid", []):
            baseline.append({**row, "repeat": repeat})
        for row in load_by_type(routed_debugs[repeat]).get("hybrid", []):
            routed.append({**row, "repeat": repeat})
    print(json.dumps(paired_route_risk(baseline, routed), indent=2, sort_keys=True))


def shadow_calibration(shadow_dir: Path) -> None:
    debug_paths = all_rep_debugs(shadow_dir)
    if not debug_paths:
        print(f"no debug.jsonl under {shadow_dir}")
        return
    records: list[dict[str, Any]] = []
    for repeat, debug_path in sorted(debug_paths.items()):
        for row in load_by_type(debug_path).get("hybrid", []):
            records.append({**row, "repeat": repeat})
    print(json.dumps(shadow_route_risk(records), indent=2, sort_keys=True))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "root", type=Path, help="xplan run dir, arms/ dir, or single arm dir"
    )
    ap.add_argument(
        "--compare",
        nargs=2,
        metavar=("ARM_A", "ARM_B"),
        help="pair of arm names under root/arms",
    )
    ap.add_argument(
        "--hybrid-vs-keyword",
        action="store_true",
        help="analyze one arm (or all arms)",
    )
    ap.add_argument(
        "--route-calibration",
        nargs=2,
        metavar=("GLOBAL_ARM", "ROUTED_ARM"),
        help="measure protected global candidates lost by admitted narrowing",
    )
    ap.add_argument(
        "--shadow-calibration",
        metavar="SHADOW_ARM",
        help="measure projected route loss from one non-mutating shadow arm",
    )
    args = ap.parse_args()
    root: Path = args.root

    arms_root = root / "arms" if (root / "arms").is_dir() else root
    if args.compare:
        a, b = args.compare
        compare_arms(arms_root / a, arms_root / b)
        return 0

    if args.route_calibration:
        baseline, routed = args.route_calibration
        route_calibration(arms_root / baseline, arms_root / routed)
        return 0

    if args.shadow_calibration:
        shadow_calibration(arms_root / args.shadow_calibration)
        return 0

    if args.hybrid_vs_keyword:
        if (arms_root / "debug.jsonl").exists() or any(arms_root.glob("rep*")):
            hybrid_vs_keyword(arms_root)
        else:
            for arm in sorted(p for p in arms_root.iterdir() if p.is_dir()):
                if first_rep_debug(arm):
                    hybrid_vs_keyword(arm)
                    print()
        return 0

    ap.error(
        "pass --compare ARM_A ARM_B, --route-calibration GLOBAL ROUTED, "
        "--shadow-calibration SHADOW, or --hybrid-vs-keyword"
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
