#!/usr/bin/env python3
"""Offline per-query RR analysis from xplan retrieval_quality debug.jsonl.

Usage:
  python3 tests/benchmarks/xplan/analyze_query_class.py \\
      build/benchmarks/search_vector_weight_0_20_multicorp/STAMP \\
      --compare scifact_v030 scifact_v020

  python3 tests/benchmarks/xplan/analyze_query_class.py ARM_DIR --hybrid-vs-keyword
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path
from typing import Any


def load_by_type(debug_path: Path) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    if not debug_path.is_file():
        return out
    for line in debug_path.open():
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
        out.setdefault(st, []).append(
            {
                "query": q,
                "rr": rr,
                "rank": rank,
                "hit": rr > 0,
                "n_tok": len(tokens),
                "has_digit": any(any(c.isdigit() for c in t) for t in tokens),
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


def mean(xs: list[float]) -> float:
    return statistics.mean(xs) if xs else float("nan")


def compare_arms(a_dir: Path, b_dir: Path) -> None:
    a_debug = first_rep_debug(a_dir)
    b_debug = first_rep_debug(b_dir)
    if a_debug is None or b_debug is None:
        missing = a_dir if a_debug is None else b_dir
        print(f"no debug.jsonl under {missing}")
        return
    a = load_by_type(a_debug)
    b = load_by_type(b_debug)
    ha = {x["query"]: x for x in a.get("hybrid", [])}
    hb = {x["query"]: x for x in b.get("hybrid", [])}
    common = sorted(set(ha) & set(hb))
    deltas = [(hb[q]["rr"] - ha[q]["rr"], q, ha[q], hb[q]) for q in common]
    deltas.sort()
    print(
        f"compare {a_dir.name} -> {b_dir.name}  n={len(common)} "
        f"meanΔrr={mean([d[0] for d in deltas]):.4f}"
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("root", type=Path, help="xplan run dir, arms/ dir, or single arm dir")
    ap.add_argument(
        "--compare",
        nargs=2,
        metavar=("ARM_A", "ARM_B"),
        help="pair of arm names under root/arms",
    )
    ap.add_argument("--hybrid-vs-keyword", action="store_true", help="analyze one arm (or all arms)")
    args = ap.parse_args()
    root: Path = args.root

    arms_root = root / "arms" if (root / "arms").is_dir() else root
    if args.compare:
        a, b = args.compare
        compare_arms(arms_root / a, arms_root / b)
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

    ap.error("pass --compare ARM_A ARM_B or --hybrid-vs-keyword")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
