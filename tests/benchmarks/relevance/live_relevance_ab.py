#!/usr/bin/env python3
"""Live-daemon relevance A/B over the user's own corpus.

Runs a judged query set through the real `yams search` path (the running,
configured release daemon) and reports nDCG@K / Recall@K / MRR. Pass --label to
tag a run, restart the daemon under a different configuration, rerun, and diff.

Usage:
  uv run tests/benchmarks/relevance/live_relevance_ab.py \
      --queries tests/benchmarks/relevance/yams_corpus_queries.json \
      --label baseline --k 10 --type hybrid --out /tmp/relevance_baseline.json

  # Restart the daemon with the candidate typed configuration, then compare.
  uv run .../live_relevance_ab.py --label candidate --out /tmp/relevance_candidate.json
  uv run .../live_relevance_ab.py --diff /tmp/relevance_baseline.json /tmp/relevance_candidate.json
"""
import argparse
import json
import math
import subprocess
import sys
from pathlib import Path


def run_search(query, k, search_type, yams_bin, path_scope=None, use_cwd=False):
    cmd = [yams_bin, "search", query, "--limit", str(k), "--type", search_type, "--json"]
    if use_cwd:
        cmd += ["--cwd"]
    if path_scope:
        cmd += ["--path", path_scope]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(f"yams search failed ({proc.returncode}): {proc.stderr.strip()[:300]}")
    data = json.loads(proc.stdout)
    return [r.get("path", "") for r in data.get("results", [])]


def is_relevant(path, relevant_substrings):
    return any(sub in path for sub in relevant_substrings)


def score_query(ranked_paths, relevant_substrings, k):
    hits = [1 if is_relevant(p, relevant_substrings) else 0 for p in ranked_paths[:k]]
    dcg = sum(h / math.log2(rank + 2) for rank, h in enumerate(hits))
    ideal_n = min(len(relevant_substrings), k)
    idcg = sum(1 / math.log2(rank + 2) for rank in range(ideal_n)) if ideal_n else 0.0
    ndcg = dcg / idcg if idcg else 0.0
    recall = (sum(hits) / len(relevant_substrings)) if relevant_substrings else 0.0
    rr = 0.0
    for rank, h in enumerate(hits):
        if h:
            rr = 1 / (rank + 1)
            break
    first_rank = next((rank + 1 for rank, h in enumerate(hits) if h), None)
    return ndcg, recall, rr, first_rank


def evaluate(queries, k, search_type, yams_bin, path_scope=None, use_cwd=False, repeats=1):
    repeats = max(1, repeats)
    rows = []
    for q in queries:
        runs = []
        rankings = []
        for _ in range(repeats):
            ranked = run_search(q["query"], k, search_type, yams_bin, path_scope, use_cwd)
            ndcg, recall, rr, first_rank = score_query(ranked, q["relevant"], k)
            runs.append({"ndcg": ndcg, "recall": recall, "rr": rr, "first_rank": first_rank,
                         "num_results": len(ranked)})
            rankings.append(tuple(ranked[:k]))
        m = len(runs)
        distinct = len(set(rankings))
        rows.append({
            "query": q["query"],
            "ndcg": sum(r["ndcg"] for r in runs) / m,
            "recall": sum(r["recall"] for r in runs) / m,
            "rr": sum(r["rr"] for r in runs) / m,
            "found_any": any(r["first_rank"] is not None for r in runs),
            "first_ranks": [r["first_rank"] for r in runs],
            "distinct_rankings": distinct,
            "stable": distinct == 1,
            "repeats": m,
        })
    n = len(rows) or 1
    agg = {
        "ndcg_at_k": sum(r["ndcg"] for r in rows) / n,
        "recall_at_k": sum(r["recall"] for r in rows) / n,
        "mrr": sum(r["rr"] for r in rows) / n,
        "num_queries": len(rows),
        "found": sum(1 for r in rows if r["found_any"]),
        "stable_queries": sum(1 for r in rows if r["stable"]),
        "repeats": repeats,
    }
    return agg, rows


def load_results(path):
    return json.loads(Path(path).read_text())


def cmd_diff(a_path, b_path):
    a = load_results(a_path)
    b = load_results(b_path)
    print(f"\n  A = {a['label']:<14} B = {b['label']}\n")
    print(f"  {'metric':<14}{'A':>10}{'B':>10}{'B-A':>10}")
    for m in ("ndcg_at_k", "recall_at_k", "mrr"):
        av, bv = a["agg"][m], b["agg"][m]
        print(f"  {m:<14}{av:>10.4f}{bv:>10.4f}{bv-av:>+10.4f}")
    print(f"  {'found':<14}{a['agg']['found']:>10}{b['agg']['found']:>10}")
    amap = {r["query"]: r for r in a["rows"]}
    moved = []
    for r in b["rows"]:
        ar = amap.get(r["query"])
        if ar and abs(r["ndcg"] - ar["ndcg"]) > 1e-9:
            moved.append((r["query"], ar["ndcg"], r["ndcg"]))
    if moved:
        print("\n  per-query nDCG changes (A -> B):")
        for q, av, bv in sorted(moved, key=lambda x: x[2] - x[1]):
            print(f"    {bv-av:+.3f}  {av:.3f}->{bv:.3f}  {q}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default="tests/benchmarks/relevance/yams_corpus_queries.json")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--repeats", type=int, default=3,
                    help="run each query N times; report mean metrics + top-k stability")
    ap.add_argument("--type", default="hybrid", choices=["hybrid", "keyword", "semantic"])
    ap.add_argument("--label", default="run")
    ap.add_argument("--path", default=None, help="path-prefix scope passed to yams search --path")
    ap.add_argument("--cwd", action="store_true", help="scope retrieval to CWD (yams search --cwd)")
    ap.add_argument("--yams-bin", default="yams")
    ap.add_argument("--out", default=None)
    ap.add_argument("--diff", nargs=2, metavar=("A", "B"), default=None)
    args = ap.parse_args()

    if args.diff:
        cmd_diff(*args.diff)
        return 0

    qspec = json.loads(Path(args.queries).read_text())
    queries = qspec["queries"]
    agg, rows = evaluate(queries, args.k, args.type, args.yams_bin, args.path, args.cwd,
                         args.repeats)

    scope = "cwd" if args.cwd else (args.path or "(global)")
    print(f"\n  live-daemon relevance: label={args.label} type={args.type} k={args.k} "
          f"scope={scope} queries={agg['num_queries']} repeats={agg['repeats']}")
    print(f"  nDCG@{args.k}={agg['ndcg_at_k']:.4f}  Recall@{args.k}={agg['recall_at_k']:.4f}  "
          f"MRR={agg['mrr']:.4f}  found={agg['found']}/{agg['num_queries']}")
    if agg["repeats"] > 1:
        print(f"  stability: {agg['stable_queries']}/{agg['num_queries']} queries returned an "
              f"identical top-{args.k} across all {agg['repeats']} repeats")
        unstable = [(r["query"], r["distinct_rankings"]) for r in rows if not r["stable"]]
        if unstable:
            print("  unstable queries (distinct top-k orderings across repeats):")
            for q, d in sorted(unstable, key=lambda x: -x[1]):
                print(f"    {d}x  {q}")
    misses = [r["query"] for r in rows if not r["found_any"]]
    if misses:
        print("  misses (relevant doc not in top-k in any repeat):")
        for q in misses:
            print(f"    - {q}")

    if args.out:
        Path(args.out).write_text(json.dumps(
            {"label": args.label, "type": args.type, "k": args.k, "agg": agg, "rows": rows},
            indent=2))
        print(f"  wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
