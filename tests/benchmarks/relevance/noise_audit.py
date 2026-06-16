#!/usr/bin/env python3
"""Quantify how much retrieval miss-rate is noise-doc crowding vs genuinely weak retrieval.

For each judged query, runs `yams search` over a wide window (default top-40), classifies every
returned path (relevant / noise / code / doc / other), and reports: (a) what fraction of top-10
slots go to noise (PDFs, build artifacts, fixtures, coverage), and (b) for each query whether the
relevant file is a top-10 hit, buried below noise at rank 11-N (hygiene/scoping fixable), or absent
from the window (genuinely weak retrieval). Runs each query a few times and takes the best rank, so
the dynamic-index nondeterminism does not understate recall.

Usage:
  python3 tests/benchmarks/relevance/noise_audit.py --cwd
  python3 tests/benchmarks/relevance/noise_audit.py            # global (unscoped)
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

NOISE_MARKERS = (
    ".pdf", ".tsv", "/paperseed/", "/fixtures/", "/coverage/", "tarpaulin",
    "/build-", "/builddir/", "/build/", "/cmake-build", "/node_modules/", ".lock",
)
CODE_EXT = (".cpp", ".hpp", ".h", ".hh", ".cc", ".cxx", ".c", ".ipp", ".py", ".rs",
            ".js", ".ts", ".go", ".java", ".rb", ".sh", ".cmake")
DOC_EXT = (".md", ".rst", ".txt", ".adoc")


def classify(path, relevant):
    if any(sub in path for sub in relevant):
        return "relevant"
    low = path.lower()
    if any(m in low for m in NOISE_MARKERS):
        return "noise"
    if low.endswith(CODE_EXT):
        return "code"
    if low.endswith(DOC_EXT):
        return "doc"
    return "other"


def run_search(query, limit, search_type, yams_bin, use_cwd):
    cmd = [yams_bin, "search", query, "--limit", str(limit), "--type", search_type, "--json"]
    if use_cwd:
        cmd += ["--cwd"]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        return []
    try:
        return [r.get("path", "") for r in json.loads(proc.stdout).get("results", [])]
    except json.JSONDecodeError:
        return []


def best_relevant_rank(rankings, relevant):
    best = None
    for ranked in rankings:
        for i, p in enumerate(ranked):
            if any(sub in p for sub in relevant):
                if best is None or i < best:
                    best = i
                break
    return best  # 0-based, or None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default="tests/benchmarks/relevance/yams_corpus_queries.json")
    ap.add_argument("--window", type=int, default=40, help="how deep to look for the relevant doc")
    ap.add_argument("--topk", type=int, default=10, help="slot-occupancy window for noise share")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--type", default="hybrid", choices=["hybrid", "keyword", "semantic"])
    ap.add_argument("--cwd", action="store_true")
    ap.add_argument("--yams-bin", default="yams")
    args = ap.parse_args()

    queries = json.loads(Path(args.queries).read_text())["queries"]
    slot_counts = {"relevant": 0, "noise": 0, "code": 0, "doc": 0, "other": 0}
    total_slots = 0
    buckets = {"hit": [], "buried": [], "weak": []}

    for q in queries:
        rankings = [run_search(q["query"], args.window, args.type, args.yams_bin, args.cwd)
                    for _ in range(args.repeats)]
        # slot occupancy from the first repeat's top-k
        top = rankings[0][:args.topk]
        for p in top:
            slot_counts[classify(p, q["relevant"])] += 1
            total_slots += 1
        rank = best_relevant_rank(rankings, q["relevant"])
        if rank is None:
            buckets["weak"].append(q["query"])
        elif rank < args.topk:
            buckets["hit"].append((q["query"], rank + 1))
        else:
            noise_above = sum(1 for p in rankings[0][:rank] if classify(p, q["relevant"]) == "noise")
            buckets["buried"].append((q["query"], rank + 1, noise_above))

    scope = "cwd" if args.cwd else "(global)"
    nq = len(queries)
    print(f"\n  noise audit: type={args.type} scope={scope} window={args.window} "
          f"topk={args.topk} repeats={args.repeats} queries={nq}")
    print(f"\n  top-{args.topk} slot occupancy ({total_slots} slots):")
    for cat in ("relevant", "code", "doc", "noise", "other"):
        c = slot_counts[cat]
        pct = 100.0 * c / total_slots if total_slots else 0.0
        print(f"    {cat:<9} {c:>4}  {pct:5.1f}%")

    print(f"\n  miss diagnosis (relevant doc location, best of {args.repeats} repeats):")
    print(f"    HIT  (in top-{args.topk}):           {len(buckets['hit'])}/{nq}")
    print(f"    BURIED (rank {args.topk+1}-{args.window}, noise-crowded): {len(buckets['buried'])}/{nq}")
    print(f"    WEAK (absent from top-{args.window}):   {len(buckets['weak'])}/{nq}")
    if buckets["buried"]:
        print("\n  BURIED (hygiene/scoping fixable) — query, rank, #noise-docs-above:")
        for q, r, n in sorted(buckets["buried"], key=lambda x: x[1]):
            print(f"    rank {r:>3}  ({n} noise above)  {q}")
    if buckets["weak"]:
        print("\n  WEAK (genuine retrieval gap) — relevant doc not in window:")
        for q in buckets["weak"]:
            print(f"    - {q}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
