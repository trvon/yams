#!/usr/bin/env python3
"""Task #57 analysis: compare per-query topology seed-set fingerprints across bench cells.

Usage:
    scripts/analyze_seed_fingerprints.py /tmp/bench_sgc_xxl_fp.jsonl
"""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def main(path: str) -> int:
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]
    if not rows:
        print(f"no rows in {path}", file=sys.stderr)
        return 1

    cells = defaultdict(list)
    for r in rows:
        key = (r.get("topology_engine_requested", "?"), r.get("feature_smoothing_hops", 0))
        cells[key].append(r)

    def mean(xs):
        return statistics.mean(xs) if xs else 0.0

    print("Per-cell summary (3-rep means; ranges in [])")
    print(f"{'engine':<12}{'hops':>5}  {'ndcg_mean':>10}  {'ndcg_range':>11}  "
          f"{'clusters':>9}  {'manifest_hash(rep1)':<22}  {'manifest_unique_reps':>4}")
    cell_means = {}
    cell_manifests = {}
    for key, recs in sorted(cells.items()):
        ndcgs = [r["ndcg_at_k"] for r in recs]
        mans = [r.get("topology_seed_set_manifest_hash", "") for r in recs]
        cell_means[key] = mean(ndcgs)
        cell_manifests[key] = mans
        print(f"{key[0]:<12}{key[1]:>5}  {mean(ndcgs):>10.5f}  "
              f"{(max(ndcgs) - min(ndcgs)):>11.5f}  "
              f"{int(mean([r.get('cluster_count', 0) for r in recs])):>9}  "
              f"{mans[0]:<22}  {len(set(mans)):>4}")

    print()
    print("Manifest-hash equality between cells (rep 1):")
    keys = sorted(cells.keys())
    print(f"{'cell':<22}" + "".join(f"{repr(k):<24}" for k in keys))
    for i, ki in enumerate(keys):
        row = [f"{repr(ki):<22}"]
        for kj in keys:
            a = cell_manifests[ki][0] if cell_manifests[ki] else ""
            b = cell_manifests[kj][0] if cell_manifests[kj] else ""
            row.append(f"{'EQ' if a == b and a else 'NE':<24}")
        print("".join(row))

    baseline_key = ("connected", 0)
    if baseline_key not in cells:
        print("\n(no connected/hops=0 baseline to compare per-query fingerprints against)")
        return 0
    baseline = cells[baseline_key][0].get("topology_seed_set_fingerprints")
    if not baseline:
        print("\n(long-form topology_seed_set_fingerprints not present; rerun with "
              "YAMS_BENCH_EMIT_SEED_FINGERPRINTS=1)")
        return 0

    print()
    print(f"Per-query fingerprint match rate vs CC baseline ({len(baseline)} queries, rep 1):")
    for key in keys:
        other = cells[key][0].get("topology_seed_set_fingerprints") or []
        if not other or len(other) != len(baseline):
            print(f"  {repr(key):<22}  (missing or length mismatch)")
            continue
        matches = sum(1 for a, b in zip(baseline, other) if a == b)
        pct = matches / len(baseline) * 100.0
        print(f"  {repr(key):<22}  {matches:>4}/{len(baseline)}  ({pct:5.1f}%)")

    # Decision rule
    print()
    print("Decision:")
    match_rates = {}
    for key in keys:
        other = cells[key][0].get("topology_seed_set_fingerprints") or []
        if len(other) == len(baseline) and len(baseline) > 0:
            match_rates[key] = sum(1 for a, b in zip(baseline, other) if a == b) / len(baseline)
    if not match_rates:
        print("  insufficient fingerprint data to apply decision rule")
        return 0
    min_rate = min(match_rates.values())
    cc_ndcg = cell_means.get(baseline_key, 0.0)
    tied_at_cc = sum(1 for k, v in cell_means.items() if abs(v - cc_ndcg) < 1e-5)
    if min_rate >= 0.95:
        print(f"  ≥95% match across all cells (min={min_rate*100:.1f}%) → cap saturation confirmed.")
        print("  Next: integration-surface probe (RecallExpand × max_docs ∈ {32,64,128}).")
    elif tied_at_cc >= 3:
        print(f"  seeds differ (min_match={min_rate*100:.1f}%) but ≥3 cells tied at CC nDCG "
              f"({cc_ndcg:.5f}) → backbone dominance confirmed.")
        print("  Next: retire cluster-aware retrieval on scifact/Simeon; proceed to Task #50.")
    else:
        print(f"  soft dominance (min_match={min_rate*100:.1f}%, tied_at_CC={tied_at_cc}) "
              f"→ lean toward retirement; optional sharp-signal subset analysis.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/bench_sgc_xxl_fp.jsonl"))
