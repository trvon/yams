#!/usr/bin/env python3
"""
TurboQuant Benchmark Comparison Script

Compares baseline 8-bit linear quantization vs TurboQuant storage retrieval benchmarks.
Detects regressions in encode/decode latency, storage efficiency, and quality metrics.

Usage:
    # First, capture baseline (before TurboQuant integration):
    ./builddir/tests/benchmarks/turboquant_bench --vectors=1000 > bench_old.json

    # After TurboQuant integration:
    ./builddir/tests/benchmarks/turboquant_bench --vectors=1000 > bench_new.json

    # Run comparison:
    ./scripts/bench_turboquant_compare.py --old bench_old.json --new bench_new.json

Output:
    - Console summary of regressions
    - benchmark_comparison.json with detailed results
    - Exit code 0 if no regressions, 1 if regressions found
"""

import json
import sys
import argparse
from dataclasses import dataclass
from typing import Optional


@dataclass
class RegressionThresholds:
    storage_increase_max: float = 1.05  # Allow 5% tolerance
    encode_latency_increase_max: float = 1.50  # Allow 50% increase
    decode_latency_increase_max: float = 1.50  # Allow 50% increase
    mse_max: float = 0.01  # MSE must be < 1%
    recall_at_1_min: float = 0.85  # Recall must be > 85%


@dataclass
class BenchmarkResult:
    dimension: int
    bitwidth: int
    storage_bytes: float
    encode_latency_us_p50: float
    decode_latency_us_p50: float
    baseline_encode_us_p50: float
    baseline_decode_us_p50: float
    mse: float
    recall_at_1: float
    speedup_encode: float
    speedup_decode: float


class BenchmarkComparator:
    def __init__(self, old_data: dict, new_data: dict, thresholds: RegressionThresholds):
        self.old_data = old_data
        self.new_data = new_data
        self.thresholds = thresholds
        self.regressions = []
        self.warnings = []

    def find_result(self, data: dict, dimension: int, bitwidth: int) -> Optional[BenchmarkResult]:
        for r in data.get("results", []):
            if r["dimension"] == dimension and r["bitwidth"] == bitwidth:
                return BenchmarkResult(
                    dimension=r["dimension"],
                    bitwidth=r["bitwidth"],
                    storage_bytes=r.get("storage_bytes", 0),
                    encode_latency_us_p50=r.get("encode_latency_us_p50", 0),
                    decode_latency_us_p50=r.get("decode_latency_us_p50", 0),
                    baseline_encode_us_p50=r.get("baseline_encode_us_p50", r.get("encode_latency_us_p50", 0)),
                    baseline_decode_us_p50=r.get("baseline_decode_us_p50", r.get("decode_latency_us_p50", 0)),
                    mse=r.get("mse", 0),
                    recall_at_1=r.get("recall_at_1", 1.0),
                    speedup_encode=r.get("speedup_encode", 1.0),
                    speedup_decode=r.get("speedup_decode", 1.0),
                )
        return None

    def compare(self) -> tuple[bool, list, list]:
        old_results = self.old_data.get("results", [])
        new_results = self.new_data.get("results", [])

        # Index new results by dimension/bitwidth
        new_indexed = {(r["dimension"], r["bitwidth"]): r for r in new_results}

        for old_r in old_results:
            dim = old_r["dimension"]
            bits = old_r["bitwidth"]

            if (dim, bits) not in new_indexed:
                self.warnings.append(f"Missing new result for dim={dim}, bits={bits}")
                continue

            new_r = new_indexed[(dim, bits)]

            # Compare storage (should decrease, not increase)
            old_storage = old_r.get("storage_bytes", 0)
            new_storage = new_r.get("storage_bytes", 0)
            if old_storage > 0 and new_storage > old_storage * self.thresholds.storage_increase_max:
                self.regressions.append(
                    f"STORAGE: dim={dim}, bits={bits}: "
                    f"{old_storage:.0f} bytes -> {new_storage:.0f} bytes "
                    f"({new_storage/old_storage:.2f}x)"
                )

            # Compare encode latency (should not increase too much)
            old_encode = old_r.get("baseline_encode_us_p50", old_r.get("encode_latency_us_p50", 0))
            new_encode = new_r.get("encode_latency_us_p50", 0)
            if old_encode > 0 and new_encode > old_encode * self.thresholds.encode_latency_increase_max:
                self.regressions.append(
                    f"ENCODE_LATENCY: dim={dim}, bits={bits}: "
                    f"{old_encode:.2f}μs -> {new_encode:.2f}μs "
                    f"({new_encode/old_encode:.2f}x increase)"
                )

            # Compare decode latency (should not increase too much)
            old_decode = old_r.get("baseline_decode_us_p50", old_r.get("decode_latency_us_p50", 0))
            new_decode = new_r.get("decode_latency_us_p50", 0)
            if old_decode > 0 and new_decode > old_decode * self.thresholds.decode_latency_increase_max:
                self.regressions.append(
                    f"DECODE_LATENCY: dim={dim}, bits={bits}: "
                    f"{old_decode:.2f}μs -> {new_decode:.2f}μs "
                    f"({new_decode/old_decode:.2f}x increase)"
                )

            # Check MSE quality
            new_mse = new_r.get("mse", 0)
            if new_mse > self.thresholds.mse_max:
                self.regressions.append(
                    f"MSE_QUALITY: dim={dim}, bits={bits}: "
                    f"MSE={new_mse:.6f} exceeds threshold {self.thresholds.mse_max}"
                )

            # Check recall quality
            new_recall = new_r.get("recall_at_1", 0)
            if new_recall < self.thresholds.recall_at_1_min:
                self.regressions.append(
                    f"RECALL_QUALITY: dim={dim}, bits={bits}: "
                    f"recall@1={new_recall:.4f} below threshold {self.thresholds.recall_at_1_min}"
                )

        has_regressions = len(self.regressions) > 0
        return has_regressions, self.regressions, self.warnings


def print_summary(old_data: dict, new_data: dict, regressions: list, warnings: list):
    print("=" * 70)
    print("TURBOQUANT BENCHMARK COMPARISON")
    print("=" * 70)
    print()

    # Print configuration
    old_config = old_data.get("config", {})
    new_config = new_data.get("config", {})
    print(f"Baseline config: {old_config}")
    print(f"TurboQuant config: {new_config}")
    print()

    # Print table header
    print(f"{'Dim':>6} {'Bits':>5} {'Storage Ratio':>14} {'Encode Ratio':>13} {'Decode Ratio':>13} {'MSE':>10} {'Recall@1':>10}")
    print("-" * 70)

    old_results = old_data.get("results", [])
    new_results = {r["dimension"]: r for r in new_data.get("results", [])}

    for old_r in old_results:
        dim = old_r["dimension"]
        bits = old_r["bitwidth"]
        if dim not in new_results:
            continue
        new_r = new_results[dim]

        old_storage = old_r.get("storage_bytes", 0)
        new_storage = new_r.get("storage_bytes", 0)
        storage_ratio = new_storage / old_storage if old_storage > 0 else 0

        old_encode = old_r.get("baseline_encode_us_p50", 0)
        new_encode = new_r.get("encode_latency_us_p50", 0)
        encode_ratio = new_encode / old_encode if old_encode > 0 else 0

        old_decode = old_r.get("baseline_decode_us_p50", 0)
        new_decode = new_r.get("decode_latency_us_p50", 0)
        decode_ratio = new_decode / old_decode if old_decode > 0 else 0

        mse = new_r.get("mse", 0)
        recall = new_r.get("recall_at_1", 0)

        print(f"{dim:>6} {bits:>5} {storage_ratio:>13.2f}x {encode_ratio:>12.2f}x {decode_ratio:>12.2f}x {mse:>10.6f} {recall:>10.4f}")

    print()

    # Print regressions
    if regressions:
        print("REGRESSIONS DETECTED:")
        for r in regressions:
            print(f"  ✗ {r}")
        print()
    else:
        print("✓ No regressions detected")
        print()

    # Print warnings
    if warnings:
        print("WARNINGS:")
        for w in warnings:
            print(f"  ! {w}")
        print()

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Compare TurboQuant vs baseline benchmark results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare benchmarks
  %(prog)s --old bench_old.json --new bench_new.json

  # Custom thresholds
  %(prog)s --old bench_old.json --new bench_new.json --encode-threshold 2.0

  # JSON output
  %(prog)s --old bench_old.json --new bench_new.json --output comparison.json
        """
    )
    parser.add_argument("--old", required=True, help="Baseline benchmark JSON file")
    parser.add_argument("--new", required=True, help="TurboQuant benchmark JSON file")
    parser.add_argument("--output", default="benchmark_comparison.json", help="Output JSON file")
    parser.add_argument("--storage-threshold", type=float, default=1.05,
                        help="Max storage increase ratio (default: 1.05)")
    parser.add_argument("--encode-threshold", type=float, default=1.50,
                        help="Max encode latency increase ratio (default: 1.50)")
    parser.add_argument("--decode-threshold", type=float, default=1.50,
                        help="Max decode latency increase ratio (default: 1.50)")
    parser.add_argument("--mse-max", type=float, default=0.01,
                        help="Max MSE threshold (default: 0.01)")
    parser.add_argument("--recall-min", type=float, default=0.85,
                        help="Min recall@1 threshold (default: 0.85)")

    args = parser.parse_args()

    # Load benchmark files
    try:
        with open(args.old) as f:
            old_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading --old file: {e}", file=sys.stderr)
        return 1

    try:
        with open(args.new) as f:
            new_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading --new file: {e}", file=sys.stderr)
        return 1

    # Create thresholds
    thresholds = RegressionThresholds(
        storage_increase_max=args.storage_threshold,
        encode_latency_increase_max=args.encode_threshold,
        decode_latency_increase_max=args.decode_threshold,
        mse_max=args.mse_max,
        recall_at_1_min=args.recall_min,
    )

    # Run comparison
    comparator = BenchmarkComparator(old_data, new_data, thresholds)
    has_regressions, regressions, warnings = comparator.compare()

    # Print summary
    print_summary(old_data, new_data, regressions, warnings)

    # Write output JSON
    result = {
        "has_regressions": has_regressions,
        "regression_count": len(regressions),
        "regressions": regressions,
        "warnings": warnings,
        "thresholds": {
            "storage_increase_max": thresholds.storage_increase_max,
            "encode_latency_increase_max": thresholds.encode_latency_increase_max,
            "decode_latency_increase_max": thresholds.decode_latency_increase_max,
            "mse_max": thresholds.mse_max,
            "recall_at_1_min": thresholds.recall_at_1_min,
        },
        "files": {
            "baseline": args.old,
            "turboquant": args.new,
            "output": args.output,
        }
    }

    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Results written to: {args.output}")

    return 1 if has_regressions else 0


if __name__ == "__main__":
    sys.exit(main())
