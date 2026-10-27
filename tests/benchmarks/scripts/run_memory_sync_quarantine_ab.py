#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 YAMS Contributors
"""Compare recovered, within-budget quarantine calls; overflow is behavior-only.

Run identical optimized builds on an otherwise idle host. Each process has a fresh
filesystem corpus; recovery is outside timed calls. Alternate arm order between
repeats. Process high-water RSS includes setup and is not quarantine-only memory.
No daemon, authentication, startup, thermal, or cross-host claim follows from this
unsigned tombstone microbenchmark.
"""

import argparse
import hashlib
import json
import math
import statistics
import subprocess
from pathlib import Path


def quantile(values: list[float], fraction: float) -> float:
    """Use the median for p50 and the nearest-rank convention for other percentiles."""
    if fraction == 0.5:
        return statistics.median(values)
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1))]


def run(args: argparse.Namespace) -> None:
    if not 3 <= args.repeats <= 20 or not 1 <= args.iterations <= 1024:
        raise ValueError("require repeats 3..20 and iterations 1..1024")
    args.out.mkdir(parents=True, exist_ok=False)
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    identities = {arm: hashlib.sha256(path.read_bytes()).hexdigest()
                  for arm, path in binaries.items()}
    if identities["baseline"] == identities["candidate"]:
        raise ValueError("baseline and candidate binaries are identical")
    rows = []
    for records, limit in [(2, 8192), (64, 8192), (512, 8192), (2, 1)]:
        overflow = records > limit
        for repeat in range(args.repeats):
            arms = ["baseline", "candidate"] if repeat % 2 == 0 else ["candidate", "baseline"]
            for arm in arms:
                name = f"n{records}-cap{limit}-r{repeat}-{arm}"
                expected = "reject" if overflow and arm == "candidate" else "accept"
                command = [str(binaries[arm]), str(args.out / name / "corpus"),
                           str(records), str(limit), str(args.iterations), expected]
                stdout = args.out / f"{name}.stdout"
                exit_path = args.out / f"{name}.exit"
                with stdout.open("w") as output, (args.out / f"{name}.stderr").open("w") as errors:
                    try:
                        result = subprocess.run(command, stdout=output, stderr=errors,
                                                check=False, timeout=120)
                    except subprocess.TimeoutExpired as error:
                        exit_path.write_text("timeout after 120s; child killed by subprocess\n")
                        raise RuntimeError(f"benchmark timeout: {name}; logs preserved") from error
                exit_path.write_text(str(result.returncode) + "\n")
                if result.returncode != 0:
                    raise RuntimeError(f"benchmark failed: {name}; see preserved stdout/stderr/exit")
                row = json.loads(stdout.read_text())
                if (row["records"], row["identity_limit"], row["iterations"], row["expected_outcome"]) != (
                        records, limit, args.iterations, expected):
                    raise ValueError(f"benchmark metadata mismatch: {name}")
                row.update(arm=arm, repeat=repeat, binary_sha256=identities[arm],
                           comparable=not overflow, command=command)
                rows.append(row)
                with (args.out / "results.jsonl").open("a") as output:
                    output.write(json.dumps(row) + "\n")
                print(name, "p50_us", round(quantile(row["wall_us"], 0.5), 3), flush=True)
    if any(hashlib.sha256(path.read_bytes()).hexdigest() != identities[arm]
           for arm, path in binaries.items()):
        raise RuntimeError("benchmark binary changed during the run; do not compare mixed builds")
    summary = []
    for records, limit in [(2, 8192), (64, 8192), (512, 8192), (2, 1)]:
        for arm in binaries:
            selected = [row for row in rows if row["records"] == records and
                        row["identity_limit"] == limit and row["arm"] == arm]
            p50s = [quantile(row["wall_us"], 0.5) for row in selected]
            summary.append({"records": records, "identity_limit": limit, "arm": arm,
                            "comparable": records <= limit, "process_repeats": len(selected),
                            "wall_p50_us_mean": statistics.mean(p50s),
                            "wall_p50_us_stdev": statistics.stdev(p50s),
                            "wall_p95_us_mean": statistics.mean(
                                quantile(row["wall_us"], 0.95) for row in selected),
                            "cpu_us_per_call_mean": statistics.mean(
                                (row["user_us"] + row["system_us"]) / row["iterations"]
                                for row in selected),
                            "process_peak_rss_bytes_mean": statistics.mean(
                                row["process_peak_rss_bytes"] for row in selected)})
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=32)
    args = parser.parse_args()
    try:
        run(args)
    except (OSError, ValueError, RuntimeError) as error:
        parser.exit(1, f"quarantine benchmark: {error}\n")


if __name__ == "__main__":
    main()
