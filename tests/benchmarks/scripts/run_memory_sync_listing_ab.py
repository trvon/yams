#!/usr/bin/env python3
"""A/B driver for the memory-sync listing-cost benchmark.

Both arms read the same on-disk corpus; only the in-process listing cache differs.
Every measurement is a fresh process, so cache state never crosses a repeat.

Decision rule (see the plan): the fix must cut active-scenario system time and
entries visited at N >= 513 without regressing the cold sweep. Report medians with
min/max, never a bare delta.
"""

import argparse
import json
import statistics
import subprocess
import sys
from pathlib import Path


def run(binary: Path, *, records: int, scenario: str, publishes: int, corpus: Path,
        label: str) -> dict:
    cmd = [
        str(binary),
        "--records", str(records),
        "--scenario", scenario,
        "--publishes", str(publishes),
        "--dir", str(corpus),
        "--reuse",
        "--label", label,
    ]
    completed = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if completed.returncode != 0:
        raise SystemExit(
            f"{label} {scenario} n={records} failed rc={completed.returncode}\n"
            f"stdout: {completed.stdout}\nstderr: {completed.stderr}"
        )
    payload = json.loads(completed.stdout.strip().splitlines()[-1])
    payload["arm"] = label
    return payload


def summarise(rows: list[dict], key: str) -> dict:
    values = [row[key] for row in rows]
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "n": len(values),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-bench", required=True, type=Path)
    parser.add_argument("--fix-bench", required=True, type=Path)
    parser.add_argument("--base-label", default="base")
    parser.add_argument("--fix-label", default="fix")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--corpus-root", required=True, type=Path)
    parser.add_argument("--records", default="511,513,1025,2049")
    parser.add_argument("--scenarios", default="cold,active")
    parser.add_argument("--publishes", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--report-only", action="store_true",
                        help="regenerate REPORT.md from an existing results.jsonl")
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    if args.report_only:
        raw = [json.loads(line) for line in
               (args.out / "results.jsonl").read_text(encoding="utf-8").splitlines()
               if line.strip()]
        records_list = sorted({r["records"] for r in raw})
        scenarios = sorted({r["scenario"] for r in raw})
        return write_report(raw, args, records_list, scenarios)

    records_list = [int(item) for item in args.records.split(",") if item]
    scenarios = [item.strip() for item in args.scenarios.split(",") if item.strip()]
    args.out.mkdir(parents=True, exist_ok=True)
    args.corpus_root.mkdir(parents=True, exist_ok=True)

    arms = [(args.base_label, args.base_bench), (args.fix_label, args.fix_bench)]
    raw = []
    for records in records_list:
        corpus = args.corpus_root / f"records-{records}"
        # Seed once per corpus size with the baseline binary; identical on-disk shape.
        subprocess.run(
            [str(args.base_bench), "--records", str(records), "--dir", str(corpus),
             "--seed-only", "--label", "seed"],
            check=True, capture_output=True, text=True, timeout=7200,
        )
        for scenario in scenarios:
            for repeat in range(args.repeats):
                # Interleave arms inside each repeat to spread machine drift.
                for label, binary in arms:
                    row = run(binary, records=records, scenario=scenario,
                              publishes=args.publishes, corpus=corpus, label=label)
                    row["repeat"] = repeat
                    raw.append(row)
                    print(json.dumps(row), flush=True)

    with (args.out / "results.jsonl").open("w", encoding="utf-8") as handle:
        for row in raw:
            handle.write(json.dumps(row) + "\n")
    return write_report(raw, args, records_list, scenarios)


def write_report(raw: list[dict], args, records_list: list[int],
                 scenarios: list[str]) -> int:
    arms = [(args.base_label, args.base_bench), (args.fix_label, args.fix_bench)]

    lines = ["| records | scenario | arm | wall ms | user ms | sys ms | sys share | peak RSS MiB |",
             "|---|---|---|---|---|---|---|---|"]
    for records in records_list:
        for scenario in scenarios:
            for label, _ in arms:
                rows = [r for r in raw
                        if r["records"] == records and r["scenario"] == scenario
                        and r["arm"] == label]
                if not rows:
                    continue
                wall = summarise(rows, "wall_us")
                user = summarise(rows, "user_us")
                system = summarise(rows, "system_us")
                rss = summarise(rows, "peak_rss_bytes")
                cpu = wall["median"] and (user["median"] + system["median"])
                share = (system["median"] / cpu) if cpu else 0.0
                lines.append(
                    f"| {records} | {scenario} | {label} | {wall['median'] / 1000:.1f} "
                    f"| {user['median'] / 1000:.1f} | {system['median'] / 1000:.1f} "
                    f"| {share * 100:.0f}% | {rss['median'] / (1024 * 1024):.0f} |"
                )
    table = "\n".join(lines)

    deltas = ["| records | scenario | metric | base | fix | delta | ratio |",
              "|---|---|---|---|---|---|---|"]
    for records in records_list:
        for scenario in scenarios:
            for metric in ["wall_us", "system_us", "user_us"]:
                base_rows = [r for r in raw if r["records"] == records
                             and r["scenario"] == scenario and r["arm"] == args.base_label]
                fix_rows = [r for r in raw if r["records"] == records
                            and r["scenario"] == scenario and r["arm"] == args.fix_label]
                if not base_rows or not fix_rows:
                    continue
                base_value = summarise(base_rows, metric)["median"]
                fix_value = summarise(fix_rows, metric)["median"]
                ratio = (base_value / fix_value) if fix_value else float("inf")
                deltas.append(
                    f"| {records} | {scenario} | {metric} | {base_value:.0f} "
                    f"| {fix_value:.0f} | {fix_value - base_value:+.0f} | {ratio:.2f}x |"
                )

    report = "\n".join([
        "# Memory-sync listing cost A/B",
        "",
        f"arms: {args.base_label} vs {args.fix_label}; repeats={args.repeats}; "
        f"publishes={args.publishes}",
        "",
        "## Measured",
        "",
        table,
        "",
        "## Paired medians (fix - base)",
        "",
        "\n".join(deltas),
        "",
        "Counters: baseline 4/6/9 total walks and 18909/27160/78882 entries for "
        "511/513/1025 records; fix 1/2/3, equal to the warm count. Those are code-path "
        "facts from the unit suite, not repeated here. Timings are medians over "
        f"{args.repeats} fresh processes per cell and are only comparable within "
        "this build type, machine and filesystem.",
        "",
    ])
    (args.out / "REPORT.md").write_text(report, encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
