#!/usr/bin/env python3

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path


SUMMARY_RE = re.compile(r"^SUMMARY: AddressSanitizer: (.+)$")
FRAME_RE = re.compile(r"^#0\s+0x[0-9a-fA-F]+\s+in\s+(.+)$")


def classify(summary: str) -> str:
    s = summary.lower()
    if "use-after-free" in s:
        return "UAF"
    if "double-free" in s:
        return "DoubleFree"
    if "heap-buffer-overflow" in s or "stack-buffer-overflow" in s:
        return "OOB"
    if "global-buffer-overflow" in s:
        return "OOB"
    if "container-overflow" in s:
        return "OOB"
    if "out-of-bounds" in s:
        return "OOB"
    if "null" in s and "dereference" in s:
        return "NULL"
    if "unknown-crash" in s:
        return "Unknown"
    return "Other"


def parse_one(text: str) -> tuple[str, str]:
    summary = "(no SUMMARY line found)"
    top = "(no #0 frame found)"

    for line in text.splitlines():
        m = SUMMARY_RE.match(line.strip())
        if m:
            summary = m.group(1)
            break

    for line in text.splitlines():
        m = FRAME_RE.match(line.strip())
        if m:
            top = m.group(1)
            break

    return summary, top


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Cluster ASAN traces by summary + top frame"
    )
    ap.add_argument("paths", nargs="+", help="ASAN log files (text)")
    args = ap.parse_args()

    groups: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for p in args.paths:
        path = Path(p)
        try:
            text = path.read_text(errors="replace")
        except OSError as e:
            print(f"error: {path}: {e}", file=sys.stderr)
            continue
        summary, top = parse_one(text)
        groups[(summary, top)].append(path)

    ranked = sorted(
        groups.items(),
        key=lambda kv: (-len(kv[1]), kv[0][0], kv[0][1]),
    )

    for (summary, top), files in ranked:
        sev = classify(summary)
        print(f"[{len(files):3d}] {sev:10s} | {summary}")
        print(f"      top: {top}")
        for f in files[:5]:
            print(f"      - {f}")
        if len(files) > 5:
            print(f"      ... +{len(files) - 5} more")
        print("")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
