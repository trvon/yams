#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
from pathlib import Path
from yams_sdk.hound import load_hound_graphs


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert Hound graphs to GraphJSON v1")
    ap.add_argument("project", type=str, help="Path to Hound project directory")
    ap.add_argument("--out", type=str, default=None, help="Output directory for GraphJSON files")
    args = ap.parse_args()

    graphs = load_hound_graphs(args.project)
    if not graphs:
        print("No graphs found.")
        return 1
    outdir = Path(args.out) if args.out else Path(args.project) / "graphs_graphjson"
    outdir.mkdir(parents=True, exist_ok=True)
    for name, gj in graphs:
        (outdir / f"{name}.graphjson").write_text(json.dumps(gj, indent=2))
        print(f"Wrote {name}.graphjson")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

