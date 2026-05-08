#!/usr/bin/env python3

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path


FILE_RE = re.compile(r"^File '([^']+)'$")
LINES_RE = re.compile(r"^Lines executed:([0-9]+(?:\.[0-9]+)?|nan)% of ([0-9]+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh daemon gcov summaries from gcov run JSON and/or live gcov captures"
    )
    parser.add_argument(
        "--build-dir",
        default="build/coverage",
        help="Coverage build directory (default: build/coverage)",
    )
    parser.add_argument(
        "--workspace-root",
        default=".",
        help="Workspace root used for path normalization (default: .)",
    )
    parser.add_argument(
        "--run-json",
        action="append",
        default=[],
        help="Existing gcov run JSON input (repeatable)",
    )
    parser.add_argument(
        "--capture-artifact",
        action="append",
        default=[],
        help="Artifact path (.o or .gcno) to run gcov against (repeatable)",
    )
    parser.add_argument(
        "--keep-captured-history",
        action="store_true",
        help="Keep existing run-json entries for recaptured artifacts instead of replacing them",
    )
    parser.add_argument(
        "--capture-out",
        help="Optional path to write captured gcov entries as JSON",
    )
    parser.add_argument(
        "--write-merged-run-json",
        help="Optional path to write merged gcov run JSON",
    )
    parser.add_argument(
        "--summary-json",
        help="Raw summary output path (default: <build-dir>/daemon_gcov_summary.json)",
    )
    parser.add_argument(
        "--normalized-json",
        help="Normalized summary output path (default: <build-dir>/daemon_gcov_summary_normalized.json)",
    )
    parser.add_argument(
        "--dedup-json",
        help="Deduped summary output path (default: <build-dir>/daemon_gcov_summary_dedup.json)",
    )
    return parser.parse_args()


def load_run_entries(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Expected list in {path}")
    return data


def normalize_artifact_path(path_text: str, workspace_root: Path) -> str:
    artifact_path = Path(path_text)
    if not artifact_path.is_absolute():
        artifact_path = workspace_root / artifact_path
    return str(artifact_path.resolve(strict=False))


def capture_gcov_entries(
    artifacts: list[str], workspace_root: Path, build_dir: Path
) -> list[dict]:
    entries = []
    for artifact in artifacts:
        artifact_path = Path(normalize_artifact_path(artifact, workspace_root))
        proc = subprocess.run(
            ["gcov", "-b", "-c", str(artifact_path)],
            cwd=build_dir,
            capture_output=True,
            text=True,
        )
        entries.append(
            {
                "file": str(artifact_path),
                "code": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }
        )
    return entries


def is_daemon_source(path_text: str) -> bool:
    normalized = path_text.replace("\\", "/")
    if not normalized.endswith((".cpp", ".cc", ".cxx")):
        return False
    return (
        "/daemon/" in normalized
        or normalized.startswith("daemon/")
        or "src/daemon/" in normalized
    )


def normalize_source_path(source: str, workspace_root: Path, build_dir: Path) -> str:
    normalized = source.replace("\\", "/")
    if os.path.isabs(normalized):
        return str(Path(normalized).resolve())

    while normalized.startswith("./"):
        normalized = normalized[2:]
    while normalized.startswith("../"):
        normalized = normalized[3:]

    if normalized.startswith(("src/", "include/", "tests/")):
        return str((workspace_root / normalized).resolve(strict=False))

    src_idx = normalized.find("src/")
    if src_idx != -1:
        return str((workspace_root / normalized[src_idx:]).resolve(strict=False))

    include_idx = normalized.find("include/")
    if include_idx != -1:
        return str((workspace_root / normalized[include_idx:]).resolve(strict=False))

    return str((build_dir / normalized).resolve(strict=False))


def parse_run_entries(
    entries: list[dict], workspace_root: Path, build_dir: Path
) -> tuple[list[dict], list[dict]]:
    raw_records = []
    normalized_records = []

    for entry in entries:
        text_parts = []
        stdout = entry.get("stdout")
        stderr = entry.get("stderr")
        if stdout:
            text_parts.append(stdout)
        if stderr:
            text_parts.append(stderr)
        text = "\n".join(text_parts)
        current_source = None

        for line in text.splitlines():
            file_match = FILE_RE.match(line.strip())
            if file_match:
                current_source = file_match.group(1)
                continue

            lines_match = LINES_RE.match(line.strip())
            if not lines_match or current_source is None:
                continue
            if not is_daemon_source(current_source):
                current_source = None
                continue

            pct_text, lines_text = lines_match.groups()
            pct = 0.0 if pct_text == "nan" else round(float(pct_text), 2)
            lines = int(lines_text)

            raw_record = {"file": current_source, "pct": pct, "lines": lines}
            raw_records.append(raw_record)
            normalized_records.append(
                {
                    "file": normalize_source_path(
                        current_source, workspace_root, build_dir
                    ),
                    "pct": pct,
                    "lines": lines,
                }
            )
            current_source = None

    return raw_records, normalized_records


def dedup_records(records: list[dict]) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        groups[record["file"]].append(record)

    deduped = []
    for file_path, variants in groups.items():
        best = max(variants, key=lambda item: (item["pct"], item["lines"]))
        deduped.append(
            {
                "file": file_path,
                "pct": best["pct"],
                "lines": best["lines"],
                "variants": len(variants),
            }
        )
    deduped.sort(key=lambda item: item["file"])
    return deduped


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=False)
        handle.write("\n")


def main() -> int:
    args = parse_args()
    workspace_root = Path(args.workspace_root).resolve()
    build_dir = (workspace_root / args.build_dir).resolve()

    run_json_paths = args.run_json or [str(build_dir / "daemon_gcov_run.json")]
    run_entries = []
    for run_json in run_json_paths:
        run_entries.extend(load_run_entries((workspace_root / run_json).resolve()))

    captured_artifacts = {
        normalize_artifact_path(path_text, workspace_root)
        for path_text in args.capture_artifact
    }
    replaced_run_entries = 0
    if captured_artifacts and not args.keep_captured_history:
        filtered_run_entries = []
        for entry in run_entries:
            entry_file = entry.get("file")
            if (
                isinstance(entry_file, str)
                and normalize_artifact_path(entry_file, workspace_root)
                in captured_artifacts
            ):
                replaced_run_entries += 1
                continue
            filtered_run_entries.append(entry)
        run_entries = filtered_run_entries

    captured_entries = capture_gcov_entries(
        args.capture_artifact, workspace_root, build_dir
    )
    merged_entries = [*run_entries, *captured_entries]

    raw_records, normalized_records = parse_run_entries(
        merged_entries, workspace_root, build_dir
    )
    deduped_records = dedup_records(normalized_records)

    summary_json = (
        Path(args.summary_json)
        if args.summary_json
        else build_dir / "daemon_gcov_summary.json"
    )
    normalized_json = (
        Path(args.normalized_json)
        if args.normalized_json
        else build_dir / "daemon_gcov_summary_normalized.json"
    )
    dedup_json = (
        Path(args.dedup_json)
        if args.dedup_json
        else build_dir / "daemon_gcov_summary_dedup.json"
    )

    write_json(summary_json.resolve(), raw_records)
    write_json(normalized_json.resolve(), normalized_records)
    write_json(dedup_json.resolve(), deduped_records)

    if args.capture_out:
        write_json(Path(args.capture_out).resolve(), captured_entries)
    if args.write_merged_run_json:
        write_json(Path(args.write_merged_run_json).resolve(), merged_entries)

    print(
        json.dumps(
            {
                "run_entries": len(run_entries),
                "replaced_run_entries": replaced_run_entries,
                "captured_entries": len(captured_entries),
                "raw_records": len(raw_records),
                "normalized_records": len(normalized_records),
                "deduped_records": len(deduped_records),
                "summary_json": str(summary_json.resolve()),
                "normalized_json": str(normalized_json.resolve()),
                "dedup_json": str(dedup_json.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
