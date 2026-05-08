#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/dev/benchmark_taskflow_ab.sh [options]

Benchmarks current HEAD against a baseline commit on the same machine using
temporary git worktrees, separate build directories, and a fixed dataset.

By default:
  - current ref:  HEAD
  - baseline ref: HEAD^
  - dataset:      <repo>/src
  - build dir:    build/release
  - api iters:    5
  - throughput repeats: 3

Important:
  - This script avoids your dirty main worktree by creating detached worktrees.
  - It overlays the current full-pipeline benchmark source into both worktrees so
    the throughput harness is identical across commits.

Options:
  --baseline-ref <ref>    Baseline git ref to compare against (default: HEAD^)
  --current-ref <ref>     Current git ref to benchmark (default: HEAD)
  --dataset <path>        Fixed dataset path to ingest (default: <repo>/src)
  --work-root <path>      Temporary worktree/results root
  --build-jobs <n>        Meson compile parallelism (default: 4)
  --api-iters <n>         Iterations for yams_api_benchmarks (default: 5)
  --repeat <n>            Repeats for ingestion_throughput_bench (default: 3)
  --keep-worktrees        Keep temporary worktrees/results after completion
  --no-overlay-bench      Do not copy current throughput benchmark source into both worktrees
  -h, --help              Show this help

Examples:
  scripts/dev/benchmark_taskflow_ab.sh
  scripts/dev/benchmark_taskflow_ab.sh --baseline-ref a5d5ddc9 --dataset "$PWD/src"
EOF
}

repo_root="$(git rev-parse --show-toplevel)"
baseline_ref="HEAD^"
current_ref="HEAD"
dataset_path="${repo_root}/src"
work_root="${TMPDIR:-/tmp}/yams-taskflow-ab"
build_jobs=4
api_iters=5
repeat_count=3
keep_worktrees=0
overlay_bench=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --baseline-ref)
      baseline_ref="$2"
      shift 2
      ;;
    --current-ref)
      current_ref="$2"
      shift 2
      ;;
    --dataset)
      dataset_path="$2"
      shift 2
      ;;
    --work-root)
      work_root="$2"
      shift 2
      ;;
    --build-jobs)
      build_jobs="$2"
      shift 2
      ;;
    --api-iters)
      api_iters="$2"
      shift 2
      ;;
    --repeat)
      repeat_count="$2"
      shift 2
      ;;
    --keep-worktrees)
      keep_worktrees=1
      shift
      ;;
    --no-overlay-bench)
      overlay_bench=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "$dataset_path" ]]; then
  printf 'Dataset path does not exist: %s\n' "$dataset_path" >&2
  exit 2
fi

baseline_sha="$(git rev-parse --verify "$baseline_ref")"
current_sha="$(git rev-parse --verify "$current_ref")"
baseline_short="$(git rev-parse --short "$baseline_sha")"
current_short="$(git rev-parse --short "$current_sha")"

work_root="${work_root%/}-${baseline_short}-vs-${current_short}"
baseline_dir="${work_root}/baseline"
current_dir="${work_root}/current"
results_dir="${work_root}/results"

mkdir -p "$results_dir"

cleanup() {
  if [[ "$keep_worktrees" -eq 1 ]]; then
    return
  fi
  git worktree remove --force "$baseline_dir" >/dev/null 2>&1 || true
  git worktree remove --force "$current_dir" >/dev/null 2>&1 || true
  rm -rf "$work_root"
}
trap cleanup EXIT

git worktree remove --force "$baseline_dir" >/dev/null 2>&1 || true
git worktree remove --force "$current_dir" >/dev/null 2>&1 || true
rm -rf "$baseline_dir" "$current_dir"

git worktree add --detach "$baseline_dir" "$baseline_sha" >/dev/null
git worktree add --detach "$current_dir" "$current_sha" >/dev/null

overlay_files=()
if [[ "$overlay_bench" -eq 1 ]]; then
  overlay_files+=("tests/benchmarks/ingestion_throughput_bench.cpp")
fi

overlay_into_worktree() {
  local worktree="$1"
  local rel
  for rel in "${overlay_files[@]}"; do
    if [[ -f "${repo_root}/${rel}" && -f "${worktree}/${rel}" ]]; then
      cp "${repo_root}/${rel}" "${worktree}/${rel}"
    fi
  done
}

find_native_file() {
  local worktree="$1"
  local candidates=(
    "${worktree}/build/release/build-release/conan/conan_meson_native.ini"
    "${worktree}/build/release/conan/conan_meson_native.ini"
    "${worktree}/build/release/conan_meson_native.ini"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -f "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  printf 'Unable to locate Conan Meson native file under %s/build/release\n' "$worktree" >&2
  return 1
}

generate_throughput_config() {
  local out_json="$1"
  local out_jsonl="$2"
  DATASET_PATH="$dataset_path" OUT_JSON="$out_json" OUT_JSONL="$out_jsonl" \
    REPEAT_COUNT="$repeat_count" python3 - <<'PY'
import json
import os
from pathlib import Path

cfg = {
    "dataset_path": os.environ["DATASET_PATH"],
    "fixture_manifest": "unused",
    "output_metrics": os.environ["OUT_JSONL"],
    "post_run_cleanup": True,
    "runs": [
        {
            "label": "taskflow-ab-1w",
            "workers": 1,
            "repeat": int(os.environ["REPEAT_COUNT"]),
            "args": [
                "--recursive",
                "--include=*.cpp,*.h,*.hpp",
                "--exclude=**/.git/**,**/node_modules/**,**/build/**",
            ],
        }
    ],
}

Path(os.environ["OUT_JSON"]).write_text(json.dumps(cfg, indent=2) + "\n")
PY
}

build_worktree() {
  local worktree="$1"
  overlay_into_worktree "$worktree"

  git -C "$worktree" submodule update --init --recursive >/dev/null

  conan install "$worktree" -of "$worktree/build/release" \
    -s build_type=Release \
    -s compiler.cppstd=20 \
    -b missing >/dev/null

  local native_file
  native_file="$(find_native_file "$worktree")"

  if [[ -d "$worktree/build/release" && -f "$worktree/build/release/build.ninja" ]]; then
    meson setup "$worktree/build/release" \
      "$worktree" \
      --reconfigure \
      --native-file "$native_file" \
      --buildtype=release \
      -Dbuild-tests=true \
      -Dbuild-benchmarks=true \
      >/dev/null
  else
    meson setup "$worktree/build/release" \
      "$worktree" \
      --native-file "$native_file" \
      --buildtype=release \
      -Dbuild-tests=true \
      -Dbuild-benchmarks=true \
      >/dev/null
  fi

  meson compile -C "$worktree/build/release" -j"$build_jobs" yams_api_benchmarks ingestion_throughput_bench >/dev/null
}

run_worktree() {
  local label="$1"
  local worktree="$2"
  local sha="$3"
  local short_sha
  short_sha="$(git -C "$worktree" rev-parse --short HEAD)"

  local api_jsonl="${results_dir}/${label}-${short_sha}-api.jsonl"
  local throughput_json="${results_dir}/${label}-${short_sha}-throughput.json"
  local throughput_jsonl="${results_dir}/${label}-${short_sha}-throughput.jsonl"

  generate_throughput_config "$throughput_json" "$throughput_jsonl"

  (
    cd "$worktree/build/release"
    ./tests/benchmarks/yams_api_benchmarks --iterations "$api_iters" --quiet --no-archive --output "$api_jsonl" >/dev/null
    ./tests/benchmarks/ingestion_throughput_bench --config "$throughput_json" >/dev/null
  )

  LABEL="$label" SHA="$sha" API_JSONL="$api_jsonl" THROUGHPUT_JSONL="$throughput_jsonl" \
    SUMMARY_JSON="${results_dir}/${label}-${short_sha}-summary.json" python3 - <<'PY'
import json
import os
from pathlib import Path

label = os.environ["LABEL"]
sha = os.environ["SHA"]
api_path = Path(os.environ["API_JSONL"])
throughput_path = Path(os.environ["THROUGHPUT_JSONL"])
summary_path = Path(os.environ["SUMMARY_JSON"])

summary = {
    "label": label,
    "sha": sha,
    "api": {},
    "throughput": {},
}

if api_path.exists():
    for line in api_path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        summary["api"][row["name"]] = {
            "ops_per_sec": row.get("ops_per_sec"),
            "duration_ms": row.get("duration_ms"),
        }

if throughput_path.exists():
    rows = [json.loads(line) for line in throughput_path.read_text().splitlines() if line.strip()]
    if rows:
        summary["throughput"] = {
            "avg_docs_per_sec": sum(r["throughput_files_per_second"] for r in rows) / len(rows),
            "avg_duration_seconds": sum(r["duration_seconds"] for r in rows) / len(rows),
            "avg_documents_total": sum(r.get("documents_total", 0) for r in rows) / len(rows),
            "all_drained": all(bool(r.get("drained")) for r in rows),
            "samples": len(rows),
        }

summary_path.write_text(json.dumps(summary, indent=2) + "\n")
PY
}

printf 'Taskflow A/B benchmark\n'
printf '  baseline: %s (%s)\n' "$baseline_ref" "$baseline_short"
printf '  current : %s (%s)\n' "$current_ref" "$current_short"
printf '  dataset : %s\n' "$dataset_path"
printf '  workroot: %s\n' "$work_root"

build_worktree "$baseline_dir"
run_worktree baseline "$baseline_dir" "$baseline_sha"

build_worktree "$current_dir"
run_worktree current "$current_dir" "$current_sha"

BASELINE_SUMMARY="${results_dir}/baseline-${baseline_short}-summary.json" \
CURRENT_SUMMARY="${results_dir}/current-${current_short}-summary.json" \
RESULTS_DIR="$results_dir" python3 - <<'PY'
import json
import os
from pathlib import Path

results_dir = Path(os.environ["RESULTS_DIR"])
baseline = json.loads(Path(os.environ["BASELINE_SUMMARY"]).read_text())
current = json.loads(Path(os.environ["CURRENT_SUMMARY"]).read_text())

def delta(cur, base):
    if base in (None, 0):
        return None
    return ((cur - base) / base) * 100.0

print("\nAPI throughput delta (same-machine)")
for name in sorted(set(baseline["api"]) | set(current["api"])):
    b = baseline["api"].get(name, {}).get("ops_per_sec")
    c = current["api"].get(name, {}).get("ops_per_sec")
    d = delta(c, b) if b is not None and c is not None else None
    if b is None or c is None:
        print(f"  {name}: unavailable")
    else:
        print(f"  {name}: {b:.2f} -> {c:.2f} ({d:+.1f}%)")

print("\nFull pipeline throughput delta (same-machine)")
b = baseline.get("throughput", {}).get("avg_docs_per_sec")
c = current.get("throughput", {}).get("avg_docs_per_sec")
d = delta(c, b) if b is not None and c is not None else None
if b is None or c is None:
    print("  unavailable")
else:
    print(f"  docs/sec: {b:.2f} -> {c:.2f} ({d:+.1f}%)")
    print(f"  baseline drained: {baseline['throughput'].get('all_drained')}")
    print(f"  current  drained: {current['throughput'].get('all_drained')}")

print("\nArtifacts")
print(f"  {results_dir}")
PY

if [[ "$keep_worktrees" -eq 1 ]]; then
  printf '\nKept worktrees and results under %s\n' "$work_root"
fi
