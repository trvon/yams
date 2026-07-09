#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
BUILD_DIR="${YAMS_BENCH_BUILD_DIR:-${YAMS_BUILD_DIR:-$ROOT/build/release}}"
exec python3 "$ROOT/tests/benchmarks/xplan/runner.py" run simeon_rerank --build-dir "$BUILD_DIR" "$@"
