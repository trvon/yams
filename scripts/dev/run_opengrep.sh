#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

ENGINE="${OPENGREP_BIN:-}"
if [[ -z "$ENGINE" ]]; then
  if command -v opengrep >/dev/null 2>&1; then
    ENGINE="opengrep"
  elif command -v semgrep >/dev/null 2>&1; then
    ENGINE="semgrep"
    echo "[opengrep] opengrep not found; using semgrep-compatible fallback" >&2
  else
    echo "error: opengrep not found (and semgrep fallback unavailable)" >&2
    echo "install hint: curl -fsSL https://raw.githubusercontent.com/opengrep/opengrep/main/install.sh | bash" >&2
    exit 127
  fi
fi

OUT_DIR="${OPENGREP_OUT_DIR:-.artifacts/opengrep}"
mkdir -p "$OUT_DIR"
PROFILE="${OPENGREP_PROFILE:-default}"
CONFIGS=()
case "$PROFILE" in
  default) CONFIGS=("tools/opengrep/rules/default") ;;
  audit) CONFIGS=("tools/opengrep/rules/audit") ;;
  all) CONFIGS=("tools/opengrep/rules") ;;
  trailofbits) CONFIGS=("p/trailofbits") ;;
  public) CONFIGS=("p/default" "p/security-audit" "p/trailofbits" "p/c") ;;
  *) CONFIGS=("$PROFILE") ;;
esac
JSON_OUT="$OUT_DIR/opengrep-yams-${PROFILE//\//-}.json"
SARIF_OUT="$OUT_DIR/opengrep-yams-${PROFILE//\//-}.sarif"
TARGETS=("${@:-src include tests}")
if [[ $# -eq 0 ]]; then
  TARGETS=(src include tests)
fi

COMMON=(scan
  --timeout "${OPENGREP_TIMEOUT:-60}"
  --exclude build
  --exclude 'build*'
  --exclude third_party
  --exclude external
  --exclude .artifacts
  --exclude plugins/yams-ghidra-plugin
)

for cfg in "${CONFIGS[@]}"; do
  COMMON+=(--config "$cfg")
done

HELP="$($ENGINE scan --help 2>/dev/null || true)"
if grep -q -- '--metrics' <<<"$HELP"; then
  COMMON+=(--metrics=off)
fi
if grep -q -- '--max-target-bytes' <<<"$HELP"; then
  COMMON+=(--max-target-bytes "${OPENGREP_MAX_TARGET_BYTES:-2000000}")
fi
if grep -q -- '--force-exclude' <<<"$HELP"; then
  COMMON+=(--force-exclude)
fi
if [[ "$ENGINE" == *opengrep* ]]; then
  COMMON+=(--taint-intrafile --dynamic-timeout)
  if grep -q -- '--allow-rule-timeout-control' <<<"$HELP"; then
    COMMON+=(--allow-rule-timeout-control)
  fi
  if grep -q -- '--semgrepignore-filename' <<<"$HELP"; then
    COMMON+=(--semgrepignore-filename=.opengrepignore)
  fi
fi

if [[ "${OPENGREP_STRICT:-0}" == "1" ]]; then
  COMMON+=(--error)
fi

# Optional delta-vs-ref mode. Set OPENGREP_BASELINE_COMMIT=<ref> (e.g. HEAD~1
# or origin/main) to filter results to findings new since that commit. Only
# wires through when the installed opengrep/semgrep advertises the flag.
if [[ -n "${OPENGREP_BASELINE_COMMIT:-}" ]]; then
  if grep -q -- '--baseline-commit' <<<"$HELP"; then
    COMMON+=(--baseline-commit "$OPENGREP_BASELINE_COMMIT")
  else
    echo "[opengrep] $ENGINE has no --baseline-commit; ignoring OPENGREP_BASELINE_COMMIT" >&2
  fi
fi

"$ENGINE" "${COMMON[@]}" --json --output "$JSON_OUT" "${TARGETS[@]}"
"$ENGINE" "${COMMON[@]}" --sarif-output "$SARIF_OUT" "${TARGETS[@]}" >/dev/null

echo "[opengrep] profile: $PROFILE"
echo "[opengrep] config:  ${CONFIGS[*]}"
echo "[opengrep] JSON:    $JSON_OUT"
echo "[opengrep] SARIF:   $SARIF_OUT"
