#!/usr/bin/env bash
set -euo pipefail

# Runs the 5-variant x N-tier ladder for the topology routing-variant ablation.
# Tiers:
#   S   synthetic   6 topics / 13 docs/topic / 12 noise      (~90 docs, mock embeds)
#   M   synthetic  12 topics / 40 docs/topic / 60 noise      (~540 docs, mock embeds)
#   L   synthetic  24 topics / 80 docs/topic / 120 noise     (~2K docs, mock embeds)
#   L+  synthetic  24 topics / 80 docs/topic / 120 noise     (~2K docs, simeon embeds)
#   XL  BEIR nfcorpus                                        (3.6K docs, simeon embeds)
#   XXL BEIR scifact                                         (5K docs,   simeon embeds)
#
# Override tier list with YAMS_LADDER_TIERS=S,M,XL (comma-separated).
# XL/XXL are skipped with a warning if the cached dataset is missing.

TIERS_DEFAULT="S M L L+ XL XXL"
BIN="${YAMS_BENCH_BIN:-build/debug/tests/benchmarks/topology_ablation_quality_bench}"
OUT_DIR="${YAMS_BENCH_OUT_DIR:-/tmp}"
WAIT_MS="${YAMS_BENCH_TOPOLOGY_WAIT_MS:-300000}"
BEIR_CACHE="${YAMS_BENCH_BEIR_CACHE_ROOT:-$HOME/.cache/yams/benchmarks}"

if [[ ! -x "$BIN" ]]; then
  echo "error: bench binary not found at $BIN" >&2
  echo "  build with: meson compile -C build/debug topology_ablation_quality_bench" >&2
  exit 1
fi

# Tier selection.
if [[ -n "${YAMS_LADDER_TIERS:-}" ]]; then
  TIERS="${YAMS_LADDER_TIERS//,/ }"
else
  TIERS="$TIERS_DEFAULT"
fi

# Per-tier env (macOS bash 3.x compatible: parallel indexed arrays, no declare -A).
tier_name=()
tier_env=()

add_tier() {
  tier_name+=("$1")
  tier_env+=("$2")
}

add_tier "S"   "YAMS_BENCH_TOPICS=6 YAMS_BENCH_DOCS_PER_TOPIC=13 YAMS_BENCH_NOISE_DOCS=12"
add_tier "M"   "YAMS_BENCH_TOPICS=12 YAMS_BENCH_DOCS_PER_TOPIC=40 YAMS_BENCH_NOISE_DOCS=60"
add_tier "L"   "YAMS_BENCH_TOPICS=24 YAMS_BENCH_DOCS_PER_TOPIC=80 YAMS_BENCH_NOISE_DOCS=120"
add_tier "L+"  "YAMS_BENCH_TOPICS=24 YAMS_BENCH_DOCS_PER_TOPIC=80 YAMS_BENCH_NOISE_DOCS=120"
add_tier "XL"  "YAMS_BENCH_BEIR_DATASET=nfcorpus"
add_tier "XXL" "YAMS_BENCH_BEIR_DATASET=scifact"

lookup_env() {
  local want="$1"
  local i=0
  while [[ $i -lt ${#tier_name[@]} ]]; do
    if [[ "${tier_name[$i]}" == "$want" ]]; then
      echo "${tier_env[$i]}"
      return 0
    fi
    i=$((i+1))
  done
  return 1
}

beir_ready() {
  local ds="$1"
  [[ -f "$BEIR_CACHE/$ds/corpus.jsonl" ]] && [[ -f "$BEIR_CACHE/$ds/queries.jsonl" ]] && [[ -f "$BEIR_CACHE/$ds/qrels/test.tsv" ]]
}

ran_files=()
for tier in $TIERS; do
  env_block=$(lookup_env "$tier" || true)
  if [[ -z "$env_block" ]]; then
    echo "warn: unknown tier '$tier', skipping" >&2
    continue
  fi

  if [[ "$tier" == "XL" ]] && ! beir_ready nfcorpus; then
    echo "warn: skipping tier XL, NFCorpus missing at $BEIR_CACHE/nfcorpus" >&2
    continue
  fi
  if [[ "$tier" == "XXL" ]] && ! beir_ready scifact; then
    echo "warn: skipping tier XXL, SciFact missing at $BEIR_CACHE/scifact" >&2
    continue
  fi

  tier_slug="${tier//+/p}"
  out="$OUT_DIR/topo_quality_ladder_${tier_slug}.jsonl"
  rm -f "$out"

  echo "=== Tier $tier -> $out ==="
  # shellcheck disable=SC2086
  env $env_block \
    YAMS_BENCH_OUTPUT="$out" \
    YAMS_BENCH_TIER="$tier" \
    YAMS_BENCH_TOPOLOGY_WAIT_MS="$WAIT_MS" \
    "$BIN" '*axis-3 routing variants on corpus ladder*' \
    > "$OUT_DIR/topo_quality_ladder_${tier_slug}.log" 2>&1 \
    || echo "warn: tier $tier bench exited non-zero (see $OUT_DIR/topo_quality_ladder_${tier_slug}.log)" >&2

  if [[ -f "$out" ]] && [[ -s "$out" ]]; then
    ran_files+=("$out")
  else
    echo "warn: tier $tier produced no JSONL" >&2
  fi
done

if [[ ${#ran_files[@]} -eq 0 ]]; then
  echo "No tiers produced output." >&2
  exit 1
fi

echo
echo "=== Phase D ladder summary ==="
jq -sr '
  def pick: {tier: .tier, variant: .variant, ndcg: .ndcg_at_k, mrr: .mrr_at_k,
             recall: .recall_at_k, backend: .embedding_backend,
             seeded: .topology_seeded};
  [.[] | pick] as $rows |
  ($rows | group_by(.tier)) as $by_tier |
  "tier | backend | variant         | nDCG@10 | MRR@10 | Δ nDCG",
  "-----+---------+-----------------+---------+--------+-------" ,
  ($by_tier | map(
    . as $tier_rows |
    ($tier_rows[] | select(.variant=="baseline") | .ndcg) as $base |
    $tier_rows | map(
      [.tier, .backend, .variant,
       (.ndcg | tostring | .[:7]),
       (.mrr  | tostring | .[:6]),
       ((.ndcg - $base) | tostring | .[:6])] | @tsv
    )
  ) | flatten | .[])
' "${ran_files[@]}" | column -t -s $'\t'
