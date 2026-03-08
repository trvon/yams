# Retrieval Precision Optimization

This runbook defines the accuracy-first loop for retrieval tuning.

Scope:

- optimize ranking quality (ordering) from a fixed corpus/query baseline,
- keep vectors enabled,
- avoid mixing throughput tuning concerns into ranking decisions.

## Baseline Inputs

Use existing optimization artifacts as baseline anchors:

- `builddir/retrieval_opt_scifact_isolated.jsonl`
- `builddir/retrieval_opt_scifact_stability.jsonl` (optional repeat set)

## Build a Baseline Summary

Generate a normalized summary artifact from one or more runs:

```bash
python3 tests/scripts/summarize_retrieval_opt_jsonl.py \
  --input builddir/retrieval_opt_scifact_isolated.jsonl \
  --input builddir/retrieval_opt_scifact_stability.jsonl \
  --output builddir/retrieval_opt_scifact_summary.json
```

The summary includes:

- latest successful row per candidate,
- aggregate objective stats per candidate (mean/stddev/min/max),
- canonical winner snapshot and env overrides.

## Accuracy Targets

Primary target:

- `nDCG@10`

Secondary targets:

- `MRR@10`
- `MAP`

Guardrail:

- `recall@100` (do not accept large regressions)

## Precision Levers (Ranking)

Focus tuning on ranking layers in this order:

1. fusion and component weighting,
2. vector-only gating (`vectorOnlyThreshold`, `vectorOnlyPenalty`),
3. full-text score shaping (query-intent-aware penalties),
4. graph rerank,
5. reranker and symbol post-processing.

## Runtime Override Knobs

Enable environment overrides and adjust precision knobs:

```bash
YAMS_ENABLE_ENV_OVERRIDES=1
YAMS_SEARCH_VECTOR_ONLY_THRESHOLD=0.92
YAMS_SEARCH_VECTOR_ONLY_PENALTY=0.75
YAMS_SEARCH_RRF_K=10
YAMS_SEARCH_CONCEPT_BOOST_WEIGHT=0.08
```

These are intended for benchmark/ablation loops before hardcoding defaults.

## Notes

- Keep dataset size/query count constant while comparing ranking configs.
- Prefer isolated candidate execution (one process per candidate) to avoid
  daemon/plugin lifecycle contamination.
- Promote a new baseline only after stable gains on the fixed benchmark slice.
