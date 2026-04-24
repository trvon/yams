# Retrieval Precision Optimization

This runbook defines the accuracy-first loop for retrieval tuning.

Scope:

- optimize ranking quality (ordering) from a fixed corpus/query baseline,
- keep vectors enabled,
- avoid mixing throughput tuning concerns into ranking decisions.

## Baseline Inputs

Use one or more JSONL candidate runs from the same fixed corpus/query slice as baseline anchors.

## Build a Baseline Summary

Generate a normalized summary artifact from one or more runs:

```bash
python3 tests/scripts/summarize_retrieval_opt_jsonl.py \
  --input <candidate-run-a.jsonl> \
  --input <candidate-run-b.jsonl> \
  --output <summary.json>
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

## Precision loop with the simeon backend

The default daemon/ONNX embedding path costs ~25–100 ms per document on
CPU, which makes this loop sluggish: re-ingesting a medium corpus between
lever changes takes minutes. The vendored simeon backend
(`third_party/simeon`, `YAMS_EMBED_BACKEND=simeon`) runs at 60k+ docs/sec
in the VerySparse 4096→384 configuration, which turns the loop into
seconds.

Use simeon while iterating on fusion weights / `vectorOnlyThreshold` /
RRF_k; validate the final configuration against daemon afterwards to
catch paraphrase cases where simeon’s lexical sketch underperforms a
learned embedding.

Fast iterate:

```bash
YAMS_ENABLE_ENV_OVERRIDES=1 \
YAMS_EMBED_BACKEND=simeon \
YAMS_SEARCH_VECTOR_ONLY_THRESHOLD=0.92 \
YAMS_SEARCH_VECTOR_ONLY_PENALTY=0.75 \
YAMS_SEARCH_RRF_K=10 \
  ./build/debug/tests/benchmarks/retrieval_quality_bench \
  --benchmark_out=run_simeon.jsonl --benchmark_out_format=json
```

Matrix both backends in one invocation via the topology harness:

```bash
tests/scripts/run_topology_retrieval_matrix.sh \
  --output-dir .artifacts/precision_loop \
  --scale standard \
  --embedding-backends daemon,simeon
```

Each backend’s results land under
`.artifacts/precision_loop/<backend>/`. Every emitted row carries an
`embedding_backend` field so downstream summarizers can group without
rescanning environment state.

See the A/B at
[`simeon_vs_onnx_embedding_ab.md`](simeon_vs_onnx_embedding_ab.md) and
the full simeon ablation at
[`simeon_ablation_matrix.md`](simeon_ablation_matrix.md) for the quality
cost of running the loop under simeon.
