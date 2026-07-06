# Lean-Guided Optimization Lane — 2026-07-06

## Goal

Use Lean contracts as the guardrails for future ingestion/search optimizations. The lane separates what Lean proves from what C++ benchmarks measure:

- Lean proves that optimizations preserve visibility, publication order, and searchability preconditions.
- Benchmarks measure latency, throughput, and ranking/search effects.

## Current proof anchors

- `formal/topology/Yams/Topology/Storage.lean`
  - `StoreOptimizationContract`
  - `batchedCommit_contract`
  - Guards durable content/manifest and committed refs before metadata visibility.
- `formal/topology/Yams/Topology/Contracts.lean`
  - `PipelineCompletionContract`
  - `completion_keywordSearchable`
  - `completion_semanticSearchable`
  - `completion_graphNavigable`
  - Guards that completed pipeline stages imply keyword, semantic, and graph availability.

## Lane stages

### 1. Search-impact instrumentation

Add fixed post-ingest probes to the ingestion ablation harness:

- keyword query latency and result count
- semantic query latency and result count, when embeddings are enabled
- graph/rerank availability or route count, when KG/rerank stages are enabled
- optional top-k hash stability for deterministic fixture queries

Done when each ablation JSON includes a `search_impact` section.

### 2. Lean benchmark contract surface

Extend the Lean model with measured-observation contracts that can validate imported benchmark summaries, for example:

- keyword result count is non-zero when `completion_keywordSearchable` applies
- semantic probe is skipped/disabled unless `completion_semanticSearchable` applies
- graph probe is skipped/disabled unless `completion_graphNavigable` applies
- latency regression checks are explicit threshold predicates over measured values

Done when `lake build` proves the contracts over example observations.

### 3. Optimization candidates under contract

Prioritize changes only when they preserve the proof boundary:

- Storage encoding and commit batching: covered by `StoreOptimizationContract`.
- Pipeline scheduling and stage fusion: covered by `PipelineCompletionContract`.
- Search-path fast paths: require a new Lean contract for query result equivalence or bounded degradation.

Done when each optimization proposal names the Lean contract it preserves before C++ edits land.

### 4. Validation gate

Each optimization must run:

```bash
source ~/.zshenv 2>/dev/null || true
meson compile -C build/debug -j4 <focused-target>
meson test -C build/debug <focused-test> --print-errorlogs
cd formal/topology && lake build
YAMS_BENCH_CORPUS_SIZE=80 YAMS_BENCH_DOC_SIZE=1000 \
  tests/benchmarks/scripts/ingestion_pipeline_ablation.sh
```

If the change can affect search, also run the search-impact probes and compare against the prior artifact.
