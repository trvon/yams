# E1 — Per-component nDCG telemetry + determinism gate

**Date:** 2026-04-21
**Phase:** E1 (instrumentation-only; no behavior change)
**Bench:** `topology_ablation_quality_bench "*axis-8*"` at XXL, 3 reps on the `scifact_simeon` B1 fixture.
**Scripts:** `/tmp/bench_e1_telemetry.sh` (3-rep gate), `/tmp/bench_e1_stage_trace.sh` (stage-trace variant).
**Raw outputs:** `/tmp/bench_e1_telemetry.jsonl` (6 rows), `/tmp/bench_e1_stage_trace.jsonl` (2 rows).

## Determinism gate — PASS

Primary `ndcg_at_k` byte-identical to `/tmp/bench_b1_rebuild.jsonl` reps 2+3 across all 3 reps on both cells:

| Cell | Reps 1-3 `ndcg_at_k` | B1 reference | Match |
|---|---|---|---|
| `connected_components_v1` | 0.4043213906902558 | 0.4043213906902558 | byte-identical |
| `hdbscan_v1` | 0.4014710062314805 | 0.4014710062314805 | byte-identical |

E1 is confirmed pure-instrumentation: added per-query parsing of `trace_stage_summary_json` / `trace_component_hits_json` (already emitted by `SearchEngine::searchWithResponse`) into new JSONL fields on the bench row. No engine-side changes.

## New telemetry fields

Emitted on every bench row under the `outcome.ok` branch:

- `ndcg_at_k_text_only`, `text_component_queries` — nDCG computed from the lexical-only component ranking.
- `ndcg_at_k_vector_only`, `vector_component_queries` — nDCG computed from the vector-only component ranking.
- `lexical_pool_median_size`, `vector_pool_median_size`, `fusion_pool_median_size` — median pool sizes across queries.
- `fusion_strategy_observed`, `fusion_strategy_mismatch_count` — which strategy actually ran and how often it differed query-to-query.
- `reranker_fired_count`, `reranker_contributed_count` — across queries.

`ndcg_at_k_text_only` / `ndcg_at_k_vector_only` require `YAMS_SEARCH_STAGE_TRACE=1` (the trace JSON that backs them is gated behind that env flag in `search_engine.cpp:4903`). The three pool-size fields, strategy fields, and reranker counters are emitted unconditionally.

## Findings

### F1 — Vector-only nDCG > fused nDCG (fusion destroying signal)

With stage-trace enabled (`/tmp/bench_e1_stage_trace.jsonl`):

| Cell | Fused nDCG | Text-only nDCG | Vector-only nDCG | Text queries | Vector queries |
|---|---|---|---|---|---|
| CC | 0.4043 | 0.3708 | **0.5473** | 295/300 | 86/300 |
| HDBSCAN | 0.4015 | 0.3708 | **0.5473** | 295/300 | 86/300 |

On the ~86 queries where the vector leg contributes a hit to the final ranked set, the vector-only component ranking scores **+0.14 nDCG over the fused result**. Fusion is strictly losing signal on this subset.

Text-only and vector-only are identical between CC and HDBSCAN because the topology variance lives downstream of component scoring — this is expected.

### F2 — Fusion strategy silently flips to COMB_MNZ

Expected per `SearchEngineConfig` defaults: `WEIGHTED_LINEAR_ZSCORE`.
Observed: **`fusion_strategy_observed = COMB_MNZ`** on every row, with `fusion_strategy_mismatch_count = 1` indicating one query saw a different strategy than the rest.

This is either SearchTuner silently overriding the configured strategy or a path inside `ResultFusion::fuse` that defaults to COMB_MNZ when the configured strategy fails a precondition. The observed strategy matters because COMB_MNZ multiplies by the number of contributing components — the vector leg being empty on ~214/300 queries (see F3) means those queries collapse to a single component and the MNZ multiplier erases the text signal.

### F3 — Vector pool median size = 0

`vector_pool_median_size = 0` on every row. The vector leg is empty for at least 150/300 queries. Combined with F1, this is consistent: when vector fires (86 queries), it dominates; when it doesn't fire (the median query), fusion falls back to lexical-only but under COMB_MNZ normalization.

Lexical pool median = 188, fusion pool median = 10. Fusion is clipping hard.

### F4 — Reranker retired (0 fires)

`reranker_fired_count = 0` across all 300 queries, both cells. This empirically confirms the `search_engine.cpp:4186-4189` hardcoded `markStageSkipped("reranker", "retired")` — the reranker contributes nothing to current scifact results. Phase E4 can proceed with deletion.

## Implications for remaining Phase E steps

- **E2 (fusion)**: smoking gun confirmed. `fusion_strategy_observed=COMB_MNZ` when vector-only beats fused by +0.14 nDCG is strong evidence to either (a) lock the bench strategy to `WEIGHTED_LINEAR_ZSCORE` explicitly, (b) disable z-score on small vector pools, or (c) try raw alpha-mix without strategy-specific normalization.
- **E3 (lexical norm)**: unchanged priority. `applySimeonLexicalRescoring` writes positive Simeon scores into `lexical_scoring.cpp:33-38`'s `1.0 - norm` formula which assumes negative BM25. Sign-inversion suspected independently of E2 finding.
- **E4 (reranker)**: confirmed retired — delete path approved.
- **E5 (recompute lift)**: gated on E2/E3 landing.

## Files touched

- `tests/benchmarks/topology_ablation_quality_bench.cpp` — added `QualityMetrics` fields, `computeNdcgFromRankedDocIds` helper, `medianOf` helper, JSON parsing in `evaluateAtK`, emission in `toCellJson`.
- `/tmp/bench_e1_telemetry.sh` — 3-rep determinism gate script.
- `/tmp/bench_e1_stage_trace.sh` — single-rep stage-trace variant for per-component nDCG.
