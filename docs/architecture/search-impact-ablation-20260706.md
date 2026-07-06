# Search Impact Ablation — 2026-07-06

## Artifact

```text
build/benchmarks/ingestion-pipeline-ablation/20260706-092341/
```

This run used the ingestion ablation harness after adding fixed post-ingest search probes. Pipeline duration still measures ingest-to-completion only; search probe latency is recorded separately under `search_impact`.

## Ingestion comparison

Compared with the prior post-compression-threshold artifact:

```text
build/benchmarks/ingestion-pipeline-ablation/20260706-005504/
```

| Variant | Prior | Search-probe run |
| --- | ---: | ---: |
| baseline | 726 ms / 110.19 docs/s | 600 ms / 133.33 docs/s |
| no_kg | 618 ms / 129.45 docs/s | 699 ms / 114.45 docs/s |
| no_vectors | 843 ms / 94.90 docs/s | 703 ms / 113.80 docs/s |
| no_vectors_no_kg | 642 ms / 124.61 docs/s | 611 ms / 130.93 docs/s |
| no_gliner | 832 ms / 96.15 docs/s | 798 ms / 100.25 docs/s |

## Search probes

| Variant | Keyword | Semantic | Graph/rerank hybrid |
| --- | ---: | ---: | ---: |
| baseline | 3 ms, 80 total | 19 ms, 10 total | 7 ms, 10 total |
| no_kg | 2 ms, 80 total | 19 ms, 10 total | skipped: kg_disabled |
| no_vectors | 3 ms, 80 total | skipped: vectors_disabled | skipped: vectors_disabled |
| no_vectors_no_kg | 3 ms, 80 total | skipped: vectors_disabled | skipped: kg_disabled |
| no_gliner | 3 ms, 80 total | 18 ms, 10 total | 6 ms, 10 total |

## Interpretation

- Keyword searchability is preserved across all variants: each keyword probe returns all 80 documents.
- Semantic probes are required and successful only when vectors are enabled.
- Graph/rerank hybrid probes are required and successful only when both vectors and KG are enabled.
- These skip/success conditions match the Lean contracts in `Yams.Topology.SearchImpact`:
  - `probeRequiredByConfig`
  - `SearchImpactObservationContract`
  - `completion_*ProbeCapability`
  - `SearchResultEquivalenceContract`
