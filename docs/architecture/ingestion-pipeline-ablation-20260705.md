# Ingestion Pipeline Ablation — 2026-07-05

Artifacts: `build/benchmarks/ingestion-pipeline-ablation/20260705-235409/`

Configuration: 80 documents, ~1000 bytes each, `YAMS_BENCH_POLL_INTERVAL_MS=100`.
All variants completed and reported `dropped_batches = 0`.

## Variant Summary

| Variant | Total ms | Delta vs baseline | Docs/s | Observed post/embed/kg | Interpretation |
| --- | ---: | ---: | ---: | --- | --- |
| `baseline` | 811 | 0 | 98.64 | 80/80/80 | Reference path. |
| `no_kg` | 719 | -92 ms (-11.3%) | 111.27 | 80/80/0 | KG disable improves wall time, but not by removing measurable graph CPU. |
| `no_vectors` | 709 | -102 ms (-12.6%) | 112.83 | 80/0/80 | Vector disable improves wall time similarly. |
| `no_vectors_no_kg` | 713 | -98 ms (-12.1%) | 112.20 | 80/0/0 | Effects are not additive; likely overlap behind same drain/storage bottleneck. |
| `no_gliner` | 720 | -91 ms (-11.2%) | 111.11 | 80/80/80 | Similar improvement, but entity/title queues stayed zero, so this run does not isolate GLiNER CPU. |

## Bottleneck Signals

The dominant measured cumulative time is document/content storage rather than graph or embedding compute:

```text
baseline document_store_total: 2885 ms cumulative
baseline content_store:        2585 ms cumulative
  manifest_store:              1162 ms
  chunk_store_refs:             965 ms
  ref_commit:                   369 ms
```

Post-ingest text/index work is visible but smaller:

```text
baseline post process_batch:   314 ms
baseline commit_content_index: 221 ms
baseline prepare_metadata:      92 ms
```

Embedding is not the hot path for this workload:

```text
baseline vdb_insert/build_records: 101 ms
baseline gather:                    25 ms
baseline infer:                      0 ms
baseline semantic_graph_update:      7 ms
```

KG graph compute does not show measurable CPU time in this run:

```text
baseline kg_graph_component: 0 ms
baseline kg_process_batch:   0 ms
baseline kg_queue_wait:   2133 ms cumulative wait
```

The high `kg_queue_wait` appears to be queue/drain timing rather than graph algorithm cost. Queue samples show store-document backlog peaking around 39–51 tasks, while KG/embed depths are modest.

## Decision

Do not optimize topology/KG algorithms first based on this ablation. The first optimization target should be the storage/content commit path, especially manifest storage, chunk-store reference writes, and reference-counter commit batching.

Use Lean immediately for correctness contracts, but use benchmark data to rank performance work:

1. State semantic contracts in Lean for pipeline/search/topology behavior.
2. Add C++ tests that exercise those contracts.
3. Use ablations/microbenchmarks to select the hot path.
4. Prove the proposed optimization preserves the Lean contract.
5. Land C++ optimization with focused tests and rerun this ablation.

## Next Measurement

Before changing code, run a focused storage-path benchmark or add phase-level detail around:

- manifest-store write batching
- `chunk_store_refs`
- `ref_commit` / reference-counter commit path
- post-ingest `commit_content_index`

Expected KPI: reduce baseline wall time and cumulative `document_store_total` without regressing pipeline completion or searchability invariants.
