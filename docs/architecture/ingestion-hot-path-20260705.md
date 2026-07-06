# Ingestion Hot Path — 2026-07-05

Primary artifacts: `build/benchmarks/ingestion-pipeline-ablation/20260705-235409/`

This note selects the first optimization target for the Lean-driven correctness / optimization loop.

## Decision

Optimize the document/content storage path first, not topology/KG graph algorithms.

The best first target is the per-document content-store commit sequence:

1. chunk existence/store/reference preparation
2. manifest serialization/store
3. reference-counter commit
4. synchronous document-service wait around content-store + metadata insert

## Evidence

Baseline workload: 80 documents, ~1000 bytes each.

```text
baseline total wall time: 811 ms
baseline throughput:       98.64 docs/s
```

Dominant cumulative phase timings:

| Phase | Total ms | Calls | Max ms | Source hook |
| --- | ---: | ---: | ---: | --- |
| document `store_total` | 2885 | 80 | 51 | `src/app/services/document_service.cpp:1457` |
| document `content_store` | 2585 | 80 | 48 | `src/app/services/document_service.cpp:1395` |
| content `store_total` | 2585 | 80 | 48 | `src/api/content_store_impl.cpp:349` |
| content `manifest_store` | 1162 | 80 | 26 | `src/api/content_store_impl.cpp:326` |
| content `chunk_store_refs` | 965 | 80 | 34 | `src/api/content_store_impl.cpp:287` |
| content `ref_commit` | 369 | 80 | 13 | `src/api/content_store_impl.cpp:335` |
| document `metadata_insert` | 226 | 80 | 11 | `src/app/services/document_service.cpp:1444` |

By comparison, optional enrichment is smaller or mostly queue/drain time:

| Phase | Total ms | Calls | Max ms |
| --- | ---: | ---: | ---: |
| post `process_batch` | 314 | 21 | 43 |
| post `commit_content_index` | 221 | 21 | 42 |
| embedding `build_records` / `vdb_insert` | 101 | 9 | 31 |
| embedding `semantic_graph_update` | 7 | 9 | 4 |
| KG `kg_queue_wait` | 2133 | 80 | 94 |
| KG graph component/process CPU | 0 | n/a | 0 |

`kg_queue_wait` is high cumulatively, but the KG graph component itself did not register measurable work in this run. That points to drain scheduling/overlap rather than graph algorithm CPU.

Ablation wall-time deltas also support this interpretation:

| Variant | Total ms | Delta vs baseline | Docs/s |
| --- | ---: | ---: | ---: |
| `baseline` | 811 | 0 | 98.64 |
| `no_kg` | 719 | -92 ms | 111.27 |
| `no_vectors` | 709 | -102 ms | 112.83 |
| `no_vectors_no_kg` | 713 | -98 ms | 112.20 |
| `no_gliner` | 720 | -91 ms | 111.11 |

The `no_kg` and `no_vectors` improvements are similar and are not additive. This suggests they are waiting behind the same storage/post-ingest drain rather than consuming independent dominant CPU budgets.

## Optimization Hypothesis

The likely high-ROI optimization is to reduce per-document synchronous storage commits and metadata/ref-counter write amplification.

Candidate changes to investigate, in order:

1. Batch or coalesce reference-counter commits for ingestion batches while preserving immediate visibility guarantees required by callers.
2. Avoid redundant per-document manifest writes when content is already present and manifest content is identical.
3. Reduce `chunk_store_refs` work for small files / single-chunk documents by collapsing repeated existence + size lookups where correctness permits.
4. Add more timing detail inside `manifest_store` and `chunk_store_refs` before larger rewrites, so we know whether cost is storage engine I/O, compression, locking, or reference metadata.

## Correctness Contract To Preserve

Any optimization must preserve these observable semantics:

- a successful store publishes durable content and manifest before metadata visibility;
- reference counters are committed before metadata publication;
- rollback removes newly stored chunks/manifest if a later phase fails;
- duplicate content remains deduplicated and reference counts remain correct;
- post-ingest completion still implies keyword/semantic/graph availability according to the enabled pipeline stages.

These are the right next Lean proof targets for task #8: model the store transaction order as a small state machine and prove a batched/coalesced commit is observationally equivalent to per-document commits under the required visibility boundary.

## KPI

Initial KPI target for task #9:

- reduce baseline wall time by at least 8–10%;
- reduce cumulative `document_store_phase_timings.store_total` and `content_store_phase_timings.store_total`;
- keep `pipeline_status.complete = true` and `dropped_batches = 0` for all ablation variants.

Validation command after implementation:

```bash
source ~/.zshenv 2>/dev/null || true
meson compile -C build/debug -j4 bench_ingestion_e2e
YAMS_BENCH_CORPUS_SIZE=80 YAMS_BENCH_DOC_SIZE=1000 \
  tests/benchmarks/scripts/ingestion_pipeline_ablation.sh
```
