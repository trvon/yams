# Ingestion Compression Threshold Optimization — 2026-07-06

## Change

`ContentStoreBuilder::createDefaultComponents()` now keeps explicit compression config overrides, but changes the default policy for very small objects:

- `neverCompressBelow = 1024`
- `alwaysCompressAbove = 1024`

Previously the defaults were effectively eager compression for all non-empty objects. The ingestion hot-path measurements showed manifest and chunk-reference storage dominate for ~1 KiB document ingestion, so avoiding compression for sub-KiB chunks/manifests removes CPU work with little storage cost.

## Correctness boundary

This does not change content-addressing, manifest durability, reference-counter commit ordering, or metadata publication order. It only changes the default physical encoding decision for objects below 1 KiB. Retrieval still returns the same bytes, and larger objects continue to use the compression path by default.

## Validation

Commands run after sourcing `~/.zshenv`:

```bash
meson compile -C build/debug -j4 catch2_api_submodule
meson test -C build/debug api_submodule --print-errorlogs
meson compile -C build/debug -j4 catch2_topology_baseline
meson test -C build/debug topology_baseline --print-errorlogs
meson compile -C build/debug -j4 bench_ingestion_e2e
cd formal/topology && lake build
YAMS_BENCH_CORPUS_SIZE=80 YAMS_BENCH_DOC_SIZE=1000 \
  tests/benchmarks/scripts/ingestion_pipeline_ablation.sh
```

The focused API test verifies that an 800-byte object is stored without a compression header under default builder settings and that a 4096-byte object still stores with a compression header.

## Ablation rerun

New artifact directory:

```text
build/benchmarks/ingestion-pipeline-ablation/20260706-005504/
```

Compared against:

```text
build/benchmarks/ingestion-pipeline-ablation/20260705-235409/
```

| Variant | Before | After | Throughput delta |
| --- | ---: | ---: | ---: |
| baseline | 811 ms / 98.64 docs/s | 726 ms / 110.19 docs/s | +11.71% |
| no_kg | 719 ms / 111.27 docs/s | 618 ms / 129.45 docs/s | +16.34% |
| no_vectors | 709 ms / 112.83 docs/s | 843 ms / 94.90 docs/s | -15.90% |
| no_vectors_no_kg | 713 ms / 112.20 docs/s | 642 ms / 124.61 docs/s | +11.06% |
| no_gliner | 720 ms / 111.11 docs/s | 832 ms / 96.15 docs/s | -13.46% |

Baseline storage timing improved in the intended path:

| Metric | Before | After |
| --- | ---: | ---: |
| content_store/store_total | 2585 ms | 2394 ms |
| chunk_store_refs | 965 ms | 802 ms |
| manifest_store | 1162 ms | 1157 ms |
| ref_commit | 369 ms | 358 ms |
| document_store/store_total | 2885 ms | 2659 ms |

The baseline and storage hot-path counters improved; two ablation variants regressed, likely from daemon/background scheduling variance rather than the compression threshold itself because the physical storage path is shared across variants.
