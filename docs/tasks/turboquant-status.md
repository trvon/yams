# TurboQuant Status

TASK: turboquant-doc-cleanup
MODE: engineering
PHASE: complete
AGENT: opencode-turboquant-doc-cleanup

CONTEXT FOUND:
- YAMS: prior TurboQuant plans, benchmark notes, and exploratory compressed-ANN artifacts were indexed before cleanup.
- Repo: `TurboQuantPackedReranker` is now the default TurboQuant search feature and compressed ANN remains opt-in.

ACTIONS:
- Consolidated the TurboQuant planning, benchmark, and decision trail into this single status document.
- Removed exploratory compressed-ANN benchmark sources that are no longer registered in `tests/benchmarks/meson.build`.
- Removed superseded task documents that were used during design and investigation but are not meant to ship.
- Kept the active benchmark pair: `tests/benchmarks/turboquant_bench.cpp` and `tests/benchmarks/compressed_ann_decision_bench.cpp`.

CURRENT DECISION:
- Default `enableTurboQuantRerank = true` in `include/yams/search/search_engine.h`.
- Keep `enableCompressedANN = false` by default.
- Treat compressed ANN as an optional coarse retrieval experiment, not as a replacement for the regular vector path plus reranking.
- If compressed ANN is enabled, keep `compressedAnnBits` aligned with the stored TurboQuant sidecar; current evidence favors 8-bit over 4-bit for any standalone ANN experiment.

IMPLEMENTATION STATUS:
- `TurboQuantMSE` storage integration is stable, including quantized-primary persistence and float reconstruction on read.
- `TurboQuantPackedReranker` is stable and is the default-on TurboQuant path in search.
- `CompressedANNIndex` invalidation is wired through ingest so the packed index rebuilds lazily on the next query.
- Benchmark registration is reduced to the canonical validation pair in `tests/benchmarks/meson.build`.

DECISION EVIDENCE:
- Real YAMS indexed-data benchmark (`tests/benchmarks/compressed_ann_decision_bench.cpp`):
  - `4b-fit`: ceiling `41-42%` R@1, graph `45%`, reranker `67%`
  - `8b-unfit`: ceiling `45-56%` R@1, graph `56%`, reranker `62%`
  - `8b-fit`: ceiling `56-58%` R@1, graph `58-60%`, reranker `72%`
- Interpretation:
  - 4-bit standalone compressed ANN is not viable.
  - 8-bit standalone compressed ANN is only marginally viable.
  - `TurboQuantPackedReranker` remains the quality anchor and the production path.

RETAINED ARTIFACTS:
- Code:
  - `include/yams/search/search_engine.h`
  - `src/search/search_engine.cpp`
  - `include/yams/search/turboquant_packed_reranker.h`
  - `src/search/turboquant_packed_reranker.cpp`
  - `include/yams/vector/compressed_ann.h`
  - `src/vector/compressed_ann.cpp`
- Benchmarks:
  - `tests/benchmarks/turboquant_bench.cpp`
  - `tests/benchmarks/compressed_ann_decision_bench.cpp`

NEXT:
- Keep `turboquant_bench` as the implementation/reference benchmark.
- Keep `compressed_ann_decision_bench` as the only compressed-ANN shipping decision benchmark.
- Revisit compressed ANN only if new real-data results or latency requirements justify another round of evaluation.

USED_CONTEXT:
- indexed cleanup artifacts in YAMS (checkpointed before deletion)

CITATIONS:
- `include/yams/search/search_engine.h`
- `src/search/search_engine.cpp`
- `tests/benchmarks/meson.build`
- `tests/benchmarks/compressed_ann_decision_bench.cpp`
- `tests/benchmarks/turboquant_bench.cpp`
