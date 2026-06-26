# YAMS Performance Benchmark Report

**Generated**: 2026-06-09
**Last Updated**: 2026-06-19
**YAMS Version**: 0.17.0
**Build Configuration**: Release microbenchmarks (`meson compile -C build/release`); live-mirror suite (`meson compile -C builddir`)
**Host**: Apple M4 Max, macOS, Clang 21, 16 cores

## Contents

- [Executive Summary](#executive-summary)
- [Latest Local Refresh](#latest-local-refresh)
- [Historical Comparisons](#historical-comparisons)
- [Core Microbenchmarks](#core-microbenchmarks)
- [KG Edge/Entity Insert Microbenchmarks](#kg-edgeentity-insert-microbenchmarks)
- [Simeon Lexical Rescoring Observations](#simeon-lexical-rescoring-observations)
- [Multi-Client Benchmarks](#multi-client-benchmarks)
- [Storage Backends](#storage-backends)

## Executive Summary

- Coverage: API, metadata, core, IPC, tree, WriteCoordinator, multi-client, local storage, grep, search microbenchmarks, and live-mirror ingestion/retrieval.
- Medium-document ingest (`api_benchmarks`, clean release): `363.6 ops/s` p50.
- IPC streaming (clean release): `StreamingFramer_32x10` 22,857 ops/s, `UnaryFramer_8KB` 233,046 ops/s — above the May 9 baselines.
- Multi-client ingest: clean through 16 clients; 32 clients failed with 416 add failures.
- Mixed read/write search p95: 525 ms in the 4-client case, 41 ms in the 16-client preseeded case.
- Grep microbenchmarks: literal search is now close to `std::find`; newline scan still trails `memchr` on this host.
- High-entropy Zstd L3 (1 MiB): 0.11 ms p50, 9,308 ops/s (incompressible input, ratio ~1.0).
- Live-mirror suite (June 19, M4 Max, Simeon embeddings): 500-document ingestion completed in ~983 ms median (~509 docs/s) — improved from the June 18 1,086 ms / 460.4 docs/s baseline via the dedicated-writer ingestion centralization (MetadataInsertWriter + ContentIndexWriter); retrieval over 200 queries holds MRR 1.0 and Recall@10 0.1628.

## Latest Local Refresh

Throughput values use p50 latency where available.

### Live-Mirror Ingestion And Retrieval (2026-06-19)

Validated artifact: `/tmp/yams_live_mirror_suite_20260619_162112` (suite ingestion run 1,059 ms).
Ingestion repeats (steady-state, poll 10 ms): 1,017 / 936 / 983 ms → median **~983 ms**.

Configuration:

- Build dir: `builddir`
- Dataset: synthetic live-mirror workload
- Corpus: 500 documents
- Retrieval queries: 200
- Top K: 10
- Embeddings: real Simeon (`YAMS_EMBED_BACKEND=simeon`, `YAMS_BENCH_FORCE_MOCK_EMBEDDINGS=0`)
- Core systems enabled: plugin discovery, Glint, semantic graph/topology, post-ingest pipeline
- Change since last refresh: write-path centralization onto dedicated-thread writers
  (`MetadataInsertWriter` coalesces document inserts; `ContentIndexWriter` moves content-index
  commits off the shared io_context). The synchronous store path is now well below the embedding/KG
  enrichment floor — wall is enrichment-bound (`embedding_generation` / `kg_extraction` ≈ wall;
  `metadata_storage` ≈ 0.6× wall).

| Workload | Result | Notes |
|----------|-------:|-------|
| Ingestion wall time | ~983 ms median | `ingestion_e2e.json`; suite run 1,059 ms (vs 1,086 ms June 18) |
| Ingestion throughput | ~509 docs/s | 500-document run (vs 460.4 docs/s June 18) |
| Retrieval MRR | 1.0000 | `stage_trace.jsonl` `hybrid_summary` |
| Retrieval MAP | 1.0000 | Hybrid search |
| Retrieval nDCG@10 | 1.0000 | Hybrid search |
| Retrieval Precision@10 | 1.0000 | Hybrid search |
| Retrieval Recall@10 | 0.1628 | Synthetic qrels are broad; exact grep baseline shares this recall ceiling |

Post-ingest timing signal from the suite ingestion run (1,059 ms):

| Phase | Total | Calls | Notes |
|-------|------:|------:|-------|
| `process_batch` | 549.3 ms | 128 | Full post-ingest batch processing (was 703.6 ms) |
| `commit_batch_results` | 385.9 ms | 128 | Content/FTS metadata writes (was 422.1 ms) |
| `commit_content_index` | 383.3 ms | 128 | Now on the `ContentIndexWriter` thread (was 420.2 ms) |
| `dispatch_successes` | 27.1 ms | 128 | DispatchPlan + io_context decongestion (was 194.8 ms) |
| `prepare_metadata` | 135.1 ms | 128 | Extraction-result preparation |

The retrieval stage trace contains a valid `hybrid_summary` event. The xctrace hot-zone export is usable for coarse direction (`total samples=95215000000`), with top visible samples in prune classification, SQLite VM execution, search internals, WAL checksum, and Simeon NEON dot product. Treat those samples as directional, not as a replacement for focused phase timings.

### API And Metadata

Refreshed 2026-06-19 from a clean release build (`build/release`, `--with-tests`; a stale
`-DTRACY_ENABLE` flag that blocked the benchmark compile was cleared). These microbenchmarks
exercise ContentStore / MetadataRepository paths that are largely independent of the June 2026
ingestion-pipeline centralization work; the gains versus the June 9 row mostly reflect the clean
(non-instrumented) build rather than that work.

| Benchmark | p50 Latency | p50 Throughput | Notes |
|-----------|------------:|---------------:|-------|
| `Ingestion_SmallDocument` | 0.20 ms | 5,068 ops/s | 1 KB document |
| `Ingestion_MediumDocument` | 2.75 ms | 363.6 ops/s | 100 KB document |
| `Metadata_SingleUpdate` | 6.36 ms | 15,711 ops/s | 100 updates/iteration over 1,000 docs |
| `Metadata_BulkUpdate(500)` | 2.14 ms | 234,048 ops/s | 500 metadata entries/batch |

### IPC Streaming

| Benchmark | p50 Latency | p50 Throughput | Notes |
|-----------|------------:|---------------:|-------|
| `StreamingFramer_32x10_256B` | 0.48 ms | 22,857 ops/s | 10 chunks, 32 results/chunk |
| `StreamingFramer_64x6_512B` | 1.01 ms | 6,908 ops/s | 6 chunks, 64 results/chunk |
| `UnaryFramer_Success_8KB` | < 0.01 ms | 233,046 ops/s | 8 KB payload |

### Tree Builder

| Files | Latency | Throughput | Notes |
|-------|--------:|-----------:|-------|
| 100 | 28.5 ms | 3,505/s | 512-byte files |
| 500 | 98.1 ms | 5,098/s | 512-byte files |
| 1,000 | 200.2 ms | 4,994/s | 512-byte files |
| 5,000 | 955.6 ms | 5,232/s | 512-byte files |
| 10,000 | 1,935.1 ms | 5,167/s | linear scaling still holds |

### WriteCoordinator

| Phase | Elapsed | Metadata | Relationships | Nodes | Edges | Max Apply |
|-------|--------:|---------:|--------------:|------:|------:|----------:|
| Cold ingest, 100 files | 48.3 ms | 46 | 0 | 100 | 50 | 9 ms |
| Version churn, 3 iter | 8.7 ms | 1108 | 79 | 1400 | 700 | 4706 ms |
| Final totals | - | 1833 | 96 | 1600 | 800 | 4706 ms |

## Historical Comparisons

### Headline API And IPC Throughput

| Benchmark | Jan 2026 | Apr 30 | May 9 | June 9 Local | June 19 (clean) | June 9 -> June 19 |
|-----------|---------:|-------:|------:|-------------:|----------------:|------------------:|
| `Ingestion_SmallDocument` | 2,821 ops/s | 4,896 ops/s | 3,550 ops/s | 3,163 ops/s | 5,068 ops/s | +60.2% |
| `Ingestion_MediumDocument` | 57 ops/s | 336 ops/s | 129 ops/s | 254.5 ops/s | 363.6 ops/s | +42.9% |
| `Metadata_SingleUpdate` | 13,966 ops/s | 14,022 ops/s | 12,101 ops/s | 6,723 ops/s | 15,711 ops/s | +133.7% |
| `Metadata_BulkUpdate(500)` | 51,341 ops/s | 196,852 ops/s | 102,742 ops/s | 161,429 ops/s | 234,048 ops/s | +45.0% |
| `IPC StreamingFramer_32x10` | 3,732 ops/s | 20,976 ops/s | 5,837 ops/s | 17,739 ops/s | 22,857 ops/s | +28.9% |
| `IPC UnaryFramer_8KB` | 10,088 ops/s | 221,453 ops/s | 15,889 ops/s | 183,908 ops/s | 233,046 ops/s | +26.7% |

The June 9 row was an instrumented/slower local run (the June 19 column is a clean release rebuild on
the same host), so most of the June 9 → June 19 gain is the build state, not a specific code change.

### Previous Debug Refresh (M4, 2026-04-08)

| Benchmark | Throughput | Delta vs Apr 7 | Notes |
|-----------|-----------:|---------------:|-------|
| `Ingestion_SmallDocument` | 3,378 ops/s | +15.7% | 1 KB document |
| `Ingestion_MediumDocument` | 106 ops/s | +8.2% | 100 KB document |
| `Metadata_SingleUpdate` | 12,038 ops/s | +26.4% | 1,000 docs |
| `Metadata_BulkUpdate(500)` | 150,875 ops/s | +8.7% | 500 updates/batch |
| `StreamingFramer_32x10_256B` | 4,680 ops/s | +4.9% | IPC streaming |
| `StreamingFramer_64x6_512B` | 1,780 ops/s | +5.6% | IPC streaming |
| `UnaryFramer_Success_8KB` | 13,158 ops/s | +9.5% | IPC unary |

### Previous Release Refresh (M3, 2026-02-12)

| Benchmark | Latency | Throughput |
|-----------|--------:|-----------:|
| `Ingestion_SmallDocument` | 0.23 ms | 4,329 ops/s |
| `Ingestion_MediumDocument` | 3.26 ms | 307 ops/s |
| `Metadata_SingleUpdate` | 6.57 ms | 15,232 ops/s |
| `Metadata_BulkUpdate` | 2.75 ms | 181,818 ops/s |
| `StreamingFramer_32x10_256B` | 0.66 ms | 16,579 ops/s |
| `StreamingFramer_64x6_512B` | 1.27 ms | 5,531 ops/s |
| `UnaryFramer_Success_8KB` | 0.02 ms | 50,000 ops/s |

### Previous Multi-Client Results

| Test | Clients | Throughput | Add p50 | Add p95 | Notes |
|------|--------:|-----------:|--------:|--------:|-------|
| Baseline single client | 1 | 83.2 docs/s | 11.2 ms | 11.3 ms | Apr 8 debug |
| Concurrent pure ingest | 4 | 244.0 docs/s | 11.2 ms | 11.3 ms | Apr 8 debug |
| Mixed read/write | 16 | 200.2 ops/s | 15.9 ms | 82.6 ms | 0 failures |
| Mixed read/write clean tier | 68 | 278.8 ops/s | 10.9 ms | 502.3 ms | 17/17/17/17 layout |
| Connection contention | 32 burst | 1,524.8 ops/s | 11.5 ms | 38.6 ms | 0 retry-after responses |

### Previous Scaling Validation

| Clients | Aggregate Throughput | Per-client Throughput | Efficiency | Memory | Failures |
|---------|---------------------:|----------------------:|-----------:|-------:|---------:|
| 1 | 47.3 docs/s | 47.4 docs/s | 100.0% | 80.8 MB | 0 |
| 2 | 95.3 docs/s | 47.7 docs/s | 100.7% | 83.1 MB | 0 |
| 4 | 192.3 docs/s | 48.1 docs/s | 101.6% | 87.2 MB | 0 |
| 8 | 380.1 docs/s | 47.6 docs/s | 100.4% | 88.8 MB | 0 |
| 16 | 762.1 docs/s | 47.8 docs/s | 100.7% | 91.3 MB | 0 |
| 32 | 1,468.3 docs/s | 46.1 docs/s | 97.0% | 97.3 MB | 0 |
| 64 | 2,937.3 docs/s | 47.5 docs/s | 97.0% | 107.5 MB | 0 |
| 80 | 3,703.1 docs/s | 48.6 docs/s | 97.8% | 115.3 MB | 0 |

## Core Microbenchmarks

| Benchmark | p50 Latency | p50 Throughput | Notes |
|-----------|------------:|---------------:|-------|
| `Hashing_SHA256_1KB` | < 0.01 ms | 1,777,778 ops/s | tiny input overhead dominates |
| `Hashing_SHA256_1MB` | 0.30 ms | 3,286 ops/s | about 3.29 GiB/s |
| `Chunking_Rabin_1MB` | 1.22 ms | 69,647 chunks/s | 94 chunks/iteration |
| `Compression_Zstd_10KB_Text_L3` | < 0.01 ms | 1,000,000 ops/s | 71-byte output |
| `Compression_Zstd_1MB_Text_L9` | 0.21 ms | 4,742 ops/s | 158-byte output |
| `Compression_Zstd_1MB_HighEntropy_L3` | 0.11 ms | 9,308 ops/s | not smaller than input |

### Grep Algorithmic Microbenchmarks

| Case | Current Path | Comparator | Result |
|------|-------------:|-----------:|--------|
| Short pattern, 1 MiB | BMH p50 0.63 ms | `std::find` p50 0.65 ms | BMH is faster |
| Medium pattern, 1 MiB | BMH p50 0.60 ms | `std::find` p50 0.60 ms | Tie |
| Long pattern, 1 MiB | BMH p50 0.62 ms | `std::find` p50 0.61 ms | std::find is faster |
| Newlines, 10 MiB | SIMD p50 1.06 ms | `memchr` p50 0.92 ms | memchr is faster |

## Multi-Client Benchmarks

Vectors/model loading disabled. In-process daemon harness. ASAN+coverage lane.

| Test | Clients | Throughput | Latency Snapshot | Failures |
|------|--------:|-----------:|------------------|---------:|
| Baseline single client | 1 | 426 docs/s | add p50 1.27 ms, p95 1.33 ms | 0 |
| Concurrent pure ingest | 4 | 781 docs/s | add p50 1.19 ms, p95 1.38 ms | 0 |
| Mixed read/write | 4 | 54 ops/s | add p95 2.71 ms, search p95 525.55 ms, list p95 6.30 ms | 0 |
| 16-client mixed ops | 16 | 1,089 ops/s | add p95 6.45 ms, search p95 41.13 ms, list p95 14.00 ms | 0 |
| Connection contention | 16 burst | 1,326 ops/s | op p50 5.29 ms, p95 81.13 ms | 0; retry-after 0 |

### Scaling Curve

| Clients | Aggregate Throughput | Per-client Throughput | Efficiency | Memory | Failures |
|---------|---------------------:|----------------------:|-----------:|-------:|---------:|
| 1 | 428.8 docs/s | 429.6 docs/s | 100.0% | 417.4 MB | 0 |
| 2 | 781.0 docs/s | 398.5 docs/s | 91.1% | 532.2 MB | 0 |
| 4 | 1,644.9 docs/s | 422.7 docs/s | 95.9% | 561.5 MB | 0 |
| 8 | 3,101.7 docs/s | 399.1 docs/s | 90.4% | 599.1 MB | 0 |
| 16 | 4,421.4 docs/s | 295.2 docs/s | 64.5% | 625.2 MB | 0 |
| 32 | 1,225.4 docs/s | 62.2 docs/s | 8.9% | 719.8 MB | 416 |

## KG Edge/Entity Insert Microbenchmarks

**New (2026-06-24)** — `kg_edge_insert_bench` executable. Covers the `addEdgesUnique`,
`addEdges` (no dedup), `upsertNodes`, and `WriteBatch` insert paths at varying batch
sizes. These metrics form the baseline for the KGWriteBuffer optimization (#2).

### Edge Insert Throughput

| Benchmark | Batch Size | p50 Throughput | Notes |
|-----------|-----------:|---------------:|-------|
| `AddEdgesUnique_SingleTx` | 100 | 194,367 edges/s | dedup in single transaction |
| `AddEdgesUnique_SingleTx` | 500 | 196,781 edges/s | |
| `AddEdgesUnique_SingleTx` | 1000 | 194,126 edges/s | |
| `AddEdgesUnique_OneByOne` | 100 | 14,627 edges/s | per-edge insert + dedup (13.3× slower than bulk) |
| `AddEdgesUnique_OneByOne` | 500 | 13,717 edges/s | |
| `AddEdges_NoDedup_Bulk` | 100 | 123,609 edges/s | no dedup check (upper bound) |
| `AddEdges_NoDedup_Bulk` | 500 | 121,065 edges/s | |
| `AddEdges_NoDedup_Bulk` | 1000 | 115,969 edges/s | |
| `WriteBatch_Edges` | 100 | 192,270 edges/s | explicit WriteBatch path |
| `WriteBatch_Edges` | 500 | 188,565 edges/s | |
| `WriteBatch_Edges` | 1000 | 197,334 edges/s | |

### Node Upsert Throughput

| Benchmark | Batch Size | p50 Throughput | Notes |
|-----------|-----------:|---------------:|-------|
| `UpsertNodes_Bulk` | 100 | 14,689 nodes/s | bulk transaction |
| `UpsertNodes_Bulk` | 500 | 9,643 nodes/s | |
| `UpsertNodes_Bulk` | 1000 | 11,056 nodes/s | |
| `UpsertNodes_OneByOne` | 100 | 15,357 nodes/s | individual upserts (small-batch parity with bulk) |
| `UpsertNodes_OneByOne` | 500 | 10,537 nodes/s | |

### Target for KGWriteBuffer (#2)

The one-by-one insert paths simulate the current ingest behavior where post-ingest
KG extraction yields per-document edges/entities without batching. The KGWriteBuffer
should bring one-by-one throughput close to the bulk path by accumulating writes in
memory and flushing in batch.

**Expected improvement**: 2-5x edges/sec in the one-by-one path, approaching the
`AddEdges_NoDedup_Bulk` upper bound.

---

## Simeon Lexical Rescoring Observations

Captured from the live-mirror suite retrieval stage trace. The Simeon lexical backend
(`SimeonLexicalBackend::score()`) runs per-component after FTS5 candidate retrieval.

**Observed (June 19 suite, M4 Max)**: The Simeon NEON dot product appears in xctrace
hot-zone samples, confirming SIMD acceleration is active. The `hybrid_summary` event
in the stage trace captures fusion pipeline timing but does not break out the simeon
lexical component separately from text scoring.

**Gap for hot-term cache (#3)**: No per-term `score()` latency differentiation exists
in the current baseline. The hot-term posting list cache optimization needs:

- P50/P95 `score()` latency for top-100 IDF terms ("function", "class", "return")
- P50/P95 `score()` latency for bottom-100 IDF terms (rare symbols)
- Cache hit rate under the live-mirror query workload

These will be instrumented via `SearchEngineConfig::includeComponentTiming` and
`SearchEngine::Statistics` new counters (`simeonLexicalCacheHits`,
`simeonLexicalScoreMicros`) in task #3.

---

## Storage Backends

See [Storage Backends](storage_backends.md) for local vs S3-compatible backend comparisons.
docs/benchmarks/README.md
