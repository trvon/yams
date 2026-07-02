# YAMS Performance Benchmark Report

**Generated**: 2026-06-09
**Last Updated**: 2026-07-02
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
- [Appendix: Notes And Raw Runs](#appendix-notes-and-raw-runs)
- [Storage Backends](#storage-backends)

## Executive Summary

- Latest headline: live-mirror steady-state ingestion drift is attributed to `090a5ca6`;
  per-object durable `StorageEngine::atomicWrite` fsyncs inflated manifest/chunk-ref writes.
- June 19 live-mirror baseline remains ~983 ms median for 500 docs (~509 docs/s),
  with retrieval MRR/MAP/nDCG@10 at 1.0.
- July 1 KG write coalescing improved the coordinator-shaped A/B by ~1.35x median and
  removed the versioning-bench apply spike (4,706 ms -> 35 ms max apply).
- Hot-term posting-list cache was rejected: Simeon score latency is corpus-size-bound, not
  term-frequency-bound.
- Core clean-release refresh: medium-document ingest 363.6 ops/s p50; IPC streaming
  `StreamingFramer_32x10` 22,857 ops/s; `UnaryFramer_8KB` 233,046 ops/s.

## Latest Local Refresh

Throughput values use p50 latency where available. Detailed notes and raw runs are in
[the appendix](#appendix-notes-and-raw-runs).

### KG Write Coalescing In WriteCoordinator (2026-07-01)

`WriteCoordinator::applyBatches` now bulk-prefetches node keys, coalesces duplicate
edges per iteration, and memoizes fallback lookups. Result: lower median wall time and
large apply-spike reduction in the daemon-shaped benchmark.

| Config / Case | Result | Key signal |
|---|---:|---|
| KG dedup off | 623 ms median | 48.1k offered edges/s |
| KG dedup on | 463 ms median | 64.8k offered edges/s |
| Version churn max apply | 35 ms | down from 4,706 ms on 2026-06-19 |

### Live-Mirror Wall-Time Drift (2026-07-01/02, attributed)

Verdict: first bad tested commit is `090a5ca6`, specifically
`StorageConfig::fsyncBeforeRename=true` and the resulting `F_FULLFSYNC`/`fsync` before
each atomic rename. The hot path is `StorageEngine::atomicWrite` under content-store
manifest and chunk-ref writes.

| Build | 500-doc result | Content-store signal |
|---|---:|---|
| `aea9b644` parent-side control | 839 ms / 595.9 docs/s | `store_total` 642 ms cumulative |
| `090a5ca6` | 3,296 ms / 151.7 docs/s | `store_total` 14,968 ms cumulative |
| `090a5ca6` + `fsyncBeforeRename=false` probe | 776 ms / 644.3 docs/s | `store_total` 472 ms cumulative |

Fix direction: preserve durable-store crash consistency, but avoid a full media flush per
tiny live-mirror object/manifest; prefer a typed fsync policy or a higher-level batched
sync boundary.

### Live-Mirror Ingestion And Retrieval (2026-06-19)

Validated artifact: `/tmp/yams_live_mirror_suite_20260619_162112`. The 500-document
steady-state suite completed in ~983 ms median with retrieval quality unchanged.

| Workload | Result |
|---|---:|
| Ingestion wall time | ~983 ms median |
| Ingestion throughput | ~509 docs/s |
| Retrieval MRR / MAP / nDCG@10 / Precision@10 | 1.0000 |
| Retrieval Recall@10 | 0.1628 |

### API And Metadata

Refreshed 2026-06-19 from a clean release build (`build/release`, `--with-tests`).

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

| Files | Latency | Throughput |
|-------|--------:|-----------:|
| 100 | 28.5 ms | 3,505/s |
| 500 | 98.1 ms | 5,098/s |
| 1,000 | 200.2 ms | 4,994/s |
| 5,000 | 955.6 ms | 5,232/s |
| 10,000 | 1,935.1 ms | 5,167/s |

### WriteCoordinator

| Phase | Elapsed | Nodes | Edges | Max Apply |
|-------|--------:|------:|------:|----------:|
| Cold ingest, 100 files | 101.9 ms | 200 | 100 | 26 ms |
| Version churn, 3 iter | ~1.4 ms/iter | 1500 total | 750 total | 35 ms |

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

### Target for KGWriteBuffer (#2) — landed 2026-07-01

The daemon-shaped bottleneck was deferred-edge resolution and duplicate-edge churn, not
the isolated one-by-one insert path. The landed WriteCoordinator coalescing measured
~1.35x median on the coordinator A/B and cut versioning-bench max apply to 35 ms. The
microbench rows above remain regression sentinels.

---

## Simeon Lexical Rescoring Observations

Verdict for hot-term posting-list cache (#3): **no-go**. `SimeonLexicalBackend::score()`
latency is flat across common vs rare terms and scales with corpus size, so caching hot
posting lists would not target the dominant cost.

| Corpus | Common p50/p95 | Rare p50/p95 | Query-cache warm p50 |
|--------|---------------:|-------------:|---------------------:|
| 1,000 docs / 2,000 vocab | 89.1 / 97.3 µs | 85.6 / 88.6 µs | 70.4 µs |
| 10,000 docs / 8,000 vocab | 864.7 / 955.6 µs | 858.0 / 901.2 µs | 701.0 µs |

Next useful lever: candidate-restricted scoring or the existing hot-query LRU cache, not
per-term posting-list caching.

## Appendix: Notes And Raw Runs

### Live-Mirror Drift Attribution

Bisect/probe workload: direct `ingestion_e2e_bench`, 500 docs, poll 10 ms, Simeon
embeddings. A HEAD control build reproduced the bad ~3.6 s class before the July 1
WriteCoordinator changes, ruling out KG coalescing/ODR cleanup. KG timings stayed
negligible.

| Build | Result | Content-store phase timings |
|---|---:|---|
| `6a7adac9` report-era good | 925 ms / 540.5 docs/s | `store_total` 448 ms; `manifest_store` 0 ms; `chunk_store_refs` 13 ms; `ref_commit` 37 ms |
| `aea9b644` parent-side control | 839 ms / 595.9 docs/s | `store_total` 642 ms; `manifest_store` 23 ms; `chunk_store_refs` 78 ms; `ref_commit` 47 ms |
| `090a5ca6` | 3,296 ms / 151.7 docs/s | `store_total` 14,968 ms; `manifest_store` 7,253 ms; `chunk_store_refs` 5,258 ms; `ref_commit` 1,946 ms |
| `090a5ca6` + `fsyncBeforeRename=false` | 776 ms / 644.3 docs/s | `store_total` 472 ms; `manifest_store` 5 ms; `chunk_store_refs` 20 ms; `ref_commit` 32 ms |

The phase timers are cumulative across concurrent calls, so ~15 s cumulative
`store_total` corresponds to a ~3.3 s wall-time run.

### KG Write Coalescing Notes

The original 2-5x expectation applied to an isolated one-by-one insert shape; the daemon
path was already transaction-amortized by WriteCoordinator. The real costs were
`getNodeByKey` N+1 lookups during deferred-edge resolution and duplicate-row
`ON CONFLICT` churn.

The KGWriteBuffer optimization (#2) landed in `WriteCoordinator::applyBatches`: bulk
node-key prefetch via `getNodesByKeys`, per-iteration edge coalescing through
`KGWriteBuffer::flushInto`, and memoized fallback lookups. Default-on; opt out via
`tuning.write_coordinator.kg_dedup_enabled = false`. `kg_dedup_max_edges` bounds the
in-memory buffer.

A/B shape: `write_coordinator_bench` `[kg-dedup]`, 1,000 deferred-edge WriteBatches x
30 edges, 60% hot-key traffic, 2,000 preseeded nodes, 5 repeats. Median speedup was
~1.35x, with clean repeats up to ~1.7x and a high-duplicate small-batch shape reaching
~2.2x. The one-by-one `kg_edge_insert_bench` rows are unchanged, as expected, because
those isolated paths were not modified.

### June 19 Live-Mirror Suite Notes

Configuration: `builddir`, synthetic live-mirror workload, 500 documents, 200
retrieval queries, top K 10, real Simeon embeddings, plugin discovery, Glint, semantic
graph/topology, and post-ingest pipeline enabled.

Post-ingest timing from the 1,059 ms suite run:

| Phase | Total | Calls |
|---|---:|---:|
| `process_batch` | 549.3 ms | 128 |
| `commit_batch_results` | 385.9 ms | 128 |
| `commit_content_index` | 383.3 ms | 128 |
| `dispatch_successes` | 27.1 ms | 128 |
| `prepare_metadata` | 135.1 ms | 128 |

### Simeon Lexical Cache Decision Notes

Instrumentation added `SearchEngine::Statistics` counters for
`simeonLexicalCacheHits`, `simeonLexicalScoreMicros`, and
`simeonLexicalScoreCalls`; component timing reports
`componentTimingMicros["simeon_lexical_score"]`.

`simeon_lexical_score_bench` used a Zipf synthetic corpus, 100 most-common terms vs
100 rarest terms, 3 calls per term. Per-term latency differed by less than 1% at p50
and scaled linearly with corpus size, indicating full-corpus score-vector cost.

The June 19 xctrace hot-zone export was useful only as coarse direction: visible samples
included prune classification, SQLite VM execution, search internals, WAL checksum, and
Simeon NEON dot product.

## Storage Backends

See [Storage Backends](storage_backends.md) for local vs S3-compatible backend comparisons.
docs/benchmarks/README.md
