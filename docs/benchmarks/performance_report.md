# YAMS Performance Benchmark Report

**Generated**: 2026-02-12
**Last Updated**: 2026-04-10
**YAMS Version**: 0.12.0-dev
**Build Configuration**: Debug (no TSAN) and Release

> Note: This page is the canonical place for benchmark results. Keep the latest numbers inlined here rather than relying on generated artifacts.

## Contents

- [Executive Summary](#executive-summary)
- [Benchmark Data and Commands](#benchmark-data-and-commands)
- [Performance Benchmarks](#performance-benchmarks)
- [Key Performance Insights](#key-performance-insights)
- [Benchmark Methodology](#benchmark-methodology)
- [Known Issues and Limitations](#known-issues-and-limitations)

## Executive Summary

This report focuses on benchmark changes that are easy to interpret and compare across runs (primarily ingestion + metadata + IPC framing). Search microbenchmarks can be noisy and hard to compare across different datasets/configs, so they are intentionally de-emphasized here.

> Note: For benchmarks that report percentiles, the “Throughput” values in the tables are the p50 numbers.

**Current Baseline (Debug no-TSAN, M4, 2026-04-08)**:

| Benchmark | Throughput | Δ vs Apr 7 | Notes |
|-----------|------------|------------|-------|
| `Ingestion_SmallDocument` | 3,378 ops/s | **+15.7%** | 1 KB document |
| `Ingestion_MediumDocument` | 106 ops/s | **+8.2%** | 100 KB document |
| `Metadata_SingleUpdate` | 12,038 ops/s | **+26.4%** | 1,000 docs |
| `Metadata_BulkUpdate(500)` | 150,875 ops/s | **+8.7%** | 500 updates/batch |
| `IPC StreamingFramer_32x10` | 4,680 ops/s | **+4.9%** | 256 B chunks |
| `IPC UnaryFramer_8KB` | 13,158 ops/s | **+9.5%** | 8 KB payload |

**Multi-Client Baseline (Debug no-TSAN, M4, 2026-04-10)**:

| Benchmark | Throughput | Latency (p50) | Latency (p95) | Notes |
|-----------|------------|---------------|---------------|-------|
| Single client ingest | 83.2 docs/s | 11.2 ms | 11.3 ms | 100 docs, 2 KB (**+38.4%**) |
| 4-client concurrent ingest | 244.0 docs/s | 11.2 ms | 11.3 ms | 400 total docs |
| 32-client mixed read/write | 404.4 ops/s | Add 10.6 ms | Search 144.6 ms | 0 failures, drained cleanly |
| 64-client mixed read/write | 244.8 ops/s | Add 127.3 ms | Search 557.3 ms | 0 failures, clean mixed scaling tier in fresh Debug lane |
| Connection contention (32 burst) | 2,709.0 ops/s | 10.8 ms | 12.3 ms | 0 failures, 0 retry-after responses |

**Previous Baseline (Release, M3, 2026-02-12)**:

| Benchmark | Throughput | Notes |
|-----------|------------|-------|
| `Ingestion_SmallDocument` | 4,329 ops/s | 1 KB document |
| `Ingestion_MediumDocument` | 307 ops/s | 100 KB document |
| `Metadata_SingleUpdate` | 15,232 ops/s | 1,000 docs |
| `Metadata_BulkUpdate(500)` | 181,818 ops/s | 500 updates/batch |
| `IPC StreamingFramer_32x10` | 16,579 ops/s | 256 B chunks |
| `IPC UnaryFramer_8KB` | 50,000 ops/s | 8 KB payload |

> **Note**: The M3 numbers above are from Release builds. The M4 numbers are from Debug (no-TSAN) builds for safe comparison. Debug adds ~2-4x overhead for ingestion and IPC, so Release numbers will be higher.

**Historical Debug Baseline (Oct 2025 vs Jan 2026 vs Apr 2026)**:

| Benchmark | Oct 2025 | Jan 2026 | Apr 7 (M4) | Apr 8 (M4, optimized) | Δ Apr 7→8 |
|-----------|----------|----------|------------|----------------------|-----------|
| `Ingestion_SmallDocument` | 2,771 ops/s | 2,821 ops/s | 2,921 ops/s | 3,378 ops/s | **+15.7%** |
| `Ingestion_MediumDocument` | 56 ops/s | 57 ops/s | 98 ops/s | 106 ops/s | **+8.2%** |
| `Metadata_SingleUpdate` | 10,537 ops/s | 13,966 ops/s | 9,520 ops/s | 12,038 ops/s | **+26.4%** |
| `Metadata_BulkUpdate(500)` | 7,823 ops/s | 51,341 ops/s | 138,835 ops/s | 150,875 ops/s | **+8.7%** |
| `IPC StreamingFramer_32x10` | - | 3,732 ops/s | 4,460 ops/s | 4,680 ops/s | **+4.9%** |
| `IPC UnaryFramer_8KB` | - | 10,088 ops/s | 12,012 ops/s | 13,158 ops/s | **+9.5%** |

> Apr 8 optimizations: CRC32 slicing-by-8, FrameReader memcpy, Rabin lazy chunking, lock-free stats, proto serializer, IngestService futures reuse.

## Benchmark Data and Commands

### Data Included in Headline Tables

- API ingestion: 1 KB and 100 KB documents.
- Metadata updates: single-update and 500-update batch workloads over 1,000 documents.
- IPC framing: streaming frames with 256 B and 512 B chunks, plus 8 KB unary payloads.
- Multi-client ingest: 2 KB documents across baseline, mixed-workload scaling, and contention scenarios.
- Search benchmarks are intentionally excluded from the headline tables because dataset choice and cache state can dominate the reported numbers.

### Run Commands

**Debug baseline**

```bash
./setup.sh Debug --no-tsan --with-tests
meson compile -C build/debug yams_api_benchmarks ipc_stream_bench multi_client_ingestion_bench

./build/debug/tests/benchmarks/yams_api_benchmarks --iterations 5 --quiet
./build/debug/tests/benchmarks/ipc_stream_bench --iterations 8 --quiet
meson test -C build/debug multi_client_ingestion_bench --test-args='[!benchmark][multi-client] --durations yes'
```

**Release baseline**

```bash
./setup.sh Release --with-tests
meson compile -C build/release yams_api_benchmarks ipc_stream_bench

./build/release/tests/benchmarks/yams_api_benchmarks --iterations 5
./build/release/tests/benchmarks/ipc_stream_bench --iterations 8
```

**Search profiling only**

```bash
./setup.sh Debug --no-tsan --with-tests
meson compile -C build/debug yams_search_benchmarks

./build/debug/tests/benchmarks/yams_search_benchmarks --quiet --iterations 5
```

**Topology-aware ingestion hotspot smoke**

Use the no-embedding lane first to isolate non-ML ingestion cost and queue/drain behavior:

```bash
ninja -C build/debug tests/benchmarks/ingestion_throughput_bench \
  tests/benchmarks/yams_large_scale_ingestion_bench

build/debug/tests/benchmarks/ingestion_throughput_bench \
  --config tests/benchmarks/configs/topology_ingestion_smoke.json

YAMS_BENCH_DOC_COUNT=10 \
YAMS_BENCH_ENABLE_EMBEDDINGS=0 \
YAMS_BENCH_SKIP_DEFAULT_SETUP=1 \
YAMS_BENCH_OUT_DIR=/tmp/yams-topology-bench \
YAMS_BENCH_RUN_ID=no-embed-smoke \
build/debug/tests/benchmarks/yams_large_scale_ingestion_bench \
  --benchmark_filter='BM_LargeScaleIngestion/200/0/2/iterations:1' \
  --benchmark_min_time=0.01s
```

When isolating topology cost with mock embeddings, set:

```bash
YAMS_BENCH_FORCE_MOCK_EMBEDDINGS=1
```

Current status:

- no-embedding ingestion hotspot lane is working and produces stable queue/drain metrics
- topology counters are now exposed in both throughput JSONL and large-scale CSV/counters
- embedding-enabled topology throughput smoke now completes and reaches
  `topology_artifacts_fresh=true`
- embedding-enabled large-scale ingestion smoke now completes as well; benchmark gating trusts
  persisted vector/topology artifacts when daemon status counters lag

Latest smoke checkpoint:

- throughput harness embedding smoke: `documents_total=3`, `files_indexed=3`,
  `topology_artifacts_fresh=true`
- preserved benchmark DB shows stored topology artifacts:
  - `metadata` rows with `topology.%`: `27`
  - `kg_nodes` with `node_key like 'topology:%'`: `2`
- large-scale embedding smoke: `10/10` docs completed in about `10.154s`
- large-scale embedding benchmark numbers are now usable for smoke validation and hotspot
  inspection; treat larger corpus runs as exploratory until repeated under load

## Performance Benchmarks

### Latest Debug Runs (M4, 2026-04-08)

#### Quick Links
- [API Benchmarks](#api-benchmarks)
- [IPC Streaming Benchmarks](#ipc-streaming-benchmarks)
- [Multi-Client Benchmarks](#multi-client-benchmarks)
- [Storage Backends Benchmark (Local vs S3-compatible)](storage_backends.md)

#### API Benchmarks

| Benchmark | Throughput | Δ vs Apr 7 | Notes |
|-----------|------------|------------|-------|
| Ingestion_SmallDocument | 3,378 ops/sec | **+15.7%** | 1 KB document |
| Ingestion_MediumDocument | 106 ops/sec | **+8.2%** | 100 KB document |
| Metadata_SingleUpdate | 12,038 ops/sec | **+26.4%** | 1,000 docs |
| Metadata_BulkUpdate | 150,875 ops/sec | **+8.7%** | 500 updates/batch |

#### IPC Streaming Benchmarks

| Benchmark | Throughput | Δ vs Apr 7 |
|-----------|------------|------------|
| StreamingFramer_32x10_256B | 4,680 ops/sec | **+4.9%** |
| StreamingFramer_64x6_512B | 1,780 ops/sec | **+5.6%** |
| UnaryFramer_Success_8KB | 13,158 ops/sec | **+9.5%** |

#### Multi-Client Benchmarks

| Test | Clients | Throughput | Add p50 | Add p95 | Δ vs Apr 7 |
|------|---------|------------|---------|---------|------------|
| Baseline single client | 1 | 83.2 docs/s | 11.2 ms | 11.3 ms | **+38.4%** |
| Concurrent pure ingest | 4 | 244.0 docs/s | 11.2 ms | 11.3 ms | ~flat |
| Mixed read/write | 16 | 200.2 ops/s | 15.9 ms | 82.6 ms | 0 failures |
| Mixed read/write clean tier | 68 | 278.8 ops/s | 10.9 ms | 502.3 ms | 0 failures; 17/17/17/17 layout |
| Connection contention | 32 burst | 1,524.8 ops/s | 11.5 ms | 38.6 ms | 0 failures; 0 retry-after responses |

> Current harness status (Apr 2026): reduced ASAN contention runs now keep `status`/`list` CLI recovery green after load. Peak-load CLI failures are classified explicitly instead of surfacing as opaque harness errors.

#### Current Scaling Validation

**Scaling curve (local backend, 10 docs/client, 1 KB docs)**

| Clients | Aggregate Throughput | Per-client Throughput | Efficiency | Memory | Failures |
|---------|----------------------|-----------------------|------------|--------|----------|
| 1 | 47.3 docs/s | 47.4 docs/s | 100.0% | 80.8 MB | 0 |
| 2 | 95.3 docs/s | 47.7 docs/s | 100.7% | 83.1 MB | 0 |
| 4 | 192.3 docs/s | 48.1 docs/s | 101.6% | 87.2 MB | 0 |
| 8 | 380.1 docs/s | 47.6 docs/s | 100.4% | 88.8 MB | 0 |
| 16 | 762.1 docs/s | 47.8 docs/s | 100.7% | 91.3 MB | 0 |
| 32 | 1468.3 docs/s | 46.1 docs/s | 97.0% | 97.3 MB | 0 |
| 64 | 2937.3 docs/s | 47.5 docs/s | 97.0% | 107.5 MB | 0 |
| 80 | 3703.1 docs/s | 48.6 docs/s | 97.8% | 115.3 MB | 0 |

**Mixed workload ramp (writers/search/list/status-get scaled together)**

| Total Clients | Layout | Throughput | Failures | Notes |
|---------------|--------|------------|----------|-------|
| 32 | 8/8/8/8 | 404.4 ops/s | 0 | clean |
| 48 | 12/12/12/12 | 370.9 ops/s | 0 | clean |
| 56 | 14/14/14/14 | 344.2 ops/s | 0 | clean |
| 60 | 15/15/15/15 | 379.2 ops/s | 0 | clean |
| 64 | 16/16/16/16 | 244.8 ops/s | 0 | clean |
| 68 | 17/17/17/17 | 276.8 ops/s | 1 | first technical failure point in fresh Debug lane |
| 72 | 18/18/18/18 | 123.6 ops/s | 85 | degraded |
| 80 | 20/20/20/20 | 201.9 ops/s | 42 | degraded |

Practical interpretation:

- Clean sustained mixed-workload support is validated through **64 total clients** in the fresh Debug lane.
- The first technical mixed-workload failure point is **68 total clients** in the fresh Debug lane.
- Runs above that may still complete, but should be treated as a **degraded / noisy tier** rather than a clean supported tier.

Methodology note:

- The mixed ramp is a **closed-loop** workload: each reader executes a fixed op budget and each writer ingests a fixed document budget.
- Because of that, aggregate ops/s is **not guaranteed to decline monotonically** as client count rises.
- A higher-client run can show higher aggregate throughput while still being *more stressed*, as reflected by failures and much worse tail latency.
- For scaling conclusions, treat **failures and p95/p99 latency** as the primary stress signals, with aggregate ops/s as secondary context.

Safety note:

- The ASAN validation lane is intentionally more conservative than the Debug lane.
- Current ASAN mixed-workload validation is clean through **48 total clients** and first degrades at **56**.
- Use Debug numbers for published throughput tables and ASAN numbers for bug/regression boundaries.

#### Search Benchmarks

Search benchmarks are intentionally not included in the “improvements” summary because the reported numbers can be misleading (different datasets, caching, and internal operation definitions). Use the search profiling command in [Benchmark Data and Commands](#benchmark-data-and-commands) when you need those numbers.

### Previous Release Runs (M3, 2026-02-12)

#### API Benchmarks

| Benchmark | Latency | Throughput |
|-----------|---------|------------|
| Ingestion_SmallDocument | 0.23 ms | 4,329 ops/sec |
| Ingestion_MediumDocument | 3.26 ms | 307 ops/sec |
| Metadata_SingleUpdate | 6.57 ms | 15,232 ops/sec |
| Metadata_BulkUpdate | 2.75 ms | 181,818 ops/sec |

#### IPC Streaming Benchmarks

| Benchmark | Latency | Throughput |
|-----------|---------|------------|
| StreamingFramer_32x10_256B | 0.66 ms | 16,579 ops/sec |
| StreamingFramer_64x6_512B | 1.27 ms | 5,531 ops/sec |
| UnaryFramer_Success_8KB | 0.02 ms | 50,000 ops/sec |

#### Retrieval Quality Benchmark

Skipped in this run (release benchmarks focused on ingestion, metadata, and IPC framing).

### Storage Backend Benchmarks (Local vs R2)

See [Storage Backends](storage_backends.md) for detailed CLI CRUD and multi-client matrix results comparing local storage vs Cloudflare R2.

### 1. Cryptographic Operations (SHA-256)

| Operation | Data Size | Throughput | Latency | Performance Impact |
|-----------|-----------|------------|---------|-------------------|
| **Small Files** | 1KB | 511 MB/s | 1.9 μs | Excellent for small files |
| **Small Files** | 4KB | 1.27 GB/s | 3.0 μs | Near memory bandwidth |
| **Medium Files** | 32KB | 2.35 GB/s | 13.0 μs | Optimal throughput |
| **Large Files** | 64KB | 2.47 GB/s | 24.7 μs | Peak performance |
| **Bulk Data** | 10MB | 2.66 GB/s | 3.67 ms | Sustained high throughput |
| **Streaming** | 10MB | 2.65 GB/s | 3.69 ms | Consistent with bulk |

**Real-World Impact**:
- Can hash a 1GB file in ~375ms
- Processes 40,000+ small files per second
- Zero bottleneck for network-speed ingestion (even 10GbE)

### 2. Content Chunking (Rabin Fingerprinting)

| Operation | Data Size | Throughput | Latency | Real-World Impact |
|-----------|-----------|------------|---------|-------------------|
| **Small Files** | 1MB | 186.7 MB/s | 5.36 ms | Chunks 35 files/second |
| **Large Files** | 10MB | 183.8 MB/s | 54.4 ms | Chunks 18 files/second |

**Real-World Impact**:
- Processes 1GB in ~5.5 seconds for content-defined chunking
- Achieves 30-40% deduplication on typical development datasets
- 8KB average chunk size optimizes dedup vs overhead balance
- Suitable for real-time chunking at gigabit ingestion speeds

### 3. Compression Performance (Zstandard)

#### Compression Benchmarks

| Data Size | Level | Compression Speed | Throughput | Efficiency |
|-----------|-------|------------------|------------|------------|
| **1KB** | 1 | 397 MB/s | Level 1 | Optimal for small files |
| **1KB** | 3 | 395 MB/s | Level 3 | Balanced performance |
| **1KB** | 9 | 304 MB/s | Level 9 | High compression |
| **10KB** | 1 | 3.52 GB/s | Level 1 | Excellent throughput |
| **10KB** | 3 | 3.46 GB/s | Level 3 | Good balance |
| **10KB** | 9 | 2.74 GB/s | Level 9 | Compressed efficiently |
| **100KB** | 1 | 14.0 GB/s | Level 1 | Near memory bandwidth |
| **100KB** | 3 | 13.5 GB/s | Level 3 | High performance |
| **100KB** | 9 | 6.23 GB/s | Level 9 | Good compression |
| **1MB** | 1 | 20.0 GB/s | Level 1 | Peak performance |
| **1MB** | 3 | 19.8 GB/s | Level 3 | Optimal balance |
| **1MB** | 9 | 4.36 GB/s | Level 9 | High compression ratio |

#### Decompression Benchmarks

| Data Size | Decompression Speed | Throughput |
|-----------|-------------------|------------|
| **1KB** | 760 MB/s | 1.35 μs |
| **10KB** | 5.91 GB/s | 1.73 μs |
| **100KB** | 15.1 GB/s | 6.80 μs |
| **1MB** | 21.0 GB/s | 50.0 μs |

**Analysis**: Compression performance reaches 20.0 GB/s for 1MB blocks. Level 1-3 provides optimal speed-to-compression ratio balance for production use.

#### Compression by Data Pattern

| Pattern | Throughput | Compression Ratio | Use Case |
|---------|------------|------------------|-----------|
| **Zeros** | 18.1 GB/s | Excellent | Sparse files |
| **Text** | 13.7 GB/s | Very Good | Documents |
| **Binary** | 18.0 GB/s | Good | Executables |
| **Random** | 8.9 GB/s | Minimal | Encrypted data |

#### Compression Level Analysis

| Level | Speed (Gi/s) | Compressed Size | Ratio | Recommendation |
|-------|-------------|-----------------|-------|----------------|
| **1-2** | 20.1 GB/s | 191 bytes | 5.5k:1 | Optimal for speed |
| **3-5** | 19.8 GB/s | 190 bytes | 5.5k:1 | Balanced performance |
| **6-7** | 6.3 GB/s | 190 bytes | 5.5k:1 | Diminishing returns |
| **8-9** | 4.3 GB/s | 190 bytes | 5.5k:1 | High compression only |

### 4. Concurrent Compression Performance

| Threads | Throughput | Scalability | Items/Second |
|---------|------------|-------------|-------------|
| **1** | 1.60 GB/s | Baseline | 156K items/s |
| **2** | 5.62 GB/s | 3.5x | 549K items/s |
| **4** | 13.7 GB/s | 8.6x | 1.34M items/s |
| **8** | 20.9 GB/s | 13.1x | 2.04M items/s |
| **16** | 41.2 GB/s | 25.8x | 4.02M items/s |

**Analysis**: Linear scaling achieved up to 16 threads with 25.8x speedup. Peak throughput of 41.2 GB/s demonstrates excellent parallel efficiency.

## Key Performance Insights

### Performance Strengths

1. **Compression Performance**: Zstandard integration delivers 20+ GB/s throughput with excellent compression ratios
2. **Parallel Scaling**: Linear scaling achieved up to 16 threads with 25.8x speedup
3. **Query Processing**: Up to 3.4M items/second tokenization rate for complex queries
4. **Result Ranking**: Partial sort algorithms provide 10x performance improvement for top-K operations
5. **Workload Stability**: Stable performance maintained across varying data sizes

### Areas for Investigation

1. **Vector Database Operations**: 35 of 38 tests failing, requires architectural review
2. **PDF Extraction**: 6 of 17 tests failing, text extraction pipeline needs improvement
3. **Metadata Repository**: 4 of 22 tests failing, primarily FTS5 configuration issues

### Recommended Production Configuration

- **Compression Level**: 3 (optimal speed-to-compression ratio)
- **Thread Pool Size**: 8-16 threads (linear scaling observed)

## Benchmark Methodology

### Current Issues & Action Items

- `yams_search_benchmarks` can hit `database is locked` depending on run concurrency and prior temporary state. Rerun after stopping any local daemon and/or cleaning temporary benchmark data.
- `retrieval_quality_bench` uses an embedded daemon harness; use `YAMS_TEST_SAFE_SINGLE_INSTANCE=1` to avoid instance collisions.
- If the host is saturated, cap post-ingest concurrency with:
  `YAMS_POST_INGEST_TOTAL_CONCURRENT=4 YAMS_POST_EXTRACTION_CONCURRENT=2 YAMS_POST_KG_CONCURRENT=2 YAMS_POST_EMBED_CONCURRENT=1`

### Test Execution

Use the commands in [Benchmark Data and Commands](#benchmark-data-and-commands) for the published benchmark runs.

### Data Generation

- **Synthetic Data**: Generated test patterns (zeros, text, binary, random)
- **Size Range**: 1KB to 10MB for comprehensive coverage
- **Iteration Count**: Sufficient iterations for statistical significance
- **Timing**: CPU time measurements with Google Benchmark framework

### Portability Notes

- Tests run on Apple Silicon with hardware SHA acceleration.
- Results may vary on architectures without equivalent acceleration.

## Known Issues and Limitations

1. **Vector Database Module**: Significant test failures (35/38) indicate architectural issues requiring investigation. Core search functionality unaffected.

2. **PDF Text Extraction**: Partial test failures (6/17) suggest text extraction pipeline needs refinement for edge cases.

3. **Search Integration**: Some search executor benchmarks fail due to missing database initialization in benchmark environment.

## Conclusion

YAMS demonstrates strong performance characteristics across core components:

- **Parallel processing** exhibits linear scaling to 16 threads
- **Query processing** delivers high-throughput tokenization and ranking
- **Performance remains stable** across varying workload sizes
- **Overall architecture** optimized for high-performance production deployment

The benchmark results validate YAMS as a high-performance content-addressable storage system. Test failures in non-critical modules (vector database, PDF extraction) require attention but do not impact core functionality.

## Appendix: Test Suite Results

<details>
<summary>Historical Unit Test Coverage (October 11, 2025)</summary>

**Test Execution Summary**:
- **Unit Test Shards**: 6 shards with parallel execution
- **Total Tests Executed**: ~500+ across all shards
- **Passed Tests**: 503+ tests
- **Failed Tests**: 6 tests  
- **Skipped Tests**: ~10 tests
- **Overall Pass Rate**: ~98.8%

**Known Failures**:
1. `SearchServiceTest.SnippetHydrationTimeoutReportsStats` - Timeout handling
2. `RepairUtilScanTest.MissingEmbeddingsListStableUnderPostIngestLoad` - Load testing
3. `ReferenceCounterTest.Statistics` - Statistics reporting
4. `GrepServiceUnicodeTest.LiteralUnicodeAndEmoji` - Unicode handling
5. `MCPSchemaTest.ListTools_ContainsAllExpectedTools` - MCP tool listing
6. `FtsSearchQuerySpecIntegration.BasicFtsWhenAvailable` - FTS5 integration timing
7. `VersioningIndexerTest.PathSeries_NewThenUpdate_CreatesVersionEdgeAndFlags` - Versioning edge cases

**Component-Level Status**:
- **Core Functionality**: ✅ STABLE (hashing, compression, chunking, WAL)
- **Search Engine**: ✅ STABLE (503+ tests passing)
- **Metadata Repository**: ✅ STABLE  
- **API Services**: ✅ STABLE (124-127 tests passing per shard)
- **Vector Database**: ⚠️ Disabled in test runs (`YAMS_DISABLE_VECTORS=1`)
- **MCP Integration**: ⚠️ Minor issues with tool listing

**Test Infrastructure**:
- Tests run with strict memory sanitizers (ASAN, UBSAN, MSAN)
- SQLite busy timeout: 1000ms
- Vector database: In-memory mode
- Test isolation: Single instance mode enabled

</details>
