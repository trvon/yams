# YAMS Performance Benchmark Report

**Generated**: 2026-02-12
**Last Updated**: 2026-04-08
**YAMS Version**: 0.12.0-dev
**Test Environment**: macOS 26.4 (Apple Silicon M4 Max, 16 cores, 128GB RAM)
**Build Configuration**: Debug (no TSAN) and Release

> Note: This page is the canonical place for benchmark results. Keep the latest numbers inlined here (avoid relying on generated `bench_results/*` artifacts).

## Contents

- [Executive Summary](#executive-summary)
- [Test Environment Specifications](#test-environment-specifications)
- [Performance Benchmarks](#performance-benchmarks)
  - [Latest Debug Runs (M4 Max)](#latest-debug-runs-m4-max-2026-04-08)
  - [Multi-Client Benchmarks (M4 Max)](#multi-client-benchmarks-m4-max-2026-04-08)
  - [Previous Release Runs (M3 Max)](#previous-release-runs-m3-max-2026-02-12)
  - [Storage Backend Benchmarks](#storage-backend-benchmarks-local-vs-r2)
  - [Cryptographic Operations (SHA-256)](#1-cryptographic-operations-sha-256)
  - [Content Chunking (Rabin Fingerprinting)](#2-content-chunking-rabin-fingerprinting)
  - [Compression Performance (Zstandard)](#3-compression-performance-zstandard)
  - [Concurrent Compression Performance](#4-concurrent-compression-performance)

## Executive Summary

This report focuses on benchmark changes that are easy to interpret and compare across runs (primarily ingestion + metadata + IPC framing). Search microbenchmarks can be noisy and hard to compare across different datasets/configs, so they are intentionally de-emphasized here.

> Note: For benchmarks that report percentiles, the “Throughput” values in the tables are the p50 numbers.

**Current Baseline (Debug no-TSAN, M4 Max, 2026-04-08)**:

| Benchmark | Throughput | Δ vs Apr 7 | Notes |
|-----------|------------|------------|-------|
| `Ingestion_SmallDocument` | 3,378 ops/s | **+15.7%** | 1 KB document |
| `Ingestion_MediumDocument` | 106 ops/s | **+8.2%** | 100 KB document |
| `Metadata_SingleUpdate` | 12,038 ops/s | **+26.4%** | 1,000 docs |
| `Metadata_BulkUpdate(500)` | 150,875 ops/s | **+8.7%** | 500 updates/batch |
| `IPC StreamingFramer_32x10` | 4,680 ops/s | **+4.9%** | 256 B chunks |
| `IPC UnaryFramer_8KB` | 13,158 ops/s | **+9.5%** | 8 KB payload |

**Multi-Client Baseline (Debug no-TSAN, M4 Max, 2026-04-08)**:

| Benchmark | Throughput | Latency (p50) | Latency (p95) | Notes |
|-----------|------------|---------------|---------------|-------|
| Single client ingest | 83.2 docs/s | 11.2 ms | 11.3 ms | 100 docs, 2 KB (**+38.4%**) |
| 4-client concurrent ingest | 244.0 docs/s | 11.2 ms | 11.3 ms | 400 total docs |
| Mixed read/write (4 clients) | — | — | — | SIGSEGV (pre-existing) |
| Connection contention (16 burst) | 1,368 ops/s | 11.2 ms | 12.0 ms | 0 failures (**+17.1%**) |

**Previous Baseline (Release, M3 Max, 2026-02-12)**:

| Benchmark | Throughput | Notes |
|-----------|------------|-------|
| `Ingestion_SmallDocument` | 4,329 ops/s | 1 KB document |
| `Ingestion_MediumDocument` | 307 ops/s | 100 KB document |
| `Metadata_SingleUpdate` | 15,232 ops/s | 1,000 docs |
| `Metadata_BulkUpdate(500)` | 181,818 ops/s | 500 updates/batch |
| `IPC StreamingFramer_32x10` | 16,579 ops/s | 256 B chunks |
| `IPC UnaryFramer_8KB` | 50,000 ops/s | 8 KB payload |

> **Note**: The M3 Max numbers above are from Release builds. The M4 Max numbers are from Debug (no-TSAN) builds for safe comparison — Debug adds ~2-4x overhead for ingestion and IPC. Release M4 Max numbers will be higher.

**Historical Debug Baseline (M3 Max, Jan 2026 vs Oct 2025)**:

| Benchmark | Oct 2025 | Jan 2026 | Apr 7 (M4) | Apr 8 (M4, optimized) | Δ Apr 7→8 |
|-----------|----------|----------|------------|----------------------|-----------|
| `Ingestion_SmallDocument` | 2,771 ops/s | 2,821 ops/s | 2,921 ops/s | 3,378 ops/s | **+15.7%** |
| `Ingestion_MediumDocument` | 56 ops/s | 57 ops/s | 98 ops/s | 106 ops/s | **+8.2%** |
| `Metadata_SingleUpdate` | 10,537 ops/s | 13,966 ops/s | 9,520 ops/s | 12,038 ops/s | **+26.4%** |
| `Metadata_BulkUpdate(500)` | 7,823 ops/s | 51,341 ops/s | 138,835 ops/s | 150,875 ops/s | **+8.7%** |
| `IPC StreamingFramer_32x10` | - | 3,732 ops/s | 4,460 ops/s | 4,680 ops/s | **+4.9%** |
| `IPC UnaryFramer_8KB` | - | 10,088 ops/s | 12,012 ops/s | 13,158 ops/s | **+9.5%** |

> Apr 8 optimizations: CRC32 slicing-by-8, FrameReader memcpy, Rabin lazy chunking, lock-free stats, proto serializer, IngestService futures reuse.

## Test Environment Specifications

### Current (M4 Max)

- **Platform**: macOS 26.4 (Build 25E246)
- **Hardware**: MacBook Pro (Apple M4 Max)
- **CPU**: Apple M4 Max, 16 cores
- **Memory**: 128 GB unified memory
- **Compiler**: Apple Clang 21.0.0 (clang-2100.0.123.102) with C++23 standard
- **Build Type**: Debug (no-TSAN) for safe baseline; Release for production numbers
- **Package Management**: Conan 2.0
- **Build System**: Meson

### Previous (M3 Max)

- **Platform**: macOS 26.2 (Build 25C56)
- **Hardware**: MacBook Pro (Mac15,9)
- **CPU**: Apple M3 Max, 16 cores (12 performance + 4 efficiency)
- **Memory**: 48 GB unified memory
- **Cache Hierarchy**: L1D 64KB, L1I 128KB, L2 4MB (x16)
- **Compiler**: Apple Clang 17.0.0 (clang-1700.6.3.2) with C++23 standard
- **Build Type**: Release
- **Package Management**: Conan 2.0
- **Build System**: Meson

> **Note**: Use Release builds for public-facing numbers. Debug + TSAN can add ~2-6x overhead.

### Available Benchmark Executables

Located in `build/release/tests/benchmarks/` (when the Release build is configured with `-Dbuild-tests=true`):
- `yams_api_benchmarks` - API ingestion and metadata operations (writes `bench_results/api_*`)
- `yams_search_benchmarks` - Search + query parsing (writes `bench_results/search_*`)
- `ipc_stream_bench` - IPC streaming performance (writes `bench_results/ipc_stream_*`)
- `retrieval_quality_bench` - Retrieval-quality evaluation (stdout metrics; uses embedded daemon harness)
- `yams_retrieval_service_benchmarks` - Retrieval service benchmarks
- `metadata_path_query_bench` - Metadata query performance
- `tree_list_filter_bench` - Tree-based list filtering
- `tree_diff_benchmarks` - Tree diff operations
- `ingestion_throughput_bench` - Ingestion throughput
- `daemon_socket_accept_bench` - Daemon socket operations (GTest runner)

## Performance Benchmarks

### Latest Debug Runs (M4 Max, 2026-04-08)

#### Quick Links
- [API Benchmarks](#api-benchmarks-m4)
- [IPC Streaming Benchmarks](#ipc-streaming-benchmarks-m4)
- [Multi-Client Benchmarks](#multi-client-benchmarks)
- [Storage Backends Benchmark (Local vs S3-compatible)](storage_backends.md)

#### Run Commands
```bash
# Debug build (no TSAN) for benchmarks
./setup.sh Debug --no-tsan --with-tests
meson compile -C builddir

# API
./builddir/tests/benchmarks/yams_api_benchmarks --iterations 5 --quiet

# IPC streaming
./builddir/tests/benchmarks/ipc_stream_bench --iterations 8 --quiet

# Multi-client
./builddir/tests/benchmarks/multi_client_ingestion_bench “[!benchmark][multi-client]” --durations yes
```

#### API Benchmarks (M4)

(From `builddir/tests/benchmarks/yams_api_benchmarks`, `--iterations 5`, Debug no-TSAN)

| Benchmark | Throughput | Δ vs Apr 7 | Notes |
|-----------|------------|------------|-------|
| Ingestion_SmallDocument | 3,378 ops/sec | **+15.7%** | 1 KB document |
| Ingestion_MediumDocument | 106 ops/sec | **+8.2%** | 100 KB document |
| Metadata_SingleUpdate | 12,038 ops/sec | **+26.4%** | 1,000 docs |
| Metadata_BulkUpdate | 150,875 ops/sec | **+8.7%** | 500 updates/batch |

#### IPC Streaming Benchmarks (M4)

(From `builddir/tests/benchmarks/ipc_stream_bench`, `--iterations 8`, Debug no-TSAN)

| Benchmark | Throughput | Δ vs Apr 7 |
|-----------|------------|------------|
| StreamingFramer_32x10_256B | 4,680 ops/sec | **+4.9%** |
| StreamingFramer_64x6_512B | 1,780 ops/sec | **+5.6%** |
| UnaryFramer_Success_8KB | 13,158 ops/sec | **+9.5%** |

#### Multi-Client Benchmarks (M4)

(From `builddir/tests/benchmarks/multi_client_ingestion_bench`, Debug no-TSAN, 2 KB docs)

| Test | Clients | Throughput | Add p50 | Add p95 | Δ vs Apr 7 |
|------|---------|------------|---------|---------|------------|
| Baseline single client | 1 | 83.2 docs/s | 11.2 ms | 11.3 ms | **+38.4%** |
| Concurrent pure ingest | 4 | 244.0 docs/s | 11.2 ms | 11.3 ms | ~flat |
| Mixed read/write | 4 | — | — | — | no longer blocked by the old concurrent-ingest SIGSEGV; rerun canonical baseline before publishing replacement numbers |
| Connection contention | 16 | 1,368 ops/s | 11.2 ms | 12.0 ms | **+17.1%**; current harness also requires post-stress CLI recovery and classifies peak-load CLI timeouts as `timeout_under_load` |

> Current harness status (Apr 2026): reduced ASAN contention runs now keep `status`/`list` CLI recovery green after load. Peak-load CLI failures are no longer opaque harness errors; they are classified explicitly, with `timeout_under_load` the only accepted failure mode during saturation.

#### Search Benchmarks

Search benchmarks are intentionally not included in the “improvements” summary because the reported numbers can be misleading (different datasets, caching, and internal operation definitions). If you need them for profiling, run the benchmark binary and inspect its output locally:

```bash
./builddir/tests/benchmarks/yams_search_benchmarks --quiet --iterations 5
```

### Previous Release Runs (M3 Max, 2026-02-12)

#### API Benchmarks (M3, Release)

| Benchmark | Latency | Throughput |
|-----------|---------|------------|
| Ingestion_SmallDocument | 0.23 ms | 4,329 ops/sec |
| Ingestion_MediumDocument | 3.26 ms | 307 ops/sec |
| Metadata_SingleUpdate | 6.57 ms | 15,232 ops/sec |
| Metadata_BulkUpdate | 2.75 ms | 181,818 ops/sec |

#### IPC Streaming Benchmarks (M3, Release)

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
5. **Memory Efficiency**: Stable performance maintained across varying data sizes

### Areas for Investigation

1. **Vector Database Operations**: 35 of 38 tests failing, requires architectural review
2. **PDF Extraction**: 6 of 17 tests failing, text extraction pipeline needs improvement
3. **Metadata Repository**: 4 of 22 tests failing, primarily FTS5 configuration issues

### Recommended Production Configuration

- **Compression Level**: 3 (optimal speed-to-compression ratio)
- **Thread Pool Size**: 8-16 threads (linear scaling observed)
- **Memory Allocation**: Match L2 cache size (4MB per core)

## Benchmark Methodology

### Current Issues & Action Items

- `yams_search_benchmarks` can hit `database is locked` depending on run concurrency and prior temporary state. Rerun after stopping any local daemon and/or cleaning temporary benchmark data.
- `retrieval_quality_bench` uses an embedded daemon harness; use `YAMS_TEST_SAFE_SINGLE_INSTANCE=1` to avoid instance collisions.
- If the host is saturated, cap post-ingest concurrency with:
  `YAMS_POST_INGEST_TOTAL_CONCURRENT=4 YAMS_POST_EXTRACTION_CONCURRENT=2 YAMS_POST_KG_CONCURRENT=2 YAMS_POST_EMBED_CONCURRENT=1`

### Test Execution

**For Release Build Benchmarks** (recommended):
```bash
# Configure and build release version (with tests enabled so benchmark executables are built)
./setup.sh Release --with-tests
meson compile -C build/release

# Run benchmarks
./build/release/tests/benchmarks/yams_api_benchmarks --iterations 5
./build/release/tests/benchmarks/ipc_stream_bench --iterations 8
```

**For Debug Build** (current):
```bash
cd build/debug
./tests/benchmarks/yams_api_benchmarks
./tests/benchmarks/yams_search_benchmarks
```

**Unit Tests** (for test pass rate statistics):
```bash
cd build/debug
meson test --suite unit --print-errorlogs
```

### Data Generation

- **Synthetic Data**: Generated test patterns (zeros, text, binary, random)
- **Size Range**: 1KB to 10MB for comprehensive coverage
- **Iteration Count**: Sufficient iterations for statistical significance
- **Timing**: CPU time measurements with Google Benchmark framework

### Hardware Considerations

- Tests run on Apple Silicon with hardware SHA acceleration
- Results may vary on different architectures (x86_64, ARM64 without acceleration)
- Memory bandwidth and cache performance significantly impact results

## Known Issues and Limitations

1. **Vector Database Module**: Significant test failures (35/38) indicate architectural issues requiring investigation. Core search functionality unaffected.

2. **PDF Text Extraction**: Partial test failures (6/17) suggest text extraction pipeline needs refinement for edge cases.

3. **Search Integration**: Some search executor benchmarks fail due to missing database initialization in benchmark environment.

## Conclusion

YAMS demonstrates strong performance characteristics across core components:

- **Parallel processing** exhibits linear scaling to 16 threads
- **Query processing** delivers high-throughput tokenization and ranking
- **Memory efficiency** maintained across varying workload sizes
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
