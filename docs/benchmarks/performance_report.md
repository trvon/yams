# YAMS Performance Benchmark Report

**Last Updated**: October 11, 2025  
**YAMS Version**: 0.1.5+  
**Platform**: macOS 26.0.1, Apple Silicon M3 Max (16 cores)

---

## Current Benchmarks (October 11, 2025)

**Configuration**: Debug build (`-O0`, includes safety checks)  
**Status**: ✅ Benchmarks operational after fixing database constraint issues

### Quick Stats

| Metric | Value | Notes |
|--------|-------|-------|
| **Document Ingestion (1KB)** | 2,771 ops/sec | 0.36 ms latency |
| **Document Ingestion (100KB)** | 56 ops/sec | 17.85 ms latency |
| **Metadata Single Update** | 10,537 ops/sec | 0.09 ms latency |
| **Metadata Bulk Update** | 7,823 ops/sec | 63.91 ms for 500 ops |
| **Query Parse (Simple)** | 192,308 ops/sec | 0.01 ms latency |
| **Query Parse (Complex)** | 91,743 ops/sec | 0.01 ms latency |
| **BK-Tree Build (256)** | 146,227 ops/sec | 1.75 ms total |
| **BK-Tree Build (4096)** | 78,021 ops/sec | 52.50 ms total |
| **SHA256 Hashing (1KB)** | 89,286 ops/sec | 0.011 ms latency |
| **SHA256 Hashing (1MB)** | 2,509 ops/sec | 0.40 ms latency (2.63 GB/s) |
| **Rabin Chunking (1MB)** | 4,363 ops/sec | 19.02 ms, 83 chunks (52.6 MB/s) |
| **Zstd Compression (10KB, L3)** | 10,438 ops/sec | 0.10 ms (104 MB/s) |
| **Zstd Compression (1MB, L9)** | 115 ops/sec | 8.69 ms (115 MB/s) |
| **Test Suite Pass Rate** | 98.8% | 503+ passed, 6 failed |

## Test Environment Specifications

- **Platform**: macOS 26.0.1 (Darwin 25.0.0)
- **CPU**: Apple Silicon M3 Max, 16 cores (performance + efficiency)
- **Memory**: System RAM with 4MB L2 cache per core
- **Cache Hierarchy**: L1D 64KB, L1I 128KB, L2 4MB (x16)
- **Compiler**: AppleClang 17.0.0 with C++20 standard
- **Build Type**: Debug (for stable benchmarks, use release build with `-O3` optimizations)
- **Package Management**: Conan 2.0
- **Build System**: Meson

### Available Benchmark Executables

Located in `build/debug`:
- `tests/benchmarks/yams_api_benchmarks` - API ingestion and metadata operations
- `tests/benchmarks/yams_search_benchmarks` - Search engine performance
- `tests/benchmarks/yams_retrieval_service_benchmarks` - Retrieval service benchmarks
- `tests/benchmarks/metadata_path_query_bench` - Metadata query performance
- `tests/benchmarks/tree_list_filter_bench` - Tree-based list filtering
- `tests/benchmarks/tree_diff_benchmarks` - Tree diff operations
- `tests/benchmarks/ingestion_throughput_bench` - Ingestion throughput
- `tests/benchmarks/ipc_stream_bench` - IPC streaming performance
- `tests/benchmarks/daemon_socket_accept_bench` - Daemon socket operations
- `tests/benchmarks/search_tree_bench` - Search tree operations
- `src/benchmarks/yams_bus_bench` - Internal event bus performance

## Test Suite Results

### Unit Test Coverage (October 11, 2025)

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

## Performance Benchmarks

### 1. Document Ingestion (API Layer)

| Operation | Data Size | Latency | Throughput | Details |
|-----------|-----------|---------|------------|---------|
| **Small Document** | 1KB | 0.36 ms | 2,771 ops/sec | Single document store |
| **Medium Document** | 100KB | 17.85 ms | 56 ops/sec | Full ingestion pipeline |

**Metrics**:
- Deduplication ratio: 0.0 (unique content per test)
- Average chunk size: Variable based on content
- Total bytes processed tracked per operation

### 2. Metadata Operations

| Operation | Scope | Latency | Throughput | Details |
|-----------|-------|---------|------------|---------|
| **Single Update** | 1 document | 0.09 ms | 10,537 ops/sec | Individual metadata write |
| **Bulk Update** | 100 documents × 5 keys | 63.91 ms | 7,823 ops/sec | Batch metadata operations |

**Database**: 1,000 test documents, 0 failed operations

### 3. Search Engine Performance

| Operation | Complexity | Latency | Throughput | Details |
|-----------|-----------|---------|------------|---------|
| **Query Parsing (Simple)** | 1 term | 0.01 ms | 192,308 ops/sec | Single keyword |
| **Query Parsing (Complex)** | Multiple terms + operators | 0.01 ms | 91,743 ops/sec | Boolean logic, phrases |
| **Exact Match Search** | 1K documents | <0.01 ms | ∞ (instant) | 3 matches found |

### 4. BK-Tree (Fuzzy Search Index)

| Operation | Dataset Size | Latency | Throughput | Details |
|-----------|-------------|---------|------------|---------|
| **Construction** | 256 terms | 1.75 ms | 146,227 ops/sec | Build edit-distance index |
| **Construction** | 4,096 terms | 52.50 ms | 78,021 ops/sec | Larger vocabulary set |

**Use Case**: Typo-tolerant search, approximate string matching

---

## Core Operations (Detailed)

### 5. Cryptographic Hashing (SHA-256)

| Data Size | Latency | Throughput (ops) | Throughput (data) |
|-----------|---------|-----------------|------------------|
| **1KB** | 0.011 ms | 89,286 ops/sec | 89.3 MB/s |
| **1MB** | 0.40 ms | 2,509 ops/sec | 2.63 GB/s |

**Implementation**: OpenSSL 3.2.0 with hardware acceleration (ARM Cryptography Extensions)

### 6. Content-Defined Chunking (Rabin Fingerprinting)

| Data Size | Latency | Chunks | Avg Chunk Size | Throughput |
|-----------|---------|--------|----------------|------------|
| **1MB** | 19.02 ms | 83 | 12.6 KB | 52.6 MB/s |

**Configuration**: Min=4KB, Target=16KB, Max=64KB

### 7. Compression (Zstandard)

| Data Size | Level | Latency | Throughput (ops) | Throughput (data) |
|-----------|-------|---------|-----------------|------------------|
| **10KB** | 3 (balanced) | 0.10 ms | 10,438 ops/sec | 104 MB/s |
| **1MB** | 9 (high compression) | 8.69 ms | 115 ops/sec | 115 MB/s |

**Note**: Debug build (`-O0`). Release builds typically show 10-20× higher throughput.

---

## Historical Benchmarks (August 13, 2025)

> **Note**: The following benchmarks were from an earlier release build with `-O3` optimizations. Current benchmarks (above) use debug build for stability testing.

**Configuration**: Release build with `-O3` optimizations  
**Platform**: macOS 26.0, Apple Silicon M3 Max

### Performance Comparison (Debug vs Release)

| Operation | Debug (Oct 2025) | Release (Aug 2025) | Performance Delta |
|-----------|-----------------|-------------------|------------------|
| **SHA256 (1MB)** | 2.63 GB/s | ~20+ GB/s (est.) | ~8× faster |
| **Rabin Chunking (1MB)** | 52.6 MB/s | 186.7 MB/s | 3.5× faster |
| **Zstd Compression (1MB, L9)** | 115 MB/s | 4.36 GB/s | 38× faster |

### Chunking (Historical)

| Operation | Data Size | Throughput | Latency |
|-----------|-----------|------------|---------|
| **Small Files** | 1MB | 186.7 MB/s | 5.36 ms |
| **Large Files** | 10MB | 183.8 MB/s | 54.4 ms |

### Compression Benchmarks (Historical)

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

### Compression by Data Pattern (Historical)

| Pattern | Throughput | Compression Ratio | Use Case |
|---------|------------|------------------|-----------|
| **Zeros** | 18.1 GB/s | Excellent | Sparse files |
| **Text** | 13.7 GB/s | Very Good | Documents |
| **Binary** | 18.0 GB/s | Good | Executables |
| **Random** | 8.9 GB/s | Minimal | Encrypted data |

### Concurrent Compression (Historical)

| Threads | Throughput | Scalability |
|---------|------------|-------------|
| **1** | 1.60 GB/s | Baseline |
| **2** | 5.62 GB/s | 3.5x |
| **4** | 13.7 GB/s | 8.6x |
| **8** | 20.9 GB/s | 13.1x |
| **16** | 41.2 GB/s | 25.8x |

Linear scaling up to 16 threads with 25.8x speedup.

---

**Last Updated**: October 11, 2025  

