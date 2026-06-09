# YAMS Performance Benchmark Report

**Generated**: 2026-06-09
**Last Updated**: 2026-06-09
**YAMS Version**: 0.16.0
**Build Configuration**: Release (`meson compile -C build/release`)
**Host**: Apple M4 Max, macOS, Clang 21, 16 cores

## Contents

- [Executive Summary](#executive-summary)
- [Latest Local Refresh](#latest-local-refresh)
- [Historical Comparisons](#historical-comparisons)
- [Core Microbenchmarks](#core-microbenchmarks)
- [Multi-Client Benchmarks](#multi-client-benchmarks)
- [Storage Backends](#storage-backends)

## Executive Summary

- Coverage: API, metadata, core, IPC, tree, WriteCoordinator, multi-client, local storage, grep, and search microbenchmarks.
- Medium-document ingest: `11.5 ops/s` p50.
- IPC streaming: below May 9 baselines in the ASAN+coverage lane.
- Multi-client ingest: clean through 16 clients; 32 clients failed with 416 add failures.
- Mixed read/write search p95: 525 ms in the 4-client case, 41 ms in the 16-client preseeded case.
- Grep microbenchmarks: literal search is now close to `std::find`; newline scan still trails `memchr` on this host.
- High-entropy Zstd L3: 132.06 ms p50 for 1 MiB.

## Latest Local Refresh

Throughput values use p50 latency where available.

### API And Metadata

| Benchmark | p50 Latency | p50 Throughput | Notes |
|-----------|------------:|---------------:|-------|
| `Ingestion_SmallDocument` | 0.32 ms | 3,163 ops/s | 1 KB document |
| `Ingestion_MediumDocument` | 3.93 ms | 254.5 ops/s | 100 KB document |
| `Metadata_SingleUpdate` | 14.87 ms | 6,723 ops/s | 100 updates/iteration over 1,000 docs |
| `Metadata_BulkUpdate(500)` | 3.10 ms | 161,429 ops/s | 500 metadata entries/batch |

### IPC Streaming

| Benchmark | p50 Latency | p50 Throughput | Notes |
|-----------|------------:|---------------:|-------|
| `StreamingFramer_32x10_256B` | 0.62 ms | 17,739 ops/s | 10 chunks, 32 results/chunk |
| `StreamingFramer_64x6_512B` | 1.29 ms | 5,416 ops/s | 6 chunks, 64 results/chunk |
| `UnaryFramer_Success_8KB` | 0.01 ms | 183,908 ops/s | 8 KB payload |

### Tree Builder

| Files | Latency | Throughput | Notes |
|-------|--------:|-----------:|-------|
| 100 | 28.0 ms | 3,569/s | 512-byte files |
| 500 | 135.9 ms | 3,676/s | 512-byte files |
| 1,000 | 233.3 ms | 4,285/s | 512-byte files |
| 5,000 | 1,178.5 ms | 4,242/s | 512-byte files |
| 10,000 | 2,322.6 ms | 4,305/s | linear scaling still holds |

### WriteCoordinator

| Phase | Elapsed | Metadata | Relationships | Nodes | Edges | Max Apply |
|-------|--------:|---------:|--------------:|------:|------:|----------:|
| Cold ingest, 100 files | 48.3 ms | 46 | 0 | 100 | 50 | 9 ms |
| Version churn, 3 iter | 8.7 ms | 1108 | 79 | 1400 | 700 | 4706 ms |
| Final totals | - | 1833 | 96 | 1600 | 800 | 4706 ms |

## Historical Comparisons

### Headline API And IPC Throughput

| Benchmark | Oct 2025 | Jan 2026 | Apr 30 | May 9 | June 9 Local | May 9 -> June 9 |
|-----------|---------:|---------:|-------:|------:|-------------:|----------------:|
| `Ingestion_SmallDocument` | 2,771 ops/s | 2,821 ops/s | 4,896 ops/s | 3,550 ops/s | 3,163 ops/s | -10.9% |
| `Ingestion_MediumDocument` | 56 ops/s | 57 ops/s | 336 ops/s | 129 ops/s | 254.5 ops/s | +97.3% |
| `Metadata_SingleUpdate` | 10,537 ops/s | 13,966 ops/s | 14,022 ops/s | 12,101 ops/s | 6,723 ops/s | -44.4% |
| `Metadata_BulkUpdate(500)` | 7,823 ops/s | 51,341 ops/s | 196,852 ops/s | 102,742 ops/s | 161,429 ops/s | +57.1% |
| `IPC StreamingFramer_32x10` | - | 3,732 ops/s | 20,976 ops/s | 5,837 ops/s | 17,739 ops/s | +203.9% |
| `IPC UnaryFramer_8KB` | - | 10,088 ops/s | 221,453 ops/s | 15,889 ops/s | 183,908 ops/s | +1057.4% |

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
| `Hashing_SHA256_1KB` | < 0.01 ms | 1,333,333 ops/s | tiny input overhead dominates |
| `Hashing_SHA256_1MB` | 0.44 ms | 2,273 ops/s | about 2.27 GiB/s |
| `Chunking_Rabin_1MB` | 1.57 ms | 59,960 chunks/s | 94 chunks/iteration |
| `Compression_Zstd_10KB_Text_L3` | < 0.01 ms | 738,279 ops/s | 71-byte output |
| `Compression_Zstd_1MB_Text_L9` | 0.26 ms | 3,801 ops/s | 158-byte output |
| `Compression_Zstd_1MB_HighEntropy_L3` | 0.13 ms | 7,444 ops/s | not smaller than input |

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

## Storage Backends

See [Storage Backends](storage_backends.md) for local vs S3-compatible backend comparisons.
