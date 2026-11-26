# Benchmarks

What YAMS achieves with its **default settings**, and what its indexing, storage,
and background work cost. Each table states when, how, and on which host it was
measured; numbers from different hosts are kept in separate columns or notes.

## Retrieval quality

**Measured 2026-11-25** · `yams 0.23.1` · default hybrid search (FTS5 + Simeon
PQ-ADC embeddings + graph, topology assist on) · top 10 · Apple M4 Max (16-core,
128 GB), macOS, Release build (`libsql` backend).

| Corpus | Mode | Documents | Queries | nDCG@10 | Recall@10 | MRR | MAP |
|---|---|---:|---:|---:|---:|---:|---:|
| **BEIR scifact** | **Hybrid (default)** | 5,183 | 300 | **66.3** | **78.8** | **62.9** | **61.7** |
| BEIR scifact | Keyword (FTS5 only) | 5,183 | 300 | 64.6 | 77.4 | 60.8 | 60.1 |
| **BEIR nfcorpus** | **Hybrid (default)** | 3,633 | 323 | **31.6** | **15.5** | **52.1** | **12.3** |
| BEIR nfcorpus | Keyword (FTS5 only) | 3,633 | 323 | 30.8 | 14.3 | 51.1 | 11.4 |
| BEIR nfcorpus | Grep (literal exact) | 3,633 | 323 | 12.8 | 6.2 | 20.3 | 5.4 |

Hybrid search beats FTS5 keyword-only on both corpora (**+2.05 MRR** on scifact,
**+1.04 MRR** on nfcorpus). `nfcorpus` recall@10 is structurally low for every
system because its queries average many relevant documents each, so ten results
cannot hold most of them.

### Historical retrieval comparison

| Run | Host / Config | scifact nDCG@10 | scifact Recall@10 | nfcorpus nDCG@10 | nfcorpus Recall@10 |
|---|---|---:|---:|---:|---:|
| **2026-11-25** | Apple M4 Max, macOS, `yams 0.23.1` (`simeon_pq_adc`) | **66.3** | **78.8** | **31.6** | **15.5** |
| 2026-11-12 | Ryzen AI Max+ PRO 395, Linux (3-repeat mean) | 67.3 | 79.1 | 31.4 | 15.0 |
| 2026-07-09 | Local macOS, pre-Simeon default, topology off (50 queries) | 63.4 | 71.3 | — | — |

On macOS, switching to Simeon + topology-assisted hybrid retrieval lifted
scifact by **+2.9 nDCG@10** and **+7.5 Recall@10** over the July 2026 snapshot.

Two optional routing modes were also measured on 2026-11-12 and left off by
default:

| Option (scifact, 2026-11-12 Linux) | nDCG@10 | Recall@10 | Verdict |
|---|---:|---:|---|
| Vector search restricted to routed topology clusters | 68.3 | 80.6 | Better on scifact, mixed on nfcorpus (+0.2 nDCG, −0.3 MRR); not promoted |
| Learned cluster routing (fitted route vectors) | 68.3 | 80.5 | No end-to-end gain over centroid routing; off by default |

## Core storage, compression & chunking (vs. git baseline)

**Measured 2026-11-25** · `yams_core_benchmarks`, Apple M4 Max, macOS, Release
build · compared against the tracked baseline in
`tests/benchmarks/baseline/core_benchmarks.baseline.json` (2025-10).

| Benchmark | Git baseline (2025-10) | Current (2026-11-25) | Throughput | Delta |
|---|---:|---:|---:|---:|
| `Chunking_Rabin_1MB` | 19.02 ms | **1.05 ms** (p50 1.04 ms) | 52,472 ops/s | **18.1× faster** |
| `Compression_Zstd_1MB_Text_L9` | 8.69 ms | **0.28 ms** (p50 0.28 ms) | 3,546 ops/s | **31.0× faster** |
| `Compression_Zstd_10KB_Text_L3` | 0.096 ms | **< 0.01 ms** | 668.6k ops/s | **> 10× faster** |
| `Compression_Zstd_1MB_HighEntropy_L3` | — | **0.14 ms** (1.00× skip) | 7,045 ops/s | Clean bailout |
| `Hashing_SHA256_1MB` | 0.40 ms | **0.44 ms** (p50 0.42 ms) | 2,289 ops/s (~2.3 GB/s) | Parity |
| `Hashing_SHA256_1KB` | 0.011 ms | **< 0.01 ms** | 919.5k ops/s | Faster |

## Indexing, lexical scoring & grep microbenchmarks

**Measured 2026-11-25** · Apple M4 Max, macOS, Release build (`fts5_indexing_bench`,
`simeon_lexical_score_bench`, `yams_search_benchmarks`, `yams_grep_benchmarks`).

| Operation | Workload | Latency | Throughput |
|---|---|---:|---:|
| FTS5 batch insert + index | 128 documents | 3.13 ms | 40.9k docs/s (8.12 MiB/s) |
| FTS5 single-document index | 1 document | 47.2 µs | 4.18 MiB/s |
| FTS5 keyword search | 512 documents | 9.54 µs | — |
| Simeon lexical IDF scoring | 1,000 docs / 2k vocab (common) | 3.2 µs p50 / 4.5 µs p95 | 10–11 ms index build |
| Simeon lexical IDF scoring | 1,000 docs / 2k vocab (rare) | 2.6 µs p50 / 3.5 µs p95 | 100% cache hit |
| Facet aggregation | 1,000 items | 0.03 ms (p50 0.02 ms) | 80.0k ops/s |
| Grep SIMD newline scan | 10 MB / 100K lines | 0.82 ms | 122.3M lines/s |

## Embedding throughput

**Measured 2026-04** · daemon embedding path, batches of 100 documents.

| Backend | Dimensions | Throughput | p50 batch latency |
|---|---:|---:|---:|
| Simeon (default) | 1024 | 9,032 docs/s | 10.9 ms |
| ONNX MiniLM-L6 | 384 | 463 docs/s | 230.8 ms |

Simeon needs no model download and is deterministic. It is a lexical and
topical encoder that composes well with BM25; on scifact it matches MiniLM's
quality, but that does not hold on every corpus (see the
[Simeon announcement](../newsletter.md#2026-04-25-simeon-is-now-the-default-embedding-backend)).

## Background work

**Measured 2026-11-12/13** · AMD Ryzen AI Max+ PRO 395, Linux, optimized build.

| Operation | Workload | Time | Peak memory |
|---|---|---:|---:|
| Full vector index rebuild (product quantization) | 147,712 vectors × 1024 dims, a real store | 6.8 s | 280 MB |
| Watch pass, nothing changed | 2,379-file source tree | 19 ms | — |

The rebuild streams vectors instead of loading them all; before that change it
took 6.5 s at 870 MB. The watch pass took 122 ms before its pattern matching
was reworked, and a project that is not changing is polled progressively less
often.

## Reproduce

Retrieval quality comes from `retrieval_quality_bench`, which starts an
isolated daemon, ingests a BEIR corpus, waits until it is fully indexed, then
scores every query against the dataset's relevance judgments with `trec_eval`
definitions.

```bash
# BEIR datasets are read from ~/.cache/yams/benchmarks/<name>
mkdir -p ~/.cache/yams/benchmarks && cd ~/.cache/yams/benchmarks
curl -LO https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip
unzip scifact.zip && cd -

# Needs Google Benchmark; the build skips the target without it
./setup.sh Release
meson setup --reconfigure build/release -Dbuild-tests=true -Dbuild-benchmarks=true
meson compile -C build/release \
  retrieval_quality_bench \
  yams_core_benchmarks \
  yams_search_benchmarks \
  yams_grep_benchmarks \
  simeon_lexical_score_bench \
  fts5_indexing_bench

# The guard keeps the benchmark's isolated daemon from resetting shared IO state
YAMS_TEST_SAFE_SINGLE_INSTANCE=1 YAMS_BENCH_DATASET=scifact \
  build/release/tests/benchmarks/retrieval_quality_bench
YAMS_TEST_SAFE_SINGLE_INSTANCE=1 YAMS_BENCH_DATASET=nfcorpus \
  build/release/tests/benchmarks/retrieval_quality_bench
```

Raw results land under `build/benchmarks/` (not tracked). The
[ingestion oracle](ingestion_oracle.md) defines the correctness gates for
ingest throughput work. Maintainers with the optional local `xplan` harness run
multi-arm plans through it; it is not part of a clean checkout.

??? note "Earlier snapshot (2026-07-09, local macOS)"

    Recorded before Simeon became the default and before the topology work;
    kept for reference, not comparable with the Linux tables above.

    | Workload | Result |
    |---|---|
    | Ingest, 80 × 1 KB synthetic documents | 97 docs/s (111 with vectors, graph and entity extraction off) |
    | Retrieval load, hybrid | p50 28 ms, p95 41 ms, 123 queries/s |
    | Retrieval load, keyword only | p50 1.2 ms, p95 5.5 ms, 362 queries/s |
    | scifact, 50 queries, topology off | nDCG 63.4, recall@10 71.3, MRR 61.3 |
    | Repair, 20 injected faults | full-text and graph repaired; embedding repair timed out at 120 s in that run |
