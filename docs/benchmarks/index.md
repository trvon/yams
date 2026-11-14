# Benchmarks

What YAMS achieves with its **default settings**, and what its background work
costs. Each table says when and how it was measured; numbers from different
dates or hosts are not compared with each other.

## Retrieval quality

**Measured 2026-11-12** · default hybrid search (full-text + Simeon embeddings +
graph, topology assist on) · top 10 · three repeats, mean shown · AMD Ryzen AI
Max+ PRO 395, Linux, optimized build.

| Corpus | Documents | Queries | nDCG@10 | Recall@10 | MRR |
|---|---:|---:|---:|---:|---:|
| BEIR scifact | 5,183 | 300 | 67.3 | 79.1 | 64.1 |
| BEIR nfcorpus | 3,633 | 323 | 31.4 | 15.0 | 52.0 |

Repeats varied by at most 0.35 points. nfcorpus recall is low for every system:
its queries have many relevant documents each, so ten results cannot hold most
of them.

Two options were measured against this default in the same run and left off:

| Option (scifact) | nDCG@10 | Recall@10 | Verdict |
|---|---:|---:|---|
| Vector search restricted to routed topology clusters | 68.3 | 80.6 | Better on scifact, mixed on nfcorpus (+0.2 nDCG, −0.3 MRR); not promoted |
| Learned cluster routing (fitted route vectors) | 68.3 | 80.5 | No end-to-end gain over centroid routing; off by default |

Query latency was not recorded in this run.

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

**Measured 2026-11-12/13** on the host above.

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
meson configure build/release -Dbuild-tests=true -Dbuild-benchmarks=true
meson compile -C build/release retrieval_quality_bench

# The guard keeps the benchmark's isolated daemon from resetting shared IO state
YAMS_TEST_SAFE_SINGLE_INSTANCE=1 YAMS_BENCH_DATASET=scifact \
  build/release/tests/benchmarks/retrieval_quality_bench
```

Raw results land under `build/benchmarks/` (not tracked). The
[ingestion oracle](ingestion_oracle.md) defines the correctness gates for
ingest throughput work. Maintainers with the optional local `xplan` harness run
multi-arm plans through it; it is not part of a clean checkout.

??? note "Earlier snapshot (2026-07-09, local macOS)"

    Recorded before Simeon became the default and before the topology work;
    kept for reference, not comparable with the tables above.

    | Workload | Result |
    |---|---|
    | Ingest, 80 × 1 KB synthetic documents | 97 docs/s (111 with vectors, graph and entity extraction off) |
    | Retrieval load, hybrid | p50 28 ms, p95 41 ms, 123 queries/s |
    | Retrieval load, keyword only | p50 1.2 ms, p95 5.5 ms, 362 queries/s |
    | scifact, 50 queries, topology off | nDCG 63.4, recall@10 71.3, MRR 61.3 |
    | Repair, 20 injected faults | full-text and graph repaired; embedding repair timed out at 120 s in that run |
