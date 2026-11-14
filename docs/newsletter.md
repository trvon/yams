# Newsletter

Product updates and occasional deep-dives.

## Subscribe

- SourceHut list: https://lists.sr.ht/~trvon/newsletter
- RSS (GitHub releases): https://github.com/trvon/yams/releases.atom

## What you'll get

- Release notes and breaking changes
- Benchmark updates
- Design notes (search, storage, plugins)

## 2026-11: Watching, honest grep and measured routing

A week of work on how YAMS keeps up with a project and how it answers.

- **`yams watch` works for every project.** Watching used to follow only the
  current session, so enabling it in a second project quietly stopped the first.
  Every watch-enabled session is now followed. The watcher remembers what it has
  seen across restarts, reads every `.gitignore` in the tree, waits ten minutes
  before removing a file that disappeared (so a branch switch does not churn the
  index), backs off when nothing changes, and pauses under memory or CPU
  pressure. A no-change pass over a 2,400-file repository went from 122 ms to
  19 ms.
- **Grep says when it is incomplete.** `yams grep -l` and `-c` returned nothing
  for patterns the plain mode found; they now list files and count lines. A scan
  cut short by its time budget now says so instead of reading as "no matches".
- **`yams list` time filters work.** `--since`, `--indexed-after` and the other
  date filters were accepted and ignored; they now select documents.
- **`yams get --json`** returns a document's complete content together with its
  hash, metadata and timestamps, for scripts and agents that need provenance.
- **`yams init` keeps your config.** Initializing a second data directory no
  longer replaces the existing configuration.
- **Learned routing, measured.** We tried the learned cluster router from
  Apple's [faster retrieval](https://machinelearning.apple.com/research/faster-retrieval)
  work. It improves how often the right topology cluster is picked, but not
  end-to-end quality on the current topology, so it ships off by default. Default
  hybrid search measures 67.3 nDCG@10 on scifact and 31.4 on nfcorpus; see
  [benchmarks](benchmarks/index.md).

## 2026-04-25: Simeon Is Now The Default Embedding Backend

Post: https://manta.black/posts/2026-04-25-simeon/

YAMS is switching its default retrieval embeddings backend to **Simeon** — training-free, SIMD/NEON, no model downloads. The short version: 7,000 docs/s encode throughput at the quality-matching tier vs MiniLM-L6's 322 docs/s on the same CPU. That speed plus byte-identical determinism is why it's the new default.

**BEIR scifact — speed/quality Pareto** (M-series CPU, single-threaded):

| Configuration | nDCG@10 | enc docs/s | qps |
|---|---:|---:|---:|
| `bm25_only` | 0.633 | — | 31,800 |
| `bm25_pool500_linear_alpha075_4096_384` | 0.638 | 7,000 | 5,800 |
| `bm25_pool500_linear_alpha075_4096_768` | 0.638 | 3,700 | 4,900 |
| `router_default_4096_768` | 0.654 | 3,600 | 1,500 |
| MiniLM-L6 (384-d float32, CPU) | 0.654 | 322 | 1,829 |

`router_default_4096_768` matches MiniLM-L6 on nDCG@10 (0.654) and beats it on MRR@10 (0.626 vs 0.607). The router picks among `Bm25Atire`, `Bm25SabSmooth`, and `CascadeLinearAlpha` per query. For medium/long semantic queries it dispatches to **PHSS** (Per-Hit Score Smoothing) — fragment geometry scoring that cuts the similarity graph at its largest gap.

**PHSS cross-corpus** (`LargestGapApprox`, richcov builder t=8):

| Corpus | BM25 | PHSS | Δ |
|---|---:|---:|---:|
| scifact | 0.6188 | 0.6188 | +0.000 |
| NFCorpus | 0.2521 | 0.2544 | +0.002 |
| FiQA | 0.2053 | 0.2089 | +0.004 |

BM25 parity on scifact, small consistent lifts on the other two — at ~2× the query throughput of the heavy `LargestGap` variant.

**Transfer caveat:** scifact parity with MiniLM does not transfer. On FiQA the scifact-tuned router lands at 0.202 vs MiniLM's 0.359. Simeon is a lexical/topical backend that composes well with BM25; it is not a general learned-embedding replacement.

**Fusion note:** per-component nDCG instrumentation found cases where vector-only ordering was higher than the fused result — fusion was destroying signal, not the embeddings. Linear-α z-scored fusion is the only strategy that consistently beats BM25 alone on top-10 ranking.

| Requested backend | Observed provider | Dim | p50 | p95 | Result |
|---|---|---:|---:|---:|---|
| `simeon` | `plugin:Simeon` / `Simeon` | 1024 | 11.075 ms | 11.094 ms | pass |
| `onnxruntime` | `plugin:ABIModelProvider` / `all-MiniLM-L6-v2` | 384 | 10.669 ms | 21.259 ms | pass |

The same benchmark now sweeps 100- and 1,000-document batches (`--benchmark_filter=EmbeddingBackendAB_Generate`,
3 iterations per row). Current daemon-path throughput:

| Requested backend | Docs/batch | Avg latency | p50 throughput | p95 throughput | Avg throughput |
|---|---:|---:|---:|---:|---:|
| `simeon` | 100 | 10.887 ms | 9,032 docs/s | 8,803 docs/s | 9,185 docs/s |
| `onnxruntime` | 100 | 230.831 ms | 463 docs/s | 381 docs/s | 433 docs/s |
| `simeon` | 1,000 | 61.368 ms | 30,867 docs/s | 8,363 docs/s | 16,295 docs/s |
| `onnxruntime` | 1,000 | 2,579.397 ms | 386 docs/s | 369 docs/s | 388 docs/s |

Links:
- Simeon repo: https://github.com/trvon/simeon
- Benchmarks and research notes: `third_party/simeon/docs/research/`
