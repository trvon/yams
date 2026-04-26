# Newsletter

Product updates and occasional deep-dives.

## Subscribe

- SourceHut list: https://lists.sr.ht/~trvon/newsletter
- RSS (GitHub releases): https://github.com/trvon/yams/releases.atom

## What you'll get

- Release notes and breaking changes
- Benchmark updates
- Design notes (search, storage, plugins)

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

Links:
- Simeon repo: https://github.com/trvon/simeon
- Benchmarks and research notes: `third_party/simeon/docs/research/`
