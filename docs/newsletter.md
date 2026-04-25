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

YAMS is switching its default retrieval embeddings backend to **Simeon** (training-free, SIMD/NEON text->vector), and the reason is simple: it makes local-first search faster to iterate on and more deterministic to benchmark.

Simeon is model-free: it maps text to vectors via char/word n-grams, hashed count-sketch, optional random projection, and L2 normalization. No model downloads. No ONNX embedding runtime required for the default path.

This change is also a reaction to the broader "training-free retrieval" momentum (NUMEN and friends): once you take throughput-per-watt and operational simplicity seriously, model-free backends stop looking like a novelty and start looking like a sensible default. The other lesson from our own reproduction attempts is that benchmark fixtures and harness discipline matter as much as the core idea.

This is not a claim that "training-free beats learned" in general. Simeon is best understood as a lexical/topical retrieval backend that composes well with BM25 and hybrid fusion, and it is explicit about transfer limits and negative results.

One benchmark highlight from the vendored Simeon research notes:
- On BEIR scifact, the shipped routed hybrid (`router_default_4096_768`) matches a MiniLM-L6 reference on `nDCG@10` (0.654 vs 0.654) and beats it on `MRR@10` (0.626 vs 0.607), while trailing on recall.

The engineering punchline is throughput: Simeon's encode path is fast enough to turn tuning loops from minutes into seconds (see `docs/benchmarks/retrieval_precision_optimization.md`). That speed, plus byte-identical determinism, is a great fit for YAMS' topology and retrieval ablation harnesses.

One internal benchmark takeaway worth calling out: when YAMS instrumented per-component nDCG on scifact/Simeon, it found cases where vector-only ordering quality was higher than the final fused result, meaning fusion was destroying real signal rather than embeddings being "bad" (see `docs/benchmarks/e1_per_component_telemetry.md`).

Links:
- Simeon repo: https://github.com/trvon/simeon
- Benchmarks and research notes: `third_party/simeon/docs/research/`
