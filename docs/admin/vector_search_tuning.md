# Vector Search Tuning

Concise guidance to select, size, and tune vector search for YAMS. Calibrate parameters against your data and latency/recall targets. Validate with repeatable benchmarks.

## TL;DR

- Choose cosine similarity with normalized embeddings (normalize to unit length).
- Start simple: HNSW index, M=16–32, efConstruction=200–400, efSearch=32–128.
- IVF-PQ for large, memory-constrained datasets: nlist≈sqrt(N)–4·sqrt(N), nprobe=1–8, PQ with 8–16 bytes/vector.
- Hybrid ranking: keyword/filter → ANN (K×5–10 candidates) → exact re-rank.
- Precompute embeddings offline; batch ingest; snapshot indices; measure p50/p95 latency and recall@K.

---

## Embeddings

- Model selection
  - Pick a domain-appropriate embedding model (code, text, multi-lingual).
  - Dimensionality: 384–1024 typical; higher dims increase memory, compute, and index size.
- Normalization and metric
  - Use cosine similarity for most text/code embeddings.
  - If the engine uses inner-product (dot), normalize vectors to unit length to emulate cosine.
  - For L2, normalization changes semantics; align metric with embedding training objective.
- Precision and storage
  - Default to FP32 for quality; FP16 halves memory with small quality loss (validate).
  - Quantization (e.g., PQ): reduces memory and improves speed at some recall cost (see IVF-PQ).

---

## Index selection

- Flat (exact)
  - Baseline for quality; not scalable for large N (O(N) per query).
  - Good for small datasets, re-ranking, and correctness tests.
- HNSW (Hierarchical Navigable Small World)
  - Fast, high-recall ANN; memory-heavy vs IVF.
  - Recommended default when memory is sufficient.
- IVF / IVF-PQ (Inverted File with Product Quantization)
  - Scales to large N with lower memory; tune nlist/nprobe and PQ bits.
  - Slight quality loss; suitable for multi-tenant or cost-sensitive deployments.

---

## HNSW parameters (typical ranges)

- M (graph degree): 16–48
  - Higher M → higher recall, more memory/build time. Start with 16 or 32.
- efConstruction: 200–800
  - Higher → better graph quality; slower build. Start at 200–400.
- efSearch: 32–256 (query-time knob)
  - Higher → higher recall, higher latency. Start at 64–128, calibrate to meet SLOs.
- Memory estimate (rule-of-thumb)
  - ~ (vector_bytes + overhead(M)) per vector. Overhead grows with M and graph layers; validate empirically.

---

## IVF/IVF-PQ parameters (typical ranges)

- nlist (coarse clusters)
  - Start with nlist ≈ sqrt(N) to 4·sqrt(N).
  - Larger nlist → finer partitioning (better recall potential) but higher build/memory.
- nprobe (clusters probed at query)
  - 1–16; higher → higher recall/latency. Start at 4–8.
- PQ (product quantization)
  - Code size: 8–16 bytes/vector for a good quality/speed trade-off.
  - OPQ (rotational PQ) can recover some quality loss; validate on your data.
- Residual/IVF-PQ hits
  - Use residual quantization (if available) to preserve fine detail after coarse assignment.

---

## Hybrid search (keyword + ANN)

- Strategy
  - Pre-filter by metadata/keyword/permissions → ANN on reduced candidate set.
  - Or ANN first → metadata filter → exact re-rank (when metadata selectivity is poor).
- Candidate sizes
  - If final topK=K, set ANN candidates to K×(5–10) before exact or hybrid re-ranking.
  - Calibrate multiplier to maintain recall while respecting latency budgets.
- Metadata filters
  - Use cheap, selective filters early. Avoid ANN over broad, unfiltered corpora when a filter can prune.

---

## Ingest and maintenance

- Precompute embeddings offline; batch inserts to the index.
- Build strategies
  - HNSW: online insertion acceptable; rebuild offline for major re-sharding/model updates.
  - IVF/IVF-PQ: prefer offline training (k-means for nlist) and bulk add; periodic re-train as corpus evolves.
- Snapshots and migration
  - Keep dual indices during model changes; A/B traffic and cut over on SLO and quality acceptance.
  - Pin versioned embedding pipelines (model, tokenizer, normalization) for reproducibility.
- Drift and re-train
  - Monitor recall and drift; re-train codebooks (IVF) or rebuild graphs (HNSW) as data distributions shift.

---

## Memory, storage, and sharding

- Memory
  - HNSW: plan for index overhead; avoid swapping. Consider FP16 vectors to reduce footprint.
  - IVF-PQ: PQ reduces memory; tune code size to fit RAM and hit latency targets.
- Storage layout
  - Co-locate index with active data; use fast NVMe and OS page cache effectively.
- Sharding/partitioning
  - Partition by tenant/project/namespace to bound working set and limit cross-tenant scans.
  - Avoid tiny shards (inefficient); avoid massive shards (slow rebuilds). Target balanced shard sizes.
- Concurrency
  - Size thread pools to physical cores; measure contention. Avoid oversubscription.

---

## Query tuning workflow

1. Establish a ground-truth subset (exact search or human-labeled) to compute recall@K.
2. Set initial parameters:
   - HNSW: M=16–32, efConstruction=200–400, efSearch=64–128.
   - IVF-PQ: nlist≈2·sqrt(N), nprobe=4–8, PQ=8–16 bytes.
3. Measure p50/p95 latency and recall@K on cold and warm caches.
4. Adjust one knob at a time:
   - Increase efSearch or nprobe for higher recall; reduce if latency exceeds SLOs.
   - Increase nlist to improve partition resolution; re-train and re-measure.
5. Validate in production-like traffic, including hybrid filter paths.

---

## Evaluation and metrics

- Quality
  - recall@K, nDCG, precision@K on representative queries.
- Latency
  - p50/p95/p99 per query class; separate cold/warm; include network and filter costs.
- Throughput and resource
  - QPS at target SLOs; CPU, memory RSS, fault rates; disk latency p95+.
- Index health
  - HNSW connectivity stats (if exposed), IVF cluster balance, PQ quantization error.

---

## Troubleshooting

- Low recall
  - Increase efSearch (HNSW) or nprobe (IVF); increase nlist; reduce PQ compression.
  - Verify embedding normalization and metric alignment (cosine vs dot/L2).
- High latency
  - Reduce efSearch/nprobe; introduce/selective filters; shrink candidate multiplier for re-rank.
  - Ensure index and hot vectors fit in RAM; confirm NVMe performance.
- Poor IVF balance
  - Re-train k-means with better initialization or higher iterations; review nlist sizing.
- Memory pressure
  - Use PQ/FP16; reduce M (HNSW) with careful recall validation; shard by namespace.

---

## Model management and runtime discovery

- Preferred model
  - Configure with embeddings.preferred_model (or via CLI: yams config embeddings model <name>).
  - The CLI model suite helps you inspect and verify: yams model list | download | info | check.
- Model root and autodiscovery
  - embeddings.model_path sets the root for name-based models.
  - Resolution order for name-based models: configured root → ~/.yams/models → models/ → /usr/local/share/yams/models. Full absolute paths are used as-is.
  - yams model list enumerates models found under the resolved roots (expects model.onnx in a per-model directory).
- ONNX provider/plugin discovery
  - The ONNX model provider is loaded dynamically. Builds define an install prefix so runtime discovery includes $prefix/lib/yams/plugins.
  - You can also point discovery to a custom directory with YAMS_PLUGIN_DIR when developing or in CI.
  - Use yams model check to verify ONNX runtime support and plugin directory status.
- Keep‑hot vs lazy loading
  - embeddings.keep_model_hot=true pre-creates sessions so embeddings are immediately usable after preload; embeddings.preload_on_startup controls startup behavior.
  - On constrained systems or when startup delays matter, prefer lazy loading (keep_model_hot=false; preload_on_startup=false). You can also disable preload with YAMS_DISABLE_MODEL_PRELOAD=1.
- Readiness and observability
  - If models are not ready, hybrid/semantic paths may degrade to keyword-only until the provider becomes available.
  - yams status shows service readiness and a WAIT line during initialization; yams stats -v surfaces recommendations and service status details.

## References

- Architecture → Vector Search: ../architecture/vector_search_architecture.md
- Admin → Performance Tuning: ./performance_tuning.md
- Guides → Vector Search: ../user_guide/vector_search_guide.md