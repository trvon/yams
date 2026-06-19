# YAMS Pipeline Architecture

## Ingestion Pipeline

```
                    ┌──────────────────┐
  yams add ────────▶│  IngestService   │──▶ PostIngestQueue
                    └──────────────────┘    ▲profiling
                        4 ZoneScopes        │5 YAMS_ASSERT
                                            │1 ZoneScope
                                            ▼
                    ┌──────────────────────────────────┐
                    │     PostIngestQueue::processBatch │
                    │  ┌──────────────────────────────┐ │
                    │  │ post_ingest_enrichment        │ │
                    │  │   extraction ─▶ KG ─▶ symbol  │ │
                    │  │   ─▶ entity ─▶ embedding     │ │
                    │  │   ─▶ FTS5 ─▶ title           │ │
                    │  │   5 ZoneScopes                │ │
                    │  └──────────────────────────────┘ │
                    └──────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
  │ EntityGraph  │   │  Embedding   │   │ GraphComponent│
  │ 7 ZoneScopes │   │ 15 ZoneScopes│   │ 10 ZoneScopes │
  └──────────────┘   └──────────────┘   └──────────────┘
```

## Retrieval Pipeline (with Precision Data)

```
  yams search ──────▶ SearchEngine
                         │ 12 ZoneScopes
                         ▼
                    ┌──────────────────┐
                    │  Query Parser    │
                    │  Tokenizer       │
                    └──────────────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
     ┌────────────────┐   ┌────────────────┐
     │ Lexical (FTS5) │   │ Vector (Simeon)│
     │ keyword results │   │ ANN + PQ codes │
     └────────────────┘   └────────────────┘
              │                     │
              └──────────┬──────────┘
                         ▼
              ┌──────────────────┐
              │  ResultFusion    │  ← +38.9% MRR gain
              │  4 ZoneScopes    │    recovers low pre-fusion scores
              └──────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │  Graph Rerank    │  ← neutral on small corpus
              │  (optional)      │    significant for large corpora
              └──────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │  TopologyManager │──▶ cluster rerank
              │  12 ZoneScopes   │
              │  TopologyTuner   │
              │  10 ZoneScopes   │
              └──────────────────┘
                         │
                         ▼
              ┌──────────────────┐
              │ Final Results    │
              │ (topK documents) │
              └──────────────────┘
```

### Stage Precision (YAMS_SEARCH_STAGE_TRACE, 30 docs, 10 queries, topK=10)

```
  Stage                  MRR     nDCG    Delta
  ───────────────────────────────────────────
  pre_fusion            0.611   0.630   ← lexical+vector raw
  graphless_post_fusion 1.000   1.000   +38.9% ← fusion recovery
  post_fusion           1.000   1.000   +
  post_graph            1.000   1.000   =
  final                 1.000   1.000   =

  Timing: 44.2ms total (4.4ms/query avg, 26.9ms stddev)
```

### Optimization Loop

```
  1. Baseline: run retrieval_quality_bench with YAMS_SEARCH_STAGE_TRACE=1
  2. Identify pre-fusion gaps (stage with lowest MRR)
  3. Tune: adjust lexical/vector weights, fusion strategy, rerank threshold
  4. Re-run: measure MRR delta, latency delta
  5. Record: per-stage timing → docs/design/pipeline-diagram.md

  Helper:
    tests/benchmarks/scripts/retrieval_opt_loop.sh --corpus-size 100 --queries 20
    captures best candidate, recommended env overrides, and JSONL debug output.
    Default mode prefers live embeddings/plugin path; pass `--mock-embeddings`
    only for explicitly synthetic tuning runs.
```

## SQL Write Paths Audit

```
                         ┌────────────────────┐
  yams add ─────────────▶│  IngestService     │
                         └────────────────────┘
                                  │
                                  ▼
                         ┌────────────────────┐
                         │  PostIngestQueue   │──▶ WriteCoordinator (batched)
                         │  11 WriteCoord refs│
                         └────────────────────┘
                                  │
                         ┌────────┴────────┐
                         ▼                 ▼
                  ┌────────────┐   ┌────────────────┐
                  │ WriteCoord │   │ Metadata Repo  │ ← DIRECT SQL (no batching!)
                  │ (batched)  │   │ content_ops    │    executeQueryOnPool
                  └────────────┘   │ doc_crud_ops   │    → pool_.acquire()
                          │        │ path_tree_ops  │    → db.execute()
                          ▼        │ relationship_  │
                   SQLite Writes   │ metadata_kv    │
                          │        │ history_ops    │
                          └────────┴────────────────┘
                                   ▼
                            SQLite Writes
                            (unbatched, per-op)

  Problem: 10+ repository/*.cpp files call SQL directly with ZERO
  WriteCoordinator usage. Every INSERT/UPDATE hits SQLite per-op.
  Fix: route metadata writes through WriteCoordinator or add
  write-queue for metadata operations.
```

## Profiling Data Capture Infrastructure

| Tool | Status | Use |
|------|--------|-----|
| xctrace (Instruments) | ✅ Working | CPU sampling, Time Profiler traces |
| Tracy ZoneScopes | ✅ 108 zones compiled | Frame-level profiling (needs server) |
| YAMS_SEARCH_STAGE_TRACE | ✅ Working | Per-stage MRR/NDCG in debugStats |
| YAMS_BENCH_OPT_LOOP | ✅ Working | Optimization sweep with debug output |
| retrieval_quality_bench | ✅ Working | Precision + latency measurement |

### Benchmark Fidelity Labels

Each benchmark/profile helper should emit a small manifest JSON with a heuristic fidelity label:

- `daemon-faithful` — real plugin discovery and real embeddings enabled
- `live-ish` — real retrieval path, but some startup/queue/plugin shortcuts remain
- `synthetic` — mock embeddings, synthetic datasets, or other shortcuts dominate

Current helper scripts write `*.manifest.json` alongside their primary artifacts.

For correlated audit bundles, use:
`tests/benchmarks/scripts/live_benchmark_bundle.sh`
This captures a single run's manifest, benchmark log, stage-trace JSONL, xctrace trace, exported profile XML, and parsed hot-zone summary in one directory.

For a higher-level live-mirror suite, use:
`tests/benchmarks/scripts/live_mirror_suite.sh`
This pairs daemon-ish ingestion metrics with a correlated retrieval/profile bundle so startup/ingestion and retrieval evidence can be reviewed together.
Use `--steady-state` when you want longer runs that reduce startup noise and collect denser queue telemetry.

### Quick Profile Recipe

```bash
# Search precision + timing (small corpus, fast)
YAMS_BENCH_CORPUS_SIZE=100 YAMS_BENCH_NUM_QUERIES=20 \
YAMS_BENCH_DATASET=synthetic YAMS_SEARCH_STAGE_TRACE=1 \
YAMS_BENCH_FORCE_MOCK_EMBEDDINGS=1 \
./build/debug/tests/benchmarks/retrieval_quality_bench \
  --benchmark_filter=BM_RetrievalQuality

# CPU profile with xctrace (large corpus, long run)
YAMS_BENCH_CORPUS_SIZE=2000 YAMS_BENCH_NUM_QUERIES=200 \
YAMS_BENCH_DATASET=synthetic YAMS_TEST_SAFE_SINGLE_INSTANCE=1 \
xcrun xctrace record --template "Time Profiler" --time-limit 600s \
  --output /tmp/yams-traces/search.trace --no-prompt \
  --launch -- ./build/debug/tests/benchmarks/retrieval_quality_bench

# Parse xctrace hot zones
tests/benchmarks/scripts/search_profile.sh --corpus-size 500 --queries 100
```

## Repair Pipeline

```
  yams doctor/repair ──▶ RepairService (10 ZoneScopes)
                            │
                            ├── RepairServiceScheduling (6 ZoneScopes)
                            ├── repair_health_probe (2 ZoneScopes)
                            ├── repair_plan_builder (4 ZoneScopes)
                            │
                            ▼
                       embedding_repair_util ──▶ PostIngestQueue
                                                  (high-priority RPC channel)
```

| Stage | Files | ZoneScopes |
|-------|-------|-----------|
| RepairService | 1 | 10 |
| RepairServiceScheduling | 1 | 6 |
| repair_health_probe | 1 | 2 |
| repair_plan_builder | 1 | 4 |
| embedding_repair_util | 1 | 0 (template/utility) |
| **Repair total** | **5** | **22** |

## Profiling Coverage

| Stage | Files | ZoneScopes | Status |
|-------|-------|-----------|--------|
| IngestService | 1 | 4 | ✅ |
| PostIngestQueue | 1 | 1 | ✅ (1, needs more) |
| PostIngestEnrich | 1 | 5 | ✅ |
| EntityGraphService | 1 | 7 | ✅ |
| EmbeddingService | 1 | 15 | ✅ |
| GraphComponent | 1 | 10 | ✅ |
| SearchEngine | 1 | 12 | ✅ |
| SearchFusion | 1 | 4 | ✅ |
| TopologyManager | 1 | 12 | ✅ |
| TopologyTuner | 1 | 10 | ✅ |
| VectorIndexCoordinator | 1 | 6 | ✅ |
| **Total** | **11** | **86** | |

## Assertion Coverage

| Stage | YAMS_ASSERT | YAMS_DCHECK | Status |
|-------|------------|-------------|--------|
| PostIngestQueue | 5 | 0 | ⚠️ Has invariants, no DCHECK |
| All other stages | 0 | 0 | ❌ Missing |

## Benchmark Coverage

| Stage | Benchmarks | Status |
|-------|-----------|--------|
| Ingestion | ingestion_throughput, large_scale, multi_client, e2e, single_pipeline | ✅ |
| Extraction | zyp_extraction, symbol_extraction | ✅ |
| KG | hnsw_entity_search | ✅ |
| Symbol | symbol_extraction | ✅ |
| Entity | — | ❌ Missing |
| Embedding | vector_backend_compare, vector_memory_scaling | ✅ |
| FTS5 | — | ❌ Missing |
| Search/Fusion | search_benchmarks, fusion_experiment, retrieval_quality, metapath_ab | ✅ |
| Topology | topology_ablation | ✅ |

## Pipeline Invariants (Recommended DCHECKs)

- PostIngestQueue: `queued == in_flight + consumed + dropped`
- EmbeddingService: `embed_prepared == embed_queued + embed_dropped`
- EntityGraphService: `kg_consumed + kg_dropped == total_ingested`
- SearchEngine: `result_count <= requested_limit`
- TopologyManager: `cluster_count <= document_count`
- GradientLimiter: `inFlight >= 0`, `rejectCount >= 0`
