# Search + Topology + Graph Rerank: Architecture Map and Literature Review

> Captured: 2026-04-15
> Trigger: Post-fix of reciprocal-community InternalBenchmark SIGSEGV; user request to map benchmark ↔ topology ↔ search-engine interactions and survey literature for improvements.
> Companion plan: `/Users/trevon/.claude/plans/effervescent-inventing-newt.md`

## Scope

This document is a durable snapshot of the as-is architecture (benchmark feedback loop, runtime adaptive tuner, topology system, epoch tracking, fusion + graph rerank pipeline) and a filtered literature review whose contributions map onto currently-open YAMS design questions. It is the source-of-truth companion to the initiative plan and should be updated when a referenced file/line drifts or a paper's recommendation is superseded.

## Architecture map (as-is)

### Benchmark → Tuner feedback loop

```
InternalBenchmark::runWithQueries (src/search/internal_benchmark.cpp:459-521)
    ├── SearchEngine::search         → per-query execution
    ├── aggregateResults              → MRR / Recall@K / latency
    └── SearchTuner(corpusStats)      → captures TuningState + TunedParams
         └── stored in BenchmarkResults.tuningState / tunedParams
              ↑ computed, never persisted, never re-read   ← GAP
```

Benchmark callers: `src/cli/commands/doctor_command.cpp:4364`, FSM tuning integration tests, reranker integration tests.

### Runtime adaptive loop (real closed loop)

- `SearchEngine::searchInternal` (src/search/search_engine.cpp:1603…).
- Corpus-warming gate: `IndexFreshnessSnapshot::corpusWarming()` consumed at src/search/search_engine.cpp:1756-1769 to disable graph features when the corpus is warming.
- Per-query telemetry → `SearchTuner::observe` (src/search/search_engine.cpp:5282-5346 → src/search/search_tuner.cpp:326-489).
- EWMA-driven pressures: `kg_latency_pressure`, `graph_skip_pressure`, `kg_utility_recovery` (src/search/search_tuner.cpp:407, 434, 450).

### Topology system (src/topology/, include/yams/topology/)

- Builder: `ConnectedComponentTopologyEngine::buildArtifacts / defineDirtyRegion / updateArtifacts` (src/topology/topology_baseline.cpp:31, 227, 367). Connected-component clustering over `semantic_neighbor` edges with dirty-region incremental updates.
- Inputs: `TopologyInputExtractor` (metadata + KG edges + VectorDB) ← `EmbeddingService::updateSemanticNeighborGraph` (src/daemon/components/EmbeddingService.cpp:259).
- Store: `MetadataKgTopologyArtifactStore` (src/topology/topology_metadata_store.cpp). Cluster memberships persisted as KG node properties + document metadata.
- Trigger: post-ingest drain + explicit repair (`ServiceManager::rebuildTopologyArtifacts`, `RepairService::rebuildTopologyArtifacts` at src/daemon/components/RepairService.cpp:2185).
- Query-time consumer: `StableClusterTopologyRouter::route()` inside `buildTopologyWeakQueryCandidateSet` (src/search/search_engine.cpp:82, 98, 213). Candidate **generator** for weak queries: seed from Tier-1 vector hits → medoid/bridge expansion with score boosts (+0.05 medoid, +0.03 bridge).

### Epoch

Epoch in YAMS is lexical-index freshness, not topology versioning:

- Writers: `SearchEngineManager::noteLexicalDeltaQueued` / `noteLexicalDeltaPublished` (src/daemon/components/SearchEngineManager.cpp:22-44, 67-83).
- Reader: `IndexFreshnessSnapshot::corpusWarming()` (include/yams/search/search_execution_context.h:28-32) + debug stats in `SearchEngine`.
- Topology freshness inherits transitively; topology artifacts currently use Unix-ts `snapshotId`, no explicit topology epoch.

### Fusion + graph rerank pipeline

1. Tier-1 lexical (parallel): text (FTS5), path-tree, tag, metadata (src/search/search_engine.cpp:2303-2339).
2. Tier-2 semantic: chunk-level vector (TurboQuant-quantized `vec_chunks`), compressed ANN, optional entity-vector.
3. RRF fusion with field-aware scales (src/search/search_engine.cpp:1003-1056): text=1.0, pathTree=0.85, vector=0.75, compressedANN=0.25, kg=0.80, entityVector=0.35.
4. Graph rerank on top-N (src/search/search_engine.cpp:3589-3764):
   - `kgScorer_->score`: entity Jaccard, structural 1-hop, query-coverage, path-support features.
   - `computeReciprocalCommunitySupport` (src/search/search_engine.cpp:436-522): community signal from bidirectional `semantic_neighbor` connected components, normalized as `(size-1)/8`.
   - Corroboration guard (`corrobFloor + … × max(lexicalAnchorRatio, queryCoverageRatio)`) and rank prior `1/sqrt(1+i)`.
5. Optional TurboQuant cross-reranker.

### Gaps (verbatim from plan)

1. Benchmark outputs (`BenchmarkResults.tuningState`, `tunedParams`) are computed but never persisted or consumed.
2. `BenchmarkKgReadinessPolicy` utilities (include/yams/search/internal_benchmark.h:283-356) are declared but never invoked.
3. Per-query `QueryExecution` rows discarded unless `includeExecutions=true`.
4. Topology artifacts have no explicit epoch/version stamp for consistency checks at query time.
5. `kg_edges.weight` unused in `computeReciprocalCommunitySupport` (edges treated as unweighted bidirectional presence — contrast `src/search/graph_expansion.cpp:460,653,674` which does use `edge.weight`).
6. Only connected-components community detection — no Louvain / label propagation / hierarchical alternative evaluated.
7. Reciprocal-community normalization is candidate-set-adaptive (`(component.size()-1)/(candidateIds.size()-1)` at src/search/search_engine.cpp:519-521) but not corpus-adaptive — no reference to global cluster-size distribution, so identical structural support produces different absolute scores depending only on how wide the candidate set happens to be.
8. Tuner EWMA state is in-memory only; lost on restart.

## Literature review

Paperbridge/Zotero was unreachable during the initial pass; the Hugging Face papers index was used. A follow-up Zotero cross-check is tracked as an open question in the plan.

### Topology and community-aware retrieval

| Paper | arXiv | Why it matters for YAMS |
|---|---|---|
| CommunityKG-RAG — Chang & Zhang (2024) | [2408.08535](https://hf.co/papers/2408.08535) | Community structures in KG used for fact-checking RAG. Validates YAMS's reciprocal-community direction and suggests using community-level (not just per-doc) retrieval signals. |
| Youtu-GraphRAG — Dong et al. (2025) | [2508.19855](https://hf.co/papers/2508.19855) | "Dually-perceived community detection" + hierarchical knowledge tree with top-down filtering / bottom-up reasoning. Target end-state for replacing flat connected components with hierarchical topology. |
| LEGO-GraphRAG — Cao et al. (2024) | [2411.05844](https://hf.co/papers/2411.05844) | Modular contract: subgraph extraction → path filtering → path refinement. Matches YAMS stages but formalizes the interface; useful design reference. |
| Topology-Aware Retrieval — Wang et al. (2024) | [2405.17602](https://hf.co/papers/2405.17602) | Separates proximity-based from role-based retrieval (medoid vs bridge). YAMS already does this implicitly; paper suggests *learning* the boosts instead of hardcoding +0.05 / +0.03. |
| Graph Retrieval-Augmented Generation: A Survey — Peng et al. (2024) | [2408.08921](https://hf.co/papers/2408.08921) | Reference taxonomy for GraphRAG; use to check which axes YAMS covers vs doesn't. |

### Adaptive tuning and hybrid fusion

| Paper | arXiv | Why it matters |
|---|---|---|
| AutoRAG-HP — Fu et al. (2024) | [2406.19251](https://hf.co/papers/2406.19251) | Hierarchical multi-armed bandit for online hyper-param tuning of RAG. Direct shape for SearchTuner upgrade: fusion weights + graph-rerank knobs as arms, benchmark MRR/Recall as reward. |
| An Analysis of Fusion Functions for Hybrid Retrieval — Bruch, Gai, Ingber (2022) | [2210.11934](https://hf.co/papers/2210.11934) | Convex combination beats RRF on sample-efficiency when tuning budget exists. Motivates a post-convergence switch from weighted-RRF to convex-combination. |
| Context Tuning for RAG — Anantha et al. (2023) | [2312.05708](https://hf.co/papers/2312.05708) | Smart context signals + RRF/LambdaMART fusion. Reinforces feeding `routeDecision.intent` into scoring instead of dropping it. |

### Incremental / dynamic graph maintenance

| Paper | arXiv | Why it matters |
|---|---|---|
| FreshDiskANN — Singh et al. (2021) | [2105.09613](https://hf.co/papers/2105.09613) | Graph-based ANN with real-time inserts/deletes; validates YAMS's "no full rebuild on every change" topology posture and suggests a more mature incremental protocol than the current dirty-region merge. |
| Revisiting Dynamic Graph Clustering via Matrix Factorization — Li et al. (2025) | [2502.06117](https://hf.co/papers/2502.06117) | Temporal separated MF + selective embedding updating. Directly relevant to T7 dirty-region dynamic maintenance in the topology roadmap. |
| Decentralized and Self-adaptive Core Maintenance on Temporal Graphs — Rucci et al. (2025) | [2510.00758](https://hf.co/papers/2510.00758) | Incremental coreness updates — lightweight centrality augment for connected-components clusters. |

### Reranking and routing

| Paper | arXiv | Why it matters |
|---|---|---|
| Learning to Route in Similarity Graphs — Baranchuk et al. (2019) | [1905.10987](https://hf.co/papers/1905.10987) | Learnable routing over similarity graphs — replaces `StableClusterTopologyRouter`'s deterministic seed-expansion with a learned policy. |
| JointRank: Rank Large Set with Single Pass — Dedov (2025) | [2506.22262](https://hf.co/papers/2506.22262) | Single-pass listwise reranking over large candidate sets with implicit pairwise comparisons. Candidate replacement for per-doc graph rerank when candidate budget grows. |

## Mapping: gaps → initiatives → literature

| Gap | Initiative (from plan) | Primary literature anchor |
|---|---|---|
| #1 dead benchmark outputs, #3 discarded per-query rows, #8 ephemeral tuner state | P1 Persist benchmark outcomes | AutoRAG-HP (bandit needs persistent history) |
| #4 no topology epoch | P2 Topology epoch + query-time consistency stamp | FreshDiskANN (versioned graph semantics) |
| #5 unused edge weights, #6 single algorithm | P3 Louvain / hierarchical community detection | Youtu-GraphRAG, CommunityKG-RAG |
| #7 hardcoded normalization | P4 Corpus-adaptive reciprocal-community normalization | Topology-Aware Retrieval (learned role weights) |
| #2 dead readiness policy | P5 Wire `BenchmarkKgReadinessPolicy` | — (cleanup) |
| #8 rule-based tuner | P6 Hierarchical MAB tuner | AutoRAG-HP |
| Fusion form lock-in | P7 Convex-combination fusion post-convergence | Bruch et al. 2022 |
| Unused edge weight + temporal signal | P8 Use `kg_edges.weight` + temporal decay | Rucci et al. 2025 (temporal cores) |

## Critical files

| File | Role |
|---|---|
| src/search/internal_benchmark.cpp:459-521 | Benchmark harness + tuning-state capture; dead outputs |
| include/yams/search/internal_benchmark.h:213-356 | `BenchmarkResults`, `BenchmarkKgReadinessPolicy` (dead) |
| src/search/search_tuner.cpp:225-489 | Adaptive runtime loop (observe + EWMA + pressure rules) |
| src/search/search_engine.cpp:436-522, 1603…, 1756-1769, 3450-3764, 5282-5346 | Fusion, graph rerank, community signal, corpus-warming gate, tuner observe |
| include/yams/search/search_execution_context.h | Freshness snapshot + lexical epoch + topology overlay hashes |
| src/topology/topology_baseline.cpp | Connected-component engine + dirty-region update |
| src/topology/topology_metadata_store.cpp | Artifact persistence into KG + metadata |
| src/daemon/components/SearchEngineManager.cpp:22-83 | Lexical delta epoch writers |
| src/daemon/components/EmbeddingService.cpp:259 | Produces semantic-neighbor graph (topology input) |
| docs/design/topology_service_architecture.md, docs/research/topology_cluster_routing.md, docs/design/search_routing_tuning_flow.md | Existing topology roadmap (T1–T7) |

## Daemon/IPC surface audit (P1 implementation guardrails)

Captured 2026-04-15 during P1 scaffolding.

**Current state:**
- `yams doctor benchmark` (src/cli/commands/doctor_command.cpp:4329-4517) runs entirely in-process from the CLI via `cli_->ensureStorageInitialized()` + `cli_->getAppContext()`. No daemon IPC on this path.
- `include/yams/daemon/ipc/proto/ipc_envelope.proto` has **no** `Benchmark*`, `Tuner*`, or `Doctor*` request/response types.
- `src/daemon/components/RequestDispatcher*.cpp` has no benchmark/tuner handlers.
- The only daemon reference to "benchmark" is `YAMS_BENCH_OPT_LOOP` env-var fastpath in `ServiceManager.cpp:1141` — a shutdown tunable, not a benchmark runner.

**Implications for this plan:**
- **P1-A/B (history store + `doctor --history`)**: Safe to keep CLI-side and file-based. No proto or RequestDispatcher changes. Atomic rename in `BenchmarkHistoryStore` protects any future daemon reader. File location: `<data_dir>/benchmark_history.json`.
- **P1-C (tuner EWMA persistence)**: Daemon-owned — `SearchTuner` lives inside the search engine inside the daemon in normal mode. File-level persistence (same `<data_dir>`) avoids needing IPC, but the save cadence must be throttled to avoid write amplification on every `observe()` call.
- **P6 (hierarchical MAB tuner)**: If the daemon is ever to *run* benchmarks on its own schedule (to feed the MAB reward signal), it will need new proto types: `RunBenchmarkRequest/Response`, `GetBenchmarkHistoryRequest/Response`, `GetTunerStateRequest/Response`. That is a **prerequisite for P6**, not P1. Flagging here so the schema is designed once.
- **Concurrency**: Today the CLI's `doctor benchmark` conflicts with a running daemon because both try to open the same storage. That is a pre-existing constraint outside P1 scope; P1 inherits it unchanged.



- Retry paperbridge/Zotero to cross-check the HF shortlist against the curated library.
- Stand up a larger canonical regression corpus (BEIR subset) before measuring P3/P6/P7 effects; current harness has 3 docs / 1 query.
- Decide whether P2 (topology epoch) lands together with P1 (benchmark persistence) so the history rows have a consistent (lexical_epoch, topology_epoch) key from day one.

## Change log

- 2026-04-15: Initial capture post SIGSEGV fix and architecture pass.
- 2026-04-15: Corrected Gap #7 — normalization is already candidate-set-adaptive; remaining concern is corpus-adaptivity. Refined Gap #5 to note `edge.weight` is used elsewhere (`graph_expansion.cpp`) but skipped in the reciprocal-community computation specifically.
