# Topology-Aware Cluster Routing For YAMS

_Last updated: 2026-04-13_

## Thesis

- Persistent homology (PH) and Mapper are not good direct hot-path retrieval primitives for YAMS.
- A topology library can still be useful if it converts corpus geometry into durable routing
  artifacts such as cluster labels, medoids, bridge documents, and stability scores.
- The right first move is a cold-path `TopologyService`, not a hot-path PH or Mapper query
  executor.

## Current YAMS Baseline

YAMS already has multiple topology-shaped retrieval mechanisms.

- Vector retrieval uses HNSW-style approximate nearest neighbor search.
  - `src/vector/vector_database.cpp:1190-1200`
  - `include/yams/vector/vector_database.h:19-23`
- Post-ingest embedding builds a document-level semantic graph by creating reciprocal
  `semantic_neighbor` edges from document embeddings.
  - `src/daemon/components/EmbeddingService.cpp:254-475`
- Search already uses graph-aware query expansion, graph vector expansion, and graph reranking.
  - `include/yams/search/search_engine.h:277-301`
  - `include/yams/search/search_engine.h:357-381`
  - `src/search/search_engine.cpp:3377-3545`
- Search already computes reciprocal-community support over the rerank candidate window.
  - `src/search/search_engine.cpp:264-365`
- The daemon already runs semantic topology maintenance and prunes one-way semantic edges after
  post-ingest drain.
  - `src/daemon/components/GraphComponent.cpp:948-1038`
  - `src/metadata/kg_topology_analysis.cpp:27-212`

This matters because any new topology layer has to add signal beyond the combination of:

- ANN navigation in the vector tier
- semantic-neighbor graph construction in post-ingest
- reciprocal-community support in graph rerank
- existing topology analysis and maintenance in the KG layer

## Cost Model In The Current Code

The main architectural risk is not the abstract cost of topology. It is stacking another global
graph pass on top of code that already spends real budget on global or semi-global structure.

- Vector search is already optimized for fast ANN lookup.
  - `src/vector/vector_database.cpp:1196-1198`
- Compressed ANN experiments already document expensive build-time graph work.
  - `src/vector/compressed_ann.cpp:186-253`
  - `buildMetricAligned()` documents `O(n^2 x dim)` and `O(n^2 x byte_len)` build cost.
- Semantic-neighbor construction in `EmbeddingService` scores each newly embedded document against
  the corpus of document vectors currently known to the vector database.
  - `src/daemon/components/EmbeddingService.cpp:344-450`
- Query-time graph rerank is intentionally bounded to the top rerank window rather than the full
  corpus.
  - `src/search/search_engine.cpp:3361-3545`

This means direct PH or Mapper on the full semantic graph would not replace an existing expensive
step. In the naive design it would add a second expensive global structure pass.

## Why PH Or Mapper Should Not Go Straight Into The Hot Path

### 1. They do not match the query contract

YAMS needs ranked relevance for arbitrary code, path, prose, and mixed queries. PH produces
topological invariants. Mapper produces a cover graph and overlapping clusters. Neither one maps
cleanly to "what should be rank 1 for this query" without an additional routing and scoring layer.

### 2. They are hard to maintain under continuous ingest

YAMS ingest is asynchronous and continuous.

- `IngestService` batches and drains document ingest.
  - `src/daemon/components/IngestService.cpp:178-305`
- `PostIngestQueue` prepares extracted content and dispatches embedding work.
  - `src/daemon/components/PostIngestQueue.cpp:2653-2739`
- `EmbeddingService` writes vectors and semantic edges.
  - `src/daemon/components/EmbeddingService.cpp:2125-2282`

PH and Mapper want a relatively stable global view of the graph or point cloud. In a continuously
changing corpus, the choices are all awkward:

- recompute often and pay a high cold-path cost
- accept stale topology artifacts
- implement incremental approximations with weaker guarantees

### 3. Mapper is parameter-sensitive in a mixed corpus

Mapper requires choices for:

- filter function
- interval count / resolution
- overlap / gain
- cluster threshold within each cover cell

YAMS is intentionally mixed across code, prose, docs, and graph-shaped corpora. A single Mapper
configuration is unlikely to behave well across those modes. That makes stable, comparable routing
artifacts harder to maintain than existing HNSW or graph-rerank behavior.

### 4. Topological invariants do not encode query intent

YAMS already routes by intent and community.

- `src/search/query_router.cpp:77-189`
- `include/yams/search/search_engine.h:110-121`

PH summaries can tell us something about corpus shape, persistence, and stability, but they do not
know whether a query is code-heavy, path-like, or prose-heavy. For actual ranked retrieval, query
intent still has to dominate.

### 5. They may duplicate stronger existing signals

YAMS already has explicit semantic graph edges and a query-local reciprocal community signal.

- `semantic_neighbor` edges are explicit semantic relationships, not just geometric proximity.
- `computeReciprocalCommunitySupport(...)` already acts like a bounded, query-local cluster
  coherence prior.

If PH or Mapper are used directly, they risk duplicating:

- HNSW layer navigation in the vector tier
- semantic-neighbor graph connectivity in the KG tier
- reciprocal-community support in graph rerank

The new work only becomes clearly valuable after translation into durable routing artifacts.

## Strongest Case For A Topology Library Anyway

The counterargument is strongest when topology is treated as a cold-path artifact generator rather
than a new search engine.

### 1. The useful output is not the raw topology object

The hot path does not need persistence diagrams or a live Mapper graph. It needs cheap artifacts
that can be blended into retrieval:

- `cluster_id`
- `cluster_level`
- `cluster_persistence_score`
- `cluster_medoid_doc`
- `bridge_doc`
- `overlap_membership`

### 2. YAMS already has the right seam

The cleanest insertion point is after vectors exist and after semantic-neighbor edges are created.

- `src/daemon/components/EmbeddingService.cpp:2257-2282`
- `src/daemon/components/GraphComponent.cpp:948-1038`

This suggests a topology service that runs after the current semantic graph build instead of trying
to replace it.

### 3. A library isolates the algorithmic risk

A dedicated topology library can keep the experiment reversible. If PH or Mapper do not justify
their maintenance cost, YAMS can keep the current HNSW plus graph-rerank design with minimal hot-path
churn.

### 4. Incremental and partial computation can narrow the problem

The most practical early scope is not full multiparameter PH. It is:

- 0-dimensional persistence or persistence-like cluster stability
- per-component or per-neighborhood computation
- snapshot or rolling recomputation
- stable cluster artifact emission

That is much more aligned with YAMS than trying to run a full topological analysis in the query
loop.

## Recommended Architecture

### Hot Path

Keep the current retrieval path:

1. query routing
2. text / path / KG / vector components
3. fusion
4. graph rerank
5. optional cross rerank and semantic rescue

Relevant code:

- `src/search/query_router.cpp:77-189`
- `src/search/search_engine.cpp:1667-1763`
- `src/search/search_engine.cpp:3377-3545`

### Cold Path

Add a `TopologyService` that runs after embeddings and semantic-neighbor edges are updated.

Responsibilities:

- read document embeddings or semantic-neighbor edges
- compute stable cluster artifacts
- store cluster metadata on document nodes or in document metadata
- support partial recomputation after ingest drain

Natural integration points:

- `EmbeddingService::updateSemanticNeighborGraph(...)`
- `GraphComponent::maintainSemanticTopology(...)`
- `ServiceManager` post-ingest drain callback

Relevant code:

- `src/daemon/components/EmbeddingService.cpp:254-475`
- `src/daemon/components/ServiceManager.cpp:2098-2118`
- `src/daemon/components/GraphComponent.cpp:948-1038`

### Query Use Of Topology Artifacts

Topology artifacts should only be used where they are cheap and clearly additive.

Recommended first uses:

1. weak-query routing when lexical evidence is sparse
2. graph-rerank priors using cluster stability or medoid proximity
3. opt-in diagnostics or research-only search mode

Not recommended initially:

- PH or Mapper execution in the normal query loop
- replacing HNSW with topology-derived routing
- using topology scores as a primary ranking signal before offline validation

## Library Boundary

The library should expose small, queryable artifacts rather than forcing the search layer to know
about raw PH or Mapper internals.

Minimum useful outputs:

- cluster assignments
- cluster representatives or medoids
- persistence or stability scores
- overlap memberships
- bridge or outlier annotations
- partial recomputation API

Operational requirements:

- deterministic outputs for the same snapshot
- partial or rolling recomputation
- zero-copy or low-allocation interfaces for handoff into daemon code
- ability to compute on subgraphs or windows rather than always the full corpus

## Suggested Evaluation Sequence

1. Offline only: generate cluster artifacts from the existing semantic graph.
2. Attach those artifacts to documents without touching the current search ranking.
3. Measure cluster quality, stability, and update cost under ingest.
4. Add a weak-query routing experiment that consults cluster medoids first.
5. Only after that, test topology-aware rerank priors.

Metrics to collect:

- precision@k
- recall@k
- NDCG@k
- query latency delta
- ingest-time update overhead
- topology artifact freshness lag
- cluster stability across ingest windows

## Literature Signals

These papers are the strongest signals found during the HF paper scan.

- `Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small
  World graphs`
  - https://hf.co/papers/1603.09320
  - Reinforces that layered navigable graph search is already a strong baseline.
- `Fiber-Navigable Search: A Geometric Approach to Filtered ANN`
  - https://hf.co/papers/2604.00102
  - Suggests geometry-aware routing and filtering are more directly retrieval-relevant than raw PH.
- `Learning to Route in Similarity Graphs`
  - https://hf.co/papers/1905.10987
  - Supports better routing over similarity graphs rather than full topological analysis.
- `Beyond Nearest Neighbors: Semantic Compression and Graph-Augmented Retrieval for Enhanced
  Vector Search`
  - https://hf.co/papers/2507.19715
  - Supports graph-augmented retrieval and coverage-aware routing.
- `MODE: Mixture of Document Experts for RAG`
  - https://hf.co/papers/2509.00100
  - Cluster-and-route is a more practical retrieval pattern than live PH or Mapper execution.
- `Topological Metric for Unsupervised Embedding Quality Evaluation`
  - https://hf.co/papers/2512.15285
  - Useful as an evaluation signal for embedding geometry, not yet a direct retrieval primitive.
- `On the Expressivity of Persistent Homology in Graph Learning`
  - https://hf.co/papers/2302.09826
  - Good theory signal, but still distant from fast ranked retrieval in a live daemon.

## Recommendation

Do not put PH or Mapper directly into the YAMS search hot path.

Instead:

1. build a small topology library
2. run it in a cold-path `TopologyService`
3. emit persistent cluster-routing artifacts
4. use those artifacts only for weak-query routing and optional rerank priors
5. keep reciprocal-community support as the canonical existing topology signal until the new layer
   proves value offline

That gives YAMS a credible way to explore topology-aware retrieval without destabilizing the ANN and
graph-retrieval stack that already exists.

## Code References

- `src/daemon/components/IngestService.cpp:178-305`
- `src/daemon/components/PostIngestQueue.cpp:2653-2739`
- `src/daemon/components/EmbeddingService.cpp:254-475`
- `src/daemon/components/EmbeddingService.cpp:2125-2282`
- `src/daemon/components/GraphComponent.cpp:948-1038`
- `src/daemon/components/ServiceManager.cpp:2098-2118`
- `src/metadata/kg_topology_analysis.cpp:27-212`
- `src/search/query_router.cpp:77-189`
- `src/search/search_engine.cpp:264-365`
- `src/search/search_engine.cpp:3377-3545`
- `src/vector/compressed_ann.cpp:186-253`
- `src/vector/vector_database.cpp:1190-1200`
- `include/yams/search/search_engine.h:110-121`
- `include/yams/search/search_engine.h:277-301`
- `include/yams/search/search_engine.h:357-381`
- `include/yams/vector/vector_database.h:19-23`
