# Topology Service Architecture

## Executive Summary

This note turns the topology-cluster-routing research direction into a concrete library and daemon
seam.

The design decision is:

- keep PH and Mapper out of the normal search hot path
- add a small topology library boundary under `include/yams/topology/`
- run topology computation in a cold-path `TopologyService`
- emit stable routing artifacts that existing search stages can consume cheaply

The new public API sketch is intentionally artifact-oriented rather than PH-specific.

- `include/yams/topology/topology_artifacts.h`
- `include/yams/topology/topology_engine.h`
- `include/yams/topology/topology_baseline.h`
- `include/yams/topology/topology_metadata_store.h`

Current prototype implementation:

- `src/topology/topology_baseline.cpp`
- `src/topology/topology_input_extractor.cpp`
- `src/topology/topology_metadata_store.cpp`
- `src/topology/topology_offline_analyzer.cpp`
- `ServiceManager::rebuildTopologyArtifacts(...)`
- `ServiceManager::requestTopologyRebuild(...)`
- `RepairService::rebuildTopologyArtifacts(...)`

## Goals

1. Create a stable seam for topology experiments without rewriting the current search engine.
2. Preserve the existing HNSW plus graph-rerank path as the default retrieval baseline.
3. Make topology output cheap to consume at query time.
4. Keep the implementation reversible if the research does not justify the maintenance cost.

## Non-Goals

1. Do not execute persistent homology or Mapper in the normal query loop.
2. Do not replace HNSW navigation or current graph rerank.
3. Do not commit to a single topology algorithm in the public interface.
4. Do not add user-facing flags until offline evaluation shows real retrieval value.

## Why The Interface Is Artifact-Oriented

The search layer does not need raw persistence diagrams or a live Mapper graph. It needs durable,
cheap artifacts such as:

- cluster assignments
- cluster hierarchy levels
- cluster medoids or representatives
- bridge and outlier annotations
- stability or persistence-like scores

That is why the new headers expose three responsibilities instead of a single algorithm type:

- `ITopologyEngine` builds or updates artifacts
- `ITopologyArtifactStore` persists and reloads them
- `ITopologyRouter` turns artifacts into candidate cluster routes for weak-query retrieval

## Proposed Public Surface

### `topology_artifacts.h`

This header defines the algorithm-agnostic data model.

- `TopologyDocumentInput`
  - minimal document input for a topology pass
  - contains document hash, path, optional embedding, known neighbors, and metadata
- `TopologyBuildConfig`
  - controls exact vs incremental vs approximate mode
  - controls overlap, rolling window size, and minimum cluster thresholds
- `DocumentClusterMembership`
  - per-document assignment and role (`Core`, `Bridge`, `Medoid`, `Outlier`)
- `ClusterArtifact`
  - stable cluster object with persistence, cohesion, medoid, and overlap data
- `TopologyArtifactBatch`
  - snapshot payload for persistence and search consumption
- `TopologyRouteRequest` and `ClusterRoute`
  - query-facing route hints for weak-query routing experiments

### `topology_engine.h`

This header defines the three core interfaces.

- `ITopologyEngine`
  - `buildArtifacts(...)`
  - `updateArtifacts(...)`
- `ITopologyArtifactStore`
  - `storeBatch(...)`
  - `loadLatest(...)`
  - `loadMemberships(...)`
- `ITopologyRouter`
  - `route(...)`

This separation matters because YAMS may want to:

- compute artifacts in one daemon component
- persist them via metadata or KG storage
- use them from search without depending on the compute implementation
- extract topology inputs from real metadata, KG, and vector stores without coupling the engine to
  daemon-only data plumbing

### `topology_input_extractor.h` and `topology_offline_analyzer.h`

These headers add the first real-store prototype path.

- `TopologyInputExtractor`
  - reads documents from `IMetadataRepository`
  - reads semantic-neighbor graph state from `KnowledgeGraphStore`
  - reads embeddings from `VectorDatabase`
  - produces `TopologyDocumentInput` records for offline analysis
- `TopologyOfflineAnalyzer`
  - runs extraction plus `ITopologyEngine::buildArtifacts(...)`
  - gives YAMS a cold-path analysis entrypoint without changing the query path

## Recommended Daemon Integration

### Cold-Path Ownership

The topology layer should run after the current semantic-neighbor graph exists.

Best integration points:

1. `EmbeddingService::updateSemanticNeighborGraph(...)`
2. `GraphComponent::maintainSemanticTopology(...)`
3. `ServiceManager` drain-time coordination

Relevant files:

- `src/daemon/components/EmbeddingService.cpp`
- `src/daemon/components/GraphComponent.cpp`
- `src/daemon/components/ServiceManager.cpp`

### Proposed `TopologyService`

Responsibilities:

1. collect topology input from semantic-neighbor edges or document embeddings
2. build or incrementally update topology artifacts
3. persist those artifacts through `ITopologyArtifactStore`
4. expose freshness and lag metrics to daemon status surfaces

Suggested initial behavior:

- trigger only after post-ingest drain or explicit maintenance windows
- compute over changed documents plus their neighborhood when possible
- fall back to larger snapshot recomputation when locality assumptions break

Current implementation status:

- incremental scheduling is wired from the post-ingest drain callback in `ServiceManager`
- the daemon now expands incremental rebuilds from changed documents to a local closure formed from:
  - prior cluster membership in the latest topology snapshot
  - current `semantic_neighbor` graph neighbors
- the current incremental path merges rebuilt cluster artifacts back into the stored snapshot rather
  than always replacing the full corpus snapshot
- explicit full rebuild is available via daemon repair orchestration with the new `repair`
  topology phase
- topology freshness and lag telemetry are exposed through `ServiceManager`, cached by
  `DaemonMetrics`, and surfaced through daemon status request counts and readiness states

## Recommended Search Integration

The first search use should be deliberately narrow.

### Phase 1: No Ranking Changes

- persist artifacts only
- expose diagnostics and inspection surfaces
- measure build cost, freshness lag, and artifact stability
- support daemon-owned rebuild entrypoints before any query-path use

### Phase 2: Weak-Query Routing Only

- use `ITopologyRouter` only when lexical evidence is weak
- route toward cluster medoids or stable cluster representatives
- preserve current text, vector, fusion, and graph-rerank logic after candidate narrowing

### Phase 3: Optional Rerank Prior

- test a small additive prior from cluster stability or bridge membership
- keep the current reciprocal-community signal as the canonical topology-like baseline
- require benchmark evidence before enabling any default behavior

## Storage Strategy

The interface intentionally does not hard-code storage, but the likely first choices are:

1. document metadata for per-document memberships and lightweight annotations
2. KG node properties for cluster ids and medoid / bridge flags
3. snapshot-style metadata record for `TopologyArtifactBatch`

The storage layer should preserve enough information to support:

- loading memberships for a rerank candidate window
- loading the latest artifact snapshot for diagnostics
- partial invalidation when a set of documents changes

## Algorithm Flexibility

The new interface does not force YAMS to choose PH or Mapper now.

Possible backends that fit the same boundary:

- reciprocal-community and connected-component baselines
- hierarchical clustering on the semantic-neighbor graph
- 0-dimensional persistence or persistence-like stability analysis
- Mapper-style overlapping cover graphs

The current prototype is the simplest one in that list: a connected-component baseline over the
provided topology document inputs, plus a metadata and KG-backed artifact store.

This is intentional. The boundary should survive algorithm changes.

## Validation Plan

1. Add the library boundary first.
2. Keep runtime behavior unchanged.
3. Implement a no-op or offline prototype behind the new seam.
4. Replace full-snapshot rebuilds with partial recomputation and merge semantics.
5. Measure artifact quality, freshness lag, and maintenance cost.
6. Only then test weak-query routing.

Success criteria for the architecture stage:

- the daemon has a clean cold-path ownership model for topology work
- search can consume topology artifacts without importing topology internals
- the interface is usable by both graph-first and embedding-first experiments

## Open Questions

1. Should topology artifacts live primarily in metadata or primarily in KG node properties?
2. Should the first artifact builder consume semantic-neighbor edges only, or also direct
   document embeddings?
3. What invalidation policy is acceptable before a full artifact recomputation is required?
4. Should the first routing experiment use medoids only, or allow overlap-aware bridge routing?

## Related Artifacts

- `docs/research/topology_cluster_routing.md`
- `docs/tasks/topology-cluster-routing-plan.md`
- `include/yams/topology/topology_artifacts.h`
- `include/yams/topology/topology_engine.h`
- `include/yams/topology/topology_baseline.h`
- `include/yams/topology/topology_input_extractor.h`
- `include/yams/topology/topology_offline_analyzer.h`
- `include/yams/topology/topology_metadata_store.h`
- `src/topology/topology_baseline.cpp`
- `src/topology/topology_input_extractor.cpp`
- `src/topology/topology_offline_analyzer.cpp`
- `src/topology/topology_metadata_store.cpp`
- `src/daemon/components/ServiceManager.cpp`
- `src/daemon/components/DaemonMetrics.cpp`
- `src/daemon/components/dispatcher/request_dispatcher_status.cpp`
- `src/daemon/components/RepairService.cpp`
- `src/cli/commands/repair_command.cpp`
