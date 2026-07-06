# Search Engine Lean Model — 2026-07-06

## What was modeled

New Lean module:

```text
formal/topology/Yams/Topology/SearchEngine.lean
```

The model abstracts the live C++ search architecture as:

```text
SearchConfig + ComponentCandidate[]
  -> collectCandidates   -- mode/config gates component sources
  -> fuse                -- combines candidate sources into result docs
  -> graphRerank         -- may alter scores, must not invent docs
  -> topK                -- user-visible response limit
  -> SearchResponse      -- skipped/failed/timedOut/degraded observability
```

The source set mirrors the major C++ fanout/rerank surfaces:

- text
- path
- vector
- entityVector
- knowledgeGraph
- tag
- metadata
- graphText
- graphVector

## What Lean now proves

Key theorems:

- `topK_respectsLimit`
  - visible results never exceed the user limit.
- `collectCandidates_enabled`
  - every collected candidate came from an enabled source.
- `collectCandidates_subset`
  - component gating cannot invent candidates.
- `fuse_noInvent`
  - fusion cannot invent document IDs.
- `graphRerank_noInvent`
  - graph rerank/boosting cannot invent document IDs.
- `keywordMode_noVectorCandidates`
  - keyword mode excludes vector/entity-vector/graph-vector candidates.
- `semanticMode_noLexicalOnlyCandidates`
  - semantic mode excludes lexical-only candidates.
- `disabledSource_isSkipped`
  - disabled/mode-skipped components are observable in `skipped`.
- `runSearch_degradedFlag`
  - degraded mode reflects failed or timed-out components.
- `runSearch_resultsComeFromEnabledCandidates`
  - every visible result came from an enabled input candidate.
- `runSearch_respectsLimit`
  - the whole pipeline obeys the user limit.

## Architecture pressure from the model

The useful architectural boundary is not “one large search function.” It is a staged pipeline with explicit contracts:

1. **Routing/gating stage**
   - decides which sources are allowed by mode and availability.
   - should emit skipped reasons.
2. **Component fanout stage**
   - produces typed candidates with source provenance.
3. **Fusion stage**
   - may merge and score, but must only emit docs present in candidates.
4. **Graph/rerank stage**
   - may reorder or boost, but must not add docs unless it is explicitly modeled as a candidate-generating stage.
5. **Top-k stage**
   - owns the user-visible limit.
6. **Observability stage**
   - reports skipped, failed, timed-out, contributing, and degraded status.

This suggests a cleaner C++ architecture:

```text
SearchRouter
  -> SourceGate
  -> CandidateFanout
  -> CandidateFusion
  -> RerankPipeline
  -> TopKLimiter
  -> SearchTrace/Observability
```

The current `SearchEngine::Impl::searchInternal` contains most of these concerns inline. Lean makes the desired seams explicit, so future optimization can move logic behind stage interfaces while checking the same invariants.

## Optimization guidance

Safe search optimizations should name which theorem they preserve:

- Candidate scheduling changes: preserve `collectCandidates_enabled` and skipped observability.
- Fusion rewrites: preserve `fuse_noInvent` and result equivalence/bounded degradation from `SearchImpact.lean`.
- Graph/rerank rewrites: preserve `graphRerank_noInvent` unless graph expansion is explicitly modeled as a candidate source.
- Limit/semantic rescue rewrites: preserve `topK_respectsLimit` and document any bounded-result degradation.
- Timeout/fallback changes: preserve `runSearch_degradedFlag` and explicit skipped/failed/timedOut reporting.

## Next refinement

This model is intentionally architecture-level. A deeper model can add:

- deduplication by document hash vs path
- score monotonicity or rank stability
- reciprocal-rank fusion semantics
- semantic rescue slots
- timeout-aware partial responses
- freshness/visibility link from `Storage.lean` and `Pipeline.lean` into `SearchEngine.lean`
