# Graph-aware retrieval design note — LEGO-GraphRAG mapped onto YAMS

**Source:** LEGO-GraphRAG: Modularizing Graph-based Retrieval-Augmented Generation for Design Space Exploration (Cao et al., arXiv 2411.05844, Nov 2024)
**Context:** Phase H Track H-C — give YAMS' knowledge-graph path the attention it has lacked.
**Predecessors:** `phase_g_topology_tuner.md`, `phase_g_persistence_bench.md`.
**Status:** research / design proposal. Implementation blocked on user approval of the recommended architecture at the end of this document.

---

## What LEGO-GraphRAG actually decomposes

**Two modules** (not three, as I initially assumed):

1. **Subgraph-Extraction (SE)** — given a query `q` and graph `G`, produce a small candidate subgraph `g_q`.
2. **Path-Retrieval (PR)** — given `g_q` and `q`, produce ranked candidate paths `𝒫_q` that are fed to an LLM.

Each module has two implementation families:

| Module | Structure-based | Semantic-augmented |
|---|---|---|
| SE | Random Walk, K-hop, **Personalized PageRank (PPR)** | EEM (BM25 / Sentence-Transformer / BGE-Reranker), LLM |
| PR | Enumerated (DFS/BFS), Shortest Path (Dijkstra) | OSAR (post-filter), ISAR (Beam + LLM) |

**Pruning strategies** inside SE: Node Pruning (NP), Edge Pruning (EP), Triple Pruning (TP). Applied after the semantic scoring step.

**Trade-off axes:** reasoning quality / runtime / token cost / GPU memory.

---

## Key empirical findings (that change the YAMS design)

1. **PPR is the runtime bottleneck.** On the paper's Freebase benchmark (100M nodes, 300M edges), PPR takes ~75s per query. This is the primary cost in GraphRAG runtime. ← YAMS's KG is 10⁴–10⁵ nodes, so PPR is cheap for us.

2. **LLMs in SE are actively harmful — they over-prune.** The paper explicitly recommends against using an LLM for subgraph-extraction. ← Aligns with YAMS' no-LLM-in-retrieval philosophy.

3. **EEM (ST / BGE) + OSAR is the empirical sweet spot.** Top Hits@1 on WebQSP: ~0.71 via `SE=PPR+EEM prune` + `PR=OSAR(ST rerank)`. LLM-ISAR costs 60–120s extra for marginal gain.

4. **If you can only afford one semantic module, prefer PR over SE.** "Prioritizing the path-retrieval module over subgraph-extraction is more effective." ← Concrete guidance for staged rollout.

5. **Structure-only methods degrade on multi-hop.** Semantic augmentation is required for multi-hop queries. ← YAMS's retrieval is single-hop-dominant today; this gates when graph-path retrieval adds value.

---

## Architecture mapping — YAMS today vs. LEGO recipe

### YAMS today

```
┌──────────────┐
│ SearchEngine │
│              │
│ Text (BM25)  │──┐
│ Vector (HNSW)│──┼──▶ ResultFusion (RRF/COMB_MNZ) ──▶ results
│ KG expansion │──┘    │
│  (boost only)│       │
└──────────────┘       │
                       │
┌────────────────┐     │
│ TopologyManager│     │
│  HDBSCAN / CC  │─────┘  (routing boost, not primary retrieval)
│  (Phase G MAB) │
└────────────────┘
```

- **`TopologyManager`** is structure-based SE (HDBSCAN / CC) but its output is cluster membership, not a subgraph.
- **`KG expansion`** (in `src/search/graph_expansion.cpp`) currently adds ~1-hop text-BM25-scored neighbors, used as a score boost — NOT as a separate candidate pool.
- **No PR module exists.** There is no path-ranking stage.

### YAMS + LEGO recipe (proposed)

```
┌──────────────┐
│ SearchEngine │
│              │
│ Text  ───────│─────────────────────┐
│ Vector ──────│─────────────────────┤
│              │                     │
│ ┌──SE──────┐ │                     │
│ │ PPR on   │ │    g_q              │
│ │  KG      │─┼───▶ ┌──PR────────┐ │
│ │ + EEM    │ │     │ Dijkstra + │ │
│ │  rerank  │ │     │ ST OSAR    │─┼──▶ ResultFusion (RRF+) ──▶ results
│ │ + NP/EP  │ │     │ rerank     │ │
│ └──────────┘ │     └────────────┘ │
│              │                     │
└──────────────┘                     │
                                     │
┌────────────────┐                   │
│ TopologyManager│                   │
│  (Phase G MAB) │────▶ cluster-boost (unchanged)
└────────────────┘                   │
```

**Fundamental shift:** the KG becomes a primary retrieval surface. Today it's a boost layer; after this change, it produces its own candidate pool that feeds fusion alongside text + vector + cluster-boost.

---

## Concrete YAMS design

### SE module: `SubgraphExtractor`

**File:** new `include/yams/search/subgraph_extractor.{h,cpp}` (live inside `yams_search` because it depends on KG store + embedding generator)

**Algorithm:** PPR on YAMS' `KnowledgeGraphStore`.
- Seeds = entity nodes mentioned in the query (YAMS already has `QueryConceptExtractor`).
- Damping factor α=0.15 (LEGO's default).
- Cap output to 1000 nodes (LEGO's default).
- Followed by EEM semantic rerank: Simeon encoder embeds the query + each node's canonical text, cosine-scores, keep top 64 nodes as `g_q`.

**Reuse:**
- `KnowledgeGraphStore::getNode` / `getNeighbors` (already cached, see Phase F's `neighbor_cache_capacity` knob)
- `QueryConceptExtractor` for entity seeds (`src/search/query_concept_extractor.cpp`)
- `simeon::Encoder` (via `SimeonEmbeddingBackend`) for EEM reranking — no new encoder needed

**Complexity on YAMS:** O(E) for PPR iterations (E = edges in KG ≈ 10⁵). Negligible (<10ms) vs. LEGO's 75s bottleneck on Freebase.

### PR module: `PathRetriever`

**File:** new `include/yams/search/path_retriever.{h,cpp}`

**Algorithm:** Shortest-path enumeration (Dijkstra from each pair of seed entities) followed by OSAR re-rank.
- Limit: path length ≤ 4 hops, top 32 candidate paths.
- OSAR: Simeon encoder embeds each path's concatenated node-text; cosine against query; keep top-K.

**Reuse:**
- Same `simeon::Encoder` instance for OSAR rerank (no new dependency)
- Existing path-enumeration primitives in `src/topology/topology_baseline.cpp` if any can be lifted

**Complexity:** O(B × L × E_subgraph) where B = beam/paths, L = max length, E_subgraph = edges in the PPR-pruned subgraph (≤ 1000 nodes). ~10ms.

### Fusion wiring

**File:** `src/search/search_result_fusion.cpp` and `SearchEngineConfig`

Add a fourth component to `ComponentResult::Source`: `Subgraph` (alongside existing `Text` / `Vector` / `Tag` etc.). The fusion pipeline already handles multi-component inputs; the new source slots in cleanly with its own weight.

Default weight for `subgraphWeight = 0.0` (opt-in via TOML `[search.subgraph].enabled = true`). When enabled, re-normalize fusion weights.

### Config surface

```toml
[search.subgraph]
enabled = false
# SE
ppr_damping = 0.15
ppr_iterations = 50
ppr_node_cap = 1000
se_rerank_top_k = 64
# PR
pr_max_path_length = 4
pr_enumeration_top_n = 32
pr_osar_rerank_top_k = 10
# Fusion
subgraph_weight = 0.10
```

### Phase G tuner integration

TopologyTuner's arm grid gains a **7th parameter axis**: `subgraph_enabled ∈ {false, true}`. Total arm grid blow-up: ≈74 arms (was 37). UCB1 handles this gracefully. Reward formula is unchanged — subgraph retrieval that improves nDCG will show up as higher intrinsic reward on the corpus tuner's `RewardSource::Labels` signal if labels are present.

---

## Expected impact

LEGO's WebQSP result: +0.20 Hits@1 when going from structure-only (Instance 1) to `PPR + EEM + OSAR` (Instance 2). On YAMS' scifact bench:

- **Optimistic (knowledge-graph is dense and relevant):** +0.10–0.15 nDCG. Would push the fixture from 0.404 baseline toward 0.55+ band.
- **Pessimistic (scifact's KG is sparse or query-KG alignment is weak):** +0.01–0.03 nDCG. Still positive but marginal.

Which case scifact falls into is the **primary open question this proposal can't answer without running the bench**.

---

## Known risks

1. **Scifact's KG may not be dense enough for PPR to matter.** The fixture has ~5183 docs; typical KG density is 3–10 entities per doc. PPR only helps when queries have entity seeds that exist in the KG. Mitigation: gate H-C rollout behind a corpus-stats check — if `kgEdgeDensity < threshold`, leave subgraph retrieval disabled.

2. **Phase G + Phase F history warns against adding components to fusion.** R2 (RRF) was refuted because widening the candidate pool under COMB_MNZ amplified noise. Adding subgraph as a fourth leg risks the same pattern. Mitigation: default `subgraphWeight = 0.10` is small; ramp up only if the bench shows it helps.

3. **OSAR rerank doubles Simeon encode cost.** Query embedding runs twice: once for vector retrieval, once for path rerank. Mitigation: cache the query embedding. Already done in `SearchEngine::Impl` for the text leg; extend to path rerank.

4. **LEGO's benchmark is WebQSP / KGQA with an LLM reader.** YAMS has no LLM in the retrieval path. Whether "Hits@1 on KGQA" translates to "nDCG@10 on scifact" is unvalidated. Mitigation: explicit bench gate — ship behind `enabled = false` until the scifact run proves the lift.

5. **`project_sgc_hdbscan_verdict` warning applies:** SGC smoothing improved cluster geometry but inverted retrieval on scifact. The same inversion risk applies here — graph topology may produce cleaner subgraphs that don't translate to retrieval quality. Mitigation: Phase G's shadow-divergence detector (`observeShadowExtrinsic`) already exists; wire it into the subgraph retrieval path's reward signal.

---

## Execution plan (if approved)

**Phase H-C.1 — PPR over YAMS KG (structure-only SE)**
- Implement `SubgraphExtractor::ppr()` without any semantic rerank.
- Unit test: PPR on a synthetic 100-node graph converges to the seed's 1-hop neighborhood with expected damping.
- Bench gate: scifact_simeon fixture, `subgraph_weight = 0` (dormant). Measure cost only. If PPR > 50ms per query on 5k-doc corpus, stop and diagnose.

**Phase H-C.2 — EEM rerank + NP pruning (semantic SE)**
- Add Simeon cosine rerank + node pruning.
- Unit test: rerank preserves query-relevant nodes; NP retains query-entity-connected nodes.
- Bench gate: `subgraph_weight = 0.05` (minimal). Compare nDCG to Phase G baseline. If regression > 0.01, revert.

**Phase H-C.3 — PR module (Dijkstra + OSAR)**
- Add path enumeration and OSAR rerank.
- Unit tests: path enumeration deterministic; OSAR scores monotonic in path-query cosine.
- Bench gate: `subgraph_weight = 0.10`. Full 3-rep XXL bench. F4-style pass-band decision:
  - nDCG ≥ 0.50 → big win, enable by default
  - 0.45–0.50 → meaningful win, opt-in default-off
  - 0.40–0.45 → marginal, keep as bench-only experiment
  - < 0.40 → regression, revert

**Phase H-C.4 — Snapshot + bandit arm**
- Write `docs/benchmarks/phase_h_c_subgraph_retrieval.md`.
- Add `subgraph_enabled ∈ {false, true}` to Phase G's TopologyTuner arm grid.

---

## Recommendation

**Implement H-C.1 (structure-only PPR) immediately** as a cheap experiment:
- One new file, ~300 lines.
- No fusion changes (dormant output, weight = 0).
- Purely a cost measurement to validate PPR is feasible on YAMS' KG.

**Implement H-C.2 + H-C.3 after H-C.1 cost is verified.** If PPR takes >50ms/query on scifact, abort — the paper's bottleneck warning applies. If <10ms, proceed with confidence.

**Defer H-C.4 until the bench gate in H-C.3 passes ≥ 0.45 band.** The Phase G tuner absorbs subgraph_enabled as a new arm only if subgraph retrieval empirically pays off.

**Hard gate:** if scifact's KG has `<1 edge per doc` on average (easy to check via `KnowledgeGraphStore::getStats`), skip H-C entirely. PPR is only useful on reasonably-connected graphs.
