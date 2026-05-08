# Topology Research — Paperbridge Query Queue

Consolidated arxiv / paperbridge search plan for filling the gaps
enumerated in `docs/benchmarks/topology_ablation_matrix.md`. One query
per row, mapped to a matrix axis, so each search returns evidence
against a concrete engineering question.

This doc **does not run paperbridge**. It hands the user a paste-ready
query list and the known priors so each run starts from the current
frontier instead of the first-page defaults.

Source material:
- `docs/research/search_topology_literature_review.md` (2026-04-15) —
  12 already-curated papers
- `docs/research/topology_cluster_routing.md` — design priors
- `docs/tasks/t7-dynamic-topology-maintenance-matrix.md:27–36` — arxiv
  links already cited against T7 work
- `docs/benchmarks/topology_ablation_matrix.md` — axis index

## Query queue

### Q1 — learned role-score weights (Matrix axis 2)

> **Search query:** "learned role-aware retrieval boost graph rerank medoid bridge centrality"
> **Alternate phrasings:** "community role embedding retrieval", "learnable cluster role score IR"

**Target axis:** Axis 2 — role-score weights (currently hardcoded
`+0.05` medoid, `+0.03` bridge at `src/search/search_engine.cpp:240,244`).

**Known priors (skip these in search results):**
- Wang et al. 2024, *Topology-Aware Retrieval* — [2405.17602](https://hf.co/papers/2405.17602) — already proposes learning role weights
- Fu et al. 2024, *AutoRAG-HP* — [2406.19251](https://hf.co/papers/2406.19251) — hierarchical MAB for RAG hyper-params

**Gap to close:** need a concrete bandit arm specification (reward
shaping, exploration schedule) before replacing the literals. Phase B
sweep will reveal whether a bandit is worth building or whether a static
grid converges.

### Q2 — corpus-adaptive reciprocal-community normalization (Literature gap #7)

> **Search query:** "community-size normalization retrieval cluster distribution global statistic"
> **Alternate phrasings:** "cluster-size prior dense retrieval", "global graph statistic rerank normalization"

**Target axis:** Axis 6 (indirectly — normalization is consumed in
fusion at `src/search/search_engine.cpp:519–521`).

**Known priors:**
- Peng et al. 2024, *GraphRAG Survey* — [2408.08921](https://hf.co/papers/2408.08921) — taxonomy; check §3–§4 for normalization discussion

**Gap to close:** literature review flagged this as gap #7. Current
normalization is candidate-set-adaptive (divides by candidate count);
need evidence that corpus-level cluster-size distribution improves
score-comparability across queries.

### Q3 — alternative community-detection for dense retrieval (Matrix axis 6)

> **Search query:** "Louvain Leiden label propagation dense retrieval reranking embedding graph"
> **Alternate phrasings:** "hierarchical community detection RAG candidate generation", "modularity-based retrieval clustering benchmark"

**Target axis:** Axis 6 — community-detection algorithm (one impl today:
`ConnectedComponentTopologyEngine`).

**Known priors:**
- Chang & Zhang 2024, *CommunityKG-RAG* — [2408.08535](https://hf.co/papers/2408.08535) — validates community direction
- Dong et al. 2025, *Youtu-GraphRAG* — [2508.19855](https://hf.co/papers/2508.19855) — "dually-perceived" hierarchical detection
- Cao et al. 2024, *LEGO-GraphRAG* — [2411.05844](https://hf.co/papers/2411.05844) — modular subgraph-extraction contract

**Gap to close:** head-to-head benchmark evidence of
connected-components vs Louvain vs label-propagation for **retrieval
recall**, not just graph-quality metrics (modularity, conductance). We
need to know if the algorithm choice measurably changes nDCG before
building the second engine.

### Q4 — dynamic maintenance under continuous ingest (Matrix axis 4)

> **Search query:** "incremental graph clustering streaming update latency dense retrieval"
> **Alternate phrasings:** "dynamic community maintenance online inserts deletes ANN", "dirty-region rebuild incremental topology"

**Target axis:** Axis 4 — maintenance dynamics (dirty-region, coalesced
rebuild).

**Known priors:**
- Singh et al. 2021, *FreshDiskANN* — [2105.09613](https://hf.co/papers/2105.09613) — graph-based ANN with real-time inserts/deletes
- Li et al. 2025, *Revisiting Dynamic Graph Clustering via Matrix Factorization* — [2502.06117](https://hf.co/papers/2502.06117) — temporal separated MF + selective updates
- Rucci et al. 2025, *Decentralized and Self-adaptive Core Maintenance* — [2510.00758](https://hf.co/papers/2510.00758) — incremental coreness
- Baranchuk et al. 2019, *Learning to Route in Similarity Graphs* — [1905.10987](https://hf.co/papers/1905.10987) — learned router, see also T7.3

**Gap to close:** the T7 maintenance gates
(`docs/tasks/t7-dynamic-topology-maintenance-matrix.md`) specify
0-fallback-to-full-rebuild and bounded region expansion. Need
published workload evidence (not synthetic) that these targets are
achievable with connected-components + dirty-region under bursty
ingest. Current code has the dirty-region path
(`include/yams/topology/topology_artifacts.h:58–74`) but the paper-grade
validation is open.

### Q5 — fusion form (Matrix axis 7)

> **Search query:** "convex combination fusion retrieval sample efficiency RRF comparison"
> **Alternate phrasings:** "learned fusion weights hybrid retrieval", "LambdaMART hybrid ranking"

**Target axis:** Axis 7 — RRF fusion weights.

**Known priors:**
- Bruch, Gai, Ingber 2022, *An Analysis of Fusion Functions for Hybrid Retrieval* — [2210.11934](https://hf.co/papers/2210.11934) — convex-combination vs RRF
- Anantha et al. 2023, *Context Tuning for RAG* — [2312.05708](https://hf.co/papers/2312.05708)
- Fu et al. 2024, *AutoRAG-HP* — [2406.19251](https://hf.co/papers/2406.19251) — MAB tuning

**Gap to close:** decide when (not if) to switch from weighted-RRF to
convex-combination. Bruch 2022 is the headline; need follow-up
replications on heterogeneous corpora comparable to YAMS's code+prose
mix.

### Q6 — listwise rerank over large candidate sets (follow-up)

> **Search query:** "listwise rerank single-pass implicit pairwise candidate budget"
> **Alternate phrasings:** "JointRank listwise LLM reranker scalability", "pointwise vs listwise retrieval rerank budget"

**Target axis:** downstream of Axis 5 — relevant when Phase B shows KG
rerank alone is insufficient at wide candidate budgets.

**Known priors:**
- Dedov 2025, *JointRank: Rank Large Set with Single Pass* — [2506.22262](https://hf.co/papers/2506.22262)

**Gap to close:** if Axis 1 wide configs produce too many candidates
for `KGScorer` to handle in-budget, a listwise-pass replacement is the
natural follow-up. Need latency numbers at YAMS-scale candidate sets
(100–400 docs).

### Q7 — role-role routing: medoid vs bridge as distinct signals (Matrix axis 2, deeper)

> **Search query:** "medoid bridge distinct retrieval signal RAG candidate role-separated"
> **Alternate phrasings:** "structural vs semantic signal decomposition RAG", "bridge node retrieval expansion"

**Target axis:** Axis 2 (deeper cut).

**Known priors:**
- Wang et al. 2024, *Topology-Aware Retrieval* — already cited
- CommunityKG-RAG — already cited

**Gap to close:** the current code collapses medoid and bridge into
additive score adjustments. Separating them as independently-weighted
signal channels in fusion (rather than score bumps) is a larger
refactor; need evidence it's worth it.

## Quick reference — already-cited papers

| arxiv ID | Title | Primary axis |
|---|---|---|
| [1905.10987](https://hf.co/papers/1905.10987) | Learning to Route in Similarity Graphs | Axis 4 / routing |
| [2105.09613](https://hf.co/papers/2105.09613) | FreshDiskANN | Axis 4 |
| [2210.11934](https://hf.co/papers/2210.11934) | Fusion Functions for Hybrid Retrieval | Axis 7 |
| [2301.13187](https://hf.co/papers/2301.13187) | *(T7-cited, title not in lit review)* | Axis 4 |
| [2312.05708](https://hf.co/papers/2312.05708) | Context Tuning for RAG | Axis 7 |
| [2405.17602](https://hf.co/papers/2405.17602) | Topology-Aware Retrieval | Axis 2 |
| [2406.19251](https://hf.co/papers/2406.19251) | AutoRAG-HP | Axis 2, 7 |
| [2408.08535](https://hf.co/papers/2408.08535) | CommunityKG-RAG | Axis 6 |
| [2408.08921](https://hf.co/papers/2408.08921) | GraphRAG Survey | Axes 2,6,7 |
| [2411.05844](https://hf.co/papers/2411.05844) | LEGO-GraphRAG | Axis 6 |
| [2502.06117](https://hf.co/papers/2502.06117) | Dynamic Graph Clustering via MF | Axis 4 |
| [2506.22262](https://hf.co/papers/2506.22262) | JointRank | follow-up |
| [2508.19855](https://hf.co/papers/2508.19855) | Youtu-GraphRAG | Axis 6 |
| [2510.00758](https://hf.co/papers/2510.00758) | Decentralized Core Maintenance | Axis 4 |

## Workflow

1. Run paperbridge / Zotero / arxiv-search for each Q above, skipping
   the known-priors list.
2. Record hits back into `docs/research/search_topology_literature_review.md`
   under the appropriate section (topology-and-community, adaptive
   tuning, incremental maintenance, reranking).
3. Update the `Mapping: gaps → initiatives → literature` table at
   `docs/research/search_topology_literature_review.md:110–119` when a
   new paper supersedes a known prior.
4. If a hit directly motivates a code change, open a row in
   `docs/tasks/retrieval_improvement_matrix.md`.

## Non-goals

- Not a commitment to implement any of the follow-ups. These queries
  feed decision-quality, not a build plan.
- Not a replacement for the existing literature review — this doc
  queues searches; the lit review consumes their results.
- Not a stand-in for Phase B numbers. Evidence from our own bench runs
  has higher weight than paper claims.
