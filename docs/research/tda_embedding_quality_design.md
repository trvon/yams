# TDA / Persistent-Homology design note — "Persistence" embedding-quality metric for YAMS

**Source:** Topological Metric for Unsupervised Embedding Quality Evaluation (Shestov et al., arXiv 2512.15285, Dec 2025)
**Context:** Phase H Track H-TDA — use mathematical topology (persistent homology) as a quality signal for YAMS' encoder fixtures and clustering outputs.
**Predecessors:** `phase_g_topology_tuner.md`, `project_sgc_hdbscan_verdict` (memory). The Phase G intrinsic reward (singleton/giant/gini/intraEdge) is a *geometric* quality proxy. The paper claims a *topological* metric with better correlation to downstream tasks.
**Status:** research / design proposal. Implementation blocked on whether the paper's claims generalize to 1024-dim text embeddings on scifact-scale corpora (the paper doesn't test this).

---

## What the Persistence metric actually is

**Input:** point cloud `X_n = {x_1, …, x_n} ⊂ ℝ^d`. For YAMS, this would be a sample of the 1024-dim simeon embeddings (fwht projected, L2-normalized).

**Output:** single scalar ∈ [0, 1] (normalized by max pairwise distance).

**Formula:**
```
Persistence_k(X_n) = Σ_{(b_i, d_i) ∈ H_k} (d_i - b_i) / max_{i,j} ||x_i - x_j||
```

Sum over birth/death pairs in homology dimension k. Paper evaluates H_0 (connected components) and H_1 (1-dim holes / loops). Total persistence = Persistence_0 + Persistence_1.

**Filtration:** Vietoris-Rips complex. Grow radius `r` continuously; each simplex enters when all its vertex pairs are within `r`.

### Interpretation

- **Persistence_0** measures how connected the embedding space is — high when many 0-dim features (clusters) persist over long intervals, low when everything merges quickly.
- **Persistence_1** measures loop structure — high when the embedding manifold has holes that persist (meaningful topology), low when the space is simply-connected (collapsed or uniformly dense).

A good embedding, per the paper, has **high total Persistence**: meaningful structure at both short-range (H_0) and mid-range (H_1) scales.

---

## Key empirical results

**Evaluated domains (NOT retrieval):**
- Financial analytics: gender / age prediction on bank customer embeddings
- User Behavioral Modeling: Synerise dataset (RecSys Challenge 2025)
- Collaborative filtering: MovieLens-20M item embeddings

**Correlation with downstream task performance (Spearman):**

| Task | Spearman ρ | Rank vs. baselines |
|---|---|---|
| UBM (Synerise) | **0.840** | #1 (next: NESum at 0.303) |
| Collaborative filtering (MovieLens-20M, items) | **0.792** | #1 |
| Financial analytics | 0.560 | top-2 |
| Epoch selection (financial) | 0.609 | top-2 |

**Baselines beaten:**
- Spectral: RankMe, α-ReQ, NESum, μ_0-incoherence, StableRank, P-C number
- Clustering-based: SelfCluster

### Critical caveat

**No retrieval benchmarks.** All three evaluation domains are classification / CF / prediction, not nDCG-style retrieval. The paper's strongest claim ("top-tier correlation with downstream performance") is unproven for text retrieval.

---

## Complexity and engineering reality

**The paper gives no Big-O analysis.** Standard Vietoris-Rips persistent homology complexity:
- H_0: O(n² log n) (edge sorting dominates, as the simplicial complex reduces to union-find on MST).
- H_1: Up to O(n³) worst case, but practical implementations (Ripser, Giotto-TDA) use sparse filtrations and typically run in sub-cubic on low-dimensional data.

**For YAMS:**
- Scifact fixture: n = 5183 docs, d = 1024 dims.
- H_0 compute: ~10–100ms with Ripser.
- H_1 compute: seconds to minutes in practice; would need subsampling.

**Subsampling:** paper provides no guidance. Standard practice in TDA: uniform random subsample to n ≈ 500–1000 before Rips. Persistence metric is approximately stable under subsampling (empirical observation, not proven in this paper).

### C++ integration reality check

**No existing C++ dep in YAMS for PH.** Options:
1. **Ripser C++** ([github.com/Ripser/ripser](https://github.com/Ripser/ripser)) — single header, Apache 2.0 license, fast. Vendor as `third_party/ripser` (~2k LOC).
2. **GUDHI** — heavy dep, Boost-heavy, more features than needed.
3. **Our own H_0-only implementation** — connected-components persistence reduces to union-find on MST edges. ~100 LOC. Skip H_1 entirely.

Recommendation for YAMS: **option 3 (H_0 only)** for a first experiment. Union-find + sort edges by Rips radius → record merge events. The paper's best results are on H_0 anyway; H_1 is a nice-to-have that doubles engineering cost.

---

## YAMS integration — three candidate use cases

### Use case A — Encoder-fixture quality gate (lowest risk)

Compute Persistence_0 on a sample of the fixture's embeddings. Store in fixture metadata.

**Where it lives:** persistent-corpus bench harness. Computed once per fixture build.

**Value:** detect when a new encoder version produces degenerate embeddings (e.g., all collapsed to a single cluster) before running full nDCG benchmarks.

**Reuse:** `simeon::Encoder` (existing). New file `include/yams/search/topological_quality.{h,cpp}` with `computePersistenceH0(spanOfEmbeddings) → double`.

**Cost:** one-time at fixture build. Negligible per-query.

### Use case B — Topology tuner shadow-extrinsic signal (medium risk)

Wire Persistence_0 computed post-rebuild as the EXTRINSIC signal fed into `TopologyTuner::observeShadowExtrinsic()`. The existing shadow-divergence detector compares it against the intrinsic geometric reward.

**Where it lives:** post-rebuild hook in `TopologyManager::rebuildArtifacts` — already set up for this exact pattern in Phase G.

**Value:** detect when the tuner's intrinsic reward (singleton/giant/gini) diverges from the topological quality signal. If intrinsic says "good cluster shapes" but Persistence says "collapsed manifold," surface a warning.

**Reuse:** the Phase G `observeShadowExtrinsic` plumbing is already in place (`divergenceWarningCount()` accessor + `kDivergenceWindow = 3` + threshold).

**Cost:** one Persistence computation per rebuild (minutes, seconds). Cooldown gate already limits rebuild frequency to ≥10 min.

### Use case C — Arm selection feature (high risk)

Feed Persistence as a feature into TunerMAB's context, making arm selection aware of topological quality per corpus.

**Problem:** this crosses from "observability" to "control loop." Risk of amplifying whatever bias Persistence has for the specific embedding distributions YAMS uses. Not enough empirical backing from the paper.

**Recommendation:** defer until Use Case B validates that Persistence correlates with retrieval nDCG on scifact. If no correlation (paper's claim doesn't generalize), don't bother.

---

## Concrete YAMS design — start with Use Case A + B

### New files

- `include/yams/search/topological_quality.h` — declaration of `computePersistenceH0` free function + minimal Metric struct.
- `src/search/topological_quality.cpp` — union-find implementation. H_0 only.
- `tests/unit/search/topological_quality_catch2_test.cpp` — 4 TEST_CASEs.

### Reuse

- No new dependencies. Union-find is ~100 LOC inline.
- Access embedding data via existing `vector::VectorDatabase::batchGetEmbeddings`.
- Subsampling: existing `std::mt19937` seeded deterministically from fixture hash.

### Algorithm sketch (H_0 only)

```cpp
// Input: N d-dim embeddings (L2-normalized), max subsample size M = 1024.
// Output: Persistence_0 scalar ∈ [0, 1].

1. Subsample N' = min(N, M) embeddings deterministically.
2. Compute all pairwise distances d_ij (N'² / 2 entries).
3. Sort edges by distance ascending.
4. Initialize union-find: each point is its own component.
5. max_dist = d_ij[-1] (largest pairwise distance)
6. total_persistence = 0
7. For each edge (i, j, d_ij) in sorted order:
     if find(i) != find(j):
         # Two components merge at radius d_ij.
         # The younger component "dies" here (standard PH convention).
         # Record the lifetime.
         birth = 0  # all H_0 features are born at r=0
         death = d_ij
         total_persistence += (death - birth) / max_dist
         union(i, j)
8. Return total_persistence
```

Note: the last merge produces the "essential" H_0 feature (infinite persistence); skip it or cap its lifetime at `max_dist`.

### Unit tests

1. **Deterministic on uniform grid** — 100 points on a 10×10 grid → expected Persistence_0 ≈ some known constant.
2. **Collapsed space** — all 100 points within ε of origin → Persistence_0 ≈ 0.
3. **Well-separated clusters** — two clusters of 50 each, widely separated → Persistence_0 is high.
4. **Subsampling stability** — compute on full 1000-point dataset and on 500-point subsample; delta ≤ 0.05.

### Integration point — shadow signal

In `TopologyManager::rebuildArtifacts`, after the tuner observes `RebuildStats`, compute Persistence_0 on a 1024-point sample of the post-rebuild embedding cluster and feed it to `observeShadowExtrinsic`:

```cpp
if (tuner_ && !tunerCurrentArmId_.empty()) {
    tuner_->observeRebuildStats(tunerCurrentArmId_, stats);
    auto sample = sampleEmbeddings(vectorDb, 1024);
    double persistence = computePersistenceH0(sample);
    tuner_->observeShadowExtrinsic(persistence);
    // Warn-on-divergence already wired by Phase G
}
```

### Config surface

```toml
[topology.tuner.shadow]
# Phase H-TDA: enable persistent-homology shadow signal
enabled = false
sample_size = 1024
# Rebuilds happen ~10 min apart per Phase G cooldown; PH at 1024 points ~ 50ms
```

---

## Expected impact

**Optimistic:** Persistence_0 tracks retrieval-relevant embedding quality. On scifact it catches the "HDBSCAN collapses to 1 cluster" failure mode from a *topological* angle that the geometric intrinsic reward missed. Bandit is steered away from SGC+hops≥1 arms faster.

**Realistic:** Persistence_0 correlates weakly with nDCG on text embeddings (paper's best results are on tabular data). The shadow-divergence detector fires occasionally but doesn't consistently identify the right arms to avoid.

**Pessimistic:** Persistence_0 on 1024-dim L2-normalized text embeddings is dominated by the unit sphere's geometry, not the semantic structure. No useful signal. Paper's claims don't generalize.

**Bench gate:** run the Phase G persistence bench with H-TDA use case B enabled. Measure correlation (Spearman) between Persistence_0 post-rebuild and the fused nDCG of that spawn. If ρ > 0.3 across a few dozen rebuilds, keep. If ρ ≈ 0, discard.

---

## Known risks

1. **No published evidence that Persistence works on text retrieval.** All paper benchmarks are tabular / CF / prediction. Mitigation: treat the bench gate as primary evidence; discard if ρ < 0.3.

2. **Subsampling introduces noise.** 1024 from 5183 samples ≈ 20% of the corpus. Persistence_0 estimates will vary run-to-run. Mitigation: deterministic seed derived from fixture hash; record the sample set in state persistence.

3. **L2-normalized embeddings live on a unit sphere.** All pairwise distances bounded in [0, 2]. Compresses the persistence spectrum. Mitigation: normalize differently (cosine-distance as filtration value) or don't normalize before the metric. Paper doesn't address this.

4. **Adds a dependency even without third_party libs.** Union-find + pairwise distance matrix on 1024 points = 1024² / 2 = ~500k floats = 2MB. Allocated per-rebuild; reusable scratch.

5. **YAMS has no prior TDA infrastructure.** First PH code in the codebase. Testing burden lives entirely on the new unit tests.

---

## Execution plan (if approved)

**Phase H-TDA.1 — H_0 only, fixture-quality gate (Use Case A)**
- Implement `computePersistenceH0` + 4 unit tests.
- Add one-shot CLI tool: `yams debug persistence --fixture <path>` that prints the metric.
- Measure: Persistence_0 on scifact_simeon fixture. Store as fixture-build metadata.
- Gate: if the metric is 0 or NaN on a well-formed fixture, algorithm is broken; debug before proceeding.

**Phase H-TDA.2 — Shadow-extrinsic wiring (Use Case B)**
- Wire `computePersistenceH0` → `TopologyTuner::observeShadowExtrinsic` in `TopologyManager`.
- Run Phase G persistence bench (3 reps × 2 cells + tuner on). Record per-spawn Persistence values.
- Analysis: Spearman correlation between Persistence_0 and fused nDCG across the 6 spawns.

**Phase H-TDA.3 — Decision**
- Correlation ρ > 0.5 → ship Use Case B as opt-in default-off. Consider Use Case C.
- 0.2 < ρ < 0.5 → keep as bench observability only; don't ship.
- ρ < 0.2 → discard. Document the negative result.

---

## Recommendation

**Implement H-TDA.1 (fixture-quality gate) only, first.** It's ~150 LOC + unit tests, zero integration risk, produces a useful observability tool even if the paper's claims don't generalize. The CLI tool has standalone value: operators can measure their corpus's topological structure without touching the bench.

**Defer H-TDA.2 (shadow signal) until H-TDA.1 shows Persistence computes correctly + produces non-degenerate values on real fixtures.** If H_0 on scifact comes out as exactly 0 or exactly 1, something is wrong with the normalization and H-TDA.2 would just feed garbage into the tuner.

**Skip Use Case C entirely.** Not enough empirical backing from the paper to justify feeding PH into arm selection.

**Hard gate:** if the correlation in H-TDA.2 (when run) is below 0.3, this entire track is a dead end on text retrieval. Document the null result and move on to Track H-A (Dense PRF) which has stronger published evidence.

---

## Comparison to Track H-C

| | H-C (GraphRAG) | H-TDA (Persistence) |
|---|---|---|
| **Paper strength** | Extensive WebQSP/CWQ benchmarks; BubbleRAG/LEGO/GRAG all consistent | Paper is new (Dec 2025); only non-text benchmarks |
| **Maps to YAMS** | Directly — KG + subgraph retrieval | Indirectly — embedding-quality metric |
| **LOC estimate** | 1500 (SE + PR + fusion wiring) | 150 (H_0 only) + optional wire-up |
| **Blast radius** | Medium (new retrieval pool) | Low (observability) |
| **Expected nDCG lift** | +0.05–0.15 if scifact's KG is dense | 0 direct; indirect via better tuner decisions |
| **First gate decision** | Does YAMS have enough KG density? | Does Persistence correlate with nDCG on text? |

**Relative verdict:** H-C has higher ceiling and stronger published evidence; H-TDA is cheaper and adds permanent observability infrastructure. They are complementary — H-TDA can be implemented in parallel with H-C Phase H-C.1 without conflict.
