# ADR-0003: Fusion Strategy Analysis and Simplification

**Status**: Draft
**Date**: 2026-01-16
**Epic**: yams-eg1l
**Task**: yams-7nib

## Context

YAMS search engine currently implements 8 fusion strategies to merge results from 7 parallel components (FTS5, Vector, EntityVector, KG, PathTree, Tag, Metadata). This complexity causes:

1. **Quality regression**: DENSE_FIRST prioritizes weak vector results (0.02-0.05 scores), causing hybrid MRR=0.002 vs keyword MRR=0.084
2. **Maintenance burden**: ~600 lines of fusion code with significant duplication
3. **Configuration confusion**: Users must understand 8 strategies to tune search

## Current Strategies

### Simple Strategies (use `fuseSinglePass`)

| Strategy | Formula | Lines | Usage |
|----------|---------|-------|-------|
| WEIGHTED_SUM | `score * weight` | 4 | Unused |
| RECIPROCAL_RANK | `1 / (k + rank)` | 5 | Default (builder) |
| BORDA_COUNT | `weight * (100 - rank)` | 4 | Unused |
| WEIGHTED_RECIPROCAL | `weight * 1/(k+rank) * (1+score)` | 12 | Unused |
| COMB_MNZ | `weight * 1/(k+rank) * numComponents` | 16 | Default (tuner) |

### Complex Strategies (custom loops)

| Strategy | Anchor | Threshold | Extra Boost | Lines | Usage |
|----------|--------|-----------|-------------|-------|-------|
| TEXT_ANCHOR | text components | vector > 0.85 | 0.15 * vectorScore | 120 | Unused |
| DENSE_FIRST | vector components | text rank < 20 | text boost | 140 | SCIENTIFIC corpus |
| CONCEPT_ANCHOR | vector components | text rank < 20 | concept match | 200 | Experimental |

## Analysis

### Redundancy Matrix

```
                    Uses        Uses        Uses         Anchor    Code
Strategy            RRF         Weight      Score Boost  Logic     Duplication
─────────────────────────────────────────────────────────────────────────────
WEIGHTED_SUM        ✗           ✓           ✗            ✗         Base
RECIPROCAL_RANK     ✓           ✗           ✗            ✗         ~WEIGHTED_SUM
BORDA_COUNT         ✗           ✓           ✗            ✗         ~WEIGHTED_SUM
WEIGHTED_RECIPROCAL ✓           ✓           ✓            ✗         Superset
COMB_MNZ            ✓           ✓           ✗            ✗         +MNZ boost
TEXT_ANCHOR         ✓           ✓           ✓            text      Custom loop
DENSE_FIRST         ✓           ✓           ✓            vector    Custom loop
CONCEPT_ANCHOR      ✓           ✓           ✓            vector    ~DENSE_FIRST
```

### Key Findings

1. **All simple strategies are mathematically similar**: They differ only in the scoring function `f(rank, score)`.

2. **TEXT_ANCHOR and DENSE_FIRST are inverses**: Same code structure, opposite anchor logic.

3. **CONCEPT_ANCHOR duplicates DENSE_FIRST**: ~80% code duplication, adds concept boost.

4. **Root cause of quality regression**: DENSE_FIRST anchors on vector results. With weak embeddings (general-purpose all-MiniLM-L6-v2), this returns wrong documents.

5. **Component weights already provide tuning**: The weights (textWeight=0.45, vectorWeight=0.15, etc.) control importance. Anchor logic adds unnecessary complexity.

### Benchmark Evidence

**SciFact dataset comparison** (50 queries, all-MiniLM-L6-v2 embeddings):

| Strategy | Hybrid MRR | Keyword MRR | Delta | Improvement |
|----------|------------|-------------|-------|-------------|
| DENSE_FIRST | 0.0020 | 0.0839 | -0.0819 | (baseline) |
| TEXT_ANCHOR | 0.2402 | 0.8233 | -0.5832 | **120x** vs DENSE_FIRST |

**Full TEXT_ANCHOR results**:
| Metric | Hybrid | Keyword | Delta |
|--------|--------|---------|-------|
| MRR | 0.2402 | 0.8233 | -0.5832 |
| Recall@10 | 0.8200 | 0.8800 | -0.0600 |
| Precision@10 | 0.0840 | 0.0900 | -0.0060 |
| nDCG@10 | 0.3766 | 0.8376 | -0.4610 |
| MAP | 0.2417 | 0.8233 | -0.5816 |

**Key observations**:
1. TEXT_ANCHOR is **120x better** than DENSE_FIRST for hybrid search
2. Keyword-only is still **3.4x better** than best hybrid strategy
3. Recall is comparable (0.82 vs 0.88), but ranking quality differs significantly
4. With weak vector scores (0.02-0.05), anchor-based strategies cannot recover

## Proposed Simplification

### Unified Fusion Algorithm

Replace all 8 strategies with one configurable algorithm:

```cpp
struct UnifiedFusionConfig {
    // Scoring parameters (always use weighted RRF with score boost)
    float rrfK = 60.0f;

    // Component weights (existing)
    float textWeight = 0.45f;
    float vectorWeight = 0.15f;
    // ... other weights

    // Optional: MNZ boost for multi-component matches
    bool enableMnzBoost = true;

    // Anchor mode removed - weights handle relative importance
};

double unifiedScore(const ComponentResult& comp, size_t componentCount) {
    double rrfScore = 1.0 / (rrfK + comp.rank);
    double scoreBoost = 1.0 + clamp(comp.score, 0.0, 1.0);
    double mnzBoost = enableMnzBoost ? componentCount : 1.0;
    return weight[comp.source] * rrfScore * scoreBoost * mnzBoost;
}
```

### Why This Works

1. **Weights control importance**: If text should dominate, set textWeight=0.70.
2. **MNZ naturally handles agreement**: Documents found by multiple components rank higher.
3. **Score boost rewards confidence**: High-scoring matches get boosted.
4. **No anchor threshold magic**: No 0.85 vector threshold, no "top 20" text fallback.

### Migration Path

| Current Strategy | Equivalent Unified Config |
|------------------|---------------------------|
| WEIGHTED_SUM | rrfK=∞ (disable RRF), no MNZ |
| RECIPROCAL_RANK | No weights, no MNZ |
| BORDA_COUNT | rrfK=∞, no MNZ |
| WEIGHTED_RECIPROCAL | Standard config, no MNZ |
| COMB_MNZ | Standard config |
| TEXT_ANCHOR | textWeight=0.70, vectorWeight=0.10 |
| DENSE_FIRST | textWeight=0.25, vectorWeight=0.70 |
| CONCEPT_ANCHOR | Separate concept boost phase |

## Recommendations

1. ~~**Immediate**: Benchmark TEXT_ANCHOR vs DENSE_FIRST on SciFact to confirm hypothesis~~ ✅ **Done** (yams-f5m2)
2. **Phase 1**: Implement unified fusion, deprecate simple strategies (yams-30a4)
3. **Phase 2**: Update SearchTuner to adjust weights instead of strategies (yams-xssv)
4. **Phase 3**: Remove deprecated strategies, simplify config (yams-wz5r)

**Conclusion**: TEXT_ANCHOR confirms hypothesis - anchor logic is not the solution. Even the best anchor strategy (TEXT_ANCHOR, 120x better than DENSE_FIRST) cannot match keyword-only quality. The unified weighted approach with component weights is the correct path forward.

## Code Impact

| Component | Current LOC | After | Reduction |
|-----------|-------------|-------|-----------|
| ResultFusion methods | ~600 | ~100 | 83% |
| FusionStrategy enum | 8 values | 0 | 100% |
| SearchTuner strategy selection | 30 | 0 | 100% |
| Test coverage | 8 strategies | 1 | 87% |

## Decision

Proceed with unified fusion implementation per this analysis.

## References

- Epic: yams-eg1l
- Benchmark debug log: `/tmp/bench_debug.jsonl`
- SciFact dataset: 5,183 docs, 300 queries
