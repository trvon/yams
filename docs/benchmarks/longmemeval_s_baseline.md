# LongMemEval_S Retrieval Quality Baseline

**Date:** 2026-04-08
**Dataset:** LongMemEval_S (ICLR 2025) — `xiaowu0162/longmemeval-cleaned`, split `longmemeval_s_cleaned`
**Conversion:** `scripts/prepare_longmemeval_s.py` (BEIR format)

## Dataset Statistics

| Stat | Value |
|------|-------|
| Sessions (corpus docs) | 940 |
| Queries evaluated | 470 (of 500; 30 abstention/false-premise filtered) |
| Qrels | 890 |
| Chunks (embedded) | 5045 |
| Avg chunks/doc | 5.37 |
| Vectors generated | 5985 |
| Embedding model | embeddinggemma-300m (768-dim) |

## Baseline Results

| Metric | Hybrid (text+vector) | Keyword-only (FTS5) | Delta |
|--------|---------------------|---------------------|-------|
| MRR | 0.4074 | 0.5026 | -0.0951 |
| Recall@10 | 0.6529 | 0.6202 | +0.0327 |
| NDCG@10 | 0.4181 | — | — |
| MAP | 0.3629 | 0.4893 | -0.1264 |
| Precision@10 | 0.1148 | 0.1481 | -0.0333 |

## Auto-Tuned Configuration

The benchmark auto-selected:
```
state=SCIENTIFIC
zoom=MAP
rrfK=12
textWeight=0.70
vectorWeight=0.25
fusion=WEIGHTED_RECIPROCAL
semanticRescueSlots=0
similarityThreshold=0.40
enableSubPhraseRescoring=true
rerankAnchoredMinRelativeScore=0.70
```

Reranker: `bge-reranker-base` (cross-encoder, CPU)

## Run Configuration

```bash
./setup.sh Debug --with-tests
meson compile -C build/debug retrieval_quality_bench

DYLD_INSERT_LIBRARIES=... \
YAMS_TEST_SAFE_SINGLE_INSTANCE=1 \
YAMS_BENCH_DATASET=longmemeval_s \
YAMS_BENCH_EMBED_MAX_WAIT=0 \
./build/debug/tests/benchmarks/retrieval_quality_bench
```

Total runtime: ~4 hours

## Root Cause Analysis

Vector search adds only +3.3% recall and MRR *regresses* by -0.095. Three compounding issues:

### 1. MAX-POOL Chunk Aggregation

Only the single highest-scoring chunk per document contributes to the vector score.
With 5.37 chunks/doc average, 80%+ of chunk-level
semantic signal is discarded. For multi-session conversational data where different
parts of a session may be relevant, this is the wrong trade-off.

### 2. ~5x Text Advantage in Fusion

- Component weights: text=0.70, vector=0.25 (2.8x ratio)
- Field-aware score scaling: text=1.00, vector=0.45 (2.2x additional ratio)
- Vector-only penalty: 0.8x multiplier for semantic-only matches
- Combined: text contributions are ~5x stronger than vector in fusion scoring

### 3. Semantic Rescue Disabled

SCIENTIFIC profile sets `semanticRescueSlots=0`. High-confidence vector matches
that fall below the fusion cutoff are never rescued into the final top-K.

## Reference Baselines

The LongMemEval paper (Table 3) reports on LongMemEval_M (larger variant):
- Best retrieval config (K=V+fact): Recall@10=0.784, NDCG@10=0.536
- Retrievers evaluated: BM25, Contriever, Stella V5 1.5B, GTE-Qwen2 7B-instruct
- No published retrieval baselines for LongMemEval_S specifically

## First Tuning Attempt (2026-04-08)

Changes applied:
- Chunk aggregation: MAX → SUM
- SCIENTIFIC profile: textWeight 0.70→0.60, vectorWeight 0.25→0.35
- Vector scoreScale: 0.45 → 0.75
- Semantic rescue: 2 slots, min score 0.65
- Rerank top-K: 5 → 12

| Metric | Baseline | Attempt 1 | Delta |
|--------|----------|-----------|-------|
| MRR | 0.407 | 0.414 | +0.007 |
| Recall@10 | 0.653 | 0.485 | **-0.168** |
| NDCG@10 | 0.418 | 0.387 | -0.031 |
| MAP | 0.363 | 0.397 | +0.034 |

**Result: Recall@10 regressed significantly.** Root causes:

### 4. Similarity Threshold Filters Most Vector Results

`similarityThreshold=0.40` but embeddinggemma-300m produces cosine similarities
of 0.25-0.55 on this dataset. Of 479 HNSW searches:
- 54 (11.3%) returned **zero** results (all 150 candidates filtered)
- Many others heavily filtered (e.g. returned=1/150, 7/150, 17/150)

Raising vector weight without lowering the threshold just weakened text scoring.

### 5. CorpusStats-Driven Graph Reranking Activated

The `473274de` commit's CorpusStats-driven tuning detected high "symbol density"
(9.05) in chat sessions (proper nouns = brand names, people, places) and enabled
graph reranking that was OFF in baseline:
- `kg_weight`: 0.00 → 0.14 (consumed weight from text/vector)
- `graph_rerank_weight`: 0.00 → 0.32
- `graph_community_weight`: 0.00 → 0.16

Auto-tuner also reduced textWeight from 0.60→0.51 and vectorWeight from 0.35→0.30
due to weight redistribution to graph features.

### Key Env-Vars for A/B Testing

```bash
YAMS_SEARCH_SIMILARITY_THRESHOLD=0.25   # let vector results through
YAMS_SEARCH_ENABLE_GRAPH_RERANK=false   # disable confounding graph rerank
YAMS_SEARCH_TEXT_WEIGHT=0.60            # override auto-tuner adjustment
YAMS_SEARCH_VECTOR_WEIGHT=0.35          # override auto-tuner adjustment
YAMS_SEARCH_CHUNK_AGGREGATION=sum       # SUM chunk aggregation
```

## Improvement Targets

- Recall@10: 0.653 -> 0.75+
- MRR: 0.407 -> 0.50+
