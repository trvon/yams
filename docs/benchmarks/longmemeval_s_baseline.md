# LongMemEval_S Retrieval Quality Baseline

**Date:** 2026-06-09
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

## Reference Baselines

The LongMemEval paper (Table 3) reports on LongMemEval_M (larger variant):
- Best retrieval config (K=V+fact): Recall@10=0.784, NDCG@10=0.536
- Retrievers evaluated: BM25, Contriever, Stella V5 1.5B, GTE-Qwen2 7B-instruct
- No published retrieval baselines for LongMemEval_S specifically
