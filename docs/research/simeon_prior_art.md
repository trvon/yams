# Prior Art Review: Model-Free SIMD Embeddings (simeon)

**Status:** Phase 0 of the simeon experiment. Internal technical memo. Drafted 2026-04-18.
**Scope:** Survey the literature that already covers training-free hashing embeddings for retrieval, and determine whether our planned simeon library (char n-gram → hash sketch → (optional) random projection → SIMD similarity) would rediscover known results. Pin down what is already known so that the simeon contribution can be framed honestly.

## TL;DR

A paper published three months before we started planning — **NUMEN** (Sharma, arXiv:2601.15205, January 2026) — already demonstrates that a deterministic character n-gram + CRC32 hash, mapped directly into a very high-dimensional dense vector (8,192 / 16,384 / 32,768), beats BM25 on the LIMIT benchmark with zero training. The method we were about to invent exists.

What does **not** exist in the literature we can find:

1. A **production-quality, vendorable C++ implementation** with runtime-dispatched NEON/AVX2 kernels. NUMEN is a single Jupyter notebook on top of NumPy, Apache + Commons Clause licensed (non-commercial). No SIMD, no C++ bindings, no integration into a real retrieval engine.
2. An **ablation on a projection head** that maps the NUMEN-style very-high-dim sketch down to an HNSW-friendly low-dim vector (e.g., Achlioptas sparse random projection 32k → 384/512/768). NUMEN does not study this; at 32,768d the vector is memory-hostile and HNSW is impractical at scale.
3. **Throughput / ingestion-cost evaluation** against learned embeddings on real corpora. NUMEN reports retrieval quality only; the engineering case for training-free embeddings — the throughput-per-watt argument yams cares about — is unquantified.
4. **Integration with topology-aware retrieval** (yams' routing engine) or with late-fusion BM25 at scale. NUMEN stands alone; hybrid performance is open.

This reshapes simeon's positioning from "novel algorithm" to **"engineering + ablation contribution on top of a known training-free method,"** which is defensible and aligned with yams' product needs.

## Survey

### Training-free dense retrieval

- **NUMEN — Beyond the Geometric Curse: High-Dimensional N-Gram Hashing for Dense Retrieval** (Sharma, 2026, arXiv:2601.15205). Character 3/4/5-grams, CRC32 hash, index directly into a fixed-width vector of dim 8k/16k/32k. Achieves 93.90 % Recall@100 on LIMIT at 32,768d — first dense method to exceed BM25 (93.6 %). No training, no learned weights. Python notebook implementation. Non-commercial license.
- **On the Theoretical Limitations of Embedding-Based Retrieval** (Weller, Boratko, Naim, Lee et al., arXiv:2508.21038, August 2025). Proves that the number of top-k document subsets a fixed-dim embedding space can represent is upper-bounded by the dimension itself. Introduces the LIMIT benchmark. BM25 is not subject to the bound because it effectively lives in an unbounded sparse space. This paper is the theoretical motivation NUMEN leans on and is the reason "go high-dim" beats "go learned" on LIMIT.
- **Semi-Parametric Retrieval via Binary Token Index (SVDR)** (Zhou, Dong, Wei, Chen, arXiv:2405.01924). Hybrid that combines dense embeddings with a binary token index. Related in spirit; still requires a trained dense component.

### Classical building blocks

- **Feature hashing** (Weinberger, Dasgupta, Langford, Smola, Attenberg, ICML 2009). The hashing trick with a signed hash function preserves inner products in expectation and is the foundation of any "map arbitrary tokens into fixed-width vectors" approach. Widely understood; quality does not suffer at dim ≥ 10⁴ in text classification.
- **SimHash** (Charikar, STOC 2002). Hashes feature vectors to binary signatures so Hamming distance approximates cosine. A candidate if we want 256-bit sketches with popcount distances instead of float vectors.
- **MinHash / LSH** (Broder 1997; later work by Li, Shrivastava et al. on optimal densification, arXiv:1703.04664). Jaccard-similarity-preserving signatures. Relevant for set-of-tokens similarity rather than weighted bag-of-n-grams.
- **Database-friendly random projections** (Achlioptas, JCSS 2003). Projection matrix entries drawn from {−1, 0, +1} with probabilities {1/6, 2/3, 1/6}. Johnson–Lindenstrauss-style distance preservation at 3× the speed of dense Gaussian. This is the projection family simeon would use for the "high-dim sketch → HNSW-friendly dim" head.
- **Very sparse random projections** (Li, Hastie, Church, KDD 2006). Generalizes Achlioptas to arbitrarily sparse matrices with acceptable JL distortion.

### Hash-based compression, not hash-based embedding

Several 2023–2026 papers use hashing to compress *learned* embeddings rather than replace them. These are not the same problem simeon attacks, but the techniques may inform future simeon phases:

- **Injecting Domain Adaptation with Learning-to-hash for Effective and Efficient Zero-shot Dense Retrieval** (Thakur, Reimers, Lin, arXiv:2205.11498). Learning-to-hash applied to BEIR.
- **RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound for ANN Search** (Gao, Long, arXiv:2405.12497). Randomized binary quantization with SIMD-friendly distance estimation. Already referenced elsewhere in the yams tree (`third_party/sqlite-vec-cpp` uses a RaBitQ mode).
- **MUVERA: Multi-Vector Retrieval via Fixed Dimensional Encodings** (Dhulipala et al., arXiv:2405.19504). Random-projection-style aggregation of multi-vector (ColBERT-like) outputs into a single fixed-dim vector.
- **FastText.zip** (Joulin, Grave, Bojanowski, Douze, Jégou, Mikolov, arXiv:1612.03651). Product quantization of learned word embeddings. Prior art for the general "compress a vector for retrieval" family.

### SIMD / CPU-efficient retrieval engineering

- **Efficient Inverted Indexes for Approximate Retrieval over Learned Sparse Representations** (Bruch, Nardini, Rulli, Venturini, arXiv:2404.18812 — the Seismic system). Production-quality inverted-index engineering for learned sparse embeddings; documents the same "engineering to close the latency gap with BM25" story we want to tell on the dense side.
- **Co-design Hardware and Algorithm for Vector Search / FANNS** (Jiang et al., arXiv:2306.11182). Hardware-side precedent for taking vector search seriously as an engineering problem.
- **LightRetriever** (Ma et al., arXiv:2505.12260). LLM-based hybrid with lightweight query encoders achieving 1000× query-inference speedup. Different problem (still learned), but precedent for the "throughput as first-class metric" framing we want to adopt.

## Implications for simeon

Given what's already published, here is what simeon can and should claim:

### In scope for simeon (defensible contributions)

1. **A production C++ library**, vendorable, GPLv3-licensed, with runtime-dispatched NEON/AVX2/scalar kernels and standalone tests. NUMEN's notebook cannot be used in yams (license + not a library + no SIMD).
2. **An Achlioptas projection-head ablation** that maps the NUMEN-style very-high-dim hash sketch (up to 32,768) down to HNSW-friendly dims (128/256/384/512/768). The question "how much retrieval quality is lost when we JL-compress a training-free high-dim sketch to an HNSW-friendly dim?" appears not to be answered in the NUMEN paper or the DeepMind LIMIT paper. If the projected vector preserves meaningful recall, this is a genuinely novel practical result, because it makes the training-free method compatible with existing vector-store infrastructure.
3. **Real-corpus evaluation on yams-docs and paperbridge-sourced arXiv slices**, not only on LIMIT. LIMIT is a synthetic stress-test; real-corpus behavior under topology routing is open.
4. **Throughput numbers** (docs/sec end-to-end ingestion, tokens/sec, p50/p99 per-phase latency, RSS) comparing training-free SIMD against the daemon/ONNX MiniLM path. This is the practical case for simeon and is unquantified in prior work.
5. **BM25 late-fusion behavior.** Neither NUMEN nor the DeepMind paper reports how a training-free dense method composes with BM25 in a production hybrid. Given yams already exposes BM25, the fused score is a natural evaluation axis.
6. **Ablation over hash families** — CRC32 (NUMEN's choice) vs SplitMix64 vs xxHash64 vs polynomial rolling hash. NUMEN uses CRC32 without justification. If a cheaper/faster hash yields equivalent recall, that is a useful engineering finding.

### Out of scope / explicitly not claimed

1. **Algorithmic novelty of the core method.** "Training-free char n-gram hashing embedding for dense retrieval" is NUMEN's contribution. Simeon should cite NUMEN as the direct precedent and position itself as an engineering + ablation follow-up.
2. **Theoretical novelty on LIMIT.** DeepMind's paper is the canonical reference for why the method works at high dim.
3. **Universal retrieval SOTA.** Both NUMEN and the DeepMind LIMIT result are about a specific failure mode of learned low-dim embeddings. On standard BEIR tasks, learned models (E5, Arctic-Embed, Gecko) still win on semantic queries. Simeon must not overclaim.

## Open questions carried into implementation

- **Projection-head dim sweet spot.** If Achlioptas projection from a 32,768d NUMEN sketch to 512d retains most of NUMEN's LIMIT Recall@100, that is the simeon headline. Needs ablation.
- **Does topology routing (yams' cluster-based query expansion) interact with training-free embeddings differently than with learned embeddings?** If yes, that is a yams-specific paper in its own right.
- **Is the CRC32 choice in NUMEN load-bearing, or does any well-distributed 32/64-bit hash work?** Directly testable in simeon's ablation.
- **Quality–throughput frontier.** Simeon should produce a Pareto plot: retrieval quality (nDCG@10 or Recall@100) on the y-axis, ingestion throughput (docs/sec) on the x-axis, with points for {MiniLM-daemon, NUMEN-at-various-dims, simeon-with-projection-at-various-dims, BM25, simeon+BM25 hybrid}.

## Can simeon support reranking?

Short answer: **yes, as a first-stage or cheap late-rerank layer; not as a replacement for a learned cross-encoder.**

Because simeon produces deterministic L2-normalized vectors with the same encoder on both sides, cos(query, candidate) reduces to a plain dot product — cheap with or without SIMD, and HNSW-native since sqlite-vec stores unit vectors. Three deployment shapes are realistic:

1. **Late fusion with BM25 (recommended default).** `score = α · BM25 + (1−α) · cos(Eq, Ed)` over the BM25 top-K. This is the pattern the plan already commits to — simeon as a lexical-adjacent re-ranker, not a semantic one.
2. **Simeon as first-stage ANN recall, cross-encoder on top-K.** Simeon fills the "fast recall" role (HNSW over 384–768d simeon vectors), a learned cross-encoder (e.g. bge-reranker, MiniLM-cross) does semantic rescoring on top-K=100–500. This preserves simeon's throughput story while keeping the semantic ceiling high.
3. **Simeon-only rerank.** Reorder BM25 top-K using cos(Eq, Ed). Cheap and model-free, but the quality floor is bounded by char-n-gram + random projection: simeon cannot learn synonymy or paraphrase. Best for correcting tail lexical noise, not semantic mismatch.

Explicit non-goals: simeon does **not** replace a cross-encoder. Cross-encoders jointly attend over query+document and learn similarity; simeon's vectors are independent content-only features with no query-aware signal. Claiming simeon can stand in for a cross-encoder on semantic tasks would contradict both the NUMEN results (no paraphrase tests) and the LIMIT ceiling (fixed-dim recall bound).

This should be carried into Phase 5 A/B: include a "simeon-rerank-over-BM25" column alongside "simeon-only" and "simeon+cross-encoder" to make the trade-off explicit.

## Citations (for the eventual write-up)

- Sharma, "Beyond the Geometric Curse: High-Dimensional N-Gram Hashing for Dense Retrieval," arXiv:2601.15205, 2026. Code: https://github.com/sangeet01/limitnumen (Apache + Commons Clause).
- Weller, Boratko, Naim, Lee et al., "On the Theoretical Limitations of Embedding-Based Retrieval," arXiv:2508.21038, 2025.
- Weinberger, Dasgupta, Langford, Smola, Attenberg, "Feature Hashing for Large Scale Multitask Learning," ICML 2009.
- Charikar, "Similarity Estimation Techniques from Rounding Algorithms," STOC 2002 (SimHash).
- Broder, "On the resemblance and containment of documents," SEQUENCES 1997 (MinHash).
- Achlioptas, "Database-friendly random projections: Johnson–Lindenstrauss with binary coins," JCSS 2003.
- Li, Hastie, Church, "Very Sparse Random Projections," KDD 2006.
- Gao, Long, "RaBitQ: Quantizing High-Dimensional Vectors with a Theoretical Error Bound for ANN Search," arXiv:2405.12497, 2024.
- Dhulipala, Hadian, Jayaram, Lee, Mirrokni, "MUVERA: Multi-Vector Retrieval via Fixed Dimensional Encodings," arXiv:2405.19504, 2024.
- Bruch, Nardini, Rulli, Venturini, "Efficient Inverted Indexes for Approximate Retrieval over Learned Sparse Representations" (Seismic), arXiv:2404.18812, 2024.
- Shrivastava, "Optimal Densification for Fast and Accurate Minwise Hashing," arXiv:1703.04664, 2017.
- Thakur, Reimers, Lin, "Injecting Domain Adaptation with Learning-to-hash for Effective and Efficient Zero-shot Dense Retrieval," arXiv:2205.11498, 2022.
