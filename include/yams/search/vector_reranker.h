#pragma once

#include <yams/core/types.h>
#include <yams/vector/vector_types.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::search {

/**
 * @brief Input to vector-aware reranking
 *
 * Bundles a transformed query vector and candidate records for reranking.
 * The transformed query is computed once per query; candidates are scored
 * without full decode.
 */
struct VectorRerankInput {
    /**
     * Transformed query vector from TurboQuantMSE::transformQuery().
     * Computed once per query and reused for all candidates.
     */
    std::vector<float> transformed_query;

    /**
     * Map from chunk_id -> VectorRecord for reranking candidates.
     * Only records with non-empty quantized.packed_codes participate in reranking;
     * others receive a default score (0.0f).
     */
    std::unordered_map<std::string, yams::vector::VectorRecord> candidates;

    /**
     * Initial relevance scores from first-stage retrieval, used as a tiebreaker
     * when TurboQuant scores are equal. If absent, 0.0f is assumed.
     */
    std::unordered_map<std::string, float> initial_scores;
};

/**
 * @brief Output from vector-aware reranking
 *
 * Reranked candidates with asymmetric TurboQuant scores.
 */
struct VectorRerankOutput {
    /**
     * Reranked candidates in descending order of rerank_score.
     */
    struct Candidate {
        std::string chunk_id;
        float rerank_score;    // TurboQuant asymmetric score
        float initial_score;   // Score from first-stage retrieval
        float blended_score;   // rerank_score blended with initial_score
        bool has_packed_codes; // true if scored via TurboQuant; false if fallback
    };

    std::vector<Candidate> candidates;

    /**
     * Number of candidates that had packed codes and were scored asymmetrically.
     */
    size_t packed_candidates_scored = 0;

    /**
     * Number of candidates skipped (no packed codes, no initial fallback).
     */
    size_t candidates_skipped = 0;
};

/**
 * @brief Interface for vector-aware reranking using compressed codes
 *
 * IVectorReranker operates on the compressed representation of candidate
 * embeddings. Unlike IReranker (text cross-encoder), this reranker:
 *   - Takes a transformed query (precomputed once per query)
 *   - Scores candidates from packed TurboQuant codes without full decode
 *   - Provides a dedicated rerank stage between first-stage retrieval
 *     and optional cross-encoder reranking
 *
 * Usage:
 *   1. Implement concrete reranker (e.g., TurboQuantPackedReranker)
 *   2. Build TurboQuantConfig and TurboQuantMSE from it
 *   3. Call transformQuery() once per query
 *   4. Call rerank() for each candidate set
 *
 * Pipeline ordering (recommended):
 *   1. First-stage retrieval/fusion
 *   2. TurboQuant vector rerank (this interface)
 *   3. Optional cross-encoder rerank via IReranker/ModelProviderRerankerAdapter
 *   4. Final blended ranking
 */
class IVectorReranker {
public:
    virtual ~IVectorReranker() = default;

    /**
     * @brief Rerank candidates using compressed-code scoring
     *
     * @param input Transformed query and candidate records
     * @return Reranked candidates sorted by rerank_score descending,
     *         or error on failure (e.g., dimension mismatch, unconfigured state)
     */
    virtual Result<VectorRerankOutput> rerank(const VectorRerankInput& input) = 0;

    /**
     * @brief Check if the reranker is ready to score candidates
     */
    virtual bool isReady() const = 0;

    /**
     * @brief Name of the reranker for logging/debugging
     */
    virtual std::string name() const = 0;

    /**
     * @brief Embedding dimension this reranker was configured with
     */
    virtual size_t dimension() const = 0;

    /**
     * @brief Bits per channel for compressed scoring
     */
    virtual uint8_t bitsPerChannel() const = 0;
};

} // namespace yams::search
