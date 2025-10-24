#pragma once

#include <yams/core/types.h>

#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::search {

/**
 * KG score components for a single candidate result.
 *
 * entity   - similarity between query-linked entities and candidate's entities
 * structural - structural prior (e.g., Node2Vec proximity, shortest-path heuristics)
 *
 * Both scores are expected to be in [0, 1] where higher is better.
 */
struct KGScore {
    float entity = 0.0f;     // [0,1] entity similarity
    float structural = 0.0f; // [0,1] structural prior
    // Optional auxiliary features (normalized when possible). Keys are implementation-defined
    // but recommended examples include:
    //  - feature_entity_overlap_ratio
    //  - feature_query_coverage_ratio
    //  - feature_neighbor_overlap_ratio
    std::unordered_map<std::string, float> features;
};

/**
 * Optional explanation for a single candidate's KG score.
 *
 * components - named score parts (e.g., "entity_cosine", "neighbor_boost")
 * reasons    - human-readable reasons (e.g., "shares entities: A,B", "nearby via rel: works_at")
 */
struct KGExplain {
    std::string id;
    std::unordered_map<std::string, double> components;
    std::vector<std::string> reasons;
};

/**
 * Configuration for KG scoring.
 *
 * max_neighbors  - limit for neighbors per entity considered
 * max_hops       - structural search depth (1 for neighbors-only; keep small for latency)
 * budget         - time budget for KG scoring; implementations should best-effort honor it
 */
struct KGScoringConfig {
    // Neighborhood controls
    size_t max_neighbors = 32;            // Per-node neighbor cap
    size_t max_hops = 1;                  // Structural depth (1 = neighbors only)
    std::chrono::milliseconds budget{20}; // Best-effort time budget

    // Relation filters (optional). If non-empty, scorer MAY honor allow/block lists
    // to restrict traversals or entity matching to specific relation types.
    std::vector<std::string> relation_allow; // Empty = allow all
    std::vector<std::string> relation_block; // Empty = block none

    // Path enumeration flags (optional, implementation-defined)
    bool enable_path_enumeration = false; // If true, scorer MAY enumerate paths within budget
    size_t max_paths = 64;                // Soft cap for number of paths to consider
    float hop_decay = 1.0f;               // Weight decay per hop (1.0 = no decay)
};

/**
 * Symbol query detection hints
 */
struct SymbolQueryHints {
    bool looksLikeSymbol = false;          // camelCase, snake_case, qualified names
    bool hasQualifiers = false;            // Contains :: or . namespace separators
    std::vector<std::string> symbolTokens; // Extracted symbol-like tokens
    std::string detectedLanguage;          // Inferred language (cpp, python, etc.)
};

/**
 * Interface for Knowledge Graph Scoring.
 *
 * Implementations should:
 * - Be safe for concurrent read access
 * - Best-effort respect the provided time budget
 * - Return scores normalized to [0, 1]
 * - Optionally provide explanations for debugging/traceability
 *
 * Typical usage within HybridSearchEngine:
 * 1) Collect candidate ids from vector/keyword engines.
 * 2) Call score(query_text, candidate_ids).
 * 3) Blend returned KGScore into the final hybrid score using configured weights.
 */
class KGScorer {
public:
    virtual ~KGScorer() = default;

    // Configuration
    virtual void setConfig(const KGScoringConfig& cfg) = 0;
    virtual const KGScoringConfig& getConfig() const = 0;

    /**
     * Score candidate IDs with respect to the query text.
     *
     * @param query_text     The raw query text (implementations may run entity linking internally).
     * @param candidate_ids  Document/Chunk IDs to score.
     * @return Map from candidate id -> KGScore (missing IDs imply zero scores).
     */
    virtual Result<std::unordered_map<std::string, KGScore>>
    score(const std::string& query_text, const std::vector<std::string>& candidate_ids) = 0;

    /**
     * Optional: return explanations for the last scoring call.
     * Implementations may return an empty vector if explanations are disabled.
     */
    virtual std::vector<KGExplain> getLastExplanations() const { return {}; }

    /**
     * Detect if query looks like a symbol search (PBI-059).
     * Returns hints about symbol patterns in the query.
     */
    virtual SymbolQueryHints detectSymbolQuery(const std::string& /*query_text*/) const {
        return SymbolQueryHints{};
    }
};

} // namespace yams::search
