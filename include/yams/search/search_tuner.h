#pragma once

#include <yams/search/search_engine.h>
#include <yams/storage/corpus_stats.h>

#include <nlohmann/json.hpp>
#include <string>

namespace yams::search {

/**
 * @brief Tuning state for the SearchTuner FSM.
 *
 * Each state corresponds to a specific corpus profile and has associated
 * optimal search parameters (RRF k, component weights, etc.).
 */
enum class TuningState {
    SMALL_CODE,  // < 1K docs, mostly code (.cpp, .py, .rs, etc.)
    LARGE_CODE,  // >= 1K docs, mostly code
    SMALL_PROSE, // < 1K docs, mostly prose (.md, .txt, etc.)
    LARGE_PROSE, // >= 1K docs, mostly prose
    SCIENTIFIC,  // Prose with no path/tag structure (benchmark-like corpora)
    MIXED,       // Balanced content types
    MINIMAL      // < 100 docs, any type (very small corpus)
};

/**
 * @brief Convert TuningState to string for logging/debugging.
 */
[[nodiscard]] constexpr const char* tuningStateToString(TuningState state) noexcept {
    switch (state) {
        case TuningState::SMALL_CODE:
            return "SMALL_CODE";
        case TuningState::LARGE_CODE:
            return "LARGE_CODE";
        case TuningState::SMALL_PROSE:
            return "SMALL_PROSE";
        case TuningState::LARGE_PROSE:
            return "LARGE_PROSE";
        case TuningState::SCIENTIFIC:
            return "SCIENTIFIC";
        case TuningState::MIXED:
            return "MIXED";
        case TuningState::MINIMAL:
            return "MINIMAL";
    }
    return "UNKNOWN";
}

/**
 * @brief Tuned parameter set for a specific corpus state.
 *
 * Contains all tunable parameters for the search engine, optimized for
 * a particular corpus profile (size, content type, feature availability).
 */
struct TunedParams {
    // RRF constant (k in 1/(k+rank))
    // Lower k = more weight on top-ranked items
    // Higher k = smoother ranking across positions
    int rrfK = 60;

    // Component weights (must sum to ~1.0)
    float textWeight = 0.45f;         // Full-text search (FTS5 + symbol)
    float vectorWeight = 0.15f;       // Vector similarity (document embeddings)
    float entityVectorWeight = 0.05f; // Entity/symbol vector similarity (code search)
    float pathTreeWeight = 0.15f;     // Path tree hierarchical
    float kgWeight = 0.10f;           // Knowledge graph
    float tagWeight = 0.10f;          // Tag-based search
    float metadataWeight = 0.05f;     // Metadata attribute matching

    // Similarity threshold for vector search
    float similarityThreshold = 0.65f;

    // Fusion strategy (default: COMB_MNZ, but TEXT_ANCHOR for scientific corpora)
    SearchEngineConfig::FusionStrategy fusionStrategy =
        SearchEngineConfig::FusionStrategy::COMB_MNZ;

    /**
     * @brief Apply tuned parameters to a SearchEngineConfig.
     */
    void applyTo(SearchEngineConfig& config) const {
        config.textWeight = textWeight;
        config.vectorWeight = vectorWeight;
        config.entityVectorWeight = entityVectorWeight;
        config.pathTreeWeight = pathTreeWeight;
        config.kgWeight = kgWeight;
        config.tagWeight = tagWeight;
        config.metadataWeight = metadataWeight;
        config.similarityThreshold = similarityThreshold;
        config.rrfK = static_cast<float>(rrfK);
        config.fusionStrategy = fusionStrategy;
    }

    /**
     * @brief Serialize to JSON.
     */
    [[nodiscard]] nlohmann::json toJson() const {
        return nlohmann::json{{"rrf_k", rrfK},
                              {"text_weight", textWeight},
                              {"vector_weight", vectorWeight},
                              {"entity_vector_weight", entityVectorWeight},
                              {"path_tree_weight", pathTreeWeight},
                              {"kg_weight", kgWeight},
                              {"tag_weight", tagWeight},
                              {"metadata_weight", metadataWeight},
                              {"similarity_threshold", similarityThreshold}};
    }
};

/// Get tuned parameters for a specific tuning state.
[[nodiscard]] inline TunedParams getTunedParams(TuningState state) noexcept {
    TunedParams params;

    switch (state) {
        case TuningState::SMALL_CODE:
            params.rrfK = 20;
            params.textWeight = 0.45f;
            params.vectorWeight = 0.15f;
            params.entityVectorWeight = 0.15f;
            params.pathTreeWeight = 0.15f;
            params.kgWeight = 0.05f;
            params.tagWeight = 0.03f;
            params.metadataWeight = 0.02f;
            break;

        case TuningState::LARGE_CODE:
            params.rrfK = 60;
            params.textWeight = 0.40f;
            params.vectorWeight = 0.20f;
            params.entityVectorWeight = 0.15f;
            params.pathTreeWeight = 0.10f;
            params.kgWeight = 0.05f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::SMALL_PROSE:
            params.rrfK = 25;
            params.textWeight = 0.50f;
            params.vectorWeight = 0.40f;
            params.entityVectorWeight = 0.00f;
            params.pathTreeWeight = 0.00f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::LARGE_PROSE:
            params.rrfK = 60;
            params.textWeight = 0.40f;
            params.vectorWeight = 0.45f;
            params.entityVectorWeight = 0.00f;
            params.pathTreeWeight = 0.05f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::SCIENTIFIC:
            // Lower rrfK gives more weight to top-ranked results (rank 0: 1/10=0.10 vs 1/30=0.03)
            params.rrfK = 10;
            // Text-heavy weighting for scientific corpora (papers, abstracts)
            params.textWeight = 0.60f;
            params.vectorWeight = 0.35f;
            params.entityVectorWeight = 0.00f;
            params.pathTreeWeight = 0.00f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.00f;
            params.metadataWeight = 0.05f;
            // TEXT_ANCHOR: text results as primary, vector for re-ranking boost
            params.fusionStrategy = SearchEngineConfig::FusionStrategy::TEXT_ANCHOR;
            break;

        case TuningState::MIXED:
            params.rrfK = 45;
            params.textWeight = 0.40f;
            params.vectorWeight = 0.25f;
            params.entityVectorWeight = 0.10f;
            params.pathTreeWeight = 0.10f;
            params.kgWeight = 0.05f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::MINIMAL:
            params.rrfK = 15;
            params.textWeight = 0.55f;
            params.vectorWeight = 0.30f;
            params.entityVectorWeight = 0.05f;
            params.pathTreeWeight = 0.05f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.03f;
            params.metadataWeight = 0.02f;
            break;
    }

    return params;
}

/**
 * @brief Adaptive search tuner that selects optimal parameters based on corpus.
 *
 * The SearchTuner implements a finite state machine that observes corpus
 * characteristics and selects optimal search parameters. It replaces the
 * static CorpusProfile system with dynamic, data-driven tuning.
 *
 * Usage:
 * @code
 *   // Get corpus stats (computed or cached)
 *   auto stats = metadataRepo->getCorpusStats();
 *
 *   // Create tuner and get optimal config
 *   SearchTuner tuner(stats.value());
 *   auto config = tuner.getConfig();
 *
 *   // Use tuned config for search
 *   SearchEngine engine(..., config);
 * @endcode
 *
 * The tuner can also be used to observe and log tuning decisions:
 * @code
 *   spdlog::info("Search tuned to {} ({})",
 *                tuner.currentState(),
 *                tuner.stateReason());
 * @endcode
 */
class SearchTuner {
public:
    /**
     * @brief Construct a tuner from corpus statistics.
     *
     * Immediately computes the optimal state and parameters based on
     * the provided corpus metrics.
     *
     * @param stats Corpus statistics (from MetadataRepository::getCorpusStats)
     */
    explicit SearchTuner(const storage::CorpusStats& stats);

    /**
     * @brief Get the current tuning state.
     */
    [[nodiscard]] TuningState currentState() const noexcept { return state_; }

    /**
     * @brief Get a human-readable explanation of why this state was selected.
     *
     * Useful for debugging and observability.
     */
    [[nodiscard]] const std::string& stateReason() const noexcept { return stateReason_; }

    /**
     * @brief Get the tuned parameter set for the current state.
     */
    [[nodiscard]] const TunedParams& getParams() const noexcept { return params_; }

    /**
     * @brief Get a SearchEngineConfig with tuned parameters applied.
     *
     * Returns a config with weights and thresholds optimized for the corpus.
     * This is the main entry point for integrating the tuner with SearchEngine.
     */
    [[nodiscard]] SearchEngineConfig getConfig() const;

    /**
     * @brief Get the RRF k constant for this tuning state.
     *
     * The k value controls how much weight is given to top-ranked results
     * in Reciprocal Rank Fusion. Lower k means top ranks dominate more.
     */
    [[nodiscard]] int getRrfK() const noexcept { return params_.rrfK; }

    /**
     * @brief Serialize tuner state to JSON for logging/debugging.
     */
    [[nodiscard]] nlohmann::json toJson() const;

    /**
     * @brief Compute the optimal state from corpus statistics.
     *
     * Static method for external use (e.g., testing, CLI display).
     *
     * @param stats Corpus statistics
     * @return Computed tuning state
     */
    [[nodiscard]] static TuningState computeState(const storage::CorpusStats& stats);

    /**
     * @brief Compute state with reason string.
     *
     * @param stats Corpus statistics
     * @param outReason Output parameter for state reason explanation
     * @return Computed tuning state
     */
    [[nodiscard]] static TuningState computeState(const storage::CorpusStats& stats,
                                                  std::string& outReason);

private:
    TuningState state_;
    TunedParams params_;
    std::string stateReason_;
    storage::CorpusStats stats_; // Keep a copy for toJson()
};

} // namespace yams::search
