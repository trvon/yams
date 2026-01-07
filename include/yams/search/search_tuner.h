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
    float textWeight = 0.45f;     // Full-text search (FTS5 + symbol)
    float vectorWeight = 0.15f;   // Vector similarity
    float pathTreeWeight = 0.15f; // Path tree hierarchical
    float kgWeight = 0.10f;       // Knowledge graph
    float tagWeight = 0.10f;      // Tag-based search
    float metadataWeight = 0.05f; // Metadata attribute matching

    // Similarity threshold for vector search
    float similarityThreshold = 0.65f;

    /**
     * @brief Apply tuned parameters to a SearchEngineConfig.
     */
    void applyTo(SearchEngineConfig& config) const {
        config.textWeight = textWeight;
        config.vectorWeight = vectorWeight;
        config.pathTreeWeight = pathTreeWeight;
        config.kgWeight = kgWeight;
        config.tagWeight = tagWeight;
        config.metadataWeight = metadataWeight;
        config.similarityThreshold = similarityThreshold;
        config.rrfK = static_cast<float>(rrfK);
    }

    /**
     * @brief Serialize to JSON.
     */
    [[nodiscard]] nlohmann::json toJson() const {
        return nlohmann::json{{"rrf_k", rrfK},
                              {"text_weight", textWeight},
                              {"vector_weight", vectorWeight},
                              {"path_tree_weight", pathTreeWeight},
                              {"kg_weight", kgWeight},
                              {"tag_weight", tagWeight},
                              {"metadata_weight", metadataWeight},
                              {"similarity_threshold", similarityThreshold}};
    }
};

/**
 * @brief Get tuned parameters for a specific tuning state.
 *
 * Returns the optimal parameter set for the given corpus profile.
 * These are empirically derived from benchmarking across corpus types.
 *
 * Parameter matrix (from epic yams-7ez4):
 * | State        | k  | text | vector | path | kg   | tag  | meta |
 * |--------------|---:|-----:|-------:|-----:|-----:|-----:|-----:|
 * | SMALL_CODE   | 20 | 0.50 |   0.15 | 0.20 | 0.10 | 0.03 | 0.02 |
 * | LARGE_CODE   | 60 | 0.45 |   0.20 | 0.15 | 0.10 | 0.05 | 0.05 |
 * | SMALL_PROSE  | 25 | 0.50 |   0.40 | 0.00 | 0.00 | 0.05 | 0.05 |
 * | LARGE_PROSE  | 60 | 0.40 |   0.45 | 0.05 | 0.00 | 0.05 | 0.05 |
 * | SCIENTIFIC   | 30 | 0.55 |   0.40 | 0.00 | 0.00 | 0.00 | 0.05 |
 * | MIXED        | 45 | 0.45 |   0.25 | 0.10 | 0.10 | 0.05 | 0.05 |
 * | MINIMAL      | 15 | 0.60 |   0.30 | 0.05 | 0.00 | 0.03 | 0.02 |
 */
[[nodiscard]] inline TunedParams getTunedParams(TuningState state) noexcept {
    TunedParams params;

    switch (state) {
        case TuningState::SMALL_CODE:
            // Small code corpus: emphasize text search, path hierarchy
            params.rrfK = 20;
            params.textWeight = 0.50f;
            params.vectorWeight = 0.15f;
            params.pathTreeWeight = 0.20f;
            params.kgWeight = 0.10f;
            params.tagWeight = 0.03f;
            params.metadataWeight = 0.02f;
            break;

        case TuningState::LARGE_CODE:
            // Large code corpus: balanced with vector boost
            params.rrfK = 60;
            params.textWeight = 0.45f;
            params.vectorWeight = 0.20f;
            params.pathTreeWeight = 0.15f;
            params.kgWeight = 0.10f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::SMALL_PROSE:
            // Small prose: heavy vector emphasis, no KG/path
            params.rrfK = 25;
            params.textWeight = 0.50f;
            params.vectorWeight = 0.40f;
            params.pathTreeWeight = 0.00f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::LARGE_PROSE:
            // Large prose: vector-dominant, minimal path
            params.rrfK = 60;
            params.textWeight = 0.40f;
            params.vectorWeight = 0.45f;
            params.pathTreeWeight = 0.05f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::SCIENTIFIC:
            // Scientific/benchmark: text+vector only, no structure
            params.rrfK = 30;
            params.textWeight = 0.55f;
            params.vectorWeight = 0.40f;
            params.pathTreeWeight = 0.00f;
            params.kgWeight = 0.00f;
            params.tagWeight = 0.00f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::MIXED:
            // Mixed corpus: balanced across all components
            params.rrfK = 45;
            params.textWeight = 0.45f;
            params.vectorWeight = 0.25f;
            params.pathTreeWeight = 0.10f;
            params.kgWeight = 0.10f;
            params.tagWeight = 0.05f;
            params.metadataWeight = 0.05f;
            break;

        case TuningState::MINIMAL:
            // Minimal corpus: text-heavy, low k for small lists
            params.rrfK = 15;
            params.textWeight = 0.60f;
            params.vectorWeight = 0.30f;
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
