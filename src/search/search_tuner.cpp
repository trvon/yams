#include <yams/search/search_tuner.h>

#include <spdlog/spdlog.h>

#include <sstream>

namespace yams::search {

SearchTuner::SearchTuner(const storage::CorpusStats& stats) : stats_(stats) {
    state_ = computeState(stats, stateReason_);
    params_ = getTunedParams(state_);

    spdlog::debug("SearchTuner initialized: state={}, reason='{}'", tuningStateToString(state_),
                  stateReason_);
}

SearchEngineConfig SearchTuner::getConfig() const {
    SearchEngineConfig config;

    // Apply tuned weights (includes fusion strategy)
    params_.applyTo(config);

    // Set corpus profile to CUSTOM since we're using tuned params
    config.corpusProfile = SearchEngineConfig::CorpusProfile::CUSTOM;

    // Enable profiling for observability
    config.enableProfiling = false;

    // fusionStrategy is set by params_.applyTo() based on corpus type
    // (e.g., TEXT_ANCHOR for SCIENTIFIC corpora, COMB_MNZ for others)

    return config;
}

nlohmann::json SearchTuner::toJson() const {
    nlohmann::json j;
    j["state"] = tuningStateToString(state_);
    j["reason"] = stateReason_;
    j["params"] = params_.toJson();
    j["rrf_k"] = params_.rrfK;

    // Include corpus stats summary
    j["corpus"] = nlohmann::json::object();
    j["corpus"]["doc_count"] = stats_.docCount;
    j["corpus"]["code_ratio"] = stats_.codeRatio;
    j["corpus"]["prose_ratio"] = stats_.proseRatio;
    j["corpus"]["path_depth_avg"] = stats_.pathDepthAvg;
    j["corpus"]["tag_coverage"] = stats_.tagCoverage;
    j["corpus"]["embedding_coverage"] = stats_.embeddingCoverage;
    j["corpus"]["symbol_density"] = stats_.symbolDensity;

    return j;
}

TuningState SearchTuner::computeState(const storage::CorpusStats& stats) {
    std::string reason;
    return computeState(stats, reason);
}

TuningState SearchTuner::computeState(const storage::CorpusStats& stats, std::string& outReason) {
    std::ostringstream reason;

    // Empty corpus edge case
    if (stats.isEmpty()) {
        outReason = "Empty corpus (0 documents)";
        return TuningState::MINIMAL;
    }

    // Size-based classification
    const bool isMinimal = stats.isMinimal(); // < 100 docs
    const bool isSmall = stats.isSmall();     // < 1000 docs

    // Content type detection (using helpers from CorpusStats)
    const bool isCode = stats.isCodeDominant();   // codeRatio > 0.7
    const bool isProse = stats.isProseDominant(); // proseRatio > 0.7

    // Scientific corpus detection: prose + flat structure + no tags
    // (typical of benchmark datasets like SciFact, BEIR, etc.)
    const bool isScientific = stats.isScientific();

    // Feature availability (for zeroing out weights)
    [[maybe_unused]] const bool hasKG = stats.hasKnowledgeGraph();
    [[maybe_unused]] const bool hasPaths = stats.hasPaths();
    [[maybe_unused]] const bool hasTags = stats.hasTags();
    [[maybe_unused]] const bool hasEmbeddings = stats.hasEmbeddings();

    // FSM state selection (ordered by specificity)
    TuningState state;

    if (isMinimal) {
        // Very small corpus: aggressive text search with low k
        state = TuningState::MINIMAL;
        reason << "docCount=" << stats.docCount << " < 100";
    } else if (isScientific) {
        // Scientific/benchmark corpus: text+vector focus, no structure
        state = TuningState::SCIENTIFIC;
        reason << "prose_dominant (" << static_cast<int>(stats.proseRatio * 100)
               << "%), flat_paths (depth=" << stats.pathDepthAvg
               << "), no_tags (coverage=" << static_cast<int>(stats.tagCoverage * 100) << "%)";
    } else if (isCode && isSmall) {
        // Small code corpus: emphasize text + path hierarchy
        state = TuningState::SMALL_CODE;
        reason << "code_dominant (" << static_cast<int>(stats.codeRatio * 100) << "%), small ("
               << stats.docCount << " docs)";
    } else if (isCode) {
        // Large code corpus: balanced with vector boost
        state = TuningState::LARGE_CODE;
        reason << "code_dominant (" << static_cast<int>(stats.codeRatio * 100) << "%), large ("
               << stats.docCount << " docs)";
    } else if (isProse && isSmall) {
        // Small prose corpus: vector emphasis
        state = TuningState::SMALL_PROSE;
        reason << "prose_dominant (" << static_cast<int>(stats.proseRatio * 100) << "%), small ("
               << stats.docCount << " docs)";
    } else if (isProse) {
        // Large prose corpus: heavy vector emphasis
        state = TuningState::LARGE_PROSE;
        reason << "prose_dominant (" << static_cast<int>(stats.proseRatio * 100) << "%), large ("
               << stats.docCount << " docs)";
    } else {
        // Mixed corpus: balanced weights
        state = TuningState::MIXED;
        reason << "mixed (code=" << static_cast<int>(stats.codeRatio * 100)
               << "%, prose=" << static_cast<int>(stats.proseRatio * 100) << "%)";
    }

    // Add feature availability notes
    if (!hasEmbeddings) {
        reason << ", no_embeddings";
    }
    if (!hasKG) {
        reason << ", no_kg";
    }

    outReason = reason.str();
    return state;
}

} // namespace yams::search
