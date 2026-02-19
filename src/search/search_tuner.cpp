#include <yams/search/search_tuner.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <sstream>

namespace yams::search {

namespace {

void normalizeComponentWeights(TunedParams& params) {
    params.textWeight = std::max(0.0f, params.textWeight);
    params.vectorWeight = std::max(0.0f, params.vectorWeight);
    params.entityVectorWeight = std::max(0.0f, params.entityVectorWeight);
    params.pathTreeWeight = std::max(0.0f, params.pathTreeWeight);
    params.kgWeight = std::max(0.0f, params.kgWeight);
    params.tagWeight = std::max(0.0f, params.tagWeight);
    params.metadataWeight = std::max(0.0f, params.metadataWeight);

    const float sum = params.textWeight + params.vectorWeight + params.entityVectorWeight +
                      params.pathTreeWeight + params.kgWeight + params.tagWeight +
                      params.metadataWeight;
    if (sum > 0.0f) {
        const float inv = 1.0f / sum;
        params.textWeight *= inv;
        params.vectorWeight *= inv;
        params.entityVectorWeight *= inv;
        params.pathTreeWeight *= inv;
        params.kgWeight *= inv;
        params.tagWeight *= inv;
        params.metadataWeight *= inv;
    }
}

void applyGraphAwareAdjustments(const storage::CorpusStats& stats, TunedParams& params,
                                std::string& stateReason) {
    const bool hasKG = stats.hasKnowledgeGraph();
    if (!hasKG) {
        params.kgWeight = 0.0f;
        params.enableGraphRerank = false;
        normalizeComponentWeights(params);
        stateReason += ", graph=off(no_kg)";
        return;
    }

    const float graphRichness =
        std::clamp(static_cast<float>((stats.symbolDensity - 0.1) / 1.5), 0.0f, 1.0f);
    const float graphBoost = 0.04f + 0.10f * graphRichness;

    // Move rank mass toward KG while preserving text/vector dominance for lexical quality.
    params.kgWeight += graphBoost;
    params.textWeight = std::max(0.0f, params.textWeight - graphBoost * 0.55f);
    params.vectorWeight = std::max(0.0f, params.vectorWeight - graphBoost * 0.30f);
    params.entityVectorWeight = std::max(0.0f, params.entityVectorWeight - graphBoost * 0.10f);
    params.tagWeight = std::max(0.0f, params.tagWeight - graphBoost * 0.05f);

    // Enable graph rerank and scale knobs by graph richness.
    params.enableGraphRerank = true;
    params.graphRerankTopN = stats.docCount >= 1000 ? 40 : 30;
    params.graphRerankWeight = 0.18f + 0.14f * graphRichness;
    params.graphRerankMaxBoost = 0.22f + 0.16f * graphRichness;
    params.graphRerankMinSignal = std::max(0.005f, 0.02f - 0.012f * graphRichness);

    normalizeComponentWeights(params);

    std::ostringstream suffix;
    suffix << ", graph=on(symbol_density=" << stats.symbolDensity
           << ", kg_weight=" << params.kgWeight
           << ", graph_rerank_weight=" << params.graphRerankWeight << ")";
    stateReason += suffix.str();
}

} // namespace

SearchTuner::SearchTuner(const storage::CorpusStats& stats) : stats_(stats) {
    state_ = computeState(stats, stateReason_);
    params_ = getTunedParams(state_);
    applyGraphAwareAdjustments(stats_, params_, stateReason_);

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
    // (COMB_MNZ is used for most corpus types)

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
               << "%), benchmark_like (path_depth=" << stats.pathDepthAvg
               << ", tag_coverage=" << static_cast<int>(stats.tagCoverage * 100)
               << "%, symbol_density=" << stats.symbolDensity << ")";
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
