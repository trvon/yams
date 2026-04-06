#include <yams/search/search_tuner.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <sstream>
#include <vector>

namespace yams::search {

namespace {

constexpr std::uint64_t kAdaptiveWarmupObservations = 5;
constexpr std::uint64_t kAdaptiveCooldownObservations = 4;
constexpr double kEwmaAlpha = 0.20;
constexpr float kMinKgWeightWhenAvailable = 0.02f;
constexpr float kMaxKgWeight = 0.22f;
constexpr size_t kMinKgMaxResults = 12;
constexpr size_t kMaxKgMaxResults = 160;
constexpr int kMinGraphBudgetMs = 3;
constexpr int kMaxGraphBudgetMs = 25;
constexpr size_t kMinGraphRerankTopN = 10;
constexpr size_t kMaxGraphRerankTopN = 60;
constexpr int kMinRrfK = 8;
constexpr int kMaxRrfK = 80;

double ewmaUpdate(double current, double sample, std::uint64_t observations) {
    if (observations <= 1) {
        return sample;
    }
    return current + kEwmaAlpha * (sample - current);
}

double shareOf(double part, double total) {
    if (total <= 1e-9) {
        return 0.0;
    }
    return std::clamp(part / total, 0.0, 1.0);
}

double zoomLevelDepth(SearchEngineConfig::NavigationZoomLevel level) {
    switch (level) {
        case SearchEngineConfig::NavigationZoomLevel::Auto:
            return 0.0;
        case SearchEngineConfig::NavigationZoomLevel::Map:
            return 1.0;
        case SearchEngineConfig::NavigationZoomLevel::Neighborhood:
            return 2.0;
        case SearchEngineConfig::NavigationZoomLevel::Street:
            return 3.0;
    }
    return 0.0;
}

nlohmann::json zoomLevelCountJson(const std::map<std::string, std::uint64_t>& counts) {
    nlohmann::json out = nlohmann::json::object();
    for (const auto& [level, count] : counts) {
        out[level] = count;
    }
    return out;
}

const SearchTuner::RuntimeStageSignal* findStage(const SearchTuner::RuntimeTelemetry& telemetry,
                                                 std::string_view name) {
    auto it = telemetry.stages.find(std::string(name));
    if (it == telemetry.stages.end()) {
        return nullptr;
    }
    return &it->second;
}

const SearchTuner::RuntimeFusionSignal* findFusion(const SearchTuner::RuntimeTelemetry& telemetry,
                                                   std::string_view name) {
    auto it = telemetry.fusionSources.find(std::string(name));
    if (it == telemetry.fusionSources.end()) {
        return nullptr;
    }
    return &it->second;
}

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

void clampGraphControls(TunedParams& params) {
    params.kgWeight = std::max(0.0f, params.kgWeight);
    params.kgMaxResults = std::clamp(params.kgMaxResults, kMinKgMaxResults, kMaxKgMaxResults);
    params.graphScoringBudgetMs =
        std::clamp(params.graphScoringBudgetMs, kMinGraphBudgetMs, kMaxGraphBudgetMs);
    params.enableGraphRerank = params.enableGraphRerank && params.graphRerankTopN > 0;
    params.graphRerankTopN =
        std::clamp(params.graphRerankTopN, kMinGraphRerankTopN, kMaxGraphRerankTopN);
    params.graphRerankWeight = std::max(0.0f, params.graphRerankWeight);
    params.graphRerankMaxBoost = std::max(0.0f, params.graphRerankMaxBoost);
    params.graphRerankMinSignal = std::max(0.0f, params.graphRerankMinSignal);
    params.graphCommunityWeight = std::clamp(params.graphCommunityWeight, 0.0f, 1.0f);
}

void applyAdaptiveClamp(const storage::CorpusStats& stats, TunedParams& params,
                        bool preserveExplicitGraphConfig = false) {
    params.rrfK = std::clamp(params.rrfK, kMinRrfK, kMaxRrfK);
    clampGraphControls(params);

    if (stats.hasKnowledgeGraph()) {
        params.kgWeight = std::clamp(params.kgWeight, kMinKgWeightWhenAvailable, kMaxKgWeight);
    } else {
        if (!preserveExplicitGraphConfig) {
            params.kgWeight = 0.0f;
            params.kgMaxResults = 0;
            params.graphScoringBudgetMs = 0;
            params.enableGraphRerank = false;
            params.graphRerankTopN = 0;
            params.graphRerankWeight = 0.0f;
            params.graphRerankMaxBoost = 0.0f;
            params.graphRerankMinSignal = 0.0f;
            params.graphCommunityWeight = 0.0f;
        }
    }

    normalizeComponentWeights(params);
}

std::string buildAdaptiveDecision(bool changed, const std::vector<std::string>& reasons) {
    if (!changed) {
        return reasons.empty() ? "steady" : reasons.front();
    }

    std::ostringstream oss;
    for (size_t i = 0; i < reasons.size(); ++i) {
        if (i > 0) {
            oss << ", ";
        }
        oss << reasons[i];
    }
    return oss.str();
}

void applyGraphAwareAdjustments(const storage::CorpusStats& stats, TunedParams& params,
                                std::string& stateReason) {
    const bool hasKG = stats.hasKnowledgeGraph();
    if (!hasKG) {
        params.kgWeight = 0.0f;
        params.kgMaxResults = 0;
        params.graphScoringBudgetMs = 0;
        params.enableGraphRerank = false;
        normalizeComponentWeights(params);
        stateReason += ", graph=off(no_kg)";
        return;
    }

    const float graphRichness =
        std::clamp(static_cast<float>((stats.symbolDensity - 0.1) / 1.5), 0.0f, 1.0f);
    const float graphBoost = 0.04f + 0.10f * graphRichness;

    params.kgWeight += graphBoost;
    params.textWeight = std::max(0.0f, params.textWeight - graphBoost * 0.55f);
    params.vectorWeight = std::max(0.0f, params.vectorWeight - graphBoost * 0.30f);
    params.entityVectorWeight = std::max(0.0f, params.entityVectorWeight - graphBoost * 0.10f);
    params.tagWeight = std::max(0.0f, params.tagWeight - graphBoost * 0.05f);

    params.enableGraphRerank = true;
    params.graphRerankTopN = stats.docCount >= 1000 ? 40 : 30;
    params.graphRerankWeight = 0.18f + 0.14f * graphRichness;
    params.graphRerankMaxBoost = 0.22f + 0.16f * graphRichness;
    params.graphRerankMinSignal = std::max(0.005f, 0.02f - 0.012f * graphRichness);
    params.graphCommunityWeight = 0.08f + 0.08f * graphRichness;
    params.kgMaxResults = static_cast<size_t>(std::lround(60.0 + 60.0 * graphRichness));
    params.graphScoringBudgetMs = static_cast<int>(std::lround(8.0 + 6.0 * graphRichness));

    normalizeComponentWeights(params);

    std::ostringstream suffix;
    suffix << ", graph=on(symbol_density=" << stats.symbolDensity
           << ", kg_weight=" << params.kgWeight
           << ", graph_rerank_weight=" << params.graphRerankWeight
           << ", graph_community_weight=" << params.graphCommunityWeight << ")";
    stateReason += suffix.str();
}

} // namespace

SearchTuner::SearchTuner(const storage::CorpusStats& stats) : SearchTuner(stats, std::nullopt) {}

SearchTuner::SearchTuner(const storage::CorpusStats& stats, std::optional<TuningState> forcedState)
    : stats_(stats) {
    if (forcedState.has_value()) {
        state_ = *forcedState;
        stateReason_ = std::string("forced_override(") + tuningStateToString(state_) + ")";
    } else {
        state_ = computeState(stats, stateReason_);
    }

    params_ = getTunedParams(state_);
    applyGraphAwareAdjustments(stats_, params_, stateReason_);
    applyAdaptiveClamp(stats_, params_);
    baseParams_ = params_;
    baseConfig_ = buildConfigFromParamsLocked();

    spdlog::debug("SearchTuner initialized: state={}, reason='{}'", tuningStateToString(state_),
                  stateReason_);
}

void SearchTuner::seedRuntimeConfig(const SearchEngineConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    baseConfig_ = config;
    params_.zoomLevel = config.zoomLevel;
    params_.textWeight = config.textWeight;
    params_.vectorWeight = config.vectorWeight;
    params_.entityVectorWeight = config.entityVectorWeight;
    params_.pathTreeWeight = config.pathTreeWeight;
    params_.kgWeight = config.kgWeight;
    params_.tagWeight = config.tagWeight;
    params_.metadataWeight = config.metadataWeight;
    params_.similarityThreshold = config.similarityThreshold;
    params_.vectorBoostFactor = config.vectorBoostFactor;
    params_.rrfK = static_cast<int>(std::lround(config.rrfK));
    params_.fusionStrategy = config.fusionStrategy;
    params_.vectorOnlyThreshold = config.vectorOnlyThreshold;
    params_.vectorOnlyPenalty = config.vectorOnlyPenalty;
    params_.vectorOnlyNearMissReserve = config.vectorOnlyNearMissReserve;
    params_.vectorOnlyNearMissSlack = config.vectorOnlyNearMissSlack;
    params_.vectorOnlyNearMissPenalty = config.vectorOnlyNearMissPenalty;
    params_.enablePathDedupInFusion = config.enablePathDedupInFusion;
    params_.lexicalFloorTopN = config.lexicalFloorTopN;
    params_.lexicalFloorBoost = config.lexicalFloorBoost;
    params_.enableLexicalTieBreak = config.enableLexicalTieBreak;
    params_.lexicalTieBreakEpsilon = config.lexicalTieBreakEpsilon;
    params_.semanticRescueSlots = config.semanticRescueSlots;
    params_.semanticRescueMinVectorScore = config.semanticRescueMinVectorScore;
    params_.fusionEvidenceRescueSlots = config.fusionEvidenceRescueSlots;
    params_.fusionEvidenceRescueMinScore = config.fusionEvidenceRescueMinScore;
    params_.enableAdaptiveVectorFallback = config.enableAdaptiveVectorFallback;
    params_.adaptiveVectorSkipMinTier1Hits = config.adaptiveVectorSkipMinTier1Hits;
    params_.adaptiveVectorSkipRequireTextSignal = config.adaptiveVectorSkipRequireTextSignal;
    params_.adaptiveVectorSkipMinTextHits = config.adaptiveVectorSkipMinTextHits;
    params_.adaptiveVectorSkipMinTopTextScore = config.adaptiveVectorSkipMinTopTextScore;
    params_.enableGraphRerank = config.enableGraphRerank;
    params_.graphRerankTopN = config.graphRerankTopN;
    params_.graphRerankWeight = config.graphRerankWeight;
    params_.graphRerankMaxBoost = config.graphRerankMaxBoost;
    params_.graphRerankMinSignal = config.graphRerankMinSignal;
    params_.graphCommunityWeight = config.graphCommunityWeight;
    params_.kgMaxResults = config.kgMaxResults;
    params_.graphScoringBudgetMs = config.graphScoringBudgetMs;
    const bool preserveExplicitGraphConfig =
        !stats_.hasKnowledgeGraph() &&
        (config.enableGraphRerank || config.kgWeight > 0.0f || config.graphRerankWeight > 0.0f ||
         config.graphRerankMaxBoost > 0.0f || config.graphCommunityWeight > 0.0f ||
         config.kgMaxResults > 0 || config.graphScoringBudgetMs > 0);
    applyAdaptiveClamp(stats_, params_, preserveExplicitGraphConfig);
    baseParams_ = params_;
}

SearchEngineConfig SearchTuner::buildConfigFromParamsLocked() const {
    SearchEngineConfig config = baseConfig_;
    params_.applyTo(config);
    config.corpusProfile = SearchEngineConfig::CorpusProfile::CUSTOM;
    config.enableProfiling = false;
    return config;
}

SearchEngineConfig SearchTuner::getConfig() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return buildConfigFromParamsLocked();
}

void SearchTuner::observe(const RuntimeTelemetry& telemetry) {
    std::lock_guard<std::mutex> lock(mutex_);

    adaptive_.observations++;
    adaptive_.lastObservationChanged = false;

    const auto* kgStage = findStage(telemetry, "kg");
    const auto* graphStage = findStage(telemetry, "graph_rerank");
    const auto* kgFusion = findFusion(telemetry, "kg");

    const double latencyMs = std::max(0.0, telemetry.latencyMs);
    const double kgLatencyShare =
        kgStage ? shareOf(std::max(0.0, kgStage->durationMs), latencyMs) : 0.0;
    const double kgContributionRate =
        kgFusion && kgFusion->enabled ? (kgFusion->contributedToFinal ? 1.0 : 0.0) : 0.0;
    const double kgScoreMassShare = kgFusion ? std::clamp(kgFusion->finalScoreMass, 0.0, 1.0) : 0.0;
    const double kgFinalDocShare =
        kgFusion ? shareOf(static_cast<double>(kgFusion->finalTopDocCount),
                           static_cast<double>(std::max<std::size_t>(telemetry.topWindow, 1)))
                 : 0.0;
    const double kgUtility = std::clamp(
        0.55 * kgScoreMassShare + 0.30 * kgContributionRate + 0.15 * kgFinalDocShare, 0.0, 1.0);
    const double graphLatencyMs = graphStage ? std::max(0.0, graphStage->durationMs) : 0.0;
    const double graphSkipRate =
        graphStage && graphStage->enabled ? (graphStage->skipped ? 1.0 : 0.0) : 0.0;
    const double graphContributionRate =
        graphStage && graphStage->enabled ? (graphStage->contributed ? 1.0 : 0.0) : 0.0;

    adaptive_.ewmaLatencyMs =
        ewmaUpdate(adaptive_.ewmaLatencyMs, latencyMs, adaptive_.observations);
    adaptive_.ewmaKgLatencyShare =
        ewmaUpdate(adaptive_.ewmaKgLatencyShare, kgLatencyShare, adaptive_.observations);
    adaptive_.ewmaKgContributionRate =
        ewmaUpdate(adaptive_.ewmaKgContributionRate, kgContributionRate, adaptive_.observations);
    adaptive_.ewmaKgScoreMassShare =
        ewmaUpdate(adaptive_.ewmaKgScoreMassShare, kgScoreMassShare, adaptive_.observations);
    adaptive_.ewmaKgFinalDocShare =
        ewmaUpdate(adaptive_.ewmaKgFinalDocShare, kgFinalDocShare, adaptive_.observations);
    adaptive_.ewmaKgUtility =
        ewmaUpdate(adaptive_.ewmaKgUtility, kgUtility, adaptive_.observations);
    adaptive_.ewmaGraphRerankLatencyMs =
        ewmaUpdate(adaptive_.ewmaGraphRerankLatencyMs, graphLatencyMs, adaptive_.observations);
    adaptive_.ewmaGraphRerankSkipRate =
        ewmaUpdate(adaptive_.ewmaGraphRerankSkipRate, graphSkipRate, adaptive_.observations);
    adaptive_.ewmaGraphRerankContributionRate = ewmaUpdate(
        adaptive_.ewmaGraphRerankContributionRate, graphContributionRate, adaptive_.observations);
    adaptive_.ewmaZoomDepth = ewmaUpdate(
        adaptive_.ewmaZoomDepth, zoomLevelDepth(telemetry.zoomLevel), adaptive_.observations);
    adaptive_.lastZoomLevel = telemetry.zoomLevel;
    adaptive_
        .zoomLevelCounts[SearchEngineConfig::navigationZoomLevelToString(telemetry.zoomLevel)]++;

    if (!stats_.hasKnowledgeGraph()) {
        adaptive_.lastDecision = "steady_no_kg";
        return;
    }

    const bool warmedUp = adaptive_.observations >= kAdaptiveWarmupObservations;
    const bool cooldownExpired = adaptive_.observations >= adaptive_.lastAdjustmentObservation +
                                                               kAdaptiveCooldownObservations;
    if (!warmedUp || !cooldownExpired) {
        std::vector<std::string> reasons;
        if (!warmedUp) {
            reasons.push_back("warming_up");
        }
        if (!cooldownExpired) {
            reasons.push_back("cooldown_active");
        }
        adaptive_.lastDecision = buildAdaptiveDecision(false, reasons);
        return;
    }

    TunedParams candidate = params_;
    bool changed = false;
    std::vector<std::string> reasons;

    const bool kgLatencyPressure =
        adaptive_.ewmaKgLatencyShare > 0.33 && adaptive_.ewmaKgUtility < 0.18;
    const bool kgHealthyUtility =
        adaptive_.ewmaKgUtility > 0.26 && adaptive_.ewmaKgLatencyShare < 0.22;
    const bool graphMostlySkipping = adaptive_.ewmaGraphRerankSkipRate > 0.70 &&
                                     adaptive_.ewmaGraphRerankContributionRate < 0.25;

    if (kgLatencyPressure) {
        const auto nextKgMax = std::max(kMinKgMaxResults, candidate.kgMaxResults * 4 / 5);
        const auto nextBudget = std::max(kMinGraphBudgetMs, candidate.graphScoringBudgetMs - 2);
        const auto nextTopN = std::max(kMinGraphRerankTopN, candidate.graphRerankTopN > 4
                                                                ? candidate.graphRerankTopN - 4
                                                                : candidate.graphRerankTopN);
        const auto nextRrfK = std::min(kMaxRrfK, candidate.rrfK + 2);

        if (nextKgMax != candidate.kgMaxResults) {
            candidate.kgMaxResults = nextKgMax;
            changed = true;
        }
        if (nextBudget != candidate.graphScoringBudgetMs) {
            candidate.graphScoringBudgetMs = nextBudget;
            changed = true;
        }
        if (nextTopN != candidate.graphRerankTopN) {
            candidate.graphRerankTopN = nextTopN;
            changed = true;
        }
        if (graphMostlySkipping) {
            const float nextKgWeight =
                std::max(kMinKgWeightWhenAvailable, candidate.kgWeight - 0.01f);
            if (std::abs(nextKgWeight - candidate.kgWeight) > 1e-6f) {
                candidate.kgWeight = nextKgWeight;
                changed = true;
            }
        }
        if (nextRrfK != candidate.rrfK) {
            candidate.rrfK = nextRrfK;
            changed = true;
        }
        reasons.push_back("kg_latency_pressure");
        if (graphMostlySkipping) {
            reasons.push_back("graph_skip_pressure");
        }
    } else if (kgHealthyUtility) {
        const auto nextKgMax = std::min(kMaxKgMaxResults, candidate.kgMaxResults + 8);
        const auto nextBudget = std::min(kMaxGraphBudgetMs, candidate.graphScoringBudgetMs + 1);
        const auto nextTopN = std::min(kMaxGraphRerankTopN, candidate.graphRerankTopN + 2);
        const auto nextRrfK = std::max(kMinRrfK, candidate.rrfK - 1);
        const float nextKgWeight = std::min(kMaxKgWeight, candidate.kgWeight + 0.005f);

        if (nextKgMax != candidate.kgMaxResults) {
            candidate.kgMaxResults = nextKgMax;
            changed = true;
        }
        if (nextBudget != candidate.graphScoringBudgetMs) {
            candidate.graphScoringBudgetMs = nextBudget;
            changed = true;
        }
        if (nextTopN != candidate.graphRerankTopN) {
            candidate.graphRerankTopN = nextTopN;
            changed = true;
        }
        if (std::abs(nextKgWeight - candidate.kgWeight) > 1e-6f) {
            candidate.kgWeight = nextKgWeight;
            changed = true;
        }
        if (nextRrfK != candidate.rrfK) {
            candidate.rrfK = nextRrfK;
            changed = true;
        }
        reasons.push_back("kg_utility_recovery");
    } else {
        reasons.push_back("steady_band");
    }

    applyAdaptiveClamp(stats_, candidate);
    if (changed) {
        params_ = candidate;
        adaptive_.lastAdjustmentObservation = adaptive_.observations;
        adaptive_.lastObservationChanged = true;
    }
    adaptive_.lastDecision = buildAdaptiveDecision(changed, reasons);
}

nlohmann::json SearchTuner::adaptiveStateToJsonLocked() const {
    return {
        {"observations", adaptive_.observations},
        {"last_adjustment_observation", adaptive_.lastAdjustmentObservation},
        {"changed_last_observation", adaptive_.lastObservationChanged},
        {"last_decision", adaptive_.lastDecision},
        {"ewma_latency_ms", adaptive_.ewmaLatencyMs},
        {"ewma_kg_latency_share", adaptive_.ewmaKgLatencyShare},
        {"ewma_kg_utility", adaptive_.ewmaKgUtility},
        {"ewma_kg_score_mass_share", adaptive_.ewmaKgScoreMassShare},
        {"ewma_kg_final_doc_share", adaptive_.ewmaKgFinalDocShare},
        {"ewma_kg_contribution_rate", adaptive_.ewmaKgContributionRate},
        {"ewma_graph_rerank_latency_ms", adaptive_.ewmaGraphRerankLatencyMs},
        {"ewma_graph_rerank_skip_rate", adaptive_.ewmaGraphRerankSkipRate},
        {"ewma_graph_rerank_contribution_rate", adaptive_.ewmaGraphRerankContributionRate},
        {"ewma_zoom_depth", adaptive_.ewmaZoomDepth},
        {"last_zoom_level",
         SearchEngineConfig::navigationZoomLevelToString(adaptive_.lastZoomLevel)},
        {"zoom_level_counts", zoomLevelCountJson(adaptive_.zoomLevelCounts)},
        {"current_params", params_.toJson()},
        {"base_params", baseParams_.toJson()},
    };
}

nlohmann::json SearchTuner::adaptiveStateToJson() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return adaptiveStateToJsonLocked();
}

nlohmann::json SearchTuner::toJson() const {
    std::lock_guard<std::mutex> lock(mutex_);

    nlohmann::json j;
    j["state"] = tuningStateToString(state_);
    j["reason"] = stateReason_;
    j["params"] = params_.toJson();
    j["rrf_k"] = params_.rrfK;
    j["adaptive"] = adaptiveStateToJsonLocked();

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

    if (stats.isEmpty()) {
        outReason = "Empty corpus (0 documents)";
        return TuningState::MINIMAL;
    }

    const bool isMinimal = stats.isMinimal();
    const bool isSmall = stats.isSmall();
    const bool isCode = stats.isCodeDominant();
    const bool isProse = stats.isProseDominant();
    const bool isScientific = stats.isScientific();

    [[maybe_unused]] const bool hasKG = stats.hasKnowledgeGraph();
    [[maybe_unused]] const bool hasPaths = stats.hasPaths();
    [[maybe_unused]] const bool hasTags = stats.hasTags();
    [[maybe_unused]] const bool hasEmbeddings = stats.hasEmbeddings();

    TuningState state;

    if (isMinimal) {
        state = TuningState::MINIMAL;
        reason << "docCount=" << stats.docCount << " < 100";
    } else if (isScientific) {
        state = TuningState::SCIENTIFIC;
        reason << "prose_dominant (" << static_cast<int>(stats.proseRatio * 100)
               << "%), benchmark_like (path_depth=" << stats.pathDepthAvg
               << ", tag_coverage=" << static_cast<int>(stats.tagCoverage * 100)
               << "%, symbol_density=" << stats.symbolDensity << ")";
    } else if (isCode && isSmall) {
        state = TuningState::SMALL_CODE;
        reason << "code_dominant (" << static_cast<int>(stats.codeRatio * 100) << "%), small ("
               << stats.docCount << " docs)";
    } else if (isCode) {
        state = TuningState::LARGE_CODE;
        reason << "code_dominant (" << static_cast<int>(stats.codeRatio * 100) << "%), large ("
               << stats.docCount << " docs)";
    } else if (isProse && isSmall) {
        state = TuningState::SMALL_PROSE;
        reason << "prose_dominant (" << static_cast<int>(stats.proseRatio * 100) << "%), small ("
               << stats.docCount << " docs)";
    } else if (isProse) {
        state = TuningState::LARGE_PROSE;
        reason << "prose_dominant (" << static_cast<int>(stats.proseRatio * 100) << "%), large ("
               << stats.docCount << " docs)";
    } else {
        if (hasEmbeddings) {
            state = TuningState::MIXED_PRECISION;
            reason << "mixed_precision (code=" << static_cast<int>(stats.codeRatio * 100)
                   << "%, prose=" << static_cast<int>(stats.proseRatio * 100)
                   << "%, embeddings=" << static_cast<int>(stats.embeddingCoverage * 100) << "%)";
        } else {
            state = TuningState::MIXED;
            reason << "mixed (code=" << static_cast<int>(stats.codeRatio * 100)
                   << "%, prose=" << static_cast<int>(stats.proseRatio * 100) << "%)";
        }
    }

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
