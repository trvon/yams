#include <yams/search/search_tuner.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <system_error>
#include <vector>

namespace yams::search {

namespace {

constexpr std::uint64_t kAdaptiveWarmupObservations = 5;
constexpr std::uint64_t kAdaptiveCooldownObservations = 4;
constexpr std::uint64_t kAdaptivePersistInterval = 20;
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
// Adaptive similarity threshold bounds. Floor keeps us above HNSW noise on
// degenerate corpora; ceiling protects precision on dense, well-separated ones.
constexpr float kMinSimilarityThreshold = 0.05f;
constexpr float kMaxSimilarityThreshold = 0.70f;
// Stream hysteresis: only lower threshold after N consecutive empty vector pools,
// to avoid oscillating on a handful of odd queries.
constexpr std::uint64_t kAdaptiveVectorEmptyStreakThreshold = 5;
// Step sizes for threshold adjustment; small enough to avoid overshoot.
constexpr float kSimilarityThresholdLowerStep = 0.05f;
constexpr float kSimilarityThresholdRaiseStep = 0.02f;
// Margin: only raise threshold when EWMA max-sim is well above current threshold.
constexpr float kSimilarityThresholdRaiseMargin = 0.20f;
constexpr double kFusionDroppedPressureThreshold = 0.35;
constexpr double kAnchoredDroppedPressureThreshold = 0.18;
constexpr double kTopTextDroppedPressureThreshold = 0.12;
constexpr std::uint64_t kFusionPressureWarmupObservations = 5;
constexpr size_t kMaxAdaptiveLexicalFloorTopN = 12;
constexpr float kMaxAdaptiveLexicalFloorBoost = 0.18f;
constexpr float kAdaptiveLexicalFloorBoostStep = 0.04f;
constexpr float kMaxAdaptiveLexicalTieBreakEpsilon = 0.010f;
constexpr float kAdaptiveLexicalTieBreakEpsilonStep = 0.0025f;
constexpr float kMinAdaptiveVectorOnlyPenalty = 0.85f;
// Vector-only pressure thresholds — activate when too many vector-only
// pre-fusion docs are being dropped by the fusion guardrails and the
// corpus has a meaningful vector-only presence.
constexpr double kVectorOnlyPressureShareThreshold = 0.25;
constexpr double kVectorOnlyPressureDropThreshold = 0.40;
constexpr double kSemanticRescueSaturationThreshold = 0.80;
constexpr float kVectorOnlyThresholdLowerStep = 0.05f;
constexpr float kMinAdaptiveVectorOnlyThreshold = 0.65f;
constexpr size_t kMaxAdaptiveSemanticRescueSlots = 12;
constexpr float kSemanticRescueMinScoreLowerStep = 0.05f;
constexpr float kMinAdaptiveSemanticRescueMinScore = 0.45f;

bool statsAreOverlayBacked(const storage::CorpusStats& stats) {
    return stats.usedOnlineOverlay;
}

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

void clampGraphControls(TunedParams& params) {
    params.weights.kg.value = std::max(0.0f, params.weights.kg.value);
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
    setClamp(params.similarityThreshold, params.similarityThreshold.value,
             params.similarityThreshold.source, kMinSimilarityThreshold, kMaxSimilarityThreshold);
    clampGraphControls(params);

    if (stats.hasKnowledgeGraph()) {
        // Clamp KG weight to valid range (respects pinned via set())
        const float clamped =
            std::clamp(params.weights.kg.value, kMinKgWeightWhenAvailable, kMaxKgWeight);
        params.weights.kg.set(clamped, params.weights.kg.source);
    } else {
        if (!preserveExplicitGraphConfig) {
            params.weights.kg.set(0.0f, TuningLayer::Corpus);
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

    if (statsAreOverlayBacked(stats) && !preserveExplicitGraphConfig) {
        params.weights.kg.set(std::min(params.weights.kg.value, 0.08f), TuningLayer::Corpus);
        params.kgMaxResults = std::min(params.kgMaxResults, size_t{48});
        params.graphScoringBudgetMs = std::min(params.graphScoringBudgetMs, 8);
        params.graphRerankTopN = std::min(params.graphRerankTopN, size_t{24});
        params.graphRerankWeight = std::min(params.graphRerankWeight, 0.16f);
        params.graphRerankMaxBoost = std::min(params.graphRerankMaxBoost, 0.18f);
        if (stats.pathDepthMaxApproximate) {
            params.graphEnablePathEnumeration = false;
        }
    }

    params.weights.normalize();
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
        params.weights.kg.set(0.0f, TuningLayer::Corpus);
        params.kgMaxResults = 0;
        params.graphScoringBudgetMs = 0;
        params.enableGraphRerank = false;
        params.weights.normalize();
        stateReason += ", graph=off(no_kg)";
        return;
    }

    // Compute graph richness from edge density when available (faithful signal),
    // falling back to symbol density when KG edges have not been reconciled yet.
    // GLiNER NER entity density alone can be very high on scientific corpora
    // without meaningful structural edges — using it as the sole richness proxy
    // would over-activate graph reranking and path enumeration on flat prose.
    const float kgEdgeDensityAvailable = stats.kgEdgeDensity > 0.0
                                             ? static_cast<float>(stats.kgEdgeDensity)
                                             : static_cast<float>(stats.symbolDensity);
    const float graphRichness = std::clamp((kgEdgeDensityAvailable - 0.1f) / 1.5f, 0.0f, 1.0f);

    // When only NER entities exist but no edges, damp graph activation
    const bool nerOnlyNoEdges = stats.nerEntityDensity > 0.1 && stats.nativeSymbolDensity < 0.1 &&
                                stats.kgEdgeDensity < 0.5;
    const float effectiveRichness = nerOnlyNoEdges ? graphRichness * 0.35f : graphRichness;

    // Multiplicative scaling preserves profile weight ratios (unlike the
    // old absolute subtraction which destroyed them).
    params.weights.kg.scaleBy(1.3f + (1.2f * effectiveRichness), TuningLayer::Corpus);
    // Scale down other weights proportionally to make room for KG
    const float reductionFactor = 1.0f - (0.15f * effectiveRichness);
    params.weights.text.scaleBy(reductionFactor, TuningLayer::Corpus);
    params.weights.simeonText.scaleBy(reductionFactor, TuningLayer::Corpus);
    params.weights.vector.scaleBy(reductionFactor, TuningLayer::Corpus);
    params.weights.entityVector.scaleBy(reductionFactor, TuningLayer::Corpus);
    params.weights.tag.scaleBy(reductionFactor, TuningLayer::Corpus);

    params.enableGraphRerank = true;
    params.graphRerankTopN = stats.docCount >= 1000 ? 40 : 30;
    params.graphRerankWeight = 0.18f + (0.14f * effectiveRichness);
    params.graphRerankMaxBoost = 0.22f + (0.16f * effectiveRichness);
    params.graphRerankMinSignal = std::max(0.005f, 0.02f - (0.012f * effectiveRichness));
    params.graphCommunityWeight = 0.08f + (0.08f * effectiveRichness);
    params.kgMaxResults = static_cast<size_t>(std::lround(60.0 + (60.0 * effectiveRichness)));
    params.graphScoringBudgetMs = static_cast<int>(std::lround(8.0 + (6.0 * effectiveRichness)));

    // Enable path enumeration and graph query expansion for rich KGs with structural edges
    const bool hasRichTopology = stats.hasRichGraphTopology();
    params.graphEnablePathEnumeration = (hasRichTopology && effectiveRichness > 0.3F);
    params.enableGraphQueryExpansion = (hasRichTopology && effectiveRichness > 0.3F);

    params.weights.normalize();

    if (statsAreOverlayBacked(stats)) {
        params.weights.kg.set(std::min(params.weights.kg.value, 0.10f), TuningLayer::Corpus);
        params.kgMaxResults = std::min(params.kgMaxResults, size_t{48});
        params.graphScoringBudgetMs = std::min(params.graphScoringBudgetMs, 8);
        params.graphRerankTopN = std::min(params.graphRerankTopN, size_t{24});
        params.graphRerankWeight = std::min(params.graphRerankWeight, 0.16f);
        params.graphRerankMaxBoost = std::min(params.graphRerankMaxBoost, 0.18f);
        params.enableGraphQueryExpansion = false;
        if (stats.pathDepthMaxApproximate) {
            params.graphEnablePathEnumeration = false;
        }
        params.weights.normalize();
        stateReason += ", graph_damped(overlay_stats)";
    }

    std::ostringstream suffix;
    suffix << ", graph=on(symbol_density=" << stats.symbolDensity
           << ", edge_density=" << stats.kgEdgeDensity << ", kg_weight=" << params.weights.kg.value
           << ", graph_rerank_weight=" << params.graphRerankWeight
           << ", graph_community_weight=" << params.graphCommunityWeight
           << ", path_enum=" << params.graphEnablePathEnumeration
           << ", expansion=" << params.enableGraphQueryExpansion
           << ", ner_only_no_edges=" << nerOnlyNoEdges << ")";
    stateReason += suffix.str();
}

void applyCorpusStateAdjustments(const storage::CorpusStats& stats, TuningState state,
                                 TunedParams& params, std::string& stateReason) {
    std::ostringstream cs;
    cs << std::fixed << std::setprecision(2);
    bool any = false;

    const bool isSci = stats.isScientific();
    const bool isProseLarge = state == TuningState::LARGE_PROSE;
    const bool isOverlay = statsAreOverlayBacked(stats);

    // ── Extraction / FTS readiness ────────────────────────────────────────
    if (stats.contentExtractedCoverage > 0.9) {
        const size_t prev = params.textMaxResults;
        params.textMaxResults = static_cast<size_t>(std::lround(prev * 1.33));
        cs << "text_max=" << prev << "->" << params.textMaxResults;
        any = true;
    }
    if (stats.ftsIndexedCoverage > 0.8) {
        const size_t prev = params.textMaxResults;
        params.textMaxResults = std::max(prev, size_t{400});
        cs << (any ? ", " : "") << "fts_text_max=" << prev << "->" << params.textMaxResults;
        any = true;
    }

    // ── Scientific / flat-prose recall push ───────────────────────────────
    // Only when not overlay or contentExtractedCoverage confirmed from atomics.
    const bool readyForRecallPush = !isOverlay || stats.contentExtractedCoverage > 0.85;

    if (readyForRecallPush && (isSci || isProseLarge)) {
        // Minimal, non-destructive recall adjustments.
        // Benchmarks show that aggressive sim_thresh lowering (< 0.25) and
        // fanout explosion flood fusion with noise and regress recall.

        // 1. Widen vector pool slightly for weak-query corpora.
        const size_t prevVec = params.vectorMaxResults;
        if (prevVec < 180) {
            params.vectorMaxResults = 180;
            cs << (any ? ", " : "") << "vec_max=" << prevVec << "->180";
            any = true;
        }

        // 2. Boost semantic rescue for vector-only docs.
        const size_t prevRescue = params.semanticRescueSlots.value;
        if (prevRescue < 3) {
            params.semanticRescueSlots.set(size_t{3}, TuningLayer::Corpus);
            cs << (any ? ", " : "") << "rescue_slots=" << prevRescue << "->3";
            any = true;
        }

        // 3. Stronger weak-query fanout: SciFact queries are short claims.
        if (params.weakQueryVectorFanoutMultiplier < 2.3f) {
            params.weakQueryVectorFanoutMultiplier = 2.3f;
            cs << (any ? ", " : "") << "weak_vec_fanout->2.3";
            any = true;
        }
    }

    // ── NER entity density → entity vector expansion ──────────────────────
    if (stats.nerEntityDensity > 10.0) {
        params.weights.entityVector.scaleBy(1.8f, TuningLayer::Corpus);
        params.entityVectorMaxResults = std::max(params.entityVectorMaxResults, size_t{80});
        cs << (any ? ", " : "") << "entity_vec_on(d=" << stats.nerEntityDensity << ")";
        any = true;
    }

    if (any) {
        stateReason += ", corpus_state=active(" + cs.str() + ")";
    } else {
        stateReason += ", corpus_state=stable(no_adjustments)";
    }
}

double rate(std::size_t part, std::size_t total) {
    return shareOf(static_cast<double>(part), static_cast<double>(total));
}

bool applyVectorOnlyGuardrailAdjustments(TunedParams& candidate, const RuntimeTelemetry& telemetry,
                                         std::vector<std::string>& reasons) {
    const double vectorOnlyDropRate =
        rate(telemetry.vectorOnlyBelowThresholdCount, telemetry.vectorOnlyDocCount);
    const double vectorOnlyShare =
        rate(telemetry.vectorOnlyDocCount, telemetry.preFusionUniqueDocCount);

    const bool vectorPressure = vectorOnlyShare >= kVectorOnlyPressureShareThreshold &&
                                vectorOnlyDropRate >= kVectorOnlyPressureDropThreshold;

    if (!vectorPressure)
        return false;

    bool changed = false;

    const float nextThreshold =
        std::max(kMinAdaptiveVectorOnlyThreshold,
                 candidate.vectorOnlyThreshold - kVectorOnlyThresholdLowerStep);
    if (nextThreshold + 1e-5f < candidate.vectorOnlyThreshold) {
        candidate.vectorOnlyThreshold = nextThreshold;
        changed = true;
    }

    const double semanticRescueRate =
        rate(telemetry.semanticRescueFinalCount, telemetry.semanticRescueTarget);
    if (candidate.semanticRescueSlots.value > 0 &&
        semanticRescueRate >= kSemanticRescueSaturationThreshold &&
        candidate.semanticRescueSlots.value < kMaxAdaptiveSemanticRescueSlots) {
        candidate.semanticRescueSlots.set(candidate.semanticRescueSlots.value + 1,
                                          TuningLayer::Runtime);
        changed = true;
    }

    const float nextMinScore =
        std::max(kMinAdaptiveSemanticRescueMinScore,
                 candidate.semanticRescueMinVectorScore - kSemanticRescueMinScoreLowerStep);
    if (nextMinScore + 1e-6f < candidate.semanticRescueMinVectorScore) {
        candidate.semanticRescueMinVectorScore = nextMinScore;
        changed = true;
    }

    if (changed) {
        reasons.push_back("vector_only_pressure");
    }
    return changed;
}

constexpr size_t kMinVectorMaxResults = 16;
constexpr size_t kMaxVectorMaxResults = 500;
constexpr size_t kVectorMaxResultsStep = 16;
constexpr size_t kMinTextMaxResults = 50;
constexpr size_t kMaxTextMaxResults = 500;
constexpr size_t kTextMaxResultsStep = 25;

bool applyResultPoolAdjustments(TunedParams& candidate, const RuntimeTelemetry& telemetry,
                                std::vector<std::string>& reasons) {
    const double fusionDropRate = rate(telemetry.fusionDroppedDocCount,
                                       std::max<std::size_t>(telemetry.preFusionUniqueDocCount, 1));
    const double vectorOnlyShare =
        rate(telemetry.vectorOnlyDocCount, telemetry.preFusionUniqueDocCount);

    const bool vectorDominant = vectorOnlyShare >= 0.30;
    const bool highDrop = fusionDropRate >= 0.20;

    if (!vectorDominant || !highDrop)
        return false;

    bool changed = false;

    const size_t nextVectorMax =
        std::min(kMaxVectorMaxResults, candidate.vectorMaxResults + kVectorMaxResultsStep);
    if (nextVectorMax > candidate.vectorMaxResults) {
        candidate.vectorMaxResults = nextVectorMax;
        changed = true;
    }

    if (candidate.textMaxResults > kMinTextMaxResults + kTextMaxResultsStep) {
        candidate.textMaxResults =
            std::max(kMinTextMaxResults, candidate.textMaxResults - kTextMaxResultsStep);
        changed = true;
    }

    if (changed)
        reasons.push_back("result_pool_resize");
    return changed;
}

constexpr size_t kMinRerankTopK = 5;
constexpr size_t kMaxRerankTopK = 30;
constexpr size_t kRerankTopKStep = 2;
constexpr float kRerankAnchoredMinStep = 0.05f;
constexpr float kMinRerankAnchoredScore = 0.30f;

bool applyRerankerAdjustments(TunedParams& candidate, const RuntimeTelemetry& telemetry,
                              std::vector<std::string>& reasons) {
    const double fusionDropRate = rate(telemetry.fusionDroppedDocCount,
                                       std::max<std::size_t>(telemetry.preFusionUniqueDocCount, 1));
    const double rerankDropSignal =
        rate(telemetry.fusionDroppedDocCount - telemetry.anchoredFusionDroppedDocCount,
             std::max<std::size_t>(telemetry.postFusionDocCount, 1));

    const bool needWiderRerank = fusionDropRate >= 0.25 && rerankDropSignal >= 0.10;
    if (!needWiderRerank)
        return false;

    bool changed = false;

    const size_t nextRerankTopK = std::min(kMaxRerankTopK, candidate.rerankTopK + kRerankTopKStep);
    if (nextRerankTopK > candidate.rerankTopK) {
        candidate.rerankTopK = nextRerankTopK;
        changed = true;
    }

    const float nextAnchoredScore = std::max(
        kMinRerankAnchoredScore, candidate.rerankAnchoredMinRelativeScore - kRerankAnchoredMinStep);
    if (nextAnchoredScore + 1e-6f < candidate.rerankAnchoredMinRelativeScore) {
        candidate.rerankAnchoredMinRelativeScore = nextAnchoredScore;
        changed = true;
    }

    if (changed)
        reasons.push_back("reranker_widen");
    return changed;
}

bool applyFusionGuardrailAdjustments(TunedParams& candidate, const RuntimeTelemetry& telemetry,
                                     std::vector<std::string>& reasons) {
    const double fusionDropRate = rate(telemetry.fusionDroppedDocCount,
                                       std::max<std::size_t>(telemetry.preFusionUniqueDocCount, 1));
    const double anchoredDropRate =
        rate(telemetry.anchoredFusionDroppedDocCount, telemetry.anchoredPreFusionDocCount);
    const double topTextDropRate =
        rate(telemetry.topTextFusionDroppedDocCount, telemetry.topTextPreFusionDocCount);

    const bool lexicalPressure = fusionDropRate >= kFusionDroppedPressureThreshold &&
                                 (anchoredDropRate >= kAnchoredDroppedPressureThreshold ||
                                  topTextDropRate >= kTopTextDroppedPressureThreshold);
    if (!lexicalPressure) {
        return applyVectorOnlyGuardrailAdjustments(candidate, telemetry, reasons);
    }

    bool changed = false;

    if (!candidate.enableLexicalTieBreak) {
        candidate.enableLexicalTieBreak = true;
        changed = true;
    }

    const float nextTieBreakEpsilon =
        std::min(kMaxAdaptiveLexicalTieBreakEpsilon,
                 std::max(candidate.lexicalTieBreakEpsilon, kAdaptiveLexicalTieBreakEpsilonStep));
    if (nextTieBreakEpsilon > candidate.lexicalTieBreakEpsilon + 1e-6f) {
        candidate.lexicalTieBreakEpsilon = nextTieBreakEpsilon;
        changed = true;
    }

    const size_t desiredFloorTopN =
        candidate.lexicalFloorTopN == 0
            ? std::min<size_t>(6, kMaxAdaptiveLexicalFloorTopN)
            : std::min(kMaxAdaptiveLexicalFloorTopN, candidate.lexicalFloorTopN + size_t{2});
    if (desiredFloorTopN > candidate.lexicalFloorTopN) {
        candidate.lexicalFloorTopN = desiredFloorTopN;
        changed = true;
    }

    const float nextFloorBoost =
        std::min(kMaxAdaptiveLexicalFloorBoost,
                 std::max(candidate.lexicalFloorBoost + kAdaptiveLexicalFloorBoostStep,
                          kAdaptiveLexicalFloorBoostStep));
    if (nextFloorBoost > candidate.lexicalFloorBoost + 1e-6f) {
        candidate.lexicalFloorBoost = nextFloorBoost;
        changed = true;
    }

    if (candidate.vectorOnlyPenalty < kMinAdaptiveVectorOnlyPenalty) {
        candidate.vectorOnlyPenalty = kMinAdaptiveVectorOnlyPenalty;
        changed = true;
    }

    if (changed) {
        reasons.push_back("fusion_lexical_pressure");
    }
    return changed;
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
    applyCorpusStateAdjustments(stats_, state_, params_, stateReason_);
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
    params_.weights.text.value = config.textWeight;
    params_.weights.simeonText.value = config.simeonTextWeight;
    params_.weights.vector.value = config.vectorWeight;
    params_.weights.entityVector.value = config.entityVectorWeight;
    params_.weights.pathTree.value = config.pathTreeWeight;
    params_.weights.kg.value = config.kgWeight;
    params_.weights.tag.value = config.tagWeight;
    params_.weights.metadata.value = config.metadataWeight;
    params_.similarityThreshold.value = config.similarityThreshold;
    params_.vectorBoostFactor = config.vectorBoostFactor;
    params_.rrfK = static_cast<int>(std::lround(config.rrfK));
    params_.fusionStrategy.value = config.fusionStrategy;
    params_.vectorMaxResults = config.vectorMaxResults;
    params_.bm25NormDivisor = config.bm25NormDivisor;
    params_.vectorOnlyThreshold = config.vectorOnlyThreshold;
    params_.vectorOnlyPenalty = config.vectorOnlyPenalty;
    params_.vectorOnlyNearMissReserve = config.vectorOnlyNearMissReserve;
    params_.vectorOnlyNearMissSlack = config.vectorOnlyNearMissSlack;
    params_.vectorOnlyNearMissPenalty = config.vectorOnlyNearMissPenalty;
    params_.enableStrongVectorOnlyRelief = config.enableStrongVectorOnlyRelief;
    params_.strongVectorOnlyMinScore = config.strongVectorOnlyMinScore;
    params_.strongVectorOnlyTopRank = config.strongVectorOnlyTopRank;
    params_.strongVectorOnlyPenalty = config.strongVectorOnlyPenalty;
    params_.enablePathDedupInFusion = config.enablePathDedupInFusion;
    params_.lexicalFloorTopN = config.lexicalFloorTopN;
    params_.lexicalFloorBoost = config.lexicalFloorBoost;
    params_.enableLexicalTieBreak = config.enableLexicalTieBreak;
    params_.lexicalTieBreakEpsilon = config.lexicalTieBreakEpsilon;
    params_.semanticRescueSlots.value = config.semanticRescueSlots;
    params_.semanticRescueMinVectorScore = config.semanticRescueMinVectorScore;
    params_.fusionEvidenceRescueSlots = config.fusionEvidenceRescueSlots;
    params_.fusionEvidenceRescueMinScore = config.fusionEvidenceRescueMinScore;
    params_.enableAdaptiveFusion = config.enableAdaptiveFusion;
    params_.weakQueryMinTextHits = config.weakQueryMinTextHits;
    params_.weakQueryMinTopTextScore = config.weakQueryMinTopTextScore;
    params_.enableWeakQueryFanoutBoost = config.enableWeakQueryFanoutBoost;
    params_.weakQueryVectorFanoutMultiplier = config.weakQueryVectorFanoutMultiplier;
    params_.weakQueryEntityVectorFanoutMultiplier = config.weakQueryEntityVectorFanoutMultiplier;
    params_.rerankTopK = config.rerankTopK;
    params_.rerankAnchoredMinRelativeScore = config.rerankAnchoredMinRelativeScore;
    params_.enableReranking = config.enableReranking;
    params_.rerankReplaceScores = config.rerankReplaceScores;
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

void SearchTuner::pinEnvOverrides(bool textPinned, bool simeonTextPinned, bool vectorPinned,
                                  bool kgPinned, bool similarityThresholdPinned) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (textPinned) {
        params_.weights.text.forceSet(params_.weights.text.value, TuningLayer::Env);
    }
    if (simeonTextPinned) {
        params_.weights.simeonText.forceSet(params_.weights.simeonText.value, TuningLayer::Env);
    }
    if (vectorPinned) {
        params_.weights.vector.forceSet(params_.weights.vector.value, TuningLayer::Env);
    }
    if (kgPinned) {
        params_.weights.kg.forceSet(params_.weights.kg.value, TuningLayer::Env);
    }
    if (similarityThresholdPinned) {
        params_.similarityThreshold.forceSet(params_.similarityThreshold.value, TuningLayer::Env);
    }
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

void SearchTuner::observeRelevanceFeedback(const RelevanceSession& session) {
    if (session.queries.empty()) {
        return;
    }
    std::optional<std::string> pendingSnapshot;
    std::filesystem::path snapshotPath;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // Aggregate reward per-query into the EWMA. This keeps the channel
        // comparable in cadence to the runtime-telemetry EWMA (one update per
        // query). We use `observations` as the EWMA step counter so that a
        // brand-new tuner (observations==0) will take the first sample as-is.
        for (const auto& q : session.queries) {
            ++adaptive_.relevanceQueries;
            adaptive_.ewmaRelevanceReward =
                ewmaUpdate(adaptive_.ewmaRelevanceReward, std::clamp(q.reward, 0.0, 1.0),
                           adaptive_.relevanceQueries);
        }
        ++adaptive_.relevanceSessions;
        adaptive_.lastRelevanceTimestamp = session.timestamp;

        if (!persistPath_.empty()) {
            pendingSnapshot = adaptiveStateToJsonLocked().dump(2);
            snapshotPath = persistPath_;
            lastPersistedObservation_ = adaptive_.observations;
        }
    }
    if (pendingSnapshot) {
        auto written = saveAdaptiveState(snapshotPath);
        if (!written) {
            spdlog::warn("SearchTuner: failed to persist adaptive state to {}: {}",
                         snapshotPath.string(), written.error().message);
        }
    }
}

bool SearchTuner::hasConverged(std::size_t minObservations) const noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    if (adaptive_.observations < minObservations) {
        return false;
    }
    const std::uint64_t cooldown = adaptive_.observations - adaptive_.lastAdjustmentObservation;
    return cooldown >= kAdaptiveCooldownObservations;
}

void SearchTuner::observe(const RuntimeTelemetry& telemetry) {
    std::optional<std::string> pendingSnapshot;
    std::filesystem::path snapshotPath;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        observeLocked(telemetry);
        if (!persistPath_.empty() &&
            adaptive_.observations >= lastPersistedObservation_ + kAdaptivePersistInterval) {
            pendingSnapshot = adaptiveStateToJsonLocked().dump(2);
            snapshotPath = persistPath_;
            lastPersistedObservation_ = adaptive_.observations;
        }
    }
    if (pendingSnapshot) {
        auto written = saveAdaptiveState(snapshotPath);
        if (!written) {
            spdlog::warn("SearchTuner: failed to persist adaptive state to {}: {}",
                         snapshotPath.string(), written.error().message);
        }
    }
}

void SearchTuner::observeLocked(const RuntimeTelemetry& telemetry) {
    adaptive_.observations++;
    adaptive_.lastObservationChanged = false;

    const auto* kgStage = findStage(telemetry, "kg");
    const auto* graphStage = findStage(telemetry, "graph_rerank");
    const auto* vectorStage = findStage(telemetry, "vector");
    const auto* kgFusion = findFusion(telemetry, "kg");

    if (vectorStage && vectorStage->enabled && vectorStage->attempted && !vectorStage->skipped) {
        adaptive_.vectorStageObservations++;
        if (vectorStage->scoreStatsValid) {
            adaptive_.ewmaVectorMaxSimilarity =
                ewmaUpdate(adaptive_.ewmaVectorMaxSimilarity, vectorStage->maxScore,
                           adaptive_.vectorStageObservations);
            adaptive_.vectorStageEmptyStreak = 0;
        } else {
            adaptive_.vectorStageEmptyStreak++;
        }
    }

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
    const double fusionDroppedRate =
        rate(telemetry.fusionDroppedDocCount, telemetry.preFusionUniqueDocCount);
    const double anchoredFusionDroppedRate =
        rate(telemetry.anchoredFusionDroppedDocCount, telemetry.anchoredPreFusionDocCount);
    const double topTextFusionDroppedRate =
        rate(telemetry.topTextFusionDroppedDocCount, telemetry.topTextPreFusionDocCount);
    const double vectorOnlyShare =
        rate(telemetry.vectorOnlyDocCount, telemetry.preFusionUniqueDocCount);
    const double semanticRescueRate =
        rate(telemetry.semanticRescueFinalCount, telemetry.semanticRescueTarget);
    adaptive_.ewmaFusionDroppedRate =
        ewmaUpdate(adaptive_.ewmaFusionDroppedRate, fusionDroppedRate, adaptive_.observations);
    adaptive_.ewmaAnchoredFusionDroppedRate = ewmaUpdate(
        adaptive_.ewmaAnchoredFusionDroppedRate, anchoredFusionDroppedRate, adaptive_.observations);
    adaptive_.ewmaTopTextFusionDroppedRate = ewmaUpdate(
        adaptive_.ewmaTopTextFusionDroppedRate, topTextFusionDroppedRate, adaptive_.observations);
    adaptive_.ewmaVectorOnlyShare =
        ewmaUpdate(adaptive_.ewmaVectorOnlyShare, vectorOnlyShare, adaptive_.observations);
    adaptive_.ewmaSemanticRescueRate =
        ewmaUpdate(adaptive_.ewmaSemanticRescueRate, semanticRescueRate, adaptive_.observations);
    adaptive_.lastZoomLevel = telemetry.zoomLevel;
    adaptive_
        .zoomLevelCounts[SearchEngineConfig::navigationZoomLevelToString(telemetry.zoomLevel)]++;

    if (statsAreOverlayBacked(stats_) &&
        (!telemetry.adaptiveFusionEnabled ||
         adaptive_.observations < kFusionPressureWarmupObservations)) {
        adaptive_.lastDecision = "steady_overlay_stats";
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

    if (telemetry.adaptiveFusionEnabled &&
        adaptive_.observations >= kFusionPressureWarmupObservations) {
        changed = applyFusionGuardrailAdjustments(candidate, telemetry, reasons) || changed;
    }

    if (telemetry.adaptiveFusionEnabled && adaptive_.observations >= kAdaptiveWarmupObservations) {
        changed = applyResultPoolAdjustments(candidate, telemetry, reasons) || changed;
        changed = applyRerankerAdjustments(candidate, telemetry, reasons) || changed;
    }

    if (statsAreOverlayBacked(stats_)) {
        applyAdaptiveClamp(stats_, candidate);
        if (changed) {
            params_ = candidate;
            adaptive_.lastAdjustmentObservation = adaptive_.observations;
            adaptive_.lastObservationChanged = true;
        } else if (reasons.empty()) {
            reasons.push_back("steady_overlay_stats");
        }
        adaptive_.lastDecision = buildAdaptiveDecision(changed, reasons);
        return;
    }

    if (!stats_.hasKnowledgeGraph()) {
        applyAdaptiveClamp(stats_, candidate);
        if (changed) {
            params_ = candidate;
            adaptive_.lastAdjustmentObservation = adaptive_.observations;
            adaptive_.lastObservationChanged = true;
        } else if (reasons.empty()) {
            reasons.push_back("steady_no_kg");
        }
        adaptive_.lastDecision = buildAdaptiveDecision(changed, reasons);
        return;
    }

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
                std::max(kMinKgWeightWhenAvailable, candidate.weights.kg.value - 0.01f);
            if (std::abs(nextKgWeight - candidate.weights.kg.value) > 1e-6f) {
                candidate.weights.kg.set(nextKgWeight, TuningLayer::Runtime);
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
        const float nextKgWeight = std::min(kMaxKgWeight, candidate.weights.kg.value + 0.005f);

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
        if (std::abs(nextKgWeight - candidate.weights.kg.value) > 1e-6f) {
            candidate.weights.kg.set(nextKgWeight, TuningLayer::Runtime);
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

    if (!candidate.similarityThreshold.pinned) {
        const float currentThreshold = candidate.similarityThreshold.value;
        const float observedMaxSim = static_cast<float>(adaptive_.ewmaVectorMaxSimilarity);
        const bool haveVectorSignal =
            adaptive_.vectorStageObservations >= kAdaptiveWarmupObservations;

        if (haveVectorSignal) {
            if (adaptive_.vectorStageEmptyStreak >= kAdaptiveVectorEmptyStreakThreshold) {
                float nextThreshold = currentThreshold - kSimilarityThresholdLowerStep;
                if (observedMaxSim > 0.0f) {
                    nextThreshold = std::min(nextThreshold, observedMaxSim * 0.5f);
                }
                nextThreshold =
                    std::clamp(nextThreshold, kMinSimilarityThreshold, kMaxSimilarityThreshold);
                if (nextThreshold + 1e-5f < currentThreshold) {
                    if (candidate.similarityThreshold.set(nextThreshold, TuningLayer::Runtime)) {
                        changed = true;
                        reasons.push_back("vector_empty_pool_streak");
                        adaptive_.vectorStageEmptyStreak = 0;
                    }
                }
            } else if (adaptive_.vectorStageEmptyStreak == 0 &&
                       observedMaxSim > currentThreshold + kSimilarityThresholdRaiseMargin) {
                const float nextThreshold =
                    std::clamp(currentThreshold + kSimilarityThresholdRaiseStep,
                               kMinSimilarityThreshold, kMaxSimilarityThreshold);
                if (nextThreshold > currentThreshold + 1e-5f) {
                    if (candidate.similarityThreshold.set(nextThreshold, TuningLayer::Runtime)) {
                        changed = true;
                        reasons.push_back("vector_sim_headroom");
                    }
                }
            }
        }
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
        {"ewma_relevance_reward", adaptive_.ewmaRelevanceReward},
        {"relevance_sessions", adaptive_.relevanceSessions},
        {"relevance_queries", adaptive_.relevanceQueries},
        {"last_relevance_timestamp", adaptive_.lastRelevanceTimestamp},
        {"ewma_vector_max_similarity", adaptive_.ewmaVectorMaxSimilarity},
        {"vector_stage_observations", adaptive_.vectorStageObservations},
        {"vector_stage_empty_streak", adaptive_.vectorStageEmptyStreak},
        {"ewma_fusion_dropped_rate", adaptive_.ewmaFusionDroppedRate},
        {"ewma_anchored_fusion_dropped_rate", adaptive_.ewmaAnchoredFusionDroppedRate},
        {"ewma_top_text_fusion_dropped_rate", adaptive_.ewmaTopTextFusionDroppedRate},
        {"ewma_vector_only_share", adaptive_.ewmaVectorOnlyShare},
        {"ewma_semantic_rescue_rate", adaptive_.ewmaSemanticRescueRate},
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
    j["corpus"]["native_symbol_density"] = stats_.nativeSymbolDensity;
    j["corpus"]["ner_entity_density"] = stats_.nerEntityDensity;
    j["corpus"]["content_extracted_coverage"] = stats_.contentExtractedCoverage;
    j["corpus"]["fts_indexed_coverage"] = stats_.ftsIndexedCoverage;
    j["corpus"]["title_coverage"] = stats_.titleCoverage;
    j["corpus"]["kg_edge_density"] = stats_.kgEdgeDensity;
    j["corpus"]["kg_alias_density"] = stats_.kgAliasDensity;
    j["corpus"]["used_online_overlay"] = stats_.usedOnlineOverlay;
    j["corpus"]["reconciled_computed_at_ms"] = stats_.reconciledComputedAtMs;
    j["corpus"]["path_depth_max_approximate"] = stats_.pathDepthMaxApproximate;

    return j;
}

void SearchTuner::setAdaptivePersistPath(std::filesystem::path path) {
    std::lock_guard<std::mutex> lock(mutex_);
    persistPath_ = std::move(path);
    lastPersistedObservation_ = adaptive_.observations;
}

Result<void> SearchTuner::saveAdaptiveState(const std::filesystem::path& path) const {
    std::string contents;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        contents = adaptiveStateToJsonLocked().dump(2);
    }
    std::error_code ec;
    if (path.has_parent_path()) {
        std::filesystem::create_directories(path.parent_path(), ec);
        if (ec) {
            return Error{ErrorCode::IOError,
                         "Cannot create parent directory: " + path.parent_path().string() + " (" +
                             ec.message() + ")"};
        }
    }
    auto tmp = path;
    tmp += ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            return Error{ErrorCode::IOError, "Cannot open temp file for write: " + tmp.string()};
        }
        out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
        if (!out) {
            return Error{ErrorCode::IOError, "Write failed for tuner state temp: " + tmp.string()};
        }
    }
    std::filesystem::rename(tmp, path, ec);
    if (ec) {
        return Error{ErrorCode::IOError, "Atomic rename failed: " + tmp.string() + " -> " +
                                             path.string() + " (" + ec.message() + ")"};
    }
    return {};
}

Result<void> SearchTuner::loadAdaptiveState(const std::filesystem::path& path) {
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        return {};
    }
    std::ifstream in(path);
    if (!in) {
        return Error{ErrorCode::IOError, "Cannot open tuner state: " + path.string()};
    }
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(in, nullptr, /*allow_exceptions=*/true,
                                  /*ignore_comments=*/true);
    } catch (const std::exception& e) {
        spdlog::warn("Tuner state at {} is corrupt ({}); starting fresh", path.string(), e.what());
        return {};
    }
    if (!j.is_object()) {
        spdlog::warn("Tuner state at {} is not a JSON object; starting fresh", path.string());
        return {};
    }

    std::lock_guard<std::mutex> lock(mutex_);
    adaptive_.observations = j.value("observations", adaptive_.observations);
    adaptive_.lastAdjustmentObservation =
        j.value("last_adjustment_observation", adaptive_.lastAdjustmentObservation);
    adaptive_.lastObservationChanged =
        j.value("changed_last_observation", adaptive_.lastObservationChanged);
    adaptive_.lastDecision = j.value("last_decision", adaptive_.lastDecision);
    adaptive_.ewmaLatencyMs = j.value("ewma_latency_ms", adaptive_.ewmaLatencyMs);
    adaptive_.ewmaKgLatencyShare = j.value("ewma_kg_latency_share", adaptive_.ewmaKgLatencyShare);
    adaptive_.ewmaKgUtility = j.value("ewma_kg_utility", adaptive_.ewmaKgUtility);
    adaptive_.ewmaKgScoreMassShare =
        j.value("ewma_kg_score_mass_share", adaptive_.ewmaKgScoreMassShare);
    adaptive_.ewmaKgFinalDocShare =
        j.value("ewma_kg_final_doc_share", adaptive_.ewmaKgFinalDocShare);
    adaptive_.ewmaKgContributionRate =
        j.value("ewma_kg_contribution_rate", adaptive_.ewmaKgContributionRate);
    adaptive_.ewmaGraphRerankLatencyMs =
        j.value("ewma_graph_rerank_latency_ms", adaptive_.ewmaGraphRerankLatencyMs);
    adaptive_.ewmaGraphRerankSkipRate =
        j.value("ewma_graph_rerank_skip_rate", adaptive_.ewmaGraphRerankSkipRate);
    adaptive_.ewmaGraphRerankContributionRate =
        j.value("ewma_graph_rerank_contribution_rate", adaptive_.ewmaGraphRerankContributionRate);
    adaptive_.ewmaZoomDepth = j.value("ewma_zoom_depth", adaptive_.ewmaZoomDepth);
    adaptive_.ewmaRelevanceReward = j.value("ewma_relevance_reward", adaptive_.ewmaRelevanceReward);
    adaptive_.relevanceSessions = j.value("relevance_sessions", adaptive_.relevanceSessions);
    adaptive_.relevanceQueries = j.value("relevance_queries", adaptive_.relevanceQueries);
    adaptive_.lastRelevanceTimestamp =
        j.value("last_relevance_timestamp", adaptive_.lastRelevanceTimestamp);
    adaptive_.ewmaVectorMaxSimilarity =
        j.value("ewma_vector_max_similarity", adaptive_.ewmaVectorMaxSimilarity);
    adaptive_.vectorStageObservations =
        j.value("vector_stage_observations", adaptive_.vectorStageObservations);
    adaptive_.vectorStageEmptyStreak =
        j.value("vector_stage_empty_streak", adaptive_.vectorStageEmptyStreak);
    adaptive_.ewmaFusionDroppedRate =
        j.value("ewma_fusion_dropped_rate", adaptive_.ewmaFusionDroppedRate);
    adaptive_.ewmaAnchoredFusionDroppedRate =
        j.value("ewma_anchored_fusion_dropped_rate", adaptive_.ewmaAnchoredFusionDroppedRate);
    adaptive_.ewmaTopTextFusionDroppedRate =
        j.value("ewma_top_text_fusion_dropped_rate", adaptive_.ewmaTopTextFusionDroppedRate);
    adaptive_.ewmaVectorOnlyShare =
        j.value("ewma_vector_only_share", adaptive_.ewmaVectorOnlyShare);
    adaptive_.ewmaSemanticRescueRate =
        j.value("ewma_semantic_rescue_rate", adaptive_.ewmaSemanticRescueRate);
    if (j.contains("zoom_level_counts") && j["zoom_level_counts"].is_object()) {
        adaptive_.zoomLevelCounts.clear();
        for (auto it = j["zoom_level_counts"].begin(); it != j["zoom_level_counts"].end(); ++it) {
            if (it.value().is_number_unsigned()) {
                adaptive_.zoomLevelCounts[it.key()] = it.value().get<std::uint64_t>();
            }
        }
    }
    lastPersistedObservation_ = adaptive_.observations;
    return {};
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
               << "%), scientific (relative_path_depth=" << stats.pathRelativeDepthAvg
               << ", tag_coverage=" << static_cast<int>(stats.tagCoverage * 100)
               << "%, native_symbol_density=" << stats.nativeSymbolDensity
               << ", docs=" << stats.docCount << ")";
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
    if (stats.usedOnlineOverlay) {
        reason << ", overlay_stats";
    }

    reason << ", extracted=" << static_cast<int>(stats.contentExtractedCoverage * 100)
           << "% fts=" << static_cast<int>(stats.ftsIndexedCoverage * 100)
           << "% titles=" << static_cast<int>(stats.titleCoverage * 100) << "% edges=" << std::fixed
           << std::setprecision(1) << stats.kgEdgeDensity;

    outReason = reason.str();
    return state;
}

} // namespace yams::search
