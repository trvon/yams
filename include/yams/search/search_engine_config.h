#pragma once

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::search {

struct SearchParams {
    int limit = 100;
    int offset = 0;
    bool semanticOnly = false;
    std::vector<std::string> tags;
    bool matchAllTags = false;
    std::optional<std::string> mimeType;
    std::optional<std::string> extension;
    std::optional<int64_t> modifiedAfter;
    std::optional<int64_t> modifiedBefore;
};

struct SearchEngineConfig {
    enum class CorpusProfile {
        CODE,
        PROSE,
        DOCS,
        MIXED,
        CUSTOM
    } corpusProfile = CorpusProfile::MIXED;

    [[nodiscard]] static constexpr const char*
    corpusProfileToString(CorpusProfile profile) noexcept {
        switch (profile) {
            case CorpusProfile::CODE:
                return "CODE";
            case CorpusProfile::PROSE:
                return "PROSE";
            case CorpusProfile::DOCS:
                return "DOCS";
            case CorpusProfile::MIXED:
                return "MIXED";
            case CorpusProfile::CUSTOM:
                return "CUSTOM";
        }
        return "MIXED";
    }

    enum class NavigationZoomLevel {
        Auto,
        Map,
        Neighborhood,
        Street,
    } zoomLevel = NavigationZoomLevel::Auto;

    [[nodiscard]] static constexpr const char*
    navigationZoomLevelToString(NavigationZoomLevel level) noexcept {
        switch (level) {
            case NavigationZoomLevel::Auto:
                return "AUTO";
            case NavigationZoomLevel::Map:
                return "MAP";
            case NavigationZoomLevel::Neighborhood:
                return "NEIGHBORHOOD";
            case NavigationZoomLevel::Street:
                return "STREET";
        }
        return "AUTO";
    }

    float textWeight = 0.70f;
    float simeonTextWeight = 0.15f;
    float graphTextWeight = 0.12f;
    float pathTreeWeight = 0.08f;
    float kgWeight = 0.04f;
    float vectorWeight = 0.30f;
    float graphVectorWeight = 0.08f;
    float vectorOnlyPenalty = 0.8f;
    float vectorOnlyThreshold = 0.90f;
    bool enableStrongVectorOnlyRelief = false;
    float strongVectorOnlyMinScore = 0.97f;
    size_t strongVectorOnlyTopRank = 0;
    float strongVectorOnlyPenalty = 0.95f;
    size_t vectorOnlyNearMissReserve = 0;
    float vectorOnlyNearMissSlack = 0.05f;
    float vectorOnlyNearMissPenalty = 0.60f;
    float vectorBoostFactor = 0.10f;
    float entityVectorWeight = 0.05f;
    float tagWeight = 0.05f;
    float metadataWeight = 0.05f;

    float conceptBoostWeight = 0.10f;
    float conceptMinConfidence = 0.40f;
    size_t conceptMaxCount = 6;
    float conceptMaxBoost = 0.25f;
    size_t conceptMaxScanResults = 200;
    bool waitForConceptExtraction = true;

    /// Controls which backend produces query-time concepts for entity matching
    /// and graph-based reranking. Fallback uses sub-phrase mining + IDF-weighted
    /// tokens (<1ms per query); GlinerWithFallback tries GLiNER first and falls
    /// back when GLiNER returns empty or times out.
    enum class ConceptExtractionBackend {
        Fallback,
        GlinerWithFallback,
    } conceptExtractionBackend = ConceptExtractionBackend::GlinerWithFallback;

    static const char* conceptExtractionBackendToString(ConceptExtractionBackend backend) noexcept {
        switch (backend) {
            case ConceptExtractionBackend::Fallback:
                return "Fallback";
            case ConceptExtractionBackend::GlinerWithFallback:
                return "GlinerWithFallback";
        }
        return "Fallback";
    }

    size_t maxResults = 100;
    float similarityThreshold = 0.0f;
    bool enableParallelExecution = true;
    std::chrono::milliseconds componentTimeout = std::chrono::milliseconds(0);

    bool enableTieredExecution = true;
    bool tieredNarrowVectorSearch = false;
    size_t tieredMinCandidates = 10;
    size_t weakQueryMinTextHits = 3;
    float weakQueryMinTopTextScore = 0.30f;
    bool enableWeakQueryFanoutBoost = true;
    float weakQueryVectorFanoutMultiplier = 2.0f;
    float weakQueryEntityVectorFanoutMultiplier = 1.5f;

    enum class TopologyRoutingMode {
        Disabled,
        WeakQueryOnly,
        HybridAssist,
        RerankOnly,
    } topologyRoutingMode = TopologyRoutingMode::Disabled;

    [[nodiscard]] static constexpr const char*
    topologyRoutingModeToString(TopologyRoutingMode mode) noexcept {
        switch (mode) {
            case TopologyRoutingMode::Disabled:
                return "disabled";
            case TopologyRoutingMode::WeakQueryOnly:
                return "weak_query_only";
            case TopologyRoutingMode::HybridAssist:
                return "hybrid_assist";
            case TopologyRoutingMode::RerankOnly:
                return "rerank_only";
        }
        return "disabled";
    }

    /// How routed topology members interact with the global vector retriever.
    /// Augment preserves the bounded ANN leg and unions query-ranked routed members.
    /// Narrow replaces ANN with the routed allowed-document set.
    enum class TopologyVectorPolicy {
        Augment,
        Narrow,
    } topologyVectorPolicy = TopologyVectorPolicy::Augment;

    [[nodiscard]] static constexpr const char*
    topologyVectorPolicyToString(TopologyVectorPolicy policy) noexcept {
        switch (policy) {
            case TopologyVectorPolicy::Augment:
                return "augment";
            case TopologyVectorPolicy::Narrow:
                return "narrow";
        }
        return "augment";
    }

    enum class TopologyRouteScoringMode {
        Current,
        SizeWeighted,
        SeedCoverage,
    } topologyRouteScoringMode = TopologyRouteScoringMode::Current;

    [[nodiscard]] static constexpr const char*
    topologyRouteScoringModeToString(TopologyRouteScoringMode mode) noexcept {
        switch (mode) {
            case TopologyRouteScoringMode::Current:
                return "current";
            case TopologyRouteScoringMode::SizeWeighted:
                return "size_weighted";
            case TopologyRouteScoringMode::SeedCoverage:
                return "seed_coverage";
        }
        return "current";
    }

    // Legacy compatibility switch. Prefer topologyRoutingMode for new code.
    bool enableTopologyWeakQueryRouting = false;
    /// Minimum and maximum cluster probes. Adaptive probing is disabled when
    /// topologyAdaptiveProbeScoreGap is zero, preserving fixed maxClusters behavior.
    size_t topologyMinClusters = 1;
    size_t topologyMaxClusters = 2;
    /// Highest-ranked lexical documents allowed to influence cluster routing.
    size_t topologyMaxSeedDocuments = 32;
    /// Include another cluster while its score remains this close to the best route.
    float topologyAdaptiveProbeScoreGap = 0.0f;
    /// Abstain from hard narrowing when the selected/excluded boundary is closer
    /// than this margin. Mixed-corpus calibration favors 0.20; zero disables abstention.
    float topologyNarrowMinBoundaryMargin = 0.20f;
    size_t topologyMaxDocs = 64;
    size_t topologyMaxDocsPerCluster = 0;
    float topologyMedoidBoost = 0.05f;
    float topologySparseDenseAlpha = 0.5f;
    float topologyMinRouteScore = 0.0f;
    bool topologyMedoidOnlyExpansion = false;

    /// Where topology candidates come from at search time.
    /// Clusters: seed → cluster router → member expansion (legacy).
    /// GraphNeighbors: seed → semantic_neighbor edges (no partition required).
    enum class TopologyExpansionSource {
        Clusters,
        GraphNeighbors,
    } topologyExpansionSource = TopologyExpansionSource::Clusters;

    [[nodiscard]] static constexpr const char*
    topologyExpansionSourceToString(TopologyExpansionSource source) noexcept {
        switch (source) {
            case TopologyExpansionSource::Clusters:
                return "clusters";
            case TopologyExpansionSource::GraphNeighbors:
                return "graph_neighbors";
        }
        return "clusters";
    }

    /// Min edge weight for graph_neighbors expansion (search path).
    float topologyGraphNeighborMinScore = 0.25f;
    /// Prefer reciprocal semantic_neighbor edges when expanding graph neighbors.
    bool topologyGraphNeighborReciprocalOnly = true;
    /// GraphNeighbors seed ANN k. 0 = off (product default); opt-in for experiments.
    size_t topologyGraphVectorSeedProbe = 0;

    bool bypassCorpusWarmingGate = false;
    float rrfK = 12.0f;
    float bm25NormDivisor = 25.0f;
    bool enableProfiling = false;

    enum class FusionStrategy {
        WEIGHTED_SUM,
        RECIPROCAL_RANK,
        WEIGHTED_RECIPROCAL,
        COMB_MNZ,
        CONVEX,
        WEIGHTED_LINEAR_ZSCORE
    } fusionStrategy = FusionStrategy::WEIGHTED_RECIPROCAL;

    size_t weightedLinearZScorePoolSize = 500;
    float weightedLinearZScoreAlpha = 0.75f;
    bool weightedLinearZScoreUseZScore = true;
    bool enableAdaptiveFusion = false;

    enum class ChunkAggregation {
        MAX,
        SUM,
        TOP_K_AVG,
        WEIGHTED_TOP_K_AVG
    } chunkAggregation = ChunkAggregation::WEIGHTED_TOP_K_AVG;
    size_t chunkAggregationTopK = 3;
    float chunkAggregationWeightDecay = 0.6f;

    bool enableIntentAdaptiveWeighting = true;
    bool enableFieldAwareWeightedRrf = true;
    bool enableLexicalExpansion = false;
    size_t lexicalExpansionMinHits = 3;
    float lexicalExpansionScorePenalty = 0.65f;
    bool enablePathDedupInFusion = false;
    size_t lexicalFloorTopN = 0;
    float lexicalFloorBoost = 0.0f;
    bool enableLexicalTieBreak = false;
    float lexicalTieBreakEpsilon = 0.0f;
    size_t semanticRescueSlots = 0;
    float semanticRescueMinVectorScore = 0.0f;
    size_t fusionEvidenceRescueSlots = 0;
    float fusionEvidenceRescueMinScore = 0.0f;
    size_t topologySidecarFusionRescueSlots = 0;
    float topologySidecarFusionRescueMinScore = 0.0f;

    bool enableMultiVectorQuery = false;
    size_t multiVectorMaxPhrases = 3;
    float multiVectorScoreDecay = 0.85f;

    bool enableSubPhraseExpansion = false;
    size_t subPhraseExpansionMinHits = 5;
    float subPhraseExpansionPenalty = 0.70f;

    bool enableSubPhraseRescoring = false;
    float subPhraseScoringPenalty = 0.70f;

    bool enableGraphQueryExpansion = false;
    size_t graphExpansionMinHits = 8;
    size_t graphExpansionMaxTerms = 8;
    size_t graphExpansionMaxSeeds = 6;
    size_t graphExpansionQueryNeighborK = 12;
    float graphExpansionQueryNeighborMinScore = 0.84f;
    bool graphVectorRequireCorroboration = true;
    bool graphVectorRequireTextAnchoring = true;
    bool graphVectorRequireBaselineTextAnchoring = true;
    bool enableGraphFusionWindowGuard = false;
    size_t graphFusionGuardDepthMultiplier = 2;
    size_t graphMaxAddedInFusionWindow = 0;
    float graphTextMinAdmissionScore = 0.0010f;
    float graphExpansionFtsPenalty = 0.78f;
    float graphExpansionVectorPenalty = 0.82f;

    [[nodiscard]] static constexpr const char*
    fusionStrategyToString(FusionStrategy strategy) noexcept {
        switch (strategy) {
            case FusionStrategy::WEIGHTED_SUM:
                return "WEIGHTED_SUM";
            case FusionStrategy::RECIPROCAL_RANK:
                return "RECIPROCAL_RANK";
            case FusionStrategy::WEIGHTED_RECIPROCAL:
                return "WEIGHTED_RECIPROCAL";
            case FusionStrategy::COMB_MNZ:
                return "COMB_MNZ";
            case FusionStrategy::CONVEX:
                return "CONVEX";
            case FusionStrategy::WEIGHTED_LINEAR_ZSCORE:
                return "WEIGHTED_LINEAR_ZSCORE";
        }
        return "UNKNOWN";
    }

    size_t textMaxResults = 300;
    size_t pathTreeMaxResults = 150;
    size_t kgMaxResults = 100;
    size_t vectorMaxResults = 150;
    size_t entityVectorMaxResults = 100;
    size_t tagMaxResults = 250;
    size_t metadataMaxResults = 200;
    size_t semanticBudgetVectorMaxResults = 32;
    size_t semanticBudgetEntityVectorMaxResults = 16;

    bool useConnectionPriority = true;
    size_t minChunkSizeForParallel = 50;
    bool symbolRank = true;
    bool includeDebugInfo = false;
    bool includeComponentTiming = false;

    bool enableReranking = true;
    size_t rerankTopK = 5;
    float rerankAnchoredMinRelativeScore = 0.0f;
    bool rerankReplaceScores = false;
    float rerankBlendWeight = 0.30f;
    float rerankScoreGapThreshold = 0.0f;
    size_t rerankSnippetMaxChars = 256;
    size_t fusionCandidateLimit = 0;

    bool enableGraphRerank = false;
    size_t graphRerankTopN = 25;
    float graphRerankWeight = 0.15f;
    float graphRerankMaxBoost = 0.20f;
    float graphRerankMinSignal = 0.01f;
    float graphCommunityWeight = 0.10f;
    float graphCommunityReferenceSize = 8.0f;
    float graphCommunityDecayHalfLifeDays = 0.0f;
    float graphCommunityMinEdgeWeight = 0.0f;
    bool graphUseQueryConcepts = true;
    bool graphFallbackToTopSignal = true;

    size_t graphMaxNeighbors = 16;
    size_t graphMaxHops = 1;
    int graphScoringBudgetMs = 10;
    bool graphEnablePathEnumeration = false;
    size_t graphMaxPaths = 32;
    float graphHopDecay = 0.90f;

    float graphEntitySignalWeight = 0.40F;
    float graphStructuralSignalWeight = 0.20F;
    float graphCoverageSignalWeight = 0.20F;
    float graphPathSignalWeight = 0.10F;
    float graphCorroborationFloor = 0.35F;

    static SearchEngineConfig forProfile(CorpusProfile profile) {
        SearchEngineConfig config;
        config.corpusProfile = profile;

        switch (profile) {
            case CorpusProfile::CODE:
                config.textWeight = 0.40f;
                config.pathTreeWeight = 0.15f;
                config.kgWeight = 0.10f;
                config.vectorWeight = 0.10f;
                config.entityVectorWeight = 0.15f;
                config.tagWeight = 0.05f;
                config.metadataWeight = 0.05f;
                break;
            case CorpusProfile::PROSE:
                config.textWeight = 0.45f;
                config.pathTreeWeight = 0.10f;
                config.kgWeight = 0.05f;
                config.vectorWeight = 0.25f;
                config.entityVectorWeight = 0.00f;
                config.tagWeight = 0.10f;
                config.metadataWeight = 0.05f;
                break;
            case CorpusProfile::DOCS:
                config.textWeight = 0.40f;
                config.pathTreeWeight = 0.15f;
                config.kgWeight = 0.10f;
                config.vectorWeight = 0.20f;
                config.entityVectorWeight = 0.05f;
                config.tagWeight = 0.05f;
                config.metadataWeight = 0.05f;
                break;
            case CorpusProfile::MIXED:
            case CorpusProfile::CUSTOM:
            default:
                break;
        }

        return config;
    }

    static CorpusProfile
    detectProfile(const std::unordered_map<std::string, int64_t>& extensionCounts) {
        if (extensionCounts.empty()) {
            return CorpusProfile::MIXED;
        }

        static const std::unordered_set<std::string> codeExtensions = {
            ".py", ".cpp", ".c",   ".h",     ".hpp",  ".cc", ".cxx", ".rs",  ".go",
            ".js", ".ts",  ".jsx", ".tsx",   ".java", ".kt", ".rb",  ".cs",  ".swift",
            ".m",  ".mm",  ".php", ".scala", ".lua",  ".pl", ".sh",  ".bash"};
        static const std::unordered_set<std::string> proseExtensions = {
            ".md",  ".txt",  ".pdf", ".docx", ".doc", ".rtf",
            ".tex", ".html", ".htm", ".xml",  ".rst", ".adoc"};

        int64_t totalDocs = 0;
        int64_t codeDocs = 0;
        int64_t proseDocs = 0;

        for (const auto& [ext, count] : extensionCounts) {
            totalDocs += count;
            std::string lowerExt = ext;
            std::transform(lowerExt.begin(), lowerExt.end(), lowerExt.begin(), ::tolower);
            if (codeExtensions.count(lowerExt)) {
                codeDocs += count;
            } else if (proseExtensions.count(lowerExt)) {
                proseDocs += count;
            }
        }

        if (totalDocs == 0) {
            return CorpusProfile::MIXED;
        }

        const float codeRatio = static_cast<float>(codeDocs) / static_cast<float>(totalDocs);
        const float proseRatio = static_cast<float>(proseDocs) / static_cast<float>(totalDocs);
        constexpr float kDominantThreshold = 0.60f;
        constexpr float kSignificantThreshold = 0.30f;

        if (codeRatio >= kDominantThreshold) {
            return CorpusProfile::CODE;
        }
        if (proseRatio >= kDominantThreshold) {
            return CorpusProfile::PROSE;
        }
        if (codeRatio >= kSignificantThreshold && proseRatio >= kSignificantThreshold) {
            return CorpusProfile::DOCS;
        }
        return CorpusProfile::MIXED;
    }

    // Per-query simeon bandit arm selection. When non-empty, the lexical
    // pipeline calls SimeonLexicalBackend::scoreBanditRouted() with this
    // arm name instead of the default strategy/router-based scoring.
    // Training-free at inference: the arm name is selected by TunerMAB
    // from qrel-free proxy rewards. Empty = use existing scoring path.
    std::string simeonBanditArm;

    /// When true, scale per-component result caps (textMaxResults,
    /// vectorMaxResults, etc.) per query based on signal strength.
    /// Narrow queries (1-2 terms) get reduced vector/graph budget;
    /// complex queries (4+ terms) get expanded fusion budget.
    /// Requires enableAdaptiveFusion to be true as well for budget-aware fusion.
    bool enableAdaptiveBudgeting = false;

    /// Scaling factors for adaptive per-query budgets.
    float narrowQueryVectorReduction = 0.5f;    // multiply vector caps for ≤ threshold
    float complexQueryFusionExpansion = 1.5f;   // multiply fusion caps for ≥ threshold
    std::size_t narrowQueryTokenThreshold = 2;  // ≤ this = narrow
    std::size_t complexQueryTokenThreshold = 4; // ≥ this = complex

    /// Reapply the operator-selected topology policy after corpus tuning.
    /// Topology routing is an opt-in execution policy, not a corpus-derived
    /// relevance weight, so tuning must not silently reset it to Disabled.
    void applyTopologyPolicyFrom(const SearchEngineConfig& source) noexcept {
        topologyRoutingMode = source.topologyRoutingMode;
        topologyVectorPolicy = source.topologyVectorPolicy;
        topologyRouteScoringMode = source.topologyRouteScoringMode;
        enableTopologyWeakQueryRouting = source.enableTopologyWeakQueryRouting;
        topologyMinClusters = source.topologyMinClusters;
        topologyMaxClusters = source.topologyMaxClusters;
        topologyMaxSeedDocuments = source.topologyMaxSeedDocuments;
        topologyAdaptiveProbeScoreGap = source.topologyAdaptiveProbeScoreGap;
        topologyNarrowMinBoundaryMargin = source.topologyNarrowMinBoundaryMargin;
        topologyMaxDocs = source.topologyMaxDocs;
        topologyMaxDocsPerCluster = source.topologyMaxDocsPerCluster;
        topologyMedoidBoost = source.topologyMedoidBoost;
        topologySparseDenseAlpha = source.topologySparseDenseAlpha;
        topologyMinRouteScore = source.topologyMinRouteScore;
        topologyMedoidOnlyExpansion = source.topologyMedoidOnlyExpansion;
        topologyExpansionSource = source.topologyExpansionSource;
        topologyGraphNeighborMinScore = source.topologyGraphNeighborMinScore;
        topologyGraphNeighborReciprocalOnly = source.topologyGraphNeighborReciprocalOnly;
        topologyGraphVectorSeedProbe = source.topologyGraphVectorSeedProbe;
        topologySidecarFusionRescueSlots = source.topologySidecarFusionRescueSlots;
        topologySidecarFusionRescueMinScore = source.topologySidecarFusionRescueMinScore;
    }

    // Per-query multi-armed bandit arm selections. When non-empty, the
    // tuning pipeline overrides the corresponding static config value with
    // the MAB-selected arm. These extend the existing simeonBanditArm pattern
    // to fusion strategy, vector-only threshold, and rerank top-K.
    // Empty = use the static config value (no MAB override active).
    std::string mabFusionStrategyArm;
    std::string mabVectorOnlyThresholdArm;
    std::string mabRerankTopKArm;
};

} // namespace yams::search
