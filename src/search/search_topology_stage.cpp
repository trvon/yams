#include <yams/search/search_topology_stage.h>

#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>

namespace yams::search {

namespace {

bool isLexicalTopologySeed(ComponentResult::Source source) noexcept {
    return source == ComponentResult::Source::Text ||
           source == ComponentResult::Source::SimeonText ||
           source == ComponentResult::Source::GraphText;
}

} // namespace

std::vector<yams::topology::WeightedDocumentSeed>
rankTopologySeedEvidence(const std::vector<ComponentResult>& components, std::size_t maxSeeds) {
    if (maxSeeds == 0) {
        return {};
    }
    std::unordered_map<std::string, float> bestWeight;
    bestWeight.reserve(std::min(maxSeeds * 2, components.size()));
    for (const auto& component : components) {
        if (component.documentHash.empty() || !isLexicalTopologySeed(component.source) ||
            !std::isfinite(component.score) || component.score <= 0.0F) {
            continue;
        }
        const auto rank = component.rank + 1;
        const float discount = 1.0F + std::log1p(static_cast<float>(rank));
        const float weight = component.score / discount;
        auto [it, inserted] = bestWeight.emplace(component.documentHash, weight);
        if (!inserted) {
            it->second = std::max(it->second, weight);
        }
    }

    std::vector<yams::topology::WeightedDocumentSeed> out;
    out.reserve(bestWeight.size());
    for (const auto& [hash, weight] : bestWeight) {
        out.push_back({.documentHash = hash, .weight = weight});
    }
    std::sort(out.begin(), out.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.weight != rhs.weight) {
            return lhs.weight > rhs.weight;
        }
        return lhs.documentHash < rhs.documentHash;
    });
    if (out.size() > maxSeeds) {
        out.resize(maxSeeds);
    }
    return out;
}

std::vector<std::string> mergeTopologySeedHashes(const std::vector<std::string>& tier1Seeds,
                                                 const std::vector<std::string>& vectorSeeds,
                                                 std::size_t maxVectorSeeds) {
    std::vector<std::string> out;
    out.reserve(tier1Seeds.size() + std::min(maxVectorSeeds, vectorSeeds.size()));
    std::unordered_set<std::string> seen;
    seen.reserve(tier1Seeds.size() + maxVectorSeeds);

    auto pushUnique = [&](const std::string& hash) {
        if (hash.empty() || !seen.insert(hash).second) {
            return;
        }
        out.push_back(hash);
    };

    for (const auto& hash : tier1Seeds) {
        pushUnique(hash);
    }
    // 0 = add none (not unlimited).
    if (maxVectorSeeds == 0 || vectorSeeds.empty()) {
        return out;
    }
    std::size_t vectorAdded = 0;
    for (const auto& hash : vectorSeeds) {
        if (vectorAdded >= maxVectorSeeds) {
            break;
        }
        const auto before = out.size();
        pushUnique(hash);
        if (out.size() > before) {
            ++vectorAdded;
        }
    }
    return out;
}

void fillTopologySkipReason(std::string& skipReason,
                            SearchEngineConfig::TopologyRoutingMode routingMode,
                            bool weakTier1Query, bool routingEnabled, bool hasStores,
                            bool sessionApplied, bool loadSucceeded, std::size_t routedClusters) {
    if (!skipReason.empty()) {
        return;
    }
    using Mode = SearchEngineConfig::TopologyRoutingMode;
    if (!routingEnabled) {
        skipReason = "disabled";
        return;
    }
    if (routingMode == Mode::WeakQueryOnly && !weakTier1Query) {
        skipReason = "strong_tier1_query";
        return;
    }
    if (!hasStores) {
        skipReason = "missing_store";
        return;
    }
    if (sessionApplied) {
        return;
    }
    if (loadSucceeded && routedClusters > 0) {
        skipReason = "no_added_candidates";
        return;
    }
    skipReason = loadSucceeded ? "no_routes" : "not_loaded";
}

TopologyAssistStageResult runTopologyAssistStage(const TopologyAssistStageRequest& request) {
    TopologyAssistStageResult out;
    const bool routingEnabled =
        request.routingMode != SearchEngineConfig::TopologyRoutingMode::Disabled;

    const bool useVectorSeeds = request.config.topologyExpansionSource ==
                                    SearchEngineConfig::TopologyExpansionSource::GraphNeighbors &&
                                !request.vectorSeedHashes.empty();

    out.enrichedSeedHashes =
        useVectorSeeds ? mergeTopologySeedHashes(request.tier1SeedHashes, request.vectorSeedHashes,
                                                 request.maxVectorSeeds)
                       : request.tier1SeedHashes;
    if (useVectorSeeds) {
        std::unordered_set<std::string> tier1(request.tier1SeedHashes.begin(),
                                              request.tier1SeedHashes.end());
        out.vectorSeedsAdded = static_cast<std::size_t>(
            std::count_if(out.enrichedSeedHashes.begin(), out.enrichedSeedHashes.end(),
                          [&](const std::string& h) { return !tier1.contains(h); }));
    }

    TopologyRoutingSessionRequest sessionRequest;
    sessionRequest.query = request.query;
    sessionRequest.seedDocumentHashes = out.enrichedSeedHashes;
    sessionRequest.weightedSeedDocuments = request.tier1SeedEvidence;
    sessionRequest.existingCandidateHashes = request.existingCandidateHashes;
    sessionRequest.queryEmbedding = request.queryEmbedding;
    sessionRequest.routingMode = request.routingMode;
    sessionRequest.routeScoringMode = request.config.topologyRouteScoringMode;
    sessionRequest.expansionSource = request.config.topologyExpansionSource;
    sessionRequest.weakTier1Query = request.weakTier1Query;
    sessionRequest.minClusters = request.config.topologyMinClusters;
    sessionRequest.maxClusters = request.config.topologyMaxClusters;
    sessionRequest.representativeLimit = request.config.topologyRoutingRepresentativeLimit;
    sessionRequest.adaptiveProbeScoreGap = request.config.topologyAdaptiveProbeScoreGap;
    sessionRequest.narrowMinBoundaryMargin = request.config.topologyNarrowMinBoundaryMargin;
    sessionRequest.maxDocs = request.config.topologyMaxDocs;
    sessionRequest.sparseDenseAlpha = request.config.topologySparseDenseAlpha;
    sessionRequest.minRouteScore = request.config.topologyMinRouteScore;
    sessionRequest.collectRouteMembership = true;
    sessionRequest.graphNeighborMinScore = request.config.topologyGraphNeighborMinScore;
    sessionRequest.graphNeighborReciprocalOnly = request.config.topologyGraphNeighborReciprocalOnly;
    sessionRequest.expectedTopologyEpoch = request.expectedTopologyEpoch;
    sessionRequest.snapshotCache = request.snapshotCache;

    out.session = runTopologyRoutingSession(sessionRequest, request.metadataRepo, request.kgStore);
    out.skipReason = out.session.skipReason;

    fillTopologySkipReason(
        out.skipReason, request.routingMode, request.weakTier1Query, routingEnabled,
        static_cast<bool>(request.metadataRepo && request.kgStore), out.session.applied,
        out.session.loadSucceeded, out.session.routedClusters);
    return out;
}

} // namespace yams::search
