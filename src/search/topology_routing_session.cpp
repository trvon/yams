#include <yams/search/topology_routing_session.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_metadata_store.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iterator>
#include <ranges>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::search {

TopologyRouteSelection
selectTopologyRoutesForNarrowing(const std::vector<yams::topology::ClusterRoute>& routes,
                                 std::size_t minClusters, std::size_t maxClusters,
                                 float adaptiveScoreGap, float minBoundaryMargin) {
    TopologyRouteSelection selection;
    selection.availableRoutes = routes.size();
    if (routes.empty()) {
        return selection;
    }

    const std::size_t effectiveMax =
        maxClusters == 0 ? routes.size() : std::min(maxClusters, routes.size());
    const std::size_t effectiveMin = std::min(effectiveMax, std::max<std::size_t>(1, minClusters));
    std::size_t selectedCount = effectiveMax;
    if (adaptiveScoreGap > 0.0F) {
        selectedCount = effectiveMin;
        const double bestScore = routes.front().routeScore;
        while (selectedCount < effectiveMax && bestScore - routes[selectedCount].routeScore <=
                                                   static_cast<double>(adaptiveScoreGap)) {
            ++selectedCount;
        }
    }

    selection.routes.assign(routes.begin(), routes.begin() + selectedCount);
    if (selectedCount < routes.size()) {
        selection.boundaryScoreMargin = static_cast<float>(routes[selectedCount - 1].routeScore -
                                                           routes[selectedCount].routeScore);
        selection.abstained =
            minBoundaryMargin > 0.0F && selection.boundaryScoreMargin < minBoundaryMargin;
    }
    return selection;
}

namespace {

constexpr std::string_view kDocNodePrefix = "doc:";
constexpr std::string_view kSemanticNeighborRelation = "semantic_neighbor";

using NeighborTriple = std::tuple<std::string, float, bool>; // hash, score, reciprocal
using SeedNeighborMap = std::unordered_map<std::string, std::vector<NeighborTriple>>;

std::string nodeKeyForDocumentHash(std::string_view documentHash) {
    return std::string{kDocNodePrefix} + std::string{documentHash};
}

std::optional<std::string> documentHashFromNodeKey(std::string_view nodeKey) {
    if (!nodeKey.starts_with(kDocNodePrefix) || nodeKey.size() <= kDocNodePrefix.size()) {
        return std::nullopt;
    }
    return std::string{nodeKey.substr(kDocNodePrefix.size())};
}

std::int64_t microsSince(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                                 start)
        .count();
}

/// Collect bidirectional semantic_neighbor edges for one document hash.
/// Returns nullopt when the node is missing or edge lookup fails hard.
std::optional<std::vector<NeighborTriple>>
collectSemanticNeighbors(yams::metadata::KnowledgeGraphStore& kgStore,
                         std::string_view documentHash, std::size_t edgeLimit) {
    if (documentHash.empty()) {
        return std::nullopt;
    }
    auto nodeResult = kgStore.getNodeByKey(nodeKeyForDocumentHash(documentHash));
    if (!nodeResult || !nodeResult.value().has_value()) {
        return std::nullopt;
    }
    const auto nodeId = nodeResult.value()->id;
    auto edgeResult = kgStore.getEdgesBidirectional(nodeId, kSemanticNeighborRelation, edgeLimit);
    if (!edgeResult) {
        return std::nullopt;
    }

    std::unordered_map<std::int64_t, float> scores;
    std::unordered_set<std::int64_t> fromSeed;
    std::unordered_set<std::int64_t> toSeed;
    std::vector<std::int64_t> neighborIds;
    neighborIds.reserve(edgeResult.value().size());

    auto noteNeighbor = [&](std::int64_t otherId, float weight, bool outbound) {
        if (otherId == nodeId) {
            return;
        }
        if (outbound) {
            fromSeed.insert(otherId);
        } else {
            toSeed.insert(otherId);
        }
        auto [it, inserted] = scores.try_emplace(otherId, weight);
        if (inserted) {
            neighborIds.push_back(otherId);
        } else {
            it->second = std::max(it->second, weight);
        }
    };

    for (const auto& edge : edgeResult.value()) {
        if (edge.srcNodeId == nodeId) {
            noteNeighbor(edge.dstNodeId, edge.weight, /*outbound=*/true);
        }
        if (edge.dstNodeId == nodeId) {
            noteNeighbor(edge.srcNodeId, edge.weight, /*outbound=*/false);
        }
    }
    if (neighborIds.empty()) {
        return std::vector<NeighborTriple>{};
    }

    auto nodesResult = kgStore.getNodesByIds(neighborIds);
    if (!nodesResult) {
        return std::nullopt;
    }

    std::vector<NeighborTriple> out;
    out.reserve(nodesResult.value().size());
    for (const auto& node : nodesResult.value()) {
        auto docHash = documentHashFromNodeKey(node.nodeKey);
        if (!docHash.has_value()) {
            continue;
        }
        const auto scoreIt = scores.find(node.id);
        if (scoreIt == scores.end()) {
            continue;
        }
        const bool reciprocal = fromSeed.contains(node.id) && toSeed.contains(node.id);
        out.emplace_back(std::move(*docHash), scoreIt->second, reciprocal);
    }
    return out;
}

SeedNeighborMap collectSeedNeighborMap(yams::metadata::KnowledgeGraphStore& kgStore,
                                       const std::vector<std::string>& seedHashes,
                                       std::size_t edgeLimit) {
    SeedNeighborMap seedNeighbors;
    seedNeighbors.reserve(seedHashes.size());
    for (const auto& seedHash : seedHashes) {
        auto neighbors = collectSemanticNeighbors(kgStore, seedHash, edgeLimit);
        if (!neighbors.has_value() || neighbors->empty()) {
            continue;
        }
        seedNeighbors.emplace(seedHash, std::move(*neighbors));
    }
    return seedNeighbors;
}

std::size_t edgeFetchLimit(std::size_t maxDocs) {
    return std::max<std::size_t>(maxDocs * 8, 64);
}

std::shared_ptr<TopologyRoutingSnapshotCache>
makeSnapshotCache(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                  const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore) {
    auto store =
        std::make_shared<yams::topology::MetadataKgTopologyArtifactStore>(metadataRepo, kgStore);
    return std::make_shared<TopologyRoutingSnapshotCache>(
        [store]() { return store->loadLatest(); });
}

bool loadRoutingSnapshot(const TopologyRoutingSessionRequest& request,
                         const std::shared_ptr<TopologyRoutingSnapshotCache>& snapshotCache,
                         TopologyRoutingSessionResult& result,
                         std::shared_ptr<const TopologyRoutingSnapshot>& snapshot) {
    const auto loadStart = std::chrono::steady_clock::now();
    auto lookup = snapshotCache->get(request.expectedTopologyEpoch);
    result.timings.loadMicros += microsSince(loadStart);
    if (!lookup) {
        const auto& message = lookup.error().message;
        if (message.starts_with("invalid_artifact:")) {
            result.loadSucceeded = true;
            result.skipReason = message;
        } else {
            result.skipReason = std::string{"load_failed:"} + message;
        }
        return false;
    }
    if (!lookup.value().snapshot) {
        result.skipReason = "no_snapshot";
        return false;
    }

    snapshot = lookup.value().snapshot;
    result.snapshotCacheHit = lookup.value().cacheHit;
    result.loadSucceeded = true;
    result.artifactAdmitted = true;
    result.artifactsFresh = request.expectedTopologyEpoch == 0 ||
                            snapshot->artifacts->topologyEpoch == request.expectedTopologyEpoch;
    result.topologyEpoch = snapshot->artifacts->topologyEpoch;
    return true;
}

/// Dense route → medoid anchors → pure graph neighbors of those medoids.
struct MedoidGraphExpandResult {
    std::vector<std::string> ranked;
    std::size_t routedClusters{0};
};

MedoidGraphExpandResult
tryMedoidGraphExpansion(const TopologyRoutingSessionRequest& request,
                        const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
                        const std::shared_ptr<TopologyRoutingSnapshotCache>& snapshotCache,
                        TopologyRoutingSessionResult& result) {
    MedoidGraphExpandResult out;
    std::shared_ptr<const TopologyRoutingSnapshot> snapshot;
    if (!loadRoutingSnapshot(request, snapshotCache, result, snapshot)) {
        return out;
    }
    const auto& topology = *snapshot->artifacts;

    yams::topology::TopologyRouteRequest routeRequest;
    routeRequest.queryText = request.query;
    routeRequest.seedDocumentHashes = request.seedDocumentHashes;
    routeRequest.weightedSeedDocuments = request.weightedSeedDocuments;
    routeRequest.limit = request.maxClusters > 0 ? request.maxClusters : 2;
    routeRequest.weakQueryOnly =
        request.routingMode == SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly;
    routeRequest.scoringMode = topologyRouteScoringMode(request.routeScoringMode);
    routeRequest.sparseDenseAlpha = std::clamp(request.sparseDenseAlpha, 0.0F, 1.0F);
    if (request.queryEmbedding.has_value()) {
        routeRequest.queryEmbedding = request.queryEmbedding.value();
    }

    yams::topology::SparseGuidedClusterRouter router;
    auto routes = router.route(routeRequest, topology, snapshot->sparseRouteIndex);
    if (!routes) {
        return out;
    }
    out.routedClusters = routes.value().size();

    SeedNeighborMap medoidNeighbors;
    std::vector<std::string> medoidSeeds;
    medoidSeeds.reserve(routes.value().size());
    const auto limit = edgeFetchLimit(request.maxDocs);

    for (const auto& route : routes.value()) {
        if (route.routeScore < request.minRouteScore) {
            continue;
        }
        if (!route.medoidDocumentHash.has_value() || route.medoidDocumentHash->empty()) {
            continue;
        }
        const auto& medoidHash = *route.medoidDocumentHash;
        medoidSeeds.push_back(medoidHash);
        auto neighbors = collectSemanticNeighbors(*kgStore, medoidHash, limit);
        if (!neighbors.has_value() || neighbors->empty()) {
            continue;
        }
        medoidNeighbors.emplace(medoidHash, std::move(*neighbors));
    }

    out.ranked = rankGraphNeighborCandidates(medoidNeighbors, medoidSeeds, request.maxDocs,
                                             request.graphNeighborMinScore,
                                             /*reciprocalOnly=*/false);
    return out;
}

void admitRankedCandidates(TopologyRoutingSessionResult& result,
                           const TopologyRoutingSessionRequest& request,
                           const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                           const std::vector<std::string>& ranked) {
    result.artifactAdmitted = true;
    result.routedDocs = ranked.size();
    std::unordered_set<std::string> candidateHashes = request.existingCandidateHashes;
    candidateHashes.reserve(candidateHashes.size() + ranked.size());

    for (const auto& hash : ranked) {
        const auto docLookupStart = std::chrono::steady_clock::now();
        auto docLookup = metadataRepo->getDocumentByHash(hash);
        result.timings.docLookupMicros += microsSince(docLookupStart);
        if (!docLookup || !docLookup.value().has_value()) {
            ++result.staleCandidates;
            continue;
        }
        if (request.collectRouteMembership) {
            result.routeAllowedDocumentHashes.insert(hash);
        }
        result.routedCandidateHashes.insert(hash);
        const auto insertStart = std::chrono::steady_clock::now();
        const auto [_, inserted] = candidateHashes.insert(hash);
        result.timings.candidateInsertMicros += microsSince(insertStart);
        if (inserted) {
            ++result.addedCandidates;
            result.addedCandidateHashes.push_back(hash);
        } else {
            ++result.duplicateCandidates;
        }
    }
    result.narrowApplied = request.collectRouteMembership
                               ? !result.routeAllowedDocumentHashes.empty()
                               : !result.routedCandidateHashes.empty();
    result.applied = result.narrowApplied || result.addedCandidates > 0;
}

/// Returns true when graph expansion finished the session (caller should return).
/// Returns false to fall through to cluster membership expansion.
bool tryRunGraphNeighborExpansion(
    TopologyRoutingSessionResult& result, const TopologyRoutingSessionRequest& request,
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore, bool mayExpand,
    const std::shared_ptr<TopologyRoutingSnapshotCache>& snapshotCache,
    std::chrono::steady_clock::time_point totalStart) {
    if (request.expansionSource != SearchEngineConfig::TopologyExpansionSource::GraphNeighbors) {
        return false;
    }

    const auto loadStart = std::chrono::steady_clock::now();
    auto seedNeighbors = collectSeedNeighborMap(*kgStore, request.seedDocumentHashes,
                                                edgeFetchLimit(request.maxDocs));
    result.timings.loadMicros = microsSince(loadStart);
    result.loadSucceeded = true;
    result.artifactsFresh = true;

    const auto routeStart = std::chrono::steady_clock::now();
    auto ranked = rankGraphNeighborCandidates(seedNeighbors, request.seedDocumentHashes,
                                              request.maxDocs, request.graphNeighborMinScore,
                                              request.graphNeighborReciprocalOnly);
    result.timings.routeMicros = microsSince(routeStart);
    result.routedDocs = ranked.size();
    result.routedClusters = 0;

    if (!mayExpand) {
        result.artifactAdmitted = true;
        result.skipReason = "mode_disallows_expansion";
        result.timings.totalMicros = microsSince(totalStart);
        return true;
    }

    if (ranked.empty()) {
        auto medoidExpand = tryMedoidGraphExpansion(request, kgStore, snapshotCache, result);
        result.routedClusters = medoidExpand.routedClusters;
        ranked = std::move(medoidExpand.ranked);
        if (!ranked.empty()) {
            result.skipReason = "graph_medoid_neighbors";
        }
    }

    if (ranked.empty()) {
        result.skipReason = "graph_no_neighbors_fallback_clusters";
        return false;
    }

    admitRankedCandidates(result, request, metadataRepo, ranked);
    if (!result.applied) {
        result.skipReason = "graph_all_duplicates";
    } else if (result.skipReason.empty()) {
        result.skipReason = "graph_seed_neighbors";
    }
    result.timings.totalMicros = microsSince(totalStart);
    return true;
}

std::vector<std::string> selectClusterExpansionHashes(
    const yams::topology::ClusterArtifact& cluster, const yams::topology::ClusterRoute& route,
    const TopologyRoutingSessionRequest& request, const TopologyMemberReranker& memberReranker) {
    if (request.medoidOnlyExpansion && route.medoidDocumentHash.has_value()) {
        return {*route.medoidDocumentHash};
    }
    if (memberReranker) {
        const std::size_t limit =
            request.perClusterLimit > 0
                ? request.perClusterLimit
                : (request.maxDocs > 0 ? request.maxDocs : cluster.memberDocumentHashes.size());
        auto expansionHashes = memberReranker(cluster.memberDocumentHashes, limit);
        if (!expansionHashes.empty()) {
            return expansionHashes;
        }
    }
    if (route.medoidDocumentHash.has_value()) {
        return {*route.medoidDocumentHash};
    }
    return {};
}

void coverSeedsInCluster(const yams::topology::ClusterArtifact& cluster,
                         const std::unordered_set<std::string>& seedSet,
                         std::unordered_set<std::string>& seedsCovered) {
    if (seedSet.empty()) {
        return;
    }
    for (const auto& hash : cluster.memberDocumentHashes) {
        if (seedSet.contains(hash)) {
            seedsCovered.insert(hash);
        }
    }
}

TopologyRoutingSessionResult
runClusterArtifactExpansion(const TopologyRoutingSessionRequest& request,
                            const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                            const std::shared_ptr<TopologyRoutingSnapshotCache>& snapshotCache,
                            const TopologyMemberReranker& memberReranker, bool mayExpand,
                            TopologyRoutingSessionResult result,
                            std::chrono::steady_clock::time_point totalStart) {
    std::shared_ptr<const TopologyRoutingSnapshot> snapshot;
    if (!loadRoutingSnapshot(request, snapshotCache, result, snapshot)) {
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }
    const auto& topology = *snapshot->artifacts;

    const auto requestPrepStart = std::chrono::steady_clock::now();
    yams::topology::TopologyRouteRequest routeRequest;
    routeRequest.queryText = request.query;
    routeRequest.seedDocumentHashes = request.seedDocumentHashes;
    routeRequest.weightedSeedDocuments = request.weightedSeedDocuments;
    const bool needsBoundaryRoute =
        request.adaptiveProbeScoreGap > 0.0F || request.narrowMinBoundaryMargin > 0.0F;
    routeRequest.limit = request.maxClusters > 0 && needsBoundaryRoute
                             ? request.maxClusters + static_cast<std::size_t>(1)
                             : request.maxClusters;
    routeRequest.weakQueryOnly =
        request.routingMode == SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly;
    routeRequest.scoringMode = topologyRouteScoringMode(request.routeScoringMode);
    routeRequest.sparseDenseAlpha = std::clamp(request.sparseDenseAlpha, 0.0F, 1.0F);
    if (request.queryEmbedding.has_value()) {
        routeRequest.queryEmbedding = request.queryEmbedding.value();
    }
    result.timings.requestPrepMicros = microsSince(requestPrepStart);

    yams::topology::SparseGuidedClusterRouter router;
    const auto sparseRouteStart = std::chrono::steady_clock::now();
    auto routes = router.route(routeRequest, topology, snapshot->sparseRouteIndex);
    result.timings.routeMicros = microsSince(sparseRouteStart);
    if (!routes) {
        result.skipReason = std::string{"route_failed:"} + routes.error().message;
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }

    std::vector<yams::topology::ClusterRoute> eligibleRoutes;
    eligibleRoutes.reserve(routes.value().size());
    for (const auto& route : routes.value()) {
        if (route.routeScore < request.minRouteScore) {
            ++result.routesRejected;
            continue;
        }
        eligibleRoutes.push_back(route);
    }
    auto selection = selectTopologyRoutesForNarrowing(
        eligibleRoutes, request.minClusters, request.maxClusters, request.adaptiveProbeScoreGap,
        request.narrowMinBoundaryMargin);
    result.availableRoutes = selection.availableRoutes;
    result.routeBoundaryScoreMargin = selection.boundaryScoreMargin;
    result.confidenceAbstained = selection.abstained;
    result.routedClusters = selection.routes.size();
    if (selection.abstained) {
        result.artifactAdmitted = true;
        result.skipReason = "route_low_confidence";
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }

    std::unordered_set<std::string> candidateHashes = request.existingCandidateHashes;
    const std::unordered_set<std::string> seedSet(request.seedDocumentHashes.begin(),
                                                  request.seedDocumentHashes.end());
    std::unordered_set<std::string> seedsCovered;
    double acceptedRouteScoreSum = 0.0;
    result.seedCount = seedSet.size();
    for (const auto& route : selection.routes) {
        ++result.acceptedRoutes;
        acceptedRouteScoreSum += route.routeScore;
        result.bestRouteScore =
            std::max(result.bestRouteScore, static_cast<float>(route.routeScore));
        if (route.medoidDocumentHash.has_value()) {
            result.medoidHashes.insert(*route.medoidDocumentHash);
        }

        const auto clusterLookupStart = std::chrono::steady_clock::now();
        const auto clusterIt = snapshot->clustersById.find(route.clusterId);
        result.timings.clusterLookupMicros += microsSince(clusterLookupStart);
        if (clusterIt == snapshot->clustersById.end()) {
            continue;
        }
        const auto& cluster = topology.clusters[clusterIt->second];
        coverSeedsInCluster(cluster, seedSet, seedsCovered);
        if (request.collectRouteMembership && mayExpand) {
            for (const auto& hash : cluster.memberDocumentHashes) {
                if (!hash.empty()) {
                    result.routeAllowedDocumentHashes.insert(hash);
                }
            }
        }

        if (memberReranker && !request.medoidOnlyExpansion) {
            result.memberRerankCandidates += cluster.memberDocumentHashes.size();
        }
        const auto expansionHashes =
            selectClusterExpansionHashes(cluster, route, request, memberReranker);
        if (memberReranker && !request.medoidOnlyExpansion) {
            result.memberRerankSelected += expansionHashes.size();
        }
        for (const auto& hash : expansionHashes) {
            if (request.maxDocs > 0 && result.routedDocs >= request.maxDocs) {
                break;
            }
            ++result.routedDocs;
            const auto docLookupStart = std::chrono::steady_clock::now();
            auto docLookup = metadataRepo->getDocumentByHash(hash);
            result.timings.docLookupMicros += microsSince(docLookupStart);
            if (!docLookup || !docLookup.value().has_value()) {
                ++result.staleCandidates;
                continue;
            }
            if (!mayExpand) {
                continue;
            }
            result.routedCandidateHashes.insert(hash);
            const auto insertStart = std::chrono::steady_clock::now();
            const auto [_, inserted] = candidateHashes.insert(hash);
            result.timings.candidateInsertMicros += microsSince(insertStart);
            if (inserted) {
                ++result.addedCandidates;
                result.addedCandidateHashes.push_back(hash);
            } else {
                ++result.duplicateCandidates;
            }
        }
        if (!request.collectRouteMembership && request.maxDocs > 0 &&
            result.routedDocs >= request.maxDocs) {
            break;
        }
    }

    result.seedsInRoutedClusters = seedsCovered.size();
    if (result.acceptedRoutes > 0) {
        result.meanAcceptedRouteScore =
            static_cast<float>(acceptedRouteScoreSum / static_cast<double>(result.acceptedRoutes));
    }
    result.narrowApplied =
        mayExpand && (request.collectRouteMembership ? !result.routeAllowedDocumentHashes.empty()
                                                     : !result.routedCandidateHashes.empty());
    result.applied = result.narrowApplied || result.addedCandidates > 0;
    // Preserve graph fallthrough skip reason when cluster path also fails.
    if (result.applied && result.skipReason == "graph_no_neighbors_fallback_clusters") {
        result.skipReason.clear();
    }
    result.timings.totalMicros = microsSince(totalStart);
    return result;
}

} // namespace

std::vector<std::string> rankGraphNeighborCandidates(
    const std::unordered_map<std::string, std::vector<std::tuple<std::string, float, bool>>>&
        seedNeighbors,
    const std::vector<std::string>& seedDocumentHashes, std::size_t maxDocs, float minScore,
    bool reciprocalOnly) {
    struct Cand {
        std::string hash;
        float score{0.0F};
        std::size_t seedHits{0};
    };
    std::unordered_map<std::string, Cand> byHash;
    byHash.reserve(seedDocumentHashes.size() * 8);

    for (const auto& seed : seedDocumentHashes) {
        const auto it = seedNeighbors.find(seed);
        if (it == seedNeighbors.end()) {
            continue;
        }
        for (const auto& [nbr, score, reciprocal] : it->second) {
            if (nbr.empty() || score < minScore || (reciprocalOnly && !reciprocal)) {
                continue;
            }
            auto& cand = byHash[nbr];
            if (cand.hash.empty()) {
                cand.hash = nbr;
            }
            cand.score = std::max(cand.score, score);
            ++cand.seedHits;
        }
    }

    for (const auto& seed : seedDocumentHashes) {
        byHash.erase(seed);
    }

    std::vector<Cand> ranked;
    ranked.reserve(byHash.size());
    std::ranges::transform(byHash, std::back_inserter(ranked),
                           [](auto& entry) { return std::move(entry.second); });
    std::ranges::sort(ranked, [](const Cand& a, const Cand& b) {
        if (a.seedHits != b.seedHits) {
            return a.seedHits > b.seedHits;
        }
        if (a.score != b.score) {
            return a.score > b.score;
        }
        return a.hash < b.hash;
    });

    const std::size_t take = (maxDocs == 0) ? ranked.size() : std::min(maxDocs, ranked.size());
    std::vector<std::string> out;
    out.reserve(take);
    std::ranges::transform(ranked | std::views::take(take), std::back_inserter(out),
                           [](const Cand& c) { return c.hash; });
    return out;
}

SearchEngineConfig::TopologyRoutingMode
resolveTopologyRoutingMode(const SearchEngineConfig& config) noexcept {
    using Mode = SearchEngineConfig::TopologyRoutingMode;
    if (config.topologyRoutingMode != Mode::Disabled) {
        return config.topologyRoutingMode;
    }
    return config.enableTopologyWeakQueryRouting ? Mode::WeakQueryOnly : Mode::Disabled;
}

bool topologyRoutingMayLoad(SearchEngineConfig::TopologyRoutingMode mode,
                            bool weakTier1Query) noexcept {
    using Mode = SearchEngineConfig::TopologyRoutingMode;
    switch (mode) {
        case Mode::Disabled:
            return false;
        case Mode::WeakQueryOnly:
            return weakTier1Query;
        case Mode::HybridAssist:
        case Mode::RerankOnly:
            return true;
    }
    return false;
}

bool topologyRoutingMayExpand(SearchEngineConfig::TopologyRoutingMode mode,
                              bool weakTier1Query) noexcept {
    using Mode = SearchEngineConfig::TopologyRoutingMode;
    switch (mode) {
        case Mode::Disabled:
        case Mode::RerankOnly:
            return false;
        case Mode::WeakQueryOnly:
            return weakTier1Query;
        case Mode::HybridAssist:
            return true;
    }
    return false;
}

yams::topology::RouteScoringMode
topologyRouteScoringMode(SearchEngineConfig::TopologyRouteScoringMode mode) noexcept {
    using Mode = SearchEngineConfig::TopologyRouteScoringMode;
    switch (mode) {
        case Mode::Current:
            return yams::topology::RouteScoringMode::Current;
        case Mode::SizeWeighted:
            return yams::topology::RouteScoringMode::SizeWeighted;
        case Mode::SeedCoverage:
            return yams::topology::RouteScoringMode::SeedCoverage;
    }
    return yams::topology::RouteScoringMode::Current;
}

std::optional<std::string>
validateTopologyArtifactBatchForRouting(const yams::topology::TopologyArtifactBatch& batch) {
    std::unordered_set<std::string> membershipHashes;
    membershipHashes.reserve(batch.memberships.size());
    std::unordered_set<std::string> clusterIds;
    clusterIds.reserve(batch.clusters.size());
    std::unordered_map<std::string, std::unordered_set<std::string>> clusterMembersById;
    clusterMembersById.reserve(batch.clusters.size());

    for (const auto& membership : batch.memberships) {
        if (membership.documentHash.empty()) {
            return "empty_membership_hash";
        }
        if (membership.clusterId.empty()) {
            return "empty_membership_cluster";
        }
        if (!membershipHashes.insert(membership.documentHash).second) {
            return "duplicate_membership_hash";
        }
    }

    for (const auto& cluster : batch.clusters) {
        if (cluster.clusterId.empty()) {
            return "empty_cluster_id";
        }
        if (!clusterIds.insert(cluster.clusterId).second) {
            return "duplicate_cluster_id";
        }
        if (cluster.memberCount != cluster.memberDocumentHashes.size()) {
            return "member_count_mismatch";
        }
        auto& members = clusterMembersById[cluster.clusterId];
        members.reserve(cluster.memberDocumentHashes.size());
        for (const auto& hash : cluster.memberDocumentHashes) {
            if (hash.empty()) {
                return "empty_cluster_member_hash";
            }
            if (!membershipHashes.contains(hash)) {
                return "cluster_member_without_membership";
            }
            if (!members.insert(hash).second) {
                return "duplicate_cluster_member_hash";
            }
        }
        if (cluster.medoid.has_value() && !members.contains(cluster.medoid->documentHash)) {
            return "medoid_not_cluster_member";
        }
    }

    for (const auto& membership : batch.memberships) {
        const auto clusterIt = clusterMembersById.find(membership.clusterId);
        if (clusterIt == clusterMembersById.end()) {
            return "membership_without_cluster";
        }
        if (!clusterIt->second.contains(membership.documentHash)) {
            return "membership_not_in_cluster_members";
        }
    }

    return std::nullopt;
}

TopologyRoutingSnapshotCache::TopologyRoutingSnapshotCache(TopologyRoutingSnapshotLoader loader)
    : loader_(std::move(loader)) {}

Result<TopologyRoutingSnapshotLookup>
TopologyRoutingSnapshotCache::get(std::uint64_t expectedEpoch) {
    std::lock_guard lock(mutex_);
    if (cached_ && (expectedEpoch == 0 || cached_->artifacts->topologyEpoch == expectedEpoch)) {
        return TopologyRoutingSnapshotLookup{.snapshot = cached_, .cacheHit = true};
    }
    if (!loader_) {
        return Error{ErrorCode::InvalidState, "topology snapshot cache requires a loader"};
    }

    auto loaded = loader_();
    if (!loaded) {
        return loaded.error();
    }
    if (!loaded.value().has_value()) {
        return TopologyRoutingSnapshotLookup{};
    }

    auto artifacts =
        std::make_shared<yams::topology::TopologyArtifactBatch>(std::move(loaded.value().value()));
    if (auto invalid = validateTopologyArtifactBatchForRouting(*artifacts); invalid.has_value()) {
        return Error{ErrorCode::InvalidData, std::string{"invalid_artifact:"} + *invalid};
    }
    if (expectedEpoch > 0 && artifacts->topologyEpoch != expectedEpoch) {
        return Error{ErrorCode::InvalidState,
                     "topology_epoch_mismatch:expected=" + std::to_string(expectedEpoch) +
                         ",actual=" + std::to_string(artifacts->topologyEpoch)};
    }

    auto snapshot = std::make_shared<TopologyRoutingSnapshot>();
    snapshot->artifacts = std::move(artifacts);
    snapshot->sparseRouteIndex =
        yams::topology::SparseGuidedClusterRouter::buildRouteIndex(*snapshot->artifacts);
    snapshot->clustersById.reserve(snapshot->artifacts->clusters.size());
    for (std::size_t index = 0; index < snapshot->artifacts->clusters.size(); ++index) {
        snapshot->clustersById.emplace(snapshot->artifacts->clusters[index].clusterId, index);
    }
    cached_ = std::move(snapshot);
    return TopologyRoutingSnapshotLookup{.snapshot = cached_, .cacheHit = false};
}

TopologyRoutingSessionResult
runTopologyRoutingSession(const TopologyRoutingSessionRequest& request,
                          const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                          const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
                          const TopologyMemberReranker& memberReranker) {
    TopologyRoutingSessionResult result;
    result.enabled = request.routingMode != SearchEngineConfig::TopologyRoutingMode::Disabled;
    result.loadAttempted = topologyRoutingMayLoad(request.routingMode, request.weakTier1Query);
    if (!result.loadAttempted) {
        return result;
    }
    if (!metadataRepo || !kgStore) {
        result.skipReason = "missing_topology_dependencies";
        return result;
    }

    const auto totalStart = std::chrono::steady_clock::now();
    const bool mayExpand = topologyRoutingMayExpand(request.routingMode, request.weakTier1Query);
    result.seedCount = request.seedDocumentHashes.size();
    const auto snapshotCache =
        request.snapshotCache ? request.snapshotCache : makeSnapshotCache(metadataRepo, kgStore);

    if (tryRunGraphNeighborExpansion(result, request, metadataRepo, kgStore, mayExpand,
                                     snapshotCache, totalStart)) {
        return result;
    }

    return runClusterArtifactExpansion(request, metadataRepo, snapshotCache, memberReranker,
                                       mayExpand, std::move(result), totalStart);
}

} // namespace yams::search
