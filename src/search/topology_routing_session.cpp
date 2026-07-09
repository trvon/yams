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
collectSemanticNeighbors(yams::metadata::KnowledgeGraphStore& kgStore, std::string_view documentHash,
                         std::size_t edgeLimit) {
    if (documentHash.empty()) {
        return std::nullopt;
    }
    auto nodeResult = kgStore.getNodeByKey(nodeKeyForDocumentHash(documentHash));
    if (!nodeResult || !nodeResult.value().has_value()) {
        return std::nullopt;
    }
    const auto nodeId = nodeResult.value()->id;
    auto edgeResult =
        kgStore.getEdgesBidirectional(nodeId, kSemanticNeighborRelation, edgeLimit);
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

/// Prefer reciprocal+score floor, then drop reciprocal, then drop score floor.
std::vector<std::string> rankNeighborsWithFallbacks(const SeedNeighborMap& seedNeighbors,
                                                    const std::vector<std::string>& seeds,
                                                    std::size_t maxDocs, float minScore,
                                                    bool reciprocalOnly) {
    auto ranked =
        rankGraphNeighborCandidates(seedNeighbors, seeds, maxDocs, minScore, reciprocalOnly);
    if (ranked.empty() && reciprocalOnly) {
        ranked = rankGraphNeighborCandidates(seedNeighbors, seeds, maxDocs, minScore,
                                             /*reciprocalOnly=*/false);
    }
    if (ranked.empty() && minScore > 0.0F) {
        ranked = rankGraphNeighborCandidates(seedNeighbors, seeds, maxDocs, /*minScore=*/0.0F,
                                             /*reciprocalOnly=*/false);
    }
    return ranked;
}

std::size_t edgeFetchLimit(std::size_t maxDocs) {
    return std::max<std::size_t>(maxDocs * 8, 64);
}

/// Dense route → medoid anchors → pure graph neighbors of those medoids.
struct MedoidGraphExpandResult {
    std::vector<std::string> ranked;
    std::size_t routedClusters{0};
};

MedoidGraphExpandResult tryMedoidGraphExpansion(
    const TopologyRoutingSessionRequest& request,
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore) {
    MedoidGraphExpandResult out;
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(metadataRepo, kgStore);
    auto latestTopology = topologyStore.loadLatest();
    if (!latestTopology || !latestTopology.value().has_value()) {
        return out;
    }
    const auto& topology = latestTopology.value().value();

    yams::topology::TopologyRouteRequest routeRequest;
    routeRequest.queryText = request.query;
    routeRequest.seedDocumentHashes = request.seedDocumentHashes;
    routeRequest.limit = request.maxClusters > 0 ? request.maxClusters : 2;
    routeRequest.weakQueryOnly =
        request.routingMode == SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly;
    routeRequest.scoringMode = topologyRouteScoringMode(request.routeScoringMode);
    routeRequest.sparseDenseAlpha = std::clamp(request.sparseDenseAlpha, 0.0F, 1.0F);
    if (request.queryEmbedding.has_value()) {
        routeRequest.queryEmbedding = request.queryEmbedding.value();
    }

    yams::topology::SparseGuidedClusterRouter router;
    auto routes = router.route(routeRequest, topology);
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
    result.applied = result.addedCandidates > 0;
}

/// Returns true when graph expansion finished the session (caller should return).
/// Returns false to fall through to cluster membership expansion.
bool tryRunGraphNeighborExpansion(
    TopologyRoutingSessionResult& result, const TopologyRoutingSessionRequest& request,
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore, bool mayExpand,
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
    auto ranked = rankNeighborsWithFallbacks(
        seedNeighbors, request.seedDocumentHashes, request.maxDocs, request.graphNeighborMinScore,
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
        auto medoidExpand = tryMedoidGraphExpansion(request, metadataRepo, kgStore);
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

std::unordered_map<std::string, const yams::topology::ClusterArtifact*>
indexClustersById(const yams::topology::TopologyArtifactBatch& topology) {
    std::unordered_map<std::string, const yams::topology::ClusterArtifact*> byId;
    byId.reserve(topology.clusters.size());
    for (const auto& cluster : topology.clusters) {
        byId.emplace(cluster.clusterId, &cluster);
    }
    return byId;
}

std::vector<std::string>
selectClusterExpansionHashes(const yams::topology::ClusterArtifact& cluster,
                             const yams::topology::ClusterRoute& route,
                             const TopologyRoutingSessionRequest& request,
                             const TopologyMemberReranker& memberReranker) {
    if (request.medoidOnlyExpansion && route.medoidDocumentHash.has_value()) {
        return {*route.medoidDocumentHash};
    }
    if (request.perClusterLimit > 0 && memberReranker &&
        cluster.memberDocumentHashes.size() > request.perClusterLimit) {
        auto expansionHashes =
            memberReranker(cluster.memberDocumentHashes, request.perClusterLimit);
        if (!expansionHashes.empty()) {
            return expansionHashes;
        }
    }
    return cluster.memberDocumentHashes;
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
                            const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
                            const TopologyMemberReranker& memberReranker, bool mayExpand,
                            TopologyRoutingSessionResult result,
                            std::chrono::steady_clock::time_point totalStart) {
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(metadataRepo, kgStore);
    const auto loadStart = std::chrono::steady_clock::now();
    auto latestTopology = topologyStore.loadLatest();
    result.timings.loadMicros = microsSince(loadStart);

    if (!latestTopology) {
        result.skipReason = std::string{"load_failed:"} + latestTopology.error().message;
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }
    if (!latestTopology.value().has_value()) {
        result.skipReason = "no_snapshot";
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }

    result.loadSucceeded = true;
    result.artifactsFresh = true;
    result.topologyEpoch = latestTopology.value()->topologyEpoch;
    const auto& topology = latestTopology.value().value();

    const auto validateStart = std::chrono::steady_clock::now();
    auto invalidReason = validateTopologyArtifactBatchForRouting(topology);
    result.timings.validateMicros = microsSince(validateStart);
    if (invalidReason.has_value()) {
        result.skipReason = std::string{"invalid_artifact:"} + *invalidReason;
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }
    result.artifactAdmitted = true;

    const auto requestPrepStart = std::chrono::steady_clock::now();
    yams::topology::TopologyRouteRequest routeRequest;
    routeRequest.queryText = request.query;
    routeRequest.seedDocumentHashes = request.seedDocumentHashes;
    routeRequest.limit = request.maxClusters;
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
    auto routes = router.route(routeRequest, topology);
    result.timings.routeMicros = microsSince(sparseRouteStart);
    if (!routes) {
        result.skipReason = std::string{"route_failed:"} + routes.error().message;
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }

    const auto clustersById = indexClustersById(topology);
    std::unordered_set<std::string> candidateHashes = request.existingCandidateHashes;
    const std::unordered_set<std::string> seedSet(request.seedDocumentHashes.begin(),
                                                  request.seedDocumentHashes.end());
    std::unordered_set<std::string> seedsCovered;
    double acceptedRouteScoreSum = 0.0;
    result.seedCount = seedSet.size();
    result.routedClusters = routes.value().size();

    for (const auto& route : routes.value()) {
        if (route.routeScore < request.minRouteScore) {
            ++result.routesRejected;
            continue;
        }
        ++result.acceptedRoutes;
        acceptedRouteScoreSum += route.routeScore;
        result.bestRouteScore =
            std::max(result.bestRouteScore, static_cast<float>(route.routeScore));
        if (route.medoidDocumentHash.has_value()) {
            result.medoidHashes.insert(*route.medoidDocumentHash);
        }

        const auto clusterLookupStart = std::chrono::steady_clock::now();
        const auto clusterIt = clustersById.find(route.clusterId);
        result.timings.clusterLookupMicros += microsSince(clusterLookupStart);
        if (clusterIt == clustersById.end()) {
            continue;
        }
        const auto& cluster = *clusterIt->second;
        coverSeedsInCluster(cluster, seedSet, seedsCovered);

        const auto expansionHashes =
            selectClusterExpansionHashes(cluster, route, request, memberReranker);
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
        if (request.maxDocs > 0 && result.routedDocs >= request.maxDocs) {
            break;
        }
    }

    result.seedsInRoutedClusters = seedsCovered.size();
    if (result.acceptedRoutes > 0) {
        result.meanAcceptedRouteScore = static_cast<float>(
            acceptedRouteScoreSum / static_cast<double>(result.acceptedRoutes));
    }
    result.narrowApplied = false;
    result.applied = result.addedCandidates > 0;
    // Preserve graph fallthrough skip reason when cluster path also fails.
    if (result.applied && result.skipReason == "graph_no_neighbors_fallback_clusters") {
        result.skipReason.clear();
    }
    result.timings.totalMicros = microsSince(totalStart);
    return result;
}

} // namespace

std::vector<std::string> rankGraphNeighborCandidates(
    const std::unordered_map<std::string,
                             std::vector<std::tuple<std::string, float, bool>>>& seedNeighbors,
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

    if (tryRunGraphNeighborExpansion(result, request, metadataRepo, kgStore, mayExpand,
                                     totalStart)) {
        return result;
    }

    return runClusterArtifactExpansion(request, metadataRepo, kgStore, memberReranker, mayExpand,
                                       std::move(result), totalStart);
}

} // namespace yams::search
