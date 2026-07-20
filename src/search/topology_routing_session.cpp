#include <yams/search/topology_routing_session.h>

#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_metadata_store.h>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cmath>
#include <iterator>
#include <limits>
#include <ranges>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::search {

namespace {

void fingerprintByte(std::uint64_t& hash, std::uint8_t value) {
    constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
    hash ^= value;
    hash *= kFnvPrime;
}

template <typename T> void fingerprintIntegral(std::uint64_t& hash, T value) {
    using Unsigned = std::make_unsigned_t<T>;
    auto bits = static_cast<std::uint64_t>(static_cast<Unsigned>(value));
    for (std::size_t byte = 0; byte < sizeof(Unsigned); ++byte) {
        fingerprintByte(hash, static_cast<std::uint8_t>(bits & 0xFFU));
        bits >>= 8U;
    }
}

void fingerprintString(std::uint64_t& hash, std::string_view value) {
    fingerprintIntegral(hash, value.size());
    for (const auto byte : value) {
        fingerprintByte(hash, static_cast<std::uint8_t>(byte));
    }
}

void fingerprintFloat(std::uint64_t& hash, float value) {
    fingerprintIntegral(hash, std::bit_cast<std::uint32_t>(value));
}

void fingerprintDouble(std::uint64_t& hash, double value) {
    fingerprintIntegral(hash, std::bit_cast<std::uint64_t>(value));
}

std::string fingerprintHex(std::uint64_t hash) {
    constexpr std::string_view kHex = "0123456789abcdef";
    std::string result(16, '0');
    for (std::size_t index = 0; index < result.size(); ++index) {
        const auto shift = (result.size() - index - 1) * 4;
        result[index] = kHex[(hash >> shift) & 0x0FU];
    }
    return result;
}

} // namespace

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

void accumulateRouteWork(TopologyRoutingSessionResult& result,
                         const yams::topology::SparseRouteWork& work) {
    result.routeRepresentativeDistanceEvaluations += work.representativeDistanceEvaluations;
    result.routeAnnUsed = result.routeAnnUsed || work.denseAnnUsed;
    result.routeAnnCandidates += work.denseAnnCandidates;
    result.routeAnnDistanceEvaluations += work.denseAnnDistanceEvaluations;
    result.routeExactRepresentativeDistanceEvaluations +=
        work.exactRepresentativeDistanceEvaluations;
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
    result.constructionFingerprint = snapshot->constructionFingerprint;
    for (const auto& cluster : snapshot->artifacts->clusters) {
        const auto availableRepresentativeCount =
            (cluster.centroidEmbedding.empty() ? std::size_t{0} : std::size_t{1}) +
            cluster.routingRepresentatives.size();
        const auto representativeCount =
            request.options.representativeLimit == 0
                ? availableRepresentativeCount
                : std::min(availableRepresentativeCount, request.options.representativeLimit);
        result.routeRepresentativeCountMax =
            std::max(result.routeRepresentativeCountMax, representativeCount);
    }
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
    routeRequest.limit = request.options.maxClusters > 0 ? request.options.maxClusters : 2;
    routeRequest.scoringMode = topologyRouteScoringMode(request.options.routeScoringMode);
    routeRequest.sparseDenseAlpha = std::clamp(request.options.sparseDenseAlpha, 0.0F, 1.0F);
    routeRequest.maxRoutingRepresentatives = request.options.representativeLimit;
    routeRequest.denseAnnCandidateLimit = request.options.denseAnnCandidateLimit;
    if (request.queryEmbedding.has_value()) {
        routeRequest.queryEmbedding = request.queryEmbedding.value();
    }

    yams::topology::SparseGuidedClusterRouter router;
    yams::topology::SparseRouteWork routeWork;
    auto routes = router.route(routeRequest, topology, snapshot->sparseRouteIndex, &routeWork);
    accumulateRouteWork(result, routeWork);
    if (!routes) {
        return out;
    }
    out.routedClusters = routes.value().size();

    SeedNeighborMap medoidNeighbors;
    std::vector<std::string> medoidSeeds;
    medoidSeeds.reserve(routes.value().size());
    const auto limit = edgeFetchLimit(request.options.maxDocs);

    for (const auto& route : routes.value()) {
        if (route.routeScore < request.options.minRouteScore) {
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

    out.ranked = rankGraphNeighborCandidates(medoidNeighbors, medoidSeeds, request.options.maxDocs,
                                             request.options.graphNeighborMinScore,
                                             /*reciprocalOnly=*/false);
    return out;
}

void admitRankedCandidates(TopologyRoutingSessionResult& result,
                           const TopologyRoutingSessionRequest& request,
                           const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                           const std::vector<std::string>& ranked) {
    result.artifactAdmitted = true;
    result.routedDocs = ranked.size();
    std::unordered_set<std::string> candidateHashes;
    if (!request.options.collectRouteMembership) {
        candidateHashes = request.existingCandidateHashes;
    }
    candidateHashes.reserve(candidateHashes.size() + ranked.size());

    for (const auto& hash : ranked) {
        const auto docLookupStart = std::chrono::steady_clock::now();
        auto docLookup = metadataRepo->getDocumentByHash(hash);
        result.timings.docLookupMicros += microsSince(docLookupStart);
        if (!docLookup || !docLookup.value().has_value()) {
            ++result.staleCandidates;
            continue;
        }
        if (request.options.collectRouteMembership) {
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
    result.narrowApplied = request.options.collectRouteMembership
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
    if (request.options.expansionSource !=
        SearchEngineConfig::TopologyExpansionSource::GraphNeighbors) {
        return false;
    }

    const auto loadStart = std::chrono::steady_clock::now();
    auto seedNeighbors = collectSeedNeighborMap(*kgStore, request.seedDocumentHashes,
                                                edgeFetchLimit(request.options.maxDocs));
    result.timings.loadMicros = microsSince(loadStart);
    result.loadSucceeded = true;
    result.artifactsFresh = true;

    const auto routeStart = std::chrono::steady_clock::now();
    auto ranked = rankGraphNeighborCandidates(
        seedNeighbors, request.seedDocumentHashes, request.options.maxDocs,
        request.options.graphNeighborMinScore, request.options.graphNeighborReciprocalOnly);
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

std::vector<std::string> selectClusterExpansionHashes(const yams::topology::ClusterRoute& route) {
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

struct CandidateStructureAccumulator {
    std::uint64_t scaleMask{0};
    std::size_t clusterSamples{0};
    std::size_t selectedOverlapReferences{0};
    double persistenceSum{0.0};
    double cohesionSum{0.0};
    double densitySum{0.0};
    double bridgeMax{0.0};
    bool membershipObserved{false};
};

struct RoutedMemberAccumulator {
    CandidateStructureAccumulator structure;
    float seedWeight{0.0F};
    double bestRouteScore{-std::numeric_limits<double>::infinity()};
    std::size_t firstRoute{std::numeric_limits<std::size_t>::max()};
    bool seed{false};
    bool medoid{false};
};

std::uint64_t topologyScaleBit(std::size_t level) {
    constexpr std::size_t kMaxRepresentedLevel = 63;
    return std::uint64_t{1} << std::min(level, kMaxRepresentedLevel);
}

void accumulateCandidateStructure(CandidateStructureAccumulator& accumulator,
                                  const std::string& documentHash,
                                  const yams::topology::ClusterArtifact& cluster,
                                  const TopologyRoutingSnapshot& snapshot,
                                  const std::unordered_set<std::string>& selectedClusterIds) {
    accumulator.scaleMask |= topologyScaleBit(cluster.level);
    ++accumulator.clusterSamples;
    accumulator.persistenceSum += std::clamp(cluster.persistenceScore, 0.0, 1.0);
    accumulator.cohesionSum += std::clamp(cluster.cohesionScore, 0.0, 1.0);
    accumulator.densitySum += std::clamp(cluster.densityScore, 0.0, 1.0);
    accumulator.bridgeMax =
        std::max(accumulator.bridgeMax, std::clamp(cluster.bridgeMass, 0.0, 1.0));

    if (accumulator.membershipObserved) {
        return;
    }
    const auto membershipIt = snapshot.membershipsByDocumentHash.find(documentHash);
    if (membershipIt == snapshot.membershipsByDocumentHash.end()) {
        return;
    }
    accumulator.membershipObserved = true;
    const auto& membership = snapshot.artifacts->memberships[membershipIt->second];
    accumulator.bridgeMax =
        std::max(accumulator.bridgeMax, std::clamp(membership.bridgeScore, 0.0, 1.0));
    for (const auto& overlapClusterId : membership.overlapClusterIds) {
        if (selectedClusterIds.contains(overlapClusterId)) {
            ++accumulator.selectedOverlapReferences;
        }
    }
}

TopologyCandidateStructureEvidence
finalizeCandidateStructure(const CandidateStructureAccumulator& accumulator,
                           std::uint64_t selectedScaleMask, std::size_t acceptedRoutes) {
    TopologyCandidateStructureEvidence evidence;
    if (accumulator.clusterSamples == 0) {
        return evidence;
    }

    const auto selectedScales = std::popcount(selectedScaleMask);
    const auto candidateScales = std::popcount(accumulator.scaleMask);
    evidence.scaleAgreement =
        selectedScales > 1
            ? std::clamp(static_cast<float>(candidateScales) / static_cast<float>(selectedScales),
                         0.0F, 1.0F)
            : 0.0F;
    const std::size_t repeatedMemberships = accumulator.clusterSamples - 1;
    evidence.overlapSupport =
        std::clamp(static_cast<float>(repeatedMemberships + accumulator.selectedOverlapReferences) /
                       static_cast<float>(std::max<std::size_t>(1, acceptedRoutes)),
                   0.0F, 1.0F);
    const double sampleCount = static_cast<double>(accumulator.clusterSamples);
    evidence.persistenceSupport =
        static_cast<float>(std::clamp(accumulator.persistenceSum / sampleCount, 0.0, 1.0));
    evidence.cohesionSupport =
        static_cast<float>(std::clamp(accumulator.cohesionSum / sampleCount, 0.0, 1.0));
    evidence.bridgeSupport = static_cast<float>(std::clamp(accumulator.bridgeMax, 0.0, 1.0));
    evidence.densitySupport =
        static_cast<float>(std::clamp(accumulator.densitySum / sampleCount, 0.0, 1.0));
    return evidence;
}

void materializeAllowedRouteMembers(
    const std::unordered_map<std::string, RoutedMemberAccumulator>& routedMembers,
    std::size_t maxDocs, std::uint64_t selectedScaleMask, std::size_t acceptedRoutes,
    TopologyRoutingSessionResult& result) {
    using Entry = std::pair<const std::string, RoutedMemberAccumulator>;
    std::vector<const Entry*> ranked;
    ranked.reserve(routedMembers.size());
    for (const auto& entry : routedMembers) {
        ranked.push_back(&entry);
    }

    // Lexical/vector seeds are recall anchors. Within the anchored and unanchored groups, the
    // query-ranked route score leads, medoids break same-route ties, and the hash is the stable
    // final tie-breaker. The allowed-set ANN performs the final member-level query ranking.
    std::ranges::sort(ranked, [](const Entry* lhs, const Entry* rhs) {
        const auto& left = lhs->second;
        const auto& right = rhs->second;
        if (left.seed != right.seed) {
            return left.seed > right.seed;
        }
        if (left.seedWeight != right.seedWeight) {
            return left.seedWeight > right.seedWeight;
        }
        if (left.bestRouteScore != right.bestRouteScore) {
            return left.bestRouteScore > right.bestRouteScore;
        }
        if (left.medoid != right.medoid) {
            return left.medoid > right.medoid;
        }
        if (left.firstRoute != right.firstRoute) {
            return left.firstRoute < right.firstRoute;
        }
        return lhs->first < rhs->first;
    });

    // maxDocs=0 is the explicit unbounded mode used by callers that want full route membership.
    const auto take = maxDocs == 0 ? ranked.size() : std::min(maxDocs, ranked.size());
    result.routeAllowedDocumentHashes.reserve(take);
    result.candidateStructureEvidence.reserve(take);
    for (const auto* entry : ranked | std::views::take(take)) {
        result.routeAllowedDocumentHashes.insert(entry->first);
        if (entry->second.medoid) {
            result.routedCandidateHashes.insert(entry->first);
        }
        result.candidateStructureEvidence.emplace(
            entry->first,
            finalizeCandidateStructure(entry->second.structure, selectedScaleMask, acceptedRoutes));
    }
    for (auto& routeGroup : result.routeAllowedDocumentHashGroups) {
        std::erase_if(routeGroup, [&](const auto& hash) {
            return !result.routeAllowedDocumentHashes.contains(hash);
        });
    }
    result.routedDocs = take;
}

TopologyRoutingSessionResult
runClusterArtifactExpansion(const TopologyRoutingSessionRequest& request,
                            const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                            const std::shared_ptr<TopologyRoutingSnapshotCache>& snapshotCache,
                            bool mayExpand, TopologyRoutingSessionResult result,
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
    const bool needsBoundaryRoute = request.options.adaptiveProbeScoreGap > 0.0F ||
                                    request.options.narrowMinBoundaryMargin > 0.0F;
    routeRequest.limit = request.options.maxClusters > 0 && needsBoundaryRoute
                             ? request.options.maxClusters + static_cast<std::size_t>(1)
                             : request.options.maxClusters;
    routeRequest.scoringMode = topologyRouteScoringMode(request.options.routeScoringMode);
    routeRequest.sparseDenseAlpha = std::clamp(request.options.sparseDenseAlpha, 0.0F, 1.0F);
    routeRequest.maxRoutingRepresentatives = request.options.representativeLimit;
    routeRequest.denseAnnCandidateLimit = request.options.denseAnnCandidateLimit;
    if (request.queryEmbedding.has_value()) {
        routeRequest.queryEmbedding = request.queryEmbedding.value();
    }
    result.timings.requestPrepMicros = microsSince(requestPrepStart);

    yams::topology::SparseGuidedClusterRouter router;
    const auto sparseRouteStart = std::chrono::steady_clock::now();
    yams::topology::SparseRouteWork routeWork;
    auto routes = router.route(routeRequest, topology, snapshot->sparseRouteIndex, &routeWork);
    accumulateRouteWork(result, routeWork);
    result.timings.routeMicros = microsSince(sparseRouteStart);
    if (!routes) {
        result.skipReason = std::string{"route_failed:"} + routes.error().message;
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }

    std::vector<yams::topology::ClusterRoute> eligibleRoutes;
    eligibleRoutes.reserve(routes.value().size());
    for (const auto& route : routes.value()) {
        if (route.routeScore < request.options.minRouteScore) {
            ++result.routesRejected;
            continue;
        }
        eligibleRoutes.push_back(route);
    }
    auto selection = selectTopologyRoutesForNarrowing(
        eligibleRoutes, request.options.minClusters, request.options.maxClusters,
        request.options.adaptiveProbeScoreGap, request.options.narrowMinBoundaryMargin);
    result.availableRoutes = selection.availableRoutes;
    result.routeBoundaryScoreMargin = selection.boundaryScoreMargin;
    result.confidenceAbstained = selection.abstained;
    result.routedClusters = selection.routes.size();
    std::unordered_set<std::string> selectedClusterIds;
    selectedClusterIds.reserve(selection.routes.size());
    for (const auto& route : selection.routes) {
        selectedClusterIds.insert(route.clusterId);
    }
    result.routeCoordinateEvidence.reserve(routes.value().size());
    for (const auto& route : routes.value()) {
        result.routeCoordinateEvidence.push_back(TopologyRouteCoordinateEvidence{
            .clusterId = route.clusterId,
            .semanticCost = route.semanticCost.has_value()
                                ? std::optional<float>(static_cast<float>(*route.semanticCost))
                                : std::nullopt,
            .sparseCost = route.sparseCost.has_value()
                              ? std::optional<float>(static_cast<float>(*route.sparseCost))
                              : std::nullopt,
            .distortionPenalty =
                route.distortionPenalty.has_value()
                    ? std::optional<float>(static_cast<float>(*route.distortionPenalty))
                    : std::nullopt,
            .localIntrinsicDimension =
                route.localIntrinsicDimension.has_value()
                    ? std::optional<float>(static_cast<float>(*route.localIntrinsicDimension))
                    : std::nullopt,
            .uncertaintyPenalty =
                route.uncertaintyPenalty.has_value()
                    ? std::optional<float>(static_cast<float>(*route.uncertaintyPenalty))
                    : std::nullopt,
            .persistencePenalty = static_cast<float>(route.persistencePenalty),
            .cohesionPenalty = static_cast<float>(route.cohesionPenalty),
            .sizePenalty = static_cast<float>(route.sizePenalty),
            .routeScore = static_cast<float>(route.routeScore),
            .scoreEligible = route.routeScore >= request.options.minRouteScore,
            .inSelectedPrefix = selectedClusterIds.contains(route.clusterId),
        });
    }
    if (selection.abstained) {
        result.artifactAdmitted = true;
        result.skipReason = "route_low_confidence";
        result.timings.totalMicros = microsSince(totalStart);
        return result;
    }

    std::unordered_set<std::string> candidateHashes;
    if (!request.options.collectRouteMembership) {
        candidateHashes = request.existingCandidateHashes;
    }
    const std::unordered_set<std::string> seedSet(request.seedDocumentHashes.begin(),
                                                  request.seedDocumentHashes.end());
    std::unordered_map<std::string, float> seedWeights;
    seedWeights.reserve(request.weightedSeedDocuments.size());
    for (const auto& seed : request.weightedSeedDocuments) {
        if (seed.documentHash.empty() || !std::isfinite(seed.weight) || seed.weight <= 0.0F) {
            continue;
        }
        auto [it, inserted] = seedWeights.emplace(seed.documentHash, seed.weight);
        if (!inserted) {
            it->second = std::max(it->second, seed.weight);
        }
    }
    std::unordered_set<std::string> seedsCovered;
    std::uint64_t selectedScaleMask = 0;
    for (const auto& route : selection.routes) {
        const auto clusterIt = snapshot->clustersById.find(route.clusterId);
        if (clusterIt != snapshot->clustersById.end()) {
            selectedScaleMask |= topologyScaleBit(topology.clusters[clusterIt->second].level);
        }
    }
    std::unordered_map<std::string, RoutedMemberAccumulator> routedMembers;
    if (request.options.collectRouteMembership && mayExpand) {
        routedMembers.reserve(request.options.maxDocs > 0 ? request.options.maxDocs : 64);
    }
    double acceptedRouteScoreSum = 0.0;
    result.seedCount = seedSet.size();
    for (std::size_t routeOrdinal = 0; routeOrdinal < selection.routes.size(); ++routeOrdinal) {
        const auto& route = selection.routes[routeOrdinal];
        ++result.acceptedRoutes;
        acceptedRouteScoreSum += route.routeScore;
        result.bestRouteScore =
            std::max(result.bestRouteScore, static_cast<float>(route.routeScore));
        if (route.medoidDocumentHash.has_value()) {
            result.medoidHashes.insert(*route.medoidDocumentHash);
        }
        std::unordered_set<std::string>* routeMemberGroup = nullptr;
        if (request.options.collectRouteMembership && mayExpand) {
            routeMemberGroup = &result.routeAllowedDocumentHashGroups.emplace_back();
        }

        const auto clusterLookupStart = std::chrono::steady_clock::now();
        const auto clusterIt = snapshot->clustersById.find(route.clusterId);
        result.timings.clusterLookupMicros += microsSince(clusterLookupStart);
        if (clusterIt == snapshot->clustersById.end()) {
            continue;
        }
        const auto& cluster = topology.clusters[clusterIt->second];
        if (request.options.collectRouteMembership && mayExpand) {
            for (const auto& hash : cluster.memberDocumentHashes) {
                if (hash.empty()) {
                    continue;
                }
                routeMemberGroup->insert(hash);
                auto& candidate = routedMembers[hash];
                candidate.seed = candidate.seed || seedSet.contains(hash);
                if (candidate.seed) {
                    seedsCovered.insert(hash);
                }
                if (const auto weightIt = seedWeights.find(hash); weightIt != seedWeights.end()) {
                    candidate.seedWeight = std::max(candidate.seedWeight, weightIt->second);
                }
                candidate.bestRouteScore = std::max(candidate.bestRouteScore, route.routeScore);
                candidate.firstRoute = std::min(candidate.firstRoute, routeOrdinal);
                candidate.medoid = candidate.medoid || (route.medoidDocumentHash.has_value() &&
                                                        *route.medoidDocumentHash == hash);
                accumulateCandidateStructure(candidate.structure, hash, cluster, *snapshot,
                                             selectedClusterIds);
            }
        } else {
            coverSeedsInCluster(cluster, seedSet, seedsCovered);
        }

        if (request.options.collectRouteMembership) {
            continue;
        }
        const auto expansionHashes = selectClusterExpansionHashes(route);
        for (const auto& hash : expansionHashes) {
            if (request.options.maxDocs > 0 && result.routedDocs >= request.options.maxDocs) {
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
        if (request.options.maxDocs > 0 && result.routedDocs >= request.options.maxDocs) {
            break;
        }
    }

    result.seedsInRoutedClusters = seedsCovered.size();
    if (result.acceptedRoutes > 0) {
        result.meanAcceptedRouteScore =
            static_cast<float>(acceptedRouteScoreSum / static_cast<double>(result.acceptedRoutes));
    }
    if (request.options.collectRouteMembership && mayExpand) {
        materializeAllowedRouteMembers(routedMembers, request.options.maxDocs, selectedScaleMask,
                                       result.acceptedRoutes, result);
    }
    result.narrowApplied = mayExpand && (request.options.collectRouteMembership
                                             ? !result.routeAllowedDocumentHashes.empty()
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

TopologyRoutingOptions
makeTopologyRoutingOptions(const SearchEngineConfig& config,
                           SearchEngineConfig::TopologyRoutingMode routingMode, bool weakTier1Query,
                           bool collectRouteMembership) noexcept {
    return TopologyRoutingOptions{
        .routingMode = routingMode,
        .routeScoringMode = config.topologyRouteScoringMode,
        .expansionSource = config.topologyExpansionSource,
        .weakTier1Query = weakTier1Query,
        .minClusters = config.topologyMinClusters,
        .maxClusters = config.topologyMaxClusters,
        .representativeLimit = config.topologyRoutingRepresentativeLimit,
        .denseAnnCandidateLimit = config.topologyRoutingAnnCandidateLimit,
        .adaptiveProbeScoreGap = config.topologyAdaptiveProbeScoreGap,
        .narrowMinBoundaryMargin = config.topologyNarrowMinBoundaryMargin,
        .maxDocs = config.topologyMaxDocs,
        .sparseDenseAlpha = config.topologySparseDenseAlpha,
        .minRouteScore = config.topologyMinRouteScore,
        .collectRouteMembership = collectRouteMembership,
        .graphNeighborMinScore = config.topologyGraphNeighborMinScore,
        .graphNeighborReciprocalOnly = config.topologyGraphNeighborReciprocalOnly,
    };
}

SearchEngineConfig::TopologyRoutingMode
resolveTopologyRoutingMode(const SearchEngineConfig& config,
                           std::optional<bool> legacyWeakQueryRouting) noexcept {
    // The old boolean only expressed the former opt-in weak-query lane. Preserve an explicit
    // `true`, but do not let the generated legacy `false` disable the current product default.
    // Explicit disabling now uses search.topology.mode = "disabled".
    if (legacyWeakQueryRouting.value_or(false)) {
        return SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly;
    }
    return config.topologyRoutingMode;
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
            return true;
    }
    return false;
}

bool topologyRoutingMayExpand(SearchEngineConfig::TopologyRoutingMode mode,
                              bool weakTier1Query) noexcept {
    using Mode = SearchEngineConfig::TopologyRoutingMode;
    switch (mode) {
        case Mode::Disabled:
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
        for (const auto& representative : cluster.routingRepresentatives) {
            if (representative.documentHash.empty()) {
                return "empty_routing_representative_hash";
            }
            if (!members.contains(representative.documentHash)) {
                return "routing_representative_not_cluster_member";
            }
            if (representative.embedding.empty() ||
                representative.embedding.size() != cluster.centroidEmbedding.size()) {
                return "routing_representative_dimension_mismatch";
            }
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
        std::unordered_set<std::string> overlapIds;
        overlapIds.reserve(membership.overlapClusterIds.size());
        for (const auto& overlapClusterId : membership.overlapClusterIds) {
            if (overlapClusterId.empty() || overlapClusterId == membership.clusterId) {
                return "invalid_overlap_cluster";
            }
            if (!overlapIds.insert(overlapClusterId).second) {
                return "duplicate_overlap_cluster";
            }
            const auto overlapIt = clusterMembersById.find(overlapClusterId);
            if (overlapIt == clusterMembersById.end()) {
                return "overlap_without_cluster";
            }
            if (!overlapIt->second.contains(membership.documentHash)) {
                return "overlap_not_in_cluster_members";
            }
        }
    }

    return std::nullopt;
}

std::string
topologyRoutingConstructionFingerprint(const yams::topology::TopologyArtifactBatch& batch) {
    constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
    std::uint64_t hash = kFnvOffset;
    fingerprintString(hash, batch.algorithm);
    fingerprintIntegral(hash, static_cast<std::uint8_t>(batch.inputKind));

    std::vector<const yams::topology::ClusterArtifact*> clusters;
    clusters.reserve(batch.clusters.size());
    for (const auto& cluster : batch.clusters) {
        clusters.push_back(&cluster);
    }
    std::ranges::sort(clusters, {}, [](const auto* cluster) { return cluster->clusterId; });
    fingerprintIntegral(hash, clusters.size());
    for (const auto* cluster : clusters) {
        fingerprintString(hash, cluster->clusterId);
        fingerprintString(hash, cluster->parentClusterId.value_or(""));
        fingerprintIntegral(hash, cluster->level);
        fingerprintIntegral(hash, cluster->memberCount);
        fingerprintDouble(hash, cluster->persistenceScore);
        fingerprintDouble(hash, cluster->cohesionScore);
        fingerprintDouble(hash, cluster->densityScore);
        fingerprintDouble(hash, cluster->bridgeMass);
        fingerprintIntegral(hash,
                            static_cast<std::uint8_t>(cluster->coordinateDistortion.has_value()));
        if (cluster->coordinateDistortion.has_value()) {
            fingerprintDouble(hash, *cluster->coordinateDistortion);
        }
        fingerprintIntegral(hash, cluster->distortionObservationCount);
        fingerprintIntegral(
            hash, static_cast<std::uint8_t>(cluster->localIntrinsicDimension.has_value()));
        if (cluster->localIntrinsicDimension.has_value()) {
            fingerprintDouble(hash, *cluster->localIntrinsicDimension);
        }
        fingerprintString(hash, cluster->medoid.has_value() ? cluster->medoid->documentHash : "");

        auto members = cluster->memberDocumentHashes;
        std::ranges::sort(members);
        fingerprintIntegral(hash, members.size());
        for (const auto& member : members) {
            fingerprintString(hash, member);
        }
        auto overlaps = cluster->overlapClusterIds;
        std::ranges::sort(overlaps);
        fingerprintIntegral(hash, overlaps.size());
        for (const auto& overlap : overlaps) {
            fingerprintString(hash, overlap);
        }
        fingerprintIntegral(hash, cluster->centroidEmbedding.size());
        for (const auto value : cluster->centroidEmbedding) {
            fingerprintFloat(hash, value);
        }
    }

    std::vector<const yams::topology::DocumentClusterMembership*> memberships;
    memberships.reserve(batch.memberships.size());
    for (const auto& membership : batch.memberships) {
        memberships.push_back(&membership);
    }
    std::ranges::sort(memberships, [](const auto* lhs, const auto* rhs) {
        if (lhs->documentHash != rhs->documentHash) {
            return lhs->documentHash < rhs->documentHash;
        }
        return lhs->clusterId < rhs->clusterId;
    });
    fingerprintIntegral(hash, memberships.size());
    for (const auto* membership : memberships) {
        fingerprintString(hash, membership->documentHash);
        fingerprintString(hash, membership->clusterId);
        fingerprintString(hash, membership->parentClusterId.value_or(""));
        fingerprintIntegral(hash, membership->clusterLevel);
        fingerprintDouble(hash, membership->persistenceScore);
        fingerprintDouble(hash, membership->cohesionScore);
        fingerprintDouble(hash, membership->bridgeScore);
        fingerprintIntegral(hash, static_cast<std::uint8_t>(membership->role));
        auto overlaps = membership->overlapClusterIds;
        std::ranges::sort(overlaps);
        fingerprintIntegral(hash, overlaps.size());
        for (const auto& overlap : overlaps) {
            fingerprintString(hash, overlap);
        }
    }
    return fingerprintHex(hash);
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
    snapshot->constructionFingerprint =
        topologyRoutingConstructionFingerprint(*snapshot->artifacts);
    snapshot->sparseRouteIndex =
        yams::topology::SparseGuidedClusterRouter::buildRouteIndex(*snapshot->artifacts);
    snapshot->clustersById.reserve(snapshot->artifacts->clusters.size());
    for (std::size_t index = 0; index < snapshot->artifacts->clusters.size(); ++index) {
        snapshot->clustersById.emplace(snapshot->artifacts->clusters[index].clusterId, index);
    }
    snapshot->membershipsByDocumentHash.reserve(snapshot->artifacts->memberships.size());
    for (std::size_t index = 0; index < snapshot->artifacts->memberships.size(); ++index) {
        snapshot->membershipsByDocumentHash.emplace(
            snapshot->artifacts->memberships[index].documentHash, index);
    }
    cached_ = std::move(snapshot);
    return TopologyRoutingSnapshotLookup{.snapshot = cached_, .cacheHit = false};
}

TopologyRoutingSessionResult
runTopologyRoutingSession(const TopologyRoutingSessionRequest& request,
                          const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                          const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore) {
    TopologyRoutingSessionResult result;
    result.enabled =
        request.options.routingMode != SearchEngineConfig::TopologyRoutingMode::Disabled;
    result.loadAttempted =
        topologyRoutingMayLoad(request.options.routingMode, request.options.weakTier1Query);
    if (!result.loadAttempted) {
        return result;
    }
    if (!metadataRepo || !kgStore) {
        result.skipReason = "missing_topology_dependencies";
        return result;
    }

    const auto totalStart = std::chrono::steady_clock::now();
    const bool mayExpand =
        topologyRoutingMayExpand(request.options.routingMode, request.options.weakTier1Query);
    result.seedCount = request.seedDocumentHashes.size();
    const auto snapshotCache =
        request.snapshotCache ? request.snapshotCache : makeSnapshotCache(metadataRepo, kgStore);

    if (tryRunGraphNeighborExpansion(result, request, metadataRepo, kgStore, mayExpand,
                                     snapshotCache, totalStart)) {
        return result;
    }

    return runClusterArtifactExpansion(request, metadataRepo, snapshotCache, mayExpand,
                                       std::move(result), totalStart);
}

} // namespace yams::search
