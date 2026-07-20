#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_representatives.h>
#include <yams/vector/static_cosine_ann_index.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <queue>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace yams::topology {

namespace {

using PairKey = std::pair<std::size_t, std::size_t>;
using Adjacency = std::vector<std::vector<std::pair<std::size_t, float>>>;

struct PairKeyHash {
    std::size_t operator()(const PairKey& k) const noexcept {
        // Mix via splitmix-style combine; both halves fit in 64 bits on 64-bit.
        const std::uint64_t a = static_cast<std::uint64_t>(k.first);
        const std::uint64_t b = static_cast<std::uint64_t>(k.second);
        std::uint64_t x = a * 0x9E3779B97F4A7C15ULL + b;
        x ^= x >> 33;
        x *= 0xFF51AFD7ED558CCDULL;
        x ^= x >> 33;
        return static_cast<std::size_t>(x);
    }
};

std::string makeSnapshotId(std::uint64_t unixSeconds) {
    return "topology-" + std::to_string(unixSeconds);
}

std::string makeClusterId(const std::string& anchorHash) {
    return "topology.cluster." + anchorHash;
}

std::vector<float> meanEmbedding(std::span<const TopologyDocumentInput> documents,
                                 const std::vector<std::size_t>& members) {
    std::vector<float> centroid;
    std::size_t count = 0;
    for (std::size_t idx : members) {
        if (idx >= documents.size() || documents[idx].embedding.empty()) {
            continue;
        }
        const auto& emb = documents[idx].embedding;
        if (centroid.empty()) {
            centroid.assign(emb.size(), 0.0F);
        } else if (centroid.size() != emb.size()) {
            continue;
        }
        for (std::size_t i = 0; i < emb.size(); ++i) {
            centroid[i] += emb[i];
        }
        ++count;
    }
    if (count == 0) {
        return {};
    }
    for (auto& v : centroid) {
        v /= static_cast<float>(count);
    }
    return centroid;
}

std::optional<double> normalizedCosine(std::span<const float> lhs, std::span<const float> rhs) {
    if (lhs.empty() || lhs.size() != rhs.size()) {
        return std::nullopt;
    }
    double dot = 0.0;
    double lhsSquaredNorm = 0.0;
    double rhsSquaredNorm = 0.0;
    for (std::size_t index = 0; index < lhs.size(); ++index) {
        dot += static_cast<double>(lhs[index]) * static_cast<double>(rhs[index]);
        lhsSquaredNorm += static_cast<double>(lhs[index]) * static_cast<double>(lhs[index]);
        rhsSquaredNorm += static_cast<double>(rhs[index]) * static_cast<double>(rhs[index]);
    }
    if (lhsSquaredNorm <= 0.0 || rhsSquaredNorm <= 0.0) {
        return std::nullopt;
    }
    const auto cosine = dot / std::sqrt(lhsSquaredNorm * rhsSquaredNorm);
    if (!std::isfinite(cosine)) {
        return std::nullopt;
    }
    return std::clamp(cosine, 0.0, 1.0);
}

std::optional<double>
estimateRadialIntrinsicDimension(std::span<const TopologyDocumentInput> documents,
                                 const std::vector<std::size_t>& members,
                                 std::span<const float> centroid) {
    constexpr double kMinimumRadius = 1e-9;
    std::vector<double> radii;
    radii.reserve(members.size());
    for (const auto member : members) {
        if (member >= documents.size()) {
            continue;
        }
        const auto similarity = normalizedCosine(documents[member].embedding, centroid);
        if (!similarity.has_value()) {
            continue;
        }
        const auto radius = 1.0 - *similarity;
        if (radius > kMinimumRadius) {
            radii.push_back(radius);
        }
    }
    if (radii.size() < 3) {
        return std::nullopt;
    }
    std::ranges::sort(radii);
    const auto outerRadius = radii.back();
    double logRatioSum = 0.0;
    for (const auto radius : std::span{radii}.first(radii.size() - 1)) {
        logRatioSum += std::log(radius / outerRadius);
    }
    if (logRatioSum >= -kMinimumRadius) {
        return std::nullopt;
    }
    const auto estimate = -static_cast<double>(radii.size() - 1) / logRatioSum;
    return std::isfinite(estimate) && estimate > 0.0 ? std::optional<double>{estimate}
                                                     : std::nullopt;
}

void sortComponentByHash(std::vector<std::size_t>& component,
                         std::span<const TopologyDocumentInput> documents) {
    std::ranges::sort(component, [&](std::size_t lhs, std::size_t rhs) {
        return documents[lhs].documentHash < documents[rhs].documentHash;
    });
}

/// Lean ConstructionGates anti-giant: peel greedy high-degree pieces of size ≤ cap.
/// maxComponentDocs == 0 keeps legacy unlimited CC behavior.
std::vector<std::vector<std::size_t>>
splitOversizedComponent(std::vector<std::size_t> component, std::size_t maxComponentDocs,
                        const Adjacency& adjacency,
                        std::span<const TopologyDocumentInput> documents) {
    if (maxComponentDocs == 0 || component.size() <= maxComponentDocs) {
        return {std::move(component)};
    }

    std::unordered_set<std::size_t> remaining(component.begin(), component.end());
    std::vector<std::vector<std::size_t>> pieces;
    pieces.reserve((component.size() + maxComponentDocs - 1) / maxComponentDocs);

    auto degreeInRemaining = [&](std::size_t idx) {
        double deg = 0.0;
        for (const auto& [nbr, weight] : adjacency[idx]) {
            if (remaining.contains(nbr)) {
                deg += weight;
            }
        }
        return deg;
    };

    while (!remaining.empty()) {
        const auto seedIt = std::ranges::max_element(remaining, [&](std::size_t a, std::size_t b) {
            const double da = degreeInRemaining(a);
            const double db = degreeInRemaining(b);
            if (std::abs(da - db) > 1e-9) {
                return da < db;
            }
            return documents[a].documentHash > documents[b].documentHash;
        });
        const std::size_t seed = *seedIt;

        std::vector<std::size_t> piece;
        piece.reserve(std::min(maxComponentDocs, remaining.size()));
        std::unordered_set<std::size_t> inPiece;
        using EdgeCand = std::pair<float, std::size_t>;
        auto edgeCmp = [&](const EdgeCand& a, const EdgeCand& b) {
            if (a.first != b.first) {
                return a.first < b.first;
            }
            return documents[a.second].documentHash > documents[b.second].documentHash;
        };
        std::priority_queue<EdgeCand, std::vector<EdgeCand>, decltype(edgeCmp)> frontier(edgeCmp);

        auto addNode = [&](std::size_t idx) {
            if (!remaining.contains(idx) || inPiece.contains(idx) ||
                piece.size() >= maxComponentDocs) {
                return;
            }
            remaining.erase(idx);
            inPiece.insert(idx);
            piece.push_back(idx);
            for (const auto& [nbr, weight] : adjacency[idx]) {
                if (remaining.contains(nbr) && !inPiece.contains(nbr)) {
                    frontier.push({weight, nbr});
                }
            }
        };

        addNode(seed);
        while (!frontier.empty() && piece.size() < maxComponentDocs) {
            const auto [w, nbr] = frontier.top();
            frontier.pop();
            (void)w;
            if (remaining.contains(nbr) && !inPiece.contains(nbr)) {
                addNode(nbr);
            }
        }
        if (piece.empty()) {
            piece.push_back(seed);
            remaining.erase(seed);
        }
        sortComponentByHash(piece, documents);
        pieces.push_back(std::move(piece));
    }
    return pieces;
}

void emitComponent(TopologyArtifactBatch& batch, const std::vector<std::size_t>& component,
                   const Adjacency& adjacency, std::span<const TopologyDocumentInput> documents,
                   std::size_t routingRepresentativeCount) {
    if (component.empty()) {
        return;
    }

    const std::string clusterId = makeClusterId(documents[component.front()].documentHash);
    const std::unordered_set<std::size_t> componentSet(component.begin(), component.end());

    double cohesion = 0.0;
    double persistence = 0.0;
    double distortionSum = 0.0;
    std::size_t internalEdgeCount = 0;
    std::size_t distortionObservationCount = 0;
    std::unordered_map<std::size_t, double> weightedDegree;
    weightedDegree.reserve(component.size());
    std::size_t bridgeCount = 0;

    for (std::size_t idx : component) {
        std::size_t degree = 0;
        for (const auto& [neighborIdx, weight] : adjacency[idx]) {
            if (!componentSet.contains(neighborIdx)) {
                continue;
            }
            weightedDegree[idx] += weight;
            ++degree;
            if (idx < neighborIdx) {
                cohesion += weight;
                persistence =
                    internalEdgeCount == 0 ? weight : std::min<double>(persistence, weight);
                ++internalEdgeCount;
                const auto embeddingSimilarity =
                    normalizedCosine(documents[idx].embedding, documents[neighborIdx].embedding);
                if (embeddingSimilarity.has_value()) {
                    distortionSum += std::abs(std::clamp(static_cast<double>(weight), 0.0, 1.0) -
                                              *embeddingSimilarity);
                    ++distortionObservationCount;
                }
            }
        }
        if (component.size() > 2 && degree >= 2) {
            ++bridgeCount;
        }
    }

    if (internalEdgeCount > 0) {
        cohesion /= static_cast<double>(internalEdgeCount);
    } else {
        persistence = 0.0;
    }

    const auto medoidIt = std::ranges::max_element(component, [&](std::size_t a, std::size_t b) {
        const double da = weightedDegree[a];
        const double db = weightedDegree[b];
        if (std::abs(da - db) > 1e-9) {
            return da < db;
        }
        return documents[a].documentHash > documents[b].documentHash;
    });
    const std::size_t medoidIdx = *medoidIt;
    const double medoidScore = weightedDegree[medoidIdx];

    ClusterArtifact cluster;
    cluster.clusterId = clusterId;
    cluster.level = 0;
    cluster.memberCount = component.size();
    cluster.persistenceScore = persistence;
    cluster.cohesionScore = cohesion;
    const double possibleEdges = component.size() > 1
                                     ? static_cast<double>(component.size()) *
                                           static_cast<double>(component.size() - 1) / 2.0
                                     : 0.0;
    cluster.densityScore =
        possibleEdges > 0.0 ? static_cast<double>(internalEdgeCount) / possibleEdges : 0.0;
    cluster.bridgeMass = static_cast<double>(bridgeCount) / static_cast<double>(component.size());
    cluster.distortionObservationCount = distortionObservationCount;
    if (distortionObservationCount > 0) {
        cluster.coordinateDistortion =
            distortionSum / static_cast<double>(distortionObservationCount);
    }
    cluster.medoid = ClusterRepresentative{.clusterId = clusterId,
                                           .documentHash = documents[medoidIdx].documentHash,
                                           .filePath = documents[medoidIdx].filePath,
                                           .representativeScore = std::max(0.0, medoidScore)};
    cluster.centroidEmbedding = meanEmbedding(documents, component);
    cluster.localIntrinsicDimension =
        estimateRadialIntrinsicDimension(documents, component, cluster.centroidEmbedding);
    cluster.routingRepresentatives = selectDiverseRoutingRepresentatives(
        documents, component, cluster.centroidEmbedding, routingRepresentativeCount);
    cluster.memberDocumentHashes.reserve(component.size());
    std::ranges::transform(component, std::back_inserter(cluster.memberDocumentHashes),
                           [&](std::size_t idx) { return documents[idx].documentHash; });
    batch.clusters.push_back(std::move(cluster));

    const std::size_t maxDegree = component.size() > 1 ? component.size() - 1 : 0;
    for (std::size_t idx : component) {
        const double bridgeScore =
            maxDegree == 0 ? 0.0 : weightedDegree[idx] / static_cast<double>(maxDegree);
        DocumentTopologyRole role = DocumentTopologyRole::Core;
        if (component.size() == 1) {
            role = DocumentTopologyRole::Outlier;
        } else if (idx == medoidIdx) {
            role = DocumentTopologyRole::Medoid;
        } else if (component.size() > 2 && weightedDegree[idx] >= 2.0) {
            role = DocumentTopologyRole::Bridge;
        }
        batch.memberships.push_back(DocumentClusterMembership{
            .documentHash = documents[idx].documentHash,
            .clusterId = clusterId,
            .parentClusterId = std::nullopt,
            .clusterLevel = 0,
            .persistenceScore = persistence,
            .cohesionScore = cohesion,
            .bridgeScore = bridgeScore,
            .role = role,
            .overlapClusterIds = {},
        });
    }
}

} // namespace

Result<TopologyArtifactBatch>
ConnectedComponentTopologyEngine::buildArtifacts(std::span<const TopologyDocumentInput> documents,
                                                 const TopologyBuildConfig& config) {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto nowSeconds = std::chrono::duration_cast<std::chrono::seconds>(now).count();
    const auto nowMillis = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();

    TopologyArtifactBatch batch;
    batch.snapshotId = makeSnapshotId(static_cast<std::uint64_t>(nowMillis));
    batch.algorithm = "connected_components_v1";
    batch.inputKind = config.inputKind;
    batch.generatedAtUnixSeconds = static_cast<std::uint64_t>(nowSeconds);

    if (documents.empty()) {
        return batch;
    }

    std::unordered_map<std::string, std::size_t> indexByHash;
    indexByHash.reserve(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        if (!documents[i].documentHash.empty()) {
            indexByHash[documents[i].documentHash] = i;
        }
    }

    std::unordered_map<PairKey, float, PairKeyHash> pairWeights;
    for (std::size_t i = 0; i < documents.size(); ++i) {
        for (const auto& neighbor : documents[i].neighbors) {
            if (neighbor.documentHash.empty()) {
                continue;
            }
            const auto it = indexByHash.find(neighbor.documentHash);
            if (it == indexByHash.end()) {
                continue;
            }
            const std::size_t j = it->second;
            if (i == j) {
                continue;
            }
            if (config.reciprocalOnly && !neighbor.reciprocal) {
                continue;
            }
            if (neighbor.score < static_cast<float>(config.minEdgeScore)) {
                continue;
            }
            const PairKey key{std::min(i, j), std::max(i, j)};
            auto wt = pairWeights.find(key);
            if (wt == pairWeights.end()) {
                pairWeights.emplace(key, neighbor.score);
            } else {
                wt->second = std::max(wt->second, neighbor.score);
            }
        }
    }

    Adjacency adjacency(documents.size());
    for (const auto& [key, weight] : pairWeights) {
        adjacency[key.first].push_back({key.second, weight});
        adjacency[key.second].push_back({key.first, weight});
    }

    // BFS connected components over the filtered undirected graph.
    std::vector<bool> visited(documents.size(), false);
    std::vector<std::vector<std::size_t>> components;
    components.reserve(documents.size());
    for (std::size_t root = 0; root < documents.size(); ++root) {
        if (visited[root]) {
            continue;
        }
        std::queue<std::size_t> q;
        q.push(root);
        visited[root] = true;
        std::vector<std::size_t> component;
        while (!q.empty()) {
            const std::size_t current = q.front();
            q.pop();
            component.push_back(current);
            for (const auto& [next, _weight] : adjacency[current]) {
                if (!visited[next]) {
                    visited[next] = true;
                    q.push(next);
                }
            }
        }
        sortComponentByHash(component, documents);
        for (auto& piece : splitOversizedComponent(std::move(component), config.maxComponentDocs,
                                                   adjacency, documents)) {
            components.push_back(std::move(piece));
        }
    }

    batch.memberships.reserve(documents.size());
    for (const auto& component : components) {
        emitComponent(batch, component, adjacency, documents, config.routingRepresentativeCount);
    }

    std::ranges::sort(batch.clusters, {}, &ClusterArtifact::clusterId);
    std::ranges::sort(batch.memberships, {}, &DocumentClusterMembership::documentHash);
    (void)applyOrthogonalBoundarySpill(documents, config, batch);

    return batch;
}

Result<TopologyDirtyRegion> ConnectedComponentTopologyEngine::defineDirtyRegion(
    const TopologyArtifactBatch& existing, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config) const {
    TopologyDirtyRegion region;
    region.seedDocumentHashes.reserve(changedDocuments.size());
    std::unordered_map<std::string, std::vector<std::string>> clusterMembersById;
    std::unordered_map<std::string, std::string> clusterIdByDocument;
    clusterMembersById.reserve(existing.clusters.size());
    for (const auto& cluster : existing.clusters) {
        clusterMembersById.emplace(cluster.clusterId, cluster.memberDocumentHashes);
    }
    clusterIdByDocument.reserve(existing.memberships.size());
    for (const auto& membership : existing.memberships) {
        clusterIdByDocument.emplace(membership.documentHash, membership.clusterId);
    }

    std::unordered_set<std::string> seen;
    seen.reserve(changedDocuments.size() * 2);

    for (const auto& document : changedDocuments) {
        if (document.documentHash.empty()) {
            continue;
        }
        if (seen.insert(document.documentHash).second) {
            region.seedDocumentHashes.push_back(document.documentHash);
            region.expandedDocumentHashes.push_back(document.documentHash);
        }
        if (region.seedDocumentHashes.size() >= config.maxDirtySeedCount) {
            region.requiresWiderRebuild = true;
            break;
        }

        if (config.dirtyRegionExpansion != DirtyRegionExpansionMode::NeighborsOnly) {
            region.includedPriorClusterMembers = true;
            if (auto clusterIt = clusterIdByDocument.find(document.documentHash);
                clusterIt != clusterIdByDocument.end()) {
                if (auto membersIt = clusterMembersById.find(clusterIt->second);
                    membersIt != clusterMembersById.end()) {
                    for (const auto& memberHash : membersIt->second) {
                        if (memberHash.empty()) {
                            continue;
                        }
                        if (seen.insert(memberHash).second) {
                            region.expandedDocumentHashes.push_back(memberHash);
                        }
                        if (region.expandedDocumentHashes.size() >= config.maxDirtyRegionDocs) {
                            region.exceededRegionBudget = true;
                            region.requiresWiderRebuild = true;
                            break;
                        }
                    }
                }
            }
        }
        if (region.requiresWiderRebuild) {
            break;
        }

        if (config.dirtyRegionExpansion == DirtyRegionExpansionMode::NeighborsOnly ||
            config.dirtyRegionExpansion == DirtyRegionExpansionMode::PriorClusterAndNeighbors ||
            config.dirtyRegionExpansion == DirtyRegionExpansionMode::Adaptive) {
            region.includedSemanticNeighbors = true;
            for (const auto& neighbor : document.neighbors) {
                if (neighbor.documentHash.empty()) {
                    continue;
                }
                if (seen.insert(neighbor.documentHash).second) {
                    region.expandedDocumentHashes.push_back(neighbor.documentHash);
                }
                if (region.expandedDocumentHashes.size() >= config.maxDirtyRegionDocs) {
                    region.exceededRegionBudget = true;
                    region.requiresWiderRebuild = true;
                    break;
                }
            }
        }
        if (region.requiresWiderRebuild) {
            break;
        }
    }

    // When a seed's semantic neighbor belongs to a prior cluster, that cluster's
    // other members may also need re-examination — adding an edge between clusters
    // can merge them, which requires rebuilding every member. Walk the expanded
    // region a second time so neighbors-of-seeds contribute their prior-cluster
    // members too. Bounded by maxDirtyRegionDocs / maxDirtyRegionDepth.
    if (!region.requiresWiderRebuild &&
        (config.dirtyRegionExpansion == DirtyRegionExpansionMode::PriorClusterAndNeighbors ||
         config.dirtyRegionExpansion == DirtyRegionExpansionMode::Adaptive) &&
        config.maxDirtyRegionDepth >= 2) {
        std::vector<std::string> secondWave;
        secondWave.reserve(region.expandedDocumentHashes.size());
        for (const auto& hash : region.expandedDocumentHashes) {
            if (std::find(region.seedDocumentHashes.begin(), region.seedDocumentHashes.end(),
                          hash) != region.seedDocumentHashes.end()) {
                continue;
            }
            secondWave.push_back(hash);
        }
        for (const auto& hash : secondWave) {
            auto clusterIt = clusterIdByDocument.find(hash);
            if (clusterIt == clusterIdByDocument.end()) {
                continue;
            }
            auto membersIt = clusterMembersById.find(clusterIt->second);
            if (membersIt == clusterMembersById.end()) {
                continue;
            }
            region.includedPriorClusterMembers = true;
            for (const auto& memberHash : membersIt->second) {
                if (memberHash.empty()) {
                    continue;
                }
                if (seen.insert(memberHash).second) {
                    region.expandedDocumentHashes.push_back(memberHash);
                }
                if (region.expandedDocumentHashes.size() >= config.maxDirtyRegionDocs) {
                    region.exceededRegionBudget = true;
                    region.requiresWiderRebuild = true;
                    break;
                }
            }
            if (region.requiresWiderRebuild) {
                break;
            }
        }
    }

    region.bfsDepthReached =
        (region.includedSemanticNeighbors || region.includedPriorClusterMembers) ? 1 : 0;
    if (config.fullRebuildDocThreshold > 0 &&
        region.expandedDocumentHashes.size() >= config.fullRebuildDocThreshold) {
        region.requiresWiderRebuild = true;
    }

    std::sort(region.seedDocumentHashes.begin(), region.seedDocumentHashes.end());
    std::sort(region.expandedDocumentHashes.begin(), region.expandedDocumentHashes.end());
    return region;
}

Result<TopologyArtifactBatch> ConnectedComponentTopologyEngine::updateArtifacts(
    const TopologyArtifactBatch& existing, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config, TopologyUpdateStats* stats) {
    auto rebuilt = buildArtifacts(changedDocuments, config);
    if (!rebuilt) {
        return rebuilt.error();
    }

    if (changedDocuments.empty()) {
        return existing;
    }

    std::unordered_set<std::string> affectedHashes;
    affectedHashes.reserve(changedDocuments.size());
    for (const auto& document : changedDocuments) {
        if (!document.documentHash.empty()) {
            affectedHashes.insert(document.documentHash);
        }
    }

    TopologyArtifactBatch merged = existing;
    merged.snapshotId = rebuilt.value().snapshotId;
    merged.algorithm = rebuilt.value().algorithm;
    merged.inputKind = rebuilt.value().inputKind;
    merged.generatedAtUnixSeconds = rebuilt.value().generatedAtUnixSeconds;
    merged.topologyEpoch = existing.topologyEpoch + 1;

    std::vector<ClusterArtifact> removedClustersSnapshot;
    std::size_t removedClusters = 0;
    merged.clusters.erase(std::remove_if(merged.clusters.begin(), merged.clusters.end(),
                                         [&](const ClusterArtifact& cluster) {
                                             const bool intersects = std::any_of(
                                                 cluster.memberDocumentHashes.begin(),
                                                 cluster.memberDocumentHashes.end(),
                                                 [&](const std::string& documentHash) {
                                                     return affectedHashes.contains(documentHash);
                                                 });
                                             if (intersects) {
                                                 ++removedClusters;
                                                 removedClustersSnapshot.push_back(cluster);
                                             }
                                             return intersects;
                                         }),
                          merged.clusters.end());

    std::unordered_set<std::string> removedClusterIds;
    removedClusterIds.reserve(removedClustersSnapshot.size());
    for (const auto& cluster : removedClustersSnapshot) {
        removedClusterIds.insert(cluster.clusterId);
    }

    std::size_t removedMemberships = 0;
    merged.memberships.erase(
        std::remove_if(merged.memberships.begin(), merged.memberships.end(),
                       [&](const DocumentClusterMembership& membership) {
                           const bool removed = affectedHashes.contains(membership.documentHash) ||
                                                removedClusterIds.contains(membership.clusterId);
                           if (removed) {
                               ++removedMemberships;
                           }
                           return removed;
                       }),
        merged.memberships.end());

    auto tryReuseClusterId = [&](ClusterArtifact& rebuiltCluster) {
        if (!rebuiltCluster.medoid.has_value() || rebuiltCluster.medoid->documentHash.empty()) {
            return;
        }

        const std::string& medoidHash = rebuiltCluster.medoid->documentHash;
        const ClusterArtifact* matchedCluster = nullptr;
        for (const auto& oldCluster : removedClustersSnapshot) {
            if (!oldCluster.medoid.has_value() || oldCluster.medoid->documentHash != medoidHash) {
                continue;
            }
            const bool overlaps = std::any_of(
                rebuiltCluster.memberDocumentHashes.begin(),
                rebuiltCluster.memberDocumentHashes.end(), [&](const std::string& hash) {
                    return std::find(oldCluster.memberDocumentHashes.begin(),
                                     oldCluster.memberDocumentHashes.end(),
                                     hash) != oldCluster.memberDocumentHashes.end();
                });
            if (!overlaps) {
                continue;
            }
            if (matchedCluster != nullptr) {
                return;
            }
            matchedCluster = &oldCluster;
        }

        if (matchedCluster != nullptr) {
            rebuiltCluster.clusterId = matchedCluster->clusterId;
            rebuiltCluster.parentClusterId = matchedCluster->parentClusterId;
            if (rebuiltCluster.medoid.has_value()) {
                rebuiltCluster.medoid->clusterId = matchedCluster->clusterId;
            }
        }
    };

    for (auto& rebuiltCluster : rebuilt.value().clusters) {
        tryReuseClusterId(rebuiltCluster);
    }

    std::unordered_map<std::string, std::string> continuityClusterIdByDocument;
    for (const auto& cluster : rebuilt.value().clusters) {
        for (const auto& documentHash : cluster.memberDocumentHashes) {
            continuityClusterIdByDocument[documentHash] = cluster.clusterId;
        }
    }
    for (auto& membership : rebuilt.value().memberships) {
        if (auto it = continuityClusterIdByDocument.find(membership.documentHash);
            it != continuityClusterIdByDocument.end()) {
            membership.clusterId = it->second;
        }
    }

    merged.clusters.insert(merged.clusters.end(), rebuilt.value().clusters.begin(),
                           rebuilt.value().clusters.end());
    merged.memberships.insert(merged.memberships.end(), rebuilt.value().memberships.begin(),
                              rebuilt.value().memberships.end());

    std::sort(merged.clusters.begin(), merged.clusters.end(),
              [](const ClusterArtifact& lhs, const ClusterArtifact& rhs) {
                  return lhs.clusterId < rhs.clusterId;
              });
    std::sort(merged.memberships.begin(), merged.memberships.end(),
              [](const DocumentClusterMembership& lhs, const DocumentClusterMembership& rhs) {
                  return lhs.documentHash < rhs.documentHash;
              });

    if (stats != nullptr) {
        stats->documentsProcessed = changedDocuments.size();
        stats->clustersCreated = rebuilt.value().clusters.size();
        stats->clustersUpdated = removedClusters;
        stats->membershipsUpdated = rebuilt.value().memberships.size() + removedMemberships;
        stats->dirtySeedCount = changedDocuments.size();
        stats->dirtyRegionDocs = affectedHashes.size();
        stats->coalescedDirtySets = 0;
        stats->fallbackFullRebuilds = 0;
        stats->bridgeDocsTagged =
            std::count_if(rebuilt.value().memberships.begin(), rebuilt.value().memberships.end(),
                          [](const DocumentClusterMembership& membership) {
                              return membership.role == DocumentTopologyRole::Bridge;
                          });
        stats->medoidsSelected =
            std::count_if(rebuilt.value().memberships.begin(), rebuilt.value().memberships.end(),
                          [](const DocumentClusterMembership& membership) {
                              return membership.role == DocumentTopologyRole::Medoid;
                          });
    }
    return merged;
}

namespace {

float dotProduct(const std::vector<float>& a, const std::vector<float>& b) {
    if (a.size() != b.size() || a.empty()) {
        return 0.0F;
    }
    double acc = 0.0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        acc += static_cast<double>(a[i]) * static_cast<double>(b[i]);
    }
    return static_cast<float>(acc);
}

float vectorNorm(const std::vector<float>& a) {
    if (a.empty()) {
        return 0.0F;
    }
    double acc = 0.0;
    for (const float v : a) {
        acc += static_cast<double>(v) * static_cast<double>(v);
    }
    return static_cast<float>(std::sqrt(acc));
}

} // namespace

Result<std::vector<ClusterRoute>>
SparseGuidedClusterRouter::route(const TopologyRouteRequest& request,
                                 const TopologyArtifactBatch& artifacts) const {
    const auto index = buildRouteIndex(artifacts);
    return route(request, artifacts, index);
}

SparseRouteIndex
SparseGuidedClusterRouter::buildRouteIndex(const TopologyArtifactBatch& artifacts) {
    SparseRouteIndex index;
    index.centroidNorms.reserve(artifacts.clusters.size());
    index.routingRepresentativeNorms.reserve(artifacts.clusters.size());
    std::unordered_map<std::string, std::size_t> clusterIndexById;
    clusterIndexById.reserve(artifacts.clusters.size());
    for (std::size_t clusterIndex = 0; clusterIndex < artifacts.clusters.size(); ++clusterIndex) {
        const auto& cluster = artifacts.clusters[clusterIndex];
        clusterIndexById.emplace(cluster.clusterId, clusterIndex);
        index.centroidNorms.push_back(vectorNorm(cluster.centroidEmbedding));
        auto& representativeNorms = index.routingRepresentativeNorms.emplace_back();
        representativeNorms.reserve(cluster.routingRepresentatives.size());
        for (const auto& representative : cluster.routingRepresentatives) {
            representativeNorms.push_back(vectorNorm(representative.embedding));
        }
    }

    std::vector<std::size_t> centroidIds;
    std::vector<std::vector<float>> centroids;
    centroidIds.reserve(artifacts.clusters.size());
    centroids.reserve(artifacts.clusters.size());
    for (std::size_t clusterIndex = 0; clusterIndex < artifacts.clusters.size(); ++clusterIndex) {
        const auto& centroid = artifacts.clusters[clusterIndex].centroidEmbedding;
        if (centroid.empty() || index.centroidNorms[clusterIndex] <= 0.0F) {
            centroidIds.clear();
            centroids.clear();
            break;
        }
        centroidIds.push_back(clusterIndex);
        centroids.push_back(centroid);
    }
    if (!centroids.empty()) {
        auto annIndex = yams::vector::StaticCosineAnnIndex::build(centroidIds, centroids);
        if (annIndex) {
            index.centroidAnnIndex = std::move(annIndex).value();
        }
    }

    auto addMembership = [&](const std::string& hash, const std::string& clusterId) {
        const auto clusterIt = clusterIndexById.find(clusterId);
        if (hash.empty() || clusterIt == clusterIndexById.end()) {
            return;
        }
        auto& clusters = index.clustersByDocumentHash[hash];
        if (std::ranges::find(clusters, clusterIt->second) == clusters.end()) {
            clusters.push_back(clusterIt->second);
        }
    };
    if (!artifacts.memberships.empty()) {
        for (const auto& membership : artifacts.memberships) {
            addMembership(membership.documentHash, membership.clusterId);
            if (membership.parentClusterId.has_value()) {
                addMembership(membership.documentHash, *membership.parentClusterId);
            }
        }
    } else {
        // Compatibility for lightweight callers that construct cluster-only artifacts.
        for (std::size_t clusterIndex = 0; clusterIndex < artifacts.clusters.size();
             ++clusterIndex) {
            for (const auto& hash : artifacts.clusters[clusterIndex].memberDocumentHashes) {
                index.clustersByDocumentHash[hash].push_back(clusterIndex);
            }
        }
    }
    return index;
}

Result<std::vector<ClusterRoute>>
SparseGuidedClusterRouter::route(const TopologyRouteRequest& request,
                                 const TopologyArtifactBatch& artifacts,
                                 const SparseRouteIndex& index, SparseRouteWork* work) const {
    if (index.centroidNorms.size() != artifacts.clusters.size() ||
        index.routingRepresentativeNorms.size() != artifacts.clusters.size()) {
        return Error{ErrorCode::InvalidArgument,
                     "sparse route index does not match topology clusters"};
    }

    std::unordered_set<std::string> seeds(request.seedDocumentHashes.begin(),
                                          request.seedDocumentHashes.end());
    std::unordered_map<std::string, float> weightedSeeds;
    weightedSeeds.reserve(request.weightedSeedDocuments.size());
    for (const auto& seed : request.weightedSeedDocuments) {
        if (seed.documentHash.empty() || !std::isfinite(seed.weight) || seed.weight <= 0.0F) {
            continue;
        }
        auto [it, inserted] = weightedSeeds.emplace(seed.documentHash, seed.weight);
        if (!inserted) {
            it->second = std::max(it->second, seed.weight);
        }
    }

    struct ClusterSignals {
        double sparseMass{0.0};
        float dense{0.0F};
        bool denseObserved{false};
    };
    std::vector<ClusterSignals> signals(artifacts.clusters.size());

    double maxSparseMass = 0.0;
    if (!weightedSeeds.empty()) {
        for (const auto& [hash, weight] : weightedSeeds) {
            if (work != nullptr) {
                ++work->seedClusterLookups;
            }
            const auto clusterIt = index.clustersByDocumentHash.find(hash);
            if (clusterIt == index.clustersByDocumentHash.end()) {
                continue;
            }
            for (const auto clusterIndex : clusterIt->second) {
                if (clusterIndex < signals.size()) {
                    signals[clusterIndex].sparseMass += static_cast<double>(weight);
                }
            }
        }
    } else {
        for (const auto& hash : seeds) {
            if (work != nullptr) {
                ++work->seedClusterLookups;
            }
            const auto clusterIt = index.clustersByDocumentHash.find(hash);
            if (clusterIt == index.clustersByDocumentHash.end()) {
                continue;
            }
            for (const auto clusterIndex : clusterIt->second) {
                if (clusterIndex < signals.size()) {
                    signals[clusterIndex].sparseMass += 1.0;
                }
            }
        }
    }
    for (const auto& signal : signals) {
        maxSparseMass = std::max(maxSparseMass, signal.sparseMass);
    }

    const float alpha = std::clamp(request.sparseDenseAlpha, 0.0F, 1.0F);
    const float queryNorm = vectorNorm(request.queryEmbedding);
    if (!request.queryEmbedding.empty() && work != nullptr) {
        ++work->queryNormEvaluations;
    }

    std::vector<bool> routeCandidates(artifacts.clusters.size(), true);
    const bool denseAnnEligible = alpha < 1.0F && queryNorm > 0.0F && request.limit > 0 &&
                                  request.denseAnnCandidateLimit > 0 && index.centroidAnnIndex &&
                                  artifacts.clusters.size() > request.denseAnnCandidateLimit;
    if (denseAnnEligible) {
        const auto candidateLimit = std::min(
            artifacts.clusters.size(), std::max(request.denseAnnCandidateLimit, request.limit));
        auto annResult = index.centroidAnnIndex->search(request.queryEmbedding, candidateLimit);
        if (annResult) {
            std::fill(routeCandidates.begin(), routeCandidates.end(), false);
            for (const auto& hit : annResult.value().hits) {
                if (hit.id < routeCandidates.size()) {
                    routeCandidates[hit.id] = true;
                }
            }
            for (std::size_t clusterIndex = 0; clusterIndex < signals.size(); ++clusterIndex) {
                if (signals[clusterIndex].sparseMass > 0.0) {
                    routeCandidates[clusterIndex] = true;
                }
            }
            if (work != nullptr) {
                work->denseAnnUsed = true;
                work->denseAnnCandidates = annResult.value().hits.size();
                work->denseAnnDistanceEvaluations = annResult.value().distanceEvaluations;
                work->representativeDistanceEvaluations += annResult.value().distanceEvaluations;
            }
        }
    }

    for (std::size_t clusterIndex = 0; clusterIndex < artifacts.clusters.size(); ++clusterIndex) {
        if (!routeCandidates[clusterIndex] || alpha >= 1.0F) {
            continue;
        }
        const auto& cluster = artifacts.clusters[clusterIndex];
        const float centroidNorm = index.centroidNorms[clusterIndex];
        if (queryNorm > 0.0F && centroidNorm > 0.0F && !cluster.centroidEmbedding.empty()) {
            auto dense = dotProduct(request.queryEmbedding, cluster.centroidEmbedding) /
                         (queryNorm * centroidNorm);
            // Map [-1,1] -> [0,1] so it composes with the bm25 mass cleanly.
            signals[clusterIndex].dense = std::clamp((dense + 1.0F) * 0.5F, 0.0F, 1.0F);
            signals[clusterIndex].denseObserved = true;
            if (work != nullptr) {
                ++work->representativeDistanceEvaluations;
                ++work->exactRepresentativeDistanceEvaluations;
            }
        }
        const auto& representativeNorms = index.routingRepresentativeNorms[clusterIndex];
        if (representativeNorms.size() != cluster.routingRepresentatives.size()) {
            return Error{ErrorCode::InvalidArgument,
                         "sparse route index does not match routing representatives"};
        }
        const auto extraRepresentativeLimit =
            request.maxRoutingRepresentatives == 0
                ? cluster.routingRepresentatives.size()
                : std::min(cluster.routingRepresentatives.size(),
                           request.maxRoutingRepresentatives > 0
                               ? request.maxRoutingRepresentatives - 1
                               : std::size_t{0});
        for (std::size_t representativeIndex = 0; representativeIndex < extraRepresentativeLimit;
             ++representativeIndex) {
            const auto& representative = cluster.routingRepresentatives[representativeIndex];
            const auto representativeNorm = representativeNorms[representativeIndex];
            if (queryNorm <= 0.0F || representativeNorm <= 0.0F ||
                representative.embedding.size() != request.queryEmbedding.size()) {
                continue;
            }
            const auto cosine = dotProduct(request.queryEmbedding, representative.embedding) /
                                (queryNorm * representativeNorm);
            const auto dense = std::clamp((cosine + 1.0F) * 0.5F, 0.0F, 1.0F);
            signals[clusterIndex].dense = std::max(signals[clusterIndex].dense, dense);
            signals[clusterIndex].denseObserved = true;
            if (work != nullptr) {
                ++work->representativeDistanceEvaluations;
                ++work->exactRepresentativeDistanceEvaluations;
            }
        }
    }

    std::vector<ClusterRoute> routes;
    routes.reserve(artifacts.clusters.size());
    for (std::size_t clusterIndex = 0; clusterIndex < artifacts.clusters.size(); ++clusterIndex) {
        if (!routeCandidates[clusterIndex]) {
            continue;
        }
        const auto& cluster = artifacts.clusters[clusterIndex];
        const auto& clusterSignals = signals[clusterIndex];
        const float dense = clusterSignals.dense;
        const float sparseNorm = maxSparseMass > 0.0
                                     ? static_cast<float>(clusterSignals.sparseMass / maxSparseMass)
                                     : 0.0F;
        const double blended = static_cast<double>(alpha * sparseNorm + (1.0F - alpha) * dense);
        const double cohesion = std::clamp(cluster.cohesionScore, 0.0, 1.0);
        const double stability = std::clamp(cluster.persistenceScore, 0.0, 1.0);
        const double sizeDamp = 1.0 / (1.0 + std::log1p(static_cast<double>(cluster.memberCount)));
        double routeScore = blended + (cluster.persistenceScore * 0.05);
        switch (request.scoringMode) {
            case RouteScoringMode::SizeWeighted: {
                routeScore = (blended + (0.05 * stability) + (0.05 * cohesion)) * sizeDamp;
                break;
            }
            case RouteScoringMode::SeedCoverage:
                routeScore = static_cast<double>(sparseNorm) + (0.10 * dense) +
                             (cluster.persistenceScore * 0.05);
                break;
            case RouteScoringMode::Current:
            default:
                break;
        }
        routes.push_back(ClusterRoute{
            .clusterId = cluster.clusterId,
            .medoidDocumentHash = cluster.medoid.has_value()
                                      ? std::optional<std::string>(cluster.medoid->documentHash)
                                      : std::nullopt,
            .routeScore = routeScore,
            .stabilityScore = cluster.persistenceScore,
            .memberCount = cluster.memberCount,
            .semanticCost = clusterSignals.denseObserved
                                ? std::optional<double>(1.0 - static_cast<double>(dense))
                                : std::nullopt,
            .sparseCost = maxSparseMass > 0.0
                              ? std::optional<double>(1.0 - static_cast<double>(sparseNorm))
                              : std::nullopt,
            .distortionPenalty = cluster.coordinateDistortion,
            .localIntrinsicDimension = cluster.localIntrinsicDimension,
            .persistencePenalty = 1.0 - stability,
            .cohesionPenalty = 1.0 - cohesion,
            .sizePenalty = 1.0 - sizeDamp});
    }

    std::sort(routes.begin(), routes.end(), [](const ClusterRoute& lhs, const ClusterRoute& rhs) {
        if (lhs.routeScore != rhs.routeScore) {
            return lhs.routeScore > rhs.routeScore;
        }
        return lhs.clusterId < rhs.clusterId;
    });
    if (routes.size() > 1) {
        for (std::size_t index = 0; index < routes.size(); ++index) {
            const auto upperGap = index > 0
                                      ? routes[index - 1].routeScore - routes[index].routeScore
                                      : std::numeric_limits<double>::infinity();
            const auto lowerGap = index + 1 < routes.size()
                                      ? routes[index].routeScore - routes[index + 1].routeScore
                                      : std::numeric_limits<double>::infinity();
            const auto nearestCompetitorGap = std::min(upperGap, lowerGap);
            routes[index].uncertaintyPenalty = 1.0 - std::clamp(nearestCompetitorGap, 0.0, 1.0);
        }
    }
    if (request.limit > 0 && routes.size() > request.limit) {
        routes.resize(request.limit);
    }
    return routes;
}

} // namespace yams::topology
