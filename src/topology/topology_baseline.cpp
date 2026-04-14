#include <yams/topology/topology_baseline.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <map>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace yams::topology {

namespace {

using PairKey = std::pair<std::size_t, std::size_t>;

std::string makeSnapshotId(std::uint64_t unixSeconds) {
    return "topology-" + std::to_string(unixSeconds);
}

std::string makeClusterId(const std::string& anchorHash) {
    return "topology.cluster." + anchorHash;
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

    std::map<PairKey, float> pairWeights;
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

    std::vector<std::vector<std::pair<std::size_t, float>>> adjacency(documents.size());
    for (const auto& [key, weight] : pairWeights) {
        adjacency[key.first].push_back({key.second, weight});
        adjacency[key.second].push_back({key.first, weight});
    }

    std::vector<bool> visited(documents.size(), false);
    batch.memberships.reserve(documents.size());

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

        std::sort(component.begin(), component.end(),
                  [&documents](std::size_t lhs, std::size_t rhs) {
                      return documents[lhs].documentHash < documents[rhs].documentHash;
                  });

        const std::string clusterId = makeClusterId(documents[component.front()].documentHash);
        std::unordered_set<std::size_t> componentSet(component.begin(), component.end());

        double cohesion = 0.0;
        double persistence = 0.0;
        std::size_t internalEdgeCount = 0;
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
            }
            if (component.size() > 2 && degree >= 2) {
                ++bridgeCount;
            }
        }

        for (const auto& [key, weight] : pairWeights) {
            if (componentSet.contains(key.first) && componentSet.contains(key.second)) {
                cohesion += weight;
                persistence =
                    internalEdgeCount == 0 ? weight : std::min<double>(persistence, weight);
                ++internalEdgeCount;
            }
        }

        if (internalEdgeCount > 0) {
            cohesion /= static_cast<double>(internalEdgeCount);
        } else {
            persistence = 0.0;
        }

        std::size_t medoidIdx = component.front();
        double medoidScore = -1.0;
        for (std::size_t idx : component) {
            const double candidate = weightedDegree[idx];
            if (candidate > medoidScore ||
                (std::abs(candidate - medoidScore) < 1e-9 &&
                 documents[idx].documentHash < documents[medoidIdx].documentHash)) {
                medoidIdx = idx;
                medoidScore = candidate;
            }
        }

        ClusterArtifact cluster;
        cluster.clusterId = clusterId;
        cluster.level = 0;
        cluster.memberCount = component.size();
        cluster.persistenceScore = persistence;
        cluster.cohesionScore = cohesion;
        cluster.bridgeMass =
            component.empty() ? 0.0 : static_cast<double>(bridgeCount) / component.size();
        cluster.medoid = ClusterRepresentative{.clusterId = clusterId,
                                               .documentHash = documents[medoidIdx].documentHash,
                                               .filePath = documents[medoidIdx].filePath,
                                               .representativeScore = std::max(0.0, medoidScore)};
        cluster.memberDocumentHashes.reserve(component.size());
        for (std::size_t idx : component) {
            cluster.memberDocumentHashes.push_back(documents[idx].documentHash);
        }
        batch.clusters.push_back(std::move(cluster));

        for (std::size_t idx : component) {
            const std::size_t maxDegree = component.size() > 1 ? component.size() - 1 : 0;
            const double bridgeScore = maxDegree == 0 ? 0.0 : weightedDegree[idx] / maxDegree;
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

    std::sort(batch.clusters.begin(), batch.clusters.end(),
              [](const ClusterArtifact& lhs, const ClusterArtifact& rhs) {
                  return lhs.clusterId < rhs.clusterId;
              });
    std::sort(batch.memberships.begin(), batch.memberships.end(),
              [](const DocumentClusterMembership& lhs, const DocumentClusterMembership& rhs) {
                  return lhs.documentHash < rhs.documentHash;
              });

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

Result<std::vector<ClusterRoute>>
StableClusterTopologyRouter::route(const TopologyRouteRequest& request,
                                   const TopologyArtifactBatch& artifacts) const {
    std::unordered_set<std::string> seeds(request.seedDocumentHashes.begin(),
                                          request.seedDocumentHashes.end());
    std::vector<ClusterRoute> routes;
    routes.reserve(artifacts.clusters.size());

    for (const auto& cluster : artifacts.clusters) {
        double routeScore = cluster.persistenceScore + (cluster.cohesionScore * 0.5);
        bool matchedSeed = false;
        for (const auto& documentHash : cluster.memberDocumentHashes) {
            if (seeds.contains(documentHash)) {
                matchedSeed = true;
                break;
            }
        }
        if (matchedSeed) {
            routeScore += 1.0;
        }
        routeScore += std::min<double>(0.25, static_cast<double>(cluster.memberCount) / 40.0);

        routes.push_back(ClusterRoute{
            .clusterId = cluster.clusterId,
            .medoidDocumentHash = cluster.medoid.has_value()
                                      ? std::optional<std::string>(cluster.medoid->documentHash)
                                      : std::nullopt,
            .routeScore = routeScore,
            .stabilityScore = cluster.persistenceScore,
            .memberCount = cluster.memberCount});
    }

    std::sort(routes.begin(), routes.end(), [](const ClusterRoute& lhs, const ClusterRoute& rhs) {
        if (lhs.routeScore != rhs.routeScore) {
            return lhs.routeScore > rhs.routeScore;
        }
        return lhs.clusterId < rhs.clusterId;
    });

    if (request.limit > 0 && routes.size() > request.limit) {
        routes.resize(request.limit);
    }
    return routes;
}

} // namespace yams::topology
