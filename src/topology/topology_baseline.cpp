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
    const auto now = std::chrono::duration_cast<std::chrono::seconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    TopologyArtifactBatch batch;
    batch.snapshotId = makeSnapshotId(static_cast<std::uint64_t>(now));
    batch.algorithm = "connected_components_v1";
    batch.inputKind = config.inputKind;
    batch.generatedAtUnixSeconds = static_cast<std::uint64_t>(now);

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

Result<TopologyArtifactBatch> ConnectedComponentTopologyEngine::updateArtifacts(
    const TopologyArtifactBatch& existing, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config, TopologyUpdateStats* stats) {
    auto rebuilt = buildArtifacts(changedDocuments, config);
    if (!rebuilt) {
        return rebuilt.error();
    }
    if (stats != nullptr) {
        stats->documentsProcessed = changedDocuments.size();
        stats->clustersCreated = existing.clusters.empty() ? rebuilt.value().clusters.size() : 0;
        stats->clustersUpdated = existing.clusters.empty() ? 0 : rebuilt.value().clusters.size();
        stats->membershipsUpdated = rebuilt.value().memberships.size();
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
    return rebuilt;
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
