#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_sgc.h>

#include <Hdbscan/hdbscan.hpp>
#include <Runner/hdbscanParameters.hpp>
#include <Runner/hdbscanResult.hpp>
#include <Runner/hdbscanRunner.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <map>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::topology {

namespace {

using PairKey = std::pair<std::size_t, std::size_t>;

std::string makeSnapshotId(std::uint64_t unixMillis) {
    return "topology-" + std::to_string(unixMillis);
}

std::string makeClusterId(const std::string& anchorHash) {
    return "topology.cluster." + anchorHash;
}

struct TimeStamps {
    std::uint64_t unixSeconds{0};
    std::uint64_t unixMillis{0};
};

TimeStamps nowStamps() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    TimeStamps ts;
    ts.unixSeconds =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(now).count());
    ts.unixMillis = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
    return ts;
}

std::map<PairKey, float>
buildPairWeights(std::span<const TopologyDocumentInput> documents,
                 const std::unordered_map<std::string, std::size_t>& indexByHash,
                 const TopologyBuildConfig& config) {
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
    return pairWeights;
}

std::vector<std::vector<std::pair<std::size_t, float>>>
makeAdjacency(std::size_t n, const std::map<PairKey, float>& pairWeights) {
    std::vector<std::vector<std::pair<std::size_t, float>>> adjacency(n);
    for (const auto& [key, weight] : pairWeights) {
        adjacency[key.first].emplace_back(key.second, weight);
        adjacency[key.second].emplace_back(key.first, weight);
    }
    return adjacency;
}

// Build a TopologyArtifactBatch from a per-document cluster assignment.
//
// `assignment[i]` is the cluster id for documents[i]; any value is accepted as
// an opaque bucket key. Cohesion / persistence are computed from `pairWeights`
// when provided (graph-based engines); otherwise only structural metrics
// populate. `algorithm` is stamped into the batch.
TopologyArtifactBatch buildBatchFromAssignment(std::span<const TopologyDocumentInput> documents,
                                               const std::vector<std::int64_t>& assignment,
                                               const std::map<PairKey, float>& pairWeights,
                                               std::string algorithm, const TimeStamps& ts) {
    TopologyArtifactBatch batch;
    batch.snapshotId = makeSnapshotId(ts.unixMillis);
    batch.algorithm = std::move(algorithm);
    batch.generatedAtUnixSeconds = ts.unixSeconds;

    if (documents.empty()) {
        return batch;
    }

    std::unordered_map<std::int64_t, std::vector<std::size_t>> buckets;
    buckets.reserve(assignment.size());
    for (std::size_t i = 0; i < assignment.size(); ++i) {
        buckets[assignment[i]].push_back(i);
    }

    const auto adjacency = makeAdjacency(documents.size(), pairWeights);

    std::vector<std::pair<std::int64_t, std::vector<std::size_t>>> ordered(buckets.begin(),
                                                                           buckets.end());
    for (auto& [bucketId, members] : ordered) {
        std::sort(members.begin(), members.end(), [&documents](std::size_t lhs, std::size_t rhs) {
            return documents[lhs].documentHash < documents[rhs].documentHash;
        });
    }
    std::sort(ordered.begin(), ordered.end(), [&documents](const auto& lhs, const auto& rhs) {
        return documents[lhs.second.front()].documentHash <
               documents[rhs.second.front()].documentHash;
    });

    for (auto& [bucketId, members] : ordered) {
        if (members.empty()) {
            continue;
        }
        const std::string clusterId = makeClusterId(documents[members.front()].documentHash);
        std::unordered_set<std::size_t> memberSet(members.begin(), members.end());

        double cohesion = 0.0;
        double persistence = 0.0;
        std::size_t internalEdgeCount = 0;
        std::unordered_map<std::size_t, double> weightedDegree;
        weightedDegree.reserve(members.size());
        std::size_t bridgeCount = 0;

        for (std::size_t idx : members) {
            std::size_t degree = 0;
            for (const auto& [neighborIdx, weight] : adjacency[idx]) {
                if (!memberSet.contains(neighborIdx)) {
                    continue;
                }
                weightedDegree[idx] += weight;
                ++degree;
            }
            if (members.size() > 2 && degree >= 2) {
                ++bridgeCount;
            }
        }

        for (const auto& [key, weight] : pairWeights) {
            if (memberSet.contains(key.first) && memberSet.contains(key.second)) {
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

        std::size_t medoidIdx = members.front();
        double medoidScore = -1.0;
        for (std::size_t idx : members) {
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
        cluster.memberCount = members.size();
        cluster.persistenceScore = persistence;
        cluster.cohesionScore = cohesion;
        cluster.bridgeMass =
            members.empty() ? 0.0 : static_cast<double>(bridgeCount) / members.size();
        cluster.medoid = ClusterRepresentative{.clusterId = clusterId,
                                               .documentHash = documents[medoidIdx].documentHash,
                                               .filePath = documents[medoidIdx].filePath,
                                               .representativeScore = std::max(0.0, medoidScore)};
        cluster.memberDocumentHashes.reserve(members.size());
        for (std::size_t idx : members) {
            cluster.memberDocumentHashes.push_back(documents[idx].documentHash);
        }
        batch.clusters.push_back(std::move(cluster));

        const std::size_t maxDegree = members.size() > 1 ? members.size() - 1 : 0;
        for (std::size_t idx : members) {
            const double bridgeScore =
                maxDegree == 0 ? 0.0 : weightedDegree[idx] / static_cast<double>(maxDegree);
            DocumentTopologyRole role = DocumentTopologyRole::Core;
            if (members.size() == 1) {
                role = DocumentTopologyRole::Outlier;
            } else if (idx == medoidIdx) {
                role = DocumentTopologyRole::Medoid;
            } else if (members.size() > 2 && weightedDegree[idx] >= 2.0) {
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

TopologyDirtyRegion defaultDirtyRegion(std::span<const TopologyDocumentInput> changed) {
    TopologyDirtyRegion region;
    region.requiresWiderRebuild = true;
    region.seedDocumentHashes.reserve(changed.size());
    region.expandedDocumentHashes.reserve(changed.size());
    std::unordered_set<std::string> seen;
    for (const auto& doc : changed) {
        if (doc.documentHash.empty()) {
            continue;
        }
        if (seen.insert(doc.documentHash).second) {
            region.seedDocumentHashes.push_back(doc.documentHash);
            region.expandedDocumentHashes.push_back(doc.documentHash);
        }
    }
    return region;
}

// -------------------- HDBSCAN --------------------

double cosineDistance(std::span<const float> a, std::span<const float> b) {
    if (a.size() != b.size() || a.empty()) {
        return 2.0;
    }
    double dot = 0.0;
    double na = 0.0;
    double nb = 0.0;
    for (std::size_t i = 0; i < a.size(); ++i) {
        dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        na += static_cast<double>(a[i]) * static_cast<double>(a[i]);
        nb += static_cast<double>(b[i]) * static_cast<double>(b[i]);
    }
    if (na <= 0.0 || nb <= 0.0) {
        return 2.0;
    }
    const double cos = dot / (std::sqrt(na) * std::sqrt(nb));
    return 1.0 - std::clamp(cos, -1.0, 1.0);
}

std::vector<std::int64_t> runHDBSCAN(std::span<const TopologyDocumentInput> documents,
                                     std::size_t requestedMinPoints,
                                     std::size_t requestedMinClusterSize) {
    const std::size_t n = documents.size();
    std::vector<std::int64_t> assignment(n, -1);
    if (n == 0) {
        return assignment;
    }

    std::vector<std::size_t> usable;
    usable.reserve(n);
    std::size_t embeddingDim = 0;
    for (std::size_t i = 0; i < n; ++i) {
        if (!documents[i].embedding.empty()) {
            if (embeddingDim == 0) {
                embeddingDim = documents[i].embedding.size();
            }
            if (documents[i].embedding.size() == embeddingDim) {
                usable.push_back(i);
            }
        }
    }
    if (usable.size() < 2 || embeddingDim == 0) {
        std::iota(assignment.begin(), assignment.end(), 0);
        return assignment;
    }

    std::size_t minClusterSize = requestedMinClusterSize;
    if (minClusterSize == 0) {
        minClusterSize = std::max<std::size_t>(2, static_cast<std::size_t>(std::round(std::log2(
                                                      static_cast<double>(usable.size()) + 1.0))));
    }
    minClusterSize = std::min(minClusterSize, std::max<std::size_t>(2, usable.size() / 2));

    std::size_t minPoints = requestedMinPoints == 0 ? minClusterSize : requestedMinPoints;
    minPoints = std::min(minPoints, std::max<std::size_t>(2, usable.size() - 1));

    std::vector<std::vector<double>> distances(usable.size(),
                                               std::vector<double>(usable.size(), 0.0));
    for (std::size_t i = 0; i < usable.size(); ++i) {
        for (std::size_t j = i + 1; j < usable.size(); ++j) {
            const double d =
                cosineDistance(documents[usable[i]].embedding, documents[usable[j]].embedding);
            distances[i][j] = d;
            distances[j][i] = d;
        }
    }

    hdbscanParameters params;
    params.distances = std::move(distances);
    params.distanceFunction = "";
    params.minPoints = static_cast<std::uint32_t>(minPoints);
    params.minClusterSize = static_cast<std::uint32_t>(minClusterSize);

    hdbscanRunner runner;
    hdbscanResult result;
    try {
        result = runner.run(params);
    } catch (const std::exception& e) {
        spdlog::warn("[hdbscan] runner threw: {}; falling back to singletons", e.what());
        std::iota(assignment.begin(), assignment.end(), 0);
        return assignment;
    }

    if (result.labels.size() != usable.size()) {
        spdlog::warn("[hdbscan] label count mismatch ({} vs {}); falling back to singletons",
                     result.labels.size(), usable.size());
        std::iota(assignment.begin(), assignment.end(), 0);
        return assignment;
    }

    std::unordered_map<int, std::int64_t> canon;
    for (std::size_t u = 0; u < usable.size(); ++u) {
        const int lbl = result.labels[u];
        const auto docIdx = static_cast<std::int64_t>(usable[u]);
        if (lbl == 0) {
            continue;
        }
        auto it = canon.find(lbl);
        if (it == canon.end()) {
            canon.emplace(lbl, docIdx);
        } else if (docIdx < it->second) {
            it->second = docIdx;
        }
    }

    std::int64_t orphanId = static_cast<std::int64_t>(n);
    for (std::size_t u = 0; u < usable.size(); ++u) {
        const int lbl = result.labels[u];
        if (lbl == 0) {
            assignment[usable[u]] = orphanId++;
        } else {
            assignment[usable[u]] = canon[lbl];
        }
    }
    for (std::size_t i = 0; i < n; ++i) {
        if (assignment[i] < 0) {
            assignment[i] = orphanId++;
        }
    }
    return assignment;
}

} // namespace

// ============================================================================
// HDBSCANTopologyEngine
// ============================================================================

Result<TopologyArtifactBatch>
HDBSCANTopologyEngine::buildArtifacts(std::span<const TopologyDocumentInput> documents,
                                      const TopologyBuildConfig& config) {
    const auto ts = nowStamps();
    if (documents.empty()) {
        TopologyArtifactBatch batch;
        batch.snapshotId = makeSnapshotId(ts.unixMillis);
        batch.algorithm = "hdbscan_v1";
        batch.inputKind = config.inputKind;
        batch.generatedAtUnixSeconds = ts.unixSeconds;
        return batch;
    }

    std::unordered_map<std::string, std::size_t> indexByHash;
    indexByHash.reserve(documents.size());
    for (std::size_t i = 0; i < documents.size(); ++i) {
        if (!documents[i].documentHash.empty()) {
            indexByHash[documents[i].documentHash] = i;
        }
    }
    auto pairWeights = buildPairWeights(documents, indexByHash, config);

    std::vector<TopologyDocumentInput> smoothedOwned;
    std::span<const TopologyDocumentInput> clusteringInput = documents;
    if (config.featureSmoothingHops > 0) {
        smoothedOwned.assign(documents.begin(), documents.end());
        applySGCSmoothing(smoothedOwned, config, config.featureSmoothingHops);
        clusteringInput = smoothedOwned;
    }

    auto assignment =
        runHDBSCAN(clusteringInput, config.hdbscanMinPoints, config.hdbscanMinClusterSize);
    auto batch = buildBatchFromAssignment(documents, assignment, pairWeights, "hdbscan_v1", ts);
    batch.inputKind = config.inputKind;
    return batch;
}

Result<TopologyDirtyRegion>
HDBSCANTopologyEngine::defineDirtyRegion(const TopologyArtifactBatch&,
                                         std::span<const TopologyDocumentInput> changedDocuments,
                                         const TopologyBuildConfig&) const {
    return defaultDirtyRegion(changedDocuments);
}

Result<TopologyArtifactBatch> HDBSCANTopologyEngine::updateArtifacts(
    const TopologyArtifactBatch&, std::span<const TopologyDocumentInput> changedDocuments,
    const TopologyBuildConfig& config, TopologyUpdateStats* stats) {
    if (stats != nullptr) {
        stats->fallbackFullRebuilds = 1;
    }
    return buildArtifacts(changedDocuments, config);
}

} // namespace yams::topology
