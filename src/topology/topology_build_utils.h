#pragma once

#include <yams/topology/topology_engine.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace yams::topology::detail {

using PairKey = std::pair<std::size_t, std::size_t>;

struct PairKeyHash {
    std::size_t operator()(const PairKey& key) const noexcept {
        const std::uint64_t first = static_cast<std::uint64_t>(key.first);
        const std::uint64_t second = static_cast<std::uint64_t>(key.second);
        std::uint64_t mixed = first * 0x9E3779B97F4A7C15ULL + second;
        mixed ^= mixed >> 33;
        mixed *= 0xFF51AFD7ED558CCDULL;
        mixed ^= mixed >> 33;
        return static_cast<std::size_t>(mixed);
    }
};

inline std::vector<float> meanEmbedding(std::span<const TopologyDocumentInput> documents,
                                        const std::vector<std::size_t>& members) {
    std::vector<float> centroid;
    std::size_t count = 0;
    for (const std::size_t index : members) {
        if (index >= documents.size() || documents[index].embedding.empty()) {
            continue;
        }

        const auto& embedding = documents[index].embedding;
        if (centroid.empty()) {
            centroid = embedding;
            count = 1;
            continue;
        }
        if (centroid.size() != embedding.size()) {
            continue;
        }

        for (std::size_t dimension = 0; dimension < embedding.size(); ++dimension) {
            centroid[dimension] += embedding[dimension];
        }
        ++count;
    }

    if (count == 0) {
        return {};
    }
    if (count > 1) {
        for (float& value : centroid) {
            value /= static_cast<float>(count);
        }
    }
    return centroid;
}

inline void normalizeVector(std::vector<float>& vec) {
    double normSq = 0.0;
    for (float v : vec) {
        normSq += static_cast<double>(v) * static_cast<double>(v);
    }
    if (normSq > 0.0) {
        const auto invNorm = static_cast<float>(1.0 / std::sqrt(normSq));
        for (float& v : vec) {
            v *= invNorm;
        }
    }
}

/// Primary cluster of every document in `batch`, from its memberships. Overlap (boundary-spill)
/// clusters are not primary and do not participate in roads.
inline std::unordered_map<std::string_view, std::string_view>
primaryClusterByHash(const TopologyArtifactBatch& batch) {
    std::unordered_map<std::string_view, std::string_view> primary;
    primary.reserve(batch.memberships.size());
    for (const auto& membership : batch.memberships) {
        if (!membership.documentHash.empty() && !membership.clusterId.empty()) {
            primary.emplace(membership.documentHash, membership.clusterId);
        }
    }
    return primary;
}

/// Undirected cluster pair weights keyed by the ordered (lo, hi) cluster ids. The views point
/// into the memberships the `primaryByHash` map was built from.
using ClusterPair = std::pair<std::string_view, std::string_view>;
struct ClusterPairHash {
    std::size_t operator()(const ClusterPair& key) const noexcept {
        const std::size_t lhs = std::hash<std::string_view>{}(key.first);
        const std::size_t rhs = std::hash<std::string_view>{}(key.second);
        return PairKeyHash{}(PairKey{lhs, rhs});
    }
};
using ClusterPairWeights = std::unordered_map<ClusterPair, double, ClusterPairHash>;

/// Add every raw neighbor observation in `documents` to `pairWeights`: each finite, non-negative
/// score whose endpoints sit in two different primary clusters adds to that undirected pair.
///
/// `observedDocuments` names the documents whose neighbor lists `documents` contains. Null means
/// all of them (a full build): a reciprocal pair is then observed from both sides and the weight
/// is symmetric on its own. During an incremental update only the dirty region's lists are
/// available, so a target outside `observedDocuments` is also credited with the mirror
/// observation its own list would have contributed. That is exact on a symmetric neighbor graph
/// and the stated approximation otherwise; a batch records it through
/// `TopologyArtifactBatch::roadsIncrementalPasses`.
inline void accumulateClusterPairWeights(
    std::span<const TopologyDocumentInput> documents,
    const std::unordered_map<std::string_view, std::string_view>& primaryByHash,
    const std::unordered_set<std::string_view>* observedDocuments,
    ClusterPairWeights& pairWeights) {
    for (const auto& document : documents) {
        const auto fromIt = primaryByHash.find(document.documentHash);
        if (fromIt == primaryByHash.end()) {
            continue;
        }
        const std::string_view fromCluster = fromIt->second;
        for (const auto& neighbor : document.neighbors) {
            if (neighbor.documentHash.empty() || !std::isfinite(neighbor.score) ||
                neighbor.score < 0.0F) {
                continue;
            }
            const auto toIt = primaryByHash.find(neighbor.documentHash);
            if (toIt == primaryByHash.end() || toIt->second == fromCluster) {
                continue;
            }
            const bool mirrored = observedDocuments != nullptr &&
                                  !observedDocuments->contains(std::string_view{toIt->first});
            const auto [lo, hi] = std::minmax(fromCluster, toIt->second);
            pairWeights[ClusterPair{lo, hi}] +=
                static_cast<double>(neighbor.score) * (mirrored ? 2.0 : 1.0);
        }
    }
}

/// Append both orientations of every pair weight.
inline void appendClusterRoadOrientations(const ClusterPairWeights& pairWeights,
                                          std::vector<ClusterRoad>& roads) {
    roads.reserve(roads.size() + pairWeights.size() * 2);
    for (const auto& [pair, weight] : pairWeights) {
        roads.push_back(ClusterRoad{.fromClusterId = std::string(pair.first),
                                    .toClusterId = std::string(pair.second),
                                    .weight = weight});
        roads.push_back(ClusterRoad{.fromClusterId = std::string(pair.second),
                                    .toClusterId = std::string(pair.first),
                                    .weight = weight});
    }
}

/// Order roads by (from asc, weight desc, to asc) and keep each source's `maxPerCluster`
/// heaviest; ties break on the target cluster id. `maxPerCluster == 0` keeps nothing.
inline std::vector<ClusterRoad> rankClusterRoads(std::vector<ClusterRoad> roads,
                                                 std::size_t maxPerCluster) {
    if (maxPerCluster == 0) {
        return {};
    }
    std::sort(roads.begin(), roads.end(), [](const ClusterRoad& lhs, const ClusterRoad& rhs) {
        if (lhs.fromClusterId != rhs.fromClusterId) {
            return lhs.fromClusterId < rhs.fromClusterId;
        }
        if (lhs.weight != rhs.weight) {
            return lhs.weight > rhs.weight;
        }
        return lhs.toClusterId < rhs.toClusterId;
    });

    std::vector<ClusterRoad> kept;
    kept.reserve(roads.size());
    std::size_t runLength = 0;
    for (auto& road : roads) {
        if (!kept.empty() && kept.back().fromClusterId == road.fromClusterId) {
            if (runLength >= maxPerCluster) {
                continue;
            }
        } else {
            runLength = 0;
        }
        kept.push_back(std::move(road));
        ++runLength;
    }
    return kept;
}

/// Accumulate cluster roads from the RAW `TopologyDocumentInput::neighbors`, before any
/// reciprocal / minEdgeScore construction filter. Every finite, non-negative neighbor score whose
/// endpoints sit in two different primary clusters adds to that undirected pair's weight, so a
/// reciprocal pair contributes from both sides and the weight is symmetric. Each cluster keeps
/// its `maxPerCluster` heaviest roads; ties break on the target cluster id. The result is sorted
/// by (from asc, weight desc, to asc); cluster ids are hash-anchored, so the output does not
/// depend on input order beyond floating-point summation order. `maxPerCluster == 0` returns no
/// roads.
inline std::vector<ClusterRoad>
accumulateClusterRoads(std::span<const TopologyDocumentInput> documents,
                       const std::unordered_map<std::string_view, std::string_view>& primaryByHash,
                       std::size_t maxPerCluster) {
    if (maxPerCluster == 0 || documents.empty() || primaryByHash.empty()) {
        return {};
    }
    ClusterPairWeights pairWeights;
    accumulateClusterPairWeights(documents, primaryByHash, nullptr, pairWeights);
    if (pairWeights.empty()) {
        return {};
    }
    std::vector<ClusterRoad> roads;
    appendClusterRoadOrientations(pairWeights, roads);
    return rankClusterRoads(std::move(roads), maxPerCluster);
}

/// Roads of an incrementally merged batch. A road between two clusters the update did not touch
/// is kept from `previousRoads`; every road with an endpoint in `retiredClusterIds` (the removed
/// clusters and the rebuilt ones, including reused ids) is dropped and the rebuilt clusters' roads
/// are recomputed from `regionDocuments`' raw neighbor lists against `merged`'s memberships,
/// mirroring out-of-region targets (see accumulateClusterPairWeights). Each source then re-ranks
/// its top-K over the union, so a kept road competes with the recomputed ones under the current
/// bound. An untouched source cannot recover a road its previous top-K dropped; that is what makes
/// the result derived rather than full.
inline std::vector<ClusterRoad>
deriveIncrementalRoads(std::span<const ClusterRoad> previousRoads,
                       const std::unordered_set<std::string>& retiredClusterIds,
                       std::span<const TopologyDocumentInput> regionDocuments,
                       const TopologyArtifactBatch& merged, std::size_t maxPerCluster) {
    if (maxPerCluster == 0) {
        return {};
    }
    std::unordered_set<std::string_view> finalClusterIds;
    finalClusterIds.reserve(merged.clusters.size());
    for (const auto& cluster : merged.clusters) {
        finalClusterIds.insert(cluster.clusterId);
    }

    std::vector<ClusterRoad> roads;
    roads.reserve(previousRoads.size());
    for (const auto& road : previousRoads) {
        if (retiredClusterIds.contains(road.fromClusterId) ||
            retiredClusterIds.contains(road.toClusterId) ||
            !finalClusterIds.contains(road.fromClusterId) ||
            !finalClusterIds.contains(road.toClusterId)) {
            continue;
        }
        roads.push_back(road);
    }

    std::unordered_set<std::string_view> observed;
    observed.reserve(regionDocuments.size());
    for (const auto& document : regionDocuments) {
        if (!document.documentHash.empty()) {
            observed.insert(document.documentHash);
        }
    }
    const auto primaryByHash = primaryClusterByHash(merged);
    ClusterPairWeights pairWeights;
    accumulateClusterPairWeights(regionDocuments, primaryByHash, &observed, pairWeights);
    appendClusterRoadOrientations(pairWeights, roads);
    return rankClusterRoads(std::move(roads), maxPerCluster);
}

} // namespace yams::topology::detail
