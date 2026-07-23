#include <yams/topology/topology_representatives.h>
#include <yams/vector/static_cosine_ann_index.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <unordered_map>

namespace yams::topology {

namespace {

double cosineDistance(std::span<const float> lhs, std::span<const float> rhs) {
    if (lhs.empty() || lhs.size() != rhs.size()) {
        return 2.0;
    }
    double dot = 0.0;
    double lhsNorm = 0.0;
    double rhsNorm = 0.0;
    for (std::size_t index = 0; index < lhs.size(); ++index) {
        dot += static_cast<double>(lhs[index]) * static_cast<double>(rhs[index]);
        lhsNorm += static_cast<double>(lhs[index]) * static_cast<double>(lhs[index]);
        rhsNorm += static_cast<double>(rhs[index]) * static_cast<double>(rhs[index]);
    }
    if (lhsNorm <= 0.0 || rhsNorm <= 0.0) {
        return 2.0;
    }
    return 1.0 - std::clamp(dot / (std::sqrt(lhsNorm) * std::sqrt(rhsNorm)), -1.0, 1.0);
}

} // namespace

std::vector<ClusterRoutingRepresentative> selectDiverseRoutingRepresentatives(
    std::span<const TopologyDocumentInput> documents, std::span<const std::size_t> members,
    std::span<const float> centroidEmbedding, std::size_t routingRepresentativeCount) {
    std::vector<ClusterRoutingRepresentative> selected;
    if (routingRepresentativeCount <= 1 || centroidEmbedding.empty()) {
        return selected;
    }

    std::vector<std::size_t> candidates;
    candidates.reserve(members.size());
    for (const auto member : members) {
        if (member >= documents.size() || documents[member].documentHash.empty() ||
            documents[member].embedding.size() != centroidEmbedding.size()) {
            continue;
        }
        const bool finite = std::ranges::all_of(documents[member].embedding,
                                                [](float value) { return std::isfinite(value); });
        if (finite) {
            candidates.push_back(member);
        }
    }
    std::ranges::sort(candidates, [&](std::size_t lhs, std::size_t rhs) {
        return documents[lhs].documentHash < documents[rhs].documentHash;
    });

    const auto extraLimit =
        std::min(routingRepresentativeCount - static_cast<std::size_t>(1), candidates.size());
    selected.reserve(extraLimit);
    std::vector<bool> used(candidates.size(), false);
    std::vector<double> minDistances(candidates.size(), std::numeric_limits<double>::max());

    for (std::size_t selection = 0; selection < extraLimit; ++selection) {
        std::size_t best = candidates.size();
        double bestDistance = -1.0;
        for (std::size_t candidate = 0; candidate < candidates.size(); ++candidate) {
            if (used[candidate]) {
                continue;
            }
            const auto& embedding = documents[candidates[candidate]].embedding;
            const auto distance = selection == 0
                                      ? cosineDistance(embedding, centroidEmbedding)
                                      : cosineDistance(embedding, selected.back().embedding);
            minDistances[candidate] = std::min(minDistances[candidate], distance);
            if (minDistances[candidate] > bestDistance) {
                bestDistance = minDistances[candidate];
                best = candidate;
            }
        }
        if (best == candidates.size()) {
            break;
        }
        used[best] = true;
        const auto document = candidates[best];
        selected.push_back(
            ClusterRoutingRepresentative{.documentHash = documents[document].documentHash,
                                         .embedding = documents[document].embedding});
    }
    return selected;
}

std::size_t applyOrthogonalBoundarySpill(std::span<const TopologyDocumentInput> documents,
                                         const TopologyBuildConfig& config,
                                         TopologyArtifactBatch& artifacts) {
    if (!config.allowOverlap || config.overlapLimit == 0 || artifacts.clusters.size() < 2 ||
        !std::isfinite(config.overlapBoundaryDistanceRatio) ||
        config.overlapBoundaryDistanceRatio < 1.0 ||
        !std::isfinite(config.overlapResidualPenalty) || config.overlapResidualPenalty < 0.0) {
        return 0;
    }

    std::unordered_map<std::string, const TopologyDocumentInput*> documentByHash;
    documentByHash.reserve(documents.size());
    for (const auto& document : documents) {
        if (!document.documentHash.empty() && !document.embedding.empty()) {
            documentByHash.emplace(document.documentHash, &document);
        }
    }
    std::unordered_map<std::string, std::size_t> clusterIndexById;
    clusterIndexById.reserve(artifacts.clusters.size());
    for (std::size_t index = 0; index < artifacts.clusters.size(); ++index) {
        if (!artifacts.clusters[index].clusterId.empty()) {
            clusterIndexById.emplace(artifacts.clusters[index].clusterId, index);
        }
    }

    constexpr double kResidualEpsilon = 1e-12;
    std::vector<double> observedRadiusSquared(artifacts.clusters.size(), 0.0);
    for (std::size_t clusterIndex = 0; clusterIndex < artifacts.clusters.size(); ++clusterIndex) {
        const auto& cluster = artifacts.clusters[clusterIndex];
        for (const auto& documentHash : cluster.memberDocumentHashes) {
            const auto documentIt = documentByHash.find(documentHash);
            if (documentIt == documentByHash.end() ||
                documentIt->second->embedding.size() != cluster.centroidEmbedding.size()) {
                continue;
            }
            double radiusSquared = 0.0;
            for (std::size_t dimension = 0; dimension < cluster.centroidEmbedding.size();
                 ++dimension) {
                const double residual =
                    static_cast<double>(documentIt->second->embedding[dimension]) -
                    cluster.centroidEmbedding[dimension];
                radiusSquared += residual * residual;
            }
            if (std::isfinite(radiusSquared)) {
                observedRadiusSquared[clusterIndex] =
                    std::max(observedRadiusSquared[clusterIndex], radiusSquared);
            }
        }
    }

    constexpr std::size_t kMinimumOverlapCandidateLimit = 32;
    const auto overlapCandidateLimit =
        std::max({kMinimumOverlapCandidateLimit, config.maxNeighborsPerDocument,
                  config.overlapLimit + static_cast<std::size_t>(1)});
    std::vector<std::size_t> indexedClusterIds;
    std::vector<std::vector<float>> indexedCentroids;
    indexedClusterIds.reserve(artifacts.clusters.size());
    indexedCentroids.reserve(artifacts.clusters.size());
    for (std::size_t clusterIndex = 0; clusterIndex < artifacts.clusters.size(); ++clusterIndex) {
        const auto& centroid = artifacts.clusters[clusterIndex].centroidEmbedding;
        if (observedRadiusSquared[clusterIndex] > kResidualEpsilon && !centroid.empty()) {
            indexedClusterIds.push_back(clusterIndex);
            indexedCentroids.push_back(centroid);
        }
    }
    std::shared_ptr<const yams::vector::StaticCosineAnnIndex> centroidAnnIndex;
    if (indexedClusterIds.size() > overlapCandidateLimit) {
        auto built = yams::vector::StaticCosineAnnIndex::build(indexedClusterIds, indexedCentroids);
        if (built) {
            centroidAnnIndex = std::move(built).value();
        }
    }

    struct Candidate {
        std::size_t clusterIndex{0};
        double loss{0.0};
    };
    std::size_t assignments = 0;
    const double boundaryRatioSquared =
        config.overlapBoundaryDistanceRatio * config.overlapBoundaryDistanceRatio;

    for (auto& membership : artifacts.memberships) {
        if (!membership.overlapClusterIds.empty()) {
            continue;
        }
        const auto documentIt = documentByHash.find(membership.documentHash);
        const auto primaryIt = clusterIndexById.find(membership.clusterId);
        if (documentIt == documentByHash.end() || primaryIt == clusterIndexById.end()) {
            continue;
        }
        const auto& embedding = documentIt->second->embedding;
        const auto& primaryCentroid = artifacts.clusters[primaryIt->second].centroidEmbedding;
        if (embedding.size() != primaryCentroid.size() || embedding.empty()) {
            continue;
        }

        std::vector<double> primaryResidual(embedding.size());
        double primaryNormSquared = 0.0;
        for (std::size_t dimension = 0; dimension < embedding.size(); ++dimension) {
            primaryResidual[dimension] =
                static_cast<double>(embedding[dimension]) - primaryCentroid[dimension];
            primaryNormSquared += primaryResidual[dimension] * primaryResidual[dimension];
        }
        if (!std::isfinite(primaryNormSquared)) {
            continue;
        }
        const bool primaryDirectionObserved = primaryNormSquared > kResidualEpsilon;
        if (!primaryDirectionObserved && membership.role != DocumentTopologyRole::Outlier) {
            continue;
        }

        std::vector<std::size_t> candidateClusterIndices;
        if (centroidAnnIndex && embedding.size() == centroidAnnIndex->dimension()) {
            auto nearest = centroidAnnIndex->search(embedding, overlapCandidateLimit);
            if (nearest) {
                candidateClusterIndices.reserve(nearest.value().hits.size());
                for (const auto& hit : nearest.value().hits) {
                    candidateClusterIndices.push_back(hit.id);
                }
            }
        }
        if (candidateClusterIndices.empty()) {
            candidateClusterIndices.reserve(artifacts.clusters.size());
            for (std::size_t clusterIndex = 0; clusterIndex < artifacts.clusters.size();
                 ++clusterIndex) {
                candidateClusterIndices.push_back(clusterIndex);
            }
        }

        std::vector<Candidate> candidates;
        candidates.reserve(candidateClusterIndices.size());
        for (const auto clusterIndex : candidateClusterIndices) {
            if (clusterIndex >= artifacts.clusters.size()) {
                continue;
            }
            if (clusterIndex == primaryIt->second) {
                continue;
            }
            const auto& centroid = artifacts.clusters[clusterIndex].centroidEmbedding;
            if (centroid.size() != embedding.size()) {
                continue;
            }
            double candidateNormSquared = 0.0;
            double residualDot = 0.0;
            for (std::size_t dimension = 0; dimension < embedding.size(); ++dimension) {
                const double residual =
                    static_cast<double>(embedding[dimension]) - centroid[dimension];
                candidateNormSquared += residual * residual;
                residualDot += primaryResidual[dimension] * residual;
            }
            if (!std::isfinite(candidateNormSquared)) {
                continue;
            }
            double loss = candidateNormSquared;
            if (primaryDirectionObserved) {
                if (candidateNormSquared > primaryNormSquared * boundaryRatioSquared) {
                    continue;
                }
                const double projectionSquared = (residualDot * residualDot) / primaryNormSquared;
                loss += config.overlapResidualPenalty * projectionSquared;
            } else {
                const double candidateRadiusSquared = observedRadiusSquared[clusterIndex];
                if (candidateRadiusSquared <= kResidualEpsilon ||
                    candidateNormSquared > candidateRadiusSquared * boundaryRatioSquared) {
                    continue;
                }
            }
            if (std::isfinite(loss)) {
                candidates.push_back(Candidate{.clusterIndex = clusterIndex, .loss = loss});
            }
        }
        std::ranges::sort(candidates, [&](const Candidate& lhs, const Candidate& rhs) {
            if (std::abs(lhs.loss - rhs.loss) > kResidualEpsilon) {
                return lhs.loss < rhs.loss;
            }
            return artifacts.clusters[lhs.clusterIndex].clusterId <
                   artifacts.clusters[rhs.clusterIndex].clusterId;
        });

        const auto limit = std::min(config.overlapLimit, candidates.size());
        membership.overlapClusterIds.reserve(limit);
        for (std::size_t candidateIndex = 0; candidateIndex < limit; ++candidateIndex) {
            auto& secondary = artifacts.clusters[candidates[candidateIndex].clusterIndex];
            if (std::ranges::find(secondary.memberDocumentHashes, membership.documentHash) ==
                secondary.memberDocumentHashes.end()) {
                secondary.memberDocumentHashes.push_back(membership.documentHash);
                std::ranges::sort(secondary.memberDocumentHashes);
                secondary.memberCount = secondary.memberDocumentHashes.size();
            }
            membership.overlapClusterIds.push_back(secondary.clusterId);
            ++assignments;
        }
    }
    return assignments;
}

} // namespace yams::topology
