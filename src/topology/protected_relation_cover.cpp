#include <yams/topology/protected_relation_cover.h>

#include <algorithm>
#include <iomanip>
#include <limits>
#include <map>
#include <ranges>
#include <sstream>
#include <unordered_set>

namespace yams::topology {

std::string protectedRelationConstructionIdentity(std::span<const TopologyDocumentInput> documents,
                                                  const TopologyBuildConfig& config) {
    std::unordered_set<std::string> documentHashes;
    documentHashes.reserve(documents.size());
    for (const auto& document : documents) {
        if (!document.documentHash.empty()) {
            documentHashes.insert(document.documentHash);
        }
    }

    std::map<std::pair<std::string, std::string>, float> observations;
    for (const auto& document : documents) {
        for (const auto& neighbor : document.neighbors) {
            if (document.documentHash.empty() || neighbor.documentHash.empty() ||
                document.documentHash == neighbor.documentHash ||
                !documentHashes.contains(neighbor.documentHash) ||
                (config.reciprocalOnly && !neighbor.reciprocal) ||
                neighbor.score < static_cast<float>(config.minEdgeScore)) {
                continue;
            }
            auto endpoints = std::minmax(document.documentHash, neighbor.documentHash);
            auto [it, inserted] =
                observations.emplace(std::pair{endpoints.first, endpoints.second}, neighbor.score);
            if (!inserted) {
                it->second = std::max(it->second, neighbor.score);
            }
        }
    }

    std::ostringstream descriptor;
    descriptor << std::setprecision(std::numeric_limits<float>::max_digits10)
               << "relation=semantic_neighbor;provenance=topology_input;version=1;"
                  "split=construction";
    for (const auto& [endpoints, score] : observations) {
        descriptor << ";lhs=" << endpoints.first.size() << ':' << endpoints.first
                   << ";rhs=" << endpoints.second.size() << ':' << endpoints.second
                   << ";score=" << score;
    }

    constexpr std::uint64_t kFnvOffset = 14695981039346656037ULL;
    constexpr std::uint64_t kFnvPrime = 1099511628211ULL;
    std::uint64_t fingerprint = kFnvOffset;
    for (const auto byte : descriptor.str()) {
        fingerprint ^= static_cast<unsigned char>(byte);
        fingerprint *= kFnvPrime;
    }
    std::ostringstream out;
    out << "semantic_neighbor:v1:construction:fnv1a64:" << std::hex << std::setw(16)
        << std::setfill('0') << fingerprint;
    return out.str();
}

Result<ProtectedRelationCoverIndex>
buildProtectedRelationCoverIndex(const TopologyArtifactBatch& artifacts) {
    ProtectedRelationCoverIndex index;
    index.fibers.reserve(artifacts.clusters.size());
    index.fiberById.reserve(artifacts.clusters.size());
    index.fiberIndicesByDocumentHash.reserve(artifacts.memberships.size());

    std::vector<const ClusterArtifact*> clusters;
    clusters.reserve(artifacts.clusters.size());
    for (const auto& cluster : artifacts.clusters) {
        clusters.push_back(&cluster);
    }
    std::ranges::sort(clusters, {}, &ClusterArtifact::clusterId);

    for (const auto* cluster : clusters) {
        if (cluster->clusterId.empty()) {
            return Error{ErrorCode::InvalidData, "protected relation fiber has no id"};
        }
        if (index.fiberById.contains(cluster->clusterId)) {
            return Error{ErrorCode::InvalidData, "protected relation cover has duplicate fiber id"};
        }

        auto documentHashes = cluster->memberDocumentHashes;
        std::ranges::sort(documentHashes);
        const auto uniqueEnd = std::ranges::unique(documentHashes).begin();
        if (uniqueEnd != documentHashes.end()) {
            return Error{ErrorCode::InvalidData,
                         "protected relation fiber has duplicate document membership"};
        }
        if (documentHashes.empty()) {
            return Error{ErrorCode::InvalidData, "protected relation fiber is empty"};
        }

        const auto fiberIndex = index.fibers.size();
        index.fiberById.emplace(cluster->clusterId, fiberIndex);
        for (const auto& documentHash : documentHashes) {
            if (documentHash.empty()) {
                return Error{ErrorCode::InvalidData,
                             "protected relation fiber has an empty document"};
            }
            index.fiberIndicesByDocumentHash[documentHash].push_back(fiberIndex);
        }
        index.fibers.push_back(ProtectedRelationFiber{
            .fiberId = cluster->clusterId,
            .documentHashes = std::move(documentHashes),
        });
    }

    for (const auto& membership : artifacts.memberships) {
        if (membership.documentHash.empty() ||
            !index.fiberIndicesByDocumentHash.contains(membership.documentHash)) {
            return Error{ErrorCode::InvalidData,
                         "protected relation membership is absent from the cover"};
        }
    }
    return index;
}

} // namespace yams::topology
