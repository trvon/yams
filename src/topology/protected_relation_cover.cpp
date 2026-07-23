#include <yams/topology/protected_relation_cover.h>

#include <algorithm>
#include <ranges>
#include <unordered_set>

namespace yams::topology {

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
