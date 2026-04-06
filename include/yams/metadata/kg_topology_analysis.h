#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace yams::metadata {

class KnowledgeGraphStore;

struct KGTopologySummary {
    std::size_t documentNodeCount{0};
    std::size_t semanticEdgeCount{0};
    std::size_t reciprocalSemanticEdgeCount{0};
    std::size_t unreciprocatedSemanticEdgeCount{0};
    std::size_t documentsWithSemanticNeighbors{0};
    std::size_t documentsWithReciprocalNeighbors{0};
    std::size_t reciprocalCommunityCount{0};
    std::size_t reciprocalSingletonDocumentCount{0};
    std::size_t largestReciprocalCommunitySize{0};
    std::size_t isolatedDocumentCount{0};
    std::size_t semanticSingletonCount{0};
    std::size_t connectedComponentCount{0};
    std::size_t largestComponentSize{0};
    double avgSemanticDegree{0.0};
    double semanticCoverage{0.0};
    double semanticReciprocity{0.0};
    std::vector<std::size_t> componentSizes;
    std::vector<std::size_t> reciprocalCommunitySizes;
};

std::optional<KGTopologySummary> analyzeDocumentTopology(KnowledgeGraphStore* kgStore,
                                                         std::size_t maxNeighborsPerNode = 64);

} // namespace yams::metadata
