#include <yams/metadata/kg_topology_analysis.h>

#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace yams::metadata {

namespace {

std::uint64_t makeUndirectedPairKey(std::size_t a, std::size_t b) {
    const auto [lo, hi] = std::minmax(a, b);
    return (static_cast<std::uint64_t>(lo) << 32U) | static_cast<std::uint64_t>(hi);
}

std::uint64_t makeDirectedPairKey(std::size_t src, std::size_t dst) {
    return (static_cast<std::uint64_t>(src) << 32U) | static_cast<std::uint64_t>(dst);
}

} // namespace

std::optional<KGTopologySummary> analyzeDocumentTopology(KnowledgeGraphStore* kgStore,
                                                         std::size_t maxNeighborsPerNode) {
    if (!kgStore) {
        return std::nullopt;
    }

    auto docsResult = kgStore->findNodesByType("document", 1'000'000, 0);
    if (!docsResult) {
        return std::nullopt;
    }

    const auto& docs = docsResult.value();
    KGTopologySummary summary;
    summary.documentNodeCount = docs.size();
    if (docs.empty()) {
        return summary;
    }

    std::unordered_map<std::int64_t, std::size_t> nodeIndex;
    nodeIndex.reserve(docs.size());
    for (std::size_t i = 0; i < docs.size(); ++i) {
        nodeIndex.emplace(docs[i].id, i);
    }

    std::vector<std::vector<std::size_t>> adjacency(docs.size());
    std::vector<std::vector<std::size_t>> reciprocalAdjacency(docs.size());
    std::vector<std::size_t> semanticDegree(docs.size(), 0);
    std::unordered_set<std::uint64_t> directedPairs;
    std::unordered_set<std::uint64_t> reciprocalPairs;
    std::unordered_set<std::uint64_t> reciprocalDocs;

    for (std::size_t i = 0; i < docs.size(); ++i) {
        auto edgesResult =
            kgStore->getEdgesBidirectional(docs[i].id, std::string_view("semantic_neighbor"),
                                           std::max<std::size_t>(1, maxNeighborsPerNode * 2));
        if (!edgesResult) {
            continue;
        }

        std::unordered_set<std::size_t> uniqueNeighbors;
        for (const auto& edge : edgesResult.value()) {
            const auto otherNodeId = edge.srcNodeId == docs[i].id ? edge.dstNodeId : edge.srcNodeId;
            auto it = nodeIndex.find(otherNodeId);
            if (it == nodeIndex.end()) {
                continue;
            }
            if (it->second == i) {
                continue;
            }

            if (edge.srcNodeId == docs[i].id) {
                directedPairs.insert(makeDirectedPairKey(i, it->second));
            }
            uniqueNeighbors.insert(it->second);
        }

        semanticDegree[i] = uniqueNeighbors.size();
        if (!uniqueNeighbors.empty()) {
            summary.documentsWithSemanticNeighbors++;
        }
        summary.semanticEdgeCount += uniqueNeighbors.size();

        adjacency[i].reserve(uniqueNeighbors.size());
        for (const auto neighborIndex : uniqueNeighbors) {
            adjacency[i].push_back(neighborIndex);
        }
    }

    for (const auto pair : directedPairs) {
        const auto src = static_cast<std::size_t>(pair >> 32U);
        const auto dst = static_cast<std::size_t>(pair & 0xffffffffU);
        if (directedPairs.contains(makeDirectedPairKey(dst, src))) {
            reciprocalPairs.insert(makeUndirectedPairKey(src, dst));
            reciprocalDocs.insert(src);
            reciprocalDocs.insert(dst);
            reciprocalAdjacency[src].push_back(dst);
            reciprocalAdjacency[dst].push_back(src);
        }
    }

    summary.documentsWithReciprocalNeighbors = reciprocalDocs.size();
    summary.reciprocalSemanticEdgeCount = reciprocalPairs.size();
    summary.reciprocalSingletonDocumentCount =
        summary.documentNodeCount > summary.documentsWithReciprocalNeighbors
            ? summary.documentNodeCount - summary.documentsWithReciprocalNeighbors
            : 0;

    if (summary.documentNodeCount > 0) {
        summary.avgSemanticDegree = static_cast<double>(summary.semanticEdgeCount) /
                                    static_cast<double>(summary.documentNodeCount);
        summary.semanticCoverage = static_cast<double>(summary.documentsWithSemanticNeighbors) /
                                   static_cast<double>(summary.documentNodeCount);
    }

    std::vector<bool> visited(docs.size(), false);
    for (std::size_t i = 0; i < docs.size(); ++i) {
        if (visited[i]) {
            continue;
        }

        if (adjacency[i].empty()) {
            visited[i] = true;
            summary.isolatedDocumentCount++;
            summary.semanticSingletonCount++;
            summary.connectedComponentCount++;
            summary.componentSizes.push_back(1);
            summary.largestComponentSize = std::max<std::size_t>(summary.largestComponentSize, 1);
            continue;
        }

        std::deque<std::size_t> queue;
        queue.push_back(i);
        visited[i] = true;
        std::size_t componentSize = 0;

        while (!queue.empty()) {
            const auto cur = queue.front();
            queue.pop_front();
            ++componentSize;

            for (const auto neighbor : adjacency[cur]) {
                if (visited[neighbor]) {
                    continue;
                }
                visited[neighbor] = true;
                queue.push_back(neighbor);
            }
        }

        summary.connectedComponentCount++;
        summary.componentSizes.push_back(componentSize);
        summary.largestComponentSize = std::max(summary.largestComponentSize, componentSize);
    }

    if (!summary.componentSizes.empty()) {
        std::sort(summary.componentSizes.begin(), summary.componentSizes.end(), std::greater<>());
    }

    std::vector<bool> reciprocalVisited(docs.size(), false);
    for (std::size_t i = 0; i < docs.size(); ++i) {
        if (reciprocalVisited[i] || reciprocalAdjacency[i].empty()) {
            continue;
        }

        std::deque<std::size_t> queue;
        queue.push_back(i);
        reciprocalVisited[i] = true;
        std::size_t communitySize = 0;

        while (!queue.empty()) {
            const auto cur = queue.front();
            queue.pop_front();
            ++communitySize;

            for (const auto neighbor : reciprocalAdjacency[cur]) {
                if (reciprocalVisited[neighbor]) {
                    continue;
                }
                reciprocalVisited[neighbor] = true;
                queue.push_back(neighbor);
            }
        }

        if (communitySize >= 2) {
            ++summary.reciprocalCommunityCount;
            summary.reciprocalCommunitySizes.push_back(communitySize);
            summary.largestReciprocalCommunitySize =
                std::max(summary.largestReciprocalCommunitySize, communitySize);
        }
    }

    if (!summary.reciprocalCommunitySizes.empty()) {
        std::sort(summary.reciprocalCommunitySizes.begin(), summary.reciprocalCommunitySizes.end(),
                  std::greater<>());
    }

    summary.semanticEdgeCount /= 2;
    summary.unreciprocatedSemanticEdgeCount =
        summary.semanticEdgeCount > summary.reciprocalSemanticEdgeCount
            ? summary.semanticEdgeCount - summary.reciprocalSemanticEdgeCount
            : 0;
    if (summary.semanticEdgeCount > 0) {
        summary.semanticReciprocity = static_cast<double>(summary.reciprocalSemanticEdgeCount) /
                                      static_cast<double>(summary.semanticEdgeCount);
    }
    return summary;
}

} // namespace yams::metadata
