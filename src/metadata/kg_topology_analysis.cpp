#include <yams/metadata/kg_topology_analysis.h>

#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <deque>
#include <unordered_map>
#include <unordered_set>

namespace yams::metadata {

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
    std::vector<std::size_t> semanticDegree(docs.size(), 0);

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

    summary.semanticEdgeCount /= 2;
    return summary;
}

} // namespace yams::metadata
