#include <yams/metadata/kg_topology_analysis.h>

#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::metadata {

namespace {

std::uint64_t makeUndirectedPairKey(std::size_t a, std::size_t b) {
    const auto [lo, hi] = std::minmax(a, b);
    return (static_cast<std::uint64_t>(lo) << 32U) | static_cast<std::uint64_t>(hi);
}

std::uint64_t makeDirectedPairKey(std::size_t src, std::size_t dst) {
    return (static_cast<std::uint64_t>(src) << 32U) | static_cast<std::uint64_t>(dst);
}

class DisjointSet {
public:
    explicit DisjointSet(std::size_t size) : parent_(size), size_(size, 1) {
        std::iota(parent_.begin(), parent_.end(), std::size_t{0});
    }

    std::size_t find(std::size_t value) {
        auto root = value;
        while (parent_[root] != root) {
            root = parent_[root];
        }
        while (parent_[value] != value) {
            const auto next = parent_[value];
            parent_[value] = root;
            value = next;
        }
        return root;
    }

    void unite(std::size_t lhs, std::size_t rhs) {
        auto lhsRoot = find(lhs);
        auto rhsRoot = find(rhs);
        if (lhsRoot == rhsRoot) {
            return;
        }
        if (size_[lhsRoot] < size_[rhsRoot]) {
            std::swap(lhsRoot, rhsRoot);
        }
        parent_[rhsRoot] = lhsRoot;
        size_[lhsRoot] += size_[rhsRoot];
    }

private:
    std::vector<std::size_t> parent_;
    std::vector<std::size_t> size_;
};

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

    std::vector<std::size_t> semanticDegree(docs.size(), 0);
    std::unordered_set<std::uint64_t> directedPairs;
    std::unordered_set<std::uint64_t> semanticPairs;
    std::unordered_set<std::uint64_t> reciprocalPairs;
    std::unordered_set<std::uint64_t> reciprocalDocs;
    const auto reserveHint =
        docs.size() * std::min<std::size_t>(std::max<std::size_t>(1, maxNeighborsPerNode), 16);
    directedPairs.reserve(reserveHint);
    semanticPairs.reserve(reserveHint);

    auto streamResult = kgStore->forEachEdgeByRelation(
        std::string_view("semantic_neighbor"),
        [&](std::int64_t, std::int64_t srcNodeId, std::int64_t dstNodeId, float) {
            const auto srcIt = nodeIndex.find(srcNodeId);
            const auto dstIt = nodeIndex.find(dstNodeId);
            if (srcIt == nodeIndex.end() || dstIt == nodeIndex.end()) {
                return true;
            }
            const auto src = srcIt->second;
            const auto dst = dstIt->second;
            if (src == dst) {
                return true;
            }
            directedPairs.insert(makeDirectedPairKey(src, dst));
            semanticPairs.insert(makeUndirectedPairKey(src, dst));
            return true;
        });
    if (!streamResult) {
        return std::nullopt;
    }

    DisjointSet semanticComponents(docs.size());
    for (const auto pair : semanticPairs) {
        const auto src = static_cast<std::size_t>(pair >> 32U);
        const auto dst = static_cast<std::size_t>(pair & 0xffffffffU);
        ++semanticDegree[src];
        ++semanticDegree[dst];
        semanticComponents.unite(src, dst);
    }

    for (const auto degree : semanticDegree) {
        if (degree > 0) {
            ++summary.documentsWithSemanticNeighbors;
        }
    }
    summary.semanticEdgeCount = semanticPairs.size();

    DisjointSet reciprocalComponents(docs.size());
    for (const auto pair : directedPairs) {
        const auto src = static_cast<std::size_t>(pair >> 32U);
        const auto dst = static_cast<std::size_t>(pair & 0xffffffffU);
        if (directedPairs.contains(makeDirectedPairKey(dst, src))) {
            const auto [_, inserted] = reciprocalPairs.insert(makeUndirectedPairKey(src, dst));
            if (inserted) {
                reciprocalDocs.insert(src);
                reciprocalDocs.insert(dst);
                reciprocalComponents.unite(src, dst);
            }
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

    std::unordered_map<std::size_t, std::size_t> componentSizesByRoot;
    componentSizesByRoot.reserve(docs.size());
    for (std::size_t i = 0; i < docs.size(); ++i) {
        if (semanticDegree[i] == 0) {
            summary.isolatedDocumentCount++;
            summary.semanticSingletonCount++;
        }
        ++componentSizesByRoot[semanticComponents.find(i)];
    }
    summary.connectedComponentCount = componentSizesByRoot.size();
    summary.componentSizes.reserve(componentSizesByRoot.size());
    for (const auto& [_, size] : componentSizesByRoot) {
        summary.componentSizes.push_back(size);
        summary.largestComponentSize = std::max(summary.largestComponentSize, size);
    }

    if (!summary.componentSizes.empty()) {
        std::sort(summary.componentSizes.begin(), summary.componentSizes.end(), std::greater<>());
    }

    std::unordered_map<std::size_t, std::size_t> reciprocalSizesByRoot;
    reciprocalSizesByRoot.reserve(reciprocalDocs.size());
    for (const auto doc : reciprocalDocs) {
        ++reciprocalSizesByRoot[reciprocalComponents.find(doc)];
    }
    summary.reciprocalCommunitySizes.reserve(reciprocalSizesByRoot.size());
    for (const auto& [_, size] : reciprocalSizesByRoot) {
        if (size < 2) {
            continue;
        }
        ++summary.reciprocalCommunityCount;
        summary.reciprocalCommunitySizes.push_back(size);
        summary.largestReciprocalCommunitySize =
            std::max(summary.largestReciprocalCommunitySize, size);
    }

    if (!summary.reciprocalCommunitySizes.empty()) {
        std::sort(summary.reciprocalCommunitySizes.begin(), summary.reciprocalCommunitySizes.end(),
                  std::greater<>());
    }

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
