#include <yams/metadata/kg_relation_summary.h>

#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::metadata {

namespace {

void appendFileNodeKeys(std::string_view path, std::vector<std::string>& nodeKeys) {
    if (path.empty()) {
        return;
    }

    constexpr std::string_view kPathFilePrefix = "path:file:";
    constexpr std::string_view kLegacyFilePrefix = "file:";

    if (path.rfind(kPathFilePrefix, 0) == 0) {
        nodeKeys.emplace_back(path);
        auto rawPath = path.substr(kPathFilePrefix.size());
        if (!rawPath.empty()) {
            nodeKeys.emplace_back(std::string(kLegacyFilePrefix) + std::string(rawPath));
        }
        return;
    }

    if (path.rfind(kLegacyFilePrefix, 0) == 0) {
        auto rawPath = path.substr(kLegacyFilePrefix.size());
        if (!rawPath.empty()) {
            nodeKeys.emplace_back(std::string(kPathFilePrefix) + std::string(rawPath));
        }
        nodeKeys.emplace_back(path);
        return;
    }

    nodeKeys.emplace_back(std::string(kPathFilePrefix) + std::string(path));
    nodeKeys.emplace_back(std::string(kLegacyFilePrefix) + std::string(path));
}

void appendHashNodeKeys(std::string_view hash, std::vector<std::string>& nodeKeys) {
    if (hash.empty()) {
        return;
    }

    constexpr std::string_view kDocPrefix = "doc:";
    constexpr std::string_view kBlobPrefix = "blob:";

    if (hash.rfind(kDocPrefix, 0) == 0) {
        nodeKeys.emplace_back(hash);
        auto rawHash = hash.substr(kDocPrefix.size());
        if (!rawHash.empty()) {
            nodeKeys.emplace_back(std::string(kBlobPrefix) + std::string(rawHash));
        }
        return;
    }

    if (hash.rfind(kBlobPrefix, 0) == 0) {
        nodeKeys.emplace_back(hash);
        auto rawHash = hash.substr(kBlobPrefix.size());
        if (!rawHash.empty()) {
            nodeKeys.emplace_back(std::string(kDocPrefix) + std::string(rawHash));
        }
        return;
    }

    nodeKeys.emplace_back(std::string(kDocPrefix) + std::string(hash));
    nodeKeys.emplace_back(std::string(kBlobPrefix) + std::string(hash));
}

} // namespace

std::string normalizeRelationName(std::string relation) {
    auto trimLeft = std::find_if_not(relation.begin(), relation.end(), [](unsigned char c) {
        return static_cast<bool>(std::isspace(c));
    });
    auto trimRight = std::find_if_not(relation.rbegin(), relation.rend(), [](unsigned char c) {
        return static_cast<bool>(std::isspace(c));
    });

    if (trimLeft >= trimRight.base()) {
        return {};
    }

    relation = std::string(trimLeft, trimRight.base());
    std::transform(relation.begin(), relation.end(), relation.begin(), [](unsigned char c) {
        if (c == '-' || std::isspace(c)) {
            return '_';
        }
        return static_cast<char>(std::tolower(c));
    });
    return relation;
}

std::optional<KGRelationSummary> collectFileRelationSummary(KnowledgeGraphStore* kgStore,
                                                            std::string_view path,
                                                            std::optional<std::string_view> hash,
                                                            std::size_t edgeLimit,
                                                            std::size_t topLimit) {
    if (!kgStore) {
        return std::nullopt;
    }

    std::vector<std::string> nodeKeys;
    nodeKeys.reserve(4);
    appendFileNodeKeys(path, nodeKeys);
    if (hash.has_value()) {
        appendHashNodeKeys(*hash, nodeKeys);
    }

    if (nodeKeys.empty()) {
        return std::nullopt;
    }

    std::vector<std::int64_t> nodeIds;
    nodeIds.reserve(nodeKeys.size());
    std::unordered_set<std::int64_t> seenNodeIds;

    for (const auto& nodeKey : nodeKeys) {
        auto nodeResult = kgStore->getNodeByKey(nodeKey);
        if (!nodeResult || !nodeResult.value().has_value()) {
            continue;
        }

        const auto nodeId = nodeResult.value()->id;
        if (seenNodeIds.insert(nodeId).second) {
            nodeIds.push_back(nodeId);
        }
    }

    if (nodeIds.empty()) {
        return std::nullopt;
    }

    std::unordered_set<std::int64_t> seenEdgeIds;
    std::unordered_set<std::string> seenSyntheticEdges;
    std::unordered_map<std::string, std::size_t> relationCounts;
    std::size_t totalEdges = 0;

    for (const auto nodeId : nodeIds) {
        auto edgesResult = kgStore->getEdgesBidirectional(nodeId, std::nullopt, edgeLimit);
        if (!edgesResult) {
            continue;
        }

        for (const auto& edge : edgesResult.value()) {
            if (edge.id > 0) {
                if (!seenEdgeIds.insert(edge.id).second) {
                    continue;
                }
            } else {
                std::string synthetic = std::to_string(edge.srcNodeId) + ":" +
                                        std::to_string(edge.dstNodeId) + ":" + edge.relation;
                if (!seenSyntheticEdges.insert(std::move(synthetic)).second) {
                    continue;
                }
            }

            const auto normalized = normalizeRelationName(edge.relation);
            if (normalized.empty()) {
                continue;
            }

            relationCounts[normalized] += 1;
            totalEdges += 1;
        }
    }

    if (totalEdges == 0 || relationCounts.empty()) {
        return std::nullopt;
    }

    std::vector<std::pair<std::string, std::size_t>> sorted(relationCounts.begin(),
                                                            relationCounts.end());
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) {
            return a.second > b.second;
        }
        return a.first < b.first;
    });

    if (sorted.size() > topLimit) {
        sorted.resize(topLimit);
    }

    KGRelationSummary summary;
    summary.totalEdges = totalEdges;
    summary.topRelations = std::move(sorted);
    return summary;
}

std::string formatRelationSummary(const std::vector<std::pair<std::string, std::size_t>>& pairs,
                                  bool humanReadable) {
    std::string out;
    for (std::size_t i = 0; i < pairs.size(); ++i) {
        if (i > 0) {
            out += humanReadable ? ", " : ",";
        }
        out += pairs[i].first;
        if (humanReadable) {
            out += "(" + std::to_string(pairs[i].second) + ")";
        } else {
            out += ":" + std::to_string(pairs[i].second);
        }
    }
    return out;
}

} // namespace yams::metadata
