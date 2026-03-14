// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <yams/search/search_tracing.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace yams::search {

std::string deriveDocIdFromPath(const std::string& path) {
    if (path.empty()) {
        return {};
    }

    const auto slashPos = path.find_last_of("/\\");
    std::string fileName =
        (slashPos == std::string::npos) ? path : path.substr(static_cast<size_t>(slashPos + 1));

    constexpr std::string_view kTxtSuffix = ".txt";
    if (fileName.size() > kTxtSuffix.size() &&
        fileName.compare(fileName.size() - kTxtSuffix.size(), kTxtSuffix.size(), kTxtSuffix) == 0) {
        fileName.resize(fileName.size() - kTxtSuffix.size());
    }
    return fileName;
}

std::string documentIdForTrace(const std::string& filePath, const std::string& documentHash) {
    std::string docId = deriveDocIdFromPath(filePath);
    if (!docId.empty()) {
        return docId;
    }
    return documentHash;
}

std::vector<std::string>
collectUniqueComponentDocIds(const std::vector<ComponentResult>& componentResults) {
    std::unordered_set<std::string> uniqueIds;
    uniqueIds.reserve(componentResults.size());

    for (const auto& comp : componentResults) {
        const std::string docId = documentIdForTrace(comp.filePath, comp.documentHash);
        if (!docId.empty()) {
            uniqueIds.insert(docId);
        }
    }

    std::vector<std::string> out;
    out.reserve(uniqueIds.size());
    for (const auto& id : uniqueIds) {
        out.push_back(id);
    }
    std::sort(out.begin(), out.end());
    return out;
}

std::vector<std::string> collectRankedResultDocIds(const std::vector<SearchResult>& results,
                                                   size_t maxCount) {
    const size_t count = std::min(results.size(), maxCount);
    std::vector<std::string> out;
    out.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        out.push_back(
            documentIdForTrace(results[i].document.filePath, results[i].document.sha256Hash));
    }
    return out;
}

std::vector<std::string> setDifferenceIds(const std::vector<std::string>& lhs,
                                          const std::vector<std::string>& rhs) {
    std::unordered_set<std::string> rhsSet;
    rhsSet.reserve(rhs.size());
    for (const auto& id : rhs) {
        rhsSet.insert(id);
    }

    std::vector<std::string> out;
    out.reserve(lhs.size());
    for (const auto& id : lhs) {
        if (rhsSet.find(id) == rhsSet.end()) {
            out.push_back(id);
        }
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

std::string joinWithTab(const std::vector<std::string>& values) {
    if (values.empty()) {
        return {};
    }
    std::string out;
    size_t totalChars = 0;
    for (const auto& value : values) {
        totalChars += value.size();
    }
    out.reserve(totalChars + values.size());

    bool first = true;
    for (const auto& value : values) {
        if (!first) {
            out.push_back('\t');
        }
        out += value;
        first = false;
    }
    return out;
}

nlohmann::json buildComponentHitSummaryJson(const std::vector<ComponentResult>& componentResults,
                                            size_t topPerComponent) {
    std::unordered_map<ComponentResult::Source, std::vector<std::pair<size_t, std::string>>>
        grouped;
    grouped.reserve(8);

    for (const auto& comp : componentResults) {
        const std::string docId = documentIdForTrace(comp.filePath, comp.documentHash);
        if (docId.empty()) {
            continue;
        }
        grouped[comp.source].emplace_back(comp.rank, docId);
    }

    nlohmann::json out = nlohmann::json::object();
    for (auto& [source, docs] : grouped) {
        std::sort(docs.begin(), docs.end(), [](const auto& a, const auto& b) {
            if (a.first != b.first) {
                return a.first < b.first;
            }
            return a.second < b.second;
        });

        std::vector<std::string> topDocIds;
        topDocIds.reserve(std::min(topPerComponent, docs.size()));
        std::unordered_set<std::string> seen;
        seen.reserve(docs.size());
        for (const auto& [rank, docId] : docs) {
            (void)rank;
            if (!seen.insert(docId).second) {
                continue;
            }
            topDocIds.push_back(docId);
            if (topDocIds.size() >= topPerComponent) {
                break;
            }
        }

        out[componentSourceToString(source)] = {
            {"raw_hits", docs.size()},
            {"unique_top_doc_ids", topDocIds},
        };
    }

    return out;
}

nlohmann::json buildFusionTopSummaryJson(const std::vector<SearchResult>& results, size_t maxDocs) {
    nlohmann::json out = nlohmann::json::array();
    const size_t count = std::min(results.size(), maxDocs);
    for (size_t i = 0; i < count; ++i) {
        const auto& res = results[i];
        out.push_back({
            {"rank", i + 1},
            {"doc_id", documentIdForTrace(res.document.filePath, res.document.sha256Hash)},
            {"path", res.document.filePath},
            {"score", res.score},
            {"keyword_score", res.keywordScore.value_or(0.0)},
            {"vector_score", res.vectorScore.value_or(0.0)},
            {"kg_score", res.kgScore.value_or(0.0)},
            {"path_score", res.pathScore.value_or(0.0)},
            {"tag_score", res.tagScore.value_or(0.0)},
            {"symbol_score", res.symbolScore.value_or(0.0)},
            {"reranker_score", res.rerankerScore.value_or(0.0)},
        });
    }
    return out;
}

std::optional<std::int64_t>
resolveKgDocumentId(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                    const metadata::DocumentInfo& doc) {
    if (!kgStore) {
        return std::nullopt;
    }
    if (!doc.sha256Hash.empty()) {
        auto byHash = kgStore->getDocumentIdByHash(doc.sha256Hash);
        if (byHash && byHash.value().has_value()) {
            return byHash.value();
        }
    }
    if (!doc.filePath.empty()) {
        auto byPath = kgStore->getDocumentIdByPath(doc.filePath);
        if (byPath && byPath.value().has_value()) {
            return byPath.value();
        }
    }
    return std::nullopt;
}

nlohmann::json buildGraphDocProbeJson(const std::shared_ptr<metadata::KnowledgeGraphStore>& kgStore,
                                      const std::vector<SearchResult>& results, size_t limit) {
    nlohmann::json out = nlohmann::json::array();
    if (!kgStore) {
        return out;
    }
    const size_t maxDocs = std::min(limit, results.size());
    for (size_t i = 0; i < maxDocs; ++i) {
        const auto& result = results[i];
        nlohmann::json item = {
            {"doc_id", documentIdForTrace(result.document.filePath, result.document.sha256Hash)},
            {"rank", i + 1},
            {"score", result.score},
            {"keyword_score", result.keywordScore.value_or(0.0)},
            {"vector_score", result.vectorScore.value_or(0.0)},
            {"kg_score", result.kgScore.value_or(0.0)},
        };

        auto docId = resolveKgDocumentId(kgStore, result.document);
        if (!docId.has_value()) {
            item["resolved_document_id"] = nullptr;
            item["doc_entity_count"] = 0;
            item["linked_node_count"] = 0;
            item["entity_text_samples"] = nlohmann::json::array();
            item["node_label_samples"] = nlohmann::json::array();
            out.push_back(std::move(item));
            continue;
        }

        item["resolved_document_id"] = *docId;
        auto ents = kgStore->getDocEntitiesForDocument(*docId, 2000, 0);
        if (!ents) {
            item["doc_entity_count"] = -1;
            item["linked_node_count"] = -1;
            item["entity_lookup_error"] = ents.error().message;
            out.push_back(std::move(item));
            continue;
        }

        std::unordered_set<std::int64_t> uniqueNodeIds;
        nlohmann::json entitySamples = nlohmann::json::array();
        for (const auto& ent : ents.value()) {
            if (ent.nodeId.has_value()) {
                uniqueNodeIds.insert(*ent.nodeId);
            }
            if (entitySamples.size() < 5) {
                entitySamples.push_back({
                    {"text", ent.entityText},
                    {"node_id", ent.nodeId.has_value() ? nlohmann::json(*ent.nodeId)
                                                       : nlohmann::json(nullptr)},
                    {"extractor", ent.extractor.value_or("")},
                    {"confidence", ent.confidence.has_value() ? nlohmann::json(*ent.confidence)
                                                              : nlohmann::json(nullptr)},
                });
            }
        }

        nlohmann::json nodeLabelSamples = nlohmann::json::array();
        for (auto nodeIdVal : uniqueNodeIds) {
            if (nodeLabelSamples.size() >= 5) {
                break;
            }
            auto nodeRes = kgStore->getNodeById(nodeIdVal);
            if (!nodeRes || !nodeRes.value().has_value()) {
                continue;
            }
            const auto& node = *nodeRes.value();
            nodeLabelSamples.push_back({
                {"node_id", nodeIdVal},
                {"label", node.label.value_or("")},
                {"type", node.type.value_or("")},
                {"node_key", node.nodeKey},
            });
        }

        item["doc_entity_count"] = ents.value().size();
        item["linked_node_count"] = uniqueNodeIds.size();
        item["entity_text_samples"] = std::move(entitySamples);
        item["node_label_samples"] = std::move(nodeLabelSamples);
        out.push_back(std::move(item));
    }
    return out;
}

} // namespace yams::search
