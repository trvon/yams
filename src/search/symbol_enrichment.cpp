#include <yams/search/symbol_enrichment.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <unordered_set>

namespace yams::search {

bool SymbolEnricher::enrichResult(SearchResultItem& result, const std::string& query_text) {
    if (!kg_store_) {
        return false;
    }

    try {
        std::optional<std::int64_t> docId;

        auto hashIt = result.metadata.find("sha256_hash");
        if (hashIt != result.metadata.end()) {
            auto docIdRes = kg_store_->getDocumentIdByHash(hashIt->second);
            if (docIdRes.has_value()) {
                docId = docIdRes.value();
            }
        }

        if (!docId && !result.path.empty()) {
            auto docIdRes = kg_store_->getDocumentIdByPath(result.path);
            if (docIdRes.has_value()) {
                docId = docIdRes.value();
            }
        }

        if (!docId) {
            return false;
        }

        auto entitiesRes = kg_store_->getDocEntitiesForDocument(docId.value(), 50, 0);
        if (!entitiesRes.has_value() || entitiesRes.value().empty()) {
            return false;
        }

        std::vector<std::int64_t> nodeIds;
        std::unordered_set<std::int64_t> seenNodes;
        nodeIds.reserve(entitiesRes.value().size());

        for (const auto& entity : entitiesRes.value()) {
            if (entity.nodeId && !seenNodes.count(entity.nodeId.value())) {
                seenNodes.insert(entity.nodeId.value());
                nodeIds.push_back(entity.nodeId.value());
            }
        }

        if (nodeIds.empty()) {
            return false;
        }

        auto symbolInfoMap = extractSymbolInfoBatch(nodeIds);
        if (symbolInfoMap.empty()) {
            return false;
        }

        SymbolContext ctx;
        ctx.symbols.reserve(symbolInfoMap.size());

        float maxRelevance = 0.0f;
        bool hasDefinition = false;
        float bestDefinitionScore = 1.0f;

        std::string lowerQuery;
        if (!query_text.empty()) {
            lowerQuery = query_text;
            std::transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(), ::tolower);
        }

        std::vector<std::int64_t> matchingNodeIds;

        for (const auto& entity : entitiesRes.value()) {
            if (!entity.nodeId)
                continue;

            auto it = symbolInfoMap.find(entity.nodeId.value());
            if (it == symbolInfoMap.end())
                continue;

            const auto& symInfo = it->second;
            bool matches = false;

            if (!lowerQuery.empty()) {
                std::string lowerName = symInfo.name;
                std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);

                matches = (lowerName.find(lowerQuery) != std::string::npos) ||
                          (symInfo.qualifiedName.find(query_text) != std::string::npos);
            }

            if (matches) {
                maxRelevance = std::max(maxRelevance, entity.confidence.value_or(0.5f));
                matchingNodeIds.push_back(entity.nodeId.value());

                if (entity.startOffset && entity.endOffset) {
                    hasDefinition = true;
                    bestDefinitionScore = std::max(bestDefinitionScore, 1.5f);
                    ctx.matchType = "definition";
                } else if (ctx.matchType.empty()) {
                    ctx.matchType = "usage";
                }
            }

            ctx.symbols.push_back(symInfo);
        }

        if (!hasDefinition && !matchingNodeIds.empty()) {
            size_t checkLimit = std::min(matchingNodeIds.size(), size_t(3));
            for (size_t i = 0; i < checkLimit; ++i) {
                auto inEdges = kg_store_->getEdgesTo(matchingNodeIds[i], "defines", 1, 0);
                if (inEdges.has_value() && !inEdges.value().empty()) {
                    hasDefinition = true;
                    bestDefinitionScore = 1.5f;
                    ctx.matchType = "definition";
                    break;
                }
            }
        }

        if (ctx.symbols.empty()) {
            return false;
        }

        ctx.symbolScore = maxRelevance;
        ctx.definitionScore = bestDefinitionScore;
        ctx.isSymbolQuery = !query_text.empty() && maxRelevance > 0.3f;

        result.symbolContext = std::move(ctx);
        return true;

    } catch (const std::exception& e) {
        spdlog::debug("SymbolEnricher: exception enriching result: {}", e.what());
        return false;
    }
}

std::optional<SymbolInfo> SymbolEnricher::extractSymbolInfo(std::int64_t node_id) {
    if (!kg_store_) {
        return std::nullopt;
    }

    try {
        auto nodeRes = kg_store_->getNodeById(node_id);
        if (!nodeRes.has_value() || !nodeRes.value().has_value()) {
            return std::nullopt;
        }

        return extractSymbolInfoFromNode(nodeRes.value().value());

    } catch (const std::exception& e) {
        spdlog::debug("SymbolEnricher: exception extracting symbol info: {}", e.what());
        return std::nullopt;
    }
}

std::optional<SymbolInfo>
SymbolEnricher::extractSymbolInfoFromNode(const yams::metadata::KGNode& node) {
    if (!node.type || (node.type != "function" && node.type != "class" && node.type != "method" &&
                       node.type != "variable" && node.type != "typedef" && node.type != "macro")) {
        return std::nullopt;
    }

    SymbolInfo info;
    info.kind = node.type.value();
    info.name = node.label.value_or("");
    info.qualifiedName = info.name;

    if (node.properties) {
        try {
            auto props = nlohmann::json::parse(node.properties.value());

            if (props.contains("qualified_name")) {
                info.qualifiedName = props["qualified_name"].get<std::string>();
            }
            if (props.contains("file_path")) {
                info.definitionFile = props["file_path"].get<std::string>();
            }
            if (props.contains("start_line")) {
                info.definitionLine = props["start_line"].get<int>();
            }
            if (props.contains("return_type")) {
                info.returnType = props["return_type"].get<std::string>();
            }
            if (props.contains("parameters") && props["parameters"].is_array()) {
                for (const auto& p : props["parameters"]) {
                    info.parameters.push_back(p.get<std::string>());
                }
            }
            if (props.contains("documentation")) {
                info.documentation = props["documentation"].get<std::string>();
            }
        } catch (...) {
        }
    }

    return info;
}

std::unordered_map<std::int64_t, SymbolInfo>
SymbolEnricher::extractSymbolInfoBatch(const std::vector<std::int64_t>& node_ids) {
    std::unordered_map<std::int64_t, SymbolInfo> result;

    if (!kg_store_ || node_ids.empty()) {
        return result;
    }

    try {
        auto nodesRes = kg_store_->getNodesByIds(node_ids);
        if (!nodesRes.has_value()) {
            return result;
        }

        result.reserve(nodesRes.value().size());
        for (const auto& node : nodesRes.value()) {
            auto symInfo = extractSymbolInfoFromNode(node);
            if (symInfo) {
                result.emplace(node.id, std::move(symInfo.value()));
            }
        }

    } catch (const std::exception& e) {
        spdlog::debug("SymbolEnricher: exception in batch extraction: {}", e.what());
    }

    return result;
}

} // namespace yams::search
