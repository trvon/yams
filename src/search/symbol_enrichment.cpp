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
        // Get document ID from hash or path
        std::optional<std::int64_t> docId;

        // Try extracting hash from metadata
        auto hashIt = result.metadata.find("sha256_hash");
        if (hashIt != result.metadata.end()) {
            auto docIdRes = kg_store_->getDocumentIdByHash(hashIt->second);
            if (docIdRes.has_value()) {
                docId = docIdRes.value();
            }
        }

        // Fallback to path
        if (!docId && !result.path.empty()) {
            auto docIdRes = kg_store_->getDocumentIdByPath(result.path);
            if (docIdRes.has_value()) {
                docId = docIdRes.value();
            }
        }

        if (!docId) {
            return false; // Can't resolve document
        }

        // Get symbols linked to this document
        auto entitiesRes = kg_store_->getDocEntitiesForDocument(docId.value(), 50, 0);
        if (!entitiesRes.has_value() || entitiesRes.value().empty()) {
            return false; // No symbol entities
        }

        // Build symbol context
        SymbolContext ctx;
        ctx.symbols.reserve(entitiesRes.value().size());

        std::unordered_set<std::int64_t> seenNodes;
        float maxRelevance = 0.0f;
        bool hasDefinition = false;
        float bestDefinitionScore = 1.0f;

        for (const auto& entity : entitiesRes.value()) {
            if (!entity.nodeId || seenNodes.count(entity.nodeId.value())) {
                continue;
            }
            seenNodes.insert(entity.nodeId.value());

            auto symInfo = extractSymbolInfo(entity.nodeId.value());
            if (!symInfo) {
                continue;
            }

            bool matches = false;
            if (!query_text.empty()) {
                std::string lowerQuery = query_text;
                std::string lowerName = symInfo->name;
                std::transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(), ::tolower);
                std::transform(lowerName.begin(), lowerName.end(), lowerName.begin(), ::tolower);

                matches = (lowerName.find(lowerQuery) != std::string::npos) ||
                          (symInfo->qualifiedName.find(query_text) != std::string::npos);
            }

            if (matches) {
                maxRelevance = std::max(maxRelevance, entity.confidence.value_or(0.5f));

                // PBI-074: Check KG edges to determine if this is a definition
                bool isDefinition = false;
                auto inEdges = kg_store_->getEdgesTo(entity.nodeId.value(), "defines", 10, 0);
                if (inEdges.has_value() && !inEdges.value().empty()) {
                    isDefinition = true;
                } else if (entity.startOffset && entity.endOffset) {
                    // Fallback: check if symbol has offset (likely definition)
                    isDefinition = true;
                }

                if (isDefinition) {
                    hasDefinition = true;
                    bestDefinitionScore = std::max(bestDefinitionScore, 1.5f);
                    ctx.matchType = "definition";
                } else if (ctx.matchType.empty()) {
                    ctx.matchType = "usage";
                }
            }

            ctx.symbols.push_back(std::move(symInfo.value()));
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

        const auto& node = nodeRes.value().value();

        // Only process symbol nodes
        if (!node.type ||
            (node.type != "function" && node.type != "class" && node.type != "method" &&
             node.type != "variable" && node.type != "typedef" && node.type != "macro")) {
            return std::nullopt;
        }

        SymbolInfo info;
        info.kind = node.type.value();
        info.name = node.label.value_or("");
        info.qualifiedName = info.name;

        // Parse properties JSON
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
                // JSON parse error, continue with partial info
            }
        }

        // Get callers/callees count from edges
        auto outEdges = kg_store_->getEdgesFrom(node_id, std::nullopt, 100, 0);
        if (outEdges.has_value()) {
            info.calleesCount = static_cast<int>(outEdges.value().size());

            // Extract related symbols from edges
            for (const auto& edge : outEdges.value()) {
                if (edge.relation == "calls" || edge.relation == "uses") {
                    auto targetNode = kg_store_->getNodeById(edge.dstNodeId);
                    if (targetNode.has_value() && targetNode.value().has_value()) {
                        info.relatedSymbols.push_back(
                            targetNode.value().value().label.value_or(""));
                    }
                }
            }
        }

        auto inEdges = kg_store_->getEdgesTo(node_id, std::nullopt, 100, 0);
        if (inEdges.has_value()) {
            info.callersCount = static_cast<int>(inEdges.value().size());
        }

        return info;

    } catch (const std::exception& e) {
        spdlog::debug("SymbolEnricher: exception extracting symbol info: {}", e.what());
        return std::nullopt;
    }
}

} // namespace yams::search
