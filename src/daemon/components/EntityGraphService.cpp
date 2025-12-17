#include <yams/daemon/components/EntityGraphService.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <unordered_map>
#include <boost/asio.hpp>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/plugins/symbol_extractor_v1.h>

namespace yams::daemon {

EntityGraphService::EntityGraphService(ServiceManager* services, std::size_t /*workers*/)
    : services_(services) {}

EntityGraphService::~EntityGraphService() {
    stop();
}

void EntityGraphService::start() {}

void EntityGraphService::stop() {
    stop_.store(true);
}

Result<void> EntityGraphService::submitExtraction(Job job) {
    if (stop_.load(std::memory_order_relaxed)) {
        return Error{ErrorCode::InvalidState, "service_stopped"};
    }
    if (!services_) {
        return Error{ErrorCode::InternalError, "no_services"};
    }
    auto* coordinator = services_->getWorkCoordinator();
    if (!coordinator) {
        return Error{ErrorCode::InternalError, "no_coordinator"};
    }
    accepted_.fetch_add(1, std::memory_order_relaxed);
    boost::asio::post(coordinator->getExecutor(), [this, j = std::move(job)]() mutable {
        if (stop_.load(std::memory_order_relaxed))
            return;
        try {
            if (!process(j))
                failed_.fetch_add(1, std::memory_order_relaxed);
        } catch (const std::exception& e) {
            spdlog::error("EntityGraphService: exception processing {}: {}", j.filePath, e.what());
            failed_.fetch_add(1, std::memory_order_relaxed);
        } catch (...) {
            spdlog::error("EntityGraphService: unknown exception processing {}", j.filePath);
            failed_.fetch_add(1, std::memory_order_relaxed);
        }
        processed_.fetch_add(1, std::memory_order_relaxed);
    });
    return Result<void>();
}

Result<void> EntityGraphService::reextractRange(const std::string& /*scope*/) {
    // TODO: query metadata for scope and enqueue jobs
    return Result<void>();
}

EntityGraphService::Stats EntityGraphService::getStats() const {
    return {accepted_.load(std::memory_order_relaxed), processed_.load(std::memory_order_relaxed),
            failed_.load(std::memory_order_relaxed)};
}

Result<void> EntityGraphService::materializeSymbolIndex() {
    // TODO: read KG and populate symbol_metadata derived table (or refresh views)
    return Result<void>();
}

std::vector<std::string> EntityGraphService::findSymbolsByName(const std::string& /*name*/) const {
    return {};
}

bool EntityGraphService::process(Job& job) {
    if (!services_)
        return false;

    auto kg = services_->getKgStore();
    if (!kg) {
        spdlog::debug("EntityGraphService: no KG store available");
        return true; // not an error if KG is not configured
    }

    // Locate a symbol extractor plugin that supports the language
    const auto& extractors = services_->getSymbolExtractors();
    yams_symbol_extractor_v1* table = nullptr;
    for (const auto& ex : extractors) {
        if (ex && ex->supportsLanguage(job.language)) {
            table = ex->table();
            break;
        }
    }
    if (!table || !table->extract_symbols) {
        spdlog::debug("EntityGraphService: no extractor for lang='{}' (skip)", job.language);
        return true; // not an error: just no-op
    }

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = table->extract_symbols(table->self, job.contentUtf8.data(), job.contentUtf8.size(),
                                    job.filePath.c_str(), job.language.c_str(), &result);
    if (rc != 0 || !result) {
        spdlog::warn("EntityGraphService: extract_symbols failed rc={} for {}", rc, job.filePath);
        return false;
    }

    spdlog::info("EntityGraphService: extracted {} symbols from {} (lang={})",
                 result->symbol_count, job.filePath, job.language);

    // Populate KG with rich symbol relationships
    bool success = populateKnowledgeGraph(kg, job, result);

    try {
        if (table->free_result)
            table->free_result(table->self, result);
    } catch (...) {
    }
    return success;
}

bool EntityGraphService::populateKnowledgeGraph(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    const yams_symbol_extraction_result_v1* result) {
    if (!result || result->symbol_count == 0) {
        return true; // No symbols to process
    }

    try {
        std::optional<std::int64_t> documentDbId;
        auto contextNodesRes = resolveContextNodes(kg, job, documentDbId);
        if (!contextNodesRes) {
            spdlog::warn("EntityGraphService: failed to resolve context nodes: {}",
                         contextNodesRes.error().message);
            return false;
        }
        auto& contextNodes = contextNodesRes.value();

        std::vector<std::string> symbolKeys;
        auto symbolNodeIdsRes = createSymbolNodes(kg, job, result, symbolKeys);
        if (!symbolNodeIdsRes) {
            spdlog::warn("EntityGraphService: failed to create symbol nodes: {}",
                         symbolNodeIdsRes.error().message);
            return false;
        }
        auto& symbolNodeIds = symbolNodeIdsRes.value();

        auto edgesRes = createSymbolEdges(kg, job, result, contextNodes, symbolNodeIds, symbolKeys);
        if (!edgesRes) {
            spdlog::warn("EntityGraphService: failed to create symbol edges: {}",
                         edgesRes.error().message);
            // Non-fatal, continue to doc entities
        }

        auto docEntitiesRes = createDocEntities(kg, documentDbId, result, symbolNodeIds);
        if (!docEntitiesRes) {
            spdlog::warn("EntityGraphService: failed to create doc entities: {}",
                         docEntitiesRes.error().message);
            // Non-fatal
        }

        spdlog::info("EntityGraphService: populated KG with {} symbols from {}",
                     result->symbol_count, job.filePath);
        return true;

    } catch (const std::exception& e) {
        spdlog::warn("EntityGraphService: exception populating KG: {}", e.what());
        return false;
    }
}

yams::Result<EntityGraphService::ContextNodes> EntityGraphService::resolveContextNodes(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    std::optional<std::int64_t>& documentDbId) {
    ContextNodes contextNodes;

    if (!job.documentHash.empty()) {
        auto docDbIdResult = kg->getDocumentIdByHash(job.documentHash);
        if (docDbIdResult.has_value()) {
            documentDbId = docDbIdResult.value();

            yams::metadata::KGNode docNode;
            docNode.nodeKey = "doc:" + job.documentHash;
            docNode.label = job.filePath;
            docNode.type = "document";
            nlohmann::json docProps;
            docProps["hash"] = job.documentHash;
            docProps["path"] = job.filePath;
            docProps["language"] = job.language;
            docNode.properties = docProps.dump();

            auto docNodeResult = kg->upsertNode(docNode);
            if (docNodeResult.has_value()) {
                contextNodes.documentNodeId = docNodeResult.value();
            }
        }
    }

    if (!job.filePath.empty()) {
        yams::metadata::KGNode fileNode;
        fileNode.nodeKey = "file:" + job.filePath;
        fileNode.label = job.filePath;
        fileNode.type = "file";
        nlohmann::json fileProps;
        fileProps["path"] = job.filePath;
        fileProps["language"] = job.language;
        if (!job.documentHash.empty()) {
            fileProps["current_hash"] = job.documentHash;
        }
        fileNode.properties = fileProps.dump();

        auto fileNodeResult = kg->upsertNode(fileNode);
        if (fileNodeResult.has_value()) {
            contextNodes.fileNodeId = fileNodeResult.value();
        }
    }

    if (contextNodes.fileNodeId.has_value() && contextNodes.documentNodeId.has_value()) {
        yams::metadata::KGEdge fileDocEdge;
        fileDocEdge.srcNodeId = contextNodes.fileNodeId.value();
        fileDocEdge.dstNodeId = contextNodes.documentNodeId.value();
        fileDocEdge.relation = "has_version";
        fileDocEdge.weight = 1.0f;
        nlohmann::json edgeProps;
        edgeProps["timestamp"] = std::chrono::system_clock::now().time_since_epoch().count();
        fileDocEdge.properties = edgeProps.dump();
        kg->addEdge(fileDocEdge);
    }

    if (!job.filePath.empty()) {
        size_t lastSlash = job.filePath.rfind('/');
        if (lastSlash != std::string::npos) {
            std::string dirPath = job.filePath.substr(0, lastSlash);

            yams::metadata::KGNode dirNode;
            dirNode.nodeKey = "dir:" + dirPath;
            dirNode.label = dirPath;
            dirNode.type = "directory";
            nlohmann::json dirProps;
            dirProps["path"] = dirPath;
            dirNode.properties = dirProps.dump();

            auto dirNodeResult = kg->upsertNode(dirNode);
            if (dirNodeResult.has_value()) {
                contextNodes.directoryNodeId = dirNodeResult.value();

                if (contextNodes.fileNodeId.has_value()) {
                    yams::metadata::KGEdge dirFileEdge;
                    dirFileEdge.srcNodeId = contextNodes.directoryNodeId.value();
                    dirFileEdge.dstNodeId = contextNodes.fileNodeId.value();
                    dirFileEdge.relation = "contains";
                    dirFileEdge.weight = 1.0f;
                    kg->addEdge(dirFileEdge);
                }
            }
        }
    }

    return contextNodes;
}

yams::Result<std::vector<std::int64_t>> EntityGraphService::createSymbolNodes(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    const yams_symbol_extraction_result_v1* result, std::vector<std::string>& outSymbolKeys) {
    std::vector<yams::metadata::KGNode> symbolNodes;
    symbolNodes.reserve(result->symbol_count);
    outSymbolKeys.reserve(result->symbol_count);

    auto now = std::chrono::system_clock::now().time_since_epoch().count();

    for (size_t i = 0; i < result->symbol_count; ++i) {
        const auto& sym = result->symbols[i];

        yams::metadata::KGNode node;
        std::string qualName = sym.qualified_name ? std::string(sym.qualified_name)
                                                  : (sym.name ? std::string(sym.name) : "");
        std::string kind = sym.kind ? std::string(sym.kind) : "symbol";
        node.nodeKey = kind + ":" + qualName + "@" + job.filePath;
        node.label = sym.name ? std::string(sym.name) : qualName;
        node.type = kind;

        nlohmann::json props;
        props["qualified_name"] = qualName;
        props["simple_name"] = sym.name ? std::string(sym.name) : "";
        props["file_path"] = sym.file_path ? std::string(sym.file_path) : job.filePath;
        props["language"] = job.language;
        props["start_line"] = sym.start_line;
        props["end_line"] = sym.end_line;
        props["start_offset"] = sym.start_offset;
        props["end_offset"] = sym.end_offset;
        props["last_seen"] = now;

        if (sym.return_type)
            props["return_type"] = std::string(sym.return_type);
        if (sym.documentation)
            props["documentation"] = std::string(sym.documentation);
        if (sym.parameters && sym.parameter_count > 0) {
            nlohmann::json params = nlohmann::json::array();
            for (size_t p = 0; p < sym.parameter_count; ++p) {
                if (sym.parameters[p])
                    params.push_back(std::string(sym.parameters[p]));
            }
            props["parameters"] = params;
        }
        if (!job.documentHash.empty()) {
            props["document_hash"] = job.documentHash;
        }

        node.properties = props.dump();
        symbolNodes.push_back(std::move(node));
        outSymbolKeys.push_back(qualName);
    }

    return kg->upsertNodes(symbolNodes);
}

yams::Result<void> EntityGraphService::createSymbolEdges(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    const yams_symbol_extraction_result_v1* result, const ContextNodes& contextNodes,
    const std::vector<std::int64_t>& symbolNodeIds, const std::vector<std::string>& symbolKeys) {
    // Aliases
    std::vector<yams::metadata::KGAlias> aliases;
    for (size_t i = 0; i < result->symbol_count; ++i) {
        const auto& sym = result->symbols[i];
        std::int64_t nodeId = symbolNodeIds[i];

        if (sym.name) {
            yams::metadata::KGAlias alias;
            alias.nodeId = nodeId;
            alias.alias = std::string(sym.name);
            alias.source = "symbol_name";
            alias.confidence = 1.0f;
            aliases.push_back(alias);
        }
        if (sym.qualified_name && sym.qualified_name != sym.name) {
            yams::metadata::KGAlias alias;
            alias.nodeId = nodeId;
            alias.alias = std::string(sym.qualified_name);
            alias.source = "qualified_name";
            alias.confidence = 1.0f;
            aliases.push_back(alias);
        }
        if (sym.qualified_name) {
            std::string qn(sym.qualified_name);
            size_t pos = 0;
            while ((pos = qn.find("::", pos)) != std::string::npos) {
                std::string partial = qn.substr(pos + 2);
                if (!partial.empty() && partial != sym.name) {
                    yams::metadata::KGAlias alias;
                    alias.nodeId = nodeId;
                    alias.alias = partial;
                    alias.source = "partial_qualified";
                    alias.confidence = 0.8f;
                    aliases.push_back(alias);
                }
                pos += 2;
            }
        }
    }
    if (!aliases.empty()) {
        kg->addAliases(aliases);
    }

    // Context Edges
    std::vector<yams::metadata::KGEdge> contextEdges;
    for (size_t i = 0; i < symbolNodeIds.size(); ++i) {
        std::int64_t symNodeId = symbolNodeIds[i];
        if (contextNodes.documentNodeId.has_value()) {
            nlohmann::json edgeProps;
            edgeProps["line_start"] = result->symbols[i].start_line;
            edgeProps["line_end"] = result->symbols[i].end_line;
            yams::metadata::KGEdge edge;
            edge.srcNodeId = symNodeId;
            edge.dstNodeId = contextNodes.documentNodeId.value();
            edge.relation = "defined_in";
            edge.weight = 1.0f;
            edge.properties = edgeProps.dump();
            contextEdges.push_back(edge);
        }
        if (contextNodes.fileNodeId.has_value()) {
            yams::metadata::KGEdge edge;
            edge.srcNodeId = symNodeId;
            edge.dstNodeId = contextNodes.fileNodeId.value();
            edge.relation = "located_in";
            edge.weight = 1.0f;
            contextEdges.push_back(edge);
        }
        if (contextNodes.directoryNodeId.has_value()) {
            yams::metadata::KGEdge edge;
            edge.srcNodeId = symNodeId;
            edge.dstNodeId = contextNodes.directoryNodeId.value();
            edge.relation = "scoped_by";
            edge.weight = 0.5f;
            contextEdges.push_back(edge);
        }
    }
    if (!contextEdges.empty()) {
        kg->addEdgesUnique(contextEdges);
    }

    // Symbol-to-symbol Edges
    if (result->relation_count > 0) {
        std::unordered_map<std::string, std::int64_t> qualNameToId;
        for (size_t i = 0; i < symbolKeys.size(); ++i) {
            qualNameToId[symbolKeys[i]] = symbolNodeIds[i];
        }

        std::vector<yams::metadata::KGEdge> symbolEdges;
        auto now = std::chrono::system_clock::now().time_since_epoch().count();
        for (size_t i = 0; i < result->relation_count; ++i) {
            const auto& rel = result->relations[i];
            if (!rel.src_symbol || !rel.dst_symbol || !rel.kind)
                continue;

            std::string src(rel.src_symbol);
            std::string dst(rel.dst_symbol);

            auto resolveNodeId = [&](const std::string& key) -> std::optional<std::int64_t> {
                auto it = qualNameToId.find(key);
                if (it != qualNameToId.end())
                    return it->second;
                auto aliasRes = kg->resolveAliasExact(key, 1);
                if (aliasRes.has_value() && !aliasRes.value().empty())
                    return aliasRes.value()[0].nodeId;
                return std::nullopt;
            };

            auto srcNodeIdOpt = resolveNodeId(src);
            auto dstNodeIdOpt = resolveNodeId(dst);

            if (srcNodeIdOpt && dstNodeIdOpt) {
                nlohmann::json relProps;
                relProps["source_file"] = job.filePath;
                relProps["timestamp"] = now;
                yams::metadata::KGEdge edge;
                edge.srcNodeId = *srcNodeIdOpt;
                edge.dstNodeId = *dstNodeIdOpt;
                edge.relation = std::string(rel.kind);
                edge.weight = static_cast<float>(rel.weight);
                edge.properties = relProps.dump();
                symbolEdges.push_back(edge);
            }
        }
        if (!symbolEdges.empty()) {
            kg->addEdgesUnique(symbolEdges);
        }
    }

    return yams::Result<void>();
}

yams::Result<void> EntityGraphService::createDocEntities(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg,
    std::optional<std::int64_t> documentDbId, const yams_symbol_extraction_result_v1* result,
    const std::vector<std::int64_t>& symbolNodeIds) {
    if (documentDbId.has_value()) {
        std::vector<yams::metadata::DocEntity> docEntities;
        docEntities.reserve(symbolNodeIds.size());
        for (size_t i = 0; i < symbolNodeIds.size(); ++i) {
            const auto& sym = result->symbols[i];

            yams::metadata::DocEntity entity;
            entity.documentId = documentDbId.value();
            entity.entityText = sym.qualified_name ? std::string(sym.qualified_name)
                                                   : (sym.name ? std::string(sym.name) : "");
            entity.nodeId = symbolNodeIds[i];
            entity.startOffset = sym.start_offset;
            entity.endOffset = sym.end_offset;
            entity.confidence = 1.0f;
            entity.extractor = "symbol_extractor_v1";
            docEntities.push_back(entity);
        }

        if (!docEntities.empty()) {
            kg->deleteDocEntitiesForDocument(documentDbId.value());
            return kg->addDocEntities(docEntities);
        }
    }
    return yams::Result<void>();
}

} // namespace yams::daemon
