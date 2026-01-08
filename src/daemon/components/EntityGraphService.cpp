#include <yams/daemon/components/EntityGraphService.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <chrono>
#include <filesystem>
#include <sstream>
#include <unordered_map>
#include <boost/asio.hpp>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/resource/abi_entity_extractor_adapter.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/plugins/entity_extractor_v2.h>
#include <yams/plugins/symbol_extractor_v1.h>
#include <yams/vector/vector_database.h>

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

    // Route to NL extraction for natural language content
    if (isNaturalLanguageContent(job)) {
        return processNaturalLanguage(job);
    }

    // Code path: Locate a symbol extractor plugin that supports the language
    const auto& extractors = services_->getSymbolExtractors();
    yams_symbol_extractor_v1* table = nullptr;
    const AbiSymbolExtractorAdapter* extractorAdapter = nullptr;
    for (const auto& ex : extractors) {
        if (ex && ex->supportsLanguage(job.language)) {
            table = ex->table();
            extractorAdapter = ex.get();
            break;
        }
    }
    if (!table || !table->extract_symbols) {
        // No code extractor - try NL extraction as fallback for unknown content
        const auto& nlExtractors = services_->getEntityExtractors();
        if (!nlExtractors.empty()) {
            spdlog::debug(
                "EntityGraphService: no code extractor for lang='{}', trying NL extraction",
                job.language);
            return processNaturalLanguage(job);
        }
        spdlog::debug("EntityGraphService: no extractor for lang='{}' (skip)", job.language);
        return true; // not an error: just no-op
    }

    // Get extractor ID for versioned state tracking
    std::string extractorId = extractorAdapter ? extractorAdapter->getExtractorId() : "unknown";

    yams_symbol_extraction_result_v1* result = nullptr;
    int rc = table->extract_symbols(table->self, job.contentUtf8.data(), job.contentUtf8.size(),
                                    job.filePath.c_str(), job.language.c_str(), &result);
    if (rc != 0 || !result) {
        spdlog::warn("EntityGraphService: extract_symbols failed rc={} for {}", rc, job.filePath);
        // Record failed extraction state
        if (!job.documentHash.empty()) {
            metadata::SymbolExtractionState state;
            state.extractorId = extractorId;
            state.extractedAt = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
            state.status = "failed";
            state.entityCount = 0;
            state.errorMessage = "extract_symbols returned rc=" + std::to_string(rc);
            auto upsertRes = kg->upsertSymbolExtractionState(job.documentHash, state);
            if (!upsertRes) {
                spdlog::debug("EntityGraphService: failed to record extraction failure: {}",
                              upsertRes.error().message);
            }
        }
        return false;
    }

    spdlog::info("EntityGraphService: extracted {} symbols from {} (lang={})", result->symbol_count,
                 job.filePath, job.language);

    // Populate KG with rich symbol relationships
    bool success = populateKnowledgeGraph(kg, job, result);

    // Record successful extraction state (even with 0 symbols)
    if (!job.documentHash.empty()) {
        metadata::SymbolExtractionState state;
        state.extractorId = extractorId;
        state.extractedAt = std::chrono::duration_cast<std::chrono::seconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        state.status = success ? "complete" : "failed";
        state.entityCount = static_cast<std::int64_t>(result->symbol_count);
        if (!success) {
            state.errorMessage = "populateKnowledgeGraph failed";
        }
        auto upsertRes = kg->upsertSymbolExtractionState(job.documentHash, state);
        if (!upsertRes) {
            spdlog::debug("EntityGraphService: failed to record extraction state: {}",
                          upsertRes.error().message);
        }
    }

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

    spdlog::info("EntityGraphService: received {} symbols, {} relations from {}",
                 result->symbol_count, result->relation_count, job.filePath);

    // Avoid removing historical edges when we have snapshot-scoped nodes.
    if (job.documentHash.empty() && !job.filePath.empty()) {
        auto cleanupResult = kg->deleteEdgesForSourceFile(job.filePath);
        if (cleanupResult) {
            if (cleanupResult.value() > 0) {
                spdlog::debug("EntityGraphService: cleaned up {} stale edges for {}",
                              cleanupResult.value(), job.filePath);
            }
        }
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

        auto symbolNodesRes = createSymbolNodes(kg, job, result);
        if (!symbolNodesRes) {
            spdlog::warn("EntityGraphService: failed to create symbol nodes: {}",
                         symbolNodesRes.error().message);
            return false;
        }
        auto& symbolNodes = symbolNodesRes.value();

        auto edgesRes = createSymbolEdges(kg, job, result, contextNodes, symbolNodes);
        if (!edgesRes) {
            spdlog::warn("EntityGraphService: failed to create symbol edges: {}",
                         edgesRes.error().message);
            // Non-fatal, continue to doc entities
        }

        auto docEntitiesRes =
            createDocEntities(kg, documentDbId, result, symbolNodes.versionNodeIds);
        if (!docEntitiesRes) {
            spdlog::warn("EntityGraphService: failed to create doc entities: {}",
                         docEntitiesRes.error().message);
            // Non-fatal
        }

        // Generate entity embeddings for semantic symbol search (non-blocking)
        auto embeddingsRes = generateEntityEmbeddings(job, result, symbolNodes);
        if (!embeddingsRes) {
            spdlog::debug("EntityGraphService: entity embeddings skipped or failed: {}",
                          embeddingsRes.error().message);
            // Non-fatal - KG is still populated
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
        if (!job.filePath.empty()) {
            fileProps["basename"] = std::filesystem::path(job.filePath).filename().string();
        }
        if (!job.documentHash.empty()) {
            fileProps["current_hash"] = job.documentHash;
        }
        fileNode.properties = fileProps.dump();

        auto fileNodeResult = kg->upsertNode(fileNode);
        if (fileNodeResult.has_value()) {
            contextNodes.fileNodeId = fileNodeResult.value();
        }
    }

    if (!job.filePath.empty() && !job.documentHash.empty()) {
        yams::metadata::PathNodeDescriptor descriptor;
        descriptor.snapshotId = job.documentHash;
        descriptor.path = job.filePath;
        descriptor.rootTreeHash = "";
        descriptor.isDirectory = false;
        auto pathNodeRes = kg->ensurePathNode(descriptor);
        if (pathNodeRes.has_value()) {
            contextNodes.pathNodeId = pathNodeRes.value();
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

yams::Result<EntityGraphService::SymbolNodeBatch> EntityGraphService::createSymbolNodes(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    const yams_symbol_extraction_result_v1* result) {
    std::vector<yams::metadata::KGNode> symbolNodes;
    std::vector<yams::metadata::KGNode> versionNodes;
    symbolNodes.reserve(result->symbol_count);
    versionNodes.reserve(result->symbol_count);

    SymbolNodeBatch batch;
    batch.symbolKeys.reserve(result->symbol_count);

    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    const bool hasSnapshot = !job.documentHash.empty();

    for (size_t i = 0; i < result->symbol_count; ++i) {
        const auto& sym = result->symbols[i];

        yams::metadata::KGNode canonicalNode;
        std::string qualName = sym.qualified_name ? std::string(sym.qualified_name)
                                                  : (sym.name ? std::string(sym.name) : "");
        std::string kind = sym.kind ? std::string(sym.kind) : "symbol";
        std::string canonicalKey = kind + ":" + qualName + "@" + job.filePath;
        canonicalNode.nodeKey = canonicalKey;
        canonicalNode.label = sym.name ? std::string(sym.name) : qualName;
        canonicalNode.type = kind;

        nlohmann::json canonicalProps;
        canonicalProps["qualified_name"] = qualName;
        canonicalProps["simple_name"] = sym.name ? std::string(sym.name) : "";
        canonicalProps["file_path"] = sym.file_path ? std::string(sym.file_path) : job.filePath;
        canonicalProps["language"] = job.language;
        canonicalNode.properties = canonicalProps.dump();

        symbolNodes.push_back(std::move(canonicalNode));
        batch.symbolKeys.push_back(qualName);

        if (hasSnapshot) {
            yams::metadata::KGNode versionNode;
            versionNode.nodeKey = canonicalKey + "@snap:" + job.documentHash;
            versionNode.label = sym.name ? std::string(sym.name) : qualName;
            versionNode.type = kind + "_version";

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
            props["snapshot_id"] = job.documentHash;
            props["document_hash"] = job.documentHash;
            props["canonical_key"] = canonicalKey;
            props["kind"] = kind;

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

            versionNode.properties = props.dump();
            versionNodes.push_back(std::move(versionNode));
        }
    }

    auto canonicalIdsRes = kg->upsertNodes(symbolNodes);
    if (!canonicalIdsRes) {
        return canonicalIdsRes.error();
    }

    batch.canonicalNodeIds = canonicalIdsRes.value();
    if (hasSnapshot) {
        auto versionIdsRes = kg->upsertNodes(versionNodes);
        if (!versionIdsRes) {
            return versionIdsRes.error();
        }
        batch.versionNodeIds = versionIdsRes.value();
    } else {
        batch.versionNodeIds = batch.canonicalNodeIds;
    }

    // Populate symbol_metadata table for fast SQL-based filtering
    if (hasSnapshot) {
        std::vector<yams::metadata::SymbolMetadata> symbolMetadata;
        symbolMetadata.reserve(result->symbol_count);

        for (size_t i = 0; i < result->symbol_count; ++i) {
            const auto& sym = result->symbols[i];

            yams::metadata::SymbolMetadata meta;
            meta.documentHash = job.documentHash;
            meta.filePath = sym.file_path ? std::string(sym.file_path) : job.filePath;
            meta.symbolName = sym.name ? std::string(sym.name) : "";
            meta.qualifiedName = sym.qualified_name ? std::string(sym.qualified_name)
                                                    : (sym.name ? std::string(sym.name) : "");
            meta.kind = sym.kind ? std::string(sym.kind) : "symbol";
            meta.startLine = sym.start_line;
            meta.endLine = sym.end_line;
            meta.startOffset = sym.start_offset;
            meta.endOffset = sym.end_offset;

            if (sym.return_type)
                meta.returnType = std::string(sym.return_type);
            if (sym.documentation)
                meta.documentation = std::string(sym.documentation);
            if (sym.parameters && sym.parameter_count > 0) {
                std::string params;
                for (size_t p = 0; p < sym.parameter_count; ++p) {
                    if (sym.parameters[p]) {
                        if (!params.empty())
                            params += ", ";
                        params += sym.parameters[p];
                    }
                }
                meta.parameters = params;
            }

            symbolMetadata.push_back(std::move(meta));
        }

        auto metaRes = kg->upsertSymbolMetadata(symbolMetadata);
        if (!metaRes) {
            spdlog::warn("Failed to upsert symbol_metadata for {}: {}", job.filePath,
                         metaRes.error().message);
            // Non-fatal: continue even if symbol_metadata fails
        }
    }

    return batch;
}

yams::Result<void> EntityGraphService::createSymbolEdges(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    const yams_symbol_extraction_result_v1* result, const ContextNodes& contextNodes,
    const SymbolNodeBatch& nodes) {
    // Aliases
    std::vector<yams::metadata::KGAlias> aliases;
    for (size_t i = 0; i < result->symbol_count; ++i) {
        const auto& sym = result->symbols[i];
        std::int64_t nodeId = nodes.canonicalNodeIds[i];

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
    for (size_t i = 0; i < nodes.versionNodeIds.size(); ++i) {
        std::int64_t symNodeId = nodes.versionNodeIds[i];
        if (contextNodes.documentNodeId.has_value()) {
            nlohmann::json edgeProps;
            edgeProps["line_start"] = result->symbols[i].start_line;
            edgeProps["line_end"] = result->symbols[i].end_line;
            if (!job.documentHash.empty())
                edgeProps["snapshot_id"] = job.documentHash;
            yams::metadata::KGEdge edge;
            edge.srcNodeId = symNodeId;
            edge.dstNodeId = contextNodes.documentNodeId.value();
            edge.relation = "defined_in";
            edge.weight = 1.0f;
            edge.properties = edgeProps.dump();
            contextEdges.push_back(edge);
        }
        if (contextNodes.pathNodeId.has_value()) {
            yams::metadata::KGEdge edge;
            edge.srcNodeId = symNodeId;
            edge.dstNodeId = contextNodes.pathNodeId.value();
            edge.relation = "located_in";
            edge.weight = 1.0f;
            contextEdges.push_back(edge);
        } else if (contextNodes.fileNodeId.has_value()) {
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

    // File/Path containment edges: file/path -> symbol
    std::vector<yams::metadata::KGEdge> containmentEdges;
    containmentEdges.reserve(nodes.versionNodeIds.size());
    std::optional<std::int64_t> containerId;
    if (contextNodes.fileNodeId.has_value()) {
        containerId = contextNodes.fileNodeId;
    } else if (contextNodes.pathNodeId.has_value()) {
        containerId = contextNodes.pathNodeId;
    }
    if (containerId.has_value()) {
        for (size_t i = 0; i < nodes.versionNodeIds.size(); ++i) {
            nlohmann::json edgeProps;
            edgeProps["line_start"] = result->symbols[i].start_line;
            edgeProps["line_end"] = result->symbols[i].end_line;
            if (!job.documentHash.empty())
                edgeProps["snapshot_id"] = job.documentHash;
            yams::metadata::KGEdge edge;
            edge.srcNodeId = containerId.value();
            edge.dstNodeId = nodes.versionNodeIds[i];
            edge.relation = "contains";
            edge.weight = 1.0f;
            edge.properties = edgeProps.dump();
            containmentEdges.push_back(edge);
        }
    }
    if (!containmentEdges.empty()) {
        kg->addEdgesUnique(containmentEdges);
    }

    // Canonical -> version edges for snapshot tracking
    if (!job.documentHash.empty()) {
        std::vector<yams::metadata::KGEdge> versionEdges;
        versionEdges.reserve(nodes.canonicalNodeIds.size());
        for (size_t i = 0; i < nodes.canonicalNodeIds.size(); ++i) {
            yams::metadata::KGEdge edge;
            edge.srcNodeId = nodes.canonicalNodeIds[i];
            edge.dstNodeId = nodes.versionNodeIds[i];
            edge.relation = "observed_as";
            edge.weight = 1.0f;
            nlohmann::json props;
            props["snapshot_id"] = job.documentHash;
            props["document_hash"] = job.documentHash;
            edge.properties = props.dump();
            versionEdges.push_back(edge);
        }
        kg->addEdgesUnique(versionEdges);
    }

    // Symbol-to-symbol Edges
    if (result->relation_count > 0) {
        std::unordered_map<std::string, std::int64_t> qualNameToId;
        for (size_t i = 0; i < nodes.symbolKeys.size(); ++i) {
            qualNameToId[nodes.symbolKeys[i]] = nodes.versionNodeIds[i];
        }

        std::vector<yams::metadata::KGEdge> symbolEdges;
        auto now = std::chrono::system_clock::now().time_since_epoch().count();

        // Helper to resolve symbol/alias to node ID
        auto resolveSymbolNodeId = [&](const std::string& key) -> std::optional<std::int64_t> {
            auto it = qualNameToId.find(key);
            if (it != qualNameToId.end())
                return it->second;
            auto aliasRes = kg->resolveAliasExact(key, 1);
            if (aliasRes.has_value() && !aliasRes.value().empty())
                return aliasRes.value()[0].nodeId;
            return std::nullopt;
        };

        auto resolveOrCreatePathNode = [&](const std::string& path) -> std::optional<std::int64_t> {
            if (job.documentHash.empty()) {
                return std::nullopt;
            }
            yams::metadata::PathNodeDescriptor descriptor;
            descriptor.snapshotId = job.documentHash;
            descriptor.path = path;
            descriptor.rootTreeHash = "";
            descriptor.isDirectory = false;
            auto nodeRes = kg->ensurePathNode(descriptor);
            if (nodeRes.has_value()) {
                return nodeRes.value();
            }
            return std::nullopt;
        };

        // Helper to resolve/create file node for includes
        auto resolveOrCreateFileNode = [&](const std::string& path) -> std::optional<std::int64_t> {
            // First try to find existing file node
            std::string nodeKey = "file:" + path;
            auto existingRes = kg->getNodeByKey(nodeKey);
            if (existingRes.has_value() && existingRes.value().has_value()) {
                return existingRes.value().value().id;
            }

            // Create a placeholder file node for the included file
            yams::metadata::KGNode fileNode;
            fileNode.nodeKey = nodeKey;
            fileNode.label = path;
            fileNode.type = "file";
            nlohmann::json fileProps;
            fileProps["path"] = path;
            fileProps["is_external"] = true; // Mark as external/unresolved
            fileNode.properties = fileProps.dump();

            auto newNodeRes = kg->upsertNode(fileNode);
            if (newNodeRes.has_value()) {
                return newNodeRes.value();
            }
            return std::nullopt;
        };

        for (size_t i = 0; i < result->relation_count; ++i) {
            const auto& rel = result->relations[i];
            if (!rel.src_symbol || !rel.dst_symbol || !rel.kind)
                continue;

            std::string src(rel.src_symbol);
            std::string dst(rel.dst_symbol);
            std::string kind(rel.kind);

            std::optional<std::int64_t> srcNodeIdOpt;
            std::optional<std::int64_t> dstNodeIdOpt;

            if (kind == "includes") {
                // For includes: src is the source file, dst is the included file/module
                // Use the current file node as source
                if (contextNodes.pathNodeId.has_value()) {
                    srcNodeIdOpt = contextNodes.pathNodeId;
                } else {
                    srcNodeIdOpt = contextNodes.fileNodeId;
                }
                if (auto pathNode = resolveOrCreatePathNode(dst)) {
                    dstNodeIdOpt = pathNode;
                } else {
                    dstNodeIdOpt = resolveOrCreateFileNode(dst);
                }
            } else {
                // For calls, inherits, implements: resolve as symbols
                srcNodeIdOpt = resolveSymbolNodeId(src);
                dstNodeIdOpt = resolveSymbolNodeId(dst);
            }

            if (srcNodeIdOpt && dstNodeIdOpt) {
                nlohmann::json relProps;
                relProps["source_file"] = job.filePath;
                if (!job.documentHash.empty())
                    relProps["snapshot_id"] = job.documentHash;
                relProps["timestamp"] = now;
                yams::metadata::KGEdge edge;
                edge.srcNodeId = *srcNodeIdOpt;
                edge.dstNodeId = *dstNodeIdOpt;
                edge.relation = kind;
                edge.weight = static_cast<float>(rel.weight);
                edge.properties = relProps.dump();
                symbolEdges.push_back(edge);
            }
        }
        if (!symbolEdges.empty()) {
            spdlog::info("EntityGraphService: creating {} symbol/include edges",
                         symbolEdges.size());
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

std::string EntityGraphService::buildSymbolText(const yams_symbol_extraction_result_v1* result,
                                                size_t index) {
    if (!result || index >= result->symbol_count) {
        return "";
    }

    const auto& sym = result->symbols[index];
    std::ostringstream oss;

    // Kind (function, class, method, etc.)
    if (sym.kind) {
        oss << sym.kind << " ";
    }

    // Qualified name or simple name
    if (sym.qualified_name) {
        oss << sym.qualified_name;
    } else if (sym.name) {
        oss << sym.name;
    }

    // Parameters (for functions/methods)
    if (sym.parameters && sym.parameter_count > 0) {
        oss << "(";
        for (size_t p = 0; p < sym.parameter_count; ++p) {
            if (p > 0)
                oss << ", ";
            if (sym.parameters[p])
                oss << sym.parameters[p];
        }
        oss << ")";
    }

    // Return type
    if (sym.return_type) {
        oss << " -> " << sym.return_type;
    }

    // Documentation (truncated to avoid excessive embedding text)
    if (sym.documentation) {
        std::string doc(sym.documentation);
        constexpr size_t kMaxDocLength = 500;
        if (doc.size() > kMaxDocLength) {
            doc = doc.substr(0, kMaxDocLength) + "...";
        }
        oss << " // " << doc;
    }

    return oss.str();
}

yams::Result<void>
EntityGraphService::generateEntityEmbeddings(const Job& job,
                                             const yams_symbol_extraction_result_v1* result,
                                             const SymbolNodeBatch& symbolNodes) {
    if (!services_) {
        return Result<void>(); // No services, skip silently
    }

    auto vdb = services_->getVectorDatabase();
    auto provider = services_->getModelProvider();

    if (!vdb) {
        spdlog::debug("EntityGraphService: no vector database, skipping entity embeddings");
        return Result<void>();
    }

    if (!provider || !provider->isAvailable()) {
        spdlog::debug(
            "EntityGraphService: no model provider available, skipping entity embeddings");
        return Result<void>();
    }

    if (!result || result->symbol_count == 0) {
        return Result<void>();
    }

    // Get the embedding model name
    std::string modelName = services_->getEmbeddingModelName();
    if (modelName.empty()) {
        auto loadedModels = provider->getLoadedModels();
        if (loadedModels.empty()) {
            spdlog::debug(
                "EntityGraphService: no embedding model loaded, skipping entity embeddings");
            return Result<void>();
        }
        modelName = loadedModels[0];
    }

    // Build text representations for each symbol
    std::vector<std::string> texts;
    texts.reserve(result->symbol_count);
    for (size_t i = 0; i < result->symbol_count; ++i) {
        texts.push_back(buildSymbolText(result, i));
    }

    // Filter out empty texts and track indices
    std::vector<std::string> nonEmptyTexts;
    std::vector<size_t> originalIndices;
    nonEmptyTexts.reserve(texts.size());
    originalIndices.reserve(texts.size());
    for (size_t i = 0; i < texts.size(); ++i) {
        if (!texts[i].empty()) {
            nonEmptyTexts.push_back(texts[i]);
            originalIndices.push_back(i);
        }
    }

    if (nonEmptyTexts.empty()) {
        return Result<void>();
    }

    // Generate embeddings in batch
    auto embedResult = provider->generateBatchEmbeddingsFor(modelName, nonEmptyTexts);
    if (!embedResult) {
        spdlog::warn("EntityGraphService: failed to generate entity embeddings: {}",
                     embedResult.error().message);
        return embedResult.error();
    }

    const auto& embeddings = embedResult.value();
    if (embeddings.size() != nonEmptyTexts.size()) {
        spdlog::warn("EntityGraphService: embedding count mismatch ({} vs {})", embeddings.size(),
                     nonEmptyTexts.size());
        return Error{ErrorCode::InternalError, "embedding count mismatch"};
    }

    // Get model info for versioning
    std::string modelVersion;
    auto modelInfo = provider->getModelInfo(modelName);
    if (modelInfo) {
        modelVersion = std::to_string(modelInfo.value().embeddingDim); // Use dim as pseudo-version
    }

    // Create EntityVectorRecords
    std::vector<vector::EntityVectorRecord> records;
    records.reserve(embeddings.size());

    for (size_t i = 0; i < embeddings.size(); ++i) {
        size_t origIdx = originalIndices[i];
        const auto& sym = result->symbols[origIdx];

        vector::EntityVectorRecord rec;
        // Use the symbol key from SymbolNodeBatch (qualified name)
        rec.node_key = symbolNodes.symbolKeys[origIdx];
        rec.embedding_type = vector::EntityEmbeddingType::SIGNATURE;
        rec.embedding = embeddings[i];
        rec.content = nonEmptyTexts[i];
        rec.model_id = modelName;
        rec.model_version = modelVersion;
        rec.embedded_at = std::chrono::system_clock::now();
        rec.is_stale = false;

        // Metadata from symbol
        rec.node_type = sym.kind ? std::string(sym.kind) : "symbol";
        rec.qualified_name = sym.qualified_name ? std::string(sym.qualified_name)
                                                : (sym.name ? std::string(sym.name) : "");
        rec.file_path = job.filePath;
        rec.document_hash = job.documentHash;

        records.push_back(std::move(rec));
    }

    // Insert into vector database
    auto insertResult = vdb->insertEntityVectorsBatch(records);
    if (!insertResult) {
        spdlog::warn("EntityGraphService: failed to insert entity vectors: {}",
                     insertResult.error().message);
        return insertResult.error();
    }

    spdlog::info("EntityGraphService: generated {} entity embeddings for {}", records.size(),
                 job.filePath);
    return Result<void>();
}

// ============================================================================
// Natural Language Entity Extraction (Glint/GLiNER integration)
// ============================================================================

bool EntityGraphService::isNaturalLanguageContent(const Job& job) {
    // Check MIME type first
    if (!job.mimeType.empty()) {
        if (job.mimeType == "text/plain" || job.mimeType == "text/markdown" ||
            job.mimeType == "application/json") {
            return true;
        }
    }

    // Check file extension
    if (!job.filePath.empty()) {
        std::filesystem::path p(job.filePath);
        std::string ext = p.extension().string();
        if (ext == ".txt" || ext == ".md" || ext == ".markdown" || ext == ".json" ||
            ext == ".jsonl" || ext == ".csv" || ext == ".tsv") {
            return true;
        }
    }

    // Check if language is empty or unknown (likely NL content)
    if (job.language.empty() || job.language == "unknown" || job.language == "text") {
        return true;
    }

    return false;
}

bool EntityGraphService::processNaturalLanguage(Job& job) {
    if (!services_)
        return false;

    auto kg = services_->getKgStore();
    if (!kg) {
        spdlog::debug("EntityGraphService: no KG store for NL extraction");
        return true;
    }

    // Find an entity extractor that supports the content type
    const auto& extractors = services_->getEntityExtractors();
    const AbiEntityExtractorAdapter* adapter = nullptr;

    // Determine content type to check
    std::string contentType = job.mimeType;
    if (contentType.empty()) {
        // Infer from extension
        std::filesystem::path p(job.filePath);
        std::string ext = p.extension().string();
        if (ext == ".md" || ext == ".markdown") {
            contentType = "text/markdown";
        } else if (ext == ".json" || ext == ".jsonl") {
            contentType = "application/json";
        } else {
            contentType = "text/plain";
        }
    }

    for (const auto& ex : extractors) {
        if (ex && ex->supportsContentType(contentType)) {
            adapter = ex.get();
            break;
        }
    }

    if (!adapter) {
        spdlog::debug("EntityGraphService: no NL extractor for content_type='{}' (skip)",
                      contentType);
        return true; // not an error
    }

    // Extract entities
    auto* result =
        adapter->extract(job.contentUtf8, nullptr, 0, job.language.c_str(), job.filePath.c_str());
    if (!result) {
        spdlog::warn("EntityGraphService: NL extraction failed for {}", job.filePath);
        // Record failed state
        if (!job.documentHash.empty()) {
            metadata::SymbolExtractionState state;
            state.extractorId = adapter->getExtractorId();
            state.extractedAt = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
            state.status = "failed";
            state.entityCount = 0;
            state.errorMessage = "NL extraction returned null";
            kg->upsertSymbolExtractionState(job.documentHash, state);
        }
        return false;
    }

    // Check for extraction error
    if (result->error) {
        spdlog::warn("EntityGraphService: NL extraction error for {}: {}", job.filePath,
                     result->error);
        adapter->freeResult(result);
        return false;
    }

    spdlog::info("EntityGraphService: extracted {} NL entities from {} (type={})",
                 result->entity_count, job.filePath, contentType);

    // Populate KG with NL entities
    bool success = populateKnowledgeGraphNL(kg, job, result, adapter);

    // Record extraction state
    if (!job.documentHash.empty()) {
        metadata::SymbolExtractionState state;
        state.extractorId = adapter->getExtractorId();
        state.extractedAt = std::chrono::duration_cast<std::chrono::seconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
        state.status = success ? "complete" : "failed";
        state.entityCount = static_cast<std::int64_t>(result->entity_count);
        kg->upsertSymbolExtractionState(job.documentHash, state);
    }

    adapter->freeResult(result);
    return success;
}

bool EntityGraphService::populateKnowledgeGraphNL(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    const yams_entity_extraction_result_v2* result, const AbiEntityExtractorAdapter* adapter) {
    if (!result || result->entity_count == 0) {
        return true; // No entities to process
    }

    spdlog::info("EntityGraphService: populating KG with {} NL entities from {}",
                 result->entity_count, job.filePath);

    try {
        // Resolve context nodes (document, file, directory)
        std::optional<std::int64_t> documentDbId;
        auto contextNodesRes = resolveContextNodes(kg, job, documentDbId);
        if (!contextNodesRes) {
            spdlog::warn("EntityGraphService: failed to resolve context nodes for NL: {}",
                         contextNodesRes.error().message);
            return false;
        }
        auto& contextNodes = contextNodesRes.value();

        // Create entity nodes
        auto entityNodesRes = createEntityNodes(kg, job, result);
        if (!entityNodesRes) {
            spdlog::warn("EntityGraphService: failed to create entity nodes: {}",
                         entityNodesRes.error().message);
            return false;
        }
        auto& entityNodes = entityNodesRes.value();

        // Link entities to document context
        if (contextNodes.documentNodeId.has_value() || contextNodes.fileNodeId.has_value()) {
            std::vector<yams::metadata::KGEdge> contextEdges;
            contextEdges.reserve(entityNodes.nodeIds.size());

            for (size_t i = 0; i < entityNodes.nodeIds.size(); ++i) {
                const auto& ent = result->entities[i];

                // Link to document
                if (contextNodes.documentNodeId.has_value()) {
                    nlohmann::json edgeProps;
                    edgeProps["start_offset"] = ent.start_offset;
                    edgeProps["end_offset"] = ent.end_offset;
                    edgeProps["confidence"] = ent.confidence;
                    if (!job.documentHash.empty())
                        edgeProps["snapshot_id"] = job.documentHash;

                    yams::metadata::KGEdge edge;
                    edge.srcNodeId = entityNodes.nodeIds[i];
                    edge.dstNodeId = contextNodes.documentNodeId.value();
                    edge.relation = "mentioned_in";
                    edge.weight = ent.confidence;
                    edge.properties = edgeProps.dump();
                    contextEdges.push_back(edge);
                }

                // Link to file if no document
                if (!contextNodes.documentNodeId.has_value() &&
                    contextNodes.fileNodeId.has_value()) {
                    yams::metadata::KGEdge edge;
                    edge.srcNodeId = entityNodes.nodeIds[i];
                    edge.dstNodeId = contextNodes.fileNodeId.value();
                    edge.relation = "mentioned_in";
                    edge.weight = ent.confidence;
                    contextEdges.push_back(edge);
                }
            }

            if (!contextEdges.empty()) {
                kg->addEdgesUnique(contextEdges);
            }
        }

        // Create DocEntity records for entity spans
        if (documentDbId.has_value()) {
            std::vector<yams::metadata::DocEntity> docEntities;
            docEntities.reserve(result->entity_count);

            for (size_t i = 0; i < result->entity_count; ++i) {
                const auto& ent = result->entities[i];

                yams::metadata::DocEntity docEnt;
                docEnt.documentId = documentDbId.value();
                docEnt.entityText = ent.text ? std::string(ent.text) : "";
                docEnt.nodeId = entityNodes.nodeIds[i];
                docEnt.startOffset = ent.start_offset;
                docEnt.endOffset = ent.end_offset;
                docEnt.confidence = ent.confidence;
                docEnt.extractor = adapter->getExtractorId();
                docEntities.push_back(docEnt);
            }

            if (!docEntities.empty()) {
                kg->deleteDocEntitiesForDocument(documentDbId.value());
                kg->addDocEntities(docEntities);
            }
        }

        // Generate embeddings for NL entities
        auto embeddingsRes = generateNLEntityEmbeddings(job, result, entityNodes);
        if (!embeddingsRes) {
            spdlog::debug("EntityGraphService: NL entity embeddings skipped: {}",
                          embeddingsRes.error().message);
            // Non-fatal
        }

        spdlog::info("EntityGraphService: populated KG with {} NL entities from {}",
                     result->entity_count, job.filePath);
        return true;

    } catch (const std::exception& e) {
        spdlog::warn("EntityGraphService: exception populating NL KG: {}", e.what());
        return false;
    }
}

yams::Result<EntityGraphService::EntityNodeBatch> EntityGraphService::createEntityNodes(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    const yams_entity_extraction_result_v2* result) {
    EntityNodeBatch batch;
    if (!result || result->entity_count == 0) {
        return batch;
    }

    std::vector<yams::metadata::KGNode> nodes;
    nodes.reserve(result->entity_count);
    batch.entityKeys.reserve(result->entity_count);

    auto now = std::chrono::system_clock::now().time_since_epoch().count();

    for (size_t i = 0; i < result->entity_count; ++i) {
        const auto& ent = result->entities[i];

        std::string text = ent.text ? std::string(ent.text) : "";
        std::string type = ent.type ? std::string(ent.type) : "entity";

        // Create canonical node key: type:normalized_text
        // Normalize text for canonical matching (lowercase, trim)
        std::string normalizedText = text;
        std::transform(normalizedText.begin(), normalizedText.end(), normalizedText.begin(),
                       ::tolower);

        std::string nodeKey = "nl_entity:" + type + ":" + normalizedText;

        yams::metadata::KGNode node;
        node.nodeKey = nodeKey;
        node.label = text;
        node.type = type;

        nlohmann::json props;
        props["entity_text"] = text;
        props["entity_type"] = type;
        props["confidence"] = ent.confidence;
        props["first_seen_file"] = job.filePath;
        props["last_seen"] = now;
        if (!job.documentHash.empty()) {
            props["first_seen_hash"] = job.documentHash;
        }
        if (ent.properties_json) {
            try {
                auto extraProps = nlohmann::json::parse(ent.properties_json);
                for (auto& [key, val] : extraProps.items()) {
                    props[key] = val;
                }
            } catch (...) {
            }
        }
        node.properties = props.dump();

        nodes.push_back(std::move(node));
        batch.entityKeys.push_back(nodeKey);
    }

    auto nodeIdsRes = kg->upsertNodes(nodes);
    if (!nodeIdsRes) {
        return nodeIdsRes.error();
    }

    batch.nodeIds = nodeIdsRes.value();

    // Add aliases for entity text (for search)
    std::vector<yams::metadata::KGAlias> aliases;
    for (size_t i = 0; i < result->entity_count; ++i) {
        const auto& ent = result->entities[i];
        if (!ent.text)
            continue;

        yams::metadata::KGAlias alias;
        alias.nodeId = batch.nodeIds[i];
        alias.alias = std::string(ent.text);
        alias.source = "nl_entity_text";
        alias.confidence = ent.confidence;
        aliases.push_back(alias);
    }

    if (!aliases.empty()) {
        kg->addAliases(aliases);
    }

    return batch;
}

yams::Result<void>
EntityGraphService::generateNLEntityEmbeddings(const Job& job,
                                               const yams_entity_extraction_result_v2* result,
                                               const EntityNodeBatch& entityNodes) {
    if (!services_) {
        return Result<void>();
    }

    auto vdb = services_->getVectorDatabase();
    auto provider = services_->getModelProvider();

    if (!vdb) {
        return Error{ErrorCode::NotFound, "no vector database"};
    }

    if (!provider || !provider->isAvailable()) {
        return Error{ErrorCode::NotFound, "no model provider"};
    }

    if (!result || result->entity_count == 0) {
        return Result<void>();
    }

    std::string modelName = services_->getEmbeddingModelName();
    if (modelName.empty()) {
        auto loadedModels = provider->getLoadedModels();
        if (loadedModels.empty()) {
            return Error{ErrorCode::NotFound, "no embedding model"};
        }
        modelName = loadedModels[0];
    }

    // Build text representations for entities
    std::vector<std::string> texts;
    std::vector<size_t> originalIndices;
    texts.reserve(result->entity_count);
    originalIndices.reserve(result->entity_count);

    for (size_t i = 0; i < result->entity_count; ++i) {
        const auto& ent = result->entities[i];
        if (!ent.text)
            continue;

        // Format: "TYPE: text"
        std::string text;
        if (ent.type) {
            text = std::string(ent.type) + ": " + std::string(ent.text);
        } else {
            text = std::string(ent.text);
        }

        texts.push_back(text);
        originalIndices.push_back(i);
    }

    if (texts.empty()) {
        return Result<void>();
    }

    // Generate embeddings
    auto embedResult = provider->generateBatchEmbeddingsFor(modelName, texts);
    if (!embedResult) {
        return embedResult.error();
    }

    const auto& embeddings = embedResult.value();
    if (embeddings.size() != texts.size()) {
        return Error{ErrorCode::InternalError, "embedding count mismatch"};
    }

    // Create EntityVectorRecords
    std::vector<vector::EntityVectorRecord> records;
    records.reserve(embeddings.size());

    std::string modelVersion;
    auto modelInfo = provider->getModelInfo(modelName);
    if (modelInfo) {
        modelVersion = std::to_string(modelInfo.value().embeddingDim);
    }

    for (size_t i = 0; i < embeddings.size(); ++i) {
        size_t origIdx = originalIndices[i];
        const auto& ent = result->entities[origIdx];

        vector::EntityVectorRecord rec;
        rec.node_key = entityNodes.entityKeys[origIdx];
        rec.embedding_type = vector::EntityEmbeddingType::SIGNATURE;
        rec.embedding = embeddings[i];
        rec.content = texts[i];
        rec.model_id = modelName;
        rec.model_version = modelVersion;
        rec.embedded_at = std::chrono::system_clock::now();
        rec.is_stale = false;

        rec.node_type = ent.type ? std::string(ent.type) : "entity";
        rec.qualified_name = ent.text ? std::string(ent.text) : "";
        rec.file_path = job.filePath;
        rec.document_hash = job.documentHash;

        records.push_back(std::move(rec));
    }

    auto insertResult = vdb->insertEntityVectorsBatch(records);
    if (!insertResult) {
        return insertResult.error();
    }

    spdlog::info("EntityGraphService: generated {} NL entity embeddings for {}", records.size(),
                 job.filePath);
    return Result<void>();
}

} // namespace yams::daemon
