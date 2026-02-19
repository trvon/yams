#include <yams/daemon/components/EntityGraphService.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/common/utf8_utils.h>
#include <chrono>
#include <filesystem>
#include <unordered_map>
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/KGWriteQueue.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/TuneAdvisor.h>
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

void EntityGraphService::start() {
    if (!services_)
        return;
    auto* coordinator = services_->getWorkCoordinator();
    if (!coordinator)
        return;

    stop_.store(false);
    boost::asio::co_spawn(coordinator->getExecutor(), channelPoller(), boost::asio::detached);
    spdlog::debug("EntityGraphService: channel poller started");
}

void EntityGraphService::stop() {
    stop_.store(true);
}

boost::asio::awaitable<void> EntityGraphService::channelPoller() {
    constexpr std::size_t kChannelCapacity = 4096;
    auto channel =
        InternalEventBus::instance().get_or_create_channel<InternalEventBus::EntityGraphJob>(
            "entity_graph_jobs", kChannelCapacity);

    boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);

    constexpr auto kMinIdleDelay = std::chrono::milliseconds(1);
    constexpr auto kMaxIdleDelay = std::chrono::milliseconds(10);
    auto idleDelay = kMinIdleDelay;

    while (!stop_.load(std::memory_order_relaxed)) {
        bool didWork = false;
        InternalEventBus::EntityGraphJob busJob;

        while (channel->try_pop(busJob)) {
            didWork = true;

            Job job;
            job.documentHash = std::move(busJob.documentHash);
            job.filePath = std::move(busJob.filePath);
            job.contentUtf8 = std::move(busJob.contentUtf8);
            job.language = std::move(busJob.language);
            job.mimeType = std::move(busJob.mimeType);

            try {
                if (!process(job))
                    failed_.fetch_add(1, std::memory_order_relaxed);
            } catch (const std::exception& e) {
                spdlog::error("EntityGraphService: exception processing {}: {}", job.filePath,
                              e.what());
                failed_.fetch_add(1, std::memory_order_relaxed);
            } catch (...) {
                spdlog::error("EntityGraphService: unknown exception processing {}", job.filePath);
                failed_.fetch_add(1, std::memory_order_relaxed);
            }
            processed_.fetch_add(1, std::memory_order_relaxed);
            InternalEventBus::instance().incEntityGraphConsumed();
        }

        if (didWork) {
            idleDelay = kMinIdleDelay;
            continue;
        }

        timer.expires_after(idleDelay);
        co_await timer.async_wait(boost::asio::use_awaitable);
        if (idleDelay < kMaxIdleDelay) {
            idleDelay = std::min(idleDelay * 2, kMaxIdleDelay);
        }
    }

    spdlog::debug("EntityGraphService: channel poller exited");
}

Result<void> EntityGraphService::submitExtraction(Job job) {
    if (stop_.load(std::memory_order_relaxed)) {
        return Error{ErrorCode::InvalidState, "service_stopped"};
    }
    if (!services_) {
        return Error{ErrorCode::InternalError, "no_services"};
    }

    accepted_.fetch_add(1, std::memory_order_relaxed);

    // Route through InternalEventBus for centralized backpressure and observability
    auto& bus = InternalEventBus::instance();
    auto filePath = job.filePath; // capture before move
    InternalEventBus::EntityGraphJob busJob;
    busJob.documentHash = std::move(job.documentHash);
    busJob.filePath = std::move(job.filePath);
    busJob.contentUtf8 = std::move(job.contentUtf8);
    busJob.language = std::move(job.language);
    busJob.mimeType = std::move(job.mimeType);

    constexpr std::size_t kChannelCapacity = 4096;
    auto channel = bus.get_or_create_channel<InternalEventBus::EntityGraphJob>("entity_graph_jobs",
                                                                               kChannelCapacity);

    if (channel->try_push(std::move(busJob))) {
        bus.incEntityGraphQueued();
    } else {
        bus.incEntityGraphDropped();
        spdlog::debug("EntityGraphService: channel full, dropping job for {}", filePath);
    }

    return Result<void>();
}

EntityGraphService::Stats EntityGraphService::getStats() const {
    return {accepted_.load(std::memory_order_relaxed), processed_.load(std::memory_order_relaxed),
            failed_.load(std::memory_order_relaxed)};
}

bool EntityGraphService::process(Job& job) {
    if (!services_)
        return false;

    auto kg = services_->getKgStore();
    if (!kg) {
        spdlog::debug("EntityGraphService: no KG store available");
        return true; // not an error if KG is not configured
    }

    // NL extraction handled in PostIngestQueue title+NL stage
    if (isNaturalLanguageContent(job)) {
        return true;
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
        return true; // no code extractor, NL handled in title+NL stage
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

    spdlog::debug("EntityGraphService: extracted {} symbols from {} (lang={})",
                  result->symbol_count, job.filePath, job.language);

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

    spdlog::debug("EntityGraphService: received {} symbols, {} relations from {}",
                  result->symbol_count, result->relation_count, job.filePath);

    // Require KGWriteQueue - batched writes only (no fallback to per-document commits)
    KGWriteQueue* kgQueue = services_ ? services_->getKgWriteQueue() : nullptr;
    if (!kgQueue) {
        spdlog::error("EntityGraphService: KGWriteQueue not available, cannot process symbols");
        return false;
    }

    return populateKnowledgeGraphDeferred(kg, job, result, kgQueue);
}

bool EntityGraphService::populateKnowledgeGraphDeferred(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kg, const Job& job,
    const yams_symbol_extraction_result_v1* result, KGWriteQueue* kgQueue) {
    // Build a DeferredKGBatch with all operations using nodeKey references
    // This eliminates lock contention by batching writes from multiple documents
    auto batch = std::make_unique<DeferredKGBatch>();
    batch->sourceFile = job.filePath;

    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    const bool hasSnapshot = !job.documentHash.empty();

    // Get document database ID for doc entities (read operation, safe)
    std::optional<std::int64_t> documentDbId;
    if (hasSnapshot) {
        auto docDbIdResult = kg->getDocumentIdByHash(job.documentHash);
        if (docDbIdResult.has_value()) {
            documentDbId = docDbIdResult.value();
            batch->documentIdToDelete = documentDbId; // Delete old doc entities
        }
    }

    // Optionally cleanup stale edges for this file
    if (job.documentHash.empty() && !job.filePath.empty()) {
        batch->sourceFileToDelete = job.filePath;
    }

    // === Build context nodes ===

    // Document node
    std::string docNodeKey;
    if (hasSnapshot) {
        docNodeKey = "doc:" + job.documentHash;

        yams::metadata::KGNode docNode;
        docNode.nodeKey = docNodeKey;
        docNode.label = common::sanitizeUtf8(job.filePath);
        docNode.type = "document";
        nlohmann::json docProps;
        docProps["hash"] = job.documentHash;
        docProps["path"] = common::sanitizeUtf8(job.filePath);
        docProps["language"] = common::sanitizeUtf8(job.language);
        docNode.properties = docProps.dump();
        batch->nodes.push_back(std::move(docNode));
    }

    // File node
    std::string fileNodeKey;
    if (!job.filePath.empty()) {
        fileNodeKey = "file:" + job.filePath;

        yams::metadata::KGNode fileNode;
        fileNode.nodeKey = fileNodeKey;
        fileNode.label = common::sanitizeUtf8(job.filePath);
        fileNode.type = "file";
        nlohmann::json fileProps;
        fileProps["path"] = common::sanitizeUtf8(job.filePath);
        fileProps["language"] = common::sanitizeUtf8(job.language);
        fileProps["basename"] =
            common::sanitizeUtf8(std::filesystem::path(job.filePath).filename().string());
        if (hasSnapshot) {
            fileProps["current_hash"] = job.documentHash;
        }
        fileNode.properties = fileProps.dump();
        batch->nodes.push_back(std::move(fileNode));
    }

    // Directory node
    std::string dirNodeKey;
    if (!job.filePath.empty()) {
        size_t lastSlash = job.filePath.rfind('/');
        if (lastSlash != std::string::npos) {
            std::string dirPath = job.filePath.substr(0, lastSlash);
            dirNodeKey = "dir:" + dirPath;

            yams::metadata::KGNode dirNode;
            dirNode.nodeKey = dirNodeKey;
            dirNode.label = common::sanitizeUtf8(dirPath);
            dirNode.type = "directory";
            nlohmann::json dirProps;
            dirProps["path"] = common::sanitizeUtf8(dirPath);
            dirNode.properties = dirProps.dump();
            batch->nodes.push_back(std::move(dirNode));
        }
    }

    // File -> Document edge
    if (!fileNodeKey.empty() && !docNodeKey.empty()) {
        DeferredEdge edge;
        edge.srcNodeKey = fileNodeKey;
        edge.dstNodeKey = docNodeKey;
        edge.relation = "has_version";
        edge.weight = 1.0f;
        nlohmann::json edgeProps;
        edgeProps["timestamp"] = now;
        edge.properties = edgeProps.dump();
        batch->deferredEdges.push_back(std::move(edge));
    }

    // Directory -> File edge
    if (!dirNodeKey.empty() && !fileNodeKey.empty()) {
        DeferredEdge edge;
        edge.srcNodeKey = dirNodeKey;
        edge.dstNodeKey = fileNodeKey;
        edge.relation = "contains";
        edge.weight = 1.0f;
        batch->deferredEdges.push_back(std::move(edge));
    }

    // === Build symbol nodes ===
    std::vector<std::string> canonicalNodeKeys;
    std::vector<std::string> versionNodeKeys;
    std::vector<std::string> qualifiedNames;
    canonicalNodeKeys.reserve(result->symbol_count);
    versionNodeKeys.reserve(result->symbol_count);
    qualifiedNames.reserve(result->symbol_count);

    for (size_t i = 0; i < result->symbol_count; ++i) {
        const auto& sym = result->symbols[i];

        std::string qualName = sym.qualified_name ? std::string(sym.qualified_name)
                                                  : (sym.name ? std::string(sym.name) : "");
        std::string kind = sym.kind ? std::string(sym.kind) : "symbol";
        std::string canonicalKey = kind + ":" + qualName + "@" + job.filePath;
        canonicalNodeKeys.push_back(canonicalKey);
        qualifiedNames.push_back(qualName);

        // Canonical node
        yams::metadata::KGNode canonicalNode;
        canonicalNode.nodeKey = canonicalKey;
        canonicalNode.label = sym.name ? std::string(sym.name) : qualName;
        canonicalNode.type = kind;

        nlohmann::json canonicalProps;
        canonicalProps["qualified_name"] = qualName;
        canonicalProps["simple_name"] = sym.name ? std::string(sym.name) : "";
        canonicalProps["file_path"] = sym.file_path ? std::string(sym.file_path) : job.filePath;
        canonicalProps["language"] = job.language;
        canonicalNode.properties = canonicalProps.dump();
        batch->nodes.push_back(std::move(canonicalNode));

        // Version node (if snapshot)
        if (hasSnapshot) {
            std::string versionKey = canonicalKey + "@snap:" + job.documentHash;
            versionNodeKeys.push_back(versionKey);

            yams::metadata::KGNode versionNode;
            versionNode.nodeKey = versionKey;
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
            batch->nodes.push_back(std::move(versionNode));
        } else {
            versionNodeKeys.push_back(canonicalKey); // No snapshot: version = canonical
        }
    }

    // === Build symbol metadata ===
    if (hasSnapshot) {
        batch->symbolMetadata.reserve(result->symbol_count);
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
            batch->symbolMetadata.push_back(std::move(meta));
        }
    }

    // === Build aliases ===
    // Note: Aliases are added to batch->aliases with nodeId=0; resolved at apply time
    for (size_t i = 0; i < result->symbol_count; ++i) {
        const auto& sym = result->symbols[i];
        const std::string& nodeKey = canonicalNodeKeys[i];

        // Store nodeKey in alias.source field for resolution (hacky but works)
        // The KGWriteQueue will resolve these based on the nodeKey->nodeId map
        if (sym.name) {
            yams::metadata::KGAlias alias;
            alias.nodeId = 0; // Will be resolved
            alias.alias = std::string(sym.name);
            alias.source = "symbol_name|" + nodeKey;
            alias.confidence = 1.0f;
            batch->aliases.push_back(alias);
        }
        if (sym.qualified_name && sym.qualified_name != sym.name) {
            yams::metadata::KGAlias alias;
            alias.nodeId = 0;
            alias.alias = std::string(sym.qualified_name);
            alias.source = "qualified_name|" + nodeKey;
            alias.confidence = 1.0f;
            batch->aliases.push_back(alias);
        }
        // Partial qualified name aliases
        if (sym.qualified_name) {
            std::string qn(sym.qualified_name);
            size_t pos = 0;
            while ((pos = qn.find("::", pos)) != std::string::npos) {
                std::string partial = qn.substr(pos + 2);
                if (!partial.empty() && partial != sym.name) {
                    yams::metadata::KGAlias alias;
                    alias.nodeId = 0;
                    alias.alias = partial;
                    alias.source = "partial_qualified|" + nodeKey;
                    alias.confidence = 0.8f;
                    batch->aliases.push_back(alias);
                }
                pos += 2;
            }
        }
    }

    // === Build context edges ===
    // Symbol -> document, symbol -> file, symbol -> directory
    std::string targetNodeKey = !docNodeKey.empty() ? docNodeKey : fileNodeKey;
    for (size_t i = 0; i < result->symbol_count; ++i) {
        const std::string& symNodeKey = versionNodeKeys[i];

        // Symbol -> document (defined_in)
        if (!docNodeKey.empty()) {
            DeferredEdge edge;
            edge.srcNodeKey = symNodeKey;
            edge.dstNodeKey = docNodeKey;
            edge.relation = "defined_in";
            edge.weight = 1.0f;
            nlohmann::json edgeProps;
            edgeProps["line_start"] = result->symbols[i].start_line;
            edgeProps["line_end"] = result->symbols[i].end_line;
            if (hasSnapshot)
                edgeProps["snapshot_id"] = job.documentHash;
            edge.properties = edgeProps.dump();
            batch->deferredEdges.push_back(std::move(edge));
        }

        // Symbol -> file (located_in)
        if (!fileNodeKey.empty()) {
            DeferredEdge edge;
            edge.srcNodeKey = symNodeKey;
            edge.dstNodeKey = fileNodeKey;
            edge.relation = "located_in";
            edge.weight = 1.0f;
            batch->deferredEdges.push_back(std::move(edge));
        }

        // Symbol -> directory (scoped_by)
        if (!dirNodeKey.empty()) {
            DeferredEdge edge;
            edge.srcNodeKey = symNodeKey;
            edge.dstNodeKey = dirNodeKey;
            edge.relation = "scoped_by";
            edge.weight = 0.5f;
            batch->deferredEdges.push_back(std::move(edge));
        }
    }

    // === Build containment edges ===
    // File -> symbol (contains)
    if (!fileNodeKey.empty()) {
        for (size_t i = 0; i < result->symbol_count; ++i) {
            DeferredEdge edge;
            edge.srcNodeKey = fileNodeKey;
            edge.dstNodeKey = versionNodeKeys[i];
            edge.relation = "contains";
            edge.weight = 1.0f;
            nlohmann::json edgeProps;
            edgeProps["line_start"] = result->symbols[i].start_line;
            edgeProps["line_end"] = result->symbols[i].end_line;
            if (hasSnapshot)
                edgeProps["snapshot_id"] = job.documentHash;
            edge.properties = edgeProps.dump();
            batch->deferredEdges.push_back(std::move(edge));
        }
    }

    // === Build canonical -> version edges ===
    if (hasSnapshot) {
        for (size_t i = 0; i < result->symbol_count; ++i) {
            DeferredEdge edge;
            edge.srcNodeKey = canonicalNodeKeys[i];
            edge.dstNodeKey = versionNodeKeys[i];
            edge.relation = "observed_as";
            edge.weight = 1.0f;
            nlohmann::json props;
            props["snapshot_id"] = job.documentHash;
            props["document_hash"] = job.documentHash;
            edge.properties = props.dump();
            batch->deferredEdges.push_back(std::move(edge));
        }
    }

    // === Build symbol relation edges ===
    // Note: For deferred path, we can only handle intra-file relations reliably
    // Cross-file references would need the target symbol to be in the nodeKeyToId map
    if (result->relation_count > 0) {
        std::unordered_map<std::string, std::string> qualNameToNodeKey;
        for (size_t i = 0; i < qualifiedNames.size(); ++i) {
            qualNameToNodeKey[qualifiedNames[i]] = versionNodeKeys[i];
        }

        for (size_t i = 0; i < result->relation_count; ++i) {
            const auto& rel = result->relations[i];
            if (!rel.src_symbol || !rel.dst_symbol || !rel.kind)
                continue;

            std::string src(rel.src_symbol);
            std::string dst(rel.dst_symbol);
            std::string kind(rel.kind);

            std::string srcNodeKey;
            std::string dstNodeKey;

            if (kind == "includes") {
                // For includes: src is current file, dst is included file
                srcNodeKey = fileNodeKey;
                dstNodeKey = "file:" + dst; // External file reference
            } else {
                // For calls, inherits, etc.: resolve as symbols
                auto srcIt = qualNameToNodeKey.find(src);
                auto dstIt = qualNameToNodeKey.find(dst);
                if (srcIt != qualNameToNodeKey.end())
                    srcNodeKey = srcIt->second;
                if (dstIt != qualNameToNodeKey.end())
                    dstNodeKey = dstIt->second;
            }

            // Only add edge if both endpoints can be resolved within this batch
            // or are context nodes (file, doc) that will be in the batch
            if (!srcNodeKey.empty() && !dstNodeKey.empty()) {
                DeferredEdge edge;
                edge.srcNodeKey = srcNodeKey;
                edge.dstNodeKey = dstNodeKey;
                edge.relation = kind;
                edge.weight = static_cast<float>(rel.weight);
                nlohmann::json relProps;
                relProps["source_file"] = job.filePath;
                if (hasSnapshot)
                    relProps["snapshot_id"] = job.documentHash;
                relProps["timestamp"] = now;
                edge.properties = relProps.dump();
                batch->deferredEdges.push_back(std::move(edge));
            }
        }
    }

    // === Build deferred doc entities ===
    if (documentDbId.has_value()) {
        for (size_t i = 0; i < result->symbol_count; ++i) {
            const auto& sym = result->symbols[i];

            DeferredDocEntity docEnt;
            docEnt.documentId = documentDbId.value();
            docEnt.entityText = sym.qualified_name ? std::string(sym.qualified_name)
                                                   : (sym.name ? std::string(sym.name) : "");
            docEnt.nodeKey = versionNodeKeys[i];
            docEnt.startOffset = sym.start_offset;
            docEnt.endOffset = sym.end_offset;
            docEnt.confidence = 1.0f;
            docEnt.extractor = "symbol_extractor_v1";
            batch->deferredDocEntities.push_back(std::move(docEnt));
        }
    }

    // === Enqueue and wait ===
    try {
        auto future = kgQueue->enqueue(std::move(batch));

        // Wait for the batch to be committed (with timeout)
        auto status = future.wait_for(std::chrono::seconds(60));
        if (status == std::future_status::timeout) {
            spdlog::warn("EntityGraphService: KG write queue timeout for {}", job.filePath);
            return false;
        }

        auto commitResult = future.get();
        if (!commitResult) {
            spdlog::warn("EntityGraphService: KG write queue failed for {}: {}", job.filePath,
                         commitResult.error().message);
            if (commitResult.error().message.find("database is locked") != std::string::npos) {
                TuneAdvisor::reportDbLockError();
            }
            return false;
        }

        spdlog::debug("EntityGraphService: queued KG batch with {} symbols from {}",
                      result->symbol_count, job.filePath);

        // Generate entity embeddings (runs after batch committed, non-blocking)
        // Note: symbolNodes needed for embeddings but we don't have IDs in deferred path
        // Skip embeddings in deferred path for now - can be added via separate embedding queue
        return true;

    } catch (const std::exception& e) {
        spdlog::warn("EntityGraphService: exception queueing symbol KG batch: {}", e.what());
        return false;
    }
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

} // namespace yams::daemon
