#include <yams/daemon/components/GraphComponent.h>

#include <yams/api/content_store.h>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/metadata/kg_topology_analysis.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

namespace {

using DirectedNodePair = std::pair<std::int64_t, std::int64_t>;

struct DirectedNodePairHash {
    std::size_t operator()(const DirectedNodePair& value) const noexcept {
        std::size_t seed = std::hash<std::int64_t>{}(value.first);
        seed ^= std::hash<std::int64_t>{}(value.second) + 0x9e3779b9 + (seed << 6U) + (seed >> 2U);
        return seed;
    }
};

Result<std::unordered_map<DirectedNodePair, std::int64_t, DirectedNodePairHash>>
collectDirectedSemanticNeighborEdges(metadata::KnowledgeGraphStore* kgStore,
                                     std::size_t batchSize) {
    if (!kgStore) {
        return Error{ErrorCode::InvalidArgument, "KnowledgeGraphStore is required"};
    }
    (void)batchSize;

    auto docsResult = kgStore->findNodesByType("document", 1'000'000, 0);
    if (!docsResult) {
        return docsResult.error();
    }

    std::unordered_set<std::int64_t> documentIds;
    documentIds.reserve(docsResult.value().size());
    for (const auto& doc : docsResult.value()) {
        documentIds.insert(doc.id);
    }

    std::unordered_map<DirectedNodePair, std::int64_t, DirectedNodePairHash> edgeIdsByPair;
    edgeIdsByPair.reserve(documentIds.size() * 4);

    auto streamResult = kgStore->forEachEdgeByRelation(
        std::string_view("semantic_neighbor"),
        [&](std::int64_t edgeId, std::int64_t srcNodeId, std::int64_t dstNodeId, float) {
            if (!documentIds.contains(srcNodeId)) {
                return true;
            }
            if (srcNodeId != dstNodeId && !documentIds.contains(dstNodeId)) {
                return true;
            }
            edgeIdsByPair.emplace(DirectedNodePair{srcNodeId, dstNodeId}, edgeId);
            return true;
        });
    if (!streamResult) {
        return streamResult.error();
    }

    return edgeIdsByPair;
}

bool shouldPruneSemanticTopology(const metadata::KGTopologySummary& summary) {
    if (summary.unreciprocatedSemanticEdgeCount == 0) {
        return false;
    }
    if (summary.semanticEdgeCount >= 3 && summary.reciprocalSemanticEdgeCount == 0) {
        return true;
    }
    return summary.semanticEdgeCount >= 4 && summary.semanticReciprocity < 0.5;
}

} // namespace

GraphComponent::GraphComponent(std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                               std::shared_ptr<metadata::KnowledgeGraphStore> kgStore,
                               ServiceManager* serviceManager)
    : metadataRepo_(std::move(metadataRepo)), kgStore_(std::move(kgStore)),
      serviceManager_(serviceManager) {}

GraphComponent::~GraphComponent() {
    shutdown();
}

Result<void> GraphComponent::initialize() {
    if (initialized_) {
        return Result<void>();
    }

    if (!metadataRepo_ || !kgStore_) {
        return Error{ErrorCode::InvalidArgument, "GraphComponent: missing required dependencies"};
    }

    queryService_ = app::services::makeGraphQueryService(kgStore_, metadataRepo_);
    if (!queryService_) {
        return Error{ErrorCode::InternalError, "GraphComponent: failed to create query service"};
    }

    if (serviceManager_) {
        try {
            entityService_ = std::make_shared<EntityGraphService>(serviceManager_);
            entityService_->start();
            spdlog::info("[GraphComponent] EntityGraphService initialized");
        } catch (const std::exception& e) {
            spdlog::warn("[GraphComponent] Failed to initialize EntityGraphService: {}", e.what());
        }
    }

    initialized_ = true;
    spdlog::info("[GraphComponent] Initialized successfully");
    return Result<void>();
}

void GraphComponent::shutdown() {
    if (!initialized_) {
        return;
    }

    if (entityService_) {
        entityService_->stop();
        entityService_.reset();
    }

    queryService_.reset();
    initialized_ = false;
    spdlog::info("[GraphComponent] Shutdown complete");
}

bool GraphComponent::isReady() const {
    return initialized_ && queryService_ != nullptr;
}

Result<void> GraphComponent::onDocumentIngested(const DocumentGraphContext& ctx) {
    if (!initialized_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    // Skip entity extraction if requested or if no entity service available
    if (ctx.skipEntityExtraction || !entityService_) {
        spdlog::debug("[GraphComponent] Entity extraction skipped for {}",
                      ctx.documentHash.substr(0, 12));
        return Result<void>();
    }

    // Need ServiceManager to access content store and symbol extractors
    if (!serviceManager_) {
        spdlog::debug("[GraphComponent] No service manager, skipping extraction");
        return Result<void>();
    }

    // Get content store to load document content
    auto contentStore = serviceManager_->getContentStore();
    if (!contentStore) {
        spdlog::debug("[GraphComponent] No content store available");
        return Result<void>();
    }

    // Detect language from file extension
    std::string language;
    if (!ctx.filePath.empty()) {
        std::filesystem::path path(ctx.filePath);
        std::string ext = path.extension().string();
        if (!ext.empty() && ext[0] == '.') {
            ext = ext.substr(1);
        }

        // Query symbol extractors for language mapping
        const auto& extractors = serviceManager_->getSymbolExtractors();
        for (const auto& extractor : extractors) {
            if (!extractor)
                continue;
            auto supported = extractor->getSupportedExtensions();
            auto it = supported.find(ext);
            if (it != supported.end()) {
                language = it->second;
                break;
            }
        }
    }

    // Check if NL entity extractors support this file type (for non-code files)
    bool hasNlExtractor = false;
    std::string contentType;
    if (language.empty() && !ctx.filePath.empty()) {
        std::filesystem::path path(ctx.filePath);
        std::string ext = path.extension().string();

        // Map extension to content type for NL extractors
        if (ext == ".md" || ext == ".markdown") {
            contentType = "text/markdown";
        } else if (ext == ".json" || ext == ".jsonl") {
            contentType = "application/json";
        } else if (ext == ".txt" || ext.empty()) {
            contentType = "text/plain";
        }

        // Check if any NL entity extractor supports this content type
        if (!contentType.empty()) {
            const auto& nlExtractors = serviceManager_->getEntityExtractors();
            for (const auto& ex : nlExtractors) {
                if (ex && ex->supportsContentType(contentType)) {
                    hasNlExtractor = true;
                    break;
                }
            }
        }
    }

    // Skip if no language detected AND no NL extractor supports the content type
    if (language.empty() && !hasNlExtractor) {
        spdlog::debug(
            "[GraphComponent] No language/extractor for {} (ext='{}'), skipping extraction",
            ctx.filePath,
            ctx.filePath.empty() ? "" : std::filesystem::path(ctx.filePath).extension().string());
        return Result<void>();
    }

    std::vector<std::byte> bytes;
    if (ctx.contentBytes) {
        bytes = *ctx.contentBytes;
    } else {
        // Load document content
        auto contentResult = contentStore->retrieveBytes(ctx.documentHash);
        if (!contentResult) {
            spdlog::warn("[GraphComponent] Failed to load content for {}: {}",
                         ctx.documentHash.substr(0, 12), contentResult.error().message);
            return Result<void>(); // Non-fatal, continue
        }
        bytes = std::move(contentResult.value());
    }

    // Convert bytes to UTF-8 string
    std::string contentUtf8(reinterpret_cast<const char*>(bytes.data()), bytes.size());

    // Submit for entity extraction
    EntityExtractionJob job;
    job.documentHash = ctx.documentHash;
    job.filePath = ctx.filePath;
    job.contentUtf8 = std::move(contentUtf8);
    job.language = language; // Keep copy for logging

    auto submitResult = submitEntityExtraction(std::move(job));
    if (!submitResult) {
        spdlog::warn("[GraphComponent] Failed to submit extraction for {}: {}",
                     ctx.documentHash.substr(0, 12), submitResult.error().message);
    } else {
        spdlog::info("[GraphComponent] Queued entity extraction for {} ({}) lang={} nl={}",
                     ctx.filePath, ctx.documentHash.substr(0, 12),
                     language.empty() ? "(none)" : language, hasNlExtractor ? "yes" : "no");
    }

    return Result<void>();
}

Result<void> GraphComponent::onDocumentsIngestedBatch(std::vector<DocumentGraphContext>& contexts) {
    if (!initialized_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    if (contexts.empty()) {
        return Result<void>();
    }

    auto startTime = std::chrono::steady_clock::now();
    std::size_t skipped = 0;

    // Collect all extraction jobs for batch submission
    std::vector<EntityExtractionJob> extractionJobs;
    extractionJobs.reserve(contexts.size());

    for (auto& ctx : contexts) {
        // Skip if entity extraction disabled or no service
        if (ctx.skipEntityExtraction || !entityService_) {
            skipped++;
            continue;
        }

        // Need ServiceManager to access content store
        if (!serviceManager_) {
            skipped++;
            continue;
        }

        // Get content store to load document content
        auto contentStore = serviceManager_->getContentStore();
        if (!contentStore) {
            skipped++;
            continue;
        }

        // Detect language from file extension
        std::string language;
        if (!ctx.filePath.empty()) {
            std::filesystem::path path(ctx.filePath);
            std::string ext = path.extension().string();
            if (!ext.empty() && ext[0] == '.') {
                ext = ext.substr(1);
            }

            // Query symbol extractors for language mapping
            const auto& extractors = serviceManager_->getSymbolExtractors();
            for (const auto& extractor : extractors) {
                if (!extractor)
                    continue;
                auto supported = extractor->getSupportedExtensions();
                auto it = supported.find(ext);
                if (it != supported.end()) {
                    language = it->second;
                    break;
                }
            }
        }

        // Check if NL entity extractors support this file type
        bool hasNlExtractor = false;
        std::string contentType;
        if (language.empty() && !ctx.filePath.empty()) {
            std::filesystem::path path(ctx.filePath);
            std::string ext = path.extension().string();

            // Map extension to content type for NL extractors
            if (ext == ".md" || ext == ".markdown") {
                contentType = "text/markdown";
            } else if (ext == ".json" || ext == ".jsonl") {
                contentType = "application/json";
            } else if (ext == ".txt" || ext.empty()) {
                contentType = "text/plain";
            }

            // Check if any NL entity extractor supports this content type
            if (!contentType.empty()) {
                const auto& nlExtractors = serviceManager_->getEntityExtractors();
                for (const auto& ex : nlExtractors) {
                    if (ex && ex->supportsContentType(contentType)) {
                        hasNlExtractor = true;
                        break;
                    }
                }
            }
        }

        // Skip if no language detected AND no NL extractor supports the content type
        if (language.empty() && !hasNlExtractor) {
            spdlog::debug(
                "[GraphComponent] No language/extractor for {} (ext='{}'), skipping extraction",
                ctx.filePath,
                ctx.filePath.empty() ? ""
                                     : std::filesystem::path(ctx.filePath).extension().string());
            skipped++;
            continue;
        }

        std::vector<std::byte> bytes;
        if (ctx.contentBytes) {
            bytes = *ctx.contentBytes;
        } else {
            // Load document content
            auto contentResult = contentStore->retrieveBytes(ctx.documentHash);
            if (!contentResult) {
                spdlog::warn("[GraphComponent] Failed to load content for {}: {}",
                             ctx.documentHash.substr(0, 12), contentResult.error().message);
                skipped++;
                continue;
            }
            bytes = std::move(contentResult.value());
        }

        // Convert bytes to UTF-8 string
        std::string contentUtf8(reinterpret_cast<const char*>(bytes.data()), bytes.size());

        // Build extraction job
        EntityExtractionJob job;
        job.documentHash = std::move(ctx.documentHash);
        job.filePath = std::move(ctx.filePath);
        job.contentUtf8 = std::move(contentUtf8);
        job.language = std::move(language);

        extractionJobs.push_back(std::move(job));
    }

    // Submit all extraction jobs in batch
    if (!extractionJobs.empty()) {
        for (auto& job : extractionJobs) {
            auto submitResult = submitEntityExtraction(std::move(job));
            if (!submitResult) {
                spdlog::warn("[GraphComponent] Failed to submit extraction for batch job: {}",
                             submitResult.error().message);
            }
        }
    }

    auto duration = std::chrono::steady_clock::now() - startTime;
    double ms = std::chrono::duration<double, std::milli>(duration).count();

    spdlog::debug("[GraphComponent] Batch ingested {} contexts ({} jobs, {} skipped) in {:.2f}ms",
                  contexts.size(), extractionJobs.size(), skipped, ms);

    return Result<void>();
}

Result<void>
GraphComponent::onTreeDiffApplied(int64_t diffId,
                                  const std::vector<metadata::TreeChangeRecord>& changes) {
    if (!initialized_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    (void)diffId;
    (void)changes;

    return Result<void>();
}

bool GraphComponent::shouldSkipEntityExtraction(
    const std::shared_ptr<metadata::KnowledgeGraphStore>& kg, const std::string& documentHash,
    const std::string& expectedExtractorId) {
    if (!kg || documentHash.empty()) {
        return false;
    }

    // Check the new extraction state table first
    auto stateRes = kg->getSymbolExtractionState(documentHash);
    if (stateRes.has_value() && stateRes.value().has_value()) {
        const auto& state = stateRes.value().value();
        // Skip if extraction completed successfully
        if (state.status == "complete") {
            // If we have an expected extractor ID, only skip if it matches
            if (!expectedExtractorId.empty() && state.extractorId != expectedExtractorId) {
                spdlog::debug(
                    "[GraphComponent] Extractor version changed: {} -> {}, will re-extract",
                    state.extractorId, expectedExtractorId);
                return false; // Version mismatch, need to re-extract
            }
            return true; // Already extracted with matching or any version
        }
    }

    // Fallback: check kg_doc_entities for backward compatibility with existing data
    // This handles databases that were populated before the state table existed
    auto docIdRes = kg->getDocumentIdByHash(documentHash);
    if (!docIdRes.has_value() || !docIdRes.value().has_value()) {
        return false;
    }

    auto entRes = kg->getDocEntitiesForDocument(docIdRes.value().value(), 1, 0);
    return entRes.has_value() && !entRes.value().empty();
}

Result<void> GraphComponent::submitEntityExtraction(EntityExtractionJob job) {
    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    if (!entityService_) {
        return Error{ErrorCode::NotSupported, "EntityGraphService not available"};
    }

    const std::string expectedExtractorId = resolveSymbolExtractorIdForLanguage(job.language);

    if (shouldSkipEntityExtraction(kgStore_, job.documentHash, expectedExtractorId)) {
        spdlog::debug("[GraphComponent] Skip entity extraction for {} (already extracted)",
                      job.documentHash.substr(0, 12));
        return Result<void>();
    }

    EntityGraphService::Job entityJob{
        .documentHash = std::move(job.documentHash),
        .filePath = std::move(job.filePath),
        .contentUtf8 = std::move(job.contentUtf8),
        .language = std::move(job.language),
        .mimeType = {},
    };

    return entityService_->submitExtraction(std::move(entityJob));
}

std::string GraphComponent::resolveSymbolExtractorIdForLanguage(const std::string& language) const {
    if (!serviceManager_ || language.empty()) {
        return {};
    }

    const auto& extractors = serviceManager_->getSymbolExtractors();
    for (const auto& extractor : extractors) {
        if (extractor && extractor->supportsLanguage(language)) {
            return extractor->getExtractorId();
        }
    }
    return {};
}

Result<GraphComponent::RepairStats> GraphComponent::repairGraph(bool dryRun,
                                                                RepairProgressFn progress) {
    if (!initialized_ || !metadataRepo_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    RepairStats stats;
    constexpr int kBatchSize = 500;
    constexpr uint64_t kProgressStride = 100;
    uint64_t processed = 0;
    uint64_t nextProgressAt = kProgressStride;
    const auto totalDocsRes = metadataRepo_->getDocumentCount();
    const uint64_t totalDocs = totalDocsRes ? static_cast<uint64_t>(totalDocsRes.value()) : 0ULL;
    const auto emit = [&](uint64_t done) {
        if (progress)
            progress(done, totalDocs, stats);
    };
    emit(0);

    auto runWithHeartbeat = [&](auto&& fn) {
        std::mutex hbMu;
        std::condition_variable hbCv;
        bool hbDone = false;
        std::thread hbThread([&]() {
            std::unique_lock<std::mutex> lk(hbMu);
            while (!hbDone) {
                if (hbCv.wait_for(lk, std::chrono::seconds(30), [&] { return hbDone; }))
                    break;
                emit(processed);
            }
        });
        auto result = fn();
        {
            std::lock_guard<std::mutex> lk(hbMu);
            hbDone = true;
        }
        hbCv.notify_all();
        hbThread.join();
        return result;
    };

    if (!dryRun) {
        emit(processed);
        auto orphanEdgesRes = runWithHeartbeat([&] { return kgStore_->deleteOrphanedEdges(); });
        if (!orphanEdgesRes) {
            ++stats.errors;
            stats.issues.push_back("deleteOrphanedEdges failed: " + orphanEdgesRes.error().message);
        } else if (orphanEdgesRes.value() > 0) {
            stats.issues.push_back("removed orphaned edges: " +
                                   std::to_string(orphanEdgesRes.value()));
        }

        emit(processed);
        auto orphanEntitiesRes =
            runWithHeartbeat([&] { return kgStore_->deleteOrphanedDocEntities(); });
        if (!orphanEntitiesRes) {
            ++stats.errors;
            stats.issues.push_back("deleteOrphanedDocEntities failed: " +
                                   orphanEntitiesRes.error().message);
        } else if (orphanEntitiesRes.value() > 0) {
            stats.issues.push_back("removed orphaned doc_entities: " +
                                   std::to_string(orphanEntitiesRes.value()));
        }
    }

    std::size_t offset = 0;
    while (true) {
        metadata::DocumentQueryOptions opts;
        opts.limit = kBatchSize;
        opts.offset = static_cast<int>(offset);

        auto docsRes = metadataRepo_->queryDocuments(opts);
        if (!docsRes) {
            ++stats.errors;
            stats.issues.push_back("queryDocuments failed: " + docsRes.error().message);
            break;
        }

        const auto& docs = docsRes.value();
        if (docs.empty()) {
            break;
        }

        emit(processed);

        std::vector<int64_t> docIds;
        docIds.reserve(docs.size());
        for (const auto& d : docs) {
            if (d.id > 0)
                docIds.push_back(d.id);
        }

        auto tagsRes = metadataRepo_->batchGetDocumentTags(docIds);
        if (!tagsRes) {
            ++stats.errors;
            stats.issues.push_back("batchGetDocumentTags failed: " + tagsRes.error().message);
        }
        const auto tagsByDocId =
            tagsRes ? tagsRes.value() : std::unordered_map<int64_t, std::vector<std::string>>{};

        auto batchRes = kgStore_->beginWriteBatch();
        if (!batchRes) {
            ++stats.errors;
            stats.issues.push_back("beginWriteBatch failed: " + batchRes.error().message);
            break;
        }
        auto batch = std::move(batchRes).value();

        // Collect unique nodes for this batch.
        std::vector<metadata::KGNode> nodes;
        std::vector<std::string> nodeKeys;
        nodes.reserve(docs.size() * 3);
        nodeKeys.reserve(docs.size() * 3);

        std::unordered_map<std::string, std::size_t> nodeIndex;
        nodeIndex.reserve(docs.size() * 3);

        auto addNode = [&](metadata::KGNode node) {
            auto it = nodeIndex.find(node.nodeKey);
            if (it != nodeIndex.end()) {
                return;
            }
            nodeIndex.emplace(node.nodeKey, nodes.size());
            nodeKeys.push_back(node.nodeKey);
            nodes.push_back(std::move(node));
        };

        auto makeShortHashLabel = [](const std::string& hash) -> std::string {
            if (hash.size() <= 16)
                return hash;
            return hash.substr(0, 16) + "...";
        };

        for (const auto& d : docs) {
            if (d.sha256Hash.empty()) {
                ++stats.errors;
                stats.issues.push_back(
                    "Document missing sha256Hash (document id=" + std::to_string(d.id) + ")");
                continue;
            }

            // Normalize paths to match metadata conventions.
            std::string normalizedPath;
            std::string parentPath;
            if (!d.filePath.empty()) {
                try {
                    auto derived = metadata::computePathDerivedValues(d.filePath);
                    normalizedPath = derived.normalizedPath;
                    parentPath = derived.pathPrefix;
                } catch (...) {
                    normalizedPath = d.filePath;
                    parentPath = std::filesystem::path(d.filePath).parent_path().generic_string();
                }
            }

            // blob:<hash>
            {
                metadata::KGNode blob;
                blob.nodeKey = "blob:" + d.sha256Hash;
                blob.label = makeShortHashLabel(d.sha256Hash);
                blob.type = "blob";
                addNode(std::move(blob));
            }

            // doc:<hash>
            {
                metadata::KGNode doc;
                doc.nodeKey = "doc:" + d.sha256Hash;
                if (!normalizedPath.empty()) {
                    doc.label = normalizedPath;
                } else if (!d.fileName.empty()) {
                    doc.label = d.fileName;
                } else {
                    doc.label = makeShortHashLabel(d.sha256Hash);
                }
                doc.type = "document";
                addNode(std::move(doc));
            }

            // path:file:<path> and path:dir:<parent>
            if (!normalizedPath.empty()) {
                metadata::KGNode file;
                file.nodeKey = "path:file:" + normalizedPath;
                file.label = normalizedPath;
                file.type = "file";
                addNode(std::move(file));

                if (!parentPath.empty()) {
                    metadata::KGNode dir;
                    dir.nodeKey = "path:dir:" + parentPath;
                    dir.label = parentPath;
                    dir.type = "directory";
                    addNode(std::move(dir));
                }
            }

            // tag:<name>
            auto tagIt = tagsByDocId.find(d.id);
            if (tagIt != tagsByDocId.end()) {
                for (const auto& t : tagIt->second) {
                    if (t.empty())
                        continue;
                    metadata::KGNode tag;
                    tag.nodeKey = "tag:" + t;
                    tag.label = t;
                    tag.type = "tag";
                    addNode(std::move(tag));
                }
            }
        }

        auto idsRes = batch->upsertNodes(nodes);
        if (!idsRes) {
            ++stats.errors;
            stats.issues.push_back("upsertNodes failed: " + idsRes.error().message);
            // Let WriteBatch destructor rollback.
            break;
        }
        const auto& ids = idsRes.value();
        stats.nodesCreated += static_cast<uint64_t>(ids.size()); // attempted upserts
        emit(processed);

        std::unordered_map<std::string, std::int64_t> idByKey;
        idByKey.reserve(ids.size());
        for (std::size_t i = 0; i < ids.size(); ++i) {
            idByKey.emplace(nodeKeys[i], ids[i]);
        }

        std::vector<metadata::KGEdge> edges;
        edges.reserve(docs.size() * 4);

        for (const auto& d : docs) {
            if (d.sha256Hash.empty())
                continue;

            const std::string blobKey = "blob:" + d.sha256Hash;
            const std::string docKey = "doc:" + d.sha256Hash;

            auto blobIdIt = idByKey.find(blobKey);
            auto docIdIt = idByKey.find(docKey);
            if (blobIdIt == idByKey.end() || docIdIt == idByKey.end())
                continue;

            const auto blobId = blobIdIt->second;
            const auto docNodeId = docIdIt->second;

            // doc -> blob (optional bridge)
            {
                metadata::KGEdge e;
                e.srcNodeId = docNodeId;
                e.dstNodeId = blobId;
                e.relation = "has_blob";
                edges.push_back(std::move(e));
            }

            std::string normalizedPath;
            std::string parentPath;
            if (!d.filePath.empty()) {
                try {
                    auto derived = metadata::computePathDerivedValues(d.filePath);
                    normalizedPath = derived.normalizedPath;
                    parentPath = derived.pathPrefix;
                } catch (...) {
                    normalizedPath = d.filePath;
                    parentPath = std::filesystem::path(d.filePath).parent_path().generic_string();
                }
            }

            if (!normalizedPath.empty()) {
                const std::string fileKey = "path:file:" + normalizedPath;
                auto fileIdIt = idByKey.find(fileKey);
                if (fileIdIt != idByKey.end()) {
                    const auto fileId = fileIdIt->second;

                    // file -> blob (has_version) for GraphQueryService hash resolution.
                    {
                        metadata::KGEdge e;
                        e.srcNodeId = fileId;
                        e.dstNodeId = blobId;
                        e.relation = "has_version";
                        edges.push_back(std::move(e));
                    }

                    // blob -> file (blob_at_path) helps traversals from blob.
                    {
                        metadata::KGEdge e;
                        e.srcNodeId = blobId;
                        e.dstNodeId = fileId;
                        e.relation = "blob_at_path";
                        edges.push_back(std::move(e));
                    }

                    if (!parentPath.empty()) {
                        const std::string dirKey = "path:dir:" + parentPath;
                        auto dirIdIt = idByKey.find(dirKey);
                        if (dirIdIt != idByKey.end()) {
                            metadata::KGEdge e;
                            e.srcNodeId = dirIdIt->second;
                            e.dstNodeId = fileId;
                            e.relation = "contains";
                            edges.push_back(std::move(e));
                        }
                    }
                }
            }

            // doc -> tag
            auto tagIt = tagsByDocId.find(d.id);
            if (tagIt != tagsByDocId.end()) {
                for (const auto& t : tagIt->second) {
                    if (t.empty())
                        continue;
                    const std::string tagKey = "tag:" + t;
                    auto tagIdIt = idByKey.find(tagKey);
                    if (tagIdIt == idByKey.end())
                        continue;
                    metadata::KGEdge e;
                    e.srcNodeId = docNodeId;
                    e.dstNodeId = tagIdIt->second;
                    e.relation = "has_tag";
                    edges.push_back(std::move(e));
                }
            }
        }

        auto edgesRes = batch->addEdgesUnique(edges);
        if (!edgesRes) {
            ++stats.errors;
            stats.issues.push_back("addEdgesUnique failed: " + edgesRes.error().message);
            break;
        }
        stats.edgesCreated += static_cast<uint64_t>(edges.size()); // attempted unique inserts
        emit(processed);

        if (!dryRun) {
            auto commitRes = batch->commit();
            if (!commitRes) {
                ++stats.errors;
                stats.issues.push_back("commit failed: " + commitRes.error().message);
                break;
            }
        }

        offset += docs.size();
        processed += docs.size();
        if (processed >= nextProgressAt) {
            emit(processed);
            while (nextProgressAt <= processed)
                nextProgressAt += kProgressStride;
        }
        if (static_cast<int>(docs.size()) < kBatchSize)
            break;
    }

    emit(processed);

    auto maintenanceResult = maintainSemanticTopology(dryRun);
    if (!maintenanceResult) {
        ++stats.errors;
        stats.issues.push_back("semantic topology maintenance failed: " +
                               maintenanceResult.error().message);
    } else {
        for (const auto& issue : maintenanceResult.value().issues) {
            stats.issues.push_back(issue);
        }
    }

    if (dryRun) {
        stats.issues.push_back(
            "dry-run: changes were rolled back (counts reflect attempted writes)");
    }
    return stats;
}

Result<GraphComponent::GraphHealthReport> GraphComponent::validateGraph() {
    if (!initialized_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    auto healthResult = kgStore_->healthCheck();
    if (!healthResult) {
        return Error{ErrorCode::InternalError,
                     "Health check failed: " + healthResult.error().message};
    }

    GraphHealthReport report;
    // Populate basic stats so CLI can show meaningful totals.
    // If these queries fail, validation still succeeds but reports the reason.
    {
        auto typeCounts = kgStore_->getNodeTypeCounts();
        if (typeCounts) {
            uint64_t total = 0;
            for (const auto& [type, count] : typeCounts.value()) {
                (void)type;
                total += static_cast<uint64_t>(count);
            }
            report.totalNodes = total;
        } else {
            report.issues.push_back("node type counts unavailable: " + typeCounts.error().message);
        }
    }
    {
        auto relCounts = kgStore_->getRelationTypeCounts();
        if (relCounts) {
            uint64_t total = 0;
            for (const auto& [relation, count] : relCounts.value()) {
                (void)relation;
                total += static_cast<uint64_t>(count);
            }
            report.totalEdges = total;
        } else {
            report.issues.push_back("relation type counts unavailable: " +
                                    relCounts.error().message);
        }
    }

    auto orphanDocs = kgStore_->findIsolatedNodes("document", "has_blob", 1024);
    if (orphanDocs) {
        report.orphanedNodes = static_cast<uint64_t>(orphanDocs.value().size());
        if (!orphanDocs.value().empty()) {
            report.issues.push_back("document nodes missing has_blob edges: " +
                                    std::to_string(orphanDocs.value().size()));
        }
    } else {
        report.issues.push_back("isolated document probe unavailable: " +
                                orphanDocs.error().message);
    }

    if (auto topology = metadata::analyzeDocumentTopology(kgStore_.get()); topology.has_value()) {
        report.topologyDocumentNodes = static_cast<uint64_t>(topology->documentNodeCount);
        report.topologySemanticEdges = static_cast<uint64_t>(topology->semanticEdgeCount);
        report.topologyReciprocalSemanticEdges =
            static_cast<uint64_t>(topology->reciprocalSemanticEdgeCount);
        report.topologyUnreciprocatedSemanticEdges =
            static_cast<uint64_t>(topology->unreciprocatedSemanticEdgeCount);
        report.topologyDocumentsWithNeighbors =
            static_cast<uint64_t>(topology->documentsWithSemanticNeighbors);
        report.topologyDocumentsWithReciprocalNeighbors =
            static_cast<uint64_t>(topology->documentsWithReciprocalNeighbors);
        report.topologyReciprocalCommunityCount =
            static_cast<uint64_t>(topology->reciprocalCommunityCount);
        report.topologyLargestReciprocalCommunity =
            static_cast<uint64_t>(topology->largestReciprocalCommunitySize);
        report.topologyIsolatedDocuments = static_cast<uint64_t>(topology->isolatedDocumentCount);
        report.topologyConnectedComponents =
            static_cast<uint64_t>(topology->connectedComponentCount);
        report.topologyLargestComponent = static_cast<uint64_t>(topology->largestComponentSize);

        if (topology->documentNodeCount > 0 && topology->documentsWithSemanticNeighbors == 0) {
            report.issues.push_back("semantic topology has zero connected document neighborhoods");
        }
        if (topology->documentNodeCount >= 8 && topology->largestComponentSize <= 1) {
            report.issues.push_back("semantic topology is fully fragmented into singletons");
        }
        if (topology->documentNodeCount >= 8 && topology->semanticCoverage < 0.25) {
            report.issues.push_back("semantic topology coverage below 25%");
        }
        if (topology->semanticEdgeCount >= 3 && topology->reciprocalSemanticEdgeCount == 0) {
            report.issues.push_back("semantic topology has no reciprocal document neighborhoods");
        }
        if (topology->semanticEdgeCount >= 6 && topology->semanticReciprocity < 0.5) {
            report.issues.push_back("semantic topology reciprocity below 50%");
        }
        if (topology->documentsWithReciprocalNeighbors >= 6 &&
            topology->largestReciprocalCommunitySize <= 2) {
            report.issues.push_back("semantic community scaffold is fragmented into tiny clusters");
        }
    } else {
        report.issues.push_back("topology analysis unavailable");
    }

    return report;
}

Result<GraphComponent::SemanticTopologyMaintenanceStats>
GraphComponent::maintainSemanticTopology(bool dryRun) {
    if (!initialized_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    bool expected = false;
    if (!semanticTopologyMaintenanceRunning_.compare_exchange_strong(expected, true,
                                                                     std::memory_order_acq_rel)) {
        SemanticTopologyMaintenanceStats skipped;
        skipped.skipped = true;
        skipped.issues.push_back("semantic topology maintenance already in progress");
        return skipped;
    }

    struct Guard {
        std::atomic<bool>& flag;
        ~Guard() { flag.store(false, std::memory_order_release); }
    } guard{semanticTopologyMaintenanceRunning_};

    auto topology = metadata::analyzeDocumentTopology(kgStore_.get(), 256);
    if (!topology) {
        return Error{ErrorCode::InternalError, "topology analysis unavailable"};
    }

    SemanticTopologyMaintenanceStats stats;
    stats.reciprocalCommunities = static_cast<uint64_t>(topology->reciprocalCommunityCount);
    stats.largestReciprocalCommunity =
        static_cast<uint64_t>(topology->largestReciprocalCommunitySize);

    if (!shouldPruneSemanticTopology(*topology)) {
        stats.skipped = true;
        stats.issues.push_back("semantic topology maintenance skipped: reciprocity healthy enough");
        return stats;
    }

    auto edgesByPairResult = collectDirectedSemanticNeighborEdges(kgStore_.get(), 256);
    if (!edgesByPairResult) {
        return Error{ErrorCode::InternalError, "failed to collect semantic_neighbor edges: " +
                                                   edgesByPairResult.error().message};
    }

    std::vector<std::int64_t> edgesToRemove;
    edgesToRemove.reserve(topology->unreciprocatedSemanticEdgeCount);
    for (const auto& [pair, edgeId] : edgesByPairResult.value()) {
        if (pair.first == pair.second) {
            edgesToRemove.push_back(edgeId);
            continue;
        }
        if (!edgesByPairResult.value().contains(DirectedNodePair{pair.second, pair.first})) {
            edgesToRemove.push_back(edgeId);
        }
    }

    if (edgesToRemove.empty()) {
        stats.skipped = true;
        stats.issues.push_back(
            "semantic topology maintenance skipped: no removable one-way edges found");
        return stats;
    }

    if (dryRun) {
        stats.issues.push_back("dry-run: would prune " + std::to_string(edgesToRemove.size()) +
                               " one-way semantic_neighbor edges");
        return stats;
    }

    for (const auto edgeId : edgesToRemove) {
        auto removeResult = kgStore_->removeEdgeById(edgeId);
        if (!removeResult) {
            return Error{ErrorCode::InternalError, "failed to prune semantic_neighbor edge " +
                                                       std::to_string(edgeId) + ": " +
                                                       removeResult.error().message};
        }
        ++stats.semanticEdgesPruned;
    }

    if (stats.semanticEdgesPruned > 0) {
        stats.issues.push_back("pruned " + std::to_string(stats.semanticEdgesPruned) +
                               " one-way semantic_neighbor edges");
    }

    auto updatedTopology = metadata::analyzeDocumentTopology(kgStore_.get(), 256);
    if (updatedTopology) {
        stats.reciprocalCommunities =
            static_cast<uint64_t>(updatedTopology->reciprocalCommunityCount);
        stats.largestReciprocalCommunity =
            static_cast<uint64_t>(updatedTopology->largestReciprocalCommunitySize);
    }

    return stats;
}

std::shared_ptr<app::services::IGraphQueryService> GraphComponent::getQueryService() const {
    return queryService_;
}

GraphComponent::EntityStats GraphComponent::getEntityStats() const {
    if (!entityService_) {
        return EntityStats{};
    }

    auto stats = entityService_->getStats();
    return EntityStats{
        .jobsAccepted = stats.accepted,
        .jobsProcessed = stats.processed,
        .jobsFailed = stats.failed,
    };
}

} // namespace yams::daemon
