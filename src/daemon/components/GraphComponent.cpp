#include <yams/daemon/components/GraphComponent.h>

#include <yams/api/content_store.h>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <unordered_map>
#include <unordered_set>

namespace yams::daemon {

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
    std::size_t processed = 0;
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
        processed++;
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

    if (shouldSkipEntityExtraction(kgStore_, job.documentHash)) {
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

Result<GraphComponent::RepairStats> GraphComponent::repairGraph(bool dryRun) {
    if (!initialized_ || !metadataRepo_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    RepairStats stats;
    constexpr int kBatchSize = 500;

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

            // file:<path> and dir:<parent>
            if (!normalizedPath.empty()) {
                metadata::KGNode file;
                file.nodeKey = "file:" + normalizedPath;
                file.label = normalizedPath;
                file.type = "file";
                addNode(std::move(file));

                if (!parentPath.empty()) {
                    metadata::KGNode dir;
                    dir.nodeKey = "dir:" + parentPath;
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
                const std::string fileKey = "file:" + normalizedPath;
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
                        const std::string dirKey = "dir:" + parentPath;
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

        if (!dryRun) {
            auto commitRes = batch->commit();
            if (!commitRes) {
                ++stats.errors;
                stats.issues.push_back("commit failed: " + commitRes.error().message);
                break;
            }
        }

        offset += docs.size();
        if (static_cast<int>(docs.size()) < kBatchSize)
            break;
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
    return report;
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
