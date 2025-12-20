#include <yams/daemon/components/GraphComponent.h>

#include <yams/api/content_store.h>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/daemon/components/EntityGraphService.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/resource/abi_symbol_extractor_adapter.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>

#include <spdlog/spdlog.h>

#include <filesystem>
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

    // Skip if no entity service available
    if (!entityService_) {
        spdlog::debug("[GraphComponent] No entity service, skipping extraction for {}",
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

    // Skip if no language detected (not a supported source file)
    if (language.empty()) {
        // Only log at debug for non-code files, but trace why for debugging
        spdlog::debug(
            "[GraphComponent] No language detected for {} (ext='{}'), skipping extraction",
            ctx.filePath,
            ctx.filePath.empty() ? "" : std::filesystem::path(ctx.filePath).extension().string());
        return Result<void>();
    }

    // Load document content
    auto contentResult = contentStore->retrieveBytes(ctx.documentHash);
    if (!contentResult) {
        spdlog::warn("[GraphComponent] Failed to load content for {}: {}",
                     ctx.documentHash.substr(0, 12), contentResult.error().message);
        return Result<void>(); // Non-fatal, continue
    }

    // Convert bytes to UTF-8 string
    const auto& bytes = contentResult.value();
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
        spdlog::info("[GraphComponent] Queued symbol extraction for {} ({}) lang={}", ctx.filePath,
                     ctx.documentHash.substr(0, 12), language);
    }

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

Result<void> GraphComponent::submitEntityExtraction(EntityExtractionJob job) {
    if (!initialized_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    if (!entityService_) {
        return Error{ErrorCode::NotSupported, "EntityGraphService not available"};
    }

    EntityGraphService::Job entityJob{
        .documentHash = std::move(job.documentHash),
        .filePath = std::move(job.filePath),
        .contentUtf8 = std::move(job.contentUtf8),
        .language = std::move(job.language),
    };

    return entityService_->submitExtraction(std::move(entityJob));
}

Result<GraphComponent::RepairStats> GraphComponent::repairGraph(bool dryRun) {
    if (!initialized_ || !metadataRepo_ || !kgStore_) {
        return Error{ErrorCode::NotInitialized, "GraphComponent not initialized"};
    }

    (void)dryRun;

    return Error{ErrorCode::NotImplemented, "Graph repair not yet implemented"};
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
