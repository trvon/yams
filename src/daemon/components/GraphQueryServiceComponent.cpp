#include <yams/daemon/components/GraphQueryServiceComponent.h>

#include <yams/app/services/graph_query_service.hpp>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>

#include <spdlog/spdlog.h>

namespace yams::daemon {

GraphQueryServiceComponent::GraphQueryServiceComponent(
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore)
    : metadataRepo_(std::move(metadataRepo)), kgStore_(std::move(kgStore)) {}

GraphQueryServiceComponent::~GraphQueryServiceComponent() {
    shutdown();
}

Result<void> GraphQueryServiceComponent::initialize() {
    if (initialized_) {
        return Result<void>();
    }

    if (!metadataRepo_ || !kgStore_) {
        return Error{ErrorCode::InvalidArgument,
                     "GraphQueryServiceComponent: missing required dependencies"};
    }

    // Create the service implementation
    service_ = yams::app::services::makeGraphQueryService(kgStore_, metadataRepo_);
    if (!service_) {
        return Error{ErrorCode::InternalError,
                     "GraphQueryServiceComponent: failed to create service"};
    }

    initialized_ = true;
    spdlog::info("[GraphQueryServiceComponent] Initialized successfully");
    return Result<void>();
}

void GraphQueryServiceComponent::shutdown() {
    if (!initialized_) {
        return;
    }

    service_.reset();
    initialized_ = false;
    spdlog::info("[GraphQueryServiceComponent] Shutdown complete");
}

std::shared_ptr<yams::app::services::IGraphQueryService>
GraphQueryServiceComponent::getService() const {
    if (!initialized_) {
        return nullptr;
    }
    return service_;
}

bool GraphQueryServiceComponent::isReady() const {
    return initialized_ && service_ != nullptr;
}

} // namespace yams::daemon
