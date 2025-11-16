/**
 * GraphQueryServiceComponent
 *
 * Daemon-owned wrapper for GraphQueryService that manages initialization
 * and provides clean access to graph connectivity queries for search scoring,
 * path history, and snapshot analysis.
 */
#pragma once

#include <memory>
#include <yams/core/types.h>

namespace yams::app::services {
class IGraphQueryService;
}

namespace yams::metadata {
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace yams::metadata

namespace yams::daemon {

/**
 * Component that owns and manages the GraphQueryService lifecycle.
 * Provides graph connectivity queries to search scoring and CLI operations.
 */
class GraphQueryServiceComponent {
public:
    /**
     * Construct component. Does not initialize until initialize() is called.
     *
     * @param metadataRepo Metadata repository (must remain valid for component lifetime)
     * @param kgStore Knowledge graph store (must remain valid for component lifetime)
     */
    explicit GraphQueryServiceComponent(
        std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
        std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore);

    ~GraphQueryServiceComponent();

    /**
     * Initialize the underlying GraphQueryService.
     * Currently no-op as GraphQueryService has no async initialization.
     */
    Result<void> initialize();

    /**
     * Shutdown and release resources.
     */
    void shutdown();

    /**
     * Get the underlying GraphQueryService interface.
     * Returns nullptr if not initialized or after shutdown.
     */
    std::shared_ptr<yams::app::services::IGraphQueryService> getService() const;

    /**
     * Check if service is ready for queries.
     */
    bool isReady() const;

private:
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<yams::app::services::IGraphQueryService> service_;
    bool initialized_{false};
};

} // namespace yams::daemon
