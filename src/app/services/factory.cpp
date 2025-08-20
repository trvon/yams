#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>

namespace yams::app::services {

// Compose a cohesive bundle of services using real implementations.
// - Search: provided by search_service.cpp (makeSearchService)
// - Grep:   provided by grep_service.cpp (makeGrepService)
// - Document: provided by document_service.cpp (makeDocumentService)
// - Restore: not yet implemented; returns nullptr for now to avoid stubs
ServiceBundle makeServices(const AppContext& ctx) {
    ServiceBundle bundle;

    // Use real implementations wired via their respective factory functions.
    bundle.search = makeSearchService(ctx);
    bundle.grep = makeGrepService(ctx);
    bundle.document = makeDocumentService(ctx);
    bundle.download = makeDownloadService(ctx);
    bundle.indexing = makeIndexingService(ctx);
    bundle.stats = makeStatsService(ctx);

    // Restore service is pending real implementation.
    // Once implemented, wire it here by uncommenting the line below.
    // bundle.restore  = makeRestoreService(ctx);
    bundle.restore = nullptr;

    return bundle;
}

} // namespace yams::app::services