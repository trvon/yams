#include <yams/app/services/factory.hpp>
#include <yams/app/services/graph_context_service.hpp>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/app/services/services.hpp>

namespace yams::app::services {

std::shared_ptr<IGraphQueryService> makeGraphQueryService(const AppContext& ctx) {
    if (!ctx.kgStore || !ctx.metadataRepo) {
        return nullptr;
    }
    return yams::app::services::makeGraphQueryService(ctx.kgStore, ctx.metadataRepo);
}

std::shared_ptr<IGraphContextService> makeGraphContextService(const AppContext& ctx) {
    if (!ctx.kgStore) {
        return nullptr;
    }
    return yams::app::services::makeGraphContextService(ctx.kgStore, ctx.metadataRepo);
}

} // namespace yams::app::services