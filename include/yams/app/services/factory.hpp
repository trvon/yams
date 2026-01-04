#pragma once

/**
 * Service Factory: constructs shared application services for CLI and MCP.
 *
 * This header declares a factory for creating a cohesive ServiceBundle and
 * individual service instances from a shared AppContext. Implementations
 * should live in the corresponding source files within src/app/services.
 *
 * The goal is strict parity between CLI commands and MCP tools by ensuring
 * both call the same service layer for core behavior (search, grep, document
 * operations, and restore), avoiding logic drift across front-ends.
 */

#include <memory>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>

namespace yams::app::services {

/**
 * Aggregated services for convenience. Most callers should prefer constructing
 * and using the whole bundle rather than instantiating services piecemeal.
 */
struct ServiceBundle {
    std::shared_ptr<ISearchService> search;
    std::shared_ptr<IGrepService> grep;
    std::shared_ptr<IDocumentService> document;
    std::shared_ptr<IRestoreService> restore;
    std::shared_ptr<IDownloadService> download;
    std::shared_ptr<IIndexingService> indexing;
    std::shared_ptr<IStatsService> stats;
    std::shared_ptr<IGraphQueryService> graph;

    // True if all services are non-null.
    [[nodiscard]] bool valid() const noexcept {
        return static_cast<bool>(search) && static_cast<bool>(grep) &&
               static_cast<bool>(document) && static_cast<bool>(restore) &&
               static_cast<bool>(download) && static_cast<bool>(indexing) &&
               static_cast<bool>(stats);
    }
};

/**
 * Construct the full bundle of services from the shared application context.
 *
 * Requirements:
 * - ctx.store and ctx.metadataRepo are typically required by most services.
 * - ctx.searchEngine is used by search implementations.
 *
 * Returns a ServiceBundle with all services constructed (or nullptr members if
 * a particular service cannot be created due to missing dependencies).
 */
[[nodiscard]] ServiceBundle makeServices(const AppContext& ctx);

/**
 * Individual factories for consumers that only need a subset of services.
 * These return nullptr when required dependencies are not present.
 */
[[nodiscard]] std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IGrepService> makeGrepService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IDocumentService> makeDocumentService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IRestoreService> makeRestoreService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IDownloadService> makeDownloadService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IIndexingService> makeIndexingService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IStatsService> makeStatsService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IGraphQueryService> makeGraphQueryService(const AppContext& ctx);

// Session service factory (shared across CLI/MCP)
// Declared in session_service.hpp; included above for consumers.

} // namespace yams::app::services
