#pragma once

/**
 * Service Factory: constructs shared application services for CLI and MCP.
 *
 * This header declares factories for individual service instances built from a
 * shared AppContext. Implementations
 * should live in the corresponding source files within src/app/services.
 *
 * The goal is strict parity between CLI commands and MCP tools by ensuring
 * both call the same service layer for core behavior (search, grep, document
 * operations), avoiding logic drift across front-ends.
 */

#include <memory>
#include <yams/app/services/graph_context_service.hpp>
#include <yams/app/services/graph_query_service.hpp>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>

namespace yams::app::services {

/**
 * Individual factories for consumers that only need a subset of services.
 * These return nullptr when required dependencies are not present.
 */
[[nodiscard]] std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IGrepService> makeGrepService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IDocumentService> makeDocumentService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IDownloadService> makeDownloadService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IIndexingService> makeIndexingService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IStatsService> makeStatsService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IGraphQueryService> makeGraphQueryService(const AppContext& ctx);
[[nodiscard]] std::shared_ptr<IGraphContextService> makeGraphContextService(const AppContext& ctx);

// Session service factory (shared across CLI/MCP)
// Declared in session_service.hpp; included above for consumers.

} // namespace yams::app::services
