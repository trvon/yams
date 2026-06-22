// Split from RequestDispatcher.cpp: search handler
#include <spdlog/spdlog.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <string_view>
#include <fmt/ranges.h>
#include <yams/app/services/services.hpp>
#include <yams/core/uuid.h>
#include <yams/daemon/components/admission_control.h>
#include <yams/daemon/components/dispatch_response.hpp>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/profiling.h>

namespace yams::daemon {

boost::asio::awaitable<Response> RequestDispatcher::handleSearchRequest(const SearchRequest& req) {
    YAMS_ZONE_SCOPED_N("handleSearchRequest");
    try {
        const auto requestStart = std::chrono::steady_clock::now();
        const std::string traceId = yams::core::generateUUID();
        const uint32_t searchCap = ResourceGovernor::instance().maxSearchConcurrency();
        auto searchGuard = co_await acquireSearchAdmission(searchCap);
        if (serviceManager_ != nullptr && !searchGuard) {
            co_return admission::makeBusyError("Search concurrency limit reached",
                                               admission::captureSnapshot(serviceManager_, state_));
        }

        spdlog::debug("[RequestDispatcher] Received SearchRequest with {} pathPatterns: {}",
                      req.pathPatterns.size(), req.pathPatterns.size());

        auto appContext = serviceManager_->getAppContext();
        appContext.workerExecutor = getWorkerExecutor();
        auto searchService = app::services::makeSearchService(appContext);

        app::services::SearchRequest serviceReq;
        serviceReq.query = req.query;
        serviceReq.limit = req.limit;
        serviceReq.fuzzy = req.fuzzy;
        serviceReq.similarity = static_cast<float>(req.similarity);
        serviceReq.hash = req.hashQuery;
        serviceReq.type = req.searchType.empty() ? "keyword" : req.searchType;
        serviceReq.verbose = req.verbose;
        serviceReq.literalText = req.literalText;
        serviceReq.showHash = req.showHash;
        serviceReq.pathsOnly = req.pathsOnly;
        serviceReq.jsonOutput = req.jsonOutput;
        serviceReq.showLineNumbers = req.showLineNumbers;
        serviceReq.beforeContext = req.beforeContext;
        serviceReq.afterContext = req.afterContext;
        serviceReq.context = req.context;
        serviceReq.pathPattern = req.pathPattern;
        serviceReq.pathPatterns = req.pathPatterns;
        serviceReq.tags = req.tags;
        serviceReq.matchAllTags = req.matchAllTags;
        serviceReq.extension = req.extension;
        serviceReq.mimeType = req.mimeType;
        serviceReq.fileType = req.fileType;
        serviceReq.textOnly = req.textOnly;
        serviceReq.binaryOnly = req.binaryOnly;
        serviceReq.createdAfter = req.createdAfter;
        serviceReq.createdBefore = req.createdBefore;
        serviceReq.modifiedAfter = req.modifiedAfter;
        serviceReq.modifiedBefore = req.modifiedBefore;
        serviceReq.indexedAfter = req.indexedAfter;
        serviceReq.indexedBefore = req.indexedBefore;
        serviceReq.useSession = req.useSession;
        serviceReq.sessionName = req.sessionName;
        serviceReq.globalSearch = req.globalSearch;
        if (!req.collection.empty()) {
            serviceReq.metadataFilters.emplace_back("collection", req.collection);
        }

        // NOLINTNEXTLINE(concurrency-mt-unsafe): read-only daemon compatibility env override.
        if (const char* disVec = std::getenv("YAMS_DISABLE_VECTOR");
            disVec && *disVec && std::string(disVec) != "0" && std::string(disVec) != "false") {
            serviceReq.type = "metadata";
            spdlog::debug("YAMS_DISABLE_VECTOR set; forcing metadata-only search.");
        }
        if (!state_->readiness.searchEngineReady.load()) {
            serviceReq.type = "metadata";
            spdlog::debug("Hybrid search engine not ready, falling back to metadata search.");
        }

        const auto serviceStart = std::chrono::steady_clock::now();
        auto result = co_await searchService->search(serviceReq);
        const auto serviceMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - serviceStart)
                                   .count();
        const auto serviceUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::steady_clock::now() - serviceStart)
                                   .count();
        if (!result) {
            co_return yams::daemon::dispatch::makeErrorResponse(result.error().code,
                                                                result.error().message);
        }
        const auto& serviceResp = result.value();

        const auto mapSortStart = std::chrono::steady_clock::now();
        const size_t limit =
            req.limit > 0 ? req.limit
                          : (req.pathsOnly ? serviceResp.paths.size() : serviceResp.results.size());
        auto results = req.pathsOnly
                           ? yams::daemon::dispatch::SearchResultMapper::mapPathsToSearchResults(
                                 serviceResp.paths, limit)
                           : yams::daemon::dispatch::SearchResultMapper::mapToSearchResults(
                                 serviceResp.results, limit);

        std::stable_sort(
            results.begin(), results.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
        const auto mapSortMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - mapSortStart)
                                   .count();
        const auto mapSortUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::steady_clock::now() - mapSortStart)
                                   .count();

        // Search is a read path. Do not append retrieval_served telemetry here:
        // it makes idle-ish retrieval mutate yams.db and grow the WAL for data
        // that is not needed to serve results or status diagnostics. Explicit
        // feedback APIs (MCP/tune) still write feedback_events when callers opt in.
        constexpr int64_t feedbackMs = 0;
        constexpr int64_t feedbackUs = 0;

        const auto responseStart = std::chrono::steady_clock::now();
        auto response = yams::daemon::dispatch::makeSearchResponse(
            serviceResp.total, std::chrono::milliseconds(serviceResp.executionTimeMs),
            std::move(results), traceId);

        const auto responseMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - responseStart)
                                    .count();
        const auto responseUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                    std::chrono::steady_clock::now() - responseStart)
                                    .count();
        const auto totalMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - requestStart)
                                 .count();
        const auto totalUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                 std::chrono::steady_clock::now() - requestStart)
                                 .count();

        response.queryInfo = serviceResp.queryInfo;
        response.searchStats.insert(serviceResp.searchStats.begin(), serviceResp.searchStats.end());
        for (const auto& [name, micros] : serviceResp.componentTimingMicros) {
            response.searchStats[std::string("timing_") + name + "_us"] = std::to_string(micros);
        }
        response.searchStats["phase_dispatch_service_ms"] = std::to_string(serviceMs);
        response.searchStats["phase_dispatch_service_us"] = std::to_string(serviceUs);
        response.searchStats["phase_dispatch_map_sort_ms"] = std::to_string(mapSortMs);
        response.searchStats["phase_dispatch_map_sort_us"] = std::to_string(mapSortUs);
        response.searchStats["phase_dispatch_feedback_ms"] = std::to_string(feedbackMs);
        response.searchStats["phase_dispatch_feedback_us"] = std::to_string(feedbackUs);
        response.searchStats["phase_dispatch_response_ms"] = std::to_string(responseMs);
        response.searchStats["phase_dispatch_response_us"] = std::to_string(responseUs);
        response.searchStats["phase_dispatch_total_ms"] = std::to_string(totalMs);
        response.searchStats["phase_dispatch_total_us"] = std::to_string(totalUs);
        co_return response;
    } catch (const std::exception& e) {
        co_return yams::daemon::dispatch::makeErrorResponse(
            ErrorCode::InternalError, std::string("Search failed: ") + e.what());
    }
}

} // namespace yams::daemon
