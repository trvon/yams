// Split from RequestDispatcher.cpp: search handler
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp>
#include <yams/core/uuid.h>
#include <yams/app/services/services.hpp>
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
        const std::string traceId = yams::core::generateUUID();
        if (serviceManager_) {
            serviceManager_->onSearchRequestQueued();
        }

        struct SearchRequestLifecycle {
            ServiceManager* sm{nullptr};
            bool started{false};
            ~SearchRequestLifecycle() {
                if (!sm)
                    return;
                if (started) {
                    sm->onSearchRequestFinished();
                } else {
                    sm->onSearchRequestRejected();
                }
            }
        } lifecycle{serviceManager_, false};

        const uint32_t searchCap = ResourceGovernor::instance().maxSearchConcurrency();
        if (serviceManager_ && !serviceManager_->tryStartSearchRequest(searchCap)) {
            co_return ErrorResponse{ErrorCode::ResourceExhausted,
                                    "Search concurrency limit reached; retry shortly"};
        }
        lifecycle.started = true;

        spdlog::debug("[RequestDispatcher] Received SearchRequest with {} pathPatterns: {}",
                      req.pathPatterns.size(),
                      fmt::format("{}", fmt::join(req.pathPatterns, ", ")));

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

        if (const char* disVec = std::getenv("YAMS_DISABLE_VECTOR");
            disVec && *disVec && std::string(disVec) != "0" && std::string(disVec) != "false") {
            serviceReq.type = "metadata";
            spdlog::debug("YAMS_DISABLE_VECTOR set; forcing metadata-only search.");
        }
        if (!state_->readiness.searchEngineReady.load()) {
            serviceReq.type = "metadata";
            spdlog::debug("Hybrid search engine not ready, falling back to metadata search.");
        }

        auto result = co_await searchService->search(serviceReq);
        if (!result) {
            co_return ErrorResponse{result.error().code, result.error().message};
        }
        const auto& serviceResp = result.value();

        const size_t limit = req.limit > 0 ? req.limit : serviceResp.results.size();
        auto results = yams::daemon::dispatch::SearchResultMapper::mapToSearchResults(
            serviceResp.results, limit);

        std::stable_sort(
            results.begin(), results.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

        if (appContext.metadataRepo) {
            metadata::FeedbackEvent event;
            event.eventId = yams::core::generateUUID();
            event.traceId = traceId;
            event.createdAt = std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::now());
            event.source = "daemon";
            event.eventType = "retrieval_served";

            nlohmann::json payload;
            payload["query"] = req.query;
            payload["search_type"] = serviceReq.type;
            payload["limit"] = req.limit;
            payload["total_count"] = serviceResp.total;
            payload["elapsed_ms"] = serviceResp.executionTimeMs;
            payload["session_name"] = req.sessionName;
            payload["use_session"] = req.useSession;
            payload["global_search"] = req.globalSearch;
            nlohmann::json servedIds = nlohmann::json::array();
            for (const auto& r : results) {
                servedIds.push_back(r.id);
            }
            payload["served_result_ids"] = std::move(servedIds);
            event.payloadJson = payload.dump();

            auto ins = appContext.metadataRepo->insertFeedbackEvent(event);
            if (!ins) {
                spdlog::warn(
                    "Failed to persist retrieval_served feedback event for trace_id={} : {}",
                    traceId, ins.error().message);
            }
        }

        co_return yams::daemon::dispatch::makeSearchResponse(
            serviceResp.total, std::chrono::milliseconds(serviceResp.executionTimeMs),
            std::move(results), traceId);
    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Search failed: ") + e.what()};
    }
}

} // namespace yams::daemon
