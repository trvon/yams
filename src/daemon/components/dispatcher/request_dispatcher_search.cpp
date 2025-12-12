// Split from RequestDispatcher.cpp: search handler
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/dispatch_utils.hpp>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>

namespace yams::daemon {

boost::asio::awaitable<Response> RequestDispatcher::handleSearchRequest(const SearchRequest& req) {
    try {
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
        SearchResponse response;
        response.totalCount = serviceResp.total;
        response.elapsed = std::chrono::milliseconds(serviceResp.executionTimeMs);

        // Enforce limit on results (defense-in-depth)
        const size_t limit = req.limit > 0 ? req.limit : serviceResp.results.size();
        size_t count = 0;

        for (const auto& item : serviceResp.results) {
            if (count >= limit) break;
            SearchResult resultItem;
            resultItem.id = std::to_string(item.id);
            resultItem.title = item.title;
            resultItem.path = item.path;
            resultItem.score = item.score;
            resultItem.snippet = item.snippet;
            if (!item.hash.empty()) {
                resultItem.metadata["hash"] = item.hash;
            }
            if (!item.path.empty()) {
                resultItem.metadata["path"] = item.path;
            }
            if (!item.title.empty()) {
                resultItem.metadata["title"] = item.title;
            }
            response.results.push_back(std::move(resultItem));
            ++count;
        }
        std::stable_sort(
            response.results.begin(), response.results.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
        co_return response;
    } catch (const std::exception& e) {
        co_return ErrorResponse{ErrorCode::InternalError,
                                std::string("Search failed: ") + e.what()};
    }
}

} // namespace yams::daemon
