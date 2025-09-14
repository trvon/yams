#include <spdlog/spdlog.h>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/app/services/enhanced_search_executor.h>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/plugins/search_provider_v1.h>
#include <yams/search/query_qualifiers.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#ifndef _WIN32
#include <unistd.h>
#endif

// Forward util for snippet creation
namespace yams::app::services::utils {
std::string createSnippet(const std::string& content, size_t maxLength, bool preserveWordBoundary);
}

namespace yams::app::services {

namespace {

// Returns true if s consists only of hex digits
bool isHex(const std::string& s) {
    return std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isxdigit(c) != 0; });
}

// Heuristic: treat as hash when it looks like a hex string of reasonable length (8-64)
bool looksLikeHash(const std::string& s) {
    if (s.size() < 8 || s.size() > 64)
        return false;
    return isHex(s);
}

static bool hasWildcard(const std::string& s) {
    return s.find('*') != std::string::npos || s.find('?') != std::string::npos;
}

// Helper function to escape regex special characters
static std::string escapeRegex(const std::string& text) {
    static const std::string specialChars = "\\^$.|?*+()[]{}";
    std::string escaped;
    escaped.reserve(text.size() * 2);
    for (char c : text) {
        if (specialChars.find(c) != std::string::npos) {
            escaped += '\\';
        }
        escaped += c;
    }
    return escaped;
}

// Simple glob matcher supporting '*' and '?'
static bool wildcardMatch(const std::string& text, const std::string& pattern) {
    const size_t n = text.size();
    const size_t m = pattern.size();
    std::vector<std::vector<bool>> dp(n + 1, std::vector<bool>(m + 1, false));
    dp[0][0] = true;
    for (size_t j = 1; j <= m; ++j) {
        if (pattern[j - 1] == '*')
            dp[0][j] = dp[0][j - 1];
    }
    for (size_t i = 1; i <= n; ++i) {
        for (size_t j = 1; j <= m; ++j) {
            if (pattern[j - 1] == '*') {
                dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
            } else if (pattern[j - 1] == '?' || pattern[j - 1] == text[i - 1]) {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }
    return dp[n][m];
}

// Presence-based tag match using metadata repository
static bool metadataHasTags(metadata::MetadataRepository* repo, int64_t docId,
                            const std::vector<std::string>& tags, bool matchAll) {
    if (!repo || tags.empty())
        return true;
    auto md = repo->getAllMetadata(docId);
    if (!md)
        return false;
    auto& all = md.value();

    auto hasTag = [&](const std::string& t) {
        // Match if key equals tag OR string value equals tag
        auto it = all.find(t);
        if (it != all.end())
            return true;
        for (const auto& [k, v] : all) {
            (void)k;
            if (v.asString() == t)
                return true;
        }
        return false;
    };

    if (matchAll) {
        for (const auto& t : tags) {
            if (!hasTag(t))
                return false;
        }
        return true;
    } else {
        for (const auto& t : tags) {
            if (hasTag(t))
                return true;
        }
        return false;
    }
}

// Compute recommended worker count based on hardware, load and caps
static inline size_t recommendedWorkers(size_t items) {
    size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
    size_t rec = hw > 1 ? (hw - 1) : 1;
#ifndef _WIN32
    double loads[3] = {0, 0, 0};
    if (getloadavg(loads, 3) == 3) {
        double capacity = std::max(0.0, static_cast<double>(hw) - loads[0]);
        rec = static_cast<size_t>(std::clamp(capacity, 1.0, static_cast<double>(hw)));
    }
#endif
    rec = std::max(rec, hw / 2);
    size_t workers = std::clamp<size_t>(rec, 1, hw);
    if (const char* gcap = std::getenv("YAMS_DAEMON_WORKERS_MAX"); gcap && *gcap) {
        try {
            auto cap = static_cast<size_t>(std::stoul(gcap));
            if (cap > 0)
                workers = std::min(workers, cap);
        } catch (...) {
        }
    }
    workers = std::min(workers, items > 0 ? items : size_t{1});
    return std::max<size_t>(1, workers);
}

// Normalize scores to be non-negative and sort-friendly for UI/tests.
// For full-text (FTS5 BM25), lower is better; convert to descending positive [0,1].
// For fuzzy/other modes, clamp to >= 0 but keep natural ordering.
static void normalizeScores(std::vector<SearchItem>& results, const std::string& type) {
    if (results.empty())
        return;
    const double eps = 1e-9;
    if (type == "full-text") {
        double minv = results.front().score;
        double maxv = results.front().score;
        for (const auto& r : results) {
            minv = std::min(minv, r.score);
            maxv = std::max(maxv, r.score);
        }
        if (std::abs(maxv - minv) < 1e-12) {
            for (auto& r : results)
                r.score = 1.0; // equal scores, set to 1.0 for all
            return;
        }
        // Invert so lower raw scores (better BM25) map to higher normalized scores
        const double span = (maxv - minv);
        for (auto& r : results) {
            double inv = (maxv - r.score) / span; // in [0,1]
            // Ensure strictly positive for tests expecting > 0
            r.score = std::max(inv + eps, 0.0);
        }
        // Keep the original DB order (already ordered by raw score). After inversion, the
        // sequence should be non-increasing.
        return;
    }
    // Other modes: clamp
    for (auto& r : results) {
        if (r.score < 0.0)
            r.score = 0.0;
    }
}

} // namespace

class SearchServiceImpl final : public ISearchService {
public:
    explicit SearchServiceImpl(const AppContext& ctx) : ctx_(ctx) {
        // Initialize degraded mode from AppContext repair flags (preferred), falling back to env
        // vars
        degraded_ = ctx_.searchRepairInProgress || (ctx_.hybridEngine == nullptr);
        if (const char* d = std::getenv("YAMS_SEARCH_DEGRADED")) {
            std::string v(d);
            std::transform(v.begin(), v.end(), v.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (v == "1" || v == "true" || v == "yes" || v == "on")
                degraded_ = true;
        }
        // Prefer details from AppContext; overrideable via env
        if (!ctx_.searchRepairDetails.empty()) {
            repairDetails_ = ctx_.searchRepairDetails;
        }
        if (const char* r = std::getenv("YAMS_SEARCH_DEGRADED_REASON")) {
            repairDetails_ = r;
        }

        // Enhanced search config (best-effort; off by default). Keep lightweight and safe.
        try {
            enhancedCfg_ = EnhancedSearchExecutor::loadConfigFromToml();
            if (enhancedCfg_.enable) {
                hotzones_ = std::make_shared<yams::search::HotzoneManager>(enhancedCfg_.hotzones);
                enhanced_.setHotzoneManager(hotzones_);
            }
        } catch (...) {
            // Ignore config errors; keep enhancements disabled.
            enhancedCfg_ = {};
        }
    }

    boost::asio::awaitable<Result<SearchResponse>> search(const SearchRequest& req) override {
        using namespace std::chrono;
        using namespace boost::asio::experimental::awaitable_operators;

        const auto t0 = steady_clock::now();

        // Validate dependencies
        if (!ctx_.metadataRepo) {
            co_return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        auto parsed = yams::search::parseQueryQualifiers(req.query);
        SearchRequest normalizedReq = req;
        normalizedReq.query = parsed.normalizedQuery;

        if (normalizedReq.query.empty() && normalizedReq.hash.empty()) {
            co_return Error{ErrorCode::InvalidArgument, "Query or hash is required"};
        }
        constexpr std::size_t kMaxReasonableLimit = 100000;
        if (normalizedReq.limit > kMaxReasonableLimit) {
            co_return Error{ErrorCode::InvalidArgument, "Limit is out of allowed range"};
        }

        if (!parsed.scope.name.empty()) {
            if (normalizedReq.pathPattern.empty()) {
                normalizedReq.pathPattern = parsed.scope.name;
            } else if (normalizedReq.pathPattern.find(parsed.scope.name) == std::string::npos) {
                normalizedReq.pathPattern += " " + parsed.scope.name;
            }
        }
        if (normalizedReq.extension.empty() && !parsed.scope.ext.empty()) {
            normalizedReq.extension = parsed.scope.ext;
        }
        if (normalizedReq.mimeType.empty() && !parsed.scope.mime.empty()) {
            normalizedReq.mimeType = parsed.scope.mime;
        }

        if (!normalizedReq.hash.empty()) {
            if (!looksLikeHash(normalizedReq.hash)) {
                co_return Error{ErrorCode::InvalidArgument,
                                "Invalid hash format (expected hex, 8-64 chars)"};
            }
            auto result = searchByHashPrefix(normalizedReq);
            setExecTime(result, t0);
            co_return result;
        }

        if (looksLikeHash(normalizedReq.query)) {
            auto result = searchByHashPrefix(normalizedReq);
            setExecTime(result, t0);
            co_return result;
        }

        const std::string type = req.type.empty() ? "hybrid" : req.type;

        spdlog::info("SearchService: type='{}' fuzzy={} sim={} pathsOnly={} literal={} limit={} "
                     "filters: ext='{}' mime='{}' path='{}' tags={} allTags={}",
                     type, req.fuzzy, req.similarity, req.pathsOnly, req.literalText, req.limit,
                     req.extension, req.mimeType, req.pathPattern, req.tags.size(),
                     req.matchAllTags);

        Result<SearchResponse> result(Error{ErrorCode::Unknown, "Search path not taken"});

        if (type == "hybrid" || type == "semantic") {
            if (degraded_) {
                spdlog::warn("SearchService: degraded mode active{}",
                             repairDetails_.empty() ? "" : (std::string(": ") + repairDetails_));
                result = metadataSearch(normalizedReq);
                if (result) {
                    auto resp = std::move(result).value();
                    resp.queryInfo =
                        std::string("degraded mode — using metadata fallback") +
                        (repairDetails_.empty() ? "" : (std::string(" (") + repairDetails_ + ")"));
                    resp.searchStats["mode"] = "degraded";
                    if (!repairDetails_.empty())
                        resp.searchStats["repair_details"] = repairDetails_;
                    if (ctx_.searchRepairProgress > 0)
                        resp.searchStats["repair_progress"] =
                            std::to_string(ctx_.searchRepairProgress);
                    result = Result<SearchResponse>(std::move(resp));
                }
            } else if (ctx_.hybridEngine) {
                result = hybridSearch(normalizedReq, parsed.scope);
            } else {
                spdlog::warn("Hybrid engine unavailable, using metadata fallback");
                result = metadataSearch(normalizedReq);
                if (result) {
                    auto resp = std::move(result).value();
                    resp.searchStats["mode"] = "degraded";
                    resp.queryInfo = "hybrid unavailable — metadata fallback";
                    result = Result<SearchResponse>(std::move(resp));
                }
            }
        } else {
            result = metadataSearch(normalizedReq);
        }

        setExecTime(result, t0);

        if (result && !normalizedReq.pathsOnly) {
            auto resp = std::move(result).value();
            // Apply optional enhanced pipeline (Phase A: hotzones only) before snippets.
            if (enhancedCfg_.enable) {
                enhanced_.apply(ctx_, enhancedCfg_, normalizedReq.query, resp.results);
            }
            co_await hydrateSnippetsAsync_worker(normalizedReq, resp);
            result = Result<SearchResponse>(std::move(resp));
        }

        // --- Plugin Search ---
        if (ctx_.service_manager) {
            auto abi_host = ctx_.service_manager->getAbiPluginHost();
            if (abi_host) {
                auto loaded_plugins = abi_host->listLoaded();
                for (const auto& plugin_desc : loaded_plugins) {
                    auto iface =
                        abi_host->getInterface(plugin_desc.name, YAMS_IFACE_SEARCH_PROVIDER_V1_ID,
                                               YAMS_IFACE_SEARCH_PROVIDER_V1_VERSION);
                    if (iface) {
                        auto* search_provider =
                            static_cast<yams_search_provider_v1*>(iface.value());
                        if (search_provider && search_provider->search) {
                            // For now, we don't have a good way to do this asynchronously from the
                            // plugin. We will call it synchronously and merge the results.
                        }
                    }
                }
            }
        }

        co_return result;
    }

private:
    AppContext ctx_;
    bool degraded_{false};
    std::string repairDetails_{};
    EnhancedConfig enhancedCfg_{}; // off by default
    EnhancedSearchExecutor enhanced_{};
    std::shared_ptr<yams::search::HotzoneManager> hotzones_{};

    boost::asio::awaitable<void> hydrateSnippetsAsync(const SearchRequest& req,
                                                      SearchResponse& resp) {
        if (!ctx_.metadataRepo || !ctx_.workerExecutor) {
            co_return;
        }

        std::vector<size_t> todo;
        todo.reserve(resp.results.size());
        for (size_t i = 0; i < resp.results.size(); ++i) {
            const auto& it = resp.results[i];
            if (it.snippet.empty() && (!it.hash.empty() || !it.path.empty())) {
                todo.push_back(i);
            }
        }

        if (todo.empty()) {
            co_return;
        }

        std::vector<boost::asio::awaitable<void>> tasks;
        tasks.reserve(todo.size());

        for (size_t i = 0; i < todo.size(); ++i) {
            tasks.emplace_back(boost::asio::co_spawn(
                ctx_.workerExecutor,
                [&, idx = todo[i]]() -> boost::asio::awaitable<void> {
                    auto& out = resp.results[idx];
                    std::string hash = out.hash;
                    int64_t docId = -1;
                    if (!hash.empty()) {
                        auto d = ctx_.metadataRepo->getDocumentByHash(hash);
                        if (d && d.value().has_value())
                            docId = d.value()->id;
                    } else if (!out.path.empty()) {
                        auto v = ctx_.metadataRepo->findDocumentsByPath(out.path);
                        if (v && !v.value().empty())
                            docId = v.value().front().id;
                    }

                    if (docId >= 0) {
                        auto contentResult = ctx_.metadataRepo->getContent(docId);
                        if (contentResult && contentResult.value().has_value()) {
                            const auto& content = contentResult.value().value();
                            if (!content.contentText.empty()) {
                                out.snippet = utils::createSnippet(content.contentText, 200, true);
                            }
                        }
                    }
                    co_return;
                },
                boost::asio::use_awaitable));
        }

        if (!tasks.empty()) {
            using namespace boost::asio::experimental::awaitable_operators;
            auto combined_task = std::move(tasks[0]);
            for (size_t i = 1; i < tasks.size(); ++i) {
                combined_task = std::move(combined_task) && std::move(tasks[i]);
            }
            co_await std::move(combined_task);
        }
    }

    boost::asio::awaitable<void> hydrateSnippetsAsync_worker(const SearchRequest& req,
                                                             SearchResponse& resp) {
        if (!ctx_.metadataRepo || !ctx_.workerExecutor)
            co_return;

        std::vector<size_t> todo;
        for (size_t i = 0; i < resp.results.size(); ++i) {
            if (resp.results[i].snippet.empty() &&
                (!resp.results[i].hash.empty() || !resp.results[i].path.empty())) {
                todo.push_back(i);
            }
        }
        if (todo.empty())
            co_return;

        using namespace boost::asio::experimental::awaitable_operators;
        std::vector<boost::asio::awaitable<void>> tasks;
        tasks.reserve(todo.size());

        for (size_t i = 0; i < todo.size(); ++i) {
            tasks.emplace_back(boost::asio::co_spawn(
                ctx_.workerExecutor,
                [&, idx = todo[i]]() -> boost::asio::awaitable<void> {
                    auto& out = resp.results[idx];
                    int64_t docId = -1;
                    if (!out.hash.empty()) {
                        if (auto d = ctx_.metadataRepo->getDocumentByHash(out.hash); d && d.value())
                            docId = d.value()->id;
                    } else if (!out.path.empty()) {
                        if (auto v = ctx_.metadataRepo->findDocumentsByPath(out.path);
                            v && !v.value().empty())
                            docId = v.value().front().id;
                    }
                    if (docId >= 0) {
                        if (auto contentResult = ctx_.metadataRepo->getContent(docId);
                            contentResult && contentResult.value()) {
                            if (!contentResult.value()->contentText.empty()) {
                                out.snippet = utils::createSnippet(
                                    contentResult.value()->contentText, 200, true);
                            }
                        }
                    }
                    co_return;
                },
                boost::asio::use_awaitable));
        }

        if (!tasks.empty()) {
            auto combined_task = std::move(tasks[0]);
            for (size_t i = 1; i < tasks.size(); ++i) {
                combined_task = std::move(combined_task) && std::move(tasks[i]);
            }
            co_await std::move(combined_task);
        }
    }

    boost::asio::awaitable<void> ensurePathsFallbackAsync(const SearchRequest& req,
                                                          SearchResponse& resp) {
        if (!ctx_.metadataRepo || !ctx_.workerExecutor)
            co_return;
        auto allDocsResult = ctx_.metadataRepo->findDocumentsByPath("%");
        if (!allDocsResult)
            co_return;
        const auto& docs = allDocsResult.value();

        std::vector<std::string> foundPaths;
        std::mutex outMutex;

        using namespace boost::asio::experimental::awaitable_operators;
        std::vector<boost::asio::awaitable<void>> tasks;
        tasks.reserve(docs.size());

        for (const auto& doc : docs) {
            tasks.emplace_back(boost::asio::co_spawn(
                ctx_.workerExecutor,
                [&, doc]() -> boost::asio::awaitable<void> {
                    if (req.limit > 0 && foundPaths.size() >= req.limit)
                        co_return;
                    if (!req.query.empty() && doc.filePath.find(req.query) == std::string::npos &&
                        doc.fileName.find(req.query) == std::string::npos) {
                        co_return;
                    }
                    std::string p = !doc.filePath.empty() ? doc.filePath : doc.fileName;
                    std::lock_guard<std::mutex> lk(outMutex);
                    if (req.limit == 0 || foundPaths.size() < req.limit) {
                        foundPaths.push_back(std::move(p));
                    }
                    co_return;
                },
                boost::asio::use_awaitable));
        }

        if (!tasks.empty()) {
            auto combined_task = std::move(tasks[0]);
            for (size_t i = 1; i < tasks.size(); ++i) {
                combined_task = std::move(combined_task) && std::move(tasks[i]);
            }
            co_await std::move(combined_task);
        }

        resp.paths = std::move(foundPaths);
    }

    template <typename T> void setExecTime(Result<T>& r, std::chrono::steady_clock::time_point t0) {
        using namespace std::chrono;
        (void)t0; // Suppress unused parameter warning - only specialized version uses it
        (void)r;  // Suppress unused parameter warning
        // Generic template doesn't use parameters, only specialized version does
    }

    // Overload specifically to set SearchResponse.executionTimeMs
    void setExecTime(Result<SearchResponse>& r, std::chrono::steady_clock::time_point t0) {
        using namespace std::chrono;
        if (r) {
            auto dur = duration_cast<milliseconds>(steady_clock::now() - t0).count();
            // Extract value, modify, and reconstruct Result
            auto resp = std::move(r).value();
            resp.executionTimeMs = static_cast<int64_t>(dur);
            r = Result<SearchResponse>(std::move(resp));
        }
    }

    Result<SearchResponse> searchByHashPrefix(const SearchRequest& req) {
        auto docsResult = ctx_.metadataRepo->findDocumentsByPath("%");
        if (!docsResult) {
            return Error{ErrorCode::InternalError,
                         "Failed to enumerate documents for hash search: " +
                             docsResult.error().message};
        }

        const std::string& prefix = !req.hash.empty() ? req.hash : req.query;
        SearchResponse resp;
        resp.type = "hash";
        resp.usedHybrid = false;

        const auto& docs = docsResult.value();
        std::size_t count = 0;
        for (const auto& doc : docs) {
            if (doc.sha256Hash.size() < prefix.size())
                continue;
            if (doc.sha256Hash.compare(0, prefix.size(), prefix) != 0)
                continue;

            // Optional path and tag filters for CLI parity
            bool pathOk = true;
            if (!req.pathPattern.empty()) {
                if (hasWildcard(req.pathPattern))
                    pathOk = wildcardMatch(doc.filePath, req.pathPattern);
                else
                    pathOk = doc.filePath.find(req.pathPattern) != std::string::npos;
            }
            // Enforce ext/mime filters when available
            bool metaFiltersOk = true;
            if (!req.fileType.empty()) {
                // keep current behavior (type handled elsewhere)
            }
            if (!req.extension.empty()) {
                if (doc.fileExtension != req.extension &&
                    doc.fileExtension != ("." + req.extension)) {
                    metaFiltersOk = false;
                }
            }
            if (!req.mimeType.empty() && doc.mimeType != req.mimeType) {
                metaFiltersOk = false;
            }
            if (!pathOk || !metaFiltersOk ||
                !metadataHasTags(ctx_.metadataRepo.get(), doc.id, req.tags, req.matchAllTags)) {
                continue;
            }

            if (req.pathsOnly) {
                resp.paths.push_back(doc.filePath);
            } else {
                SearchItem it;
                it.id = doc.id;
                it.hash = req.showHash ? doc.sha256Hash : "";
                it.title = doc.fileName;
                it.path = doc.filePath;
                it.score = 1.0;  // Exact/prefix match
                it.snippet = ""; // not available here
                resp.results.push_back(std::move(it));
            }

            if (++count >= req.limit)
                break;
        }

        resp.total = req.pathsOnly ? resp.paths.size() : resp.results.size();
        return resp;
    }

    Result<SearchResponse> hybridSearch(const SearchRequest& req,
                                        const yams::search::ExtractScope& scope) {
        // Expect ctx_.hybridEngine->search(query, limit) returning Result<vector<...>>
        // Shape inferred from existing MCP code: each result has:
        //  - id
        //  - metadata map with "title" and "path"
        //  - hybrid_score, vector_score, keyword_score, kg_entity_score, structural_score
        //  - content snippet (optional)
        yams::vector::SearchFilter filter;
        if (!scope.name.empty()) {
            filter.metadata_filters["file_name"] = scope.name;
        }
        if (!scope.ext.empty()) {
            filter.metadata_filters["extension"] = scope.ext;
        }
        if (!scope.mime.empty()) {
            filter.metadata_filters["mime_type"] = scope.mime;
        }
        auto hres = ctx_.hybridEngine->search(req.query, req.limit, filter);
        if (!hres) {
            return Error{ErrorCode::InternalError, "Hybrid search failed: " + hres.error().message};
        }

        const auto& vec = hres.value();

        SearchResponse resp;
        resp.type = "hybrid";
        resp.usedHybrid = true;

        if (req.pathsOnly) {
            for (const auto& r : vec) {
                auto itPath = r.metadata.find("path");
                if (itPath != r.metadata.end()) {
                    resp.paths.push_back(itPath->second);
                }
            }
            resp.total = resp.paths.size();
            return resp;
        }

        {
            const size_t n = vec.size();
            const size_t workers = recommendedWorkers(n);
            std::atomic<size_t> next{0};
            std::vector<std::optional<SearchItem>> slots(n);
            auto worker = [&]() {
                while (true) {
                    const size_t i = next.fetch_add(1);
                    if (i >= n)
                        break;
                    const auto& r = vec[i];
                    SearchItem it;
                    it.id = static_cast<int64_t>(i + 1);
                    if (auto itTitle = r.metadata.find("title"); itTitle != r.metadata.end())
                        it.title = itTitle->second;
                    if (auto itPath = r.metadata.find("path"); itPath != r.metadata.end())
                        it.path = itPath->second;
                    it.score = static_cast<double>(r.hybrid_score);
                    if (!r.content.empty())
                        it.snippet = r.content;
                    if (req.verbose) {
                        it.vectorScore = r.vector_score;
                        it.keywordScore = r.keyword_score;
                        it.kgEntityScore = r.kg_entity_score;
                        it.structuralScore = r.structural_score;
                    }
                    slots[i] = std::move(it);
                }
            };
            std::vector<std::thread> ths;
            ths.reserve(workers);
            for (size_t t = 0; t < workers; ++t)
                ths.emplace_back(worker);
            for (auto& th : ths)
                th.join();
            resp.results.reserve(n);
            for (size_t i = 0; i < n; ++i)
                if (slots[i].has_value())
                    resp.results.push_back(std::move(*slots[i]));
        }

        resp.total = resp.results.size();
        return resp;
    }

    Result<SearchResponse> metadataSearch(const SearchRequest& req) {
        // Prepare query - escape regex if literalText is requested
        std::string processedQuery = req.query;
        if (req.literalText) {
            // Escape regex special characters
            processedQuery = escapeRegex(req.query);
        }

        // Fuzzy or full-text via metadata repository
        if (req.fuzzy) {
            auto r = ctx_.metadataRepo->fuzzySearch(processedQuery, req.similarity,
                                                    static_cast<int>(req.limit));
            if (!r) {
                return Error{ErrorCode::InternalError, "Fuzzy search failed: " + r.error().message};
            }

            const auto& res = r.value();

            SearchResponse resp;
            resp.total = res.totalCount;
            resp.type = "fuzzy";
            resp.executionTimeMs = res.executionTimeMs;
            resp.usedHybrid = false;

            if (req.pathsOnly) {
                for (const auto& item : res.results) {
                    const auto& d = item.document;
                    bool pathOk = true;
                    if (!req.pathPattern.empty()) {
                        if (hasWildcard(req.pathPattern))
                            pathOk = wildcardMatch(d.filePath, req.pathPattern);
                        else
                            pathOk = d.filePath.find(req.pathPattern) != std::string::npos;
                    }
                    // Enforce ext/mime filters when available
                    bool metaFiltersOk = true;
                    if (!req.fileType.empty()) {
                        // keep current behavior (type handled elsewhere)
                    }
                    if (!req.extension.empty()) {
                        if (d.fileExtension != req.extension &&
                            d.fileExtension != ("." + req.extension)) {
                            metaFiltersOk = false;
                        }
                    }
                    if (!req.mimeType.empty() && d.mimeType != req.mimeType) {
                        metaFiltersOk = false;
                    }
                    if (pathOk && metaFiltersOk &&
                        metadataHasTags(ctx_.metadataRepo.get(), d.id, req.tags,
                                        req.matchAllTags)) {
                        resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                    }
                }
                // Fallback: if no paths matched but query is non-empty, try filename/path contains
                if (resp.paths.empty() && !processedQuery.empty()) {
                    auto allDocs = ctx_.metadataRepo->findDocumentsByPath("%");
                    if (allDocs) {
                        for (const auto& d : allDocs.value()) {
                            if (d.filePath.find(processedQuery) != std::string::npos ||
                                d.fileName.find(processedQuery) != std::string::npos) {
                                resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                                if (resp.paths.size() >= req.limit)
                                    break;
                            }
                        }
                    }
                }
                // Final fallback: if still empty, return up to 'limit' recent documents' paths
                if (resp.paths.empty()) {
                    auto allDocs = ctx_.metadataRepo->findDocumentsByPath("%");
                    if (allDocs) {
                        for (const auto& d : allDocs.value()) {
                            resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                            if (resp.paths.size() >= req.limit)
                                break;
                        }
                    }
                }
                resp.total = resp.paths.size();
                return resp;
            }

            {
                const auto& items = res.results;
                const size_t n = items.size();
                const size_t workers = recommendedWorkers(n);
                std::atomic<size_t> next{0};
                std::vector<std::optional<SearchItem>> slots(n);
                auto worker = [&]() {
                    while (true) {
                        const size_t i = next.fetch_add(1);
                        if (i >= n)
                            break;
                        const auto& item = items[i];
                        const auto& d = item.document;
                        bool pathOk = true;
                        if (!req.pathPattern.empty()) {
                            if (hasWildcard(req.pathPattern))
                                pathOk = wildcardMatch(d.filePath, req.pathPattern);
                            else
                                pathOk = d.filePath.find(req.pathPattern) != std::string::npos;
                        }
                        bool metaFiltersOk = true;
                        if (!req.extension.empty()) {
                            if (d.fileExtension != req.extension &&
                                d.fileExtension != ("." + req.extension))
                                metaFiltersOk = false;
                        }
                        if (!req.mimeType.empty() && d.mimeType != req.mimeType)
                            metaFiltersOk = false;
                        if (!pathOk || !metaFiltersOk ||
                            !metadataHasTags(ctx_.metadataRepo.get(), d.id, req.tags,
                                             req.matchAllTags))
                            continue;

                        SearchItem it;
                        it.id = item.document.id;
                        it.hash = req.showHash ? item.document.sha256Hash : "";
                        it.title = item.document.fileName;
                        it.path = item.document.filePath;
                        it.score = item.score;
                        it.snippet = item.snippet;
                        slots[i] = std::move(it);
                    }
                };
                std::vector<std::thread> ths;
                ths.reserve(workers);
                for (size_t t = 0; t < workers; ++t)
                    ths.emplace_back(worker);
                for (auto& th : ths)
                    th.join();
                resp.results.reserve(n);
                for (size_t i = 0; i < n; ++i)
                    if (slots[i].has_value())
                        resp.results.push_back(std::move(*slots[i]));
            }
            normalizeScores(resp.results, resp.type);
            return resp;
        } else {
            auto r = ctx_.metadataRepo->search(processedQuery, static_cast<int>(req.limit), 0);
            if (!r) {
                return Error{ErrorCode::InternalError,
                             "Full-text search failed: " + r.error().message};
            }

            const auto& res = r.value();
            if (!req.fuzzy && res.totalCount == 0) {
                SearchRequest req2 = req;
                req2.fuzzy = true;
                return metadataSearch(req2);
            }
            SearchResponse resp;
            resp.total = res.totalCount;
            resp.type = "full-text";
            resp.executionTimeMs = res.executionTimeMs;
            resp.usedHybrid = false;

            if (req.pathsOnly) {
                for (const auto& item : res.results) {
                    const auto& d = item.document;
                    bool pathOk = true;
                    if (!req.pathPattern.empty()) {
                        if (hasWildcard(req.pathPattern))
                            pathOk = wildcardMatch(d.filePath, req.pathPattern);
                        else
                            pathOk = d.filePath.find(req.pathPattern) != std::string::npos;
                    }
                    bool metaFiltersOk = true;
                    if (!req.extension.empty()) {
                        if (d.fileExtension != req.extension &&
                            d.fileExtension != ("." + req.extension))
                            metaFiltersOk = false;
                    }
                    if (!req.mimeType.empty() && d.mimeType != req.mimeType)
                        metaFiltersOk = false;
                    if (pathOk && metaFiltersOk &&
                        metadataHasTags(ctx_.metadataRepo.get(), d.id, req.tags,
                                        req.matchAllTags)) {
                        resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                    }
                }
                if (resp.paths.empty() && !processedQuery.empty()) {
                    auto allDocs = ctx_.metadataRepo->findDocumentsByPath("%");
                    if (allDocs) {
                        for (const auto& d : allDocs.value()) {
                            if (d.filePath.find(processedQuery) != std::string::npos ||
                                d.fileName.find(processedQuery) != std::string::npos) {
                                resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                                if (resp.paths.size() >= req.limit)
                                    break;
                            }
                        }
                    }
                }
                if (resp.paths.empty()) {
                    auto allDocs = ctx_.metadataRepo->findDocumentsByPath("%");
                    if (allDocs) {
                        for (const auto& d : allDocs.value()) {
                            resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                            if (resp.paths.size() >= req.limit)
                                break;
                        }
                    }
                }
                resp.total = resp.paths.size();
                return resp;
            }

            // Parallel shaping for full-text results (lock-free slots)
            {
                const auto& items = res.results;
                const size_t n = items.size();
                const size_t workers = recommendedWorkers(n);
                std::atomic<size_t> next{0};
                std::vector<std::optional<SearchItem>> slots(n);
                auto worker = [&]() {
                    while (true) {
                        const size_t i = next.fetch_add(1);
                        if (i >= n)
                            break;
                        const auto& item = items[i];
                        const auto& d = item.document;
                        bool pathOk = true;
                        if (!req.pathPattern.empty()) {
                            if (hasWildcard(req.pathPattern))
                                pathOk = wildcardMatch(d.filePath, req.pathPattern);
                            else
                                pathOk = d.filePath.find(req.pathPattern) != std::string::npos;
                        }
                        bool metaFiltersOk = true;
                        if (!req.extension.empty()) {
                            if (d.fileExtension != req.extension &&
                                d.fileExtension != ("." + req.extension))
                                metaFiltersOk = false;
                        }
                        if (!req.mimeType.empty() && d.mimeType != req.mimeType)
                            metaFiltersOk = false;
                        if (!pathOk || !metaFiltersOk ||
                            !metadataHasTags(ctx_.metadataRepo.get(), d.id, req.tags,
                                             req.matchAllTags))
                            continue;

                        SearchItem it;
                        it.id = item.document.id;
                        it.hash = req.showHash ? item.document.sha256Hash : "";
                        it.title = item.document.fileName;
                        it.path = item.document.filePath;
                        it.score = item.score;
                        it.snippet = item.snippet;
                        slots[i] = std::move(it);
                    }
                };
                std::vector<std::thread> ths;
                ths.reserve(workers);
                for (size_t t = 0; t < workers; ++t)
                    ths.emplace_back(worker);
                for (auto& th : ths)
                    th.join();
                resp.results.reserve(n);
                for (size_t i = 0; i < n; ++i)
                    if (slots[i].has_value())
                        resp.results.push_back(std::move(*slots[i]));
            }
            normalizeScores(resp.results, resp.type);
            return resp;
        }
    }
};

std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx) {
    return std::make_shared<SearchServiceImpl>(ctx);
}

} // namespace yams::app::services
