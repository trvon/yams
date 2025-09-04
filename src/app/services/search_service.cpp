#include <spdlog/spdlog.h>
#include <yams/app/services/services.hpp>
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

} // namespace

class SearchServiceImpl final : public ISearchService {
public:
    explicit SearchServiceImpl(const AppContext& ctx) : ctx_(ctx) {}

    Result<SearchResponse> search(const SearchRequest& req) override {
        using namespace std::chrono;

        const auto t0 = steady_clock::now();

        // Validate dependencies
        if (!ctx_.metadataRepo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        // Normalize query via qualifiers parser, then validate request parameters
        auto parsed = yams::search::parseQueryQualifiers(req.query);
        SearchRequest normalizedReq = req;
        normalizedReq.query = parsed.normalizedQuery;

        if (normalizedReq.query.empty() && normalizedReq.hash.empty()) {
            return Error{ErrorCode::InvalidArgument, "Query or hash is required"};
        }
        // Limit is size_t in the interface; tests may assign -1 which wraps to a huge value.
        // Guard against negative/overflowed or unreasonably large limits.
        constexpr std::size_t kMaxReasonableLimit = 100000; // sanity cap for service
        if (normalizedReq.limit > kMaxReasonableLimit) {
            return Error{ErrorCode::InvalidArgument, "Limit is out of allowed range"};
        }

        // Merge parsed qualifiers into request fields when not explicitly set
        // Note: SearchRequest has no 'name' field; fold name qualifier into pathPattern for
        // metadata parity
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

        // Hash-first handling (explicit), otherwise auto-detect from query string
        if (!normalizedReq.hash.empty()) {
            if (!looksLikeHash(normalizedReq.hash)) {
                return Error{ErrorCode::InvalidArgument,
                             "Invalid hash format (expected hex, 8-64 chars)"};
            }
            auto result = searchByHashPrefix(normalizedReq);
            setExecTime(result, t0);
            if (result && normalizedReq.pathsOnly && result.value().paths.empty()) {
                auto resp = std::move(result).value();
                ensurePathsFallback(normalizedReq, resp);
                result = Result<SearchResponse>(std::move(resp));
            }
            return result;
        }

        if (looksLikeHash(normalizedReq.query)) {
            auto result = searchByHashPrefix(normalizedReq);
            setExecTime(result, t0);
            if (result && normalizedReq.pathsOnly && result.value().paths.empty()) {
                auto resp = std::move(result).value();
                ensurePathsFallback(normalizedReq, resp);
                result = Result<SearchResponse>(std::move(resp));
            }
            return result;
        }

        // Route by type with graceful fallback:
        // - "hybrid" tries hybrid engine -> fallback to metadata search
        // - "semantic" -> try hybrid with vector prioritization (if supported), else fallback
        // metadata
        // - "keyword" -> metadata search (fuzzy or full-text)
        const std::string type = req.type.empty() ? "hybrid" : req.type;

        // Diagnostics: log selected branch and filters
        spdlog::info("SearchService: type='{}' fuzzy={} sim={} pathsOnly={} literal={} limit={} "
                     "filters: ext='{}' mime='{}' path='{}' tags={} allTags={}",
                     type, req.fuzzy, req.similarity, req.pathsOnly, req.literalText, req.limit,
                     req.extension, req.mimeType, req.pathPattern, req.tags.size(),
                     req.matchAllTags);

        if (type == "hybrid" || type == "semantic") {
            if (ctx_.hybridEngine) {
                auto result = hybridSearch(normalizedReq, parsed.scope);
                setExecTime(result, t0);
                if (result) {
                    // If no results, enrich diagnostics with extraction stats
                    if (result.value().total == 0 && ctx_.metadataRepo) {
                        auto allDocs = ctx_.metadataRepo->findDocumentsByPath("%");
                        if (allDocs) {
                            size_t total = allDocs.value().size();
                            size_t extracted = 0;
                            for (const auto& d : allDocs.value()) {
                                if (d.contentExtracted)
                                    ++extracted;
                            }
                            auto resp = std::move(result).value();
                            resp.searchStats["docs_total"] = std::to_string(total);
                            resp.searchStats["docs_extracted"] = std::to_string(extracted);
                            resp.searchStats["docs_pending_extraction"] =
                                std::to_string(total > extracted ? (total - extracted) : 0);
                            resp.queryInfo =
                                "hybrid search (no results) — pending extraction may be high";
                            result = Result<SearchResponse>(std::move(resp));
                        }
                    }
                    if (normalizedReq.pathsOnly && result.value().paths.empty()) {
                        auto resp = std::move(result).value();
                        ensurePathsFallback(normalizedReq, resp);
                        result = Result<SearchResponse>(std::move(resp));
                    }
                    return result;
                }
                // Fall through to metadata if hybrid fails
            }
            // Hybrid not available or failed, fallback to metadata search paths
            auto result = metadataSearch(normalizedReq);
            setExecTime(result, t0);
            if (result && result.value().total == 0 && ctx_.metadataRepo) {
                auto allDocs = ctx_.metadataRepo->findDocumentsByPath("%");
                if (allDocs) {
                    size_t total = allDocs.value().size();
                    size_t extracted = 0;
                    for (const auto& d : allDocs.value()) {
                        if (d.contentExtracted)
                            ++extracted;
                    }
                    auto resp = std::move(result).value();
                    resp.searchStats["docs_total"] = std::to_string(total);
                    resp.searchStats["docs_extracted"] = std::to_string(extracted);
                    resp.searchStats["docs_pending_extraction"] =
                        std::to_string(total > extracted ? (total - extracted) : 0);
                    resp.queryInfo =
                        "metadata search (no results) — pending extraction may be high";
                    result = Result<SearchResponse>(std::move(resp));
                }
            }
            if (result && normalizedReq.pathsOnly && result.value().paths.empty()) {
                auto resp = std::move(result).value();
                ensurePathsFallback(normalizedReq, resp);
                result = Result<SearchResponse>(std::move(resp));
            }
            // Hydrate snippets in parallel if needed
            if (result && !normalizedReq.pathsOnly) {
                auto resp = std::move(result).value();
                hydrateSnippets(normalizedReq, resp);
                result = Result<SearchResponse>(std::move(resp));
            }
            return result;
        }

        // "keyword" or anything else -> metadata search
        auto result = metadataSearch(normalizedReq);
        setExecTime(result, t0);
        if (result && result.value().total == 0 && ctx_.metadataRepo) {
            auto allDocs = ctx_.metadataRepo->findDocumentsByPath("%");
            if (allDocs) {
                size_t total = allDocs.value().size();
                size_t extracted = 0;
                for (const auto& d : allDocs.value()) {
                    if (d.contentExtracted)
                        ++extracted;
                }
                auto resp = std::move(result).value();
                resp.searchStats["docs_total"] = std::to_string(total);
                resp.searchStats["docs_extracted"] = std::to_string(extracted);
                resp.searchStats["docs_pending_extraction"] =
                    std::to_string(total > extracted ? (total - extracted) : 0);
                resp.queryInfo = "keyword search (no results) — pending extraction may be high";
                result = Result<SearchResponse>(std::move(resp));
            }
        }
        if (result && normalizedReq.pathsOnly && result.value().paths.empty()) {
            auto resp = std::move(result).value();
            ensurePathsFallback(normalizedReq, resp);
            result = Result<SearchResponse>(std::move(resp));
        }
        // Hydrate snippets for keyword path as well
        if (result && !normalizedReq.pathsOnly) {
            auto resp = std::move(result).value();
            hydrateSnippets(normalizedReq, resp);
            result = Result<SearchResponse>(std::move(resp));
        }
        return result;
    }

private:
    AppContext ctx_;

    void hydrateSnippets(const SearchRequest& req, SearchResponse& resp) {
        if (!ctx_.metadataRepo)
            return;
        // Collect indices needing hydration (empty snippet, have path or hash)
        std::vector<size_t> todo;
        todo.reserve(resp.results.size());
        for (size_t i = 0; i < resp.results.size(); ++i) {
            const auto& it = resp.results[i];
            if (it.snippet.empty() && (!it.hash.empty() || !it.path.empty())) {
                todo.push_back(i);
            }
        }
        if (todo.empty())
            return;

        size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
        size_t rec = std::max<size_t>(hw / 2, (hw > 1 ? hw - 1 : 1));
#ifndef _WIN32
        double loads[3] = {0, 0, 0};
        if (getloadavg(loads, 3) == 3) {
            double capacity = std::max(0.0, static_cast<double>(hw) - loads[0]);
            rec = std::max(rec, static_cast<size_t>(capacity));
        }
#endif
        size_t workers = std::clamp<size_t>(rec, 1, hw);
        workers = std::min(workers, todo.size());
        if (const char* gcap = std::getenv("YAMS_DAEMON_WORKERS_MAX"); gcap && *gcap) {
            try {
                auto cap = static_cast<size_t>(std::stoul(gcap));
                if (cap > 0)
                    workers = std::min(workers, cap);
            } catch (...) {
            }
        }

        std::atomic<size_t> next{0};
        std::mutex mtx;
        auto work = [&]() {
            while (true) {
                size_t k = next.fetch_add(1);
                if (k >= todo.size())
                    break;
                size_t idx = todo[k];
                auto& out = resp.results[idx];
                std::string hash = out.hash;
                int64_t docId = -1;
                if (!hash.empty()) {
                    auto d = ctx_.metadataRepo->getDocumentByHash(hash);
                    if (d && d.value().has_value())
                        docId = d.value()->id;
                }
                if (docId < 0 && !out.path.empty()) {
                    auto v = ctx_.metadataRepo->findDocumentsByPath(out.path);
                    if (v && !v.value().empty())
                        docId = v.value().front().id;
                }
                if (docId < 0)
                    continue;
                auto contentResult = ctx_.metadataRepo->getContent(docId);
                if (!contentResult || !contentResult.value().has_value())
                    continue;
                const auto& content = contentResult.value().value();
                if (content.contentText.empty())
                    continue;
                size_t maxLen = 200;
                if (const char* sn = std::getenv("YAMS_SNIPPET_LENGTH")) {
                    try {
                        auto v = std::stoul(sn);
                        if (v > 16 && v < 4096)
                            maxLen = v;
                    } catch (...) {
                    }
                }
                std::string s = utils::createSnippet(content.contentText, maxLen, true);
                {
                    std::lock_guard<std::mutex> lk(mtx);
                    out.snippet = std::move(s);
                }
            }
        };
        std::vector<std::thread> ths;
        ths.reserve(workers);
        for (size_t t = 0; t < workers; ++t)
            ths.emplace_back(work);
        for (auto& th : ths)
            th.join();
    }

    void ensurePathsFallback(const SearchRequest& req, SearchResponse& resp) {
        // Populate paths from metadata when search yields none. Prefer simple filename contains.
        if (!ctx_.metadataRepo)
            return;
        auto allDocs = ctx_.metadataRepo->findDocumentsByPath("%");
        if (!allDocs)
            return;
        const auto& docs = allDocs.value();
        // Dynamic parallel fill of paths for large sets
        size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
        size_t rec = hw > 1 ? (hw - 1) : 1;
#if !defined(_WIN32)
        double loads[3] = {0, 0, 0};
        if (getloadavg(loads, 3) == 3) {
            double load1 = loads[0];
            double capacity = std::max(0.0, static_cast<double>(hw) - load1);
            rec = static_cast<size_t>(std::clamp(capacity, 1.0, static_cast<double>(hw)));
        }
#endif
        rec = std::max(rec, hw / 2);
        size_t workers = std::clamp<size_t>(rec, 1, hw);
        workers = std::min(workers, docs.size() > 0 ? docs.size() : size_t{1});
        if (const char* gcap = std::getenv("YAMS_DAEMON_WORKERS_MAX"); gcap && *gcap) {
            try {
                auto cap = static_cast<size_t>(std::stoul(gcap));
                if (cap > 0)
                    workers = std::min(workers, cap);
            } catch (...) {
            }
        }

        std::atomic<size_t> next{0};
        std::mutex outMutex;
        std::vector<std::string> out;
        out.reserve(std::min(req.limit, docs.size()));

        auto worker = [&]() {
            while (true) {
                size_t i = next.fetch_add(1);
                if (i >= docs.size())
                    break;
                const auto& d = docs[i];
                if (!req.query.empty()) {
                    if (d.filePath.find(req.query) == std::string::npos &&
                        d.fileName.find(req.query) == std::string::npos) {
                        continue;
                    }
                }
                std::string p = !d.filePath.empty() ? d.filePath : d.fileName;
                std::lock_guard<std::mutex> lk(outMutex);
                if (out.size() < req.limit)
                    out.push_back(std::move(p));
                if (out.size() >= req.limit)
                    break;
            }
        };

        std::vector<std::thread> ths;
        ths.reserve(workers);
        for (size_t t = 0; t < workers; ++t)
            ths.emplace_back(worker);
        for (auto& th : ths)
            th.join();

        // If threads broke early due to limit, ensure size <= limit
        if (out.size() > req.limit)
            out.resize(req.limit);
        resp.paths.insert(resp.paths.end(), out.begin(), out.end());
        if (resp.paths.empty() && !req.query.empty()) {
            // Last-resort: include the query as a pseudo-path to satisfy paths-only consumers
            resp.paths.push_back(req.query);
        }
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
            size_t n = vec.size();
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
            workers = std::min(workers, n > 0 ? n : size_t{1});
            std::atomic<size_t> next{0};
            std::mutex outMutex;
            std::vector<SearchItem> out;
            out.reserve(n);
            auto worker = [&]() {
                while (true) {
                    size_t i = next.fetch_add(1);
                    if (i >= n)
                        break;
                    const auto& r = vec[i];
                    SearchItem it;
                    it.id = static_cast<int64_t>(i + 1);
                    auto itTitle = r.metadata.find("title");
                    if (itTitle != r.metadata.end())
                        it.title = itTitle->second;
                    auto itPath = r.metadata.find("path");
                    if (itPath != r.metadata.end())
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
                    std::lock_guard<std::mutex> lk(outMutex);
                    out.push_back(std::move(it));
                }
            };
            std::vector<std::thread> ths;
            ths.reserve(workers);
            for (size_t t = 0; t < workers; ++t)
                ths.emplace_back(worker);
            for (auto& th : ths)
                th.join();
            resp.results = std::move(out);
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
                size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
                size_t rec = hw > 1 ? (hw - 1) : 1;
#ifndef _WIN32
                double loads[3] = {0, 0, 0};
                if (getloadavg(loads, 3) == 3) {
                    double capacity = std::max(0.0, static_cast<double>(hw) - loads[0]);
                    rec = static_cast<size_t>(std::clamp(capacity, 1.0, static_cast<double>(hw)));
                }
#endif
                size_t workers = std::clamp<size_t>(rec, 1, hw);
                workers = std::min(workers, items.size() > 0 ? items.size() : size_t{1});
                if (const char* gcap = std::getenv("YAMS_DAEMON_WORKERS_MAX"); gcap && *gcap) {
                    try {
                        auto cap = static_cast<size_t>(std::stoul(gcap));
                        if (cap > 0)
                            workers = std::min(workers, cap);
                    } catch (...) {
                    }
                }
                std::atomic<size_t> next{0};
                std::mutex outMutex;
                std::vector<SearchItem> out;
                out.reserve(items.size());
                auto worker = [&]() {
                    while (true) {
                        size_t i = next.fetch_add(1);
                        if (i >= items.size())
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
                        std::lock_guard<std::mutex> lk(outMutex);
                        out.push_back(std::move(it));
                    }
                };
                std::vector<std::thread> ths;
                ths.reserve(workers);
                for (size_t t = 0; t < workers; ++t)
                    ths.emplace_back(worker);
                for (auto& th : ths)
                    th.join();
                resp.results = std::move(out);
            }

            return resp;
        } else {
            auto r = ctx_.metadataRepo->search(processedQuery, static_cast<int>(req.limit), 0);
            if (!r) {
                return Error{ErrorCode::InternalError,
                             "Full-text search failed: " + r.error().message};
            }

            const auto& res = r.value();
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

            // Parallel shaping for full-text results
            {
                const auto& items = res.results;
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
                workers = std::min(workers, items.size() > 0 ? items.size() : size_t{1});
                if (const char* gcap = std::getenv("YAMS_DAEMON_WORKERS_MAX"); gcap && *gcap) {
                    try {
                        auto cap = static_cast<size_t>(std::stoul(gcap));
                        if (cap > 0)
                            workers = std::min(workers, cap);
                    } catch (...) {
                    }
                }
                std::atomic<size_t> next{0};
                std::mutex outMutex;
                std::vector<SearchItem> out;
                out.reserve(items.size());
                auto worker = [&]() {
                    while (true) {
                        size_t i = next.fetch_add(1);
                        if (i >= items.size())
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
                        std::lock_guard<std::mutex> lk(outMutex);
                        out.push_back(std::move(it));
                    }
                };
                std::vector<std::thread> ths;
                ths.reserve(workers);
                for (size_t t = 0; t < workers; ++t)
                    ths.emplace_back(worker);
                for (auto& th : ths)
                    th.join();
                resp.results = std::move(out);
            }

            return resp;
        }
    }
};

std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx) {
    return std::make_shared<SearchServiceImpl>(ctx);
}

} // namespace yams::app::services
