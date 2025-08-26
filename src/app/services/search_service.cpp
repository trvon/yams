#include <yams/app/services/services.hpp>
#include <yams/search/query_qualifiers.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

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
        if (normalizedReq.limit < 0) {
            return Error{ErrorCode::InvalidArgument, "Limit must be non-negative"};
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
            return result;
        }

        if (looksLikeHash(normalizedReq.query)) {
            auto result = searchByHashPrefix(normalizedReq);
            setExecTime(result, t0);
            return result;
        }

        // Route by type with graceful fallback:
        // - "hybrid" tries hybrid engine -> fallback to metadata search
        // - "semantic" -> try hybrid with vector prioritization (if supported), else fallback
        // metadata
        // - "keyword" -> metadata search (fuzzy or full-text)
        const std::string type = req.type.empty() ? "hybrid" : req.type;

        if (type == "hybrid" || type == "semantic") {
            if (ctx_.hybridEngine) {
                auto result = hybridSearch(normalizedReq, parsed.scope);
                setExecTime(result, t0);
                if (result)
                    return result;
                // Fall through to metadata if hybrid fails
            }
            // Hybrid not available or failed, fallback to metadata search paths
            auto result = metadataSearch(normalizedReq);
            setExecTime(result, t0);
            return result;
        }

        // "keyword" or anything else -> metadata search
        auto result = metadataSearch(normalizedReq);
        setExecTime(result, t0);
        return result;
    }

private:
    AppContext ctx_;

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

        for (size_t i = 0; i < vec.size(); ++i) {
            const auto& r = vec[i];
            SearchItem it;
            it.id = static_cast<int64_t>(i + 1); // Use 1-based index as ID
            // Hash not directly available from hybrid result, only show if requested and available
            it.hash.clear();
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

            resp.results.push_back(std::move(it));
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
                        resp.paths.push_back(d.filePath);
                    }
                }
                resp.total = resp.paths.size();
                return resp;
            }

            for (const auto& item : res.results) {
                const auto& d = item.document;
                bool pathOk = true;
                if (!req.pathPattern.empty()) {
                    if (hasWildcard(req.pathPattern))
                        pathOk = wildcardMatch(d.filePath, req.pathPattern);
                    else
                        pathOk = d.filePath.find(req.pathPattern) != std::string::npos;
                }
                // Enforce name/ext/mime filters when available
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

                if (!pathOk || !metaFiltersOk ||
                    !metadataHasTags(ctx_.metadataRepo.get(), d.id, req.tags, req.matchAllTags)) {
                    continue;
                }
                SearchItem it;
                it.id = item.document.id;
                it.hash = req.showHash ? item.document.sha256Hash : "";
                it.title = item.document.fileName;
                it.path = item.document.filePath;
                it.score = item.score;
                it.snippet = item.snippet;
                resp.results.push_back(std::move(it));
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
                        resp.paths.push_back(d.filePath);
                    }
                }
                resp.total = resp.paths.size();
                return resp;
            }

            for (const auto& item : res.results) {
                const auto& d = item.document;
                bool pathOk = true;
                if (!req.pathPattern.empty()) {
                    if (hasWildcard(req.pathPattern))
                        pathOk = wildcardMatch(d.filePath, req.pathPattern);
                    else
                        pathOk = d.filePath.find(req.pathPattern) != std::string::npos;
                }
                // Enforce name/ext/mime filters when available
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

                if (!pathOk || !metaFiltersOk ||
                    !metadataHasTags(ctx_.metadataRepo.get(), d.id, req.tags, req.matchAllTags)) {
                    continue;
                }
                SearchItem it;
                it.id = item.document.id;
                it.hash = req.showHash ? item.document.sha256Hash : "";
                it.title = item.document.fileName;
                it.path = item.document.filePath;
                it.score = item.score;
                it.snippet = item.snippet;
                resp.results.push_back(std::move(it));
            }

            return resp;
        }
    }
};

std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx) {
    return std::make_shared<SearchServiceImpl>(ctx);
}

} // namespace yams::app::services