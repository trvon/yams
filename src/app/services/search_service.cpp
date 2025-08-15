#include <yams/app/services/services.hpp>

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

        // Hash-first handling (explicit), otherwise auto-detect from query string
        if (!req.hash.empty()) {
            if (!looksLikeHash(req.hash)) {
                return Error{ErrorCode::InvalidArgument,
                             "Invalid hash format (expected hex, 8-64 chars)"};
            }
            auto result = searchByHashPrefix(req.hash, req.limit, req.pathsOnly);
            setExecTime(result, t0);
            return result;
        }

        if (looksLikeHash(req.query)) {
            auto result = searchByHashPrefix(req.query, req.limit, req.pathsOnly);
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
                auto result = hybridSearch(req);
                setExecTime(result, t0);
                if (result)
                    return result;
                // Fall through to metadata if hybrid fails
            }
            // Hybrid not available or failed, fallback to metadata search paths
            auto result = metadataSearch(req);
            setExecTime(result, t0);
            return result;
        }

        // "keyword" or anything else -> metadata search
        auto result = metadataSearch(req);
        setExecTime(result, t0);
        return result;
    }

private:
    AppContext ctx_;

    template <typename T> void setExecTime(Result<T>& r, std::chrono::steady_clock::time_point t0) {
        using namespace std::chrono;
        if (r) {
            // Best effort: try to set execution time if T has that field
            // We can't SFINAE easily here; rely on overload below
        }
    }

    // Overload specifically to set SearchResponse.executionTimeMs
    void setExecTime(Result<SearchResponse>& r, std::chrono::steady_clock::time_point t0) {
        using namespace std::chrono;
        if (r) {
            auto dur = duration_cast<milliseconds>(steady_clock::now() - t0).count();
            // Can't modify through const reference, need to reconstruct
            auto resp = std::move(r).value();
            resp.executionTimeMs = static_cast<int64_t>(dur);
            r = std::move(resp);
        }
    }

    Result<SearchResponse> searchByHashPrefix(const std::string& prefix, std::size_t limit,
                                              bool pathsOnly) {
        auto docsResult = ctx_.metadataRepo->findDocumentsByPath("%");
        if (!docsResult) {
            return Error{ErrorCode::InternalError,
                         "Failed to enumerate documents for hash search: " +
                             docsResult.error().message};
        }

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

            if (pathsOnly) {
                resp.paths.push_back(doc.filePath);
            } else {
                SearchItem it;
                it.id = doc.id;
                it.hash = doc.sha256Hash;
                it.title = doc.fileName;
                it.path = doc.filePath;
                it.score = 1.0;  // Exact/prefix match
                it.snippet = ""; // not available here
                resp.results.push_back(std::move(it));
            }

            if (++count >= limit)
                break;
        }

        resp.total = pathsOnly ? resp.paths.size() : resp.results.size();
        return resp;
    }

    Result<SearchResponse> hybridSearch(const SearchRequest& req) {
        // Expect ctx_.hybridEngine->search(query, limit) returning Result<vector<...>>
        // Shape inferred from existing MCP code: each result has:
        //  - id
        //  - metadata map with "title" and "path"
        //  - hybrid_score, vector_score, keyword_score, kg_entity_score, structural_score
        //  - content snippet (optional)
        auto hres = ctx_.hybridEngine->search(req.query, req.limit);
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
            it.hash.clear();                     // Not directly provided by hybrid result
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
        // Fuzzy or full-text via metadata repository
        if (req.fuzzy) {
            auto r = ctx_.metadataRepo->fuzzySearch(req.query, req.similarity,
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
                    resp.paths.push_back(item.document.filePath);
                }
                resp.total = resp.paths.size();
                return resp;
            }

            for (const auto& item : res.results) {
                SearchItem it;
                it.id = item.document.id;
                it.hash = item.document.sha256Hash;
                it.title = item.document.fileName;
                it.path = item.document.filePath;
                it.score = item.score;
                it.snippet = item.snippet;
                resp.results.push_back(std::move(it));
            }

            return resp;
        } else {
            auto r = ctx_.metadataRepo->search(req.query, static_cast<int>(req.limit), 0);
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
                    resp.paths.push_back(item.document.filePath);
                }
                resp.total = resp.paths.size();
                return resp;
            }

            for (const auto& item : res.results) {
                SearchItem it;
                it.id = item.document.id;
                it.hash = item.document.sha256Hash;
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