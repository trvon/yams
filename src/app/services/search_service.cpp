#include <spdlog/spdlog.h>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <yams/app/services/enhanced_search_executor.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/query_helpers.h>
#ifdef YAMS_ENABLE_DAEMON_FEATURES
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/resource/plugin_host.h>
#endif
#include <yams/plugins/search_provider_v1.h>
#include <yams/search/parallel_post_processor.hpp>
#include <yams/search/query_concept_extractor.h>
#include <yams/search/query_qualifiers.hpp>
#include <yams/search/symbol_enrichment.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <regex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#ifndef _WIN32
#include <unistd.h>
#endif

#include "yams/profiling.h"

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

std::vector<search::SearchResultItem> toPostProcessItems(const std::vector<SearchItem>& items) {
    std::vector<search::SearchResultItem> out;
    out.reserve(items.size());
    for (const auto& item : items) {
        search::SearchResultItem result;
        result.documentId = item.id;
        result.title = item.title;
        result.path = item.path;
        result.contentType = item.mimeType.empty() ? item.fileType : item.mimeType;
        result.fileSize = static_cast<size_t>(item.size);
        result.contentPreview = item.snippet;
        result.metadata = item.metadata;
        result.relevanceScore = static_cast<float>(item.score);
        out.push_back(std::move(result));
    }
    return out;
}

void applyExtensionFacets(SearchResponse& resp) {
    if (resp.results.empty()) {
        return;
    }
    if (!resp.facets.empty()) {
        return;
    }
    constexpr size_t kMaxFacetValues = 10;
    auto items = toPostProcessItems(resp.results);
    auto processed = search::ParallelPostProcessor::process(std::move(items), nullptr,
                                                            {"extension"}, nullptr, 0, 0);
    for (auto& facet : processed.facets) {
        if (facet.name == "extension") {
            facet.displayName = "File Type";
        }
        const size_t total = facet.values.size();
        if (facet.values.size() > kMaxFacetValues) {
            facet.values.resize(kMaxFacetValues);
        }
        facet.totalValues = total;
        resp.facets.push_back(std::move(facet));
    }
}

// Converts a glob pattern to a regex string.
static std::string globToRegex(const std::string& glob) {
    std::string regex_str;
    regex_str.reserve(glob.size() * 2);
    for (size_t i = 0; i < glob.size(); ++i) {
        char c = glob[i];
        if (c == '*') {
            if (i + 1 < glob.size() && glob[i + 1] == '*') {
                // '**' matches any sequence of characters, including path separators
                regex_str += ".*";
                i++; // consume second '*'
            } else {
                // '*' matches any sequence of characters except path separators
                regex_str += "[^/]*";
            }
        } else if (c == '?') {
            regex_str += ".";
        } else if (c == '.' || c == '+' || c == '(' || c == ')' || c == '{' || c == '}' ||
                   c == '[' || c == ']' || c == '^' || c == '|' || c == '\\') {
            regex_str += '\\';
            regex_str += c;
        } else {
            regex_str += c;
        }
    }
    return regex_str;
}

// Robust glob matcher using regex, supporting '**'.
static bool wildcardMatch(const std::string& text, const std::string& pattern) {
    try {
        std::regex re(globToRegex(pattern));
        return std::regex_match(text, re);
    } catch (const std::regex_error& e) {
        spdlog::warn("Invalid glob pattern '{}' converted to regex: {}", pattern, e.what());
        // Fallback to simple string contains for invalid patterns
        return text.find(pattern) != std::string::npos;
    }
}

// Heuristic: treat as path/filename when the query contains a separator
// or looks like a single token with an extension and no spaces.
static std::string trimCopy(std::string s) {
    const auto isSpace = [](unsigned char c) { return static_cast<bool>(std::isspace(c)); };
    s.erase(s.begin(), std::find_if_not(s.begin(), s.end(), isSpace));
    s.erase(std::find_if_not(s.rbegin(), s.rend(), isSpace).base(), s.end());
    return s;
}

// Stricter heuristic for query routing: avoid misclassifying code-like tokens as hashes.
// Require explicit prefix or a long hex length with at least one alpha hex digit.
bool looksLikeHashQuery(const std::string& raw) {
    auto trimmed = trimCopy(raw);
    if (trimmed.empty())
        return false;

    const std::string lower = [&]() {
        std::string s = trimmed;
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return s;
    }();

    auto stripPrefix = [&](std::string_view prefix) -> std::string {
        if (lower.rfind(prefix, 0) == 0) {
            return trimmed.substr(prefix.size());
        }
        return {};
    };

    if (auto v = stripPrefix("hash:"); !v.empty()) {
        return looksLikeHash(trimCopy(v));
    }
    if (auto v = stripPrefix("sha1:"); !v.empty()) {
        return looksLikeHash(trimCopy(v));
    }
    if (auto v = stripPrefix("sha256:"); !v.empty()) {
        return looksLikeHash(trimCopy(v));
    }
    if (auto v = stripPrefix("md5:"); !v.empty()) {
        return looksLikeHash(trimCopy(v));
    }

    if (!looksLikeHash(trimmed))
        return false;

    // Require at least 8 chars for hash prefix searches (matches looksLikeHash minimum).
    // Also require at least one alpha digit to avoid common code tokens like "deadbeef".
    if (trimmed.size() < 8)
        return false;
    return std::any_of(trimmed.begin(), trimmed.end(),
                       [](unsigned char c) { return std::isalpha(c) != 0; });
}

std::optional<std::string> extractHashPrefix(const std::string& raw) {
    auto trimmed = trimCopy(raw);
    if (trimmed.empty())
        return std::nullopt;

    std::string lower = trimmed;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    auto stripPrefix = [&](std::string_view prefix) -> std::string {
        if (lower.rfind(prefix, 0) == 0) {
            return trimCopy(trimmed.substr(prefix.size()));
        }
        return {};
    };

    if (auto v = stripPrefix("hash:"); !v.empty()) {
        return looksLikeHash(v) ? std::optional<std::string>(v) : std::nullopt;
    }
    if (auto v = stripPrefix("sha1:"); !v.empty()) {
        return looksLikeHash(v) ? std::optional<std::string>(v) : std::nullopt;
    }
    if (auto v = stripPrefix("sha256:"); !v.empty()) {
        return looksLikeHash(v) ? std::optional<std::string>(v) : std::nullopt;
    }
    if (auto v = stripPrefix("md5:"); !v.empty()) {
        return looksLikeHash(v) ? std::optional<std::string>(v) : std::nullopt;
    }

    if (looksLikeHashQuery(trimmed)) {
        return trimmed;
    }

    return std::nullopt;
}

static bool hasWhitespace(const std::string& s) {
    return std::any_of(s.begin(), s.end(), [](unsigned char c) { return std::isspace(c); });
}

static bool looksLikePathToken(const std::string& token) {
    if (token.find('/') != std::string::npos || token.find('\\') != std::string::npos)
        return true;
    auto dot = token.rfind('.');
    if (dot != std::string::npos && dot > 0 && dot + 1 < token.size()) {
        // Has an extension-like suffix
        return true;
    }
    // Wildcards also indicate a path-style intent
    return hasWildcard(token);
}

static bool looksLikePathQuery(const std::string& raw) {
    auto trimmed = trimCopy(raw);
    if (trimmed.empty())
        return false;

    const bool quoted =
        (trimmed.size() >= 2 && ((trimmed.front() == '"' && trimmed.back() == '"') ||
                                 (trimmed.front() == '\'' && trimmed.back() == '\'')));
    std::string token = quoted ? trimmed.substr(1, trimmed.size() - 2) : trimmed;

    if (!quoted && hasWhitespace(token))
        return false;

    return looksLikePathToken(token);
}

// Presence-based tag match using metadata repository
static bool isTransientMetadataError(const Error& err) {
    switch (err.code) {
        case ErrorCode::NotInitialized:
        case ErrorCode::DatabaseError:
        case ErrorCode::ResourceBusy:
        case ErrorCode::OperationInProgress:
        case ErrorCode::Timeout:
            return true;
        case ErrorCode::InternalError: {
            const auto& msg = err.message;
            return msg.find("database is locked") != std::string::npos ||
                   msg.find("readonly") != std::string::npos ||
                   msg.find("busy") != std::string::npos;
        }
        default:
            return false;
    }
}

struct MetadataTelemetry {
    std::atomic<std::uint64_t> operations{0};
    std::atomic<std::uint64_t> retries{0};
    std::atomic<std::uint64_t> transientFailures{0};
};

template <typename Fn>
boost::asio::awaitable<decltype(std::declval<Fn>()())>
retryMetadataOp(Fn&& fn, std::size_t maxAttempts = 4,
                std::chrono::milliseconds initialDelay = std::chrono::milliseconds(25),
                MetadataTelemetry* telemetry = nullptr) {
    if (telemetry) {
        telemetry->operations.fetch_add(1, std::memory_order_relaxed);
    }

    auto attempt = fn();
    if (attempt) {
        co_return attempt;
    }

    auto delay = initialDelay;
    bool transient = isTransientMetadataError(attempt.error());
    if (telemetry && transient) {
        telemetry->transientFailures.fetch_add(1, std::memory_order_relaxed);
    }

    for (std::size_t i = 1; i < maxAttempts && transient; ++i) {
        if (telemetry) {
            telemetry->retries.fetch_add(1, std::memory_order_relaxed);
        }

        boost::asio::steady_timer timer(co_await boost::asio::this_coro::executor);
        timer.expires_after(delay);
        co_await timer.async_wait(boost::asio::use_awaitable);

        delay = std::min(delay * 2, std::chrono::milliseconds(250));

        attempt = fn();
        if (attempt) {
            co_return attempt;
        }

        transient = isTransientMetadataError(attempt.error());
        if (telemetry && transient) {
            telemetry->transientFailures.fetch_add(1, std::memory_order_relaxed);
        }
    }

    co_return attempt;
}

static boost::asio::awaitable<bool> metadataHasTags(metadata::MetadataRepository* repo,
                                                    int64_t docId,
                                                    const std::vector<std::string>& tags,
                                                    bool matchAll,
                                                    MetadataTelemetry* telemetry = nullptr) {
    if (!repo || tags.empty()) {
        co_return true;
    }

    auto md = co_await retryMetadataOp([&]() { return repo->getAllMetadata(docId); }, 4,
                                       std::chrono::milliseconds(25), telemetry);

    if (!md) {
        spdlog::debug("SearchService: metadata lookup failed for doc {}: {}", docId,
                      md.error().message);
        co_return false;
    }

    auto& all = md.value();

    // Debug: log all metadata keys for this document
    if (!tags.empty()) {
        std::string keys;
        for (const auto& [k, v] : all) {
            if (!keys.empty())
                keys += ", ";
            keys += k + "=" + v.asString();
        }
        spdlog::info(
            "metadataHasTags: docId={} searching for tags=[{}] matchAll={} found_metadata=[{}]",
            docId, fmt::join(tags, ","), matchAll, keys);
    }

    auto hasTag = [&](const std::string& t) {
        // Check for normalized storage (key="tag:<name>")
        auto it = all.find("tag:" + t);
        if (it != all.end()) {
            spdlog::debug("metadataHasTags: found tag:{}", t);
            return true;
        }
        // Fallback: check for legacy storage (key="tag", value="<name>")
        for (const auto& [k, v] : all) {
            if (k == "tag" && v.asString() == t) {
                spdlog::debug("metadataHasTags: found legacy tag {}", t);
                return true;
            }
        }
        spdlog::debug("metadataHasTags: tag '{}' not found", t);
        return false;
    };

    if (matchAll) {
        for (const auto& t : tags) {
            if (!hasTag(t)) {
                co_return false;
            }
        }
        co_return true;
    } else {
        for (const auto& t : tags) {
            if (hasTag(t)) {
                co_return true;
            }
        }
        co_return false;
    }
}

// Compute recommended worker count based on hardware, load and caps
static inline size_t recommendedWorkers(size_t items) {
    size_t hw = std::max<size_t>(1, std::thread::hardware_concurrency());
    size_t rec = hw > 1 ? (hw - 1) : 1;
#if !defined(_WIN32) && !defined(__ANDROID__)
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

class SearchServiceImpl final : public ISearchService,
                                public std::enable_shared_from_this<SearchServiceImpl> {
public:
    explicit SearchServiceImpl(const AppContext& ctx) : ctx_(ctx) {
        // Initialize degraded mode from AppContext repair flags (preferred), falling back to env
        // vars
        degraded_ = ctx_.searchRepairInProgress || (ctx_.searchEngine == nullptr);
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

        // PBI-074: Initialize symbol enricher for ranking boost
        if (ctx_.kgStore) {
            symbolEnricher_ = std::make_shared<yams::search::SymbolEnricher>(ctx_.kgStore);
        }

        // Symbol weight configurable via env var (default 0.15 = 15% boost)
        if (const char* w = std::getenv("YAMS_SYMBOL_WEIGHT")) {
            try {
                symbolWeight_ = std::stof(w);
                symbolWeight_ = std::max(0.0f, std::min(1.0f, symbolWeight_));
            } catch (...) {
            }
        }
    }

    boost::asio::awaitable<Result<SearchResponse>> search(const SearchRequest& req) override {
        YAMS_ZONE_SCOPED_N("search_service::search");
        using namespace std::chrono;
        const auto t0 = steady_clock::now();
        MetadataTelemetry metadataTelemetry;

        // Validate dependencies
        if (!ctx_.metadataRepo) {
            co_return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }

        YAMS_ZONE_SCOPED_N("search_service::parse_and_plan");
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
            YAMS_ZONE_SCOPED_N("search_service::hash_lookup");
            if (!looksLikeHash(normalizedReq.hash)) {
                co_return Error{ErrorCode::InvalidArgument,
                                "Invalid hash format (expected hex, 8-64 chars)"};
            }
            auto result = co_await searchByHashPrefix(normalizedReq, &metadataTelemetry);
            setExecTime(result, t0);
            if (result) {
                co_return result;
            }
            co_return result;
        }

        if (auto hashPrefix = extractHashPrefix(normalizedReq.query)) {
            normalizedReq.hash = std::move(*hashPrefix);
            auto result = co_await searchByHashPrefix(normalizedReq, &metadataTelemetry);
            setExecTime(result, t0);
            if (result) {
                co_return result;
            }
            co_return result;
        }

        bool forcedHybridFallback = false;
        const std::string type = resolveSearchType(req, &forcedHybridFallback);

        if (forcedHybridFallback) {
            const std::string requestedType = req.type.empty() ? "hybrid" : req.type;
            const std::string detail =
                repairDetails_.empty() ? "hybrid engine disabled" : repairDetails_;
            spdlog::warn("SearchService: routing {} search request to keyword path because {}",
                         requestedType, detail);
        }

        spdlog::info("SearchService: type='{}' fuzzy={} sim={} pathsOnly={} literal={} limit={} "
                     "filters: ext='{}' mime='{}' path='{}' pathPatterns={} tags={} allTags={}",
                     type, req.fuzzy, req.similarity, req.pathsOnly, req.literalText, req.limit,
                     req.extension, req.mimeType, req.pathPattern, req.pathPatterns.size(),
                     req.tags.size(), req.matchAllTags);

        Result<SearchResponse> result(Error{ErrorCode::Unknown, "Search path not taken"});

        // Path/filename-first heuristic: if the user likely typed a path-like query,
        // avoid noisy FTS5 attempts and go straight to metadata path/name contains.
        if (looksLikePathQuery(normalizedReq.query)) {
            auto pathResult = co_await pathSearch(normalizedReq, &metadataTelemetry);
            setExecTime(pathResult, t0);
            if (pathResult) {
                auto resp = std::move(pathResult).value();
                resp.type = "path";
                resp.searchStats["mode"] = "path";
                resp.queryInfo = "path/name contains match";
                co_return Result<SearchResponse>(std::move(resp));
            }
            // Fall through to standard paths on error.
        }

        if (type == "hybrid" || type == "semantic") {
            if (degraded_ || !ctx_.searchEngine) {
                spdlog::warn("SearchService: hybrid/semantic search not ready{}",
                             repairDetails_.empty() ? "" : (std::string(": ") + repairDetails_));
                co_return Error{
                    ErrorCode::InvalidState,
                    "Hybrid/semantic search not ready" +
                        (repairDetails_.empty() ? std::string{} : (" - " + repairDetails_))};
            }
            result = hybridSearch(normalizedReq, parsed.scope, &metadataTelemetry,
                                  normalizedReq.pathPattern);
        } else {
            result = metadataSearch(normalizedReq, &metadataTelemetry);
        }

        // Path filtering: prefer pathPatterns (multiple patterns) over legacy pathPattern
        const auto& patterns =
            !req.pathPatterns.empty()
                ? req.pathPatterns
                : (!req.pathPattern.empty() ? std::vector<std::string>{req.pathPattern}
                                            : std::vector<std::string>{});

        if (result && !patterns.empty()) {
            auto resp = std::move(result).value();
            std::vector<SearchItem> filtered;
            for (const auto& item : resp.results) {
                bool pathOk = false;
                // Match ANY pattern (OR logic)
                for (const auto& pattern : patterns) {
                    if (hasWildcard(pattern)) {
                        std::string normalized = pattern;
                        // Normalize glob patterns for path matching:
                        // - "*.ext" should match any path ending in .ext (prepend **/)
                        // - "dir/*.ext" should match dir/*.ext (prepend **)
                        // - "**/pattern" and "/pattern" are already absolute
                        if (!normalized.empty() && normalized.front() == '*' &&
                            (normalized.size() == 1 || normalized[1] != '*')) {
                            // Single * at start (e.g., "*.md") - match anywhere in path
                            normalized = "**/" + normalized;
                        } else if (!normalized.empty() && normalized.front() != '*' &&
                                   normalized.front() != '/' &&
                                   normalized.find(":/") == std::string::npos &&
                                   normalized.find("**/") != 0) {
                            // Relative pattern without leading ** (e.g., "src/*.cpp") - match
                            // anywhere
                            normalized = "**/" + normalized;
                        }
                        if (wildcardMatch(item.path, normalized)) {
                            pathOk = true;
                            break;
                        }
                    } else {
                        // Non-wildcard pattern: substring match
                        if (item.path.find(pattern) != std::string::npos) {
                            pathOk = true;
                            break;
                        }
                    }
                }
                if (pathOk) {
                    filtered.push_back(item);
                }
            }
            resp.results = std::move(filtered);
            resp.total = resp.results.size();
            result = Result<SearchResponse>(std::move(resp));
        }

        setExecTime(result, t0);

        if (result && !normalizedReq.pathsOnly) {
            auto resp = std::move(result).value();
            if (enhancedCfg_.enable) {
                enhanced_.apply(ctx_, enhancedCfg_, normalizedReq.query, resp.results);
            }
            co_await hydrateSnippetsAsync_worker(normalizedReq, resp, &metadataTelemetry);
            result = Result<SearchResponse>(std::move(resp));
        }

        if (result) {
            auto resp = std::move(result).value();
            if (forcedHybridFallback) {
                const std::string fallbackReason =
                    repairDetails_.empty() ? "hybrid_disabled" : repairDetails_;
                resp.searchStats["hybrid_fallback"] = fallbackReason;
                resp.searchStats["effective_type"] = type;
                resp.searchStats["mode"] = "degraded";
                if (resp.queryInfo.empty()) {
                    resp.queryInfo = "fallback to keyword search due to: " + fallbackReason;
                } else {
                    resp.queryInfo += " (degraded fallback: " + fallbackReason + ")";
                }
            }
            auto totalElapsed = duration_cast<milliseconds>(steady_clock::now() - t0).count();
            resp.executionTimeMs = static_cast<int64_t>(totalElapsed);
            resp.searchStats["latency_ms"] = std::to_string(totalElapsed);
            resp.searchStats["metadata_operations"] =
                std::to_string(metadataTelemetry.operations.load(std::memory_order_relaxed));
            resp.searchStats["metadata_retries"] =
                std::to_string(metadataTelemetry.retries.load(std::memory_order_relaxed));
            resp.searchStats["metadata_transient_failures"] =
                std::to_string(metadataTelemetry.transientFailures.load(std::memory_order_relaxed));
            result = Result<SearchResponse>(std::move(resp));
        }

        co_return result;
    }

    boost::asio::awaitable<Result<void>> lightIndexForHash_impl(const std::string& hash,
                                                                std::size_t maxBytes) {
        try {
            if (!ctx_.metadataRepo) {
                co_return Error{ErrorCode::NotInitialized, "metadata repository not available"};
            }
            auto di = co_await retryMetadataOp(
                [&]() { return ctx_.metadataRepo->getDocumentByHash(hash); });
            if (!di) {
                co_return di.error();
            }
            if (!di.value().has_value()) {
                co_return Error{ErrorCode::NotFound, "document not found by hash"};
            }
            auto info = *di.value();

            // If already extracted, nothing to do
            if (info.contentExtracted &&
                info.extractionStatus == metadata::ExtractionStatus::Success) {
                co_return Result<void>();
            }

            // Only attempt lightweight extraction for text-like types or small files
            const std::string mime = info.mimeType;
            const std::string ext = info.fileExtension;

            // Use FileTypeDetector for consistent MIME type checks (leverages magic_numbers.hpp)
            auto& detector = yams::detection::FileTypeDetector::instance();
            bool isTextLike = false;

            if (!mime.empty() && detector.isTextMimeType(mime)) {
                isTextLike = true;
            } else if (!ext.empty()) {
                // Try extension-based detection
                auto detectedMime =
                    yams::detection::FileTypeDetector::getMimeTypeFromExtension(ext);
                if (!detectedMime.empty() && detector.isTextMimeType(detectedMime)) {
                    isTextLike = true;
                }
            }

            if (!isTextLike) {
                // Skip non-text types here; daemon/background can handle richer extraction if
                // needed.
                co_return Result<void>();
            }

            if (!ctx_.store) {
                co_return Error{ErrorCode::NotInitialized, "content store not available"};
            }

            // Guard: avoid loading very large files in memory for the light path
            if (info.fileSize > 0 && static_cast<std::size_t>(info.fileSize) > maxBytes) {
                spdlog::debug("LightIndex: skipping large file {} (size={} > cap={})",
                              info.fileName, info.fileSize, maxBytes);
                co_return Result<void>();
            }

            auto bytesRes = ctx_.store->retrieveBytes(hash);
            if (!bytesRes) {
                co_return bytesRes.error();
            }
            auto& buf = bytesRes.value();
            if (buf.empty()) {
                co_return Result<void>();
            }
            if (buf.size() > maxBytes) {
                spdlog::debug("LightIndex: trimming content {} from {} to {} bytes", info.fileName,
                              buf.size(), maxBytes);
            }
            const std::size_t n = std::min<std::size_t>(buf.size(), maxBytes);
            std::string text;
            text.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                text.push_back(static_cast<char>(buf[i]));
            }

            // Very light HTML handling: strip tags naively when extension suggests HTML
            if (ext == ".html" || ext == ".htm" || mime == "text/html") {
                std::string out;
                out.reserve(text.size());
                bool intag = false;
                for (char c : text) {
                    if (c == '<') {
                        intag = true;
                        continue;
                    }
                    if (c == '>') {
                        intag = false;
                        continue;
                    }
                    if (!intag)
                        out.push_back(c);
                }
                text.swap(out);
            }

            // Index into FTS5 and fuzzy index
            (void)co_await retryMetadataOp([&]() {
                return ctx_.metadataRepo->indexDocumentContent(info.id, info.fileName, text,
                                                               mime.empty() ? "text/plain" : mime);
            });

            // Mark extraction success for this light path (avoid read-modify-write)
            (void)co_await retryMetadataOp([&]() {
                return ctx_.metadataRepo->updateDocumentExtractionStatus(
                    info.id, true, metadata::ExtractionStatus::Success);
            });

            co_return Result<void>();
        } catch (const std::exception& e) {
            co_return Error{ErrorCode::InternalError, e.what()};
        }
    }

    Result<void> lightIndexForHash(const std::string& hash,
                                   std::size_t maxBytes = 2 * 1024 * 1024) override {
        if (!ctx_.workerExecutor) {
            return Error{ErrorCode::NotInitialized, "Worker executor not available"};
        }

        // Since this is a fire-and-forget, retain a shared handle so the service outlives the
        // coroutine. Without this, tests that tear down the service immediately after invoking
        // lightIndexForHash could destroy the instance while the detached coroutine still runs.
        auto self = shared_from_this();
        boost::asio::co_spawn(
            ctx_.workerExecutor,
            [self, hash, maxBytes]() -> boost::asio::awaitable<void> {
                auto result = co_await self->lightIndexForHash_impl(hash, maxBytes);
                if (!result) {
                    spdlog::warn("lightIndexForHash failed: {}", result.error().message);
                }
            },
            boost::asio::detached);

        return Result<void>();
    }

private:
    AppContext ctx_;
    bool degraded_{false};
    std::string repairDetails_{};
    EnhancedConfig enhancedCfg_{}; // off by default
    EnhancedSearchExecutor enhanced_{};
    std::shared_ptr<yams::search::HotzoneManager> hotzones_{};
    std::shared_ptr<yams::search::SymbolEnricher> symbolEnricher_{};
    float symbolWeight_{0.15f};

    std::string resolveSearchType(const SearchRequest& req, bool* forcedHybridFallback) const {
        if (forcedHybridFallback)
            *forcedHybridFallback = false;

        const std::string requested = req.type.empty() ? "hybrid" : req.type;
        const bool wantsHybrid = (requested == "hybrid" || requested == "semantic");
        const bool searchDisabled = degraded_ || !ctx_.searchEngine;

        if (wantsHybrid && searchDisabled) {
            if (forcedHybridFallback)
                *forcedHybridFallback = true;
            return "keyword";
        }

        return requested;
    }

    boost::asio::awaitable<void> hydrateSnippetsAsync(const SearchRequest& req,
                                                      SearchResponse& resp,
                                                      MetadataTelemetry* telemetry = nullptr) {
        if (!ctx_.metadataRepo) {
            co_return;
        }

        // Defensive cap: avoid hydrating snippets for very large result sets.
        constexpr size_t kMaxSnippetHydration = 200;
        const size_t hydrateCount = std::min(resp.results.size(), kMaxSnippetHydration);
        if (hydrateCount == 0) {
            co_return;
        }

        // Collect all hashes and paths that need hydration
        std::vector<std::string> hashes;
        hashes.reserve(hydrateCount);
        std::unordered_map<std::string, std::vector<size_t>> hashToIndices;
        hashToIndices.reserve(hydrateCount);
        std::unordered_map<std::string, std::vector<size_t>> pathToIndices;
        pathToIndices.reserve(hydrateCount);

        for (size_t i = 0; i < hydrateCount; ++i) {
            const auto& it = resp.results[i];
            if (it.snippet.empty()) {
                if (!it.hash.empty()) {
                    if (hashToIndices[it.hash].empty()) {
                        hashes.push_back(it.hash);
                    }
                    hashToIndices[it.hash].push_back(i);
                } else if (!it.path.empty()) {
                    pathToIndices[it.path].push_back(i);
                }
            }
        }

        if (hashes.empty() && pathToIndices.empty()) {
            co_return;
        }

        // Batch fetch documents by hash (1 query instead of N)
        std::unordered_map<std::string, metadata::DocumentInfo> docsMap;
        docsMap.reserve(hashes.size() + pathToIndices.size());
        if (!hashes.empty()) {
            auto docsResult = co_await retryMetadataOp(
                [&]() { return ctx_.metadataRepo->batchGetDocumentsByHash(hashes); }, 4,
                std::chrono::milliseconds(25), telemetry);
            if (docsResult) {
                docsMap = std::move(docsResult.value());
            }
        }

        // Handle path-based lookups (still need individual queries for these)
        for (const auto& [path, indices] : pathToIndices) {
            auto v = co_await retryMetadataOp(
                [&]() { return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, path); }, 4,
                std::chrono::milliseconds(25), telemetry);
            if (v && !v.value().empty()) {
                const auto& doc = v.value().front();
                docsMap[doc.sha256Hash] = doc;
                // Map path results to hash
                for (size_t idx : indices) {
                    resp.results[idx].hash = doc.sha256Hash;
                }
            }
        }

        // Collect all document IDs for content fetch
        std::vector<int64_t> docIds;
        std::unordered_map<int64_t, std::vector<size_t>> docIdToIndices;

        const auto snippetBudget =
            std::chrono::milliseconds(std::max<int>(0, req.snippetHydrationTimeoutMs));
        const bool budgetEnabled = snippetBudget.count() > 0;
        const auto snippetStart = std::chrono::steady_clock::now();
        bool snippetTimeoutRecorded = false;
        auto markSnippetTimeout = [&]() {
            if (!snippetTimeoutRecorded && budgetEnabled) {
                resp.searchStats["snippet_timeout_hit"] = "true";
                resp.searchStats["snippet_budget_ms"] = std::to_string(snippetBudget.count());
                YAMS_PLOT("search_service::snippet_timeout", 1);
                snippetTimeoutRecorded = true;
            }
        };
        auto budgetExceeded = [&]() {
            if (!budgetEnabled || snippetTimeoutRecorded)
                return false;
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - snippetStart);
            if (elapsed >= snippetBudget) {
                markSnippetTimeout();
                return true;
            }
            return false;
        };
        YAMS_PLOT("search_service::snippet_budget_ms", static_cast<double>(snippetBudget.count()));
        YAMS_PLOT("search_service::snippet_hydrate_cap", static_cast<double>(hydrateCount));

        YAMS_PLOT("search_service::snippet_docs", static_cast<double>(docsMap.size()));
        for (const auto& [hash, doc] : docsMap) {
            docIds.push_back(doc.id);
            if (auto it = hashToIndices.find(hash); it != hashToIndices.end()) {
                docIdToIndices[doc.id] = it->second;
            }
        }

        // Batch fetch content for all documents (1 query instead of N)
        if (!docIds.empty()) {
            YAMS_ZONE_SCOPED_N("search_service::snippet_batch_fetch");
            auto contentResult = co_await retryMetadataOp(
                [&]() { return ctx_.metadataRepo->batchGetContent(docIds); }, 4,
                std::chrono::milliseconds(25), telemetry);

            if (budgetExceeded())
                co_return;

            if (contentResult) {
                const auto& contentMap = contentResult.value();
                YAMS_PLOT("search_service::snippet_content_rows",
                          static_cast<double>(contentMap.size()));

                // Hydrate snippets from in-memory maps (no DB queries!)
                // Limit content text to first 10KB for faster snippet generation
                constexpr size_t kMaxContentForSnippet = 10240;
                for (const auto& [docId, content] : contentMap) {
                    if (budgetExceeded())
                        break;
                    if (!content.contentText.empty()) {
                        std::string_view contentView = content.contentText;
                        if (contentView.size() > kMaxContentForSnippet) {
                            contentView = contentView.substr(0, kMaxContentForSnippet);
                        }
                        auto snippet = utils::createSnippet(std::string(contentView), 200, true);
                        if (auto it = docIdToIndices.find(docId); it != docIdToIndices.end()) {
                            for (size_t idx : it->second) {
                                resp.results[idx].snippet = snippet;
                            }
                        }
                    }
                }

                if (budgetExceeded())
                    co_return;
            }
        }

        if (budgetEnabled && !snippetTimeoutRecorded) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - snippetStart);
            if (elapsed >= snippetBudget) {
                markSnippetTimeout();
            }
        }
    }

    boost::asio::awaitable<void>
    hydrateSnippetsAsync_worker(const SearchRequest& req, SearchResponse& resp,
                                MetadataTelemetry* telemetry = nullptr) {
        // Use batch-optimized version instead
        co_return co_await hydrateSnippetsAsync(req, resp, telemetry);
    }

    boost::asio::awaitable<Result<SearchResponse>>
    pathSearch(const SearchRequest& req, MetadataTelemetry* telemetry = nullptr) {
        static auto globToSqlLike = [](const std::string& glob) {
            std::string like;
            like.reserve(glob.size());
            for (char c : glob) {
                if (c == '*') {
                    like += '%';
                } else if (c == '?') {
                    like += '_';
                } else {
                    like += c;
                }
            }
            return like;
        };

        if (!ctx_.metadataRepo) {
            co_return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }
        SearchResponse resp;
        resp.type = "path";
        resp.usedHybrid = false;
        std::vector<metadata::DocumentInfo> docs;

        const bool wildcard = hasWildcard(req.query);

        std::string likePattern;
        if (wildcard) {
            likePattern = globToSqlLike(req.query);
        } else {
            likePattern = "%" + req.query + "%";
        }

        auto r = co_await retryMetadataOp(
            [&]() { return metadata::queryDocumentsByPattern(*ctx_.metadataRepo, likePattern); }, 4,
            std::chrono::milliseconds(25), telemetry);

        if (r) {
            docs = std::move(r.value());
        } else {
            co_return r.error();
        }

        // Apply additional filters and shape results
        auto push_path = [&](const metadata::DocumentInfo& d) -> boost::asio::awaitable<void> {
            if (!req.extension.empty()) {
                if (d.fileExtension != req.extension && d.fileExtension != ("." + req.extension))
                    co_return;
            }
            if (!req.mimeType.empty() && d.mimeType != req.mimeType)
                co_return;
            if (!(co_await metadataHasTags(ctx_.metadataRepo.get(), d.id, req.tags,
                                           req.matchAllTags, telemetry)))
                co_return;
            if (req.pathsOnly) {
                resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
            } else {
                SearchItem it;
                it.id = d.id;
                it.hash = d.sha256Hash;
                it.title = d.fileName;
                it.path = d.filePath;
                it.score = 1.0; // neutral score for path matches
                resp.results.push_back(std::move(it));
            }
        };
        for (const auto& d : docs) {
            co_await push_path(d);
            if (req.limit != 0) {
                if (req.pathsOnly && resp.paths.size() >= req.limit)
                    break;
                if (!req.pathsOnly && resp.results.size() >= req.limit)
                    break;
            }
        }
        if (req.pathsOnly)
            resp.total = resp.paths.size();
        else
            resp.total = resp.results.size();
        co_return resp;
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

    boost::asio::awaitable<Result<SearchResponse>>
    searchByHashPrefix(const SearchRequest& req, MetadataTelemetry* telemetry = nullptr) {
        const std::string& rawPrefix = !req.hash.empty() ? req.hash : req.query;
        std::string prefix = rawPrefix;
        std::transform(prefix.begin(), prefix.end(), prefix.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        SearchResponse resp;
        resp.type = "hash";
        resp.usedHybrid = false;

        const std::size_t fetchLimit = std::max<std::size_t>(req.limit * 4, 32);
        auto docsResult = co_await retryMetadataOp(
            [&]() { return ctx_.metadataRepo->findDocumentsByHashPrefix(prefix, fetchLimit); }, 4,
            std::chrono::milliseconds(25), telemetry);
        if (!docsResult) {
            co_return Error{ErrorCode::InternalError,
                            "Failed to enumerate documents for hash search: " +
                                docsResult.error().message};
        }

        const auto& docs = docsResult.value();
        for (const auto& doc : docs) {
            // Optional path and tag filters for CLI parity
            bool pathOk = true;
            if (!req.pathPattern.empty()) {
                if (hasWildcard(req.pathPattern)) {
                    std::string pattern = req.pathPattern;
                    if (!pattern.empty() && pattern.front() != '*' && pattern.front() != '/' &&
                        pattern.find(":/") == std::string::npos) {
                        pattern = "*" + pattern;
                    }
                    pathOk = wildcardMatch(doc.filePath, pattern);
                } else {
                    pathOk = doc.filePath.find(req.pathPattern) != std::string::npos;
                }
            }

            bool metaFiltersOk = true;
            if (!req.extension.empty()) {
                if (doc.fileExtension != req.extension &&
                    doc.fileExtension != ("." + req.extension)) {
                    metaFiltersOk = false;
                }
            }
            if (!req.mimeType.empty() && doc.mimeType != req.mimeType) {
                metaFiltersOk = false;
            }
            if (!req.fileType.empty()) {
                auto classified = utils::classifyFileType(doc.mimeType, doc.fileExtension);
                if (classified != req.fileType) {
                    metaFiltersOk = false;
                }
            }

            if (!pathOk || !metaFiltersOk ||
                !(co_await metadataHasTags(ctx_.metadataRepo.get(), doc.id, req.tags,
                                           req.matchAllTags, telemetry))) {
                continue;
            }

            if (req.pathsOnly) {
                resp.paths.push_back(doc.filePath);
            } else {
                SearchItem it;
                it.id = doc.id;
                it.hash = doc.sha256Hash;
                it.title = doc.fileName;
                it.path = doc.filePath;
                it.score = 1.0;
                it.snippet = "";
                it.mimeType = doc.mimeType;
                it.fileType = utils::classifyFileType(doc.mimeType, doc.fileExtension);
                it.size = static_cast<std::uint64_t>(doc.fileSize);
                it.created = doc.createdTime.time_since_epoch().count();
                it.modified = doc.modifiedTime.time_since_epoch().count();
                it.indexed = doc.indexedTime.time_since_epoch().count();
                it.metadata["hash"] = doc.sha256Hash;
                it.metadata["indexed"] = std::to_string(doc.indexedTime.time_since_epoch().count());
                it.metadata["path"] = doc.filePath;
                resp.results.push_back(std::move(it));
            }

            if ((req.pathsOnly ? resp.paths.size() : resp.results.size()) >= req.limit)
                break;
        }

        resp.total = req.pathsOnly ? resp.paths.size() : resp.results.size();
        resp.wasHashSearch = true;
        resp.detectedHashQuery = rawPrefix;
        co_return resp;
    }

    Result<SearchResponse> hybridSearch(const SearchRequest& req,
                                        const yams::search::ExtractScope& scope,
                                        MetadataTelemetry* telemetry,
                                        [[maybe_unused]] const std::string& pathPattern) {
        auto parseEpochFilter =
            [](const std::string& value) -> Result<std::optional<std::int64_t>> {
            if (value.empty()) {
                return std::optional<std::int64_t>{};
            }
            auto parsed = utils::parseTimeExpression(value);
            if (!parsed) {
                return parsed.error();
            }
            return std::optional<std::int64_t>{parsed.value()};
        };

        auto createdAfter = parseEpochFilter(req.createdAfter);
        if (!createdAfter) {
            return createdAfter.error();
        }
        auto createdBefore = parseEpochFilter(req.createdBefore);
        if (!createdBefore) {
            return createdBefore.error();
        }
        auto modifiedAfter = parseEpochFilter(req.modifiedAfter);
        if (!modifiedAfter) {
            return modifiedAfter.error();
        }
        auto modifiedBefore = parseEpochFilter(req.modifiedBefore);
        if (!modifiedBefore) {
            return modifiedBefore.error();
        }
        auto indexedAfter = parseEpochFilter(req.indexedAfter);
        if (!indexedAfter) {
            return indexedAfter.error();
        }
        auto indexedBefore = parseEpochFilter(req.indexedBefore);
        if (!indexedBefore) {
            return indexedBefore.error();
        }

        // Build SearchParams for the new SearchEngine API
        yams::search::SearchParams params;
        params.limit = static_cast<int>(std::min<std::size_t>(
            req.limit, static_cast<std::size_t>(std::numeric_limits<int>::max())));
        params.offset = 0;
        // Propagate tag filters to the search engine (used for candidate gathering/ranking)
        params.tags = req.tags;
        params.matchAllTags = req.matchAllTags;

        // Apply scope filters
        if (!scope.ext.empty()) {
            std::string ext = scope.ext;
            std::transform(ext.begin(), ext.end(), ext.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (!ext.empty() && ext.front() == '.')
                ext.erase(ext.begin());
            params.extension = ext;
        }
        if (!scope.mime.empty()) {
            params.mimeType = scope.mime;
        }
        if (params.extension.has_value() == false && !req.extension.empty()) {
            std::string ext = req.extension;
            std::transform(ext.begin(), ext.end(), ext.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (!ext.empty() && ext.front() == '.') {
                ext.erase(ext.begin());
            }
            params.extension = ext;
        }
        if (params.mimeType.has_value() == false && !req.mimeType.empty()) {
            params.mimeType = req.mimeType;
        }
        if (modifiedAfter.value()) {
            params.modifiedAfter = modifiedAfter.value();
        }
        if (modifiedBefore.value()) {
            params.modifiedBefore = modifiedBefore.value();
        }

        // Defensive limit: cap engine query to prevent memory exhaustion
        constexpr int kMaxSearchResults = 10000;
        params.limit = std::min(params.limit, kMaxSearchResults);

        // GLiNER concept extraction is now handled inside SearchEngine in parallel
        // with embedding generation and text-based components. Concepts are used
        // in fusion for boosting, not for query expansion.
        auto searchRes = ctx_.searchEngine->searchWithResponse(req.query, params);
        if (!searchRes) {
            spdlog::warn("Search failed ({}), falling back to keyword-only search",
                         searchRes.error().message);
            return metadataSearch(req, telemetry);
        }

        const auto& engineResponse = searchRes.value();
        const auto& vec = engineResponse.results;

        // Enforce tag filters (hybrid path previously ignored tags)
        // Use document hashes for filtering since SearchEngine fusion only populates hash, not ID
        std::optional<std::unordered_set<std::string>> tagDocHashes;
        if (!req.tags.empty()) {
            auto docsResult = ctx_.metadataRepo->findDocumentsByTags(req.tags, req.matchAllTags);
            if (!docsResult) {
                return docsResult.error();
            }
            std::unordered_set<std::string> hashes;
            const auto& docsVec = docsResult.value();
            hashes.reserve(docsVec.size());
            for (const auto& doc : docsVec) {
                hashes.insert(doc.sha256Hash);
            }
            tagDocHashes = std::move(hashes);
        }

        // Always build a filtered candidate list so limits respect tag filtering
        std::vector<const metadata::SearchResult*> candidates;
        candidates.reserve(vec.size());

        auto normalizeLower = [](std::string value) {
            std::transform(value.begin(), value.end(), value.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            return value;
        };
        auto normalizeExtension = [&](std::string value) {
            value = normalizeLower(std::move(value));
            if (!value.empty() && value.front() == '.') {
                value.erase(value.begin());
            }
            return value;
        };

        const std::string reqMimeLower = normalizeLower(req.mimeType);
        const std::string reqExtLower = normalizeExtension(req.extension);
        const std::string reqFileTypeLower = normalizeLower(req.fileType);
        const auto createdAfterEpoch = createdAfter.value();
        const auto createdBeforeEpoch = createdBefore.value();
        const auto modifiedAfterEpoch = modifiedAfter.value();
        const auto modifiedBeforeEpoch = modifiedBefore.value();
        const auto indexedAfterEpoch = indexedAfter.value();
        const auto indexedBeforeEpoch = indexedBefore.value();
        std::vector<std::string> effectivePathPatterns = req.pathPatterns;
        if (effectivePathPatterns.empty() && !pathPattern.empty()) {
            effectivePathPatterns.push_back(pathPattern);
        }

        auto matchesPathFilters = [&](const std::string& filePath) {
            if (effectivePathPatterns.empty()) {
                return true;
            }
            for (const auto& patternRaw : effectivePathPatterns) {
                if (patternRaw.empty()) {
                    continue;
                }
                if (hasWildcard(patternRaw)) {
                    std::string pattern = patternRaw;
                    if (!pattern.empty() && pattern.front() != '*' && pattern.front() != '/' &&
                        pattern.find(":/") == std::string::npos) {
                        pattern.insert(pattern.begin(), '*');
                    }
                    if (wildcardMatch(filePath, pattern)) {
                        return true;
                    }
                } else if (filePath.find(patternRaw) != std::string::npos) {
                    return true;
                }
            }
            return false;
        };

        for (const auto& r : vec) {
            if (tagDocHashes && tagDocHashes->count(r.document.sha256Hash) == 0) {
                continue;
            }

            const auto& doc = r.document;

            if (!matchesPathFilters(doc.filePath)) {
                continue;
            }

            if (!reqExtLower.empty()) {
                if (normalizeExtension(doc.fileExtension) != reqExtLower) {
                    continue;
                }
            }

            if (!reqMimeLower.empty()) {
                if (normalizeLower(doc.mimeType) != reqMimeLower) {
                    continue;
                }
            }

            const std::string classifiedType =
                normalizeLower(utils::classifyFileType(doc.mimeType, doc.fileExtension));

            if (!reqFileTypeLower.empty() && classifiedType != reqFileTypeLower) {
                continue;
            }
            if (req.textOnly && classifiedType != "text") {
                continue;
            }
            if (req.binaryOnly && classifiedType == "text") {
                continue;
            }

            const auto createdEpoch = doc.createdTime.time_since_epoch().count();
            const auto modifiedEpoch = doc.modifiedTime.time_since_epoch().count();
            const auto indexedEpoch = doc.indexedTime.time_since_epoch().count();

            if (createdAfterEpoch && createdEpoch < *createdAfterEpoch) {
                continue;
            }
            if (createdBeforeEpoch && createdEpoch > *createdBeforeEpoch) {
                continue;
            }
            if (modifiedAfterEpoch && modifiedEpoch < *modifiedAfterEpoch) {
                continue;
            }
            if (modifiedBeforeEpoch && modifiedEpoch > *modifiedBeforeEpoch) {
                continue;
            }
            if (indexedAfterEpoch && indexedEpoch < *indexedAfterEpoch) {
                continue;
            }
            if (indexedBeforeEpoch && indexedEpoch > *indexedBeforeEpoch) {
                continue;
            }

            candidates.push_back(&r);
        }

        if (vec.size() > static_cast<size_t>(kMaxSearchResults)) {
            spdlog::warn("Search returned {} results, truncating to {} to prevent crash",
                         vec.size(), kMaxSearchResults);
        }

        SearchResponse resp;
        resp.type = "hybrid";
        resp.usedHybrid = true;
        resp.executionTimeMs = engineResponse.executionTimeMs;
        resp.isDegraded = engineResponse.isDegraded;
        resp.timedOutComponents = engineResponse.timedOutComponents;
        resp.failedComponents = engineResponse.failedComponents;
        resp.contributingComponents = engineResponse.contributingComponents;
        resp.skippedComponents = engineResponse.skippedComponents;
        resp.componentTimingMicros = engineResponse.componentTimingMicros;
        resp.usedEarlyTermination = engineResponse.usedEarlyTermination;
        resp.facets = engineResponse.facets;

        if (resp.isDegraded) {
            spdlog::info(
                "Search completed in degraded mode: {} timed out, {} failed, {} contributing",
                resp.timedOutComponents.size(), resp.failedComponents.size(),
                resp.contributingComponents.size());
        }

        if (req.pathsOnly) {
            const size_t effectiveLimit = req.limit > 0 ? req.limit : candidates.size();
            for (const auto* r : candidates) {
                if (resp.paths.size() >= effectiveLimit)
                    break;
                if (!r->document.filePath.empty()) {
                    resp.paths.push_back(r->document.filePath);
                }
            }
            resp.total = resp.paths.size();
            return resp;
        }

        {
            // Defensive: limit processing to prevent crash on large result sets
            // Also enforce req.limit to honor user's requested limit
            constexpr size_t kMaxProcessableResults = 10000;
            const size_t effectiveLimit = req.limit > 0 ? req.limit : kMaxProcessableResults;
            const size_t n = std::min({candidates.size(), kMaxProcessableResults, effectiveLimit});

            // Cap worker count to avoid thread explosion
            constexpr size_t kMaxWorkers = 16;
            const size_t workers = std::min(recommendedWorkers(n), kMaxWorkers);
            YAMS_PLOT("search_service::candidates", static_cast<double>(n));

            std::atomic<size_t> next{0};
            std::vector<std::optional<SearchItem>> slots(n);

            // Store hash for post-processing symbol enrichment
            std::vector<std::string> hashes(n);

            auto worker = [&]() {
                YAMS_ZONE_SCOPED_N("search_service::result_shape_worker");
                while (true) {
                    const size_t i = next.fetch_add(1);
                    if (i >= n)
                        break;
                    const auto& r = *candidates[i];
                    SearchItem it;
                    it.id = static_cast<int64_t>(i + 1);
                    it.hash = r.document.sha256Hash;
                    it.title = r.document.fileName;
                    it.path = r.document.filePath;
                    it.score = r.score;
                    if (!r.snippet.empty())
                        it.snippet = r.snippet;

                    // Copy score breakdown if available
                    it.vectorScore = r.vectorScore;
                    it.keywordScore = r.keywordScore;
                    it.kgEntityScore = r.kgScore;
                    it.structuralScore = r.pathScore;
                    // tagScore and symbolScore mapped to SearchItem if needed

                    // Store hash for symbol enrichment post-processing
                    hashes[i] = r.document.sha256Hash;

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

            // Build result vector and track original indices for hash lookup
            std::vector<size_t> originalIndices;
            originalIndices.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                if (slots[i].has_value()) {
                    resp.results.push_back(std::move(*slots[i]));
                    originalIndices.push_back(i);
                }
            }

            // PBI-074: Apply symbol enrichment to TOP-K results only (post-processing)
            // This avoids the N+1 query problem by only enriching a small subset
            if (symbolEnricher_ && symbolWeight_ > 0.0f && !resp.results.empty()) {
                YAMS_ZONE_SCOPED_N("search_service::symbol_enrichment_topk");
                constexpr size_t kSymbolEnrichLimit = 25;
                const size_t enrichCount = std::min(resp.results.size(), kSymbolEnrichLimit);
                const std::string& queryText = req.query;
                std::string queryLower = queryText;
                std::transform(queryLower.begin(), queryLower.end(), queryLower.begin(),
                               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                bool boosted = false;

                for (size_t i = 0; i < enrichCount; ++i) {
                    auto& it = resp.results[i];
                    const size_t origIdx = originalIndices[i];

                    yams::search::SearchResultItem enrichItem;
                    enrichItem.path = it.path;
                    enrichItem.metadata["sha256_hash"] = hashes[origIdx];

                    if (symbolEnricher_->enrichResult(enrichItem, queryText)) {
                        if (enrichItem.symbolContext && enrichItem.symbolContext->isSymbolQuery) {
                            bool queryMatchesSymbol = false;
                            if (!queryLower.empty()) {
                                for (const auto& symbol : enrichItem.symbolContext->symbols) {
                                    std::string nameLower = symbol.name;
                                    std::transform(nameLower.begin(), nameLower.end(),
                                                   nameLower.begin(), [](unsigned char c) {
                                                       return static_cast<char>(std::tolower(c));
                                                   });
                                    std::string qualifiedLower = symbol.qualifiedName;
                                    std::transform(qualifiedLower.begin(), qualifiedLower.end(),
                                                   qualifiedLower.begin(), [](unsigned char c) {
                                                       return static_cast<char>(std::tolower(c));
                                                   });

                                    if ((!nameLower.empty() &&
                                         queryLower.find(nameLower) != std::string::npos) ||
                                        (!qualifiedLower.empty() &&
                                         queryLower.find(qualifiedLower) != std::string::npos)) {
                                        queryMatchesSymbol = true;
                                        break;
                                    }
                                }
                            }

                            if (!queryMatchesSymbol) {
                                continue;
                            }

                            const float symbolScore =
                                std::clamp(enrichItem.symbolContext->symbolScore, 0.0f, 1.0f);
                            const float definitionScore =
                                std::clamp(enrichItem.symbolContext->definitionScore, 1.0f, 1.5f);

                            float boost = 1.0f + (symbolWeight_ * symbolScore);
                            boost *= (1.0f + (definitionScore - 1.0f) * 0.35f);
                            boost = std::clamp(boost, 1.0f, 1.35f);
                            it.score *= boost;
                            boosted = boosted || (boost != 1.0f);
                        }
                    }
                }

                // Re-sort after symbol boost applied
                if (boosted) {
                    std::sort(
                        resp.results.begin(), resp.results.end(),
                        [](const SearchItem& a, const SearchItem& b) { return a.score > b.score; });
                }
            }
        }

        applyExtensionFacets(resp);

        resp.total = resp.results.size();
        return resp;
    }

    Result<SearchResponse> metadataSearch(const SearchRequest& req,
                                          [[maybe_unused]] MetadataTelemetry* telemetry = nullptr) {
        YAMS_ZONE_SCOPED_N("search_service::keyword_pipeline");
        // Prepare query - escape regex if literalText is requested
        std::string processedQuery = req.query;

        if (req.literalText) {
            // Escape regex special characters
            processedQuery = escapeRegex(req.query);
        }

        // Get docIds for tags if provided
        std::optional<std::vector<int64_t>> docIds;
        if (!req.tags.empty()) {
            auto docsResult = ctx_.metadataRepo->findDocumentsByTags(req.tags, req.matchAllTags);
            if (docsResult) {
                std::vector<int64_t> ids;
                const auto& docsVec = docsResult.value();
                ids.reserve(docsVec.size());
                for (const auto& doc : docsVec) {
                    ids.push_back(doc.id);
                }
                docIds = std::move(ids);
            }
        }

        // Pre-filter by pathPatterns using SQL-level glob matching (like grep does)
        // This significantly improves performance by reducing the FTS search scope
        if (!req.pathPatterns.empty() && !docIds.has_value()) {
            YAMS_ZONE_SCOPED_N("search_service::path_prefilter");
            spdlog::debug("[SearchService] Pre-filtering by {} path patterns before FTS",
                          req.pathPatterns.size());
            auto patternDocsRes =
                metadata::queryDocumentsByGlobPatterns(*ctx_.metadataRepo, req.pathPatterns, 0);
            if (patternDocsRes) {
                std::vector<int64_t> ids;
                ids.reserve(patternDocsRes.value().size());
                for (const auto& doc : patternDocsRes.value()) {
                    ids.push_back(doc.id);
                }
                spdlog::debug("[SearchService] Path pattern filter matched {} documents",
                              ids.size());
                docIds = std::move(ids);
            }
        } else if (!req.pathPatterns.empty() && docIds.has_value()) {
            // If we already have docIds from tags, intersect with path-filtered docs
            YAMS_ZONE_SCOPED_N("search_service::path_tag_intersect");
            spdlog::debug("[SearchService] Intersecting tag filter with {} path patterns",
                          req.pathPatterns.size());
            auto patternDocsRes =
                metadata::queryDocumentsByGlobPatterns(*ctx_.metadataRepo, req.pathPatterns, 0);
            if (patternDocsRes) {
                std::unordered_set<int64_t> pathDocIds;
                for (const auto& doc : patternDocsRes.value()) {
                    pathDocIds.insert(doc.id);
                }
                std::vector<int64_t> intersected;
                for (int64_t id : docIds.value()) {
                    if (pathDocIds.count(id) > 0) {
                        intersected.push_back(id);
                    }
                }
                spdlog::debug("[SearchService] After intersection: {} documents remain",
                              intersected.size());
                docIds = std::move(intersected);
            }
        }

        // Session-isolated memory filtering (PBI-082)
        if (!req.globalSearch && req.useSession) {
            std::string sessionId = req.sessionName;
            if (sessionId.empty()) {
                auto sessionSvc = makeSessionService(&ctx_);
                if (sessionSvc) {
                    if (auto curr = sessionSvc->current()) {
                        sessionId = *curr;
                    }
                }
            }
            if (!sessionId.empty()) {
                auto sessionDocsRes = ctx_.metadataRepo->findDocumentsBySessionId(sessionId);
                if (sessionDocsRes) {
                    std::unordered_set<int64_t> sessionDocIds;
                    for (const auto& doc : sessionDocsRes.value()) {
                        sessionDocIds.insert(doc.id);
                    }
                    if (!docIds.has_value()) {
                        std::vector<int64_t> ids(sessionDocIds.begin(), sessionDocIds.end());
                        docIds = std::move(ids);
                    } else {
                        std::vector<int64_t> intersected;
                        for (int64_t id : docIds.value()) {
                            if (sessionDocIds.count(id) > 0) {
                                intersected.push_back(id);
                            }
                        }
                        docIds = std::move(intersected);
                    }
                    spdlog::debug("[SearchService] Session filter: {} docs in session '{}'",
                                  docIds->size(), sessionId);
                }
            }
        }

        auto convertResults = [&](const metadata::SearchResults& metaResults) {
            YAMS_ZONE_SCOPED_N("search_service::convert_results");
            std::vector<SearchItem> serviceResults;
            serviceResults.reserve(metaResults.results.size());
            for (const auto& item : metaResults.results) {
                SearchItem it;
                it.id = item.document.id;
                it.hash = item.document.sha256Hash;
                it.title = item.document.fileName;
                it.path = item.document.filePath;
                it.score = item.score;
                it.snippet = item.snippet;

                // Copy score breakdown if available
                it.vectorScore = item.vectorScore;
                it.keywordScore = item.keywordScore;
                it.kgEntityScore = item.kgScore;
                it.structuralScore = item.pathScore;

                serviceResults.push_back(std::move(it));
            }
            return serviceResults;
        };

        auto runFuzzySearch = [&](const SearchRequest& searchReq) -> Result<SearchResponse> {
            YAMS_ZONE_SCOPED_N("search_service::fuzzy_search");
            auto r = ctx_.metadataRepo->fuzzySearch(processedQuery, searchReq.similarity,
                                                    static_cast<int>(searchReq.limit), docIds);
            if (!r) {
                return Error{ErrorCode::InternalError, "Fuzzy search failed: " + r.error().message};
            }

            const auto& res = r.value();
            YAMS_PLOT("search_service::fuzzy_total", static_cast<double>(res.totalCount));

            SearchResponse resp;
            resp.total = res.totalCount;
            resp.type = "fuzzy";
            resp.executionTimeMs = res.executionTimeMs;
            resp.usedHybrid = false;

            if (searchReq.pathsOnly) {
                const size_t effectiveLimit =
                    searchReq.limit > 0 ? searchReq.limit : res.results.size();
                for (const auto& item : res.results) {
                    if (resp.paths.size() >= effectiveLimit)
                        break;
                    const auto& d = item.document;
                    resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                }
                resp.total = resp.paths.size();
                return resp;
            }

            resp.results = convertResults(res);
            normalizeScores(resp.results, resp.type);
            if (!searchReq.pathsOnly) {
                applyExtensionFacets(resp);
            }
            return resp;
        };

        auto runFullTextSearch = [&](const SearchRequest& searchReq,
                                     bool allowAutoFuzzyFallback) -> Result<SearchResponse> {
            YAMS_ZONE_SCOPED_N("search_service::keyword_fulltext");
            // If docIds is set to an empty vector (from tag/path/session intersection with no
            // matches), return empty results immediately since we're filtering to a known-empty
            // set. Note: std::nullopt means "no filter", but empty vector means "filter to
            // nothing".
            if (docIds.has_value() && docIds->empty()) {
                SearchResponse resp;
                resp.total = 0;
                resp.type = "full-text";
                resp.usedHybrid = false;
                return resp;
            }

            auto r = ctx_.metadataRepo->search(processedQuery, static_cast<int>(searchReq.limit), 0,
                                               docIds);
            if (!r) {
                return Error{ErrorCode::InternalError,
                             "Full-text search failed: " + r.error().message};
            }

            const auto& res = r.value();
            YAMS_PLOT("search_service::fulltext_total", static_cast<double>(res.totalCount));
            if (allowAutoFuzzyFallback && res.totalCount == 0) {
                SearchRequest fallbackReq = searchReq;
                fallbackReq.fuzzy = true;
                return runFuzzySearch(fallbackReq);
            }

            SearchResponse resp;
            resp.total = res.totalCount;
            resp.type = "full-text";
            resp.executionTimeMs = res.executionTimeMs;
            resp.usedHybrid = false;

            if (searchReq.pathsOnly) {
                const size_t effectiveLimit =
                    searchReq.limit > 0 ? searchReq.limit : res.results.size();
                for (const auto& item : res.results) {
                    if (resp.paths.size() >= effectiveLimit)
                        break;
                    const auto& d = item.document;
                    resp.paths.push_back(!d.filePath.empty() ? d.filePath : d.fileName);
                }
                resp.total = resp.paths.size();
                return resp;
            }

            resp.results = convertResults(res);
            normalizeScores(resp.results, resp.type);
            if (!searchReq.pathsOnly) {
                applyExtensionFacets(resp);
            }
            return resp;
        };

        // Fuzzy or full-text via metadata repository. When the caller enables fuzzy, run both
        // searches and merge so literal/BM25 hits are never dropped.
        if (!req.fuzzy) {
            return runFullTextSearch(req, true);
        }

        auto keywordReq = req;
        keywordReq.fuzzy = false;
        auto keywordResults = runFullTextSearch(keywordReq, false);
        auto fuzzyResults = runFuzzySearch(req);
        YAMS_PLOT("search_service::keyword_results",
                  keywordResults ? keywordResults->results.size() : 0);
        YAMS_PLOT("search_service::fuzzy_results", fuzzyResults ? fuzzyResults->results.size() : 0);

        if (!keywordResults && !fuzzyResults) {
            return fuzzyResults.error();
        }
        YAMS_ZONE_SCOPED_N("search_service::merge_keyword_fuzzy");

        if (!keywordResults) {
            return fuzzyResults;
        }
        if (!fuzzyResults) {
            return keywordResults;
        }

        auto combined = keywordResults.value();
        combined.type = "full-text+fuzzy";
        combined.executionTimeMs =
            std::max(combined.executionTimeMs, fuzzyResults.value().executionTimeMs);

        if (req.pathsOnly) {
            std::unordered_set<std::string> seen(combined.paths.begin(), combined.paths.end());
            for (const auto& path : fuzzyResults.value().paths) {
                if (seen.insert(path).second) {
                    combined.paths.push_back(path);
                }
            }
            if (req.limit > 0 && combined.paths.size() > req.limit) {
                combined.paths.resize(req.limit);
            }
            combined.total = combined.paths.size();
            return combined;
        }

        std::unordered_set<int64_t> seenIds;
        for (const auto& item : combined.results) {
            seenIds.insert(item.id);
        }
        for (const auto& item : fuzzyResults.value().results) {
            if (seenIds.insert(item.id).second) {
                combined.results.push_back(item);
            }
        }
        if (req.limit > 0 && combined.results.size() > req.limit) {
            combined.results.resize(req.limit);
        }
        combined.total = combined.results.size();
        if (!req.pathsOnly) {
            applyExtensionFacets(combined);
        }
        return combined;
    }
};

std::shared_ptr<ISearchService> makeSearchService(const AppContext& ctx) {
    return std::make_shared<SearchServiceImpl>(ctx);
}

} // namespace yams::app::services
